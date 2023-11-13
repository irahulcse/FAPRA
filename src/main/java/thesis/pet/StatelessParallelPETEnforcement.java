package thesis.pet;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.flink.Descriptors;
import thesis.flink.SwitchingDecision;
import thesis.jobs.ImageOnly;
import thesis.pet.repo.NoPET;
import thesis.util.OutputTool;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

public class StatelessParallelPETEnforcement<T extends Data<?>> extends KeyedBroadcastProcessFunction<Integer, Tuple2<String, VehicleContext>, SwitchingDecision, Tuple2<String, VehicleContext>> {

    // For measurement purpose, the switching decision is emitted to the feedback processor to record the sync time
    private final OutputTag<Tuple3<Integer, Boolean, SwitchingDecision>> signalSideOutput = new OutputTag<>("signal feedback", new TypeHint<Tuple3<Integer, Boolean, SwitchingDecision>>() {
    }.getTypeInfo());
    PETFragment petFragmentWorking;
    PETFragment petFragmentPending;
    private Tuple3<String, Long, UUID> currentSwitchingInfo;
    private final String targetDataSection;
    private final Class<T> pETAffectedClass;
    private SwitchingDecision decision;
    private final boolean measure;
    private long latestRecordTimestamp = 0L;

    public StatelessParallelPETEnforcement(String target, Class<T> inputClass, boolean measure) {
        this.targetDataSection = target;
        this.pETAffectedClass = inputClass;
        this.measure = measure;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        petFragmentWorking = new NoPET();
        currentSwitchingInfo = Tuple3.of(null, System.currentTimeMillis(), null);
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, KeyedBroadcastProcessFunction<Integer, Tuple2<String, VehicleContext>, SwitchingDecision, Tuple2<String, VehicleContext>>.ReadOnlyContext ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        if (value.f1.extractRecord(pETAffectedClass).getGenerateTime()==latestRecordTimestamp){
            //System.out.println(targetDataSection+ " Already processed. Discard.");
            return;
        }
        VehicleContext vc = value.f1;
        T data = vc.extractRecord(pETAffectedClass);
        data.setProcessBegin(System.currentTimeMillis());

        ReadOnlyBroadcastState<String, Tuple3<String, Long, UUID>> readOnlyBroadcastState = ctx.getBroadcastState(Descriptors.decisionMapState);
        Tuple3<String, Long, UUID> broadcastSwitchingInfo = readOnlyBroadcastState.get(targetDataSection);

        if (!broadcastSwitchingInfo.equals(currentSwitchingInfo)) {
            if (vc.isTrigger()) {
                if (!vc.getTriggerIDs().contains(broadcastSwitchingInfo.f2)) {
                    out.collect(Tuple2.of(targetDataSection, enforce(vc)));
                } else {
                    switchPET();
                    currentSwitchingInfo = broadcastSwitchingInfo;
                    out.collect(Tuple2.of(targetDataSection, enforce(vc)));
                }
            } else {
                if (data.getGenerateTime() < currentSwitchingInfo.f1) {
                    out.collect(Tuple2.of(targetDataSection, enforce(vc)));
                } else {
                    switchPET();
                    currentSwitchingInfo = broadcastSwitchingInfo;
                    out.collect(Tuple2.of(targetDataSection, enforce(vc)));
                }
            }
        } else {
            out.collect(Tuple2.of(targetDataSection, enforce(vc)));
        }

        //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() +" PET Enforcement: processed data with timestamp " + value.getGenerateTime() + " with PET? " + currentState);
        //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " emitted processed data at " + System.currentTimeMillis());
    }

    @Override
    public void processBroadcastElement(SwitchingDecision value, KeyedBroadcastProcessFunction<Integer, Tuple2<String, VehicleContext>, SwitchingDecision, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        if (!value.dataSections.contains(targetDataSection)) return;
       //System.out.println("Responsible for: "+ targetDataSection+ "Received switching decision " + value.decisionID.toString().split("-")[0]);
        decision = value;
        decision.setInstantiationBegin(System.currentTimeMillis());
        BroadcastState<String, Tuple3<String, Long, UUID>> broadcastState = ctx.getBroadcastState(Descriptors.decisionMapState);
        for (String ds : value.dataSections) {
            broadcastState.put(ds, Tuple3.of(value.petDescriptionString, value.generationTime, value.decisionID));
        }
        // PET provision
        petFragmentPending = PETProvider.build(new PETDescriptor(value.petDescriptionString));
        decision.setInstantiationEnd(System.currentTimeMillis());
        decision.setEvaluationMode(SwitchingDecision.DISTRIBUTED);
        ctx.output(signalSideOutput, Tuple3.of(getRuntimeContext().getIndexOfThisSubtask(), true, decision));
        if (measure) {
            OutputTool.log(decision, "#" + getRuntimeContext().getIndexOfThisSubtask());
        }

    }

    private VehicleContext enforce(VehicleContext vc) {
        VehicleContext processed = petFragmentWorking.execute(vc);
        T data = processed.extractRecord(pETAffectedClass);
        data.setProcessEnd(System.currentTimeMillis());
        processed.update(data);
        latestRecordTimestamp = vc.extractRecord(pETAffectedClass).getGenerateTime();
        return processed;
    }

    private void switchPET() throws IOException {
        decision.setSwitchBegin(System.currentTimeMillis());
        //
        petFragmentWorking = petFragmentPending;
        petFragmentPending = null;
        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " switch to new PET");
        //
        decision.setSwitchEnd(System.currentTimeMillis());
        if (measure) {
            OutputTool.log(decision, "#" + getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    public PETFragment preparePET(String petPath) throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, IOException {
        String petFromFile = new String(Files.readAllBytes(Paths.get(petPath)));
        PETDescriptor descriptor = new PETDescriptor(petFromFile);
        return PETProvider.build(descriptor);
    }

    public OutputTag<Tuple3<Integer, Boolean, SwitchingDecision>> getSignalSideOutput() {
        return signalSideOutput;
    }
}
