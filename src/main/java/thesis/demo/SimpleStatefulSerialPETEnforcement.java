package thesis.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.pet.PETDescriptor;
import thesis.pet.PETFragment;
import thesis.pet.PETProvider;
import thesis.pet.repo.NoPET;

/**
 * @param <T>
 */
public class SimpleStatefulSerialPETEnforcement<T extends Data<?>> extends ProcessFunction<Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>> {

    PETFragment pet;
    String petDescriptionAsString;
    private final Class<T> pETAffectedClass;

    public SimpleStatefulSerialPETEnforcement(Class<T> inputClass, String petDescriptionAsString) {
        this.pETAffectedClass = inputClass;
        this.petDescriptionAsString = petDescriptionAsString;
    }

    public SimpleStatefulSerialPETEnforcement(Class<T> inputClass) {
        //this.pet = new NoPET<>();
        this.pETAffectedClass = inputClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (petDescriptionAsString != null) {
            try {
                PETDescriptor petDescriptor = new PETDescriptor(petDescriptionAsString);
                this.pet = PETProvider.build(petDescriptor);
                //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " PET is set to " + petDescriptionAsString.hashCode());
            } catch (Exception e) {
                System.out.println(petDescriptionAsString);
                System.out.println("PET cannot be loaded. Set PET to default.");
                this.pet = new NoPET();
            }
        } else {
            this.pet = new NoPET();
            //System.out.println(getRuntimeContext().getTaskNameWithSubtasks()+ " PET is set to default.");
        }
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, ProcessFunction<Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        VehicleContext vc = value.f1;
//        T originalData = vc.extractRecord(pETAffectedClass);
//        T processed = pet.execute(originalData);
        //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + "Processing : " + vc);
        T data = vc.extractRecord(pETAffectedClass);
        data.setProcessBegin(System.currentTimeMillis());
        VehicleContext output = process(vc);
//        output.substitute(processed);
        out.collect(Tuple2.of(PETDescriptor.getLabel(petDescriptionAsString), output));
    }

    private VehicleContext process(VehicleContext vc){
        VehicleContext output = pet.execute(vc);
        T data = output.extractRecord(pETAffectedClass);
        data.setProcessEnd(System.currentTimeMillis());
        output.update(data);
        return output;
    }
}
