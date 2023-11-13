package thesis.jobs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import thesis.common.sources.PolicyCreator;
import thesis.common.sources.SpeedSourceEvalOne;
import thesis.context.VehicleContext;
import thesis.context.data.ScalarData;
import thesis.demo.FlatMapper;
import thesis.flink.Descriptors;
import thesis.flink.FilterByDataSection;
import thesis.flink.SituationEvaluatorFromContext;
import thesis.flink.SwitchingDecision;
import thesis.flink.sink.DataLogger;
import thesis.pet.StatefulSerialPETEnforcement;
import thesis.pet.StatefulSerialPETEnforcementUpgrade;
import thesis.policy.Policy;
import thesis.util.DBWrapper;
import thesis.util.OutputTool;

import java.io.IOException;


public class CentralizedStatefulSpeedOnly {

    public static DBWrapper dbWrapper = new DBWrapper();
    public static OutputTool outputTool;

    static {
        try {
            outputTool = new OutputTool();
            Runtime.getRuntime().addShutdownHook(outputTool.complete(false));
        } catch (IOException e) {
            System.out.println("Error initializing output tool.");
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ScalarData> scalarStream = env.addSource(new SpeedSourceEvalOne("speed", 1, 110));
        // DataStream<LocationData> locationStream = env.addSource(new LocationSource());
        //DataStream<ImageData> cameraStream = env.addSource(new CameraSource(2));

        BroadcastStream<Policy> policyBroadcastStream = env.fromElements(PolicyCreator.generatePolicy("statefulSpeedPolicy")).broadcast(Descriptors.policyStateDescriptor);

        //DataStream<VehicleContext> contextBuildingStream = scalarStream.connect(locationStream).process(new TwoInputContextBuilder<>());
        //DataStream<VehicleContext> completeContext = contextBuildingStream.connect(cameraStream).process(new VehicleContextBuilder<>());

        //Combine the input that form the context into one stream. Then fan out to be keyed by the data section to be evaluated.
        //DataStream<Tuple2<ScalarData, LocationData>> contextStream = scalarStream.connect(locationStream).process(new ContextBuilder()).setParallelism(1);
        //DataStream<VehicleContext> contextStream = scalarStream.connect(cameraStream).process(new TwoInputContextBuilder<>());
        //KeyedStream<Tuple2<String, VehicleContext>, String> keyedContextStream = contextStream.flatMap(new ContextFanOutForKeys()).keyBy((KeySelector<Tuple2<String, VehicleContext>, String>) value -> value.f0);
        KeyedStream<Tuple2<String, VehicleContext>, String> keyedContextStream = scalarStream.flatMap(new FlatMapper<>()).keyBy((KeySelector<Tuple2<String, VehicleContext>, String>) value -> value.f0);
        SingleOutputStreamOperator<SwitchingDecision> decisionMaker = keyedContextStream.connect(policyBroadcastStream).process(new SituationEvaluatorFromContext("app1")).setParallelism(1);
        //decisionMaker.addSink(new DecisionSink());
        BroadcastStream<SwitchingDecision> broadcastDecisions = decisionMaker.broadcast(Descriptors.decisionMapState);

        // Get forked data records from context stream
        DataStream<Tuple2<String, VehicleContext>> forkedVehicleContextRecords = decisionMaker.getSideOutput(SituationEvaluatorFromContext.contextOutputTag);


        DataStream<Tuple2<String, VehicleContext>> scalarSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("speed"));
        DataStream<Tuple2<String, VehicleContext>> processedSpeed = scalarSplit.connect(broadcastDecisions).process(new StatefulSerialPETEnforcementUpgrade<>("speed", ScalarData.class,true));
        processedSpeed.addSink(new DataLogger());

        //DataStream<Tuple2<String, VehicleContext>> locationSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("location"));
        //DataStream<Tuple2<String, VehicleContext>> processedLocation = locationSplit.connect(broadcastDecisions).process(new StatefulSerialPETEnforcement<>("location", LocationData.class));

        //DataStream<Tuple2<String, VehicleContext>> imageSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("image"));
        //DataStream<Tuple2<String, VehicleContext>> processedImage = PipelineUtil.getOrderedOutputStream(2, "image", ImageData.class, broadcastDecisions, imageSplit, env);
        //processedImage.addSink(new ImageSink(2));
        //processedImage.addSink(new DataLogger());
//        processedSpeed.addSink(new SinkFunction<>() {
//            @Override
//            public void invoke(Tuple2<String, VehicleContext> value, Context context) throws Exception {
//                if (value.f0.equals("speed")){
//                    ScalarData sd = value.f1.getScalarData();
//                    OutputTool.log(sd);
//                }
//            }
//        });

        env.execute("kafka streaming job");
    }

}
