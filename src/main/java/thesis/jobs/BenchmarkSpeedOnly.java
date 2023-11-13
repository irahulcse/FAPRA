package thesis.jobs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import thesis.common.GlobalConfig;
import thesis.common.sources.PolicyCreator;
import thesis.common.sources.SpeedSourceEvalOne;
import thesis.context.VehicleContext;
import thesis.context.data.ScalarData;
import thesis.demo.FlatMapper;
import thesis.demo.Selector;
import thesis.demo.SimpleStatefulSerialPETEnforcement;
import thesis.flink.Descriptors;
import thesis.flink.FilterByDataSection;
import thesis.flink.SituationEvaluatorFromContext;
import thesis.flink.SwitchingDecision;
import thesis.policy.Policy;
import thesis.util.MemoryUsage;
import thesis.util.OutputTool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;

public class BenchmarkSpeedOnly {

    private static final String petPath1 = GlobalConfig.textInputSource + "/petDescriptions/noPET.json";
    private static final String petPath2 = GlobalConfig.textInputSource + "/petDescriptions/speedPETAverage.json";

    private static final Timer timer = new Timer();
    // public static DBWrapper dbWrapper = new DBWrapper();
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

        // startMemoryLogger();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ScalarData> scalarStream = env.addSource(new SpeedSourceEvalOne("speed",2, 100));
        //DataStream<ScalarData> scalarStream = env.fromElements(new ScalarData("speed",0.0));
        BroadcastStream<Policy> policyBroadcastStream = env.fromElements(PolicyCreator.generatePolicy("statefulSpeedPolicy")).broadcast(Descriptors.policyStateDescriptor);

        DataStream<Tuple2<String, VehicleContext>> contextOnlyScalar = scalarStream.flatMap(new FlatMapper<>());

        SingleOutputStreamOperator<SwitchingDecision> decisionStream = contextOnlyScalar.keyBy((KeySelector<Tuple2<String, VehicleContext>, String>) value -> value.f0).
                connect(policyBroadcastStream).process(new SituationEvaluatorFromContext("app1"));

        DataStream<Tuple2<String, VehicleContext>> forkedVehicleContextRecords = decisionStream.getSideOutput(SituationEvaluatorFromContext.contextOutputTag);
        // Split. Otherwise, error by pushing to operator when emitting from side output.
        DataStream<Tuple2<String, VehicleContext>> scalarSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("speed"));
        //
        DataStream<Tuple2<String, VehicleContext>> processedNonePET = scalarSplit.process(new SimpleStatefulSerialPETEnforcement<>(ScalarData.class, readPETDescription(petPath1)));
        DataStream<Tuple2<String, VehicleContext>> processedPET = scalarSplit.process(new SimpleStatefulSerialPETEnforcement<>(ScalarData.class, readPETDescription(petPath2)));


        DataStream<Tuple2<String, VehicleContext>> union = processedNonePET.union(processedPET);
        DataStream<ScalarData> processedResult = union.connect(decisionStream).process(new Selector<>("speed", 2, ScalarData.class), TypeInformation.of(ScalarData.class));
/*        processedResult.addSink(new SinkFunction<>() {
            @Override
            public void invoke(ScalarData value, Context context) throws Exception {
                System.out.println("Sink: " + value);
            }
        });*/
        processedResult.addSink(new SinkFunction<>() {
            @Override
            public void invoke(ScalarData value, Context context) throws Exception {
                OutputTool.log(value, "");
            }
        });
        env.execute();

    }

    public static String readPETDescription(String path) throws IOException {
        try {
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException e) {
            throw new IOException(e.getCause());
        }
    }

    private static void startMemoryLogger() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long freeMem = Runtime.getRuntime().freeMemory();
                long maxMem = Runtime.getRuntime().maxMemory();
                long totalMem = Runtime.getRuntime().totalMemory();
                try {
                    OutputTool.log(new MemoryUsage(System.currentTimeMillis(), maxMem, totalMem, freeMem), "");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, 1000);
    }


}
