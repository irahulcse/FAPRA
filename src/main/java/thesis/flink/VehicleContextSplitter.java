package thesis.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import thesis.context.VehicleContext;

public class VehicleContextSplitter extends ProcessFunction<Tuple2<String, VehicleContext>, Void> {

    public static OutputTag<VehicleContext> filteredByScalar = new OutputTag<>("filtered by scalar", TypeInformation.of(VehicleContext.class));
    public static OutputTag<VehicleContext> filteredByLocation = new OutputTag<>("filtered by location", TypeInformation.of(VehicleContext.class));
    public static OutputTag<VehicleContext> filteredByImage = new OutputTag<>("filtered by image", TypeInformation.of(VehicleContext.class));

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, ProcessFunction<Tuple2<String, VehicleContext>, Void>.Context ctx, Collector<Void> out) throws Exception {
        switch (value.f0) {
            case "speed" -> {
                ctx.output(filteredByScalar, value.f1);
            }
            case "location" -> {
                ctx.output(filteredByLocation, value.f1);
            }
            case "image" -> {
                ctx.output(filteredByImage, value.f1);
            }
            default -> {
                System.out.println("Unknown data section.");
            }
        }
    }
}
