package thesis.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import thesis.common.Colors;
import thesis.context.VehicleContext;

public class PartialVehicleContextSink implements SinkFunction<Tuple2<String,VehicleContext>> {

    @Override
    public void invoke(Tuple2<String,VehicleContext> value, Context context) throws Exception {
        System.out.println(Colors.ANSI_PURPLE+"Sink: " +value.toString()+Colors.ANSI_RESET);
    }
}
