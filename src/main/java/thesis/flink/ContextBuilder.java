package thesis.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

public class ContextBuilder extends CoProcessFunction<ScalarData, LocationData, Tuple2<ScalarData, LocationData>> {


    private ScalarData scalarData;
    private LocationData locationData;


    @Override
    public void processElement1(ScalarData value, CoProcessFunction<ScalarData, LocationData, Tuple2<ScalarData, LocationData>>.Context ctx, Collector<Tuple2<ScalarData, LocationData>> out) throws Exception {
        if (locationData != null) {
            out.collect(Tuple2.of(value, locationData));
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " Form context tuple <" + value + ", "+locationData+">");
        } else {
            scalarData = value;
        }
    }

    @Override
    public void processElement2(LocationData value, CoProcessFunction<ScalarData, LocationData, Tuple2<ScalarData, LocationData>>.Context ctx, Collector<Tuple2<ScalarData, LocationData>> out) throws Exception {
        if (scalarData != null) {
            out.collect(Tuple2.of(scalarData, value));
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks() +" Form context tuple <" + scalarData + ", "+value+">");
        } else {
            locationData = value;
        }
    }
}
