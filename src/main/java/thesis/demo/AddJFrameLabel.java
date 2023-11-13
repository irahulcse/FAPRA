package thesis.demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.pet.PETDescriptor;

/**
 * An operator that adds the JFrame label to the data payload.
 * The name of the labels are defined in the configuration of the {@link DemoFrame}. Its downstream operator is the {@link DemoSink}.
 * @param <T>
 */
public class AddJFrameLabel<T> extends RichMapFunction<T, Tuple2<String, Object>> {

    private final String labelName;

    public AddJFrameLabel(String labelName) {
        this.labelName = labelName;
    }

    @Override
    public Tuple2<String, Object> map(T data) throws Exception {
        if (data instanceof Data<?>) {
            VehicleContext vc = new VehicleContext();
            vc.update((Data<?>) data);
            return Tuple2.of(labelName, vc);
        }
        if (data instanceof Tuple2) {
            Tuple2<String, VehicleContext> tuple = (Tuple2<String, VehicleContext>) data;

            return Tuple2.of(labelName, tuple.f1);
        }
        if (data instanceof String) {
            PETDescriptor descriptor = new PETDescriptor((String) data);
            return Tuple2.of(labelName, descriptor.getName());
        }
        throw new IllegalArgumentException("Unknown class of object: " + data.getClass().getName());
    }
}
