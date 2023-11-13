package thesis.common;

import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

import java.awt.geom.Point2D;
import java.util.Map;

/**
 * Stores the global constant variable, such as the name of the topic, of MongoDB collection, or the paths to python runtime, to pet-description, to policy description, etc...
 */
public class GlobalConfig {

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String INPUT_TOPIC = "test-topic";
    public static final String SCALAR_TOPIC= "scalar";
    public static final String LOCATION_TOPIC = "location";
    public static final String IMAGE_TOPIC = "image";

    public static final String PET_TOPIC = "pet";

    public static final String POLICY_TOPIC = "policy";
    public static final String SINK_TOPIC = "sink";
    public static final String SWITCHING_TOPIC = "switching-ready";

    public static final String POLICY_COLLECTION = POLICY_TOPIC;
    public static final String SPEED_COLLECTION = "speed";
    public static final String LOCATION_COLLECTION = LOCATION_TOPIC;
    public static final String IMAGE_COLLECTION = IMAGE_TOPIC;
    public static final String dbURL = "mongodb://localhost:27017";
    public static final String jsonPath = "/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/src/test/java/";
    public static final String csvOutputPath = "/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/result/";
    public static final String pythonRuntime = "/Users/zhudasch/opt/anaconda3/envs/AnalysisTool/bin/python";
    public static final String imagePETRuntime = "/Users/zhudasch/JavaPlayground/ImageAnonymizer/src/python/venv/bin/python";
    public static final String analyzerPath = "/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/python/main.py";
    public static final Map<String, Class<? extends Data<?>>> sectionRecordClassMap = Map.of("speed",ScalarData.class, "location", LocationData.class, "image", ImageData.class);
    public static final Map<Class<? extends Data<?>>, Class<?>> recordRawClassMap = Map.of(ScalarData.class, Double.class, LocationData.class, Point2D.class, ImageData.class, byte[].class);
    public static final String textInputSource = "/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/textInputSources";
}
