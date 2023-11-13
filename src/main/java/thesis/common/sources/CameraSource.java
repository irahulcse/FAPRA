package thesis.common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import thesis.context.data.ImageData;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

public class CameraSource implements SourceFunction<ImageData> {
    private boolean isRunning = true;
    private final int recordNumber = 1105;
    private static final String path = "/Users/zhudasch/JavaPlayground/ImageAnonymizer/pic/sequence/";

    private final int frequency;

    public CameraSource(int frequency){
        this.frequency = frequency;
    }

    @Override
    public void run(SourceContext<ImageData> ctx) throws Exception {
        Thread.sleep(1000L);
        int i = 0;
        while (isRunning && i < 341) {

            File file = new File(path + i + ".png");
            ImageData data = new ImageData(Files.readAllBytes(file.toPath()));
            data.setSequenceNumber(String.valueOf(i));
            ctx.collect(data);
            //System.out.println("Image source: sent image with serial number " + i);
            i++;
            Thread.sleep(1000/frequency);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
