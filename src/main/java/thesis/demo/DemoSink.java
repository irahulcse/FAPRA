package thesis.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import thesis.context.VehicleContext;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Receives unprocessed and processed data and present them in a JFrame.
 * The values are received in the form of a {@link Tuple2}. The String  at the first position indicates the label in the frame.
 * The payload has a generalized class of {@link Object}, since not only the {@link VehicleContext} but also the Strings indicating the
 * applied PET will be shown in the frame.
 */
public class DemoSink extends RichSinkFunction<Tuple2<String, Object>> {

    private final DemoFrame demoFrame;
    private Map<String, Queue<ImageData>> imageChannelQueue = Map.of("orig_img", new ArrayDeque<>(), "p_img_1", new ArrayDeque<>());


    public DemoSink(DemoFrame demoFrame) {
        this.demoFrame = demoFrame;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        demoFrame.setVisible(true);
    }

    @Override
    public void invoke(Tuple2<String, Object> value, Context context) throws Exception {
        String labelName = value.f0;
        if (labelName.contains("img")) {
            if (!imageChannelQueue.containsKey(labelName)) {
                throw new IllegalArgumentException(labelName + " is not defined in the imageChannelQueue.");
            }
            ImageData imageData = ((VehicleContext) (value.f1)).getImageData();
            imageChannelQueue.get(labelName).add(imageData);
            handleImageQueue();
            return;
        }

        if (labelName.contains("speed")) {
            if (labelName.contains("orig")) {
                demoFrame.updateSpeed(labelName, ((VehicleContext) value.f1).getScalarData().getData().toString());
            } else {
                demoFrame.updateSpeed(labelName, ((VehicleContext) value.f1).getScalarData().getProcessedData().toString());
            }
            return;
        }
        if (labelName.contains("loc")) {
            if (labelName.contains("orig")) {
                demoFrame.updateLocation(labelName, ((VehicleContext) value.f1).getLocationData().originalDataToString());
            } else {
                demoFrame.updateLocation(labelName, ((VehicleContext) value.f1).getLocationData().processedDataToString());
            }
        }
    }

    /**
     * To ensure synchronized display of pictures of the source and applications in the {@link DemoFrame}.
     * @throws IOException
     * @throws InterruptedException
     */
    private void handleImageQueue() throws IOException, InterruptedException {
        for (Map.Entry<String, Queue<ImageData>> entry : imageChannelQueue.entrySet()) {
            if (entry.getValue().isEmpty()) return;
        }
        Map<String, ImageIcon> output = new HashMap<>();
        for (Map.Entry<String, Queue<ImageData>> entry : imageChannelQueue.entrySet()) {
            String label = entry.getKey();
            ImageIcon icon;
            if (label.contains("p_img")) {
                icon = toImageIcon(Objects.requireNonNull(entry.getValue().poll()).getProcessedData());
            } else {
                icon = toImageIcon(Objects.requireNonNull(entry.getValue().poll()).getData());
            }
            output.put(label, icon);
        }
        demoFrame.syncUpdateImage(output);

    }

    private ImageIcon toImageIcon(byte[] imageAsByteArray) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(imageAsByteArray);
        BufferedImage bi = ImageIO.read(bais);
        return new ImageIcon(bi);
    }
}
