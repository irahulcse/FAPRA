package thesis.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import thesis.common.GlobalConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaTopicHelper {

    //TODO: integrate with Globalconfig and other Kafka settings in other parts of the framework.
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", GlobalConfig.BOOTSTRAP_SERVER);
        String switchingTopic1 = GlobalConfig.SWITCHING_TOPIC + "-" + "app1";
        String switchingTopic2 = GlobalConfig.SWITCHING_TOPIC + "-" + "app2";

        try (AdminClient adminClient = AdminClient.create(properties)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);

            ListTopicsResult topics = adminClient.listTopics(options);
            KafkaFuture<Set<String>> topicsNames = topics.names();
            Set<String> names = topicsNames.get();

            List<NewTopic> newTopics = new ArrayList<>();
            if (!names.contains(switchingTopic1)) {
                NewTopic newTopic = new NewTopic(switchingTopic1, 1, (short) 1);
                newTopics.add(newTopic);
            }
            if (!names.contains(switchingTopic2)) {
                NewTopic newTopic = new NewTopic(switchingTopic2, 1, (short) 1);
                newTopics.add(newTopic);
            }
            CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
            createTopicsResult.all().get();


        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
