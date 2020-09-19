import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.time.Instant;
import java.util.*;

public class FilteringStrings {

    private static final String[] status = new String[]{"PLACED", "COMPLETED", "SHIPPED"};
    private static final Random rand = new Random();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "test");

        DataStream<String> dataStream = env
                .addSource(new FlinkKafkaConsumer<String>("my-topic", new SimpleStringSchema(), properties));

        dataStream.print();

        List<HttpHost> httpHosts = new ArrayList<HttpHost>();

        httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<String>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<String, String>();
                        json.put("ORDER_ID", UUID.randomUUID().toString());
                        json.put("STATUS", status[rand.nextInt(status.length)]);
                        json.put("NUMBER", element);
                        json.put("UPDATED_TIMESTAMP", Long.toString(Instant.now().toEpochMilli()));

                        return Requests.indexRequest()
                                .index("my-index")
                                .source(json);
                    }

                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);

        dataStream.addSink(esSinkBuilder.build());

        env.execute("FilterStrings Strings");
    }
}
