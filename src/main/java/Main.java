import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


import java.net.InetAddress;
import java.util.Collection;
import java.util.Scanner;

//TO DO Set mapping and better think on logic
class ElasticConnector {
    PreBuiltTransportClient client = null;
    void Connect() {
        try
        {
            Settings settings = Settings.builder()
                    .put("cluster.name","docker-cluster")
                    .build();
            PreBuiltTransportClient cli = new PreBuiltTransportClient(settings);
            cli.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9300));
            this.client = cli;
        }
        catch (Exception e)
        {
            System.out.println("something wrong");
        }
    }
    void CloseConnection()
    {
        client.close();
    }
    void PutDocument(String Jsonka)
    {
        IndexRequest request = new IndexRequest("posts");
        //request.id("1");
        String jsonString = Jsonka;
        request.source(jsonString, XContentType.JSON);
        client.index(request);
    }

    Collection<Terms.Bucket> GetAggregation(String fieldname)
    {
        SearchRequestBuilder requestBuilder = client.prepareSearch("posts");
        requestBuilder.setSize(0);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("field")
                        .field(fieldname);
        requestBuilder.addAggregation(aggregation);
        SearchResponse response = requestBuilder.get();
        Terms terms = response.getAggregations().get("field");
        Collection<Terms.Bucket> buckets = (Collection<Terms.Bucket>) terms.getBuckets();
        return  buckets;

    }

    void GetAllData()
    {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("*").setQuery(query).get();
        System.out.println(response.getHits().getTotalHits());
    }
    protected void finalize()
    {
        if (client != null)
        {
            CloseConnection();
        }
        client = null;
    }
}

class ContentPrinter extends Thread
{

    public void run()
    {
        ElasticConnector conector = new ElasticConnector();
        conector.Connect();
        if (conector.client == null)
        {
            System.out.println("error with connection");
            return;
        }
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setPort(5672);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare("CONTENT_TEST", false, false, true, null);
            System.out.println(" Content Dispatcher is on");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String content = new String(delivery.getBody(), "UTF-8");
                try {
                    conector.PutDocument(content);
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            boolean autoAck = false;
            channel.basicConsume("CONTENT_TEST", autoAck, deliverCallback, consumerTag -> { });
        }
        catch (Exception e)
        {
            return;
        }

    }
}

class Statistics
{
    ElasticConnector connector =new ElasticConnector();;
    void ShowAuthorAgregation()
    {
        connector.Connect();
        Collection<Terms.Bucket>buckets =  connector.GetAggregation("Source");
        for (Terms.Bucket i : buckets)
        {
            System.out.println(i.getKey().toString() + " " +'('+ i.getDocCount() + ')');
        }
        connector.CloseConnection();
    }

}






public class Main {
    public static void main(String args []) throws InterruptedException {


        ContentPrinter t1 = new ContentPrinter();
        t1.run();

        while (true)
        {
            Scanner sc= new Scanner(System.in);
            System.out.print("Enter a string: ");
            String str= sc.nextLine();

            switch (str)
            {
                case "Stat":
                    Statistics stat = new Statistics();
                    try
                    {
                        stat.ShowAuthorAgregation();

                    }
                    catch (Exception e)
                    {
                        System.out.println(e.toString());
                        stat.connector.CloseConnection();
                        Thread.sleep(5000);
                        continue;
                    }
                    break;
                case "Stop":
                    t1.stop();
                    return;

            }


        }




    }
}
