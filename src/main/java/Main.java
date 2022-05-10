import com.google.common.hash.Hashing;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Scanner;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


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

    public String getSHA(String input) throws NoSuchAlgorithmException
    {
        return Hashing.sha256()
                .hashString(input, StandardCharsets.UTF_8)
                .toString();
    }

    String CalcContentHash(String Jsonka) throws NoSuchAlgorithmException {
        JsonElement jelement = new JsonParser().parse(Jsonka);
        JsonObject jobject = jelement.getAsJsonObject();
        System.out.println(jobject.get("Content").toString());
        System.out.println(getSHA(jobject.get("Content").toString()));
        return getSHA(jobject.get("Content").toString());
    }

    void PutDocument(String Jsonka)
    {
        IndexRequest request = new IndexRequest("posts");

        try {
            request.id(CalcContentHash(Jsonka));

        }
        catch (Exception e )
        {
            System.out.println("error getting hash");
        }
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

    public boolean IndexExists(String index) {
        return client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();

    }

    void SetMapping(String index, String jsonka) {
                ///TODo make function for mapping from json or other way
    }

    SearchResponse GetNeededNews(String index,String field,String phrase)
    {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery(field,phrase));
        searchRequest.source(sourceBuilder);
        SearchResponse resp = client.search(searchRequest).actionGet();
        return resp;
    }
    void CreateIndex(String index)
    {
        try
        {
            if(IndexExists(index))
            {
                return;
            }
            else {
                client.admin().indices().create(new CreateIndexRequest(index)).actionGet();
            }
        }
        catch (Exception e )
        {
            System.out.println("Error creating index - "+e.toString());
        }
    }

    void SetMapping(String index)  {

        try
        {
            XContentBuilder mapping = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("Source")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject();
            AcknowledgedResponse resp = client.admin().indices()
                    .preparePutMapping(index)
                    .setSource(mapping)
                    .setType("_doc")
                    .execute().actionGet();
        }
        catch (Exception e )
        {
            System.out.println("Mapping error - "+e.toString());
        }

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
        conector.CreateIndex("posts");
        conector.SetMapping("posts");
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
                    System.out.println("Appended");
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

        ElasticConnector connector =new ElasticConnector();
        connector.Connect();



        ContentPrinter t1 = new ContentPrinter();
        ContentPrinter t2 = new ContentPrinter();
        t1.run();
        t2.run();
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
                    t2.stop();
                    return;

            }


        }




    }
}
