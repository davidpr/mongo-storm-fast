package org.mongodb;
/**
 * Created by IntelliJ IDEA.
 * User: David Prat Robles
 * Date: March 15, 2014
 */
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.rapportive.storm.amqp.QueueDeclaration;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.scheme.SimpleJSONScheme;
import com.rapportive.storm.spout.AMQPSpout;
import org.mongodb.bolt.MongoInsertBolt;
import java.util.Date;

public class MongoTopology{
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String url = "mongodb://127.0.0.1:27017/regression";

        QueueDeclaration qd = new SharedQueueWithBinding("stormqueue", "stormexchange", "stormkey");
        Scheme scheme = new SimpleJSONScheme();

        builder.setSpout( "spout", new AMQPSpout("127.0.0.1", 5672, "guest", "guest", "/", qd, scheme));
        ////////////////////////////////////////////////////////////////
        //////////////////////////////BOLT DB///////////////////////////
        ////////////////////////////////////////////////////////////////
        // Mongo insert bolt instance
        String collectionNameOutput = "diabetesoutput";
        MongoInsertBolt mongoInserBolt = null;
        StormMongoObjectGrabber mapper3 = new StormMongoObjectGrabber() {//field mapper
            @Override
            public DBObject map(DBObject object, Tuple tuple) {

                Object mvalue= tuple.getValueByField("message") ;
                String svalue=mvalue.toString();
                System.out.println("svalue is: " + svalue + "\n");

                JsonParser parser = new JsonParser();
                JsonObject obj = (JsonObject)parser.parse(svalue);

                return BasicDBObjectBuilder.start()
                        .add("X", obj.get("X").getAsDouble() )
                        .add("Y", obj.get("Y").getAsDouble() )
                        .add("V", obj.get("V").getAsDouble() )
                        .add("timestamp", new Date())
                        .get(); }

        };
        WriteConcern writeConcern=new WriteConcern();
        // Create a mongo bolt
        mongoInserBolt = new MongoInsertBolt(url, collectionNameOutput, mapper3, writeConcern.NONE);
        // Add it to the builder accepting content from the sum bolt/spout
        builder.setBolt("mongo", mongoInserBolt, 1).allGrouping("spout");
        /////////////////////////////////////////////////////////////////////////////////
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(5000);

        try{StormSubmitter.submitTopology( args[0], conf, builder.createTopology() );}
        catch(AlreadyAliveException e){}
    }
}
