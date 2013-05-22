package nl.jnc.meter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;

// mongo query for aggregation

/**
 * var mapFunc = function () { emit(this.deviceId, {ts: this.ts, value: this.delta});}
 * <p/>
 * var reduceFunc =  function (deviceId, values){ var sum = 0; var maxDate; values.forEach(function(v){ sum += v.value; maxDate = maxDate? Math.max(maxDate, v.ts):v.ts;}); var d = new ISODate(); d.setTime(maxDate);var r= {ts: d, value: sum}; return r;}
 * <p/>
 * <p/>
 * db.relativeIndications.mapReduce(mapFunc, reduceFunc, {out: "absolute_map_reduce"});
 */
public class MapReduceTask implements Runnable {

    private static Logger logger = Logger.getLogger(MapReduceTask.class);

    private AppConfig appConfig;
    private DBCollection relativeCollection;
    private String absoluteCollectionName = "absolute_map_reduce";
    private String mapFunc, reduceFunc;

    public MapReduceTask(AppConfig appConfig) throws UnknownHostException {
        this.appConfig = appConfig;
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(appConfig.getDbName());
        this.relativeCollection = db.getCollection(appConfig.getRelativeCollectionName());
        this.mapFunc = "function () { emit(this.deviceId, {ts: this.ts, value: this.delta});}";
        this.reduceFunc = "function (deviceId, values){" +
                " var sum = 0; var maxDate;" +
                " values.forEach(function(v){" +
                " sum += v.value; maxDate = maxDate? Math.max(maxDate, v.ts):v.ts;" +
                "});" +
                " var d = new ISODate();" +
                " d.setTime(maxDate);" +
                "var r= {ts: d, value: sum};" +
                " return r;" +
                "}";

    }

    @Override
    public void run() {
        double oneBillion = 1000000000d;
        try {
            while (true) {
                Thread.sleep(appConfig.getAbsoluteCalculatePeriodMills());
                logger.debug("start Map-Reduce...");
                ObjectId breakPointOid = new ObjectId();
                DBObject ltQuery = new BasicDBObject("_id", new BasicDBObject("$lt", breakPointOid));
                MapReduceCommand mpCommand = new MapReduceCommand(
                        relativeCollection,
                        mapFunc,
                        reduceFunc,
                        absoluteCollectionName,
                        MapReduceCommand.OutputType.REPLACE,
                        ltQuery
                );
                long startTime = System.nanoTime();
                MapReduceOutput mapReduceOutput = relativeCollection.mapReduce(mpCommand);
                long endMapReduceTime = System.nanoTime();
                double mapReduceTime = (endMapReduceTime - startTime) / oneBillion;
                logger.debug("Map-Reduce time = " + mapReduceTime + " seconds");
                DBObject counts = (DBObject) mapReduceOutput.getCommandResult().get("counts");
                logger.debug("Input records = " + counts.get("input"));
                logger.debug("Output records = " + counts.get("output"));
            }
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }
}