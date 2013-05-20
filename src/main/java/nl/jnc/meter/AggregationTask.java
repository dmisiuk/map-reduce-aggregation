package nl.jnc.meter;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

// mongo query for aggregation
// ...aggregate({$group: {"_id": "$deviceId", absoluteValue: {$sum: "$delta"}}})
public class AggregationTask implements Runnable {

    private static Logger logger = Logger.getLogger(AggregationTask.class);
    private final String ABSOLUTE_VALUE_KEY = "absoluteValue";

    private AppConfig appConfig;
    private DBCollection relativeCollection, absoluteCollection;
    private DBObject groupOp, projectOp;

    public AggregationTask(AppConfig appConfig) throws UnknownHostException {
        this.appConfig = appConfig;
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(appConfig.getDbName());
        this.relativeCollection = db.getCollection(appConfig.getRelativeCollectionName());
        this.absoluteCollection = db.getCollection(appConfig.getAbsoluteCollectionName());
        this.groupOp = new BasicDBObject("$group",
                new BasicDBObject("_id", "$" + MeterData.DEVICE_ID_KEY)
                        .append(ABSOLUTE_VALUE_KEY, new BasicDBObject("$sum", "$" + MeterData.DELTA_KEY))
        );
        this.projectOp = new BasicDBObject("$project",
                new BasicDBObject(ABSOLUTE_VALUE_KEY, 1)
                        .append(MeterData.DEVICE_ID_KEY, "$_id")
                        .append("_id", 0));
    }

    @Override
    public void run() {
        double oneBillion = 1000000000d;
        try {
            while (true) {
                Thread.sleep(appConfig.getAbsoluteCalculatePeriodMills());
                long numberOfInputRecords = this.relativeCollection.count();
                logger.debug("start aggregation...");
                long startTime = System.nanoTime();
                AggregationOutput result = this.aggregate();

                long endAggregateTime = System.nanoTime();

                int numberOfOutputRecords = writeResult(result);
                long endInsertTime = System.nanoTime();
                double aggregateTime = (endAggregateTime - startTime) / oneBillion;
                double insertTime = (endInsertTime - endAggregateTime) / oneBillion;
                double allTime = (endInsertTime - startTime) / oneBillion;
                logger.debug("aggregation time = " + aggregateTime + " seconds. Input records ~" + numberOfInputRecords);
                logger.debug("insert time = " + insertTime + " seconds. OutputRecords = " + numberOfOutputRecords);
                logger.debug("common time = " + allTime + " seconds");
            }
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    private AggregationOutput aggregate() {
        return this.relativeCollection.aggregate(groupOp);
    }

    private int writeResult(AggregationOutput result) {
        List<DBObject> dbObjectList = new ArrayList<DBObject>();
        this.absoluteCollection.drop();
        for (DBObject dbo : result.results()) {
            dbObjectList.add(dbo);
            //this.absoluteCollection.save(dbo, WriteConcern.SAFE);
        }
        this.absoluteCollection.insert(dbObjectList, WriteConcern.SAFE);
        return dbObjectList.size();
    }
}