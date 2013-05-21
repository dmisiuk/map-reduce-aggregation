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
    private DBCollection relativeCollection, tempCollection;
    private String absoluteCollectionName;
    private DBObject groupOp;

    public AggregationTask(AppConfig appConfig) throws UnknownHostException {
        this.appConfig = appConfig;
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(appConfig.getDbName());
        this.relativeCollection = db.getCollection(appConfig.getRelativeCollectionName());
        this.absoluteCollectionName = appConfig.getAbsoluteCollectionName();
        this.tempCollection = db.getCollection("tempAbsolute");
        this.groupOp = new BasicDBObject("$group",
                new BasicDBObject("_id", "$" + MeterData.DEVICE_ID_KEY)
                        .append(ABSOLUTE_VALUE_KEY, new BasicDBObject("$sum", "$" + MeterData.DELTA_KEY))
        );
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
                logger.debug("insert time = " + insertTime + " seconds. Output records = " + numberOfOutputRecords);
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
        this.tempCollection.drop();
        for (DBObject dbo : result.results()) {
            dbObjectList.add(dbo);
            //this.absoluteCollection.save(dbo, WriteConcern.SAFE);
        }
        this.tempCollection.insert(dbObjectList, WriteConcern.SAFE);
        this.tempCollection.rename(this.absoluteCollectionName, true);
        return dbObjectList.size();
    }
}