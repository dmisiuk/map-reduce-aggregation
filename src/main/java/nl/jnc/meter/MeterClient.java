package nl.jnc.meter;

import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.Date;

public class MeterClient implements Runnable {

    private static Logger logger = Logger.getLogger(MeterClient.class);

    private DBCollection collection;
    private String deviceId;
    private AppConfig appConfig;

    public MeterClient(AppConfig config) throws UnknownHostException {
        this.appConfig = config;
        Mongo mongo = new MongoClient();
        this.collection = mongo.getDB(config.getDbName()).getCollection(config.getRelativeCollectionName());
        this.deviceId = new ObjectId().toString();
    }

    @Override
    public void run() {
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        String msg = "client with device id=%s started";
        logger.debug(String.format(msg, deviceId));
        for (int i = 0; i < this.appConfig.getNumberOfRequest(); i++) {
            long startTime = System.nanoTime();
            this.insert();
            long time = System.nanoTime() - startTime;
            if (time > max) {
                max = time;
            }
            if (time < min) {
                min = time;
            }
            try {
                Thread.sleep(appConfig.getClientSleepMills());
            } catch (InterruptedException e) {
                logger.error(e);
            }

        }
        msg = "client with device id=%s stopped. Max insert time=%s nano seconds. Min insert time=%s nano seconds";
        logger.debug(String.format(msg, deviceId, max, min));

    }

    public void insert() {
        Double delta = this.getRandomDelta();
        Date timeStamp = new Date();
        MeterData meterData = new MeterData(deviceId, timeStamp, delta);
        collection.insert(meterData.getDBObject(), WriteConcern.SAFE);
    }

    private Double getRandomDelta() {
        return Math.random();
    }
}