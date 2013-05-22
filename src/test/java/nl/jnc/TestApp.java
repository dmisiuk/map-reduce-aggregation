package nl.jnc;

import nl.jnc.meter.AggregationTask;
import nl.jnc.meter.AppConfig;
import nl.jnc.meter.MapReduceTask;
import nl.jnc.meter.MeterClient;
import org.apache.log4j.Logger;

import java.net.UnknownHostException;

public class TestApp {

    private static Logger logger = Logger.getLogger(TestApp.class);
    private static int numberOfClients = 2000;
    private static int numberOfRequests = 2000;
    private static String dbName = "electricmeter";
    private static String relativeCollectionName = "relativeIndications";
    private static String absoluteCollectionName = "absoluteIndications";
    private static long absoluteCalculatePeriod = 15000l;
    private static long clientSleepMills = 50l;
    private static AppConfig appConfig;

    static {
        appConfig = new AppConfig(dbName,
                relativeCollectionName,
                absoluteCollectionName,
                numberOfRequests,
                absoluteCalculatePeriod,
                clientSleepMills
        );
    }

    public static AppConfig getAppConfig() {
        return appConfig;
    }

    public static void main(String[] args) throws UnknownHostException {
        TestApp testApp = new TestApp();
        testApp.test();
    }

    private void startMeterClients() throws UnknownHostException {
        for (int i = 0; i < numberOfClients; i++) {
            MeterClient meterClient = new MeterClient(appConfig);
            new Thread(meterClient).start();
        }
    }

    private void startAggregationTask() throws UnknownHostException {
        AggregationTask aggregationTask = new AggregationTask(appConfig);
        new Thread(aggregationTask).start();
    }

    private void startMapReduceTask() throws UnknownHostException {
        MapReduceTask mapReduceTask = new MapReduceTask(appConfig);
        new Thread(mapReduceTask).start();
    }

    public void test() throws UnknownHostException {
        logger.debug("starting test...");
        this.startMeterClients();
        this.startAggregationTask();
        //this.startMapReduceTask();
    }
}