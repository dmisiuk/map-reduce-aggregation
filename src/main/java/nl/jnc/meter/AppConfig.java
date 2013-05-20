package nl.jnc.meter;

public class AppConfig {

    private String dbName;
    private String relativeCollectionName;
    private String absoluteCollectionName;
    private long absoluteCalculatePeriodMills;
    private long clientSleepMills;
    private long numberOfRequest;


    public AppConfig(String dbName, String relativeColl, String absoluteColl, long numberOfRequest, long absoluteCalculatePeriodMills, long clientSleepMills) {
        this.dbName = dbName;
        this.relativeCollectionName = relativeColl;
        this.absoluteCollectionName = absoluteColl;
        this.numberOfRequest = numberOfRequest;
        this.absoluteCalculatePeriodMills = absoluteCalculatePeriodMills;
        this.clientSleepMills = clientSleepMills;
    }

    public String getDbName() {
        return dbName;
    }

    public long getNumberOfRequest() {
        return numberOfRequest;
    }

    public String getRelativeCollectionName() {
        return relativeCollectionName;
    }

    public String getAbsoluteCollectionName() {
        return this.absoluteCollectionName;
    }

    public long getAbsoluteCalculatePeriodMills() {
        return absoluteCalculatePeriodMills;
    }

    public long getClientSleepMills() {
        return clientSleepMills;
    }
}