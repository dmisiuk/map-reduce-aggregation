package nl.jnc.meter;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Date;

public class MeterData {

    public static final String TIME_STAMP_KEY = "ts";
    public static final String DEVICE_ID_KEY = "deviceId";
    public static final String DELTA_KEY = "delta";

    private DBObject dbObject;

    public MeterData() {
        this.dbObject = new BasicDBObject();
    }

    public MeterData(String deviceId, Date timeStamp, Double delta) {
        this();
        this.setDeviceId(deviceId);
        this.setTimeStamp(timeStamp);
        this.setDelta(delta);
    }

    public void setTimeStamp(Date timeStamp) {
        dbObject.put(TIME_STAMP_KEY, timeStamp);
    }

    public void setDeviceId(String deviceId) {
        dbObject.put(DEVICE_ID_KEY, deviceId);
    }

    public void setDelta(Double delta) {
        dbObject.put(DELTA_KEY, delta);
    }

    public Date getTimeStamp() {
        return (Date) dbObject.get(TIME_STAMP_KEY);
    }

    public String getDeviceId() {
        return (String) dbObject.get(DEVICE_ID_KEY);
    }

    public Double getDelta() {
        return (Double) dbObject.get(DELTA_KEY);
    }

    public DBObject getDBObject() {
        return this.dbObject;
    }
}