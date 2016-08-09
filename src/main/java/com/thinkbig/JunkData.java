package com.thinkbig;

import java.text.ParseException;

/**
 * Created by rs186089 on 8/1/16.
 */
public class JunkData {
    private String endDate;
    private String eventCode;
    private String serial;
    private String procId;
    private String product;


    /**
     * Create an instance of this object utilizing another instance as the template
     *
     * @param source The source template object
     */
    public JunkData(final JunkData source) {
        this.endDate = source.getEndDate();
        this.eventCode = source.getEventCode();
        this.serial = source.getSerial();
        this.procId = source.getProcId();
        this.product = source.getProduct();
    }

    public JunkData(String endDate, String eventCode, String serial, String procId, String product) {
        this.endDate = endDate;
        this.eventCode = eventCode;
        this.serial = serial;
        this.procId = procId;
        this.product = product;
    }

    /**
     * Create an instance of the object based on comma delimited values contained in the given string
     *
     * @param key The string containing the CSV
     * @throws ParseException Thrown if the given CSV string does not contain valid data
     */
    public JunkData(String key) throws ParseException {

        String[] fields = key.split(",");
        if (fields.length < 5) {
            throw new ParseException("Tried to create a venusq history object from a string with only " + fields.length + " fields",
                    fields.length);
        }

        this.endDate = fields[0];
        this.eventCode = fields[1];
        this.serial = fields[2];
        this.procId = fields[3];
        this.product = fields[4];
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(final String endDate) {
        this.endDate = endDate;
    }

    public String getEventCode() {
        return eventCode;
    }

    public String getSerial() {
        return serial;
    }

    public String getProcId() { return procId; }

    public String getProduct() { return product; }

    /**
     * Does a quick validity check on the object to make sure all fields are of a valid format.
     *
     * @return true if the object is valie
     */
    public boolean isValid() {

        return true;
    }

    /**
     * Convert the object into a string that is a comma delimited version of the data.  This string can then be
     * passed into the constructor to reconstitute the data in a new instance
     *
     * @return A comma delimited version of the object data
     */
    public String toString() {
        return endDate + "," + eventCode + "," + serial;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JunkData that = (JunkData) o;

        if (endDate != null ? !endDate.equals(that.endDate) : that.endDate != null) return false;
        if (eventCode != null ? !eventCode.equals(that.eventCode) : that.eventCode != null) return false;
        if (serial != null ? !serial.equals(that.serial) : that.serial != null) return false;
        if (procId != null ? !procId.equals(that.procId) : that.procId != null) return false;
        if (product != null ? !product.equals(that.product) : that.product != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = endDate != null ? endDate.hashCode() : 0;
        result = 31 * result + (eventCode != null ? eventCode.hashCode() : 0);
        result = 31 * result + (serial != null ? serial.hashCode() : 0);
        result = 31 * result + (procId != null ? procId.hashCode() : 0);
        result = 31 * result + (product != null ? product.hashCode() : 0);
        return result;
    }
}