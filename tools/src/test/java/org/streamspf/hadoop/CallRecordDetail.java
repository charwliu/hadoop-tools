package org.streamspf.hadoop;

import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkeyPart;
import org.streamspf.hadoop.hbase.impl.ContextNameCreator;

@HBaseTable(name = "cdr", nameCreator = ContextNameCreator.class, autoCreate = true)
public class CallRecordDetail {

    @HRowkeyPart
    private long timestamp;

    @HRowkeyPart
    private long desc;

    @HColumn(key = "a")
    private int seconds;

    @HColumn(key = "b")
    private boolean calling;

    @HColumn(key = "c")
    private long calleeNumber;

    @HColumn(key = "d")
    private String location;

    @HColumn(key = "e")
    private String callType;

    @HColumn(key = "f")
    private int callFee;

    @HColumn(key = "g")
    private int otherFee;

    @HColumn(key = "h")
    private int subtotal;

    public int getSeconds() {
        return this.seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    public boolean isCalling() {
        return this.calling;
    }

    public void setCalling(boolean calling) {
        this.calling = calling;
    }

    public long getCalleeNumber() {
        return this.calleeNumber;
    }

    public void setCalleeNumber(long calleeNumber) {
        this.calleeNumber = calleeNumber;
    }

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getCallType() {
        return this.callType;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public int getCallFee() {
        return this.callFee;
    }

    public void setCallFee(int callFee) {
        this.callFee = callFee;
    }

    public int getOtherFee() {
        return this.otherFee;
    }

    public void setOtherFee(int otherFee) {
        this.otherFee = otherFee;
    }

    public int getSubtotal() {
        return this.subtotal;
    }

    public void setSubtotal(int subtotal) {
        this.subtotal = subtotal;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getDesc() {
        return this.desc;
    }

    public void setDesc(long desc) {
        this.desc = desc;
    }
}
