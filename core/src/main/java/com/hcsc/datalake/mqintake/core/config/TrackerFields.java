package com.hcsc.datalake.mqintake.core.config;

/**
 * Fields written into tracker message headers.
 * These become properties on the outbound tracker message.
 */
public class TrackerFields {

    private String reportingSystem;
    private String sourceSystem;
    private String messageStatus;
    private String destinationStatus;

    public TrackerFields() {
    }

    public TrackerFields(String reportingSystem, String sourceSystem,
                         String messageStatus, String destinationStatus) {
        this.reportingSystem = reportingSystem;
        this.sourceSystem = sourceSystem;
        this.messageStatus = messageStatus;
        this.destinationStatus = destinationStatus;
    }

    public String getReportingSystem() {
        return reportingSystem;
    }

    public void setReportingSystem(String reportingSystem) {
        this.reportingSystem = reportingSystem;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getMessageStatus() {
        return messageStatus;
    }

    public void setMessageStatus(String messageStatus) {
        this.messageStatus = messageStatus;
    }

    public String getDestinationStatus() {
        return destinationStatus;
    }

    public void setDestinationStatus(String destinationStatus) {
        this.destinationStatus = destinationStatus;
    }
}
