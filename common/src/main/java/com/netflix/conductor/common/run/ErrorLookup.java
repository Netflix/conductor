package com.netflix.conductor.common.run;

/**
 * @author Pradeep Palat
 */

public class ErrorLookup {
    private int id;
    private String errorCode;
    private String lookup;
    private String workflowName;
    private String generalMessage;
    private String rootCause;
    private String resolution;
    private Boolean isRequiredInReporting;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLookup() {
        return lookup;
    }

    public void setLookup(String lookup) {
        this.lookup = lookup;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getGeneralMessage() {
        return generalMessage;
    }

    public void setGeneralMessage(String generalMessage) {
        this.generalMessage = generalMessage;
    }

    public String getRootCause() {
        return rootCause;
    }

    public void setRootCause(String rootCause) {
        this.rootCause = rootCause;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public Boolean getIsRequiredInReporting() {
        return isRequiredInReporting;
    }

    public void setIsRequiredInReporting(Boolean isRequiredInReporting) {
        this.isRequiredInReporting = isRequiredInReporting;
    }


}
