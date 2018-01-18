package com.netflix.conductor.contribs.correlation;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by beimforz on 12/21/17.
 */
public class Context {
    private String version;

    @JsonProperty("sequence-no")
    private int sequenceno;

    private List<String> urns;

    public Context(){
        this.version = "v1";
        this.sequenceno = 0;
        this.urns = new ArrayList<>();
    }

	public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getSequenceno() {
        return sequenceno;
    }

    public void setSequenceno(int sequenceno) {
        this.sequenceno = sequenceno;
    }

    public List<String> getUrns() {
        return urns;
    }

    public void setUrns(List<String> urns) {
        this.urns = urns;
    }
}
