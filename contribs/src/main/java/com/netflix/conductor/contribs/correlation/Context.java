package com.netflix.conductor.contribs.correlation;

import com.sun.jersey.api.core.*;

import java.util.ArrayList;
import java.util.List;
/**
 * Created by beimforz on 12/21/17.
 */
public class Context {

    private String version;
    private int sequenceno;
    private List<String> urns;

    public Context(){
        this.version = "v1";
        this.sequenceno = 1;
        this.urns = new ArrayList<>();
    }

    public String print(HttpContext context){

        String builder = "";
        builder += "correlation-request-id=" + context.getSession().getId();
        builder += "correlation-version=" + version;
        builder += "correlation-sequence=" + sequenceno;
        builder += "correlation-urns=\"";

        for(String urn: getUrns()){
            builder += urn + " ";
        }

        builder += "\"";

        return builder;
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
