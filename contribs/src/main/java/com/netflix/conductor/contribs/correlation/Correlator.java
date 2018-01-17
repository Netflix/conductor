package com.netflix.conductor.contribs.correlation;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.core.HttpContext;
import org.apache.http.client.methods.HttpGet;
import org.json.JSONObject;
import org.slf4j.Logger;
import sun.net.www.http.HttpClient;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by beimforz on 12/21/17.
 */
public class Correlator implements ICorrelator{

    final String headerKey = "Deluxe-Owf-Context";

    Logger logger;
    Context context;
    HttpContext request;

    public Correlator(Logger logger, Context context, HttpContext request){
        this.logger = logger;
        this.context = context;
        this.request = request;
    }

     public void init(HttpContext httpContext){
        this.request = httpContext;

        context = parseHeader(httpContext);

         JSONObject json = new JSONObject(context);
         logger.trace(json.toString());
     }

     public void addIdentifier(String urn){
         if(urn.equals(null)){
            return;
         }

         urn = urn.trim().toLowerCase();
         if(context == null){
            return;
         }
         if(context.getUrns().contains(urn)){
            return;
         }
         List<String> urns = context.getUrns();
         urns.add(urn);
         context.setUrns(urns);

         JSONObject json = new JSONObject(context);
         logger.trace(json.toString());
     }

     public void attach(HttpClient client){
         HttpGet get = new HttpGet(client.getProxyHostUsed());
         JSONObject json = new JSONObject(context);
         get.addHeader(headerKey, json.toString());

     }

     public Context parseHeader(HttpContext context){
         Context result = new Context();
         ArrayList<Context> contexts = new ArrayList<>();
         Set<String> keys = context.getRequest().getRequestHeaders().keySet();
         if(context.getRequest().getRequestHeader(headerKey) != null){
             for(String key: keys){
                 if(key.equals(headerKey)){
                     try {
                         ObjectMapper mapper = new ObjectMapper();
                         Context rawContext = mapper.readValue(context.getRequest().getRequestHeaders().get(key).toString(), Context.class);
                         contexts.add(rawContext);
                     } catch (JsonParseException e) {
                         e.printStackTrace();
                     }  catch (IOException e) {
                         e.printStackTrace();
                     }
                 }
             }
             result = merge(contexts);
         }

         List<String> newValues = result.getUrns().stream().distinct().collect(Collectors.toList());
         for(String value: newValues){
             newValues.remove(value);
             newValues.add(value.toLowerCase());
         }
         result.setUrns(newValues);
         return result;
     }

     public Context merge(ArrayList<Context> contexts){
         Context result = new Context();
         for(Context context: contexts){
             result.setSequenceno(Math.max(result.getSequenceno(), context.getSequenceno() + 1));
             List<String> mergedList = result.getUrns();
             mergedList.addAll(context.getUrns());
             result.setUrns(mergedList.stream().distinct().collect(Collectors.toList()));
         }
         return result;
     }

     public Context updateSequenceNo(){
         int sequenceNo = context.getSequenceno();
         sequenceNo++;
         context.setSequenceno(sequenceNo);
         return context;
     }

}
