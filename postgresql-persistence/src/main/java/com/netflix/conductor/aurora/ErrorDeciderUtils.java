package com.netflix.conductor.aurora;

import java.util.Arrays;
import java.util.List;

public class ErrorDeciderUtils {

    private ErrorDeciderUtils(){}

    public static boolean isKnownError(String workflow, String meta){
        List<String> workflowWords = Arrays.asList(workflow.split(" "));
        List<String> metaWords = Arrays.asList(meta.split(" "));
        int commonItemsCountMeta = Math.toIntExact(workflowWords.stream().filter(metaWords::contains).count());

        return commonItemsCountMeta > 0 || commonItemsCountMeta == metaWords.size()
                || commonItemsCountMeta >= commonItemsCountMeta / 2;
    }

    public static boolean isUnknownError(String workflow,  String log){
        List<String> logWords = Arrays.asList(log.split(" "));
        List<String> workflowWords = Arrays.asList(workflow.split(" "));
        int commonItemsCountLogs = Math.toIntExact(workflowWords.stream().filter(logWords::contains).count());

        return commonItemsCountLogs > 0 && commonItemsCountLogs == logWords.size()
                && commonItemsCountLogs >= workflowWords.size() / 2;
    }
}
