package com.netflix.conductor.dao.es6rest.dao;

import com.netflix.conductor.common.run.ErrorLookup;
import com.netflix.conductor.dao.ErrorLookupDAO;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pradeep Palat
 */

public class Elasticsearch6ErrorLookupDAO implements ErrorLookupDAO {

    List<ErrorLookup> emptyList = new ArrayList<>();

    @Override
    public List<ErrorLookup> getErrors() {
        return emptyList;
    }

    @Override
    public boolean addError(ErrorLookup errorLookup) {
        return false;
    }

    @Override
    public boolean updateError(ErrorLookup errorLookup) {
        return false;
    }

    @Override
    public List<ErrorLookup> getErrorMatching(String errorString) {
        return emptyList;
    }

    @Override
    public List<ErrorLookup> getErrorMatching(String workflow, String errorString) { return emptyList; }

    @Override
    public List<ErrorLookup> getErrorByCode(String errorCode) { return emptyList; }
}
