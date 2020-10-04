package com.netflix.conductor.dao.es6rest.dao;

import com.netflix.conductor.core.utils.PriorityLookup;
import com.netflix.conductor.dao.PriorityLookupDAO;

import java.util.ArrayList;

/**
 * @author Pradeep Palat
 */
public class ElasticSearch6PriorityLookupDAO implements PriorityLookupDAO {
    @Override
    public void updatePriority(PriorityLookup priorityLookup) {}

    @Override
    public ArrayList<PriorityLookup> getAllPriorities() { return null; }

    @Override
    public ArrayList<PriorityLookup> getPriority(int priority) { return null; }

    @Override
    public void addPriority(PriorityLookup priorityLookup) {}
}
