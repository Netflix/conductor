package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.aurora.sql.ResultSetHandler;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.utils.PriorityLookup;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.PriorityLookupDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author Pradeep Palat
 */

public class AuroraPriorityLookupDAO extends AuroraBaseDAO implements PriorityLookupDAO {

	private ExecutionDAO executionDAO;
	private static Logger logger = LoggerFactory.getLogger(AuroraPriorityLookupDAO.class);

	@Inject
	public AuroraPriorityLookupDAO(DataSource dataSource, ObjectMapper mapper, ExecutionDAO executionDAO, Configuration config) {
		super(dataSource, mapper);
		this.executionDAO = executionDAO;
	}

	public ExecutionDAO getExecutionDAO() {
		return executionDAO;
	}

	public void setExecutionDAO(ExecutionDAO executionDAO) {
		this.executionDAO = executionDAO;
	}

	public static Logger getLogger() {
		return logger;
	}

	public static void setLogger(Logger logger) {
		AuroraPriorityLookupDAO.logger = logger;
	}

	@Override
	public ArrayList<PriorityLookup> getPriority(int priority) {
		AuroraPriorityLookupDAO.PriorityLookupHandler handler = new AuroraPriorityLookupDAO.PriorityLookupHandler();
		StringBuilder SQL = new StringBuilder("SELECT * FROM META_PRIORITY WHERE ? BETWEEN MIN_PRIORITY AND MAX_PRIORITY");

		return getWithTransaction( tx->
			 query( tx, SQL.toString(), q-> q.addParameter(priority).executeAndFetch(handler))
		);
	}

	@Override
	public ArrayList<PriorityLookup> getAllPriorities() {
		AuroraPriorityLookupDAO.PriorityLookupHandler handler = new AuroraPriorityLookupDAO.PriorityLookupHandler();
		StringBuilder SQL = new StringBuilder("SELECT * FROM META_PRIORITY ORDER BY MIN_PRIORITY, MAX_PRIORITY");

		return getWithTransaction( tx->
			 query( tx, SQL.toString(), q->q.executeAndFetch(handler))
		);
	}

	@Override
	public void addPriority(PriorityLookup priorityLookup) {
		//TODO - Will be updated
	}

	@Override
	public void updatePriority(PriorityLookup priorityLookup) {
		//TODO - Will be updated
	}


	class PriorityLookupHandler implements ResultSetHandler<ArrayList<PriorityLookup>> {

		public ArrayList<PriorityLookup> apply(ResultSet rs) throws SQLException {
			ArrayList<PriorityLookup> priorityLookupList = new ArrayList<PriorityLookup>();

			if( rs.next()){
				PriorityLookup priorityLookup = new PriorityLookup();
				priorityLookup.setId( rs.getInt("id"));
				priorityLookup.setMinPriority( rs.getInt("min_priority"));
				priorityLookup.setMaxPriority(rs.getInt("max_priority"));
				priorityLookup.setName(rs.getString("name"));
				priorityLookup.setValue(rs.getString("value"));
				priorityLookup.setCreatedOn( rs.getDate("created_on"));
				priorityLookup.setModifiedOn( rs.getDate("modified_on"));
				priorityLookupList.add(priorityLookup);
			}

			return priorityLookupList;
		}
	}
}
