package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.netflix.conductor.aurora.sql.ResultSetHandler;
import com.netflix.conductor.common.run.ErrorLookup;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ErrorLookupDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pradeep Palat
 */

public class AuroraErrorLookupDAO extends AuroraBaseDAO implements ErrorLookupDAO {

	private ExecutionDAO executionDAO;
	private static Logger logger = LoggerFactory.getLogger(AuroraErrorLookupDAO.class);

	private final String INSERT_SQL = "INSERT INTO meta_error_registry (error_code, lookup, workflow_name, general_message, root_cause, resolution) values (?,?,?,?,?,?)";
	private final String UPDATE_SQL = "UPDATE meta_error_registry SET error_code = ?, lookup = ?, workflow_name = ?, general_message = ?, root_cause = ?, resolution = ? WHERE id = ?";

	@Inject
	public AuroraErrorLookupDAO(DataSource dataSource, ObjectMapper mapper, ExecutionDAO executionDAO, Configuration config) {
		super(dataSource, mapper);
		this.executionDAO = executionDAO;
	}

	@Override
	public List<ErrorLookup> getErrors() {

		return getWithTransaction( tx->{
            ErrorLookupHandler handler = new ErrorLookupHandler();

			String SQL = "SELECT * FROM meta_error_registry ORDER BY id";
			return query( tx, SQL, q->q.executeAndFetch(handler));
		});
	}

	@Override
	public List<ErrorLookup> getErrorMatching(String workflow, String errorString) {
		return getWithTransaction( tx->{
            ErrorLookupHandler handler = new ErrorLookupHandler();
			logger.debug("Lookup error details for Workflow: " + workflow + " and Error: " + errorString);

			StringBuilder SQL = new StringBuilder("SELECT * FROM ( ");
			SQL.append("SELECT SUBSTRING(?, lookup) AS matched_txt, * ");
			SQL.append("FROM meta_error_registry ");
			SQL.append("WHERE (WORKFLOW_NAME = ? OR WORKFLOW_NAME IS NULL) ");
			SQL.append(") AS match_results ");
			SQL.append("WHERE matched_txt IS NOT NULL ");
			SQL.append("ORDER BY WORKFLOW_NAME, LENGTH(matched_txt) DESC ");

			// remove "null character" that is not supported by Aurora in text fields
			String normalizedErrorString = errorString.replace("\u0000", "");

			return query( tx, SQL.toString(), q-> q.addParameter(normalizedErrorString).addParameter(workflow).executeAndFetch(handler));

		});
	}


	@Override
	public List<ErrorLookup> getErrorMatching(String errorString) {
		return getErrorMatching(null, errorString);
	}

	@Override
	public List<ErrorLookup> getErrorByCode(String errorCode) {
		return getWithTransaction( tx->{
			ErrorLookupHandler handler = new ErrorLookupHandler();
			logger.debug("Lookup error details for Error Code: " + errorCode );

			String SQL = "SELECT * FROM meta_error_registry WHERE ERROR_CODE = ?";
			return query( tx, SQL, q-> q.addParameter(errorCode).executeAndFetch(handler));
		});
	}

	@Override
	public boolean addError(ErrorLookup errorLookup) {
		validate(errorLookup);
		return insertOrUpdateErrorLookup(errorLookup);
	}

	@Override
	public boolean updateError(ErrorLookup errorLookup) {
		validate(errorLookup);
		return insertOrUpdateErrorLookup(errorLookup);
	}

	private boolean insertOrUpdateErrorLookup(ErrorLookup errorLookup) {

		return getWithTransaction( tx->{
			int result = query( tx, INSERT_SQL, q->q.addParameter(errorLookup.getErrorCode())
					.addParameter(errorLookup.getLookup())
					.addParameter(errorLookup.getWorkflowName())
					.addParameter(errorLookup.getGeneralMessage())
					.addParameter(errorLookup.getRootCause())
					.addParameter(errorLookup.getResolution())
					.executeUpdate());

			if ( result > 0) {
				return true;
			}else{
				return query( tx, UPDATE_SQL, q->q.addParameter(errorLookup.getErrorCode())
						.addParameter(errorLookup.getLookup())
						.addParameter(errorLookup.getWorkflowName())
						.addParameter(errorLookup.getGeneralMessage())
						.addParameter(errorLookup.getRootCause())
						.addParameter(errorLookup.getResolution())
						.addParameter(errorLookup.getId())
						.executeUpdate() > 0);
			}
		});
	}

	class ErrorLookupHandler implements ResultSetHandler<ArrayList<ErrorLookup>> {

        public ArrayList<ErrorLookup> apply(ResultSet rs) throws SQLException {
            ArrayList<ErrorLookup> errorLookups = new ArrayList<>();

            while( rs.next()){
                ErrorLookup errorLookup = new ErrorLookup();
                errorLookup.setId( rs.getInt("id"));
                errorLookup.setErrorCode( rs.getString("error_code"));
                errorLookup.setLookup(rs.getString("lookup"));
                errorLookup.setWorkflowName(rs.getString("workflow_name"));
                errorLookup.setGeneralMessage(rs.getString("general_message"));
                errorLookup.setRootCause( rs.getString("root_cause"));
                errorLookup.setResolution( rs.getString("resolution"));
                errorLookups.add(errorLookup);
            }
            return errorLookups;
        }
    }


    private void validate(ErrorLookup errorLookup){
		Preconditions.checkNotNull(errorLookup, "ErrorLookup cannot be null");
	}
}
