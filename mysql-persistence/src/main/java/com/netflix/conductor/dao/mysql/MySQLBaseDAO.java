package com.netflix.conductor.dao.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.sql.ExecuteFunction;
import com.netflix.conductor.sql.QueryFunction;
import com.netflix.conductor.sql.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class MySQLBaseDAO {
    private static final List<String> EXCLUDED_STACKTRACE_CLASS = ImmutableList.of(
            MySQLBaseDAO.class.getName(),
            Thread.class.getName()
    );

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final ObjectMapper objectMapper;
    protected final DataSource dataSource;

    protected MySQLBaseDAO(ObjectMapper om, DataSource dataSource) {
        this.objectMapper = om;
        this.dataSource = dataSource;
    }

    protected final LazyToString getCallingMethod() {
        return new LazyToString(() -> Arrays.stream(Thread.currentThread().getStackTrace())
                .filter(ste -> !EXCLUDED_STACKTRACE_CLASS.contains(ste.getClassName()))
                .findFirst()
                .map(StackTraceElement::getMethodName)
                .orElseThrow(() -> new NullPointerException("Cannot find Caller")));
    }

    protected String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, ex);
        }
    }

    protected <T> T readValue(String json, Class<T> tClass) {
        try {
            return objectMapper.readValue(json, tClass);
        } catch (IOException ex) {
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, ex);
        }
    }

    protected <T> T readValue(String json, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (IOException ex) {
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, ex);
        }
    }

    /**
     * Initialize a new transactional {@link Connection} from {@link #dataSource} and pass it to {@literal function}.
     * <p>
     * Successful executions of {@literal function} will result in a commit and return of
     * {@link TransactionalFunction#apply(Connection)}.
     * <p>
     * If any {@link Throwable} thrown from {@code TransactionalFunction#apply(Connection)} will result in a rollback
     * of the transaction
     * and will be wrapped in an {@link ApplicationException} if it is not already one.
     * <p>
     * Generally this is used to wrap multiple {@link #execute(Connection, String, ExecuteFunction)} or
     * {@link #query(Connection, String, QueryFunction)} invocations that produce some expected return value.
     *
     * @param function The function to apply with a new transactional {@link Connection}
     * @param <R>      The return type.
     * @return The result of {@code TransactionalFunction#apply(Connection)}
     * @throws ApplicationException If any errors occur.
     */
    protected <R> R getWithTransaction(TransactionalFunction<R> function) {
        Instant start = Instant.now();
        LazyToString callingMethod = getCallingMethod();
        logger.trace("{} : starting transaction", callingMethod);

        try(Connection tx = dataSource.getConnection()) {
            boolean previousAutoCommitMode = tx.getAutoCommit();
            tx.setAutoCommit(false);
            try {
                R result = function.apply(tx);
                tx.commit();
                return result;
            } catch (Throwable th) {
                tx.rollback();
                throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, th.getMessage(), th);
            } finally {
                tx.setAutoCommit(previousAutoCommitMode);
            }
        } catch (SQLException ex) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
        } finally {
            logger.trace("{} : took {}ms", callingMethod, Duration.between(start, Instant.now()).toMillis());
        }
    }

    protected <R> R getWithTransactionWithOutErrorPropagation(TransactionalFunction<R> function) {
        Instant start = Instant.now();
        LazyToString callingMethod = getCallingMethod();
        logger.trace("{} : starting transaction", callingMethod);

        try(Connection tx = dataSource.getConnection()) {
            boolean previousAutoCommitMode = tx.getAutoCommit();
            tx.setAutoCommit(false);
            try {
                R result = function.apply(tx);
                tx.commit();
                return result;
            } catch (Throwable th) {
                tx.rollback();
                logger.info(ApplicationException.Code.CONFLICT + " " +th.getMessage());
                return null;
            } finally {
                tx.setAutoCommit(previousAutoCommitMode);
            }
        } catch (SQLException ex) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
        } finally {
            logger.trace("{} : took {}ms", callingMethod, Duration.between(start, Instant.now()).toMillis());
        }
    }


    /**
     * Wraps {@link #getWithTransaction(TransactionalFunction)} with no return value.
     * <p>
     * Generally this is used to wrap multiple {@link #execute(Connection, String, ExecuteFunction)} or
     * {@link #query(Connection, String, QueryFunction)} invocations that produce no expected return value.
     *
     * @param consumer The {@link Consumer} callback to pass a transactional {@link Connection} to.
     * @throws ApplicationException If any errors occur.
     * @see #getWithTransaction(TransactionalFunction)
     */
    protected void withTransaction(Consumer<Connection> consumer) {
        getWithTransaction(connection -> {
            consumer.accept(connection);
            return null;
        });
    }

    /**
     * Initiate a new transaction and execute a {@link Query} within that context,
     * then return the results of {@literal function}.
     *
     * @param query    The query string to prepare.
     * @param function The functional callback to pass a {@link Query} to.
     * @param <R>      The expected return type of {@literal function}.
     * @return The results of applying {@literal function}.
     */
    protected <R> R queryWithTransaction(String query, QueryFunction<R> function) {
        return getWithTransaction(tx -> query(tx, query, function));
    }

    /**
     * Execute a {@link Query} within the context of a given transaction and return the results of {@literal function}.
     *
     * @param tx       The transactional {@link Connection} to use.
     * @param query    The query string to prepare.
     * @param function The functional callback to pass a {@link Query} to.
     * @param <R>      The expected return type of {@literal function}.
     * @return The results of applying {@literal function}.
     */
    protected <R> R query(Connection tx, String query, QueryFunction<R> function) {
        try (Query q = new Query(objectMapper, tx, query)) {
            return function.apply(q);
        } catch (SQLException ex) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex);
        }
    }

    /**
     * Execute a statement with no expected return value within a given transaction.
     *
     * @param tx       The transactional {@link Connection} to use.
     * @param query    The query string to prepare.
     * @param function The functional callback to pass a {@link Query} to.
     */
    protected void execute(Connection tx, String query, ExecuteFunction function) {
        try (Query q = new Query(objectMapper, tx, query)) {
            function.apply(q);
        } catch (SQLException ex) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex);
        }
    }

    /**
     * Instantiates a new transactional connection and invokes {@link #execute(Connection, String, ExecuteFunction)}
     *
     * @param query    The query string to prepare.
     * @param function The functional callback to pass a {@link Query} to.
     */
    protected void executeWithTransaction(String query, ExecuteFunction function) {
        withTransaction(tx -> execute(tx, query, function));
    }
}
