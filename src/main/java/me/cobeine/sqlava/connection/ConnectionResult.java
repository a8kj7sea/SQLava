package me.cobeine.sqlava.connection;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.Getter;

/**
 * Represents the result of a connection attempt.
 * <p>
 * This class encapsulates either a successfully established {@link Connection}
 * or a {@link ConnectionError} describing why the connection failed.
 * </p>
 */
@Getter
public final class ConnectionResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Optional<Connection> connection;
    private final Optional<ConnectionError> error;

    // ---------- Factory Constants ----------
    public static final ConnectionResult SUCCESS_INSTANCE = new ConnectionResult(Optional.empty(), Optional.empty());
    public static final ConnectionResult FAIL_INSTANCE = new ConnectionResult(Optional.empty(), Optional.empty());

    /**
     * Creates a new ConnectionResult.
     *
     * @param connection the established connection if successful, otherwise empty
     * @param error      the error information if failed, otherwise empty
     */
    public ConnectionResult(Optional<Connection> connection, Optional<ConnectionError> error) {
        this.connection = connection;
        this.error = error;
    }

    // ---------- Factory Methods ----------
    public static ConnectionResult success(Connection connection) {
        return new ConnectionResult(Optional.of(connection), Optional.empty());
    }

    public static ConnectionResult failure(ConnectionError error) {
        return new ConnectionResult(Optional.empty(), Optional.of(error));
    }

    public static ConnectionResult failure(String message) {
        return new ConnectionResult(Optional.empty(), Optional.of(new ConnectionError(message, null)));
    }

    // ---------- Query Helpers ----------
    public boolean isSuccess() {
        return connection.isPresent();
    }

    public boolean isFailure() {
        return error.isPresent();
    }

    // ---------- Functional Helpers ----------
    public <R> Optional<R> mapConnection(Function<Connection, R> mapper) {
        return connection.map(mapper);
    }

    public void ifSuccess(Consumer<Connection> consumer) {
        connection.ifPresent(consumer);
    }

    public void ifFailure(Consumer<ConnectionError> consumer) {
        error.ifPresent(consumer);
    }

    /**
     * Returns the connection if successful, or throws a {@link RuntimeException}
     * wrapping the error message and cause if failed.
     *
     * @return the established {@link Connection}
     * @throws RuntimeException if this result represents a failure
     */
    public Connection getOrThrow() {
        if (isSuccess()) {
            return connection.get();
        }

        Optional<ConnectionError> errOpt = error;
        String msg = errOpt.map(ConnectionError::getMessage).orElse("Unknown connection error");
        Optional<Exception> causeOpt = errOpt.flatMap(ConnectionError::causeOptional);
        RuntimeException ex;
        if (causeOpt.isPresent()) {
            ex = new RuntimeException(msg, causeOpt.get());
        } else {
            ex = new RuntimeException(msg);
        }
        return throwRuntime(ex);
    }

    private Connection throwRuntime(RuntimeException ex) {
        throw ex;
    }
}
