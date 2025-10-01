package me.cobeine.sqlava.connection;

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
 *
 * <p>
 * It provides functional-style helpers for working with success/failure cases.
 * </p>
 */
@Getter
public final class ConnectionResult {

    private final Optional<Connection> connection;
    private final Optional<ConnectionError> error;

    // ---------- Factory Constants ----------

    public static final ConnectionResult SUCCESS = new ConnectionResult(Optional.empty(), Optional.empty());
    public static final ConnectionResult FAIL = new ConnectionResult(Optional.empty(), Optional.empty());

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

    /**
     * Creates a successful result with the given database connection.
     *
     * @param connection the successfully established connection
     * @return a {@code ConnectionResult} representing success
     */
    public static ConnectionResult success(Connection connection) {
        return new ConnectionResult(Optional.of(connection), Optional.empty());
    }

    /**
     * Creates a failed result with the provided error details.
     *
     * @param error the error information
     * @return a {@code ConnectionResult} representing failure
     */
    public static ConnectionResult failure(ConnectionError error) {
        return new ConnectionResult(Optional.empty(), Optional.of(error));
    }

    /**
     * Creates a failed result with only a human-readable message.
     *
     * @param message the error description
     * @return a {@code ConnectionResult} representing failure
     */
    public static ConnectionResult failure(String message) {
        return new ConnectionResult(Optional.empty(), Optional.of(new ConnectionError(message, null)));
    }

    // ---------- Query Helpers ----------

    /**
     * Checks if the connection attempt was successful.
     *
     * @return {@code true} if this result contains a connection, {@code false}
     *         otherwise
     */
    public boolean isSuccess() {
        return connection.isPresent();
    }

    /**
     * Checks if the connection attempt failed.
     *
     * @return {@code true} if this result contains an error, {@code false}
     *         otherwise
     */
    public boolean isFailure() {
        return error.isPresent();
    }

    // ---------- Functional Helpers ----------

    /**
     * Applies a mapping function to the connection if successful.
     *
     * @param <R>    the result type of the mapping function
     * @param mapper a function to transform the {@link Connection}
     * @return an {@link Optional} containing the mapped result, or empty if this is
     *         a failure
     */
    public <R> Optional<R> mapConnection(Function<Connection, R> mapper) {
        return connection.map(mapper);
    }

    /**
     * Executes the given action if the connection attempt was successful.
     *
     * @param consumer a consumer that accepts the {@link Connection}
     */
    public void ifSuccess(Consumer<Connection> consumer) {
        connection.ifPresent(consumer);
    }

    /**
     * Executes the given action if the connection attempt failed.
     *
     * @param consumer a consumer that accepts the {@link ConnectionError}
     */
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
        throw new RuntimeException(error.map(ConnectionError::getMessage).orElse("Unknown connection error"));
    }

}
