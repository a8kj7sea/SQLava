package me.cobeine.sqlava.connection;

import java.util.Optional;

/**
 * Represents detailed error information for a failed connection attempt.
 * <p>
 * This class provides factory methods for common error categories
 * (authentication, connectivity, and unknown issues).
 * </p>
 */
public final class ConnectionError {

    private final String message;
    private final Exception cause;

    /**
     * Defines common error categories for classification.
     */
    public enum Type {
        AUTH, CONNECTIVITY, UNKNOWN
    }

    /**
     * Creates a new ConnectionError.
     *
     * @param message a human-readable message describing the error
     * @param cause   the underlying exception that caused the error (may be null)
     */
    public ConnectionError(String message, Exception cause) {
        this.message = message;
        this.cause = cause;
    }

    /**
     * Returns the error message.
     *
     * @return the error message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Returns the underlying exception.
     *
     * @return the cause exception (may be null)
     */
    public Exception getCause() {
        return cause;
    }

    /**
     * Creates an error representing an authentication failure.
     *
     * @param cause the underlying exception
     * @return a {@code ConnectionError} indicating invalid credentials
     */
    public static ConnectionError authFailure(Exception cause) {
        return new ConnectionError("Authentication failed. Check username and password.", cause);
    }

    /**
     * Creates an error representing a connectivity or timeout issue.
     *
     * @param message a descriptive error message
     * @param cause   the underlying exception
     * @return a {@code ConnectionError} indicating connectivity problems
     */
    public static ConnectionError connectivityError(String message, Exception cause) {
        return new ConnectionError(message, cause);
    }

    /**
     * Creates an error representing an unknown or unexpected issue.
     *
     * @param cause the underlying exception
     * @return a {@code ConnectionError} indicating an unspecified problem
     */
    public static ConnectionError unknown(Exception cause) {
        return new ConnectionError("An unknown error occurred during connection.", cause);
    }

    /**
     * Returns the underlying cause wrapped in an {@link Optional}.
     *
     * @return an {@link Optional} containing the exception if present, otherwise
     *         empty
     */
    public Optional<Exception> causeOptional() {
        return Optional.ofNullable(cause);
    }

    /**
     * Converts this error into a {@link RuntimeException}.
     *
     * @return a new {@code RuntimeException} with the error message and cause
     */
    public RuntimeException toRuntimeException() {
        return new RuntimeException(message, cause);
    }
}
