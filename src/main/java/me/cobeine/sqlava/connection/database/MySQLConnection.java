package me.cobeine.sqlava.connection.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import me.cobeine.sqlava.connection.Callback;
import me.cobeine.sqlava.connection.ConnectionError;
import me.cobeine.sqlava.connection.ConnectionResult;
import me.cobeine.sqlava.connection.auth.AuthenticatedConnection;
import me.cobeine.sqlava.connection.auth.BasicMySQLCredentials;
import me.cobeine.sqlava.connection.auth.CredentialsHolder;
import me.cobeine.sqlava.connection.auth.CredentialsKey;
import me.cobeine.sqlava.connection.database.query.PreparedQuery;
import me.cobeine.sqlava.connection.database.query.Query;
import me.cobeine.sqlava.connection.database.table.TableCommands;
import me.cobeine.sqlava.connection.pool.ConnectionPool;
import me.cobeine.sqlava.connection.pool.PooledConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Represents a MySQL database connection using HikariCP for pooling.
 * <p>
 * This class wraps the configuration and initialization of a
 * {@link HikariDataSource}, manages a connection pool,
 * and provides helper methods for preparing queries.
 * </p>
 *
 * @author <a href="https://github.com/Cobeine">Cobeine</a>
 */
@Getter
public class MySQLConnection implements
        AuthenticatedConnection<HikariDataSource>,
        PooledConnection<HikariDataSource, Connection> {

    private ConnectionPool<HikariDataSource, Connection> pool;
    private final CredentialsHolder credentialsHolder;
    private final Logger logger;
    private HikariDataSource dataSource;
    private final TableCommands tableCommands;

    /**
     * Creates a new MySQL connection with the given credentials.
     *
     * @param record the credentials holder
     */
    public MySQLConnection(CredentialsHolder record) {
        this.credentialsHolder = record;
        this.tableCommands = new TableCommands(this);
        this.logger = Logger.getLogger(this.getClass().getName());
    }

    /**
     * Attempts to connect to the database and executes the given callback
     * with success (0) or failure (-1).
     * <p>
     * Steps:
     * <ol>
     * <li>Invoke {@link #connect()} to attempt connection</li>
     * <li>If successful, call the callback with <code>(0, null)</code></li>
     * <li>If failed, call the callback with <code>(-1, exception)</code></li>
     * </ol>
     *
     * @param callback the callback to notify of connection result
     */
    public void connectWithFallback(Callback<Integer, Exception> callback) {
        try {
            connect();
            callback.call(0, null);
        } catch (Exception e) {
            callback.call(-1, e);
        }
    }

    /**
     * Attempts to establish a connection to the database with optional
     * configuration overrides.
     * <p>
     * Steps:
     * <ol>
     * <li>Create a new {@link HikariConfig}</li>
     * <li>Apply credentials (datasource, driver, URL, lifetime, pool size) if
     * present</li>
     * <li>Apply any additional customization from the given consumer</li>
     * <li>Apply all additional credential properties into the config</li>
     * <li>Initialize a new {@link HikariDataSource}</li>
     * <li>Create a {@link ConnectionPool} that wraps the data source</li>
     * <li>Test the connection by retrieving a {@link Connection}</li>
     * <li>If successful, return {@link ConnectionResult#success(Connection)}</li>
     * <li>If failure occurs, return
     * {@link ConnectionResult#failure(ConnectionError)}</li>
     * </ol>
     *
     * @param consumer optional consumer for customizing {@link HikariConfig}
     * @return a {@link ConnectionResult} indicating success or failure
     */
    public ConnectionResult connect(Consumer<HikariConfig> consumer) {
        HikariConfig config = new HikariConfig();

        if (credentialsHolder.getProperty(BasicMySQLCredentials.DATASOURCE_CLASS_NAME, String.class) != null) {
            config.setDataSourceClassName(
                    credentialsHolder.getProperty(BasicMySQLCredentials.DATASOURCE_CLASS_NAME, String.class));
        }
        if (credentialsHolder.getProperty(BasicMySQLCredentials.DRIVER, String.class) != null) {
            config.setDriverClassName(credentialsHolder.getProperty(BasicMySQLCredentials.DRIVER, String.class));
        }
        if (credentialsHolder.getProperty(BasicMySQLCredentials.JDBC_URL, String.class) != null) {
            config.setJdbcUrl(credentialsHolder.getProperty(BasicMySQLCredentials.JDBC_URL, String.class));
        }
        if (credentialsHolder.getProperty(BasicMySQLCredentials.MAX_LIFETIME, Long.class) != null) {
            config.setMaxLifetime(credentialsHolder.getProperty(BasicMySQLCredentials.MAX_LIFETIME, Long.class));
        }
        if (credentialsHolder.getProperty(BasicMySQLCredentials.POOL_SIZE, Integer.class) != null) {
            config.setMaximumPoolSize(credentialsHolder.getProperty(BasicMySQLCredentials.POOL_SIZE, Integer.class));
        }

        if (consumer != null) {
            consumer.accept(config);
        }

        for (CredentialsKey credentialsKey : credentialsHolder.keySet()) {
            if (credentialsKey.isProperty()) {
                config.addDataSourceProperty(credentialsKey.getKey(),
                        credentialsHolder.getProperty(credentialsKey, credentialsKey.getDataType()));
            }
        }

        dataSource = new HikariDataSource(config);
        this.pool = new ConnectionPool<>(dataSource) {
            @Override
            public Connection resource() {
                try {
                    return getSource().getConnection();
                } catch (SQLException e) {
                    logger.severe("Failed to acquire connection: " + e.getMessage());
                    return null;
                }
            }
        };

        try (Connection conn = getPool().resource()) {
            return ConnectionResult.success(conn);
        } catch (Exception e) {
            return ConnectionResult.failure(ConnectionError.connectivityError("Failed to connect to MySQL", e));
        }
    }

    /**
     * Connects with default configuration.
     * <p>
     * Equivalent to calling {@link #connect(Consumer)} with {@code null}.
     * </p>
     *
     * @return a {@link ConnectionResult} indicating success or failure
     */
    @Override
    public ConnectionResult connect() {
        return connect(null);
    }

    /**
     * Prepares a statement from a {@link Query}.
     * <p>
     * Steps:
     * <ol>
     * <li>Delegate to {@link Query#prepareStatement(MySQLConnection)}</li>
     * <li>Return a {@link PreparedQuery} ready for execution</li>
     * </ol>
     *
     * @param query the query abstraction
     * @return a prepared query wrapper
     */
    public PreparedQuery prepareStatement(Query query) {
        return query.prepareStatement(this);
    }

    /**
     * Prepares a statement directly from SQL string.
     * <p>
     * Steps:
     * <ol>
     * <li>Create a new {@link PreparedQuery} with this connection and the given SQL
     * string</li>
     * <li>Return the {@link PreparedQuery} instance</li>
     * </ol>
     *
     * @param query the raw SQL string
     * @return a prepared query wrapper
     */
    public PreparedQuery prepareStatement(String query) {
        return new PreparedQuery(this, query);
    }

    /**
     * Returns the underlying {@link HikariDataSource}.
     *
     * @return the data source
     */
    @Override
    public HikariDataSource getConnection() {
        return dataSource;
    }
}
