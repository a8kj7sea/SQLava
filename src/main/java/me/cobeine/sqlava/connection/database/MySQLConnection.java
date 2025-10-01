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
 * Represents a MySQL database connection using HikariCP for connection pooling.
 * <p>
 * This class manages HikariDataSource configuration, initialization, and a
 * connection pool.
 * It provides methods to connect to the database, prepare SQL statements, and
 * handle errors
 * using the {@link ConnectionResult} pattern instead of raw exceptions.
 * </p>
 *
 * <p>
 * Example usage:
 * 
 * <pre>
 * CredentialsHolder creds = ...;
 * MySQLConnection connection = new MySQLConnection(creds);
 * ConnectionResult result = connection.connect();
 * if (result.isSuccess()) {
 *     // use result.getConnection()
 * } else {
 *     // handle error
 * }
 * </pre>
 * </p>
 *
 * @author
 *         <a href="https://github.com/Cobeine">Cobeine</a>
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
     * Constructs a new {@code MySQLConnection} instance with the provided
     * credentials.
     *
     * @param record the credentials holder containing database connection
     *               properties
     */
    public MySQLConnection(CredentialsHolder record) {
        this.credentialsHolder = record;
        this.tableCommands = new TableCommands(this);
        this.logger = Logger.getLogger(this.getClass().getName());
    }

    /**
     * Attempts to connect to the database and notifies the provided callback with
     * 0 for success or -1 for failure.
     *
     * @param callback a {@link Callback} that receives the result status and any
     *                 {@link SQLException} if failed
     */
    public void connectWithFallback(Callback<Integer, SQLException> callback) {
        try {
            connect();
            callback.call(0, null);
        } catch (SQLException e) {
            callback.call(-1, e);
        }
    }

    /**
     * Connects to the MySQL database with optional {@link HikariConfig}
     * customization.
     *
     * <p>
     * The method performs the following steps:
     * <ul>
     * <li>Reads configuration from {@link CredentialsHolder}</li>
     * <li>Applies any overrides provided via the {@code consumer}</li>
     * <li>Initializes HikariDataSource and a connection pool</li>
     * <li>Attempts to acquire a connection to test the configuration</li>
     * <li>Returns a {@link ConnectionResult} indicating success or failure</li>
     * </ul>
     * </p>
     *
     * @param consumer optional consumer to customize {@link HikariConfig} before
     *                 creating the datasource
     * @return a {@link ConnectionResult} representing the success or failure of the
     *         connection attempt
     */
    public ConnectionResult connect(Consumer<HikariConfig> consumer) {
        HikariConfig config = new HikariConfig();

        // Extract configuration values from credentials holder
        String datasourceClass = credentialsHolder.getProperty(BasicMySQLCredentials.DATASOURCE_CLASS_NAME,
                String.class);
        if (datasourceClass != null)
            config.setDataSourceClassName(datasourceClass);

        String driverClass = credentialsHolder.getProperty(BasicMySQLCredentials.DRIVER, String.class);
        if (driverClass != null)
            config.setDriverClassName(driverClass);

        String jdbcUrl = credentialsHolder.getProperty(BasicMySQLCredentials.JDBC_URL, String.class);
        if (jdbcUrl != null)
            config.setJdbcUrl(jdbcUrl);

        Long maxLifetime = credentialsHolder.getProperty(BasicMySQLCredentials.MAX_LIFETIME, Long.class);
        if (maxLifetime != null)
            config.setMaxLifetime(maxLifetime);

        Integer poolSize = credentialsHolder.getProperty(BasicMySQLCredentials.POOL_SIZE, Integer.class);
        if (poolSize != null)
            config.setMaximumPoolSize(poolSize);

        if (consumer != null)
            consumer.accept(config);

        // Add extra datasource properties
        for (CredentialsKey key : credentialsHolder.keySet()) {
            if (key.isProperty()) {
                Object value = credentialsHolder.getProperty(key, key.getDataType());
                config.addDataSourceProperty(key.getKey(), value);
            }
        }

        // Initialize datasource and pool
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

        // Test connection
        try (Connection conn = pool.resource()) {
            if (conn != null) {
                return ConnectionResult.success(conn);
            } else {
                return ConnectionResult.failure(
                        ConnectionError.connectivityError("Failed to acquire a connection", null));
            }
        } catch (SQLException e) {
            return ConnectionResult.failure(
                    ConnectionError.connectivityError("Failed to connect to MySQL", e));
        }
    }

    /**
     * Connects to the MySQL database using default configuration.
     *
     * @return a {@link ConnectionResult} indicating success or failure
     * @throws SQLException if an SQL error occurs during connection
     */
    @Override
    public ConnectionResult connect() throws SQLException {
        return connect(null);
    }

    /**
     * Prepares a SQL statement from a {@link Query} object.
     *
     * @param query the {@link Query} to prepare
     * @return a {@link PreparedQuery} ready for execution
     */
    public PreparedQuery prepareStatement(Query query) {
        return query.prepareStatement(this);
    }

    /**
     * Prepares a SQL statement directly from a raw SQL string.
     *
     * @param query the SQL string
     * @return a {@link PreparedQuery} ready for execution
     */
    public PreparedQuery prepareStatement(String query) {
        return new PreparedQuery(this, query);
    }

    /**
     * Returns the underlying {@link HikariDataSource} used by this connection.
     *
     * @return the HikariDataSource
     */
    @Override
    public HikariDataSource getConnection() {
        return dataSource;
    }
}
