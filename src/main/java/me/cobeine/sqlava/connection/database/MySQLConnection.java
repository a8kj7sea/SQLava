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

    public MySQLConnection(CredentialsHolder record) {
        this.credentialsHolder = record;
        this.tableCommands = new TableCommands(this);
        this.logger = Logger.getLogger(this.getClass().getName());
    }

    /**
     * Attempts to connect and calls the callback with 0 for success or -1 for
     * failure.
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
     * Connects with optional HikariConfig customization.
     */
    public ConnectionResult connect(Consumer<HikariConfig> consumer) {
        HikariConfig config = new HikariConfig();

        String datasource = credentialsHolder.getProperty(BasicMySQLCredentials.DATASOURCE_CLASS_NAME, String.class);
        if (datasource != null)
            config.setDataSourceClassName(datasource);

        String driver = credentialsHolder.getProperty(BasicMySQLCredentials.DRIVER, String.class);
        if (driver != null)
            config.setDriverClassName(driver);

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

        for (CredentialsKey key : credentialsHolder.keySet()) {
            if (key.isProperty()) {
                Object value = credentialsHolder.getProperty(key, key.getDataType());
                config.addDataSourceProperty(key.getKey(), value);
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

        try (Connection conn = pool.resource()) {
            if (conn != null) {
                return ConnectionResult.success(conn);
            } else {
                return ConnectionResult
                        .failure(ConnectionError.connectivityError("Failed to acquire a connection", null));
            }
        } catch (SQLException e) {
            return ConnectionResult.failure(ConnectionError.connectivityError("Failed to connect to MySQL", e));
        }
    }

    /**
     * Connects with default configuration.
     */
    @Override
    public ConnectionResult connect() throws SQLException {
        return connect(null);
    }

    /**
     * Prepares a statement from a {@link Query}.
     */
    public PreparedQuery prepareStatement(Query query) {
        return query.prepareStatement(this);
    }

    /**
     * Prepares a statement directly from SQL string.
     */
    public PreparedQuery prepareStatement(String query) {
        return new PreparedQuery(this, query);
    }

    /**
     * Returns the underlying HikariDataSource.
     */
    @Override
    public HikariDataSource getConnection() {
        return dataSource;
    }
}
