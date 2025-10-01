import me.cobeine.sqlava.connection.ConnectionResult;
import me.cobeine.sqlava.connection.auth.BasicMySQLCredentials;
import me.cobeine.sqlava.connection.auth.CredentialsHolder;
import me.cobeine.sqlava.connection.database.MySQLConnection;
import me.cobeine.sqlava.connection.database.query.PreparedQuery;
import me.cobeine.sqlava.connection.database.query.Query;
import me.cobeine.sqlava.connection.database.table.TableCommands;
import me.cobeine.sqlava.connection.util.JdbcUrlBuilder;

import java.sql.SQLException;

/**
 * Example usage of {@link MySQLConnection}, queries, and table commands.
 * <p>
 * Demonstrates:
 * <ul>
 * <li>Connecting to MySQL using {@link CredentialsHolder}</li>
 * <li>Creating tables</li>
 * <li>Executing synchronous and asynchronous queries</li>
 * <li>Handling connection errors properly using {@link ConnectionResult}</li>
 * </ul>
 * </p>
 *
 * Author: <a href="https://github.com/Cobeine">Cobeine</a>
 */
public class Examples {

    public Examples() throws SQLException {
        // Step 1: Build JDBC URL
        String url = JdbcUrlBuilder.newBuilder()
                .host("host")
                .port(3306)
                .setAutoReconnect(true)
                .database("database")
                .build();

        // Step 2: Set up MySQL credentials
        CredentialsHolder mysqlCreds = CredentialsHolder.builder()
                .add(BasicMySQLCredentials.USERNAME, "root")
                .add(BasicMySQLCredentials.PASSWORD, "easypass")
                .add(BasicMySQLCredentials.DATABASE, "exampledb")
                .add(BasicMySQLCredentials.PORT, 3306)
                .add(BasicMySQLCredentials.POOL_SIZE, 8)
                .add(BasicMySQLCredentials.JDBC_URL, url)
                .build();

        // Step 3: Initialize MySQL connection
        MySQLConnection conn = new MySQLConnection(mysqlCreds);

        // Step 4: Connect to the database and handle result
        ConnectionResult result = conn.connect();

        if (result.isFailure()) {
            // Handle connection failure
            result.getError().ifPresent(error -> {
                error.causeOptional().ifPresent(Throwable::printStackTrace);
            });
            return;
        }

        // Successfully connected
        TableCommands tableCommands = conn.getTableCommands();

        // Step 5: Create tables
        tableCommands.createTable(new ExampleTable());
        tableCommands.createTable(new ExampleTable(), tableResult -> {
            tableResult.getException().ifPresent(Throwable::printStackTrace);
        });

        // Step 6: Prepare queries
        PreparedQuery selectQuery = conn.prepareStatement(
                Query.select("example").where("uuid", "test"));

        // Step 7: Execute query synchronously
        selectQuery.executeQuery();

        // Step 8: Execute query asynchronously
        selectQuery.executeQueryAsync(asyncResult -> {
            asyncResult.getException().ifPresent(Throwable::printStackTrace);
            asyncResult.getResult().ifPresent(resultSet -> {
                // Process ResultSet here
            });
        });

        // Step 9: Prepare and execute update query
        Query updateQuery = Query.update("example")
                .set("uuid", "test")
                .where("id", 1)
                .and("uuid", "test2");

        conn.prepareStatement(updateQuery)
                .setParameter(1, "test")
                .setParameter(2, 1)
                .setParameter(3, "test2")
                .executeUpdateAsync(updateResult -> {
                    updateResult.getException().ifPresent(Throwable::printStackTrace);
                });
    }

}
