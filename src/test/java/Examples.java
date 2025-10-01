
import me.cobeine.sqlava.connection.ConnectionResult;
import me.cobeine.sqlava.connection.auth.BasicMySQLCredentials;
import me.cobeine.sqlava.connection.auth.CredentialsHolder;
import me.cobeine.sqlava.connection.database.MySQLConnection;
import me.cobeine.sqlava.connection.database.query.PreparedQuery;
import me.cobeine.sqlava.connection.database.query.Query;
import me.cobeine.sqlava.connection.database.table.TableCommands;
import me.cobeine.sqlava.connection.util.JdbcUrlBuilder;

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

    private final MySQLConnection connection;

    public Examples() throws Exception {
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
        connection = new MySQLConnection(mysqlCreds);

        // Step 4: Connect to the database and handle result
        ConnectionResult result = connection.connect();

        if (result.isFailure()) {
            // Handle connection failure
            result.getError().ifPresent(e -> e.causeOptional().ifPresent(Throwable::printStackTrace));
            return;
        }

        // Successfully connected
        TableCommands tableCommands = connection.getTableCommands();

        // Step 5: Create tables
        tableCommands.createTable(new ExampleTable());
        tableCommands.createTable(new ExampleTable(), tableResult -> {
            if (tableResult.getException().isPresent()) {
                tableResult.getException().get().printStackTrace();
                return;
            }
            // Table successfully created
        });

        // Step 6: Prepare queries
        PreparedQuery selectQuery = connection.prepareStatement(
                Query.select("example").where("uuid", "test"));

        // Step 7: Execute query synchronously
        selectQuery.executeQuery();

        // Step 8: Execute query asynchronously
        selectQuery.executeQueryAsync(asyncResult -> {
            if (asyncResult.getException().isPresent()) {
                asyncResult.getException().get().printStackTrace();
                return;
            }
            // Successfully executed query
            asyncResult.getResult().ifPresent(resultSet -> {
                // Process ResultSet here
            });
        });

        // Step 9: Prepare and execute update query
        Query updateQuery = Query.update("example")
                .set("uuid", "test")
                .where("id", 1)
                .and("uuid", "test2");

        connection.prepareStatement(updateQuery)
                .setParameter(1, "test")
                .setParameter(2, 1)
                .setParameter(3, "test2")
                .executeUpdateAsync(updateResult -> {
                    if (updateResult.getException().isPresent()) {
                        updateResult.getException().get().printStackTrace();
                        return;
                    }
                    // Successfully executed update
                });
    }

    public static void main(String[] args) {
        try {
            new Examples();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
