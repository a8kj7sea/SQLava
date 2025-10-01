<img src="https://i.imgur.com/2UVfA8d.png" alt="sqlava logo">  

<p align="center" href="https://www.codefactor.io/repository/github/cobeine/sqlava"><img src="https://www.codefactor.io/repository/github/cobeine/sqlava/badge" alt="CodeFactor" /></p> 
  
# <p align="center">SQLava</p>
 <p align="center"><b>Streamline MySQL interactions with SQLava, the beginner-friendly query builder for Java!</b></p>
 <p align="center"><b>Effortlessly construct complex queries with its intuitive syntax, designed for all skill levels</b></p>

## ✨ Key Features:

- <b>Intuitive Query Building:</b>  Craft elegant queries with ease, even without extensive SQL knowledge.
- <b>Asynchronous Excellence:</b>  Experience lightning-fast performance with asynchronous handling of connections and queries.
- <b>Beginner-Friendly:</b>  Dive in without hesitation, thanks to its clear and concise syntax.
- <b>Seamless Integration:</b> Effortlessly incorporate SQLava into your Java projects.

## Installation
[![](https://jitpack.io/v/Cobeine/SQLava.svg)](https://jitpack.io/#Cobeine/SQLava)
<p></p>
If you're using maven or gradle as a dependency manager then just follow the steps below.
Otherwise, just download the jar and add it as an artifact dependency.

### Adding the repository

###### Maven
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

###### Gradle
```groovy

repositories {
    mavenCentral()
    maven {
        url 'https://jitpack.io'
    }
}
```

### Implementing the dependency

###### Maven
```xml
<dependency>
    <groupId>com.github.Cobeine</groupId>
    <artifactId>SQLava</artifactId>
    <version>{VERSION}</version>
</dependency>
```

###### Gradle
```groovy

dependencies {
	   implementation 'com.github.Cobeine:SQLava:{VERSION}'
    //Please note that my library is dependant on Hikrai 5.0.1 or above
    //So, make sure to implement HikariCP in your project.
}
```

### Examples

#### Connection Examples:
```java
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
 * * Author: <a href="https://github.com/Cobeine">Cobeine</a>
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
                .add(BasicMySQLCredentials.USERNAME, "username")
                .add(BasicMySQLCredentials.PASSWORD, "password")
                .add(BasicMySQLCredentials.DATABASE, "database")
                .add(BasicMySQLCredentials.PORT, 3306)
                .add(BasicMySQLCredentials.POOL_SIZE, 8)
                .add(BasicMySQLCredentials.JDBC_URL, url)
                .build();

        // Step 3: Initialize MySQL connection
        connection = new MySQLConnection(mysqlCreds);

        // Step 4: Connect to the database and handle result
        ConnectionResult result = connection.connect();

        if (result.isFailure()) {
            // Handle connection failure using the new result pattern
            result.error().ifPresent(error -> {
                System.err.println("Connection Failed: " + error.message());
                error.causeOptional().ifPresent(Throwable::printStackTrace);
            });
            return;
        }

        // Successfully connected
        TableCommands tableCommands = connection.getTableCommands();

        // Step 5: Create tables (Synchronous and Asynchronous)
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

}

```
#### Table Example
```java 

public class Example extends Table {


    public Example() {
        super("example");
        addColumns(
                Column.of("id", ColumnType.INT).settings(ColumnSettings.AUTO_INCREMENT, ColumnSettings.UNIQUE),
                Column.of("uuid", ColumnType.TEXT).settings(ColumnSettings.NOT_NULL, ColumnSettings.UNIQUE).defaultValue("none"),
                Column.of("name", ColumnType.VARCHAR).size(64).settings(ColumnSettings.NOT_NULL, ColumnSettings.UNIQUE).defaultValue("none"),
                Column.of("rank", ColumnType.VARCHAR).size(64).settings(ColumnSettings.NOT_NULL).defaultValue("DEFAULT")
                );
        setPrimaryKey("id");

    }
}
```

#### Query examples
```java
public class Examples {
    public void examples() {

        Table table = new Table();
        connection.getTableCommands().createTable(table);

        connection.prepareStatement(
                        Query.select("test").where("id").and("uuid"))
                .setParameter(1, 5)
                .setParameter(2, UUID.randomUUID())
                .executeQueryAsync(result ->
                        result.executeIfPresent(resultSet -> {
			    resultSet.next(); // or hasNext depending on you
                            int id = resultSet.getInt("id"); //examples

                        }).orElse(exception -> {
                            if (exception != null)
                                exception.printStackTrace();
                        }).apply()
                );

        //or

        Query selectQuery = Query.select("table").where("uuid").and("id");
        PreparedQuery query = selectQuery.prepareStatement(connection);
        query.setParameter(1, UUID.randomUUID()).setParameter(2, 1);
        try (ResultSet set = query.executeQuery()) {
            set.getInt("id");//etc
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
}

```


### Contributing

Before contributing to this project, please note the following:

###### - Test your changes: Ensure that any changes you make to the project have been thoroughly tested before submitting them.

###### - Provide documentation: If you are adding new features or functionality, consider providing clear and concise documentation to help other contributors understand how to use them.

###### - Follow established standards: Adhere to established coding standards and best practices to ensure that your contributions are consistent with the rest of the project.

###### - Submit changes appropriately: Follow the project's preferred method of submitting changes, whether that is through pull requests, commits, or another method specified by the project owner.


#### <p align="center">Thanks for reading & reviewing my project!</p>
##### <p align="center"> Special thanks to <a href="https://github.com/AkramLZ">@AkramLZ</a> for inspiration!</p>





