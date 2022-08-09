package org.brito.jdbc;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@ExtendWith(VertxExtension.class)
public class H2StoreProcedureTest {
  protected SQLClient client;
  protected static Connection connection;
  private static final List<String> SQL = new ArrayList<>();

  static {
    System.setProperty("textdb.allow_full_path", "true");
    System.setProperty("statement.separator", ";;");

    SQL.add("drop table if exists customers");
    SQL.add(
      "create table customers(id integer auto_increment, firstname varchar(50), lastname varchar(50), added timestamp)");

    SQL.add("create procedure new_customer(firstname varchar(50), lastname varchar(50))\n" +
      "  modifies sql data\n" +
      "  insert into customers values (default, firstname, lastname, current_timestamp)");
    SQL.add("create procedure read_customer(p_firstname varchar(50))\n" +
      "  reads sql data dynamic result sets 1\n" +
      "  begin atomic \n" +
      "  declare curs cursor with return for select lastname from customers where firstname = p_firstname;\n" +
      "  open curs; \n" +
     "   end; ");
  }

  protected static JsonObject config() {
    return new JsonObject()
      .put("url", "jdbc:h2:mem:")
      .put("driver_class", "org.hsqldb.jdbcDriver");
  }

  @BeforeAll
  public static void createDb() throws Exception {
    connection = DriverManager.getConnection(config().getString("url"));
    for (String sql : SQL) {
      connection.createStatement().execute(sql);
    }
  }

  @BeforeEach
  public void setUp(Vertx vertx) {
    client = JDBCClient.create(vertx, config());
  }


  @Test
  @Timeout(4000)
  public void testGetCustomerNotNull(VertxTestContext context) {
    String readSql = "{call read_customer(?)}";
    client.callWithParams("{call new_customer(?, ?)}", new JsonArray().add("Cristian").add("Brito 2"), null, r -> {
      if (r.succeeded()) {
        try {
          CallableStatement statement = connection.prepareCall(readSql);
          statement.setString(1, "Cristian");
          boolean  hasResult = statement.execute();
          Assertions.assertFalse(hasResult);
          boolean moreResult = statement.getMoreResults();
          Assertions.assertTrue(moreResult);
          ResultSet resultSet = statement.getResultSet();
          while (resultSet.next()) {
            String lastName = resultSet.getString("LASTNAME");
            Assertions.assertTrue(lastName.startsWith("Brito"));
          }
          context.completeNow();
        } catch (Exception e) {
          context.failNow(e);
        }
      }
    });
  }

  @Test
  @Timeout(4000)
  public void procedureTest(VertxTestContext context) throws SQLException {
    String procedure = "{call new_customer(?, ?)}";
    Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
    conn.createStatement().execute("CREATE ALIAS new_customer FOR \"org.h2.engine.Constants.getVersion\"");

    CallableStatement statement = conn.prepareCall(procedure);
    boolean  hasResult = statement.execute();
    Assertions.assertTrue(hasResult);
    context.completeNow();
  }
}
