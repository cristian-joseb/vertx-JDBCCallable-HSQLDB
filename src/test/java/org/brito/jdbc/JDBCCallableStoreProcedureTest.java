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
public class JDBCCallableStoreProcedureTest {
  protected SQLClient client;
  protected static Connection connection;
  private static final List<String> SQL = new ArrayList<>();

  static {
    System.setProperty("textdb.allow_full_path", "true");
    System.setProperty("statement.separator", ";;");

    SQL.add("drop table if exists customers");
    SQL.add(
      "create table customers(id integer generated by default as identity, firstname varchar(50), lastname varchar(50), added timestamp)");
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
      .put("url", "jdbc:hsqldb:mem:test2?shutdown=true")
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
  public void testGetCustomerNull(VertxTestContext context) {
    client.callWithParams("{call new_customer(?, ?)}", new JsonArray().add("Cristian").add("Brito 1"), null, r -> {
      if (r.succeeded()) {
        client.callWithParams("{call read_customer(?)}", new JsonArray().add("Cristian"), null, r2 -> {
          if (r.succeeded()) {
            Assertions.assertNull(r.result());
            context.completeNow();
          }
        });
      }
    });
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
}