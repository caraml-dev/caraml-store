package dev.caraml.store.base;

import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Base Integration Test class. Setups postgres container. Configures related properties and beans.
 * Provides DB related clean up between tests.
 */
@SpringBootTest
@ActiveProfiles("it")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class BaseIT {

  @Container
  public static PostgreSQLContainer<?> postgreSQLContainer =
      new PostgreSQLContainer<>("postgres:14.3");

  /**
   * Configure Spring Application to use postgres and kafka rolled out in containers
   *
   * @param registry
   */
  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {

    registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
    registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
    registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
    registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
  }

  /**
   * SequentialFlow is base class that is supposed to be inherited by @Nested test classes that
   * wants to preserve context between test cases. For SequentialFlow databases is being truncated
   * only once after all tests passed.
   */
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  public static class SequentialFlow {
    @AfterAll
    public void tearDown() throws Exception {
      cleanTables();
    }
  }

  /**
   * Truncates all tables in Database (between tests or flows). Retries on deadlock
   *
   * @throws SQLException
   */
  public static void cleanTables() throws SQLException {
    Connection connection =
        DriverManager.getConnection(
            postgreSQLContainer.getJdbcUrl(),
            postgreSQLContainer.getUsername(),
            postgreSQLContainer.getPassword());

    List<String> tableNames = new ArrayList<>();
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
    while (rs.next()) {
      tableNames.add(rs.getString(1));
    }
    tableNames.remove("flyway_schema_history");
    if (tableNames.isEmpty()) {
      return;
    }

    // retries are needed since truncate require exclusive lock
    // and that often leads to Deadlock
    // since SpringApp is still running in another thread
    int num_retries = 5;
    for (int i = 1; i <= num_retries; i++) {
      try {
        statement = connection.createStatement();
        statement.execute(String.format("truncate %s cascade", String.join(", ", tableNames)));
      } catch (SQLException e) {
        if (i == num_retries) {
          throw e;
        }
        continue;
      }

      break;
    }
  }

  /** Used to determine SequentialFlows */
  public Boolean isSequentialTest(TestInfo testInfo) {
    try {
      testInfo.getTestClass().get().asSubclass(SequentialFlow.class);
    } catch (ClassCastException e) {
      return false;
    }
    return true;
  }

  @AfterEach
  public void tearDown(TestInfo testInfo) throws Exception {
    CollectorRegistry.defaultRegistry.clear();

    if (!isSequentialTest(testInfo)) {
      cleanTables();
    }
  }
}
