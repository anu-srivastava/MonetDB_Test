package com.test;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class PerformanceTest {

    @Rule
    public ContiPerfRule rule = new ContiPerfRule();

    private static final int INVOCATION_COUNT_PER_THREAD = 8;
    private static final int THREAD_COUNT = 8;

    //Queries to be executed
    private static String[] queries = null;

    /**
     * Initializes this test case.
     */
    @BeforeClass
    public static void setUpBeforeTestClass() throws Exception {
        setupSchema();
    }

    private static void setupSchema() {
        DatabaseManager manager = new DatabaseManager();
        try {
            manager.setupSchema();
            queries = manager.generateQueryStatements();
            ArrayList<String> copyQueries = manager.generateCopyStatements();
            RunCopyCommands(copyQueries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void RunCopyCommands(ArrayList<String> copyCommands) {
        for (String query : copyCommands) {
            try (Connection connection = ConnectionPool.getInstance().getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(query);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @PerfTest(invocations = INVOCATION_COUNT_PER_THREAD, threads = THREAD_COUNT)
    public void AllQueryPerformance() throws Exception {
        while (true) {
            if (queries != null) {
                for (String query : queries) {
                    try (Connection connection = ConnectionPool.getInstance().getConnection()) {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(query);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
