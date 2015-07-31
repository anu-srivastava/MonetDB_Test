package com.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Main {

    //Queries to be executed
    private static String[] queries = null;

    public static void main(String[] args) {
        setupSchema();
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

    private static void setupSchema() {
        DatabaseManager manager = new DatabaseManager();
        try {
            //manager.setupSchema();
            queries = manager.generateQueryStatements();
            ArrayList<String> copyQueries = manager.generateCopyStatements();
            //RunCopyCommands(copyQueries);
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
}

