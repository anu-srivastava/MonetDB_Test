package com.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import nl.cwi.monetdb.jdbc.MonetDatabaseMetaData;
import org.apache.commons.io.FilenameUtils;

public class DatabaseManager {

    //Data Location
    private final static String rootLocation = System.getProperty("user.dir");
    protected final static String separator = System.getProperty("file.separator");
    protected final static String baseDir = rootLocation + separator + "data" + separator;

    public ArrayList<String> generateCopyStatements() throws IOException
    {
        ArrayList<String> copyQueries = new ArrayList<>();

        File data = new File(baseDir);
        if (!data.exists() || !data.isDirectory()) {
            throw new IllegalStateException("Data directory not found: " + data.getAbsolutePath());
        }
        File[] dataFiles = data.listFiles();
        if (dataFiles != null) {
            for (File dataFile : dataFiles) {
                if (dataFile.getName().endsWith(".txt")) {
                    copyQueries.add("COPY INTO " + FilenameUtils.removeExtension(dataFile.getName()) + " FROM '" + baseDir + separator + dataFile.getName() + "' null as '\\\\N';");
                }
            }
        }
        return copyQueries;
    }

    public String[] generateQueryStatements() throws IOException {
        String[] queries = null;

        File data = new File(baseDir);
        if (!data.exists() || !data.isDirectory()) {
            throw new IllegalStateException("Data directory not found: " + data.getAbsolutePath());
        }
        File[] dataFiles = data.listFiles();
        if (dataFiles != null) {
            for (File dataFile : dataFiles) {
                if (dataFile.getName().equalsIgnoreCase("benchmark_1_queries.sql")) {
                    queries = deserializeString(dataFile).split(";");
                }
            }
        }
        return queries;
    }

    public void setupSchema() throws IOException {
        File data = new File(baseDir);
        if (!data.exists() || !data.isDirectory()) {
            throw new IllegalStateException("Data directory not found: " + data.getAbsolutePath());
        }

        Files.readAllLines(Paths.get(baseDir + "schema_setup_script.sql")).stream().filter(line -> !line.isEmpty()).forEach(line -> {
            try (Connection connection = ConnectionPool.getInstance().getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(line);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    private static String deserializeString(File file)
            throws IOException {
        int len;
        char[] chr = new char[4096];
        final StringBuilder buffer = new StringBuilder();
        try (FileReader reader = new FileReader(file)) {
            while ((len = reader.read(chr)) > 0) {
                buffer.append(chr, 0, len);
            }
        }
        return buffer.toString();
    }
}
