package com.test;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionPool {

    private final static ConnectionPool INSTANCE = new ConnectionPool();
    private final BasicDataSource dataSource;

    /**
     * Private constructor to initialize the connection pool
     * instance (singleton).
     */
    private ConnectionPool() {
        try {
            final Properties properties = new Properties();
            properties.load(ConnectionPool.class.getClassLoader().getResourceAsStream("property.properties"));
            Class.forName(properties.getProperty("driverClassName"));
            dataSource = BasicDataSourceFactory.createDataSource(properties);
// dataSource = new ComboPooledDataSource();
//            dataSource.setProperties(properties);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create connection pool");
        }
    }

    /**
     * Returns a handle to the connection pool (singleton).
     *
     * @return handle o the connection pool
     */
    public static ConnectionPool getInstance() {
        return INSTANCE;
    }

    /**
     * Returns an open connection t the underlying data source from
     * the connection pool.
     *
     * @return connection to the underlying data source
     */
    public Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to create connection");
        }
    }
}
