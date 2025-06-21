package org.bcf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleExporter {
    private static Connection connection;

    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            String url = "jdbc:oracle:thin:@your-host:port:service";
            String user = "your_user";
            String password = "your_password";

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);
        }
        return connection;
    }

    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
