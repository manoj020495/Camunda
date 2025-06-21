package com.yourcompany;

import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.ExporterDescriptor;
import io.camunda.zeebe.exporter.api.ExporterInit;
import io.camunda.zeebe.protocol.record.Record;

import java.sql.*;

public class OracleExporter implements Exporter {

    private Connection connection;

    @ExporterInit
    public void init(Context context) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521:XE";
            String user = "your_user";
            String password = "your_password";

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);

            context.setExporterDescriptor(
                ExporterDescriptor.newBuilder()
                    .name("oracle-exporter")
                    .build()
            );
        } catch (SQLException e) {
            throw new RuntimeException("Oracle connection failed", e);
        }
    }

    @Override
    public void export(Record<?> record) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO ZEEBE_EVENTS (ID, VALUE_TYPE, INTENT, TIMESTAMP) VALUES (?, ?, ?, ?)")) {
            stmt.setLong(1, record.getPosition());
            stmt.setString(2, record.getValueType().name());
            stmt.setString(3, record.getIntent().name());
            stmt.setTimestamp(4, new Timestamp(record.getTimestamp()));
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
