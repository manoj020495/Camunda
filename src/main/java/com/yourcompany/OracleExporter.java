package com.yourcompany;

import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;

import java.sql.*;

public class OracleExporter implements Exporter {

    private Connection connection;

    @Override
    public void configure(Context context) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521:XE"; // 🔧 Update with your actual DB URL
            String user = "your_user";                           // 🔧 Your DB username
            String password = "your_password";                   // 🔧 Your DB password

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException("Oracle DB connection failed", e);
        }
    }

    @Override
    public void export(Record<?> record) {
        if (record.getValue() instanceof ProcessInstanceRecordValue processInstance) {
            try (PreparedStatement stmt = connection.prepareStatement(
                    "INSERT INTO PROCESS_INSTANCE (" +
                            "PROCESS_INSTANCE_KEY, PROCESS_DEFINITION_ID, PROCESS_DEFINITION_KEY, " +
                            "STATE, START_DATE, TENANT_ID, PARENT_PROCESS_INSTANCE_KEY, " +
                            "PARENT_ELEMENT_INSTANCE_KEY, VERSION, PARTITION_ID, TREE_PATH) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

                stmt.setLong(1, record.getKey());
                stmt.setString(2, processInstance.getBpmnProcessId());
                stmt.setLong(3, processInstance.getProcessDefinitionKey());
                stmt.setString(4, record.getIntent().name());
                stmt.setTimestamp(5, new Timestamp(record.getTimestamp()));
                stmt.setString(6, processInstance.getTenantId());
                stmt.setLong(7, processInstance.getParentProcessInstanceKey());
                stmt.setLong(8, processInstance.getParentElementInstanceKey());
                stmt.setInt(9, processInstance.getVersion());
                stmt.setInt(10, record.getPartitionId());
                stmt.setString(11, "N/A"); // You can put structured tree path here later

                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
