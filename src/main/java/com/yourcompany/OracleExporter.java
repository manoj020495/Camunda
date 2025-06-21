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
        else if (record.getValue() instanceof UserTaskRecordValue userTask) {
    try (PreparedStatement stmt = connection.prepareStatement(
            "MERGE INTO USER_TASK tgt USING (SELECT ? AS USER_TASK_KEY FROM dual) src " +
            "ON (tgt.USER_TASK_KEY = src.USER_TASK_KEY) " +
            "WHEN MATCHED THEN UPDATE SET " +
            "    ELEMENT_ID = ?, NAME = ?, PROCESS_DEFINITION_ID = ?, CREATION_DATE = ?, " +
            "    COMPLETION_DATE = ?, STATE = ?, ASSIGNEE = ?, FORM_KEY = ?, " +
            "    PROCESS_DEFINITION_KEY = ?, PROCESS_INSTANCE_KEY = ?, ELEMENT_INSTANCE_KEY = ?, " +
            "    TENANT_ID = ?, DUE_DATE = ?, FOLLOW_UP_DATE = ?, CANDIDATE_GROUPS = ?, " +
            "    CANDIDATE_USERS = ?, EXTERNAL_FORM_REFERENCE = ?, PROCESS_DEFINITION_VERSION = ?, " +
            "    CUSTOM_HEADERS = ?, PRIORITY = ?, PARTITION_ID = ? " +
            "WHEN NOT MATCHED THEN INSERT (" +
            "    USER_TASK_KEY, ELEMENT_ID, NAME, PROCESS_DEFINITION_ID, CREATION_DATE, " +
            "    COMPLETION_DATE, STATE, ASSIGNEE, FORM_KEY, PROCESS_DEFINITION_KEY, " +
            "    PROCESS_INSTANCE_KEY, ELEMENT_INSTANCE_KEY, TENANT_ID, DUE_DATE, FOLLOW_UP_DATE, " +
            "    CANDIDATE_GROUPS, CANDIDATE_USERS, EXTERNAL_FORM_REFERENCE, PROCESS_DEFINITION_VERSION, " +
            "    CUSTOM_HEADERS, PRIORITY, PARTITION_ID) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

        stmt.setLong(1, record.getKey()); // USER_TASK_KEY
        stmt.setString(2, userTask.getElementId());
        stmt.setString(3, userTask.getName());
        stmt.setString(4, userTask.getBpmnProcessId());
        stmt.setTimestamp(5, new Timestamp(record.getTimestamp())); // CREATION_DATE
        stmt.setTimestamp(6, null); // COMPLETION_DATE (can set on COMPLETED intent)
        stmt.setString(7, record.getIntent().name()); // STATE
        stmt.setString(8, userTask.getAssignee());
        stmt.setString(9, userTask.getFormKey());
        stmt.setLong(10, userTask.getProcessDefinitionKey());
        stmt.setLong(11, userTask.getProcessInstanceKey());
        stmt.setLong(12, userTask.getElementInstanceKey());
        stmt.setString(13, userTask.getTenantId());
        stmt.setTimestamp(14, null); // DUE_DATE
        stmt.setTimestamp(15, null); // FOLLOW_UP_DATE
        stmt.setString(16, userTask.getCandidateGroups().toString());
        stmt.setString(17, userTask.getCandidateUsers().toString());
        stmt.setString(18, userTask.getExternalFormReference());
        stmt.setInt(19, userTask.getProcessDefinitionVersion());
        stmt.setString(20, userTask.getCustomHeaders().toString());
        stmt.setInt(21, userTask.getPriority());
        stmt.setInt(22, record.getPartitionId());

        // Bind again for insert part
        stmt.setLong(23, record.getKey()); // USER_TASK_KEY
        stmt.setString(24, userTask.getElementId());
        stmt.setString(25, userTask.getName());
        stmt.setString(26, userTask.getBpmnProcessId());
        stmt.setTimestamp(27, new Timestamp(record.getTimestamp())); // CREATION_DATE
        stmt.setTimestamp(28, null); // COMPLETION_DATE
        stmt.setString(29, record.getIntent().name()); // STATE
        stmt.setString(30, userTask.getAssignee());
        stmt.setString(31, userTask.getFormKey());
        stmt.setLong(32, userTask.getProcessDefinitionKey());
        stmt.setLong(33, userTask.getProcessInstanceKey());
        stmt.setLong(34, userTask.getElementInstanceKey());
        stmt.setString(35, userTask.getTenantId());
        stmt.setTimestamp(36, null); // DUE_DATE
        stmt.setTimestamp(37, null); // FOLLOW_UP_DATE
        stmt.setString(38, userTask.getCandidateGroups().toString());
        stmt.setString(39, userTask.getCandidateUsers().toString());
        stmt.setString(40, userTask.getExternalFormReference());
        stmt.setInt(41, userTask.getProcessDefinitionVersion());
        stmt.setString(42, userTask.getCustomHeaders().toString());
        stmt.setInt(43, userTask.getPriority());
        stmt.setInt(44, record.getPartitionId());

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
