
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.UserTaskRecordValue;

import java.sql.*;

public class DBExporter implements Exporter {

    private Connection connection;

    @Override
    public void configure(Context context) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521:XE"; // Update with your DB URL
            String user = "your_user";                           // Your DB username
            String password = "your_password";                   // Your DB password

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException("Oracle DB connection failed", e);
        }
    }

    @Override
    public void export(Record<?> record) {
        if (record.getValue() instanceof UserTaskRecordValue userTask) {
            try {
                try (PreparedStatement checkStmt = connection.prepareStatement(
                        "SELECT COUNT(*) FROM USER_TASK WHERE USER_TASK_KEY = ?")) {
                    checkStmt.setLong(1, record.getKey());
                    ResultSet rs = checkStmt.executeQuery();
                    rs.next();
                    boolean exists = rs.getInt(1) > 0;

                    if (exists) {
                        try (PreparedStatement stmt = connection.prepareStatement(
                                "UPDATE USER_TASK SET " +
                                        "ELEMENT_ID = ?, NAME = ?, PROCESS_DEFINITION_ID = ?, CREATION_DATE = ?, " +
                                        "COMPLETION_DATE = ?, STATE = ?, ASSIGNEE = ?, FORM_KEY = ?, " +
                                        "PROCESS_DEFINITION_KEY = ?, PROCESS_INSTANCE_KEY = ?, ELEMENT_INSTANCE_KEY = ?, " +
                                        "TENANT_ID = ?, DUE_DATE = ?, FOLLOW_UP_DATE = ?, CANDIDATE_GROUPS = ?, " +
                                        "CANDIDATE_USERS = ?, EXTERNAL_FORM_REFERENCE = ?, PROCESS_DEFINITION_VERSION = ?, " +
                                        "CUSTOM_HEADERS = ?, PRIORITY = ?, PARTITION_ID = ? " +
                                        "WHERE USER_TASK_KEY = ?")) {

                            stmt.setString(1, userTask.getElementId());
                            stmt.setString(2, userTask.getName());
                            stmt.setString(3, userTask.getBpmnProcessId());
                            stmt.setTimestamp(4, new Timestamp(record.getTimestamp()));
                            stmt.setTimestamp(5, record.getIntent().name().equals("COMPLETED")
                                    ? new Timestamp(record.getTimestamp()) : null);
                            stmt.setString(6, record.getIntent().name());
                            stmt.setString(7, userTask.getAssignee());
                            stmt.setString(8, userTask.getFormKey());
                            stmt.setLong(9, userTask.getProcessDefinitionKey());
                            stmt.setLong(10, userTask.getProcessInstanceKey());
                            stmt.setLong(11, userTask.getElementInstanceKey());
                            stmt.setString(12, userTask.getTenantId());
                            stmt.setTimestamp(13, null);
                            stmt.setTimestamp(14, null);
                            stmt.setString(15, userTask.getCandidateGroups().toString());
                            stmt.setString(16, userTask.getCandidateUsers().toString());
                            stmt.setString(17, userTask.getExternalFormReference());
                            stmt.setInt(18, userTask.getProcessDefinitionVersion());
                            stmt.setString(19, userTask.getCustomHeaders().toString());
                            stmt.setInt(20, userTask.getPriority());
                            stmt.setInt(21, record.getPartitionId());
                            stmt.setLong(22, record.getKey());
                            stmt.executeUpdate();
                        }
                    } else {
                        try (PreparedStatement stmt = connection.prepareStatement(
                                "INSERT INTO USER_TASK (" +
                                        "USER_TASK_KEY, ELEMENT_ID, NAME, PROCESS_DEFINITION_ID, CREATION_DATE, " +
                                        "COMPLETION_DATE, STATE, ASSIGNEE, FORM_KEY, PROCESS_DEFINITION_KEY, " +
                                        "PROCESS_INSTANCE_KEY, ELEMENT_INSTANCE_KEY, TENANT_ID, DUE_DATE, FOLLOW_UP_DATE, " +
                                        "CANDIDATE_GROUPS, CANDIDATE_USERS, EXTERNAL_FORM_REFERENCE, PROCESS_DEFINITION_VERSION, " +
                                        "CUSTOM_HEADERS, PRIORITY, PARTITION_ID) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

                            stmt.setLong(1, record.getKey());
                            stmt.setString(2, userTask.getElementId());
                            stmt.setString(3, userTask.getName());
                            stmt.setString(4, userTask.getBpmnProcessId());
                            stmt.setTimestamp(5, new Timestamp(record.getTimestamp()));
                            stmt.setTimestamp(6, record.getIntent().name().equals("COMPLETED")
                                    ? new Timestamp(record.getTimestamp()) : null);
                            stmt.setString(7, record.getIntent().name());
                            stmt.setString(8, userTask.getAssignee());
                            stmt.setString(9, userTask.getFormKey());
                            stmt.setLong(10, userTask.getProcessDefinitionKey());
                            stmt.setLong(11, userTask.getProcessInstanceKey());
                            stmt.setLong(12, userTask.getElementInstanceKey());
                            stmt.setString(13, userTask.getTenantId());
                            stmt.setTimestamp(14, null);
                            stmt.setTimestamp(15, null);
                            stmt.setString(16, userTask.getCandidateGroups().toString());
                            stmt.setString(17, userTask.getCandidateUsers().toString());
                            stmt.setString(18, userTask.getExternalFormReference());
                            stmt.setInt(19, userTask.getProcessDefinitionVersion());
                            stmt.setString(20, userTask.getCustomHeaders().toString());
                            stmt.setInt(21, userTask.getPriority());
                            stmt.setInt(22, record.getPartitionId());
                            stmt.executeUpdate();
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        else if (record.getValue() instanceof ProcessInstanceRecordValue processInstance) {
            try {
                try (PreparedStatement checkStmt = connection.prepareStatement(
                        "SELECT COUNT(*) FROM PROCESS_INSTANCE WHERE PROCESS_INSTANCE_KEY = ?")) {
                    checkStmt.setLong(1, processInstance.getProcessInstanceKey());
                    ResultSet rs = checkStmt.executeQuery();
                    rs.next();
                    boolean exists = rs.getInt(1) > 0;

                    if (exists) {
                        try (PreparedStatement stmt = connection.prepareStatement(
                                "UPDATE PROCESS_INSTANCE SET " +
                                        "PROCESS_DEFINITION_ID = ?, PROCESS_DEFINITION_KEY = ?, STATE = ?, " +
                                        "START_DATE = ?, TENANT_ID = ?, PARENT_PROCESS_INSTANCE_KEY = ?, " +
                                        "PARENT_ELEMENT_INSTANCE_KEY = ?, VERSION = ?, PARTITION_ID = ?, TREE_PATH = ? " +
                                        "WHERE PROCESS_INSTANCE_KEY = ?")) {

                            stmt.setString(1, processInstance.getBpmnProcessId());
                            stmt.setLong(2, processInstance.getProcessDefinitionKey());
                            stmt.setString(3, record.getIntent().name());
                            stmt.setTimestamp(4, new Timestamp(record.getTimestamp()));
                            stmt.setString(5, processInstance.getTenantId());
                            stmt.setLong(6, processInstance.getParentProcessInstanceKey());
                            stmt.setLong(7, processInstance.getParentElementInstanceKey());
                            stmt.setInt(8, processInstance.getVersion());
                            stmt.setInt(9, record.getPartitionId());
                            stmt.setString(10, "N/A");
                            stmt.setLong(11, processInstance.getProcessInstanceKey());
                            stmt.executeUpdate();
                        }
                    } else {
                        try (PreparedStatement stmt = connection.prepareStatement(
                                "INSERT INTO PROCESS_INSTANCE (" +
                                        "PROCESS_INSTANCE_KEY, PROCESS_DEFINITION_ID, PROCESS_DEFINITION_KEY, STATE, " +
                                        "START_DATE, TENANT_ID, PARENT_PROCESS_INSTANCE_KEY, PARENT_ELEMENT_INSTANCE_KEY, " +
                                        "VERSION, PARTITION_ID, TREE_PATH) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

                            stmt.setLong(1, processInstance.getProcessInstanceKey());
                            stmt.setString(2, processInstance.getBpmnProcessId());
                            stmt.setLong(3, processInstance.getProcessDefinitionKey());
                            stmt.setString(4, record.getIntent().name());
                            stmt.setTimestamp(5, new Timestamp(record.getTimestamp()));
                            stmt.setString(6, processInstance.getTenantId());
                            stmt.setLong(7, processInstance.getParentProcessInstanceKey());
                            stmt.setLong(8, processInstance.getParentElementInstanceKey());
                            stmt.setInt(9, processInstance.getVersion());
                            stmt.setInt(10, record.getPartitionId());
                            stmt.setString(11, "N/A");
                            stmt.executeUpdate();
                        }
                    }
                }
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
