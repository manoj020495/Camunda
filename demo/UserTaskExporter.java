package org.bcf;

import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.UserTaskRecordValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class UserTaskExporter implements Exporter {
    private Connection connection;

    @Override
    public void configure(Context context) {
        try {
            connection = OracleExporter.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Oracle DB connection failed", e);
        }
    }

    @Override
    public void export(Record<?> record) {
        if (record.getValueType() == ValueType.USER_TASK) {
            UserTaskRecordValue value = (UserTaskRecordValue) record.getValue();
            String intent = record.getIntent().name();
            Timestamp now = new Timestamp(record.getTimestamp());

            try (PreparedStatement stmt = connection.prepareStatement(
                "MERGE INTO USER_TASK_MERGED t " +
                "USING (SELECT ? AS ELEMENT_INSTANCE_KEY FROM dual) s " +
                "ON (t.ELEMENT_INSTANCE_KEY = s.ELEMENT_INSTANCE_KEY) " +
                "WHEN MATCHED THEN UPDATE SET STATE = ?, COMPLETE_TIME = ? " +
                "WHEN NOT MATCHED THEN INSERT (ELEMENT_INSTANCE_KEY, PROCESS_INSTANCE_KEY, STATE, ASSIGNEE, START_TIME) " +
                "VALUES (?, ?, ?, ?, ?)")) {

                boolean isFinal = intent.equals("COMPLETED") || intent.equals("TERMINATED") || intent.equals("CANCELLED");

                stmt.setLong(1, record.getKey());
                stmt.setString(2, intent);
                stmt.setTimestamp(3, isFinal ? now : null);
                stmt.setLong(4, record.getKey());
                stmt.setLong(5, value.getProcessInstanceKey());
                stmt.setString(6, intent);
                stmt.setString(7, value.getAssignee());
                stmt.setTimestamp(8, !isFinal ? now : null);

                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
