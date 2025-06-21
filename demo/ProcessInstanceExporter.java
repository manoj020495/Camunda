package org.bcf;

import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ProcessInstanceExporter implements Exporter {
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
        if (record.getValueType() == ValueType.PROCESS_INSTANCE) {
            ProcessInstanceRecordValue value = (ProcessInstanceRecordValue) record.getValue();
            String intent = record.getIntent().name();
            Timestamp now = new Timestamp(record.getTimestamp());

            try (PreparedStatement stmt = connection.prepareStatement(
                "MERGE INTO PROCESS_INSTANCE_MERGED t " +
                "USING (SELECT ? AS PROCESS_INSTANCE_KEY FROM dual) s " +
                "ON (t.PROCESS_INSTANCE_KEY = s.PROCESS_INSTANCE_KEY) " +
                "WHEN MATCHED THEN UPDATE SET STATE = ?, END_TIME = ? " +
                "WHEN NOT MATCHED THEN INSERT (PROCESS_INSTANCE_KEY, PROCESS_DEFINITION_ID, STATE, START_TIME) " +
                "VALUES (?, ?, ?, ?)")) {

                boolean isFinal = intent.equals("ELEMENT_COMPLETED") || intent.equals("ELEMENT_TERMINATED");

                stmt.setLong(1, record.getKey());
                stmt.setString(2, intent);
                stmt.setTimestamp(3, isFinal ? now : null);
                stmt.setLong(4, record.getKey());
                stmt.setString(5, value.getBpmnProcessId());
                stmt.setString(6, intent);
                stmt.setTimestamp(7, !isFinal ? now : null);

                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
