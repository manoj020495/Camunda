package com.example.demo;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LogHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogHandler.class);

    @JobWorker(type = "log-handle", autoComplete = true)
    public void handleLogTask(final ActivatedJob job) {
        LOG.info("Executing Script Task with variables: {}", job.getVariables());
        LOG.info("This is a simple log message from the worker!");
    }
}