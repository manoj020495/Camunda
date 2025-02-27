package com.example.demo;

import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScriptTaskWorker implements JobHandler {

   private static final Logger LOG = LoggerFactory.getLogger(ScriptTaskWorker.class);

   @Override
   public void handle(JobClient client, ActivatedJob job) {
       LOG.info("Executing Script Task with variables: {}", job.getVariables());

       // Completing the job explicitly
       client.newCompleteCommand(job.getKey()).send().join();
   }
}