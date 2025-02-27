package com.example.demo;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.ZeebeClientBuilder;
import io.camunda.zeebe.client.api.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZeebeClientConfig {

	private static final Logger LOG = LoggerFactory.getLogger(ZeebeClientConfig.class);

    @Bean
    ZeebeClientBuilder zeebeClientBuilder() {
		LOG.info("Creating ZeebeClientBuilder bean");
		return ZeebeClient.newClientBuilder().gatewayAddress("127.0.0.1:26500").usePlaintext();
	}

    @Bean
    JobWorker scriptTaskWorker(ZeebeClient client) {
		LOG.info("Creating scriptTaskWorker bean");
		return client.newWorker().jobType("script-task-type").handler(new ScriptTaskWorker()).open();
	}
}
