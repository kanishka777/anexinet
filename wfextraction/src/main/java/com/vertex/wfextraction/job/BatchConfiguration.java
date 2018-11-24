package com.vertex.wfextraction.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.vertex.wfextraction.processor.ExtractItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public MultiResourceItemWriter<String> writer;

	@Autowired
	public JdbcCursorItemReader<String> reader;

	@Bean
	public PlatformTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	@Bean
	public ExtractItemProcessor processor() {
		return new ExtractItemProcessor();
	}

	@Bean
	public Job wideFileExtractJob(Step step) {
		return jobBuilderFactory.get("wideFileExtractJob").incrementer(new RunIdIncrementer()).flow(step).end().build();
	}

	@Bean
	public Step wideFileExtractStep(MultiResourceItemWriter<String> writer) {
		return stepBuilderFactory.get("wideFileExtractStep").<String, String>chunk(1).reader(reader)
				.processor(processor()).writer(writer).build();
	}
	// end::jobstep[]
}
