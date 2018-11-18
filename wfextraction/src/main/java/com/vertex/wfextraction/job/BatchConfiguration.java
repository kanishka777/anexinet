package com.vertex.wfextraction.job;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.vertex.wfextraction.domain.Vertex;
import com.vertex.wfextraction.processor.ExtractItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    @Qualifier("writer")
    public MultiResourceItemWriter<Vertex> writer;
    
    @Autowired
    @Qualifier("reader")
    public JdbcCursorItemReader<Vertex> reader;

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }
    
    @Bean
    public ExtractItemProcessor processor() {
        return new ExtractItemProcessor();
    }

    @Bean
    public MultiResourceItemWriter<Vertex> writer(DataSource dataSource) {
    	return writer; 
    }

    
    @Bean
    public Job wideFileExtractJob(Step step) {
        return jobBuilderFactory.get("wideFileExtractJob")
            .incrementer(new RunIdIncrementer())
            .flow(step)
            .end()
            .build();
    }

    @Bean
    public Step wideFileExtractStep(MultiResourceItemWriter<Vertex> writer) {
        return stepBuilderFactory.get("wideFileExtractStep")
            .<Vertex, Vertex> chunk(1)
            .reader(reader)
            .processor(processor())
            .writer(writer)
            .build();
    }
    // end::jobstep[]
}
