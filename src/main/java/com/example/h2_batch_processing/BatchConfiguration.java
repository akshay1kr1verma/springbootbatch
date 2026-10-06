package com.example.h2_batch_processing;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {

    // ==========================================
    // STEP 1 BEANS: IMPORT FROM CSV TO DB
    // ==========================================

    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .names("firstName", "lastName")
                .targetType(Person.class)
                .build();
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .sql("INSERT into people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      FlatFileItemReader<Person> reader,
                      PersonItemProcessor processor,
                      JdbcBatchItemWriter<Person> writer) {
        return new StepBuilder("step1", jobRepository) // Added step name
                .<Person, Person>chunk(3)
                .transactionManager(transactionManager)// Moved transactionManager inside chunk mapping
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    // ==========================================
    // STEP 2 BEANS: EXPORT FROM DB TO CSV
    // ==========================================

    @Bean
    public JdbcCursorItemReader<Person> sqlReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Person>()
                .name("sqlUserReader")
                .dataSource(dataSource)
                .sql("SELECT first_name, last_name FROM people") // Standard SQL snake_case names
                // Use DataClassRowMapper instead of BeanPropertyRowMapper for Java Records
                .rowMapper(new DataClassRowMapper<>(Person.class))
                .build();
    }

    @Bean
    public FlatFileItemWriter<Person> csvWriter() {
        return new FlatFileItemWriterBuilder<Person>()
                .name("csvUserWriter")
                .resource(new FileSystemResource("outputs/people.csv"))
                .delimited()
                .delimiter(",")
                .names("firstName", "lastName") // Fixed typo: changed 'lastEmail' to 'lastName'
                .headerCallback(w -> w.write("First Name,Last Name"))
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      JdbcCursorItemReader<Person> sqlReader,
                      FlatFileItemWriter<Person> csvWriter) {
        return new StepBuilder("step2", jobRepository) // Renamed to step2
                .<Person, Person>chunk(100)
                .transactionManager(transactionManager)
                .reader(sqlReader)
                .writer(csvWriter)
                .build();
    }


    // ==========================================
    // MAIN JOB: SEQUENTIAL STEP 1 -> STEP 2
    // ==========================================

    @Bean
    public Job combinedJob(JobRepository jobRepository,
                           Step step1,
                           Step step2,
                           JobCompletionNotificationListener listener) {
        return new JobBuilder("combinedJob", jobRepository) // Added job name
                .listener(listener)
                .start(step1)
                .next(step2) // Chains Step 2 to execute right after Step 1 finishes successfully
                .build();
    }
}
