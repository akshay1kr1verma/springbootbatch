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
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Spring Batch Configuration Class.
 * Configures an end-to-end workflow consisting of two main steps:
 * 1. Step 1: Reads a local CSV file, processes records, and writes them to a database.
 * 2. Step 2: Reads data back from the database and exports it cleanly to an output CSV file.
 */
@Configuration
public class BatchConfiguration {

    // ==========================================
    // STEP 1 BEANS: IMPORT FROM CSV TO DB
    // ==========================================

    /**
     * Configures the reader for Step 1.
     * Reads line-by-line from a CSV located in the application resources path.
     *
     * @return A FlatFileItemReader that parses CSV lines into Person objects.
     */
    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader") // Assigns a unique name used for execution tracking
                .resource(new ClassPathResource("sample-data.csv")) // Source CSV inside src/main/resources
                .delimited() // Indicates comma-delimited columns
                .names("firstName", "lastName") // Matches fields sequentially with columns in the CSV
                .targetType(Person.class) // Maps columns dynamically to our Person record
                .build();
    }

    /**
     * Instantiates the custom processor component for Step 1.
     * Business logic and data transformation (e.g., uppercase conversions) happen here.
     *
     * @return An instance of PersonItemProcessor.
     */
    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    /**
     * Configures the database writer for Step 1.
     * Batches processed records and flushes them to the data store using named parameters.
     *
     * @param dataSource The active application database connection profile.
     * @return A JdbcBatchItemWriter optimized for high-throughput batch SQL execution.
     */
    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                // Named parameters match property names inside the Person object
                .sql("INSERT into people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .beanMapped() // Automates property-to-named-parameter matching
                .build();
    }

    /**
     * Assembles the structural pipeline for Step 1.
     * Links the Reader, Processor, and Writer inside a transaction-managed chunk block.
     *
     * @param jobRepository Tracks step execution state, metrics, and lifecycle events.
     * @param transactionManager Encapsulates database transactional bounds per chunk.
     * @param reader Step 1 CSV file reader bean dependency.
     * @param processor Step 1 business logic component dependency.
     * @param writer Step 1 database target writer bean dependency.
     * @return A structured, executable Step configuration.
     */
    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      FlatFileItemReader<Person> reader,
                      PersonItemProcessor processor,
                      JdbcBatchItemWriter<Person> writer) {
        return new StepBuilder("step1", jobRepository)
                .<Person, Person>chunk(3) // Defines generic chunk data types and chunks execution to 3 items per transaction
                .transactionManager(transactionManager) // Attaches Spring platform transaction boundaries
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    // ==========================================
    // STEP 2 BEANS: EXPORT FROM DB TO CSV
    // ==========================================

    /**
     * Configures the reader for Step 2.
     * Streams rows from a database table sequentially using a cursor-driven connection.
     *
     * @param dataSource The active application database connection profile.
     * @return A cursor-driven JDBC reader configured to pull data directly into Person objects.
     */
    @Bean
    public JdbcCursorItemReader<Person> sqlReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Person>()
                .name("sqlUserReader") // Unique execution tracking name
                .dataSource(dataSource)
                .sql("SELECT first_name, last_name FROM people") // SQL extraction statement
                // DataClassRowMapper accurately maps snake_case database rows directly to immutable Java records
                .rowMapper(new DataClassRowMapper<>(Person.class))
                .build();
    }

    /**
     * Configures the file writer for Step 2.
     * Recreates or appends to a comma-separated flat file stored in the local resource directory.
     *
     * @return A FlatFileItemWriter targeting a physical local workspace file path.
     */
    @Bean
    public FlatFileItemWriter<Person> csvWriter() {
        return new FlatFileItemWriterBuilder<Person>()
                .name("csvUserWriter") // Unique tracking metadata identification name
                // Routes output back into the source code development tree directory structure
                .resource(new FileSystemResource("src/main/resources/outputs/people.csv"))
                .delimited() // Dictates structured data row isolation styling
                .delimiter(",") // Splits data fields using standard commas
                .names("firstName", "lastName") // Specifies reflection fields to extract out of incoming records
                .headerCallback(w -> w.write("First Name,Last Name")) // Prepends custom structural header row to the top of the file
                .build();
    }

    /**
     * Assembles the structural pipeline for Step 2.
     * Bridges the cursor-driven Database Reader directly with the CSV File Writer.
     *
     * @param jobRepository Tracks step execution state, metrics, and lifecycle events.
     * @param transactionManager Encapsulates file stream transactional limits per chunk.
     * @param sqlReader Step 2 streaming database data source extractor.
     * @param csvWriter Step 2 target file filesystem writer bean.
     * @return A structured, executable Step configuration.
     */
    @Bean
    public Step step2(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      JdbcCursorItemReader<Person> sqlReader,
                      FlatFileItemWriter<Person> csvWriter) {
        return new StepBuilder("step2", jobRepository)
                .<Person, Person>chunk(100) // Processes records in larger blocks of 100 to reduce filesystem IO overhead
                .transactionManager(transactionManager)
                .reader(sqlReader)
                .writer(csvWriter)
                .build();
    }

    // ==========================================
    // MAIN JOB: SEQUENTIAL STEP 1 -> STEP 2
    // ==========================================

    /**
     * orchestrates the master execution workflow sequence.
     * Chains Step 1 and Step 2 to execute sequentially as a unified atomic task.
     *
     * @param jobRepository Root framework orchestrator managing overall execution metadata history.
     * @param step1 CSV-to-DB importation step component.
     * @param step2 DB-to-CSV extraction step component.
     * @param listener Interceptor hook executing lifecycle reporting code (e.g., job performance stats).
     * @return The comprehensive execution-ready composite Job.
     */
    @Bean
    public Job combinedJob(JobRepository jobRepository,
                           Step step1,
                           Step step2,
                           JobCompletionNotificationListener listener) {
        return new JobBuilder("combinedJob", jobRepository)
                .listener(listener) // Binds lifecycle monitoring alerts
                .start(step1) // Sets Step 1 as the workflow entry point
                .next(step2) // Conditionally targets Step 2 to execute immediately upon successful completion of Step 1
                .build();
    }
}
