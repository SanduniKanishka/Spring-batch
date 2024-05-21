package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.partition.ColumnRangePartition;
import com.javatechie.spring.batch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {

    private CustomerWriter customerWriter;

    //Read the CSV file
    @Bean
    public FlatFileItemReader<Customer> reader(){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob", "age");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    //Process CSV file
    @Bean
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }

    //Populate the information to the database
//    @Bean
//    public RepositoryItemWriter<Customer> writer() {
//        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }

    @Bean
    public ColumnRangePartition columnRangePartition(){
        return new ColumnRangePartition();
    }

    @Bean
    public PartitionHandler partitionHandler(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(2);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep(jobRepository, transactionManager));
        return taskExecutorPartitionHandler;
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {

        Step step = new StepBuilder("slaveStep", jobRepository)
                .<Customer, Customer> chunk(10,transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter)
                .faultTolerant()
//                .skipLimit(100)
//                .skip(NumberFormatException.class)
//                .noSkip(IllegalArgumentException.class)
                .skipPolicy(skipPolicy())
                .build();
        return step;
//        return new StepBuilder("slaveStep", jobRepository)
//                .<Customer, Customer> chunk(10,transactionManager)
//                .reader(reader())
//                .processor(processor())
//                .writer(customerWriter)
//                .build();

    }

    @Bean
    public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, Partitioner partitioner) throws Exception{

        Step step = new StepBuilder("masterStep", jobRepository)
                .partitioner(slaveStep(jobRepository,transactionManager))
                .partitioner("slaveStep", partitioner)
                .partitionHandler(partitionHandler(jobRepository, transactionManager))
                .build();
        return step;
//        return new StepBuilder("masterStep", jobRepository)
//                .partitioner(slaveStep(jobRepository,transactionManager))
//                .partitionHandler(partitionHandler(jobRepository, transactionManager))
//                .build();

    }

    @Bean
    public Job runJob(JobRepository jobRepository, PlatformTransactionManager transactionManager, Partitioner partitioner) throws Exception {
        return new JobBuilder("importUserJob", jobRepository).flow(masterStep(jobRepository, transactionManager, partitioner)).end().build();
//        return job

    }

    @Bean
    public SkipPolicy skipPolicy(){
        return new ExceptionSkipPolicy();
    }
    //Execute the job concurrently
    @Bean
    public TaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }


}
