package com.neoage;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
public class SpringBootBatchMinimalApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootBatchMinimalApplication.class, args);
    }


    @Bean
    public Job importUserJob(JobRepository jobRepository, Step step) {
        final var jobBuilder = new JobBuilder("importUserJob", jobRepository);
        return jobBuilder.incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        final var stepBuilder = new StepBuilder("step", jobRepository);
        return stepBuilder.<User, User>chunk(6000, transactionManager)
//                .taskExecutor(new VirtualThreadTaskExecutor())
                .taskExecutor(taskExecutor())  // performing better than VirtualThreadTaskExecutor
                .reader(userReader())
                .processor(userProcessor())
                .writer(userWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader<User> userReader() {
        final var itemReaderBuilder = new FlatFileItemReaderBuilder<User>();
        return itemReaderBuilder
                .name("userItemReader")
                .resource(new FileSystemResource("D:/WORKOUT/INTELLIJ_IDEA/2025/spring-boot-batch-demo/users.csv"))
                .delimited()
                .names("id", "name", "email")
                .targetType(User.class)
                .linesToSkip(1)
                .build();
    }

    @Bean
    public ItemProcessor<User, User> userProcessor() {
        return user -> new User(user.id(), user.name().toUpperCase(), user.email().toUpperCase());
    }

    @Bean
    public ItemWriter<User> userWriter() {
        return users -> users.forEach(System.out::println);
    }

    @Bean
    public TaskExecutor taskExecutor() {
        final var asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(6);
        return asyncTaskExecutor;
    }

}
