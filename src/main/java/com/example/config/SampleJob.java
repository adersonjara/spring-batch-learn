package com.example.config;

import com.example.listener.FirstJobListener;
import com.example.service.FirstTask;
import com.example.service.SecondTask;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SampleJob {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private FirstTask firstTask;

	@Autowired
	private SecondTask secondTask;

	@Autowired
	private FirstJobListener firstJobListener;
	
	@Bean
	public Job firstJob() {
		return jobBuilderFactory.get("First Job")
		.incrementer(new RunIdIncrementer())
		.start(firstStep())
		.next(secondStep())
		.listener(firstJobListener)
		.build();
	}
	
	private Step firstStep() {
		return stepBuilderFactory.get("First Step")
		.tasklet(firstTask)
		.build();
	}
	/*
	private Tasklet firstTask() {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
				System.out.println("This is first tasklet step ---------------");
				return RepeatStatus.FINISHED;
			}
		};
	}
	 */
	private Step secondStep() {
		return stepBuilderFactory.get("Second Step")
				.tasklet(secondTask)
				.build();
	}
	/*
	private Tasklet secondTask() {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
				System.out.println("This is second tasklet step ---------------");
				return RepeatStatus.FINISHED;
			}
		};
	}
 	*/
}
