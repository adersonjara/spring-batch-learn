package com.example.config;

import com.example.listener.FirstJobListener;
import com.example.listener.FirstStepListener;
import com.example.model.*;
import com.example.processor.FirstItemProcessor;
import com.example.reader.FirstItemReader;
import com.example.service.FirstTask;
import com.example.service.SecondTask;
import com.example.writer.FirstItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;

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

	@Autowired
	private FirstStepListener firstStepListener;

	@Autowired
	private FirstItemReader firstItemReader;

	@Autowired
	private FirstItemProcessor firstItemProcessor;

	@Autowired
	private FirstItemWriter firstItemWriter;
	/*
	@Autowired
	private StudentService studentService;*/

	@Bean
	@Primary
	@ConfigurationProperties(prefix = "spring.datasource")
	public DataSource dataSource(){
		return DataSourceBuilder.create().build();
	}

	@Bean
	@ConfigurationProperties(prefix = "spring.universitydatasource")
	public DataSource universityDataSource(){
		return DataSourceBuilder.create().build();
	}
	
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
		.listener(firstStepListener)
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

	@Bean
	public Job secondJob(){
		return jobBuilderFactory.get("Second Job")
				.incrementer(new RunIdIncrementer())
				.start(firstChunkStep())
				.build();
	}

	private Step firstChunkStep() {
		return stepBuilderFactory.get("first Chunk Step")
				.<StudentCsv,StudentCsv>chunk(3)
				.reader(flatFileItemReader(null))
				//.reader(jsonItemReader(null))
				//.reader(studentXmlStaxEventItemReader(null))
				//.reader(jdbcJdbcCursorItemReader())
				//.reader(itemReaderAdapter())
				//.processor(firstItemProcessor)
				//.writer(firstItemWriter)
				//.writer(flatFileItemWriter(null))
				//.writer(jsonFileItemWriter(null))
				//.writer(staxEventItemReader(null))
				.writer(jdbcJdbcBatchItemWriter())
				.build();
	}

	@StepScope
	@Bean
	public FlatFileItemReader<StudentCsv> flatFileItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource){
		FlatFileItemReader<StudentCsv> flatFileItemReader =
				new FlatFileItemReader<StudentCsv>();

		flatFileItemReader.setResource(fileSystemResource);

		/*flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>(){
			{
				setLineTokenizer(new DelimitedLineTokenizer("|"){
					{
						setNames("ID","First Name","Last Name","Email");
						// otra forma de definir el delimitador es esta
						// setDelimiter("|");
					}
				});

				setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>(){
					{
						setTargetType(StudentCsv.class);
					}
				});
			}
		});*/

		DefaultLineMapper<StudentCsv> defaultLineMapper =
				new DefaultLineMapper<StudentCsv>();

		DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
		delimitedLineTokenizer.setNames("ID","First Name","Last Name","Email");

		defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);

		BeanWrapperFieldSetMapper<StudentCsv> fieldSetMapper =
				new BeanWrapperFieldSetMapper<StudentCsv>();
		fieldSetMapper.setTargetType(StudentCsv.class);

		defaultLineMapper.setFieldSetMapper(fieldSetMapper);

		flatFileItemReader.setLineMapper(defaultLineMapper);

		flatFileItemReader.setLinesToSkip(1);

		return flatFileItemReader;

	}

	@StepScope
	@Bean
	public JsonItemReader<StudentJson> jsonItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource
	){
		JsonItemReader<StudentJson> jsonItemReader =
				new JsonItemReader<StudentJson>();

		jsonItemReader.setResource(fileSystemResource);
		jsonItemReader.setJsonObjectReader(
				new JacksonJsonObjectReader<>(StudentJson.class)
		);

		return jsonItemReader;
	}

	@StepScope
	@Bean
	public StaxEventItemReader<StudentXml> studentXmlStaxEventItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource
	){
		StaxEventItemReader<StudentXml> staxEventItemReader =
				new StaxEventItemReader<StudentXml>();

		staxEventItemReader.setResource(fileSystemResource);
		staxEventItemReader.setFragmentRootElementName("student");
		staxEventItemReader.setUnmarshaller(new Jaxb2Marshaller(){
			{
				setClassesToBeBound(StudentXml.class);
			}
		});

		return staxEventItemReader;
	}

	public JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader(){
		JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader =
				new JdbcCursorItemReader<StudentJdbc>();

		jdbcJdbcCursorItemReader.setDataSource(universityDataSource());
		jdbcJdbcCursorItemReader.setSql(
				"SELECT id, first_name as firstName,last_name as lastName," +
						"email from student"
		);

		jdbcJdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<StudentJdbc>(){
			{
				setMappedClass(StudentJdbc.class);
			}
		});

		return jdbcJdbcCursorItemReader;

	}

	/*
	public ItemReaderAdapter<StudentResponse> itemReaderAdapter(){
		ItemReaderAdapter<StudentResponse> itemReaderAdapter =
				new ItemReaderAdapter<StudentResponse>();

		itemReaderAdapter.setTargetObject(studentService);
		itemReaderAdapter.setTargetMethod("getStudent");
		itemReaderAdapter.setArguments(new Object[]{1L, "Test"});

		return itemReaderAdapter;
	}
	 */

	@StepScope
	@Bean
	public FlatFileItemWriter<StudentJdbc> flatFileItemWriter(
			@Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource
	){
		FlatFileItemWriter<StudentJdbc> flatFileItemWriter =
				new FlatFileItemWriter<StudentJdbc>();

		flatFileItemWriter.setResource(fileSystemResource);

		flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
			@Override
			public void writeHeader(Writer writer) throws IOException {
				writer.write("Id,First Name,Last Name,Email");
			}
		});

		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJdbc>(){
			{
				setFieldExtractor(new BeanWrapperFieldExtractor<StudentJdbc>(){
					{
						setNames(new String[]{"id","firstName","lastName","email"});
					}
				});
			}
		});

		flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
			@Override
			public void writeFooter(Writer writer) throws IOException {
				writer.write("Created @ "+new Date());
			}
		});

		return flatFileItemWriter;
	}

	@StepScope
	@Bean
	public JsonFileItemWriter<StudentJson> jsonFileItemWriter(
			@Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource
	){
		JsonFileItemWriter<StudentJson> jsonFileItemWriter =
				new JsonFileItemWriter<>(fileSystemResource,new JacksonJsonObjectMarshaller<StudentJson>());
		return jsonFileItemWriter;
	}

	@StepScope
	@Bean
	public StaxEventItemWriter<StudentJdbc> staxEventItemReader(
			@Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource
	){
		StaxEventItemWriter<StudentJdbc> staxEventItemReader =
				new StaxEventItemWriter<StudentJdbc>();

		staxEventItemReader.setResource(fileSystemResource);
		staxEventItemReader.setRootTagName("students");

		staxEventItemReader.setMarshaller(new Jaxb2Marshaller(){
			{
				setClassesToBeBound(StudentJdbc.class);
			}
		});

		return staxEventItemReader;
	}

	@Bean
	public JdbcBatchItemWriter<StudentCsv> jdbcJdbcBatchItemWriter(){
		JdbcBatchItemWriter<StudentCsv> jdbcJdbcBatchItemWriter =
				new JdbcBatchItemWriter<StudentCsv>();

		jdbcJdbcBatchItemWriter.setDataSource(universityDataSource());
		jdbcJdbcBatchItemWriter.setSql(
				"insert into student(id,first_name,last_name,email) " +
						"values(:id,:firstName,:lastName,:email)"
		);

		jdbcJdbcBatchItemWriter.setItemSqlParameterSourceProvider(
				new BeanPropertyItemSqlParameterSourceProvider<StudentCsv>());

		return jdbcJdbcBatchItemWriter;
	}
}
