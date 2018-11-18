package com.vertex.wfextraction.writer;

 
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.context.annotation.Bean;
 import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import com.vertex.wfextraction.domain.Vertex;

@Component
public class ExtractItemWriterConfiguration {

	public FlatFileItemWriter<Vertex> createCSVWriter() {
		FlatFileItemWriter<Vertex> writer = new FlatFileItemWriter<>();
	    StringHeaderWriter headerWriter = new StringHeaderWriter("sourceid~inputextendedprice");
	    writer.setHeaderCallback(headerWriter);
 		writer.setLineAggregator(lineAggreagator());
	//	writer.setResource(new FileSystemResource("c:/temp/abc.csv"));
		return writer;

	}
	
	private LineAggregator<Vertex> lineAggreagator() {
		DelimitedLineAggregator<Vertex> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter("~");
		FieldExtractor<Vertex> fieldExtractor = createStudentFieldExtractor();
		lineAggregator.setFieldExtractor(fieldExtractor);

		return lineAggregator;
	}

	private FieldExtractor<Vertex> createStudentFieldExtractor() {
		BeanWrapperFieldExtractor<Vertex> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] { "sourceId", "inputExtendedPrice" });
		return extractor;
	}
	
	@Bean("writer")
	public MultiResourceItemWriter<Vertex> multiWriter()
	{
		MultiResourceItemWriter<Vertex> multiWriter =new MultiResourceItemWriter<>();
		multiWriter.setResourceSuffixCreator(customSuffixCreator());
		multiWriter.setDelegate(createCSVWriter());
		multiWriter.setItemCountLimitPerResource(1);
		multiWriter.setResource(new FileSystemResource("c:/temp/widefile"));
		return multiWriter;
	}
	
	private ResourceSuffixCreator customSuffixCreator()
	{
		return new ResourceSuffixCreator() {
			
			@Override
			public String getSuffix(int index) {
				return String.valueOf("_"+ index +".csv");
			}
		};
	}
	
	
	
}
