package com.vertex.wfextraction.writer;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import com.vertex.wfextraction.domain.Column;
import com.vertex.wfextraction.domain.ColumnHelper;
import com.vertex.wfextraction.domain.Vertex;

@Component
public class ExtractItemWriterConfiguration {

	@Autowired
	public ColumnHelper helper;

	public FlatFileItemWriter<String> createCSVWriter() {
		FlatFileItemWriter<String> writer = new FlatFileItemWriter<>();
		String header = createCSVHeader(helper.getColumnList());
		StringHeaderWriter headerWriter = new StringHeaderWriter(header);
//		StringHeaderWriter headerWriter = new StringHeaderWriter("sourceid~inputextendedprice");
		writer.setHeaderCallback(headerWriter);
		writer.setLineAggregator(new PassThroughLineAggregator<>());
		// writer.setResource(new FileSystemResource("c:/temp/abc.csv"));
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

	@Bean
	public MultiResourceItemWriter<String> multiWriter() {
		MultiResourceItemWriter<String> multiWriter = new MultiResourceItemWriter<>();
		multiWriter.setResourceSuffixCreator(customSuffixCreator());
		multiWriter.setDelegate(createCSVWriter());
		multiWriter.setItemCountLimitPerResource(25);
		multiWriter.setResource(new FileSystemResource("c:/temp/widefile"));
		return multiWriter;
	}

	private ResourceSuffixCreator customSuffixCreator() {
		return new ResourceSuffixCreator() {

			@Override
			public String getSuffix(int index) {
				return String.valueOf("_" + index + ".csv");
			}
		};
	}

	private String createCSVHeader(List<Column> columns) {
		List<String> headers = new ArrayList<String>();
		columns.forEach(column -> {
			headers.add(column.getName());
		});

		String header = String.join("~", headers);
		return header;

	}

}
