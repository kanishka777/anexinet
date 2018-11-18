package com.vertex.wfextraction.reader;

import javax.sql.DataSource;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Component;

import com.vertex.wfextraction.domain.Vertex;

 

@Component
public class ExtractItemReaderConfiguration {

 	private final String sql="select * from RDBLineItem";
 	
 	
	@Bean("reader")
	public JdbcCursorItemReader<Vertex> getItemReader(DataSource ds) {
		JdbcCursorItemReader<Vertex> reader=new JdbcCursorItemReader<Vertex>();
		reader.setDataSource(ds);
		reader.setSql(sql);
		reader.setRowMapper(new BeanPropertyRowMapper<>(Vertex.class));
		return reader;
	}


}
