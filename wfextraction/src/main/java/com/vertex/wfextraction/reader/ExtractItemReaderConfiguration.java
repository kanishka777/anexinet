package com.vertex.wfextraction.reader;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.vertex.wfextraction.domain.ColumnHelper;
import com.vertex.wfextraction.utils.ExtractionQueryBuilder;

@Component
public class ExtractItemReaderConfiguration {

	private final String sql = "select * from RDBLineItem";

	@Autowired
	public ExtractionRowMapper mapper;

	@Bean
	public JdbcCursorItemReader<String> getItemReader(DataSource ds, ColumnHelper helper) {
		JdbcCursorItemReader<String> reader = new JdbcCursorItemReader<String>();
		reader.setDataSource(ds);
		String sql = constructSql(helper);
		reader.setSql(sql);
		System.out.println("The SQL generated is " + sql);
		reader.setRowMapper(mapper);
		// reader.setRowMapper(new BeanPropertyRowMapper<>(Vertex.class));
		return reader;
	}

	private String constructSql(ColumnHelper helper) {
		String sql = ExtractionQueryBuilder.getQuery(helper, "8.0");
		return sql;
	}

}
