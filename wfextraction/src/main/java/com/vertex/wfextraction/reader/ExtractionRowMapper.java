package com.vertex.wfextraction.reader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.json.JsonObject;

import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vertex.wfextraction.domain.Column;
import com.vertex.wfextraction.domain.ColumnHelper;
import com.vertex.wfextraction.utils.DomUtils;

@Component
public class ExtractionRowMapper implements RowMapper<String> {

	@Override
	public String mapRow(ResultSet rs, int rowNum) throws SQLException {
		StringBuilder builder = new StringBuilder();
		int colIndex = 1;
		for (Column col : getColumnHelper().getColumnList()) {
			if (colIndex != 1) {
				builder.append("~");
			}
			colIndex++;
			Object value = rs.getObject(col.getName());
			// Object value = rs.getObject(colIndex++);
			if (value != null) {

//				if (col.isEncode()) {
//					value = Column.encode(value.toString());
//				}

				builder.append(value.toString());
			}
		}
		return builder.toString();
	}

	@Bean
	public ColumnHelper getColumnHelper() {
		ColumnHelper helper = new ColumnHelper();
		JsonObject root = DomUtils.loadJsonDocument("wtj_columns.json");
		List<Column> columns = Column.loadColumns(root);
		helper.setColumnList(columns);
		return helper;
	}

}
