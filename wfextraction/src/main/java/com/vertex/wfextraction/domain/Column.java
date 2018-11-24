package com.vertex.wfextraction.domain;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.UUID;

import javax.json.JsonArray;
import javax.json.JsonObject;

import com.vertex.wfextraction.utils.JsonUtils;

public class Column {

	private String name;
	private DataType type;
	private Set<String> values;
	private int nulls;
	private int nonNulls;
	private long totalLength;
	private int avgLength = -1;

	private int nullRate;
	private List<String> options;
	private boolean weightedOptions;
	private String pattern;
	private int minId;
	private int maxId;
	private boolean uuid;
	private int repeatPct;
	private boolean random;
	private int minDate;
	private int maxDate;
	private double minAmount;
	private double maxAmount;
	private String queryCol;
	private Map<String, String> queriesByVersion;
	private boolean encode;
	private String oseriesVersion;
	private String wtjVersion;

	private String priorValue;

	public Column(String name) {

		this.name = name;

		if (name.endsWith("Name") || name.endsWith("Country") || name.endsWith("City") || name.endsWith("Division")
				|| name.endsWith("District1") || name.endsWith("District2") || name.endsWith("District3")
				|| name.endsWith("District4")) {
			this.type = DataType.Name;
		} else if (name.endsWith("Code")) {
			this.type = DataType.Code;
		} else if (name.endsWith("Desc")) {
			this.type = DataType.Desc;
		} else if (name.endsWith("Id")) {
			this.type = DataType.Id;
		} else if (name.endsWith("Number") || name.endsWith("Num")) {
			this.type = DataType.Number;
		} else if (name.endsWith("Type")) {
			this.type = DataType.Type;
		} else if (name.endsWith("Ind")) {
			this.type = DataType.Boolean;
		} else if (name.endsWith("Ind")) {
			this.type = DataType.Boolean;
		} else if (name.endsWith("Date")) {
			this.type = DataType.Date;
		} else if (name.endsWith("Time")) {
			this.type = DataType.Timestamp;
		} else if (name.endsWith("Price") || name.endsWith("Quantity") || name.endsWith("Amt")
				|| name.endsWith("Amount") || name.endsWith("Cost")) {
			this.type = DataType.Amount;
		} else if (name.endsWith("Rate") || name.endsWith("Percent") || name.endsWith("Pct")) {
			this.type = DataType.Percent;
		} else {
			this.type = DataType.Unknown;
		}
	}

	private Column(JsonObject colObj) {

		for (String attrName : colObj.keySet()) {
			switch (attrName) {

			case "name":
				this.name = JsonUtils.getString(colObj, attrName);
				break;

			case "type":
				this.type = DataType.valueOf(JsonUtils.getString(colObj, attrName));
				break;

			case "nullable":
				break;

			case "nullRate":
				this.nullRate = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "avgLength":
				this.avgLength = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "samples": // values
				this.values = new HashSet<>(Arrays.asList(JsonUtils.getStringArray(colObj, attrName)));
				break;

			case "options":
				this.options = new ArrayList<>(Arrays.asList(JsonUtils.getStringArray(colObj, attrName)));

				String test = this.options.get(0);
				this.weightedOptions = test.contains(":");

				break;

			case "pattern":
				this.pattern = JsonUtils.getString(colObj, attrName);
				break;

			case "queryCols":

				this.queriesByVersion = new HashMap<>();
				JsonArray array = colObj.getJsonArray(attrName);
				for (int index = 0; index < array.size(); index++) {
					JsonObject next = array.getJsonObject(index);
					String version = JsonUtils.getString(next, "oVersion");
					String column = JsonUtils.getString(next, "column");

					this.queriesByVersion.put(version, column);
				}

				break;

			case "queryCol":
				this.queryCol = JsonUtils.getString(colObj, attrName);
				break;

			case "oVersion":
				this.oseriesVersion = JsonUtils.getString(colObj, attrName);
				break;

			case "wtjVersion":
				this.wtjVersion = JsonUtils.getString(colObj, attrName);
				break;

			case "minId":
				this.minId = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "maxId":
				this.maxId = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "uuid":
				this.uuid = JsonUtils.getBoolean(colObj, attrName);
				break;

			case "encode":
				this.encode = JsonUtils.getBoolean(colObj, attrName);
				break;

			case "repeatPct":
				this.repeatPct = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "random":
				this.random = JsonUtils.getBoolean(colObj, attrName);
				break;

			case "minDate":
				this.minDate = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "maxDate":
				this.maxDate = (int) JsonUtils.getLong(colObj, attrName);
				break;

			case "minAmount":
				this.minAmount = JsonUtils.getDouble(colObj, attrName);
				break;

			case "maxAmount":
				this.maxAmount = JsonUtils.getDouble(colObj, attrName);
				break;

			case "notes":
				break;

			default:
				System.out.println("Unrecognized column attribute: " + attrName);
			}
		}
	}

	public String getName() {
		return this.name;
	}

	public DataType getType() {
		return this.type;
	}

	public Set<String> getValues() {
		return this.values;
	}

	public int getNulls() {
		return this.nulls;
	}

	public int getNonNulls() {
		return this.nonNulls;
	}

	public long getTotalLength() {
		return this.totalLength;
	}

	public int getAvgLength() {
		return (this.avgLength >= 0) ? this.avgLength
				: ((this.nonNulls > 0) ? (int) (this.totalLength / this.nonNulls) : 0);
	}

	public int getNullRate() {
		return this.nullRate;
	}

	public void setNullRate(int nullRate) {
		this.nullRate = nullRate;
	}

	public List<String> getOptions() {
		return this.options;
	}

	public String getPattern() {
		return this.pattern;
	}

	public String getQueryCol(String version) {

		String query = this.queryCol;
		if ((query == null) && (version != null) && (this.queriesByVersion != null)) {
			query = this.queriesByVersion.get(version);
		}

		return query;
	}

	public int getMinId() {
		return this.minId;
	}

	public int getMaxId() {
		return this.maxId;
	}

	public void setMaxId(int maxId) {
		this.maxId = maxId;
	}

	public boolean isUuid() {
		return this.uuid;
	}

	public boolean isEncode() {
		return this.encode;
	}

	public int getRepeatPct() {
		return this.repeatPct;
	}

	public boolean isRandom() {
		return this.random;
	}

	public int getMinDate() {
		return this.minDate;
	}

	public int getMaxDate() {
		return this.maxDate;
	}

	public double getMinAmount() {
		return this.minAmount;
	}

	public double getMaxAmount() {
		return this.maxAmount;
	}

	public String getOseriesVersion() {
		return this.oseriesVersion;
	}

	public String getWtjVersion() {
		return this.wtjVersion;
	}

	public void addValue(String value) {
		if (value != null) {
			if (this.values == null) {
				this.values = new TreeSet<>();
			}

			if (this.values.size() < 100) {
				this.values.add(value);
			}

			this.nonNulls++;
			this.totalLength += value.length();
		} else {
			this.nulls++;
		}
	}

	public String getValue(Random random) {

		String value = "";
		if ((this.nullRate != 100) && ((this.nullRate == 0) || (this.nullRate < random.nextInt(101)))) {

			int nextId = 0;
			if (this.minId < this.maxId) {
				nextId = this.minId + random.nextInt((this.maxId - this.minId) + 1);
			}
			int nextDate = 0;
			if (this.minDate < this.maxDate) {
				nextDate = this.minDate + random.nextInt((this.maxDate - this.minDate) + 1);
			}
			double nextAmount = 0.0;
			if (this.minAmount < this.maxAmount) {
				nextAmount = this.minAmount + (random.nextDouble() * (this.maxAmount - this.minAmount));
			}

			if ((this.priorValue != null) && (this.repeatPct > 0) && (this.repeatPct > random.nextInt(100))) {
				value = this.priorValue;
			} else if ((this.type == DataType.Name) || (this.type == DataType.Code) || (this.type == DataType.Type)
					|| (this.type == DataType.Desc)) {
				if (this.uuid) {
					value = UUID.randomUUID().toString();
				} else if (this.pattern != null) {
					if (this.pattern.contains("%")) {

						if (this.random) {
							value = String.format(this.pattern, Math.abs(random.nextLong()));
						} else {
							value = String.format(this.pattern, nextId);
						}
					} else {
						value = this.pattern;
					}
				} else if ((this.options != null) || (this.values != null)) {

					if (this.weightedOptions) {
						value = getRandomOption(random);
					} else {

						if (this.options == null) {
							this.options = new ArrayList<>(this.values);
						}

						int index = random.nextInt(this.options.size());
						value = this.options.get(index);
					}
				}
			} else if (this.type == DataType.Id) {
				if (this.uuid) {
					value = UUID.randomUUID().toString();
				} else if (this.options != null) {
					int index = random.nextInt(this.options.size());
					value = this.options.get(index);
				} else if (this.minId < this.maxId) {
					value = Integer.toString(nextId);
				}
			} else if (this.type == DataType.Date) {

				if (this.options != null) {
					String offset = getRandomOption(random);
					nextDate = Integer.parseInt(offset);
				}

				LocalDate date = LocalDate.now().plusDays(nextDate);

				DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
				value = date.format(dtf);
			} else if (this.type == DataType.Timestamp) {

				LocalDateTime dt = LocalDateTime.now().plus(nextDate, ChronoUnit.MINUTES);

				DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				value = dt.format(dtf);
			} else if ((this.type == DataType.Amount) || (this.type == DataType.Percent)
					|| (this.type == DataType.Number)) {
				if (this.type == DataType.Percent) {
					nextAmount /= 100.0;
				}

				String pattern = (this.type == DataType.Percent) ? "%2.6f" : "%.2f";
				value = String.format(pattern, nextAmount);
			} else if (this.type == DataType.Boolean) {
				int flag = random.nextInt(2);
				value = Integer.toString(flag);
			}

			if ((value == null) || (value.length() == 0)) {
				System.out.println("QQQQ I think we have a problem: " + this.name);
			}

			this.priorValue = value;
		}

		return value;
	}

	private String getRandomOption(Random random) {

		String value = null;
		int chance = random.nextInt(100) + 1;
		for (String option : this.options) {
			int colon = option.indexOf(':');
			String code = option.substring(0, colon);
			int weight = Integer.parseInt(option.substring(colon + 1));

			if (chance <= weight) {
				value = code;
				break;
			} else {
				chance -= weight;
			}
		}

		return value;
	}

	public static List<Column> loadColumns(JsonObject root) {

		List<Column> columns = new ArrayList<>();

		JsonArray colArray = root.getJsonArray("columns");
		for (int colIndex = 0; colIndex < colArray.size(); colIndex++) {
			JsonObject colObj = colArray.getJsonObject(colIndex);
			Column col = new Column(colObj);
			columns.add(col);
		}

		return columns;
	}

	public static String encode(String in) {
		String out = "";
		if ((in != null) && (in.length() > 0)) {

			byte[] data = in.getBytes();
			for (int index = 0; index < data.length - 1; index++) {
				data[index] ^= data[index + 1];
			}

			out = Base64.getEncoder().encodeToString(data);
		}

		return out;
	}

	public boolean versionApplies(String targetVersion) {
		return versionApplies(this.oseriesVersion, targetVersion);
	}

	private static class QueryCol {
		private String version;
		private String query;
	}

	public static boolean versionApplies(String requiredVersion, String targetVersion) {

		boolean applies = (requiredVersion == null) || (requiredVersion.equals(targetVersion));
		if (!applies) {
			List<Integer> required = getVersionNumbers(requiredVersion);
			List<Integer> target = getVersionNumbers(targetVersion);

			int index = 0;
			for (int reqNum : required) {
				int tarNum = target.get(index++);
				if (reqNum < tarNum) {
					applies = true;
					break;
				} else if (reqNum > tarNum) {
					break;
				}
			}
		}

		return applies;
	}

	private static List<Integer> getVersionNumbers(String versionStr) {

		List<Integer> versionNumbers = new ArrayList<>();
		StringTokenizer tokenizer = new StringTokenizer(versionStr, ".");
		while (tokenizer.hasMoreTokens()) {
			versionNumbers.add(Integer.parseInt(tokenizer.nextToken()));
		}

		while (versionNumbers.size() < 10) {
			versionNumbers.add(0);
		}

		assert versionNumbers.size() == 10;

		return versionNumbers;
	}

	public static void writeHeaders(Writer writer, List<Column> columns) throws IOException {

		boolean first = true;
		for (Column col : columns) {

			if (!first) {
				writer.write('~');
			} else {
				first = false;
			}

			writer.write(col.getName());
		}
		writer.write('\n');
	}

	public static String buildCreateStageTable(List<Column> columns, String schema, String table, String location) {

		StringBuilder sb = new StringBuilder();
		sb.append("DROP TABLE IF EXISTS ");
		sb.append(schema);
		sb.append(".");
		sb.append(table);
		sb.append(";\nCREATE EXTERNAL TABLE ");
		sb.append(schema);
		sb.append(".");
		sb.append(table);
		sb.append(" (\n");

		boolean first = true;
		for (Column col : columns) {

			if (!first) {
				sb.append(",\n");
			} else {
				first = false;
			}

			sb.append("\t");
			sb.append(col.getName());

			DataType type = col.getType();
			if ((type == DataType.Name) || (type == DataType.Desc) || (type == DataType.Code) || (type == DataType.Type)
					|| (type == DataType.String) || (type == DataType.Boolean) || (type == DataType.Date)
					|| (type == DataType.Timestamp)) {

				sb.append(" string");
			} else if (type == DataType.Id) {
				sb.append(" int");
			} else if (type == DataType.Percent) {
				sb.append(" decimal(10,6)");
			} else if (type == DataType.Amount) {
				sb.append(" decimal(18,3)");
			} else if (type == DataType.Number) {
				sb.append(" bigint");
			} else {
				assert false : "Unrecognized type: " + type.name() + "\t" + col.getName();
			}
		}

		sb.append(")\n");
		sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '~' ESCAPED BY '\\\\\\\\'\n");
		sb.append("STORED AS TEXTFILE\n");
		sb.append("LOCATION '");
		sb.append(location);
		sb.append("'\nTBLPROPERTIES ('skip.header.line.count'='1');\n\n");

		return sb.toString();
	}

	public static String buildCreateOrcTable(List<Column> columns, int batchId) {

		StringBuilder sb = new StringBuilder();
		if (batchId > 0) {
			String tableName = String.format("WTJ_B%08d", batchId);
			sb.append("DROP TABLE IF EXISTS ${TEMP_DB}.");
			sb.append(tableName);
			sb.append(";\n");

			sb.append("CREATE EXTERNAL TABLE ${TEMP_DB}.");
			sb.append(tableName);
			sb.append(" (\n");
		} else {
			sb.append("CREATE EXTERNAL TABLE ${ORC_DB}.WideTaxJournal (\n");
		}

		int colIndex = 0;
		for (Column col : columns) {
			colIndex++;

			if (col.getName().equalsIgnoreCase("postingDate")) {
				continue;
			}

			sb.append("\t");
			sb.append(col.getName());

			DataType type = col.getType();

			if ((type == DataType.Name) || (type == DataType.Desc) || (type == DataType.Code) || (type == DataType.Type)
					|| (type == DataType.String)) {

				sb.append(" string");
			} else if (type == DataType.Timestamp) {
				sb.append(" timestamp");
			} else if (type == DataType.Boolean) {
				sb.append(" boolean");
			} else if (type == DataType.Id) {
				sb.append(" int");
			} else if (type == DataType.Date) {

				sb.append(" date");
			} else if (type == DataType.Percent) {

				sb.append(" decimal(10,6)");
			} else if (type == DataType.Amount) {

				sb.append(" decimal(18,3)");
			} else if (type == DataType.Number) {

				sb.append(" bigint");
			} else {
				assert false : "Unrecognized type: " + type.name() + "\t" + col.getName();
			}

			if (colIndex < columns.size()) {
				sb.append(",\n");
			}

			// This is the final original column. After this point, all new columns must be
			// added at the end.
			if (col.getName().equalsIgnoreCase("returnsIndicatorField5Value")) {
				sb.append("\ttaxpayerCode string,\n");

				sb.append("\tsitusCountry string,\n");
				sb.append("\tsitusMainDivision string,\n");
				sb.append("\tsitusSubDivision string,\n");
				sb.append("\tsitusCity string,\n");

				sb.append("\timportTimestamp timestamp,\n");
				sb.append("\timportBatchId int,\n");
				sb.append("\timportFileId int,\n");
			}
		}

		sb.append(")\n");

		sb.append("PARTITIONED BY (tenantCode string, postingDate date)\n");
		sb.append("STORED AS ORC");

		if (batchId > 0) {
			sb.append("\nLOCATION '${BASE}/temp_db/");
			sb.append(String.format("B%08d", batchId));
			sb.append("';\n\n");
		} else {
			sb.append("\nLOCATION '${BASE}/data_db/wtj';\n");
		}

		return sb.toString();
	}

	public static String buildInsertOrcTable(List<Column> columns, int batchId, List<WtjFile> files,
			List<WtjPartition> partitions, long timestamp) {

		String taxpayer = "(CASE "
				+ "WHEN (UF.transPerspective='Seller' AND UF.transSubType='Self verification') OR UF.transPerspective='Buyer' THEN reflect(\"java.net.URLEncoder\", \"encode\", UF.buyrPrimPartyCode, \"UTF-8\") ||\n"
				+ "(CASE WHEN UF.buyrScndPartyCode IS NOT NULL AND LENGTH(UF.buyrScndPartyCode)>0 THEN '|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.buyrScndPartyCode, \"UTF-8\") ELSE '' END) || \n"
				+ "(CASE WHEN UF.buyrTrtryPartyCode IS NOT NULL AND LENGTH(UF.buyrTrtryPartyCode)>0 THEN '|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.buyrTrtryPartyCode, \"UTF-8\") ELSE '' END)\n"
				+ "WHEN UF.transPerspective='Seller' THEN reflect(\"java.net.URLEncoder\", \"encode\", UF.selrPrimPartyCode, \"UTF-8\") ||\n"
				+ "(CASE WHEN UF.selrScndPartyCode IS NOT NULL AND LENGTH(UF.selrScndPartyCode)>0 THEN '|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.selrScndPartyCode, \"UTF-8\") ELSE '' END) ||\n"
				+ "(CASE WHEN UF.selrTrtryPartyCode IS NOT NULL AND LENGTH(UF.selrTrtryPartyCode)>0 THEN '|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.selrTrtryPartyCode, \"UTF-8\") ELSE '' END)\n"
				+ "WHEN UF.transPerspective='Owner' THEN reflect(\"java.net.URLEncoder\", \"encode\", UF.ownrPrimPartyCode, \"UTF-8\") ||\n"
				+ "(CASE WHEN UF.ownrScndPartyCode IS NOT NULL AND LENGTH(UF.ownrScndPartyCode)>0 THEN '|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.ownrScndPartyCode, \"UTF-8\") ELSE '' END) ||\n"
				+ "(CASE WHEN UF.ownrTrtryPartyCode IS NOT NULL AND LENGTH(UF.ownrTrtryPartyCode)>0 THEN '|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.ownrTrtryPartyCode, \"UTF-8\") ELSE '' END) ELSE null END)";

		StringBuilder sb = new StringBuilder();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		sb.append("INSERT OVERWRITE TABLE ${TEMP_DB}.");
		sb.append(String.format("WTJ_B%08d", batchId));
		sb.append(" PARTITION(tenantCode,postingDate) SELECT\n");

		if (timestamp == 0L) {
			timestamp = System.currentTimeMillis();
		}

		for (Column col : columns) {

			if (col.getName().equalsIgnoreCase("postingDate")) {
				continue;
			}

			DataType type = col.getType();
			sb.append("\t");
			if (type == DataType.Boolean) {
				sb.append("cast("); // (CASE WHEN upper(UF.qqq) IN ('T','TRUE','1','Y','YES') THEN 1 ELSE 0 END)
				sb.append("(CASE WHEN UPPER(UF.");
				sb.append(col.getName());
				sb.append(") IN ('T','TRUE','1','Y','YES') THEN 1 ELSE 0 END)");
				sb.append(" as boolean)");
			} else if (type != DataType.Date) {
				sb.append("UF.");
				sb.append(col.getName());
			} else {

				String dc = "UF." + col.getName();
				sb.append(castStringToDate(dc));
				sb.append(" AS ");
				sb.append(col.getName());
			}

			sb.append(",\n");

			// This is the final original column. After this point, all new columns must be
			// added at the end.
			if (col.getName().equalsIgnoreCase("returnsIndicatorField5Value")) {

				sb.append("\t");
				sb.append(taxpayer);
				sb.append(" AS taxpayerCode,\n");

				sb.append(
						"\t(CASE WHEN UF.situsTaxAreaId=UF.d_taxAreaId THEN UF.d_taxAreaCountry WHEN UF.situsTaxAreaId=UF.po_taxAreaId THEN UF.po_taxAreaCountry WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaCountry WHEN UF.situsTaxAreaId=UF.ad_taxAreaId THEN UF.ad_taxAreaCountry WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaCountry ELSE null END) as situsCountry,\n");
				sb.append(
						"\t(CASE WHEN UF.situsTaxAreaId=UF.d_taxAreaId THEN UF.d_taxAreaMainDivision WHEN UF.situsTaxAreaId=UF.po_taxAreaId THEN UF.po_taxAreaMainDivision WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaMainDivision WHEN UF.situsTaxAreaId=UF.ad_taxAreaId THEN UF.ad_taxAreaMainDivision WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaMainDivision ELSE null END) as situsMainDivision,\n");
				sb.append(
						"\t(CASE WHEN UF.situsTaxAreaId=UF.d_taxAreaId THEN UF.d_taxAreaSubDivision WHEN UF.situsTaxAreaId=UF.po_taxAreaId THEN UF.po_taxAreaSubDivision WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaSubDivision WHEN UF.situsTaxAreaId=UF.ad_taxAreaId THEN UF.ad_taxAreaSubDivision WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaSubDivision ELSE null END) as situsSubDivision,\n");
				sb.append(
						"\t(CASE WHEN UF.situsTaxAreaId=UF.d_taxAreaId THEN UF.d_taxAreaCity WHEN UF.situsTaxAreaId=UF.po_taxAreaId THEN UF.po_taxAreaCity WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaCity WHEN UF.situsTaxAreaId=UF.ad_taxAreaId THEN UF.ad_taxAreaCity WHEN UF.situsTaxAreaId=UF.ao_taxAreaId THEN UF.ao_taxAreaCity ELSE null END) as situsCity,\n");

				sb.append("'");
				sb.append(sdf.format(new Date(timestamp)));
				sb.append("' AS importTimestamp,\n");
				sb.append("${BATCHID} AS importBatchId,\n");
				sb.append("UF.fileId AS importFileId,\n");
			}
		}

		sb.append(
				"\t'${DEPLOYMENT}|' || reflect(\"java.net.URLEncoder\", \"encode\", UF.sourceName, \"UTF-8\") AS tenantCode,\n");
		sb.append("\t");
		sb.append(castStringToDate("UF.postingDate"));
		sb.append(" AS postingDate");
		sb.append("\n");

		sb.append("FROM (\n");

		WtjFile.createFileUnion(files, sb);

		sb.append(") UF\n");

		if ((partitions != null) && (partitions.size() > 0)) {
			sdf = new SimpleDateFormat("yyyyMMdd");
			sb.append("WHERE\n (");
			boolean first = true;
			for (WtjPartition partition : partitions) {

				if (!first) {
					sb.append(" OR ");
				} else {
					first = false;
				}

				Date postingDate = partition.getPostingDate();
				String tenantCode = partition.getTenantCode();
				int bar = tenantCode.indexOf("|");
				String sourceName;
				try {
					sourceName = URLDecoder.decode(tenantCode.substring(bar + 1), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					sourceName = tenantCode.substring(bar + 1);
				}

				sb.append("(UF.postingDate='");
				sb.append(sdf.format(postingDate));
				sb.append("' AND UF.sourceName='");
				sb.append(sourceName);
				sb.append("')");
			}
			sb.append(")\n");
		}

		sb.append("ORDER BY\n");

		sb.append(taxpayer);
		sb.append(",\nUF.situsTaxAreaId");
		sb.append(";\n");

		return sb.toString();
	}

	private static String castStringToDate(String dc) {
		return "(CASE WHEN " + dc + " IS NOT NULL AND LENGTH(" + dc + ")=8 THEN " + "(SUBSTR(" + dc + ",1,4)"
				+ " || '-' || " + "SUBSTR(" + dc + ",5,2)" + " || '-' || " + "SUBSTR(" + dc + ",7,2)) ELSE null END)";
	}
}
