package com.vertex.wfextraction.domain;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vertex.wfextraction.utils.JsonUtils;

public class WtjFile {
	private int fileId;

	private String fileName;
	private String origFileName;

	private String schemaVersion;
	private int deploymentId;
	private int batchId;
	private int currentStepId;
	private Timestamp startTime;
	private Timestamp endTime;
	private Timestamp heartbeat;
	private String processTag;
	private int failureCount;

	private String fileETag;
	private String origFileETag;

	private long fileSize;

	public WtjFile() {
	}

	public int getFileId() {
		return this.fileId;
	}

	public String getFileName() {
		return this.fileName;
	}

	public String getOrigFileName() {
		return this.origFileName;
	}

	public String getSchemaVersion() {
		return this.schemaVersion;
	}

	public int getDeploymentId() {
		return this.deploymentId;
	}

	public int getBatchId() {
		return this.batchId;
	}

	public int getCurrentStepId() {
		return this.currentStepId;
	}

	public Timestamp getStartTime() {
		return this.startTime;
	}

	public Timestamp getEndTime() {
		return this.endTime;
	}

	public Timestamp getHeartbeat() {
		return this.heartbeat;
	}

	public String getProcessTag() {
		return this.processTag;
	}

	public int getFailureCount() {
		return this.failureCount;
	}

	public String getFileETag() {
		return this.fileETag;
	}

	public String getOrigFileETag() {
		return this.origFileETag;
	}

	public long getFileSize() {
		return this.fileSize;
	}

	public void setFileId(int fileId) {
		this.fileId = fileId;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setOrigFileName(String origFileName) {
		this.origFileName = origFileName;
	}

	public void setSchemaVersion(String schemaVersion) {
		this.schemaVersion = schemaVersion;
	}

	public void setDeploymentId(int deploymentId) {
		this.deploymentId = deploymentId;
	}

	public void setBatchId(int batchId) {
		this.batchId = batchId;
	}

	public void setCurrentStepId(int currentStepId) {
		this.currentStepId = currentStepId;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public void setHeartbeat(Timestamp heartbeat) {
		this.heartbeat = heartbeat;
	}

	public void setProcessTag(String processTag) {
		this.processTag = processTag;
	}

	public void setFailureCount(int failureCount) {
		this.failureCount = failureCount;
	}

	public void setFileETag(String fileETag) {
		this.fileETag = fileETag;
	}

	public void setOrigFileETag(String origFileETag) {
		this.origFileETag = origFileETag;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public static FileType determineFileType(String fileName) {

		assert fileName != null;

		int slash = fileName.indexOf('/');
		fileName = (slash > 0) ? fileName.substring(slash + 1) : fileName;

		FileType type = FileType.WTJ;
		if (fileName.startsWith("_REPAIR_")) {
			type = FileType.Repair;
		} else if (fileName.startsWith("_DELETE_")) {
			type = FileType.Delete;
		} else if (fileName.startsWith("_PURGE_")) {
			type = FileType.Purge;
		} else if (fileName.startsWith("_FORCE_")) {
			type = FileType.Force;
		}

		return type;
	}

	public static String createFileUnion(List<WtjFile> files, StringBuilder sql) {

		boolean first = true;

		for (WtjFile file : files) {

			if (determineFileType(file.getOrigFileName()) != FileType.WTJ) {
				continue;
			}

			if (!first) {
				sql.append("UNION ALL\n");
			} else {
				first = false;
			}
			sql.append("SELECT ");
			sql.append(Integer.toString(file.getFileId()));
			sql.append(" AS fileId, ");
			sql.append(String.format("F%08d", file.getFileId()));
			sql.append(".* FROM ${STG_DB}.WTJ_");
			sql.append(String.format("F%08d F%08d", file.getFileId(), file.getFileId()));
			sql.append("\n");
		}

		return sql.toString();
	}

	public String toJson() {

		Map<String, Object> params = new HashMap<>();
		params.put("fileId", this.fileId);
		params.put("fileName", this.fileName);
		params.put("origFileName", this.origFileName);
		params.put("batchId", this.batchId);
		params.put("status", (this.currentStepId >= 0) ? FileStep.values()[this.currentStepId] : "Failed");
		params.put("fileSize", this.fileSize);

		return JsonUtils.toJson(params);
	}
}
