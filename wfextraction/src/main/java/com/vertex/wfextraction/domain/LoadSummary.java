package com.vertex.wfextraction.domain;

import java.sql.Date;

public class LoadSummary {

  private int batchId;
  private int fileId;
  private String deploymentCode;
  private String tenantCode;
  private Date postingDate;
  private long totalRows;

  public int getBatchId() { return this.batchId; }
  public int getFileId() { return this.fileId; }
  public String getDeploymentCode() { return this.deploymentCode; }
  public String getTenantCode() { return this.tenantCode; }
  public Date getPostingDate() { return this.postingDate; }
  public long getTotalRows() { return this.totalRows; }

  public void setBatchId(int batchId) { this.batchId = batchId; }
  public void setFileId(int fileId) { this.fileId = fileId; }
  public void setDeploymentCode(String deploymentCode) { this.deploymentCode = deploymentCode; }
  public void setTenantCode(String tenantCode) { this.tenantCode = tenantCode; }
  public void setPostingDate(Date postingDate) { this.postingDate = postingDate; }
  public void setTotalRows(long totalRows) { this.totalRows = totalRows; }

  public WtjPartition getPartitionKey() { return new WtjPartition(this); }
}
