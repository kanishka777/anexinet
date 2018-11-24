package com.vertex.wfextraction.domain;

import java.sql.Date;

public class WtjPartition {

  private String tenantCode;
  private Date postingDate;

  public WtjPartition() {
  }

  public WtjPartition(LoadSummary loadSummary) {
    this.tenantCode = loadSummary.getTenantCode();
    this.postingDate = loadSummary.getPostingDate();
  }

  public int hashCode() {
    return this.tenantCode.hashCode() ^ this.postingDate.hashCode();
  }

  public boolean equals(Object obj) {

    boolean equals = (this == obj);
    if (!equals && (obj != null) && (obj instanceof WtjPartition)) {
      WtjPartition other = (WtjPartition)obj;
      if ((this.tenantCode.equals(other.tenantCode))) {
        long thisDate = this.postingDate.getTime();
        long otherDate = other.postingDate.getTime();
        equals = (thisDate == otherDate);
      }
    }

    return equals;
  }

  public String getTenantCode() {
    return this.tenantCode;
  }
  public Date getPostingDate() {
    return this.postingDate;
  }

  public void setTenantCode(String tenantCode) { this.tenantCode = tenantCode; }
  public void setPostingDate(Date postingDate) { this.postingDate = postingDate; }
}
