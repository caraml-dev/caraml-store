package dev.caraml.store.model;

import java.time.Instant;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import lombok.Data;

/**
 * Base object class ensuring that all objects stored in the registry have an auto-generated
 * creation and last updated time.
 */
@MappedSuperclass
@Data
public abstract class AbstractTimestampEntity {
  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "created", nullable = false)
  private Date created;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "last_updated", nullable = false)
  private Date lastUpdated;

  @PrePersist
  protected void onCreate() {
    lastUpdated = created = new Date();
  }

  @PreUpdate
  protected void onUpdate() {
    lastUpdated = new Date();
  }

  // This constructor is used for testing.
  public AbstractTimestampEntity() {
    this.created = Date.from(Instant.ofEpochMilli(0L));
    this.lastUpdated = Date.from(Instant.ofEpochMilli(0L));
  }
}
