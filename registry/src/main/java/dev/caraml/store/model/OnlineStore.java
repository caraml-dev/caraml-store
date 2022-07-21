package dev.caraml.store.model;

import dev.caraml.store.protobuf.core.OnlineStoreProto;
import dev.caraml.store.protobuf.core.OnlineStoreProto.StoreType;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@javax.persistence.Entity
@Table(name = "online_stores")
public class OnlineStore {

  // Name of the store. Must be unique
  @Id
  @Column(name = "name", nullable = false, unique = true)
  private String name;

  // Type of the store, should map to feast.core.OnlineStore.StoreType
  @Column(name = "type", nullable = false)
  private String type;

  // Description of the store
  @Column(name = "description")
  private String description;

  // Flag to set whether the store has been archived
  @Column(name = "archived", nullable = false)
  private boolean archived = false;

  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "onlineStore")
  private Set<FeatureTable> featureTables;

  public OnlineStore() {
    super();
  }

  public OnlineStore(String name, StoreType storeType, String description) {
    this.name = name;
    this.type = storeType.toString();
    this.description = description;
  }

  public static OnlineStore fromProto(OnlineStoreProto.OnlineStore onlineStoreProto)
      throws IllegalArgumentException {

    OnlineStore onlineStore = new OnlineStore();
    onlineStore.setName(onlineStoreProto.getName());
    onlineStore.setType(onlineStoreProto.getType().toString());
    onlineStore.setDescription(onlineStoreProto.getDescription());
    return onlineStore;
  }

  public OnlineStoreProto.OnlineStore toProto() {
    return OnlineStoreProto.OnlineStore.newBuilder()
        .setName(getName())
        .setType(StoreType.valueOf(getType()))
        .setDescription(getDescription())
        .build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (getClass() != obj.getClass()) return false;
    OnlineStore other = (OnlineStore) obj;

    if (!name.equals(other.name)) {
      return false;
    } else if (!type.equals(other.type)) {
      return false;
    } else return description.equals(other.description);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
