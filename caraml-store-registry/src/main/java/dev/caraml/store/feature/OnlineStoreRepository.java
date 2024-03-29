package dev.caraml.store.feature;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository supplying OnlineStore objects keyed by id. */
public interface OnlineStoreRepository extends JpaRepository<OnlineStore, String> {

  Optional<OnlineStore> findOnlineStoreByNameAndArchivedFalse(String name);

  List<OnlineStore> findAllByArchivedFalse();
}
