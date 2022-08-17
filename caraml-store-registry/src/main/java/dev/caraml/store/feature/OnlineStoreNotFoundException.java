package dev.caraml.store.feature;

/** Exception thrown when retrieval of a spec from the registry fails. */
public class OnlineStoreNotFoundException extends ResourceNotFoundException {
  public OnlineStoreNotFoundException(String onlineStore) {
    super(String.format("No such Online Store: %s", onlineStore));
  }
}
