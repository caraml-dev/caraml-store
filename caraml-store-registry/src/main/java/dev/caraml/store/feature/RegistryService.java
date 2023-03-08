package dev.caraml.store.feature;

import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyEntityResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyFeatureTableResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ArchiveOnlineStoreResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.DeleteFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetEntityRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetEntityResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeatureTableResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetOnlineStoreResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListEntitiesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListEntitiesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeaturesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeaturesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListOnlineStoresResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.RegisterOnlineStoreRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.RegisterOnlineStoreResponse;
import dev.caraml.store.protobuf.core.EntityProto;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Facilitates management of specs within the feature registry. This includes getting existing specs
 * and registering new specs.
 */
@Slf4j
@Service
public class RegistryService {

  private final EntityRepository entityRepository;
  private final FeatureTableRepository tableRepository;
  private final ProjectRepository projectRepository;
  private final OnlineStoreRepository onlineStoreRepository;
  private final ProjectValidator projectValidator;

  @Autowired
  public RegistryService(
      EntityRepository entityRepository,
      FeatureTableRepository tableRepository,
      ProjectRepository projectRepository,
      OnlineStoreRepository onlineStoreRepository,
      ProjectValidator projectValidator) {
    this.entityRepository = entityRepository;
    this.tableRepository = tableRepository;
    this.projectRepository = projectRepository;
    this.onlineStoreRepository = onlineStoreRepository;
    this.projectValidator = projectValidator;
  }

  /**
   * Get an entity matching the entity name and set project. The entity name and project are
   * required. If the project is omitted, the default would be used.
   *
   * @param request GetEntityRequest Request
   * @return Returns a GetEntityResponse containing an entity
   */
  public GetEntityResponse getEntity(GetEntityRequest request) {
    String projectName = request.getProject();
    String entityName = request.getName();

    if (entityName.isEmpty()) {
      throw new IllegalArgumentException("No entity name provided");
    }
    // Autofill default project if project is not specified
    if (projectName.isEmpty()) {
      projectName = Project.DEFAULT_NAME;
    }

    Entity entity = entityRepository.findEntityByNameAndProject_Name(entityName, projectName);

    if (entity == null) {
      throw new EntityNotFoundException(projectName, entityName);
    }

    // Build GetEntityResponse
    return GetEntityResponse.newBuilder().setEntity(entity.toProto()).build();
  }

  /**
   * Return a map of feature references and features matching the project, labels and entities
   * provided in the filter. All fields are required.
   *
   * <p>Project name must be explicitly provided or if the project name is omitted, the default
   * project would be used. A combination of asterisks/wildcards and text is not allowed.
   *
   * <p>The entities in the filter accepts a list. All matching features will be returned. Regex is
   * not supported. If no entities are provided, features will not be filtered by entities.
   *
   * <p>The labels in the filter accepts a map. All matching features will be returned. Regex is not
   * supported. If no labels are provided, features will not be filtered by labels.
   *
   * @param filter filter containing the desired project name, entities and labels
   * @return ListEntitiesResponse with map of feature references and features found matching the
   *     filter
   */
  public ListFeaturesResponse listFeatures(ListFeaturesRequest.Filter filter) {
    String project = filter.getProject();
    List<String> entities = filter.getEntitiesList();
    Map<String, String> labels = filter.getLabelsMap();

    // Autofill default project if project not specified
    if (project.isEmpty()) {
      project = Project.DEFAULT_NAME;
    }

    // Currently defaults to all FeatureTables
    List<FeatureTable> featureTables = tableRepository.findAllByProject_Name(project);

    ListFeaturesResponse.Builder response = ListFeaturesResponse.newBuilder();
    if (entities.size() > 0) {
      featureTables =
          featureTables.stream()
              .filter(featureTable -> featureTable.hasAllEntities(entities))
              .collect(Collectors.toList());
    }

    Map<String, Feature> featuresMap;
    for (FeatureTable featureTable : featureTables) {
      featuresMap = featureTable.getFeaturesByLabels(labels);
      for (Map.Entry<String, Feature> entry : featuresMap.entrySet()) {
        response.putFeatures(entry.getKey(), entry.getValue().toProto());
      }
    }

    return response.build();
  }

  /**
   * Return a list of entities matching the entity name, project and labels provided in the filter.
   * All fields are required. Use '*' in entity name and project, and empty map in labels in order
   * to return all entities in all projects.
   *
   * <p>Project name can be explicitly provided, or an asterisk can be provided to match all
   * projects. It is not possible to provide a combination of asterisks/wildcards and text. If the
   * project name is omitted, the default project would be used.
   *
   * <p>The entity name in the filter accepts an asterisk as a wildcard. All matching entities will
   * be returned. Regex is not supported. Explicitly defining an entity name is not possible if a
   * project name is not set explicitly.
   *
   * <p>The labels in the filter accepts a map. All entities which contain every provided label will
   * be returned.
   *
   * @param filter Filter containing the desired entity name, project and labels
   * @return ListEntitiesResponse with list of entities found matching the filter
   */
  public ListEntitiesResponse listEntities(ListEntitiesRequest.Filter filter) {
    String project = filter.getProject();
    Map<String, String> labelsFilter = filter.getLabelsMap();

    // Autofill default project if project not specified
    if (project.isEmpty()) {
      project = Project.DEFAULT_NAME;
    }

    List<Entity> entities = entityRepository.findAllByProject_Name(project);

    ListEntitiesResponse.Builder response = ListEntitiesResponse.newBuilder();
    if (entities.size() > 0) {
      entities =
          entities.stream()
              .filter(entity -> entity.hasAllLabels(labelsFilter))
              .collect(Collectors.toList());
      for (Entity entity : entities) {
        response.addEntities(entity.toProto());
      }
    }

    return response.build();
  }

  private Project getOrCreateNewProject(String projectName) {
    // Find project or create new one if it does not exist
    Optional<Project> existingProject = projectRepository.findById(projectName);
    if (existingProject.isEmpty()) {
      Matchers.checkValidCharactersAllowDash(projectName, "project");
      projectValidator.validateProject(projectName);
    }
    Project project = existingProject.orElse(new Project(projectName));

    // Ensure that the project retrieved from repository is not archived
    if (project.isArchived()) {
      throw new IllegalArgumentException(String.format("Project is archived: %s", projectName));
    }
    return project;
  }

  /**
   * Creates or updates an entity in the repository.
   *
   * <p>This function is idempotent. If no changes are detected in the incoming entity's schema,
   * this method will return the existing entity stored in the repository. If project is not
   * specified, the entity will be assigned to the 'default' project.
   *
   * @param newEntitySpec EntitySpecV2 that will be used to create or update an Entity.
   * @param projectName Project namespace of Entity which is to be created/updated
   */
  @Transactional
  public ApplyEntityResponse applyEntity(EntityProto.EntitySpec newEntitySpec, String projectName) {
    projectName = resolveProjectName(projectName);

    // Validate incoming entity
    EntityValidator.validateSpec(newEntitySpec);

    Project project = getOrCreateNewProject(projectName);
    // Retrieve existing Entity
    Entity entity =
        entityRepository.findEntityByNameAndProject_Name(newEntitySpec.getName(), projectName);

    EntityProto.Entity newEntity = EntityProto.Entity.newBuilder().setSpec(newEntitySpec).build();
    if (entity == null) {
      // Create new entity since it doesn't exist
      entity = Entity.fromProto(newEntity);
    } else {
      // If the entity remains unchanged, we do nothing.
      if (entity.toProto().getSpec().equals(newEntitySpec)) {
        return ApplyEntityResponse.newBuilder().setEntity(entity.toProto()).build();
      }
      entity.updateFromProto(newEntity, projectName);
    }

    // Persist the EntityV2 object
    project.addEntity(entity);
    projectRepository.saveAndFlush(project);

    // Build ApplyEntityResponse
    return ApplyEntityResponse.newBuilder().setEntity(entity.toProto()).build();
  }

  /**
   * Resolves the project name by returning name if given, autofilling default project otherwise.
   *
   * @param projectName name of the project to resolve.
   */
  public static String resolveProjectName(String projectName) {
    return (projectName.isEmpty()) ? Project.DEFAULT_NAME : projectName;
  }

  /**
   * Applies the given FeatureTable to the FeatureTable registry. Creates the FeatureTable if does
   * not exist, otherwise updates the existing FeatureTable. Applies FeatureTable in project if
   * specified, otherwise in default project.
   *
   * @param request Contains FeatureTable spec and project parameters used to create or update a
   *     FeatureTable.
   * @throws ResourceNotFoundException projects and entities referenced in request do not exist.
   * @return response containing the applied FeatureTable spec.
   */
  @Transactional
  public ApplyFeatureTableResponse applyFeatureTable(ApplyFeatureTableRequest request) {
    String projectName = resolveProjectName(request.getProject());

    Matchers.checkValidCharactersAllowDash(projectName, "project");
    // temporarily map empty storeNames to a dummy store, to make it backward compatible
    String specStoreName = "unset";

    if (!request.getTableSpec().getOnlineStore().getName().isEmpty()) {
      specStoreName = request.getTableSpec().getOnlineStore().getName();
    }

    String storeName = specStoreName;

    // fetch existing onlineStore by store name
    OnlineStore onlineStore =
        onlineStoreRepository
            .findOnlineStoreByNameAndArchivedFalse(storeName)
            .orElseThrow(
                () -> new IllegalArgumentException(String.format("Invalid store: %s", storeName)));
    // Check that specification provided is valid
    FeatureTableSpec applySpec = request.getTableSpec();
    FeatureTableValidator.validateSpec(applySpec);

    // Create or update depending on whether there is an existing Feature Table
    Optional<FeatureTable> existingTable =
        tableRepository.findFeatureTableByNameAndProject_Name(applySpec.getName(), projectName);
    FeatureTable table = FeatureTable.fromProto(projectName, applySpec, entityRepository);
    if (existingTable.isPresent() && table.equals(existingTable.get())) {
      // Skip update if no change is detected
      return ApplyFeatureTableResponse.newBuilder().setTable(existingTable.get().toProto()).build();
    }
    if (existingTable.isPresent()) {
      existingTable.get().updateFromProto(projectName, applySpec, entityRepository);
      table = existingTable.get();
    }

    table.setOnlineStore(onlineStore);
    onlineStore.getFeatureTables().add(table);

    // Commit FeatureTable (cascaded by onlineStore) to database and return applied FeatureTable
    onlineStoreRepository.saveAndFlush(onlineStore);
    return ApplyFeatureTableResponse.newBuilder().setTable(table.toProto()).build();
  }

  /**
   * List the FeatureTables matching the filter in the given filter. Scopes down listing to project
   * if specified, the default project otherwise.
   *
   * @param filter Filter containing the desired project and labels
   * @return ListFeatureTablesResponse with list of FeatureTables found matching the filter
   */
  @Transactional
  public ListFeatureTablesResponse listFeatureTables(ListFeatureTablesRequest.Filter filter) {
    String projectName = resolveProjectName(filter.getProject());
    Map<String, String> labelsFilter = filter.getLabelsMap();

    List<FeatureTable> matchingTables = tableRepository.findAllByProject_Name(projectName);

    ListFeatureTablesResponse.Builder response = ListFeatureTablesResponse.newBuilder();

    if (matchingTables.size() > 0) {
      matchingTables =
          matchingTables.stream()
              .filter(table -> table.hasAllLabels(labelsFilter))
              .filter(table -> !table.isDeleted())
              .collect(Collectors.toList());
    }
    for (FeatureTable table : matchingTables) {
      response.addTables(table.toProto());
    }

    return response.build();
  }

  /**
   * Get the FeatureTable with the name and project specified in the request. Gets FeatureTable in
   * project if specified, otherwise in default project.
   *
   * @param request containing the retrieval parameters.
   * @throws ResourceNotFoundException if no FeatureTable matches given request.
   * @return response containing the requested FeatureTable.
   */
  @Transactional
  public GetFeatureTableResponse getFeatureTable(GetFeatureTableRequest request) {
    String projectName = resolveProjectName(request.getProject());
    String featureTableName = request.getName();

    Optional<FeatureTable> retrieveTable =
        tableRepository.findFeatureTableByNameAndProject_NameAndIsDeletedFalse(
            featureTableName, projectName);
    if (retrieveTable.isEmpty()) {
      throw new FeatureTableNotFoundException(projectName, featureTableName);
    }

    return GetFeatureTableResponse.newBuilder().setTable(retrieveTable.get().toProto()).build();
  }

  @Transactional
  public void deleteFeatureTable(DeleteFeatureTableRequest request) {
    String projectName = resolveProjectName(request.getProject());
    String featureTableName = request.getName();

    Optional<FeatureTable> existingTable =
        tableRepository.findFeatureTableByNameAndProject_NameAndIsDeletedFalse(
            featureTableName, projectName);
    if (existingTable.isEmpty()) {
      throw new FeatureTableNotFoundException(projectName, featureTableName);
    }

    existingTable.get().delete();
  }

  /**
   * ListOnlineStores Lists unarchived stores
   *
   * @return ListOnlineStoresResponse - list of online stores
   */
  public ListOnlineStoresResponse listOnlineStores() {
    ListOnlineStoresResponse.Builder responseBuilder = ListOnlineStoresResponse.newBuilder();

    for (OnlineStore onlineStore : onlineStoreRepository.findAllByArchivedFalse()) {
      responseBuilder.addOnlineStore(onlineStore.toProto());
    }
    return responseBuilder.build();
  }

  /**
   * GetOnlineStore - Returns online store by name
   *
   * @param name - Online store name
   * @return GetOnlineStoreResponse - OnlineStore proto and status
   */
  public GetOnlineStoreResponse getOnlineStore(String name) {
    OnlineStore onlineStore =
        onlineStoreRepository
            .findById(name)
            .orElseThrow(() -> new OnlineStoreNotFoundException(name));

    GetOnlineStoreResponse.Status status = GetOnlineStoreResponse.Status.ACTIVE;
    if (onlineStore.isArchived()) {
      status = GetOnlineStoreResponse.Status.ARCHIVED;
    }

    return GetOnlineStoreResponse.newBuilder()
        .setOnlineStore(onlineStore.toProto())
        .setStatus(status)
        .build();
  }

  /**
   * RegisterOnlineStore registers/updates online store to feast core
   *
   * @param registerOnlineStoreRequest containing the new/updated online store definition
   * @return RegisterOnlineStoreResponse containing the new/updated online store definition
   */
  @Transactional
  public RegisterOnlineStoreResponse registerOnlineStore(
      RegisterOnlineStoreRequest registerOnlineStoreRequest) {
    OnlineStoreProto.OnlineStore newOnlineStoreProto = registerOnlineStoreRequest.getOnlineStore();

    OnlineStore existingOnlineStore =
        onlineStoreRepository.findById(newOnlineStoreProto.getName()).orElse(null);

    // Do nothing if no change
    if (existingOnlineStore != null
        && !existingOnlineStore.isArchived()
        && existingOnlineStore.toProto().equals(newOnlineStoreProto)) {
      return RegisterOnlineStoreResponse.newBuilder()
          .setStatus(RegisterOnlineStoreResponse.Status.NO_CHANGE)
          .setOnlineStore(registerOnlineStoreRequest.getOnlineStore())
          .build();
    }

    OnlineStore newOnlineStore = OnlineStore.fromProto(newOnlineStoreProto);
    onlineStoreRepository.save(newOnlineStore);

    RegisterOnlineStoreResponse.Status responseStatus =
        RegisterOnlineStoreResponse.Status.REGISTERED;
    if (existingOnlineStore != null) {
      responseStatus = RegisterOnlineStoreResponse.Status.UPDATED;
    }

    return RegisterOnlineStoreResponse.newBuilder()
        .setStatus(responseStatus)
        .setOnlineStore(registerOnlineStoreRequest.getOnlineStore())
        .build();
  }

  /**
   * Archives an online store
   *
   * @param name Name of the online store to be archived
   */
  @Transactional
  public ArchiveOnlineStoreResponse archiveOnlineStore(String name) {
    OnlineStore onlineStore =
        onlineStoreRepository
            .findById(name)
            .orElseThrow(() -> new OnlineStoreNotFoundException(name));

    onlineStore.setArchived(true);
    onlineStoreRepository.saveAndFlush(onlineStore);
    return ArchiveOnlineStoreResponse.newBuilder().build();
  }

  /**
   * Creates a project
   *
   * @param name Name of project to be created
   */
  @Transactional
  public void createProject(String name) {
    if (projectRepository.existsById(name)) {
      throw new IllegalArgumentException(String.format("Project already exists: %s", name));
    }
    projectValidator.validateProject(name);
    Project project = new Project(name);
    projectRepository.saveAndFlush(project);
  }

  /**
   * Archives a project
   *
   * @param name Name of the project to be archived
   */
  @Transactional
  public void archiveProject(String name) {
    Optional<Project> project = projectRepository.findById(name);
    if (project.isEmpty()) {
      throw new IllegalArgumentException(String.format("Could not find project: \"%s\"", name));
    }
    if (name.equals(Project.DEFAULT_NAME)) {
      throw new UnsupportedOperationException("Archiving the default project is not allowed.");
    }
    Project p = project.get();
    p.setArchived(true);
    projectRepository.saveAndFlush(p);
  }

  /**
   * List all active projects
   *
   * @return List of active projects
   */
  @Transactional
  public List<Project> listProjects() {
    return projectRepository.findAllByArchivedIsFalse();
  }
}
