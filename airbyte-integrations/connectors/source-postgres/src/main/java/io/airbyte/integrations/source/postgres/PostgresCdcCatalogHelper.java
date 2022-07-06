package io.airbyte.integrations.source.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.debezium.internals.DebeziumEventUtils;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PostgresCdcCatalogHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCdcCatalogHelper.class);

  private PostgresCdcCatalogHelper() {
  }

  /*
   * It isn't possible to recreate the state of the original database unless we include extra
   * information (like an oid) when using logical replication. By limiting to Full Refresh when we
   * don't have a primary key we dodge the problem for now. As a work around a CDC and non-CDC source
   * could be configured if there's a need to replicate a large non-PK table.
   *
   * Note: in place mutation.
   */
  public static AirbyteStream removeIncrementalWithoutPk(final AirbyteStream stream) {
    if (stream.getSourceDefinedPrimaryKey().isEmpty()) {
      final List<SyncMode> syncModes = new ArrayList<>(stream.getSupportedSyncModes());
      syncModes.remove(SyncMode.INCREMENTAL);
      stream.setSupportedSyncModes(syncModes);
    }

    return stream;
  }

  /*
   * Set all streams that do have incremental to sourceDefined, so that the user cannot set or
   * override a cursor field.
   *
   * Note: in place mutation.
   */
  public static AirbyteStream setIncrementalToSourceDefined(final AirbyteStream stream) {
    if (stream.getSupportedSyncModes().contains(SyncMode.INCREMENTAL)) {
      stream.setSourceDefinedCursor(true);
    }

    return stream;
  }

  // Note: in place mutation.
  public static AirbyteStream addCdcMetadataColumns(final AirbyteStream stream) {
    final ObjectNode jsonSchema = (ObjectNode) stream.getJsonSchema();
    final ObjectNode properties = (ObjectNode) jsonSchema.get("properties");

    final JsonNode stringType = Jsons.jsonNode(ImmutableMap.of("type", "string"));
    final JsonNode numberType = Jsons.jsonNode(ImmutableMap.of("type", "number"));
    properties.set(DebeziumEventUtils.CDC_LSN, numberType);
    properties.set(DebeziumEventUtils.CDC_UPDATED_AT, stringType);
    properties.set(DebeziumEventUtils.CDC_DELETED_AT, stringType);

    return stream;
  }

  /**
   * @return tables included in the publication. When it is not CDC mode, returns an empty set.
   */
  public static Set<AirbyteStreamNameNamespacePair> getPublicizedTables(final JdbcDatabase database) throws SQLException {
    final JsonNode sourceConfig = database.getSourceConfig();
    if (sourceConfig == null || !PostgresUtils.isCdc(sourceConfig)) {
      return Collections.emptySet();
    }

    final String publication = sourceConfig.get("replication_method").get("publication").asText();
    final List<JsonNode> tablesInPublication = database.queryJsons(
        "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?", publication);
    final Set<AirbyteStreamNameNamespacePair> publicizedTables = tablesInPublication.stream()
        .map(table -> new AirbyteStreamNameNamespacePair(table.get("tablename").asText(), table.get("schemaname").asText()))
        .collect(Collectors.toSet());
    LOGGER.info("For CDC, only tables in publication {} will be included in the sync: {}", publication,
        publicizedTables.stream().map(pair -> pair.getNamespace() + "." + pair.getName()).toList());

    return publicizedTables;
  }

}
