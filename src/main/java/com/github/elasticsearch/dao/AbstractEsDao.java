package com.github.elasticsearch.dao;

import com.github.elasticsearch.client.ElasticsearchTransportClient;
import com.github.elasticsearch.model.AbstractEsIndexParam;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.health.ClusterHealthStatus.YELLOW;

@SuppressWarnings("WeakerAccess")
public class AbstractEsDao {
    public AbstractEsDao(ElasticsearchTransportClient transportClient) {
        assert transportClient != null && transportClient.client() != null;
        this.transportClient = transportClient.client();
    }

    public boolean openIndex(AbstractEsIndexParam indexParam) {
        return openIndex(indexParam.getNewIndexName());
    }

    public boolean openIndex(String... indices) {
        return waitForClusterStatus(YELLOW, indices) && indicesAdmin().prepareOpen(indices).get().isAcknowledged();
    }

    public boolean isIndexOpened(String index) {
        try {
            return waitForClusterStatus(YELLOW, index)
                    && clusterAdmin().state(new ClusterStateRequest().clear().metadata(true).indicesOptions(IndicesOptions.strictExpand()))
                    .get().getState().getMetadata().getIndices()
                    .get(index).getState() == IndexMetadata.State.OPEN;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean closeIndex(AbstractEsIndexParam indexParam) {
        return closeIndex(indexParam.getNewIndexName());
    }

    public boolean closeIndex(String... indices) {
        return waitForClusterStatus(YELLOW, indices) && indicesAdmin().prepareClose(indices).get().isAcknowledged();
    }

    public boolean isIndexClosed(AbstractEsIndexParam indexParam) {
        return isIndexClosed(indexParam.getNewIndexName());
    }

    public boolean isIndexClosed(String index) {
        try {
            return waitForClusterStatus(YELLOW, index)
                    && clusterAdmin().state(new ClusterStateRequest().clear().metadata(true).indicesOptions(IndicesOptions.strictExpand()))
                    .get().getState().getMetadata().getIndices()
                    .get(index).getState() == IndexMetadata.State.CLOSE;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean refreshIndex(String... indices) {
        return indicesAdmin().prepareRefresh(indices).get().getFailedShards() == 0;
    }

    public boolean flushIndex(String... indices) {
        return indicesAdmin().prepareFlush(indices).get().getFailedShards() == 0;
    }

    public boolean createIndex(AbstractEsIndexParam indexParam) {
        return createIndex(indexParam.getNewIndexName(), indexParam.getTypeName(), indexParam.getSettingsJsonString(), indexParam.getMappingJsonString());
    }

    public boolean createIndex(String index, String type, Settings settings, String mappingJsonString) {
        assert mappingJsonString != null;

        CreateIndexRequestBuilder requestBuilder = indicesAdmin().prepareCreate(index);

        if (Objects.nonNull(settings)) {
            requestBuilder.setSettings(settings);
        }

        return requestBuilder
                .addMapping(type, mappingJsonString, XContentType.JSON)
                .get()
                .isAcknowledged() && isExistsIndex(index);
    }

    public boolean createIndex(String index, String type, String mappingJsonString) {
        return createIndex(index, type, "", mappingJsonString);
    }

    public boolean createIndex(String index, String type, Map<String, ?> settingsMap, Map<String, ?> mappingMap) {
        assert MapUtils.isNotEmpty(mappingMap);

        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);

            return createIndex(
                    index,
                    type,
                    MapUtils.isNotEmpty(settingsMap) ? builder.map(settingsMap).toString() : null,
                    builder.map(mappingMap).toString()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean createIndex(String index, String type, Map<String, ?> settingsMap, String mappingJsonString) {
        try {
            return createIndex(
                    index,
                    type,
                    MapUtils.isNotEmpty(settingsMap) ?
                            XContentFactory.contentBuilder(XContentType.JSON).map(settingsMap).toString() : null,
                    mappingJsonString
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean createIndex(String index, String type, String settingsJsonString, String mappingJsonString) {
        assert mappingJsonString != null;

        CreateIndexRequestBuilder requestBuilder = indicesAdmin().prepareCreate(index);

        if (StringUtils.isNotBlank(settingsJsonString)) {
            requestBuilder.setSettings(settingsJsonString, XContentType.JSON);
        }

        return requestBuilder
                .addMapping(type, mappingJsonString, XContentType.JSON)
                .get()
                .isAcknowledged() && isExistsIndex(index);
    }

    public boolean deleteIndex(String... indices) {
        return !isExistsIndex(indices) || indicesAdmin().prepareDelete(indices).get().isAcknowledged();
    }

    public boolean removeAlias(String index, String alias) {
        return indicesAdmin().prepareAliases().removeAlias(index, alias).get().isAcknowledged() && !isExistsIndex(alias);
    }

    public boolean addAlias(String index, String alias) {
        return indicesAdmin().prepareAliases().addAlias(index, alias).get().isAcknowledged() && isExistsIndex(alias);
    }

    public boolean replaceAlias(String index, String currentAlias, String newAlias) {
        return indicesAdmin().prepareAliases()
                .removeAlias(index, currentAlias)
                .addAlias(index, newAlias).get().isAcknowledged() && isExistsIndex(newAlias);
    }

    public boolean swapAlias(String alias, String currentIndex, String newIndex) {
        return indicesAdmin().prepareAliases()
                .removeAlias(currentIndex, alias)
                .addAlias(newIndex, alias).get().isAcknowledged() && isExistsIndex(newIndex);
    }

    public boolean hasAlias(String index, String alias) {
        Optional<Map<String, List<String>>> indexNameAliasesPairMap = getIndexAndAliasesPairByAlias(alias);

        return (indexNameAliasesPairMap.isPresent() &&
                indexNameAliasesPairMap.get().containsKey(index)) &&
                indexNameAliasesPairMap.get().get(index).contains(alias);
    }

    public Optional<Map<String, List<String>>> getIndexAndAliasesPairByAlias(String... alias) {
        ImmutableOpenMap<String, List<AliasMetadata>> aliasesMetaDataMap = indicesAdmin().prepareGetAliases(alias).get().getAliases();

        Map<String, List<String>> indexAliasesPairMap = new HashMap<>();

        if (aliasesMetaDataMap != null && !aliasesMetaDataMap.isEmpty()) {
            aliasesMetaDataMap.forEach(objectCursor ->
                    indexAliasesPairMap.put(
                            objectCursor.key,
                            objectCursor.value.stream().map(AliasMetadata::getAlias).collect(toList())));
        }

        return indexAliasesPairMap.isEmpty() ? Optional.empty() : Optional.of(indexAliasesPairMap);
    }

    public FlushResponse flushIndexResponse(String... indices) {
        return indicesAdmin().prepareFlush(indices).get();
    }

    public boolean isExistsIndex(String... indices) {
        return indicesAdmin().prepareExists(indices).get().isExists();
    }

    public Optional<String> getIndexNameByAlias(String alias) {
        Optional<Map<String, List<String>>> indexNameAliasesPairMap = getIndexAndAliasesPairByAlias(alias);

        if (indexNameAliasesPairMap.isPresent()) {
            return indexNameAliasesPairMap.get().entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().contains(alias))
                    .map(Map.Entry::getKey)
                    .findFirst();
        }

        return Optional.empty();
    }

    public IndexMetadata getIndexInfo(String index) {
        try {
            return clusterAdmin().state(new ClusterStateRequest().clear().metadata(true).indicesOptions(IndicesOptions.strictExpand()))
                    .get().getState().getMetadata().getIndices().get(index);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setEsIndexParamInfo(AbstractEsIndexParam indexParam) {
        indexParam.setCurrentIndexName(getIndexNameByAlias(indexParam.getAlias()).orElse(StringUtils.EMPTY));

        if (StringUtils.isNotEmpty(indexParam.getCurrentIndexName())) {
            String indexNo = indexParam.getCurrentIndexName().substring(indexParam.getCurrentIndexName().length() - 1);
            createIndexName(indexParam, indexNo);
        } else {
            indexParam.setNewIndexName(indexParam.getAlias() + "_1");
            indexParam.setCurrentIndexName(indexParam.getAlias() + "_2");
        }
    }

    private void createIndexName(AbstractEsIndexParam indexParam, String indexNo) {
        switch (indexNo) {
            case "1":
                indexParam.setNewIndexName(indexParam.getAlias() + "_2");
                break;
            case "2":
                indexParam.setNewIndexName(indexParam.getAlias() + ((indexParam.getUseIndexBackup()) ? "_3" : "_1"));
                break;
            case "3":
                indexParam.setNewIndexName(indexParam.getAlias() + "_1");
                break;
        }
    }

    private IndicesAdminClient indicesAdmin() {
        return transportClient.admin().indices();
    }

    private ClusterAdminClient clusterAdmin() {
        return transportClient.admin().cluster();
    }

    private boolean waitForClusterStatus(ClusterHealthStatus highWaterMarkStatus, String... indices) {
        ClusterHealthResponse response =
                clusterAdmin().prepareHealth(indices)
                        .setWaitForStatus(highWaterMarkStatus)
                        .setTimeout(new TimeValue(10_000L, TimeUnit.MILLISECONDS))
                        .get();

        return !response.isTimedOut();
    }

    protected final TransportClient transportClient;
}
