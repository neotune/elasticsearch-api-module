package com.github.elasticsearch.model;

import org.elasticsearch.index.VersionType;

import java.util.Optional;

public class AbstractEsIndexParam implements IAbstractEsIndexParam {
    private static final String TYPE = "_doc";

    private final String alias;

    private final String shards;

    private final String replicas;

    private String currentIndexName;

    private String newIndexName;

    private String mappingJsonString;

    private String settingsJsonString;

    private VersionType versionType;

    private boolean useCurrentIndexName;

    private boolean useIndexBackup;

    private boolean backendIndexWrite;

    public AbstractEsIndexParam(String alias, String shards, String replicas) {
        this.alias = alias;
        this.shards = shards;
        this.replicas = replicas;
    }

    public String getIndexName() {
        return useCurrentIndexName ? currentIndexName : newIndexName;
    }

    @Override
    public String getTypeName() {
        return TYPE;
    }

    @Override
    public String getShards() {
        return shards;
    }

    @Override
    public String getReplicas() {
        return replicas;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public String getCurrentIndexName() { return currentIndexName; }

    @Override
    public void setCurrentIndexName(String currentIndexName) { this.currentIndexName = currentIndexName; }

    @Override
    public String getNewIndexName() { return newIndexName; }

    @Override
    public void setNewIndexName(String newIndexName) { this.newIndexName = newIndexName; }

    @Override
    public void setMappingJsonString(String mappingJsonString) { this.mappingJsonString = mappingJsonString; }

    @Override
    public String getSettingsJsonString() { return settingsJsonString; }

    @Override
    public void setSettingsJsonString(String settingsJsonString) { this.settingsJsonString = settingsJsonString; }

    @Override
    public String getMappingJsonString() { return mappingJsonString; }

    @Override
    public Optional<VersionType> getVersionType() { return getAsOptional(versionType); }

    @Override
    public void setVersionType(VersionType versionType) {
        this.versionType = versionType;
    }

    @Override
    public boolean getUseCurrentIndexName() { return useCurrentIndexName; }

    @Override
    public void setUseCurrentIndexName(boolean useCurrentIndexName) { this.useCurrentIndexName = useCurrentIndexName; }

    @Override
    public boolean getUseIndexBackup() { return useIndexBackup; }

    @Override
    public void setUseIndexBackup(boolean useIndexBackup) { this.useIndexBackup = useIndexBackup; }

    @Override
    public boolean getBackendIndexWrite() { return backendIndexWrite; }

    @Override
    public void setBackendIndexWrite(boolean backendIndexWrite) { this.backendIndexWrite = backendIndexWrite; }
}
