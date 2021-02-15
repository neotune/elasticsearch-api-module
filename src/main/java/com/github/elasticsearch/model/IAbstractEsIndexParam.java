package com.github.elasticsearch.model;

import org.elasticsearch.index.VersionType;

import java.util.Optional;

public interface IAbstractEsIndexParam {
    String getTypeName();
    String getShards();
    String getReplicas();
    String getAlias();
    String getCurrentIndexName();
    void setCurrentIndexName(String currentIndexName);
    String getNewIndexName();
    void setNewIndexName(String newIndexName);
    String getMappingJsonString();
    void setMappingJsonString(String mappingJsonString);
    String getSettingsJsonString();
    void setSettingsJsonString(String settingsJsonString);
    Optional<VersionType> getVersionType();
    void setVersionType(VersionType versionType);
    boolean getUseCurrentIndexName();
    void setUseCurrentIndexName(boolean useCurrentIndexName);
    boolean getUseIndexBackup();
    void setUseIndexBackup(boolean useIndexBackup);
    boolean getBackendIndexWrite();
    void setBackendIndexWrite(boolean useIndexBackup);

    default <T> Optional<T> getAsOptional(T value) {
        return value == null ? Optional.empty() : Optional.of(value);
    }
}
