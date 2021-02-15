package com.github.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("WeakerAccess")
public abstract class AbstractEsDocument implements Serializable {
    @JsonIgnore
    protected String id;

    @JsonIgnore
    private String parentId;

    @JsonIgnore
    private long version = -1;

    public String getId() {
        return id;
    }

    public final void setId(String id) {
        this.id = id;
    }

    public final String getParentId() {
        return parentId;
    }

    public final void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public final long getVersion() {
        return version;
    }

    public final void setVersion(long version) {
        this.version = version;
    }

    public void postProcessAfterCreated() {}

    @JsonIgnore
    protected <T> Optional<T> getAsOptional(T value) {
        return value == null ? Optional.empty() : Optional.of(value);
    }

    @JsonIgnore
    private static final long serialVersionUID = -845624141611749767L;
}
