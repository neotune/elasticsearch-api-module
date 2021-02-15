package com.github.elasticsearch.client;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.plugins.Plugin;

import java.util.*;

@SuppressWarnings("WeakerAccess")
public class Properties {
    private Collection<Node> nodes;
    private Map<String, String> settings;
    private Collection<Class<? extends Plugin>> plugins;

    public Properties() {
    }

    public Properties(Collection<Node> nodes) {
        this(nodes, null, null);
    }

    public Properties(Collection<Node> nodes, Map<String, String> settings) {
        this(nodes, settings, null);
    }

    public Properties(Collection<Node> nodes,
                                       Map<String, String> settings,
                                       Collection<Class<? extends Plugin>> plugins) {
        this.nodes = nodes;
        this.settings = settings;
        this.plugins = plugins;
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public void setNodes(Collection<Node> nodes) {
        this.nodes = nodes;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        return plugins == null ? Collections.emptyList() : plugins;
    }

    public void setPlugins(Collection<Class<? extends Plugin>> plugins) {
        this.plugins = plugins;
    }

    public void setFullQualifiedPluginNames(Collection<String> fullQualifiedPluginNames) {
        if (fullQualifiedPluginNames != null) {
            List<Class<? extends Plugin>> plugins = new LinkedList<>();
            ClassLoader classLoader = getClass().getClassLoader();

            for (String fullQualifiedPluginName : fullQualifiedPluginNames) {
                try {
                    Class<?> expectedAssignablePlugin = classLoader.loadClass(fullQualifiedPluginName);

                    if (!Plugin.class.isAssignableFrom(expectedAssignablePlugin)) {
                        throw new IllegalArgumentException(
                                "[" + Plugin.class.getName() + "] class의 구현체만 등록 가능합니다.");
                    }

                    plugins.add((Class<? extends Plugin>) expectedAssignablePlugin);

                } catch (ClassNotFoundException cnfe) {
                    throw new RuntimeException("[" + fullQualifiedPluginName + "] class가 없습니다. " +
                            "패키지 및 클래스 이름을 확인하세요.");
                }
            }

            this.plugins = plugins;
        }
    }

    @Override
    public String toString() {
        return "TmonElasticsearchProperties{" +
                "nodes=" + nodes +
                ", settings=" + settings +
                ", plugins=" + plugins +
                '}';
    }

    void validate() {
        if (CollectionUtils.isEmpty(nodes)) {
            throw new IllegalStateException("nodes는 필수 입력 값입니다.");
        }

        if (MapUtils.isEmpty(settings) || !settings.containsKey("cluster.name")) {
            throw new IllegalStateException("settings의 cluster.name은 필수 입력 값입니다.");
        }
    }
}
