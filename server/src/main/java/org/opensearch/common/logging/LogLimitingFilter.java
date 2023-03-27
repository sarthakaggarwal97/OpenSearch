/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.Message;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Plugin(name = "LogLimitingFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE)
public class LogLimitingFilter extends AbstractFilter {

    private static class Defaults {
        private static final long THRESHOLD = 5;
        private static final long SIZE = 128;
        private static final long EXPIRY_TIME = 60;
        private static final String PREFIX = "log_limiting.filter.";
        private static final String THRESHOLD_KEY = "log_limiting._settings.threshold";
        private static final String SIZE_KEY = "log_limiting._settings.size";
        private static final String EXPIRY_TIME_KEY = "log_limiting._settings.expiry_time";

        private static final String EMPTY_STRING = "";
    }

    public static final Setting.AffixSetting<String> LOG_LIMITING_FILTER = Setting.prefixKeySetting(
        Defaults.PREFIX,
        (key) -> Setting.simpleString(key, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static final Setting<Long> LOG_LIMITING_THRESHOLD = Setting.longSetting(
        Defaults.THRESHOLD_KEY,
        Defaults.THRESHOLD,
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Long> LOG_LIMITING_SIZE = Setting.longSetting(
        Defaults.SIZE_KEY,
        Defaults.SIZE,
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Long> LOG_LIMITING_EXPIRY_TIME = Setting.longSetting(
        Defaults.EXPIRY_TIME_KEY,
        Defaults.EXPIRY_TIME,
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Map<String, Long> exceptionFrequencyCache = new ConcurrentHashMap<String, Long>();
    private static final Map<String, Boolean> componentSettingsCache = new ConcurrentHashMap<String, Boolean>();
    private Long frequencyThreshold;
    private Long size;
    private final Level level;
    private final long timestamp;
    private long expiryTime;

    public LogLimitingFilter(Settings settings, ClusterSettings clusterSettings) {
        this(Level.DEBUG, Result.ACCEPT, Result.DENY);
        this.frequencyThreshold = LOG_LIMITING_THRESHOLD.get(settings);
        this.size = LOG_LIMITING_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(LOG_LIMITING_THRESHOLD, this::setFrequencyThreshold);
        clusterSettings.addSettingsUpdateConsumer(LOG_LIMITING_SIZE, this::setSize);
        clusterSettings.addSettingsUpdateConsumer(LOG_LIMITING_EXPIRY_TIME, this::setExpiryTime);
    }

    public LogLimitingFilter(Level level, Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
        this.level = level;
        this.timestamp = System.currentTimeMillis();
    }

    public void reset() {
        this.exceptionFrequencyCache.clear();
    }

    public Result filter(Level level, Throwable throwable, String loggerName) {

        if (null == throwable || !level.isLessSpecificThan(this.level) || !validateLogLimiterClassSetting(loggerName)) {
            return Result.NEUTRAL;
        }

        validateCache();

        String exceptionKey = createExceptionKey(throwable);
        Long currentFrequency = exceptionFrequencyCache.getOrDefault(exceptionKey, 0L);

        if (currentFrequency % frequencyThreshold == 1) {
            return Result.ACCEPT;
        }

        exceptionFrequencyCache.put(exceptionKey, currentFrequency + 1);
        return Result.DENY;
    }

    private boolean validateLogLimiterClassSetting(String loggerName) {

        String componentSubpart = Defaults.EMPTY_STRING;
        int componentIndex = 0;
        while (!componentSubpart.equals(loggerName)){
            componentIndex = loggerName.indexOf(".", componentIndex);
            componentSubpart = loggerName.substring(0, componentIndex + 1);
            if (componentSettingsCache.containsKey(componentSubpart)){
                return componentSettingsCache.get(componentSubpart);
            }
        }
        return componentSettingsCache.getOrDefault(loggerName, false);
    }

    private String createExceptionKey(Throwable throwable) {
        Throwable rootCause = Throwables.getRootCause(throwable);
        return rootCause.getClass().getSimpleName();
    }


    @Override
    public Result filter(LogEvent event) {
        return filter(event.getLevel(), event.getThrown(), event.getLoggerName());
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        return filter(level, t, logger.getContext().getName());
    }

    @PluginFactory
    public static LogLimitingFilter createFilter(
        @PluginAttribute(value = "level", defaultString = "DEBUG") Level level,
        @PluginAttribute(value = "onMatch") final Result match,
        @PluginAttribute(value = "onMismatch") final Result mismatch
    ) {
        return new LogLimitingFilter(level, match, mismatch);
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public void setFrequencyThreshold(Long frequencyThreshold) {
        this.frequencyThreshold = frequencyThreshold;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public static void addComponent(String component, String value) {
        final Boolean enable = Boolean.valueOf(value);
        componentSettingsCache.put(component, enable);
    }

    private void validateCache() {

        long duration = System.currentTimeMillis() - this.timestamp;
        long elapsedTime = duration / 1000 / 60;
        if (elapsedTime >= expiryTime || exceptionFrequencyCache.size() >= size) {
            this.reset();
        }
    }

}
