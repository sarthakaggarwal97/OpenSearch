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
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.ExecutionException;

@Plugin(name = "LogLimitingFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE)
public class LogLimitingFilter extends AbstractFilter {

    private static class Defaults {
        private static final long THRESHOLD = 5;
        private static final long SIZE = 128;
        private static final long EXPIRY_TIME = 60;
        private static final String PREFIX = "log_limiting.filter.";
        private static final String THRESHOLD_KEY = "log_limiting._settings.threshold";
        private static final String LEVEL_KEY = "log_limiting._settings.level";
        private static final String LEVEL = Level.DEBUG.name();

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

    public static final Setting<String> LOG_LIMITING_LEVEL = Setting.simpleString(
        Defaults.LEVEL_KEY,
        Defaults.LEVEL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Cache<String, Long> exceptionFrequencyCache = CacheBuilder.<String, Long>builder()
        .setMaximumWeight(Defaults.SIZE)
        .setExpireAfterAccess(TimeValue.timeValueMinutes(Defaults.EXPIRY_TIME)).build();
    private static final Cache<String, Boolean> componentSettingsCache = CacheBuilder.<String, Boolean>builder()
        .setMaximumWeight(Defaults.SIZE)
        .setExpireAfterAccess(TimeValue.timeValueMinutes(Defaults.EXPIRY_TIME)).build();

    private static Long frequencyThreshold;
    private static Level level;

    public LogLimitingFilter(Settings settings, ClusterSettings clusterSettings) {
        this(Level.DEBUG, Result.ACCEPT, Result.DENY);
        frequencyThreshold = LOG_LIMITING_THRESHOLD.get(settings);
        level = Level.valueOf(LOG_LIMITING_LEVEL.get(settings));
        clusterSettings.addSettingsUpdateConsumer(LOG_LIMITING_THRESHOLD, this::setFrequencyThreshold);
        clusterSettings.addSettingsUpdateConsumer(LOG_LIMITING_LEVEL, this::setLogLimitingLevel);
    }

    public LogLimitingFilter(Level level, Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
        LogLimitingFilter.level = level;
    }

    public Result filter(Level level, Throwable throwable, String loggerName) throws ExecutionException {

        if (null == throwable || !level.isLessSpecificThan(LogLimitingFilter.level) || !validateLogLimiterClassSetting(loggerName)) {
            return Result.NEUTRAL;
        }

        String exceptionKey = createExceptionKey(throwable);
        Long currentFrequency = exceptionFrequencyCache.computeIfAbsent(exceptionKey, key -> 0L);

        exceptionFrequencyCache.put(exceptionKey, currentFrequency + 1);

        if (currentFrequency % frequencyThreshold == 0) {
            return Result.ACCEPT;
        }
        return Result.DENY;
    }

    private boolean validateLogLimiterClassSetting(String loggerName) throws ExecutionException {

        String componentSubpart = Defaults.EMPTY_STRING;
        int componentIndex = 0;
        while (!componentSubpart.equals(loggerName)) {
            componentIndex = loggerName.indexOf(".", componentIndex);
            if (componentIndex == -1) {
                break;
            }
            componentSubpart = loggerName.substring(0, componentIndex);
            componentIndex = componentIndex + 1;
            if (componentSettingsCache.get(componentSubpart) != null) {
                return componentSettingsCache.get(componentSubpart);
            }
        }
        return componentSettingsCache.computeIfAbsent(loggerName, key -> false);
    }

    private String createExceptionKey(Throwable throwable) {
        Throwable rootCause = Throwables.getRootCause(throwable);
        return rootCause.getClass().getSimpleName();
    }

    @Override
    public Result filter(LogEvent event) {
        try {
            return filter(event.getLevel(), event.getThrown(), event.getLoggerName());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        try {
            return filter(level, t, logger.getContext().getName());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @PluginFactory
    public static LogLimitingFilter createFilter(
        @PluginAttribute(value = "level", defaultString = "DEBUG") Level level,
        @PluginAttribute(value = "onMatch") final Result match,
        @PluginAttribute(value = "onMismatch") final Result mismatch
    ) {
        return new LogLimitingFilter(level, match, mismatch);
    }

    public void setFrequencyThreshold(Long frequencyThreshold) {
        LogLimitingFilter.frequencyThreshold = frequencyThreshold;
    }

    public static void addComponent(String component, String value) {
        final Boolean enable = Boolean.valueOf(value);
        componentSettingsCache.put(component, enable);
    }

    private void setLogLimitingLevel(String level) {
        LogLimitingFilter.level = Level.getLevel(level);
    }

}
