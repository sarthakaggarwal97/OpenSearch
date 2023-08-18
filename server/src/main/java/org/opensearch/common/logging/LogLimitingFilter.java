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
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestStatus;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Filter to limit occurrences of error/exception logs based on their type
 *
 * @opensearch.internal
 */
@Plugin(name = "LogLimitingFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE)
public class LogLimitingFilter extends AbstractFilter {

    private static class Defaults {
        private static final long THRESHOLD = 5;
        private static final long CACHE_SIZE = 128;
        private static final long EXPIRY_TIME = 120;
        private static final Level LEVEL = Level.DEBUG;
    }

    private static class ModuleSettings {
        public Level level;
        public Long threshold;
        ModuleSettings(Level level, Long threshold) {
            this.level = level;
            this.threshold = threshold;
        }
    }

    public static final String LOG_LIMITING_FILTER_PREFIX = "log_limiting.filter.";
    public static final String LOG_LIMITING_THRESHOLD_PREFIX = "log_limiting._settings.threshold.";
    public static final String LOG_LIMITING_LEVEL_PREFIX = "log_limiting._settings.level.";

    public enum Entry {
        NEW_ENTRY,
        THRESHOLD_MODIFICATION,
        LEVEL_MODIFICATION
    }

    public static final Setting<Boolean> LOG_LIMITING_FILTER =
        Setting.prefixKeySetting(
            LOG_LIMITING_FILTER_PREFIX,
            (key) -> Setting.boolSetting(key, false, Setting.Property.Dynamic, Setting.Property.NodeScope)
        );

    public static final Setting<Long> LOG_LIMITING_THRESHOLD =
        Setting.prefixKeySetting(
            LOG_LIMITING_THRESHOLD_PREFIX,
            (key) -> Setting.longSetting(key, Defaults.THRESHOLD, 5, Setting.Property.Dynamic, Setting.Property.NodeScope)
        );

    public static final Setting<Level> LOG_LIMITING_LEVEL =
        Setting.prefixKeySetting(
            LOG_LIMITING_LEVEL_PREFIX,
            (key) -> new Setting<>(key, Level.DEBUG.name(), Level::valueOf, Setting.Property.Dynamic, Setting.Property.NodeScope)
        );

    private static final Cache<String, Long> exceptionFrequencyCache = CacheBuilder.<String, Long>builder()
        .setMaximumWeight(Defaults.CACHE_SIZE)
        .setExpireAfterAccess(TimeValue.timeValueMinutes(Defaults.EXPIRY_TIME)).build();
    private static final Cache<String, ModuleSettings> componentSettingsCache = CacheBuilder.<String, ModuleSettings>builder()
        .setMaximumWeight(Defaults.CACHE_SIZE).build();

    public LogLimitingFilter(Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
    }

    public Result filter(Level level, Throwable throwable, String loggerName) throws ExecutionException {
        ModuleSettings moduleSettings = getModuleSettings(loggerName);

        if (null == throwable || null==moduleSettings || !level.isLessSpecificThan(moduleSettings.level)) {
            return Result.NEUTRAL;
        }

        String exceptionKey = createExceptionKey(throwable);
        Long currentFrequency = exceptionFrequencyCache.computeIfAbsent(exceptionKey, key -> 0L);

        exceptionFrequencyCache.put(exceptionKey, currentFrequency + 1);

        if (currentFrequency % moduleSettings.threshold == 0) {
            return Result.ACCEPT;
        }
        return Result.DENY;
    }

    private ModuleSettings getModuleSettings(String loggerName) {
        int componentIndex = loggerName.length();
        Cache<String, ModuleSettings> s = componentSettingsCache;
        while (componentIndex > 0) {
            String componentSubpart = loggerName.substring(0, componentIndex);
            if (componentSettingsCache.get(componentSubpart) != null) {
                return componentSettingsCache.get(componentSubpart);
            }
            componentIndex = loggerName.lastIndexOf('.', componentIndex - 1);
        }
        return null;
    }

    private String createExceptionKey(Throwable throwable) {
        RestStatus status = ExceptionsHelper.status(throwable);
        int st = status.getStatus();
        Throwable rootCause = Throwables.getRootCause(throwable);
        Class<? extends Throwable> cl = rootCause.getClass();
        String s = cl.getSimpleName();
        return s;
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
        @PluginAttribute(value = "onMatch") final Result match,
        @PluginAttribute(value = "onMismatch") final Result mismatch
    ) {
        return new LogLimitingFilter(match, mismatch);
    }

    public static void addComponent(String component, String value, Entry task) {
        switch(task) {
            case NEW_ENTRY:
                if (Objects.isNull(value) || Boolean.valueOf(value)==false) {
                    componentSettingsCache.invalidate(component);
                } else {
                    componentSettingsCache.put(component, new ModuleSettings(Defaults.LEVEL, Defaults.THRESHOLD));
                }
                break;

            case LEVEL_MODIFICATION:
                if (componentSettingsCache.get(component) != null) {
                    componentSettingsCache.put(component, new ModuleSettings(Level.getLevel(value), componentSettingsCache.get(component).threshold));
                }
                break;

            case THRESHOLD_MODIFICATION:
                if (componentSettingsCache.get(component) != null) {
                    componentSettingsCache.put(component, new ModuleSettings(componentSettingsCache.get(component).level, Long.parseLong(value)));
                }
                break;
        }
    }

}
