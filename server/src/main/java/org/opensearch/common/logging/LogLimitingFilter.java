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
import org.opensearch.common.settings.Setting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Plugin(name = "LogLimitingFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE)
public class LogLimitingFilter extends AbstractFilter {

    private static class Defaults {
        private static final long THRESHOLD = 5;
    }

    private final Map<String, Long> exceptionFrequencyCache = new ConcurrentHashMap<String, Long>();

    public static final Setting<Long> LOG_LIMITING_THRESHOLD = Setting.longSetting(
        "log_limiting.threshold",
        Defaults.THRESHOLD,
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    //TODO: ADDING SETTINGS FOR DYNAMIC LOG LIMITING FOR CLASS

    private final Long frequencyThreshold = 4L;
    private final Level level;

    public LogLimitingFilter() {
        this(Level.DEBUG, Result.ACCEPT, Result.DENY);
    }

    public LogLimitingFilter(Level level, Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
        this.level = level;
    }

    public void reset() {
        this.exceptionFrequencyCache.clear();
    }

    public Result filter(Level level, Throwable throwable, String loggerName) {

        if (null == throwable || !level.isLessSpecificThan(this.level) || !validateLogLimiterClassSetting(loggerName)) {
            return Result.NEUTRAL;
        }

        String exceptionKey = createExceptionKey(throwable);
        Long currentFrequency = exceptionFrequencyCache.getOrDefault(exceptionKey, 0L);

        if (currentFrequency + 1 == frequencyThreshold) {
            exceptionFrequencyCache.remove(exceptionKey);
            return Result.ACCEPT;
        } else {
            exceptionFrequencyCache.put(exceptionKey, currentFrequency + 1);
        }

        return Result.DENY;
    }

    private boolean validateLogLimiterClassSetting(String loggerName) {
        //TODO: Have the logic to place if the log limiting setting is applied for the class from where the log is sent
        return true;
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
}
