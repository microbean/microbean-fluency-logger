/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2019 microBean™.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.microbean.fluency.logger;

import java.io.IOException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

import org.komamitsu.fluency.Fluency;

import org.komamitsu.fluency.fluentd.FluencyBuilderForFluentd;

public class FluencyLogHandler extends Handler {

  private final Fluency fluency;

  public FluencyLogHandler() {
    this(new FluencyBuilderForFluentd().build());
  }

  public FluencyLogHandler(final Fluency fluency) {
    super();
    this.fluency = Objects.requireNonNull(fluency);
    this.setFormatter(new SimpleFormatter());
  }

  @Override
  public void close() {
    try {
      this.fluency.close();
    } catch (final IOException | RuntimeException exception) {
      this.reportError(exception.getMessage(), exception, ErrorManager.CLOSE_FAILURE);
    }
  }

  @Override
  public void flush() {
    try {
      this.fluency.flush();
    } catch (final IOException | RuntimeException exception) {
      this.reportError(exception.getMessage(), exception, ErrorManager.FLUSH_FAILURE);
    }
  }

  @Override
  public void publish(final LogRecord logRecord) {
    if (this.isLoggable(logRecord)) {
      final Map<String, Object> event = this.toMap(logRecord);
      if (event != null && !event.isEmpty()) {
        final long logRecordSecondsSinceEpoch = logRecord.getMillis() / 1000L;
        try {
          this.fluency.emit(logRecord.getLoggerName(), logRecordSecondsSinceEpoch, event);
        } catch (final IOException | RuntimeException exception) {
          this.reportError(exception.getMessage(), exception, ErrorManager.WRITE_FAILURE);
        }
      }
    }
  }

  protected Map<String, Object> toMap(final LogRecord logRecord) {
    final Map<String, Object> event = new HashMap<>();
    if (logRecord != null) {
      final Formatter formatter = this.getFormatter();
      final String message;
      if (formatter == null) {
        message = logRecord.getMessage();
      } else {
        message = formatter.format(logRecord);
      }
      final Object[] parametersArray = logRecord.getParameters();
      final Collection<?> parameters;
      if (parametersArray == null || parametersArray.length <= 0) {
        parameters = null;
      } else {
        parameters = Arrays.asList(parametersArray);
      }
      event.put("level", logRecord.getLevel());
      event.put("message", message);
      event.put("parameters", parameters);
      event.put("sequenceNumber", Long.valueOf(logRecord.getSequenceNumber()));
      event.put("sourceClassName", logRecord.getSourceClassName());
      event.put("sourceMethodName", logRecord.getSourceMethodName());
      event.put("threadID", logRecord.getThreadID());
      event.put("thrown", logRecord.getThrown());
    }
    return event;
  }

}
