/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

package logger

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLogExample(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	_ = buf
	logger := InitLogger(LoggerConfig{
		Output: os.Stdout,
		Level:  "debug",
	})

	// use logger
	logger.Debugf("debug-log-%d", 1)
	logger.WithField("field-key-1", "field-val-1").Infof("info-log-%d", 1)

	// with context, set traceId
	ctx := context.WithValue(context.Background(), TraceIdKey{}, "TRACE-ID")
	ctxlog := logger.WithContext(ctx)
	ctxlog.Debugf("debug-log-%d", 2)
	fieldlog := ctxlog.WithFields(map[string]interface{}{
		"field-key-2": "field-val-2",
		"field-key-3": "field-val-3",
	})
	// use the same field logger to avoid allocte new Entry
	fieldlog.Infof("info-log-%d", 2)
	fieldlog.Infof("info-log-%s", "2.1")

	// use logrus
	logrus.Debugf("debug-log-%d", 3)
	logrus.WithField("field-key-3", "field-val-3").Infof("info-log-%d", 3)
	fmt.Printf("%s", buf.Bytes())
}

func TestLogFile(t *testing.T) {
	InitLogger(LoggerConfig{
		Output:     nil,
		Level:      "debug",
		Filename:   "../tests/test.log",
		MaxSize:    10, // 10M
		MaxAge:     3,  // 3days
		MaxBackups: 3,
		LocalTime:  false,
		Compress:   false,
	})

	// use logrus
	logrus.Debugf("debug-log-%d", 1)
	logrus.WithField("field-key-1", "field-val-1").Infof("info-log-%d", 1)
}
