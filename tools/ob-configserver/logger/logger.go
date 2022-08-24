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
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

const defaultTimestampFormat = "2006-01-02T15:04:05.99999-07:00"
const INIT_TRACEID = "0000000000000000"

var textFormatter = &TextFormatter{
	TimestampFormat:        "2006-01-02T15:04:05.99999-07:00", // log timestamp format
	FullTimestamp:          true,
	DisableLevelTruncation: true,
	FieldMap: map[string]string{
		"WARNING": "WARN", // log level string, use WARN
	},
	// log caller, filename:line callFunction
	CallerPrettyfier: func(frame *runtime.Frame) (string, string) {
		n := 0
		filename := frame.File
		// 获取包名
		for i := len(filename) - 1; i > 0; i-- {
			if filename[i] == '/' {
				n++
				if n >= 2 {
					filename = filename[i+1:]
					break
				}
			}
		}

		name := frame.Function
		idx := strings.LastIndex(name, ".")
		return name[idx+1:], fmt.Sprintf("%s:%d", filename, frame.Line)
	},
}

type LoggerConfig struct {
	Output     io.Writer
	Level      string `yaml:"level"`
	Filename   string `yaml:"filename"`
	MaxSize    int    `yaml:"maxsize"`
	MaxAge     int    `yaml:"maxage"`
	MaxBackups int    `yaml:"maxbackups"`
	LocalTime  bool   `yaml:"localtime"`
	Compress   bool   `yaml:"compress"`
}

func InitLogger(config LoggerConfig) *logrus.Logger {
	logger := logrus.StandardLogger()
	// log output
	if config.Output == nil {
		logger.SetOutput(&lumberjack.Logger{
			Filename:   config.Filename,
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		})
	} else {
		logger.SetOutput(config.Output)
	}

	// log level
	level, err := logrus.ParseLevel(config.Level)
	if err != nil {
		panic(fmt.Sprintf("parse log level: %+v", err))
	}
	logger.SetLevel(level)

	// log format
	logger.SetFormatter(textFormatter)
	logger.SetReportCaller(true)

	return logger
}
