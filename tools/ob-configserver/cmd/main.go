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

package main

import (
	"os"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oceanbase/configserver/config"
	"github.com/oceanbase/configserver/logger"
	"github.com/oceanbase/configserver/server"
)

var (
	configserverCommand = &cobra.Command{
		Use:   "configserver",
		Short: "configserver is used to store and query ob rs_list",
		Long:  "configserver is used to store and query ob rs_list, used by observer, obproxy and other tools",
		Run: func(cmd *cobra.Command, args []string) {
			err := runConfigServer()
			if err != nil {
				log.WithField("args:", args).Errorf("start configserver failed: %v", err)
			}
		},
	}
)

func init() {
	configserverCommand.PersistentFlags().StringP("config", "c", "etc/config.yaml", "config file")
	_ = viper.BindPFlag("config", configserverCommand.PersistentFlags().Lookup("config"))
}

func main() {
	if err := configserverCommand.Execute(); err != nil {
		log.WithField("args", os.Args).Errorf("configserver execute failed %v", err)
	}
}

func runConfigServer() error {
	configFilePath := viper.GetString("config")
	configServerConfig, err := config.ParseConfigServerConfig(configFilePath)
	if err != nil {
		return errors.Wrap(err, "read and parse configserver config")
	}

	// init logger
	logger.InitLogger(logger.LoggerConfig{
		Level:      configServerConfig.Log.Level,
		Filename:   configServerConfig.Log.Filename,
		MaxSize:    configServerConfig.Log.MaxSize,
		MaxAge:     configServerConfig.Log.MaxAge,
		MaxBackups: configServerConfig.Log.MaxBackups,
		LocalTime:  configServerConfig.Log.LocalTime,
		Compress:   configServerConfig.Log.Compress,
	})

	// init config server
	configServer := server.NewConfigServer(configServerConfig)

	err = configServer.Run()
	if err != nil {
		return errors.Wrap(err, "start config server")
	}

	return nil
}
