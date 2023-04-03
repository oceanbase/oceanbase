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

package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"github.com/oceanbase/configserver/config"
	"github.com/oceanbase/configserver/ent"
	"github.com/oceanbase/configserver/lib/trace"
	"github.com/oceanbase/configserver/logger"
)

var configServer *ConfigServer

func GetConfigServer() *ConfigServer {
	return configServer
}

type ConfigServer struct {
	Config *config.ConfigServerConfig
	Server *HttpServer
	Client *ent.Client
}

func NewConfigServer(conf *config.ConfigServerConfig) *ConfigServer {
	server := &ConfigServer{
		Config: conf,
		Server: &HttpServer{
			Counter: new(Counter),
			Router:  gin.Default(),
			Server:  &http.Server{},
			Address: conf.Server.Address,
		},
		Client: nil,
	}
	configServer = server
	return configServer
}

func (server *ConfigServer) Run() error {
	client, err := ent.Open(server.Config.Storage.DatabaseType, server.Config.Storage.ConnectionUrl)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("initialize storage client with config %v", server.Config.Storage))
	}

	server.Client = client

	defer server.Client.Close()

	if err := server.Client.Schema.Create(context.Background()); err != nil {
		return errors.Wrap(err, "create configserver schema")
	}

	// start http server
	ctx, cancel := context.WithCancel(trace.ContextWithTraceId(logger.INIT_TRACEID))
	server.Server.Cancel = cancel

	// register route
	InitConfigServerRoutes(server.Server.Router)

	// run http server
	server.Server.Run(ctx)
	return nil
}
