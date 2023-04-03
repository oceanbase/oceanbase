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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/oceanbase/configserver/config"
	"github.com/oceanbase/configserver/ent"
)

func TestParseVersionOnlyNormal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	c.Request, _ = http.NewRequest("GET", "http://1.1.1.1:8080/services?Action=GetObproxyConfig&VersionOnly=true", nil)
	versionOnly, err := isVersionOnly(c)
	require.True(t, versionOnly)
	require.True(t, err == nil)
}

func TestParseVersionOnlyError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	c.Request, _ = http.NewRequest("GET", "http://1.1.1.1:8080/services?Action=GetObproxyConfig&VersionOnly=abc", nil)
	_, err := isVersionOnly(c)
	require.True(t, err != nil)
}

func TestParseVersionOnlyNotExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	c.Request, _ = http.NewRequest("GET", "http://1.1.1.1:8080/services?Action=GetObproxyConfig", nil)
	c.Params = []gin.Param{{Key: "Version", Value: "abc"}}
	versionOnly, err := isVersionOnly(c)
	require.False(t, versionOnly)
	require.True(t, err == nil)
}

func TestGetObProxyConfig(t *testing.T) {
	// test gin
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "http://1.1.1.1:8080/services?Action=GetObproxyConfig", nil)

	// mock db client
	client, _ := ent.Open("sqlite3", "file:ent?mode=memory&cache=shared&_fk=1")
	client.Schema.Create(context.Background())

	configServerConfig, _ := config.ParseConfigServerConfig("../etc/config.yaml")
	configServer = &ConfigServer{
		Config: configServerConfig,
		Client: client,
	}

	response := getObProxyConfig(context.Background(), c)
	require.Equal(t, http.StatusOK, response.Code)
}

func TestGetObproxyConfigWithTemplate(t *testing.T) {
	// test gin
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "http://1.1.1.1:8080/services?Action=GetObproxyConfig", nil)

	// mock db client
	client, _ := ent.Open("sqlite3", "file:ent?mode=memory&cache=shared&_fk=1")
	client.Schema.Create(context.Background())

	configServerConfig, _ := config.ParseConfigServerConfig("../etc/config.yaml")
	configServer = &ConfigServer{
		Config: configServerConfig,
		Client: client,
	}

	response := getObProxyConfigWithTemplate(context.Background(), c)
	require.Equal(t, http.StatusOK, response.Code)
}
