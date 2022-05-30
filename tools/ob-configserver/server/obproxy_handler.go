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
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	// log "github.com/sirupsen/logrus"

	"github.com/oceanbase/configserver/model"
)

const (
	CONFIG_URL_FORMAT = "%s/services?Action=ObRootServiceInfo&ObCluster=%s"
)

var obProxyConfigOnce sync.Once
var obProxyConfigFunc func(*gin.Context)
var obProxyConfigWithTemplateOnce sync.Once
var obProxyConfigWithTemplateFunc func(*gin.Context)

func getObProxyConfigFunc() func(*gin.Context) {
	obProxyConfigOnce.Do(func() {
		obProxyConfigFunc = handlerFunctionWrapper(getObProxyConfig)
	})
	return obProxyConfigFunc
}

func getObProxyConfigWithTemplateFunc() func(*gin.Context) {
	obProxyConfigWithTemplateOnce.Do(func() {
		obProxyConfigWithTemplateFunc = handlerFunctionWrapper(getObProxyConfigWithTemplate)
	})
	return obProxyConfigWithTemplateFunc
}

func getServiceAddress() string {
	return fmt.Sprintf("http://%s:%d", GetConfigServer().Config.Vip.Address, GetConfigServer().Config.Vip.Port)
}

func isVersionOnly(c *gin.Context) (bool, error) {
	ret := false
	var err error
	versionOnly, ok := c.GetQuery("VersionOnly")
	if ok {
		ret, err = strconv.ParseBool(versionOnly)
	}
	return ret, err
}

func getObProxyConfig(ctxlog context.Context, c *gin.Context) *ApiResponse {
	var response *ApiResponse
	client := GetConfigServer().Client

	versionOnly, err := isVersionOnly(c)
	if err != nil {
		return NewIllegalArgumentResponse(errors.Wrap(err, "invalid parameter, failed to parse versiononly"))
	}

	rootServiceInfoUrlMap := make(map[string]*model.RootServiceInfoUrl)
	clusters, err := client.ObCluster.Query().All(context.Background())
	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "query ob clusters"))
	}

	for _, cluster := range clusters {
		rootServiceInfoUrlMap[cluster.Name] = &model.RootServiceInfoUrl{
			ObCluster: cluster.Name,
			Url:       fmt.Sprintf(CONFIG_URL_FORMAT, getServiceAddress(), cluster.Name),
		}
	}
	rootServiceInfoUrls := make([]*model.RootServiceInfoUrl, 0, len(rootServiceInfoUrlMap))
	for _, info := range rootServiceInfoUrlMap {
		rootServiceInfoUrls = append(rootServiceInfoUrls, info)
	}
	obProxyConfig, err := model.NewObProxyConfig(getServiceAddress(), rootServiceInfoUrls)
	if err != nil {
		response = NewErrorResponse(errors.Wrap(err, "generate obproxy config"))
	} else {
		if versionOnly {
			response = NewSuccessResponse(model.NewObProxyConfigVersionOnly(obProxyConfig.Version))
		} else {
			response = NewSuccessResponse(obProxyConfig)
		}
	}
	return response
}

func getObProxyConfigWithTemplate(ctxlog context.Context, c *gin.Context) *ApiResponse {
	var response *ApiResponse
	client := GetConfigServer().Client

	versionOnly, err := isVersionOnly(c)
	if err != nil {
		return NewIllegalArgumentResponse(errors.Wrap(err, "invalid parameter, failed to parse versiononly"))
	}

	clusterMap := make(map[string]interface{})
	clusters, err := client.ObCluster.Query().All(context.Background())

	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "query ob clusters"))
	}

	for _, cluster := range clusters {
		clusterMap[cluster.Name] = nil
	}
	clusterNames := make([]string, 0, len(clusterMap))
	for clusterName := range clusterMap {
		clusterNames = append(clusterNames, clusterName)
	}

	obProxyConfigWithTemplate, err := model.NewObProxyConfigWithTemplate(getServiceAddress(), clusterNames)

	if err != nil {
		response = NewErrorResponse(errors.Wrap(err, "generate obproxy config with template"))
	} else {
		if versionOnly {
			response = NewSuccessResponse(model.NewObProxyConfigVersionOnly(obProxyConfigWithTemplate.Version))
		} else {
			response = NewSuccessResponse(obProxyConfigWithTemplate)
		}
	}
	return response
}
