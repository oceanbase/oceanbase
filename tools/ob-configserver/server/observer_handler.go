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
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/oceanbase/configserver/ent"
	"github.com/oceanbase/configserver/ent/obcluster"
	"github.com/oceanbase/configserver/model"
)

var obRootServiceGetOnce sync.Once
var obRootServiceGetFunc func(*gin.Context)
var obRootServicePostOnce sync.Once
var obRootServicePostFunc func(*gin.Context)
var obRootServiceDeleteOnce sync.Once
var obRootServiceDeleteFunc func(*gin.Context)
var obIdcRegionInfoOnce sync.Once
var obIdcRegionInfoFunc func(*gin.Context)

func getObIdcRegionInfoFunc() func(c *gin.Context) {
	obIdcRegionInfoOnce.Do(func() {
		obIdcRegionInfoFunc = handlerFunctionWrapper(getObIdcRegionInfo)

	})
	return obIdcRegionInfoFunc
}

func getObRootServiceGetFunc() func(*gin.Context) {
	obRootServiceGetOnce.Do(func() {
		obRootServiceGetFunc = handlerFunctionWrapper(getObRootServiceInfo)
	})
	return obRootServiceGetFunc
}

func getObRootServicePostFunc() func(*gin.Context) {
	obRootServicePostOnce.Do(func() {
		obRootServicePostFunc = handlerFunctionWrapper(createOrUpdateObRootServiceInfo)
	})
	return obRootServicePostFunc
}

func getObRootServiceDeleteFunc() func(*gin.Context) {
	obRootServiceDeleteOnce.Do(func() {
		obRootServiceDeleteFunc = handlerFunctionWrapper(deleteObRootServiceInfo)
	})
	return obRootServiceDeleteFunc
}

type RootServiceInfoParam struct {
	ObCluster   string
	ObClusterId int64
	Version     int
}

func getCommonParam(c *gin.Context) (*RootServiceInfoParam, error) {
	var err error
	name := ""
	obCluster, obClusterOk := c.GetQuery("ObCluster")
	obRegion, obRegionOk := c.GetQuery("ObRegion")
	if obClusterOk {
		name = obCluster
	}
	if obRegionOk {
		name = obRegion
	}

	if len(name) == 0 {
		return nil, errors.New("no obcluster or obregion")
	}

	obClusterId, obClusterIdOk := c.GetQuery("ObClusterId")
	obRegionId, obRegionIdOk := c.GetQuery("ObRegionId")

	var clusterId int64
	var clusterIdStr string
	if obClusterIdOk {
		clusterIdStr = obClusterId
	}
	if obRegionIdOk {
		clusterIdStr = obRegionId
	}
	if clusterIdStr != "" {
		clusterId, err = strconv.ParseInt(clusterIdStr, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "parse ob cluster id")
		}
	}

	version := 0
	versionStr, versionOk := c.GetQuery("version")
	if versionOk {
		version, err = strconv.Atoi(versionStr)
		if err != nil {
			return nil, errors.Wrap(err, "parse version")
		}
	}
	return &RootServiceInfoParam{
		ObCluster:   name,
		ObClusterId: clusterId,
		Version:     version,
	}, nil
}

func selectPrimaryCluster(clusters []*model.ObRootServiceInfo) *model.ObRootServiceInfo {
	var primaryCluster *model.ObRootServiceInfo
	for _, cluster := range clusters {
		if primaryCluster == nil {
			primaryCluster = cluster
		} else {
			if primaryCluster.Type != "PRIMARY" {
				if cluster.Type == "PRIMARY" || cluster.TimeStamp > primaryCluster.TimeStamp {
					primaryCluster = cluster
				}
			} else {
				if cluster.Type == "PRIMARY" && cluster.TimeStamp > primaryCluster.TimeStamp {
					primaryCluster = cluster
				}
			}
		}
	}
	return primaryCluster
}

func getObIdcRegionInfo(ctxlog context.Context, c *gin.Context) *ApiResponse {
	// return empty idc list
	param, err := getCommonParam(c)
	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "parse ob idc region info query parameter"))
	}

	rootServiceInfoList, err := getRootServiceInfoList(ctxlog, param.ObCluster, param.ObClusterId)
	if err != nil {
		if rootServiceInfoList != nil && len(rootServiceInfoList) == 0 {
			return NewNotFoundResponse(errors.New(fmt.Sprintf("no obcluster found with query param %v", param)))
		} else {
			return NewErrorResponse(errors.Wrap(err, fmt.Sprintf("get all rootservice info for cluster %s:%d", param.ObCluster, param.ObClusterId)))
		}
	}

	idcList := make([]*model.IdcRegionInfo, 0, 0)
	if param.Version < 2 || param.ObClusterId > 0 {
		primaryCluster := selectPrimaryCluster(rootServiceInfoList)
		obClusterIdcRegionInfo := &model.ObClusterIdcRegionInfo{
			Cluster:        primaryCluster.ObCluster,
			ClusterId:      primaryCluster.ObClusterId,
			IdcList:        idcList,
			ReadonlyRsList: "",
		}
		return NewSuccessResponse(obClusterIdcRegionInfo)
	} else {
		obClusterIdcRegionInfoList := make([]*model.ObClusterIdcRegionInfo, 0, 4)
		for _, cluster := range rootServiceInfoList {
			obClusterIdcRegionInfo := &model.ObClusterIdcRegionInfo{
				Cluster:        cluster.ObCluster,
				ClusterId:      cluster.ObClusterId,
				IdcList:        idcList,
				ReadonlyRsList: "",
			}
			obClusterIdcRegionInfoList = append(obClusterIdcRegionInfoList, obClusterIdcRegionInfo)
		}
		return NewSuccessResponse(obClusterIdcRegionInfoList)
	}
}

func getRootServiceInfoList(ctxlog context.Context, obCluster string, obClusterId int64) ([]*model.ObRootServiceInfo, error) {
	var clusters []*ent.ObCluster
	var err error
	rootServiceInfoList := make([]*model.ObRootServiceInfo, 0, 4)
	client := GetConfigServer().Client

	if obClusterId != 0 {
		log.WithContext(ctxlog).Infof("query ob clusters with obcluster %s and obcluster_id %d", obCluster, obClusterId)
		clusters, err = client.ObCluster.Query().Where(obcluster.Name(obCluster), obcluster.ObClusterID(obClusterId)).All(context.Background())
	} else {
		log.WithContext(ctxlog).Infof("query ob clusters with obcluster %s", obCluster)
		clusters, err = client.ObCluster.Query().Where(obcluster.Name(obCluster)).All(context.Background())
	}
	if err != nil {
		return nil, errors.Wrap(err, "query ob clusters from db")
	}
	if len(clusters) == 0 {
		return rootServiceInfoList, errors.New(fmt.Sprintf("no root service info found with obcluster %s, obcluster id %d", obCluster, obClusterId))
	}
	for _, cluster := range clusters {
		var rootServiceInfo model.ObRootServiceInfo
		err = json.Unmarshal([]byte(cluster.RootserviceJSON), &rootServiceInfo)
		if err != nil {
			return nil, errors.Wrap(err, "deserialize root service info")
		}
		rootServiceInfo.Fill()
		rootServiceInfoList = append(rootServiceInfoList, &rootServiceInfo)
	}
	return rootServiceInfoList, nil
}

func getObRootServiceInfo(ctxlog context.Context, c *gin.Context) *ApiResponse {
	var response *ApiResponse
	param, err := getCommonParam(c)
	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "parse rootservice query parameter"))
	}
	rootServiceInfoList, err := getRootServiceInfoList(ctxlog, param.ObCluster, param.ObClusterId)
	if err != nil {
		if rootServiceInfoList != nil && len(rootServiceInfoList) == 0 {
			return NewNotFoundResponse(errors.New(fmt.Sprintf("no obcluster found with query param %v", param)))
		} else {
			return NewErrorResponse(errors.Wrap(err, fmt.Sprintf("get all rootservice info for cluster %s:%d", param.ObCluster, param.ObClusterId)))
		}
	}

	if param.Version < 2 || param.ObClusterId > 0 {
		log.WithContext(ctxlog).Infof("return primary ob cluster")
		response = NewSuccessResponse(selectPrimaryCluster(rootServiceInfoList))
	} else {
		log.WithContext(ctxlog).Infof("return all ob clusters")
		response = NewSuccessResponse(rootServiceInfoList)
	}
	return response
}

func createOrUpdateObRootServiceInfo(ctxlog context.Context, c *gin.Context) *ApiResponse {
	var response *ApiResponse
	client := GetConfigServer().Client
	obRootServiceInfo := new(model.ObRootServiceInfo)
	err := c.ShouldBindJSON(obRootServiceInfo)
	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "bind rootservice query parameter"))
	}
	obRootServiceInfo.Fill()
	param, err := getCommonParam(c)
	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "parse rootservice query parameter"))
	}
	if len(obRootServiceInfo.ObCluster) == 0 {
		return NewIllegalArgumentResponse(errors.New("ob cluster name is required"))
	}
	if param.Version > 1 {
		if len(obRootServiceInfo.Type) == 0 {
			return NewIllegalArgumentResponse(errors.New("ob cluster type is required when version > 1"))
		}
	}

	rsBytes, err := json.Marshal(obRootServiceInfo)
	if err != nil {
		response = NewErrorResponse(errors.Wrap(err, "serialize ob rootservice info"))
	} else {
		rootServiceInfoJson := string(rsBytes)
		log.WithContext(ctxlog).Infof("store rootservice info %s", rootServiceInfoJson)

		err := client.ObCluster.
			Create().
			SetName(obRootServiceInfo.ObCluster).
			SetObClusterID(obRootServiceInfo.ObClusterId).
			SetType(obRootServiceInfo.Type).
			SetRootserviceJSON(rootServiceInfoJson).
			OnConflict().
			SetRootserviceJSON(rootServiceInfoJson).
			Exec(context.Background())
		if err != nil {
			response = NewErrorResponse(errors.Wrap(err, "save ob rootservice info"))
		} else {
			response = NewSuccessResponse("successful")
		}
	}
	return response
}

func deleteObRootServiceInfo(ctxlog context.Context, c *gin.Context) *ApiResponse {
	var response *ApiResponse
	client := GetConfigServer().Client

	param, err := getCommonParam(c)
	if err != nil {
		return NewErrorResponse(errors.Wrap(err, "parse rootservice query parameter"))
	}
	if param.Version < 2 {
		response = NewIllegalArgumentResponse(errors.New("delete obcluster rs info is only supported when version >= 2"))
	} else if param.ObClusterId == 0 {
		response = NewIllegalArgumentResponse(errors.New("delete obcluster rs info is only supported with obcluster id"))
	} else {
		affected, err := client.ObCluster.
			Delete().
			Where(obcluster.Name(param.ObCluster), obcluster.ObClusterID(param.ObClusterId)).
			Exec(context.Background())
		if err != nil {
			response = NewErrorResponse(errors.Wrap(err, fmt.Sprintf("delete obcluster %s with ob cluster id %d in db", param.ObCluster, param.ObClusterId)))
		} else {
			log.WithContext(ctxlog).Infof("delete obcluster %s with ob cluster id %d in db, affected rows %d", param.ObCluster, param.ObClusterId, affected)
			response = NewSuccessResponse("success")
		}
	}
	return response

}
