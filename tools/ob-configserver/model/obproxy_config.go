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

package model

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

const (
	OBPROXY_BIN_URL_FORMAT        = "%s/client?Action=GetObProxy"
	CONFIG_URL_FORMAT_TEMPLATE_V1 = "%s/services?Action=ObRootServiceInfo&ObRegion=${ObRegion}"
	CONFIG_URL_FORMAT_TEMPLATE_V2 = "%s/services?Action=ObRootServiceInfo&version=2&ObCluster=${ObCluster}&ObClusterId=${OBClusterId}"
)

type ObProxyConfig struct {
	ObProxyBinUrl string                `json:"ObProxyBinUrl"`
	MetaDatabase  *MetaDatabaseInfo     `json:"ObProxyDatabaseInfo"`
	ConfigUrlList []*RootServiceInfoUrl `json:"ObRootServiceInfoUrlList"`
	Version       string                `json:"Version"`
}

type ObProxyConfigWithTemplate struct {
	ObProxyBinUrl string            `json:"ObProxyBinUrl"`
	MetaDatabase  *MetaDatabaseInfo `json:"ObProxyDatabaseInfo"`
	Version       string            `json:"Version"`
	ObClusters    []string          `json:"ObClusterList"`
	TemplateV1    string            `json:"ObRootServiceInfoUrlTemplate"`
	TemplateV2    string            `json:"ObRootServiceInfoUrlTemplateV2"`
}

type ObProxyConfigVersionOnly struct {
	Version string `json:"Version"`
}

type MetaDatabaseInfo struct {
	Database  string `json:"DataBase"`
	ConfigUrl string `json:"MetaDataBase"`
	Password  string `json:"Password"`
	User      string `json:"User"`
}

type RootServiceInfoUrl struct {
	ObCluster string `json:"ObRegion"`
	Url       string `json:"ObRootServiceInfoUrl"`
}

func NewDefaultMetaDatabaseInfo(serviceAddress string) *MetaDatabaseInfo {
	return &MetaDatabaseInfo{
		Database:  "***",
		User:      "***",
		Password:  "***",
		ConfigUrl: fmt.Sprintf("%s/services?Action=ObRootServiceInfo&User_ID=alibaba&UID=admin&ObRegion=obdv1", serviceAddress),
	}
}

func NewObProxyConfigVersionOnly(version string) *ObProxyConfigVersionOnly {
	return &ObProxyConfigVersionOnly{
		Version: version,
	}
}

func NewObProxyConfig(serviceAddress string, configUrlList []*RootServiceInfoUrl) (*ObProxyConfig, error) {
	obProxyBinUrl := fmt.Sprintf(OBPROXY_BIN_URL_FORMAT, serviceAddress)
	metaDatabaseInfo := NewDefaultMetaDatabaseInfo(serviceAddress)
	metaJson, err := json.Marshal(metaDatabaseInfo)
	if err != nil {
		return nil, errors.Wrap(err, "encode obproxy metadb")
	}
	configUrlJson, err := json.Marshal(configUrlList)
	if err != nil {
		return nil, errors.Wrap(err, "encode config urls")
	}
	strForMd5 := string(configUrlJson) + string(metaJson) + obProxyBinUrl
	h := md5.New()
	h.Write([]byte(strForMd5))
	version := hex.EncodeToString(h.Sum(nil))
	return &ObProxyConfig{
		ObProxyBinUrl: obProxyBinUrl,
		MetaDatabase:  NewDefaultMetaDatabaseInfo(serviceAddress),
		ConfigUrlList: configUrlList,
		Version:       version,
	}, nil
}

func NewObProxyConfigWithTemplate(serviceAddress string, clusterNames []string) (*ObProxyConfigWithTemplate, error) {
	obProxyBinUrl := fmt.Sprintf(OBPROXY_BIN_URL_FORMAT, serviceAddress)
	metaDatabaseInfo := NewDefaultMetaDatabaseInfo(serviceAddress)
	metaJson, err := json.Marshal(metaDatabaseInfo)
	if err != nil {
		return nil, errors.Wrap(err, "encode obproxy metadb")
	}

	clusterNamesJson, err := json.Marshal(clusterNames)
	if err != nil {
		return nil, errors.Wrap(err, "encode cluster names")
	}

	templateStrV1 := fmt.Sprintf(CONFIG_URL_FORMAT_TEMPLATE_V1, serviceAddress)
	templateV1Json, err := json.Marshal(templateStrV1)
	if err != nil {
		return nil, errors.Wrap(err, "encode config url template v1")
	}

	templateStrV2 := fmt.Sprintf(CONFIG_URL_FORMAT_TEMPLATE_V2, serviceAddress)
	templateV2Json, err := json.Marshal(templateStrV2)
	if err != nil {
		return nil, errors.Wrap(err, "encode config url template v2")
	}

	strForMd5 := string(clusterNamesJson) + string(templateV1Json) + string(templateV2Json) + string(metaJson) + obProxyBinUrl
	h := md5.New()
	h.Write([]byte(strForMd5))
	version := hex.EncodeToString(h.Sum(nil))
	return &ObProxyConfigWithTemplate{
		ObProxyBinUrl: obProxyBinUrl,
		MetaDatabase:  NewDefaultMetaDatabaseInfo(serviceAddress),
		ObClusters:    clusterNames,
		TemplateV1:    templateStrV1,
		TemplateV2:    templateStrV2,
		Version:       version,
	}, nil
}
