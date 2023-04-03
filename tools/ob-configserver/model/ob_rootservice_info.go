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

type ObRootServiceInfo struct {
	ObClusterId    int64           `json:"ObClusterId"`
	ObRegionId     int64           `json:"ObRegionId"`
	ObCluster      string          `json:"ObCluster"`
	ObRegion       string          `json:"ObRegion"`
	ReadonlyRsList []*ObServerInfo `json:"ReadonlyRsList"`
	RsList         []*ObServerInfo `json:"RsList"`
	Type           string          `json:"Type"`
	TimeStamp      int64           `json:"timestamp"`
}

type ObServerInfo struct {
	Address string `json:"address"`
	Role    string `json:"role"`
	SqlPort int    `json:"sql_port"`
}

func (r *ObRootServiceInfo) Fill() {
	// fill ob cluster and ob region with real
	if len(r.ObCluster) > 0 {
		r.ObRegion = r.ObCluster
	} else if len(r.ObRegion) > 0 {
		r.ObCluster = r.ObRegion
	}

	// fill ob cluster id and ob region id with real
	if r.ObClusterId > 0 {
		r.ObRegionId = r.ObClusterId
	} else if r.ObRegionId > 0 {
		r.ObClusterId = r.ObRegionId
	}
}
