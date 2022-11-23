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

type ObClusterIdcRegionInfo struct {
	Cluster        string           `json:"ObRegion"`
	ClusterId      int64            `json:"ObRegionId"`
	IdcList        []*IdcRegionInfo `json:"IDCList"`
	ReadonlyRsList string           `json:"ReadonlyRsList"`
}

type IdcRegionInfo struct {
	Idc    string `json:"idc"`
	Region string `json:"region"`
}
