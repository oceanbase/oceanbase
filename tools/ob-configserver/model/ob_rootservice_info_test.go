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
	"github.com/stretchr/testify/require"
	"testing"
)

type Service struct {
	Address string `json:"address"`
}

func TestFillObRegion(t *testing.T) {
	info := &ObRootServiceInfo{
		ObCluster:   "helloworld",
		ObClusterId: 1,
	}
	info.Fill()
	require.Equal(t, int64(1), info.ObRegionId)
	require.Equal(t, "helloworld", info.ObRegion)
}

func TestFillObCluster(t *testing.T) {
	info := &ObRootServiceInfo{
		ObRegion:   "helloworld",
		ObRegionId: 1,
	}
	info.Fill()
	require.Equal(t, int64(1), info.ObClusterId)
	require.Equal(t, "helloworld", info.ObCluster)
}
