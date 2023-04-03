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

#ifndef OCEANBASE_ROOTSERVER_MOCK_LEADER_COORDINATE_H_
#define OCEANBASE_ROOTSERVER_MOCK_LEADER_COORDINATE_H_

#include <gmock/gmock.h>
#include "rootserver/ob_leader_coordinator.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace rootserver
{

class MockLeaderCoordinator : public ObLeaderCoordinator
{
public:
  MOCK_METHOD0(coordinate, int());
  MOCK_METHOD0(smooth_coordinate, int());
  MOCK_METHOD0(start_smooth_coordinate, int());
  MOCK_METHOD1(is_doing_smooth_coordinate, int(bool &));
  MOCK_METHOD1(is_last_switch_turn_succ, int(bool &));
  MOCK_METHOD0(signal, void());
  MOCK_METHOD1(get_leader_stat, int(ObILeaderCoordinator::ServerLeaderStat &leader_stat));
  MOCK_METHOD4(coordinate_partition_group, int(const uint64_t, const int64_t, const common::ObIArray<common::ObAddr> &, const common::ObArray<common::ObZone> &));


  virtual int check_small_tenant(const uint64_t, bool &small_tenant)
  { small_tenant = true; return common::OB_SUCCESS; }

  virtual common::ObLatch &get_lock() { return switch_leader_lock_; }
  virtual common::ObLatch &get_switch_leader_lock() { return switch_leader_lock_; }
  virtual obrpc::ObSrvRpcProxy &get_rpc_proxy() { return rpc_proxy_; }

  common::ObLatch switch_leader_lock_;
  common::ObLatch lock_;
  obrpc::MockObSrvRpcProxy rpc_proxy_;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_MOCK_LEADER_COORDINATE_H_
