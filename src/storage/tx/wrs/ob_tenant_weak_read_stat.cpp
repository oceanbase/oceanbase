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

#include "ob_tenant_weak_read_stat.h"
#include "lib/ob_define.h"
namespace oceanbase
{
namespace transaction
{
ObTenantWeakReadStat::ObTenantWeakReadStat() :
  tenant_id_(OB_INVALID_ID),
  total_part_count_(0),
  valid_inner_part_count_(0),
  valid_user_part_count_(0),
  server_version_delta_(0),
  local_cluster_version_delta_(0),
  self_(),

  cluster_heartbeat_post_tstamp_(0),
  cluster_heartbeat_post_count_(0),
  cluster_heartbeat_succ_tstamp_(0),
  cluster_heartbeat_succ_count_(0),
  cluster_master_(),

  self_check_tstamp_(0),
  local_current_tstamp_(0),

  in_cluster_service_(0),
  is_cluster_master_(0),
  cluster_service_epoch_(0),
  cluster_servers_count_(0),
  cluster_skipped_servers_count_(0),
  cluster_version_gen_tstamp_(0),
  cluster_version_delta_(0)
{}

ObTenantWeakReadStat::~ObTenantWeakReadStat()
{
  destroy();
}

void ObTenantWeakReadStat::destroy()
{
  tenant_id_ = OB_INVALID_ID;
  server_version_.reset();
  total_part_count_ = 0;
  valid_inner_part_count_ = 0;
  valid_user_part_count_ = 0;
  server_version_delta_ = 0;
  local_cluster_version_.reset();
  local_cluster_version_delta_ = 0;
  self_.reset();

  cluster_heartbeat_post_tstamp_ = 0;
  cluster_heartbeat_succ_tstamp_ = 0;
  cluster_heartbeat_post_count_ = 0;
  cluster_heartbeat_succ_count_ = 0;
  cluster_master_.reset();

  self_check_tstamp_ = 0;
  local_current_tstamp_ = 0;

  in_cluster_service_ = 0;
  is_cluster_master_ = 0;
  cluster_service_epoch_ = 0;
  cluster_servers_count_ = 0;
  cluster_skipped_servers_count_ = 0;
  cluster_version_gen_tstamp_ = 0;
  cluster_version_.reset();
  cluster_version_delta_ = 0;
  min_cluster_version_.reset();
  max_cluster_version_.reset();
}

}
}
