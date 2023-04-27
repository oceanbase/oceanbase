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

#define USING_LOG_PREFIX RS_LB
#include "ob_balance_info.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/hash/ob_hashset.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_global_stat_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "ob_unit_manager.h"
#include "ob_zone_manager.h"
#include "ob_root_utils.h"
#include "ob_root_service.h"
#include "ob_resource_weight_parser.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/ob_file_system_router.h"
#include "storage/tx/ob_i_ts_source.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

////////////////
ObStatisticsCalculator::ObStatisticsCalculator()
    :sum_(0)
{}

void ObStatisticsCalculator::reset()
{
  values_.reset();
  sum_ = 0;
}

int ObStatisticsCalculator::add_value(double v)
{
  sum_ += v;
  return values_.push_back(v);
}

double ObStatisticsCalculator::get_avg()
{
  double avg = 0;
  if (values_.count() > 0) {
    avg = sum_ / static_cast<double>(values_.count());
  }
  return avg;
}

double ObStatisticsCalculator::get_standard_deviation()
{
  double sd = 0;
  int64_t n = values_.count();
  if (n > 0) {
    double avg = get_avg();
    FOREACH(it, values_) {
      double d = (*it) - avg;
      sd += d * d;
    }
    sd = sqrt(sd/static_cast<double>(n));
  }
  return sd;
}

int ZoneUnit::assign(const ZoneUnit &other)
{
  int ret = OB_SUCCESS;
  zone_ = other.zone_;
  active_unit_cnt_ = other.active_unit_cnt_;

  load_imbalance_ = other.load_imbalance_;
  cpu_imbalance_ = other.cpu_imbalance_;
  disk_imbalance_ = other.disk_imbalance_;
  iops_imbalance_ = other.iops_imbalance_;
  memory_imbalance_ = other.memory_imbalance_;
  load_avg_ = other.load_avg_;
  cpu_avg_ = other.cpu_avg_;
  disk_avg_ = other.disk_avg_;
  iops_avg_ = other.iops_avg_;
  memory_avg_ = other.memory_avg_;

  tg_pg_cnt_ = other.tg_pg_cnt_;
  if (OB_FAIL(copy_assign(all_unit_, other.all_unit_))) {
    LOG_WARN("failed to assign all_unit_", K(ret));
  }
  return ret;
}

bool ServerStat::can_migrate_in() const
{
  return !blocked_ && active_ && online_;;
}


int UnitStat::assign(const UnitStat &other)
{
  int ret = OB_SUCCESS;
  server_ = other.server_;
  in_pool_ = other.in_pool_;
  load_factor_ = other.load_factor_;
  capacity_ = other.capacity_;
  load_ = other.load_;
  tg_pg_cnt_ = other.tg_pg_cnt_;
  outside_replica_cnt_ = other.outside_replica_cnt_;
  inside_replica_cnt_ = other.inside_replica_cnt_;
  if (OB_FAIL(copy_assign(info_, other.info_))) {
    LOG_WARN("failed to assign info_", K(ret));
  }
  return ret;
}

UnitStat &UnitStat::operator=(const UnitStat &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("fail to assign", K(ret));
    }
  }
  return *this;
}

double UnitStat::get_load_if(ObResourceWeight &weights,
                                      const LoadFactor &load_factor, const bool plus) const
{
  LoadFactor new_factor = load_factor_;
  if (plus) {
    new_factor += load_factor;
  } else {
    new_factor -= load_factor;
  }
  return weights.cpu_weight_ * (new_factor.get_cpu_usage()/get_cpu_limit())
         + weights.memory_weight_ * (new_factor.get_memory_usage()/get_memory_limit())
         + weights.disk_weight_ * (new_factor.get_disk_usage()/get_disk_limit())
         + weights.iops_weight_ * (new_factor.get_iops_usage()/get_iops_limit());
}

double UnitStat::calc_load(ObResourceWeight &weights,
                           const LoadFactor &load_factor) const
{
  return weights.cpu_weight_ * (load_factor.get_cpu_usage()/get_cpu_limit())
         + weights.memory_weight_ * (load_factor.get_memory_usage()/get_memory_limit())
         + weights.disk_weight_ * (load_factor.get_disk_usage()/get_disk_limit())
         + weights.iops_weight_ * (load_factor.get_iops_usage()/get_iops_limit());
}

int ServerReplicaCountMgr::init(const ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  // allow init twice
  inited_ = false;
  server_replica_counts_.reuse();
  ServerReplicaCount server_replica_count;
  FOREACH_CNT_X(server, servers, OB_SUCCESS == ret) {
    if (!server->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid server", "server", *server, K(ret));
    } else {
      server_replica_count.reset();
      server_replica_count.server_ = *server;
      server_replica_count.replica_count_ = 0;
      if (OB_FAIL(server_replica_counts_.push_back(server_replica_count))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int ServerReplicaCountMgr::accumulate(
    const ObAddr &server, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(cnt), K(ret));
  } else {
    FOREACH_CNT(server_replica_count, server_replica_counts_) {
      if (server_replica_count->server_ == server) {
        server_replica_count->replica_count_ += cnt;
        break;
      }
    }
  }
  return ret;
}

int ServerReplicaCountMgr::get_replica_count(
    const ObAddr &server, int64_t &replica_count)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    bool found = false;
    FOREACH_CNT_X(server_replica_count, server_replica_counts_, !found) {
      if (server_replica_count->server_ == server) {
        replica_count = server_replica_count->replica_count_;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}
