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
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_server_manager.h"
#include "ob_unit_manager.h"
#include "ob_zone_manager.h"
#include "ob_root_utils.h"
#include "ob_root_service.h"
#include "ob_replica_stat_operator.h"
#include "ob_resource_weight_parser.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_cluster_info_proxy.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

bool Partition::is_leader_valid() const
{
  bool bret = false;
  if (has_leader_) {
    bret = true;
  } else if (in_physical_restore()) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

bool Partition::can_balance() const
{
  bool bret = false;
  if (!can_balance_) {
    bret = false;
  } else if (in_physical_restore()) {
    bret = true;
  } else if (has_leader_) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

bool Partition::is_valid_quorum() const
{
  bool bret = false;
  if (quorum_ > 0) {
    bret = true;
  } else if (in_physical_restore()) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

bool Replica::need_rebuild_only_in_member_list() const
{
  bool bret = false;
  const int64_t INTERVAL = 5 * 60 * 1000 * 1000;
  int64_t now = ObTimeUtility::current_time();
  if (in_member_list_ && only_in_member_list()) {
    if (member_time_us_ + INTERVAL > now) {
      bret = false;
    } else {
      LOG_INFO("need rebuild only in member list", K(bret), K(member_time_us_), K(is_in_service()), K(now), K(*this));
      bret = true;
    }
  } else {
    bret = false;
  }
  return bret;
}

bool Replica::is_in_blacklist(int64_t task_type, ObAddr dest_server, const TenantBalanceStat* tenant_stat) const
{
  bool bret = false;
  int64_t now = ObTimeUtility::current_time();
  if (GCONF.balance_blacklist_failure_threshold == 0) {
    // do nothing ,close black list;
  } else if (OB_ISNULL(tenant_stat)) {
    LOG_WARN("tenant stat is null");
  } else if (0 > failmsg_start_pos_ || 0 > failmsg_count_) {
  } else if ((failmsg_start_pos_ + failmsg_count_) < tenant_stat->all_failmsg_.count()) {
    for (int64_t j = 0; j < failmsg_count_; ++j) {  // update fail list
      const share::ObPartitionReplica::FailMsg& fail_msg = tenant_stat->all_failmsg_.at(failmsg_start_pos_ + j);
      if (fail_msg.dest_server_ == dest_server && fail_msg.task_type_ == task_type &&
          fail_msg.get_failed_count() >= GCONF.balance_blacklist_failure_threshold &&
          GCONF.balance_blacklist_retry_interval > (now - fail_msg.get_last_fail_time())) {
        bret = true;
        ObRebalanceTaskType task_type = ObRebalanceTaskType(fail_msg.task_type_);
        LOG_INFO("task in black list",
            K(fail_msg),
            "task_type",
            ObRebalanceTask::get_task_type_str(task_type),
            "from server",
            *server_,
            "to server",
            dest_server);
        break;
      }
    }
  }
  return bret;
}
////////////////
ObStatisticsCalculator::ObStatisticsCalculator() : sum_(0)
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
    FOREACH(it, values_)
    {
      double d = (*it) - avg;
      sd += d * d;
    }
    sd = sqrt(sd / static_cast<double>(n));
  }
  return sd;
}

void LoadFactor::set_resource_usage(ObReplicaStat& rstat)
{
  sstable_read_rate_ = rstat.sstable_read_rate_;
  sstable_read_bytes_rate_ = rstat.sstable_read_bytes_rate_;
  sstable_write_rate_ = rstat.sstable_write_rate_;
  sstable_write_bytes_rate_ = rstat.sstable_write_bytes_rate_;
  log_write_rate_ = rstat.log_write_rate_;
  log_write_bytes_rate_ = rstat.log_write_bytes_rate_;
  memtable_bytes_ = rstat.memtable_bytes_;
  cpu_utime_rate_ = rstat.cpu_utime_rate_;
  cpu_stime_rate_ = rstat.cpu_stime_rate_;
  net_in_rate_ = rstat.net_in_rate_;
  net_in_bytes_rate_ = rstat.net_in_bytes_rate_;
  net_out_rate_ = rstat.net_out_rate_;
  net_out_bytes_rate_ = rstat.net_out_bytes_rate_;
}

int ZoneUnit::assign(const ZoneUnit& other)
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
  return !blocked_ && active_ && online_;
}

int UnitStat::assign(const UnitStat& other)
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

/* Calculate capacity_ratio:
 * 1 gts is not in this unit:
 *   capacity_ratio_ coefficient is 1.0
 * 2 gts is in this unit:
 *   tps_per_core: The upper limit of tps for a single core
 *   gts_capacity_per_core:
 *   The upper limit of gts service's tips for a single core
 *   unit_cnt: the number of units on this zone
 *   core_per_unit: the number of cpu count for each unit
 *   total_core=unit_cnt*core_per_unit: The total number of cores in the zone
 *   total_tps=total_core*tps_per_core: The upper limit of tps that all cores can provide
 *   gts_core_cnt=total_tps/gts_capacity_per_core: The number of cores reserved for gts
 *   capacity_ratio=gts_core_cnt/core_per_unit;
 */
int UnitStat::set_capacity_ratio(const uint64_t tenant_id, const bool is_gts_service_unit)
{
  int ret = OB_SUCCESS;
  if (!is_gts_service_unit) {
    capacity_ratio_ = 1.0;
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    // There is no single-core cpu tps (Single statement insert transaction),
    // And single-core cpu to provide gts data.
    // Assume a data:
    // int64_t gts_capacity_per_core = 100 * 1000; // single-core gts 10w tps
    // int64_t tps_per_core = 5000; // Single core provides 5000 tps service capability
    // const int64_t unit_cnt = info_.pool_.unit_count_;
    // const double total_core = core_per_unit * static_cast<double>(unit_cnt);
    // const double total_tps = total_core * static_cast<double>(tps_per_core);
    if (!tenant_config.is_valid()) {
      capacity_ratio_ = 1.0;
    } else {
      const double gts_core_cnt = tenant_config->_gts_core_num;
      const double core_per_unit = info_.config_.min_cpu_;
      const double tmp_capacity_ratio = gts_core_cnt / core_per_unit;
      const double EPS = 0.000001;
      if (tmp_capacity_ratio < EPS) {
        capacity_ratio_ = 1.0;
      } else {
        if (1 - tmp_capacity_ratio < 0.1) {
          capacity_ratio_ = 0.0;
        } else if (1 - tmp_capacity_ratio < 0.2) {
          capacity_ratio_ = 0.1;
        } else if (1 - tmp_capacity_ratio < 0.3) {
          capacity_ratio_ = 0.2;
        } else if (1 - tmp_capacity_ratio < 0.4) {
          capacity_ratio_ = 0.3;
        } else if (1 - tmp_capacity_ratio < 0.5) {
          capacity_ratio_ = 0.4;
        } else if (1 - tmp_capacity_ratio < 0.6) {
          capacity_ratio_ = 0.5;
        } else if (1 - tmp_capacity_ratio < 0.7) {
          capacity_ratio_ = 0.6;
        } else if (1 - tmp_capacity_ratio < 0.8) {
          capacity_ratio_ = 0.7;
        } else if (1 - tmp_capacity_ratio < 0.9) {
          capacity_ratio_ = 0.8;
        } else {
          capacity_ratio_ = 0.9;
        }
      }
    }
  }
  LOG_INFO("set unit capacity ratio", "unit_id", info_.unit_.unit_id_, K(capacity_ratio_), K(tenant_id));
  return ret;
}
UnitStat& UnitStat::operator=(const UnitStat& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("fail to assign", K(ret));
    }
  }
  return *this;
}

double UnitStat::get_load_if(ObResourceWeight& weights, const LoadFactor& load_factor, const bool plus) const
{
  LoadFactor new_factor = load_factor_;
  if (plus) {
    new_factor += load_factor;
  } else {
    new_factor -= load_factor;
  }
  return weights.cpu_weight_ * (new_factor.get_cpu_usage() / get_cpu_limit()) +
         weights.memory_weight_ * (new_factor.get_memory_usage() / get_memory_limit()) +
         weights.disk_weight_ * (new_factor.get_disk_usage() / get_disk_limit()) +
         weights.iops_weight_ * (new_factor.get_iops_usage() / get_iops_limit());
}

double UnitStat::calc_load(ObResourceWeight& weights, const LoadFactor& load_factor) const
{
  return weights.cpu_weight_ * (load_factor.get_cpu_usage() / get_cpu_limit()) +
         weights.memory_weight_ * (load_factor.get_memory_usage() / get_memory_limit()) +
         weights.disk_weight_ * (load_factor.get_disk_usage() / get_disk_limit()) +
         weights.iops_weight_ * (load_factor.get_iops_usage() / get_iops_limit());
}

LoadFactor Partition::get_max_load_factor(ObResourceWeight& weights, const common::ObArray<Replica>& all_replica)
{
  LoadFactor max;
  double max_sum = 0;
  FOR_BEGIN_END(r, *this, all_replica)
  {
    double sum = r->load_factor_.get_weighted_sum(weights);
    if (sum > max_sum) {
      max = r->load_factor_;
      max_sum = sum;
    }
  }
  return max;
}

int ServerReplicaCountMgr::init(const ObIArray<ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  // allow init twice
  inited_ = false;
  server_replica_counts_.reuse();
  ServerReplicaCount server_replica_count;
  FOREACH_CNT_X(server, servers, OB_SUCCESS == ret)
  {
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

int ServerReplicaCountMgr::accumulate(const ObAddr& server, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(cnt), K(ret));
  } else {
    FOREACH_CNT(server_replica_count, server_replica_counts_)
    {
      if (server_replica_count->server_ == server) {
        server_replica_count->replica_count_ += cnt;
        break;
      }
    }
  }
  return ret;
}

int ServerReplicaCountMgr::get_replica_count(const ObAddr& server, int64_t& replica_count)
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
    FOREACH_CNT_X(server_replica_count, server_replica_counts_, !found)
    {
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

bool ObPartitionGroupOrder::operator()(const Partition* left, const Partition* right)
{
  int cmp = -1;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // error happen, do nothing
  } else if (NULL == left || NULL == right) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(ret), KP(left), KP(right));
  } else {
    // 1. For one belonging to tablegroup and one not belonging to tablegroup,
    //    sort priority:
    // tablegroup_id
    cmp = compare(left->tablegroup_id_, right->tablegroup_id_);
    if (0 == cmp) {
      // 2. For non-tablegroup, sort priority
      // table_id, partition_idx
      if (OB_INVALID_ID == left->tablegroup_id_) {
        cmp = compare(left->table_id_, right->table_id_);
        if (0 == cmp) {
          cmp = compare(left->partition_idx_, right->partition_idx_);
        }
      } else {
        // 3. For tablegroup, sort priority
        // tablegroup_id, partition_idx, schema_partition_cnt, table_id
        cmp = compare(left->partition_idx_, right->partition_idx_);
        if (0 == cmp) {
          cmp = compare(right->schema_partition_cnt_, left->schema_partition_cnt_);  // the big cnt if in front
          if (0 == cmp) {
            cmp = compare(left->table_id_, right->table_id_);
          }
        }
      }
    }
  }
  return cmp < 0;
}

int TenantBalanceStat::reuse_replica_count_mgr()
{
  int ret = OB_SUCCESS;
  ObZone zone;  // empty means all zones
  ObArray<ObServerStatus> statuses;
  ObArray<ObAddr> interested;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_mgr_->get_server_statuses(zone, statuses))) {
    LOG_WARN("get_server_statuses failed", K(zone), K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    FOREACH_CNT_X(status, statuses, OB_SUCCESS == ret)
    {
      if (ObServerStatus::OB_SERVER_ADMIN_DELETING == status->admin_status_ || status->need_check_empty(now)) {
        if (OB_FAIL(interested.push_back(status->server_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replica_count_mgr_.init(interested))) {
        LOG_WARN("replica_count_mgr_ init failed", K(ret));
      }
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
TenantBalanceStat::TenantBalanceStat()
    : unit_stat_map_(),
      server_stat_map_(),
      all_replica_(),
      all_partition_(),
      all_failmsg_(),
      sorted_partition_(),
      all_pg_(),
      sorted_pg_(),
      all_tg_(),
      all_table_(),
      all_zone_unit_(),
      tenant_id_(OB_INVALID_ID),
      schema_guard_(NULL),
      readonly_info_(),
      min_source_replica_version_(0),
      inited_(false),
      valid_(false),
      unit_mgr_(NULL),
      server_mgr_(NULL),
      pt_operator_(NULL),
      remote_pt_operator_(NULL),
      task_mgr_(NULL),
      zone_mgr_(NULL),
      check_stop_provider_(NULL),
      seq_generator_(),
      sql_proxy_(NULL),
      filter_logonly_unit_(false),
      gts_partition_pos_(-1)
{}

void TenantBalanceStat::reuse()
{
  schema_guard_ = NULL;
  tenant_id_ = OB_INVALID_ID;
  all_zone_unit_.reuse();
  all_tg_.reuse();
  all_table_.reuse();
  all_pg_.reuse();
  sorted_pg_.reuse();
  sorted_partition_.reuse();
  all_partition_.reuse();
  all_replica_.reuse();
  server_stat_map_.reuse();
  unit_stat_map_.reuse();
  partition_map_.reuse();
  valid_ = false;
  ru_total_.reset();
  ru_capacity_.reset();
  readonly_info_.reuse();
  all_zone_paxos_info_.reuse();
  gts_partition_pos_ = -1;
  all_failmsg_.reuse();
  min_source_replica_version_ = 0;
  // member not reset:
  // inited_
  // balancer_
}

int TenantBalanceStat::init(ObUnitManager& unit_mgr, ObServerManager& server_mgr,
    share::ObPartitionTableOperator& pt_operator, share::ObRemotePartitionTableOperator& remote_pt_operator,
    ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr, share::ObCheckStopProvider& check_stop_provider,
    common::ObMySQLProxy& sql_proxy, const bool filter_logonly_unit)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    if (OB_FAIL(all_zone_unit_.reserve(MAX_ZONE_NUM))) {
      LOG_WARN("reserve array failed", K(ret), LITERAL_K(MAX_ZONE_NUM));
    } else if (OB_FAIL(server_stat_map_.init(MAX_SERVER_CNT))) {
      LOG_WARN("server stat map init failed", K(ret), LITERAL_K(MAX_SERVER_CNT));
    } else if (OB_FAIL(unit_stat_map_.init(MAX_UNIT_CNT))) {
      LOG_WARN("unit stat map init failed", K(ret), LITERAL_K(MAX_UNIT_CNT));
    } else if (OB_FAIL(partition_map_.create(10240LL, ObModIds::OB_REBALANCE_TASK_MGR))) {
      LOG_WARN("partition map init failed", K(ret));
    } else {
      unit_mgr_ = &unit_mgr;
      server_mgr_ = &server_mgr;
      pt_operator_ = &pt_operator;
      remote_pt_operator_ = &remote_pt_operator;
      zone_mgr_ = &zone_mgr;
      task_mgr_ = &task_mgr;
      check_stop_provider_ = &check_stop_provider;
      filter_logonly_unit_ = filter_logonly_unit;
      min_source_replica_version_ = 0;
      sql_proxy_ = &sql_proxy;

      inited_ = true;
    }
  }
  return ret;
}

int TenantBalanceStat::add_partition(const int64_t schema_partition_cnt, const int64_t schema_replica_cnt,
    const int64_t schema_full_replica_cnt, const share::ObPartitionInfo& info, const uint64_t tablegroup_id,
    const int64_t partition_idx, const bool can_do_rereplicate,
    const share::schema::ObPartitionSchema* partition_schema)
{
  int ret = OB_SUCCESS;
  Partition p;
  bool has_flag_replica = false;
  const ObPartitionReplica* leader_replica = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (schema_replica_cnt <= 0 || !info.is_valid() || OB_ISNULL(partition_schema)) {
    // tablegroup_id can be invalid (for table with no tablegroup).
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_replica_cnt), K(info), KP(partition_schema));
  } else {
    p.table_id_ = info.get_table_id();
    p.tablegroup_id_ = tablegroup_id;
    p.partition_id_ = info.get_partition_id();
    p.partition_idx_ = partition_idx;
    p.schema_partition_cnt_ = schema_partition_cnt;
    p.schema_replica_cnt_ = schema_replica_cnt;
    p.schema_full_replica_cnt_ = schema_full_replica_cnt;
    p.can_rereplicate_ = can_do_rereplicate;
    p.begin_ = all_replica_.count();
    p.in_physical_restore_ = info.get_replicas_v2().count() > 0;  // init

    FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      if (OB_FAIL(add_replica(*r))) {
        LOG_WARN("add replica failed", K(ret), "replica", *r);
      } else if (p.in_physical_restore_ && !r->in_physical_restore_status()) {
        p.in_physical_restore_ = false;
      }
    }
    if (OB_SUCC(ret)) {
      // 1. Find the nearest leader in the main scenario
      // 2. Find the previous leader in an unowned scenario
      if (OB_FAIL(info.find_latest_leader(leader_replica))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get leader", K(ret), K(info));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(leader_replica) && !leader_replica->in_physical_restore_status()) {
        // When there is a leader, record the quorum stored in the current clog
        p.quorum_ = leader_replica->quorum_;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(leader_replica) && !leader_replica->in_physical_restore_status()) {
      if (OB_FAIL(try_add_only_in_member_list_replica(leader_replica, info, has_flag_replica))) {
        LOG_WARN("fail to add only in_member_list replica", K(ret), KPC(leader_replica), K(info));
      }
    }
  }
  if (OB_SUCC(ret)) {
    p.end_ = all_replica_.count();
    p.has_flag_replica_ = has_flag_replica;
    // Always try to migrate follower first.
    FOR_BEGIN_END(r, p, all_replica_)
    {
      if (r->is_leader_by_election()) {
        if (&(*r) != &all_replica_.at(all_replica_.count() - 1)) {
          std::swap(*r, all_replica_.at(all_replica_.count() - 1));
        }
        break;
      }
    }
    ObPartitionKey part_key = p.get_key();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(all_partition_.push_back(p))) {
      LOG_WARN("push partition to all_partition failed", K(ret));
    } else if (OB_FAIL(partition_map_.set_refactored(part_key, all_partition_.count() - 1))) {
      LOG_WARN("failed to add into map", K(ret), K(p), K(part_key));
    } else if (extract_pure_id(part_key.get_table_id()) == OB_ALL_DUMMY_TID) {
      gts_partition_pos_ = all_partition_.count() - 1;
    } else {
    }  // by pass
  }
  return ret;
}

// insert flag replica exist only in leader member list
int TenantBalanceStat::try_add_only_in_member_list_replica(
    const ObPartitionReplica*& leader_replica, const ObPartitionInfo& partition_info, bool& has_flag_replica)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(leader_replica)) {
    // skip
  } else if (OB_INVALID_ID == partition_info.get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(partition_info));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr is null", K(ret));
  } else {
    ObSEArray<ObPartitionReplica::Member, ObPartitionReplica::DEFAULT_REPLICA_COUNT> flag_members;
    if (OB_FAIL(get_flag_member_array(leader_replica, partition_info, flag_members))) {
      LOG_WARN("fail to build only in member_list replica info", K(ret), KPC(leader_replica), K(partition_info));
    } else {
      // add flag member
      for (int64_t i = 0; OB_SUCC(ret) && i < flag_members.count(); i++) {
        ObReplicaType replica_type = REPLICA_TYPE_FULL;
        share::ObPartitionReplica::Member& member = flag_members.at(i);
        ObZone zone;
        // ignore get server zone error.
        int tmp_ret = server_mgr_->get_server_zone(member.server_, zone);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("find server zone failed", K(tmp_ret), "server", member.server_);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_flag_replica(member, replica_type, zone))) {
            LOG_WARN("add flag replica failed", K(ret), K(partition_info), K(member), K(replica_type), K(zone));
          }
        }
      }
      if (OB_SUCC(ret) && flag_members.count() > 0) {
        has_flag_replica = true;
      }
    }
  }
  return ret;
}

int TenantBalanceStat::get_flag_member_array(const share::ObPartitionReplica* leader_replica,
    const share::ObPartitionInfo& partition_info, common::ObIArray<share::ObPartitionReplica::Member>& flag_members)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == leader_replica)) {
    // skip
  } else if (OB_INVALID_ID == partition_info.get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(partition_info));
  } else {
    flag_members.reset();
    FOREACH_CNT_X(m, leader_replica->member_list_, OB_SUCC(ret))
    {
      bool found = false;
      FOREACH_CNT_X(r, partition_info.get_replicas_v2(), !found && OB_SUCC(ret))
      {
        if (r->server_ == m->server_) {
          found = true;
        }
      }
      if (!found) {
        if (OB_FAIL(flag_members.push_back(*m))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int TenantBalanceStat::add_readonly_info(
    const int64_t& table_id, const common::ObZone& zone, const bool& readonly_at_all)
{
  int ret = OB_SUCCESS;
  ReadonlyInfo info;
  info.table_id_ = table_id;
  info.zone_ = zone;
  info.readonly_at_all_ = readonly_at_all;
  for (int64_t i = 0; i < readonly_info_.count() && OB_SUCC(ret); i++) {
    if (table_id == readonly_info_.at(i).table_id_ && zone == readonly_info_.at(i).zone_) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("fail to add readonly info", K(ret), K(table_id), K(zone));
    }
  }
  if (OB_FAIL(ret)) {
    // nothint todo
  } else if (OB_FAIL(readonly_info_.push_back(info))) {
    LOG_WARN("fail to push back info", K(ret), K(info), K(readonly_info_));
  }
  return ret;
}

int TenantBalanceStat::get_readonly_at_all_info(
    const int64_t& table_id, const common::ObZone& zone, bool& readonly_at_all)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(zone));
  } else {
    bool find = false;
    readonly_at_all = false;
    for (int64_t i = 0; !find && i < readonly_info_.count(); ++i) {
      if (readonly_info_.at(i).table_id_ == table_id && readonly_info_.at(i).zone_ == zone) {
        readonly_at_all = readonly_info_.at(i).readonly_at_all_;
        find = true;
      }
    }
  }
  return ret;
}

int TenantBalanceStat::add_replica(const share::ObPartitionReplica& r)
{
  int ret = OB_SUCCESS;
  bool readonly_at_all = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!r.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "replica", r);
  } else if (OB_FAIL(get_readonly_at_all_info(r.table_id_, r.zone_, readonly_at_all))) {
    LOG_WARN("fail to get readonly at all info", K(ret), K(r));
  } else {
    Replica rstat;
    rstat.load_factor_.set_disk_used(r.required_size_);
    rstat.zone_ = r.zone_;
    rstat.role_ = r.role_;
    rstat.member_time_us_ = r.member_time_us_;
    rstat.set_in_member_list(r.in_member_list_);
    rstat.replica_status_ = r.replica_status_;
    rstat.rebuild_ = r.rebuild_;
    rstat.is_restore_ = r.is_restore_;
    rstat.data_version_ = r.data_version_;
    rstat.replica_type_ = r.replica_type_;
    rstat.memstore_percent_ = r.get_memstore_percent();
    rstat.readonly_at_all_ = readonly_at_all;
    rstat.modify_time_us_ = r.modify_time_us_;
    rstat.additional_replica_status_ = r.additional_replica_status_;
    rstat.quorum_ = r.quorum_;
    ObPartitionReplica::FailList fail_list;
    if (r.fail_list_.empty()) {
    } else if (OB_FAIL(ObPartitionReplica::text2fail_list(r.fail_list_.ptr(), fail_list))) {
      LOG_WARN("fail to get fail list from str", K(ret), K(r.fail_list_));
      rstat.failmsg_start_pos_ = -1;
      rstat.failmsg_count_ = 0;
      ret = OB_SUCCESS;
    } else {
      int64_t count = fail_list.count();
      rstat.failmsg_count_ = count;
      rstat.failmsg_start_pos_ = all_failmsg_.count();
      for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
        if (OB_FAIL(all_failmsg_.push_back(fail_list.at(i)))) {
          LOG_WARN("fail msg push back failed", K(ret), K(i), K(fail_list));
        }
      }
      ret = OB_SUCCESS;  // Ignore errors about faillist
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_replica(rstat, r.unit_id_, r.server_))) {
      LOG_WARN("add replica stat failed", K(ret), K(rstat), K(r));
    }
  }
  return ret;
}

int TenantBalanceStat::add_replica(Replica& r, const uint64_t unit_id, const ObAddr& server)
{
  int ret = OB_SUCCESS;
  UnitStatMap::Item* u = NULL;
  ServerStatMap::Item* s = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    // unit_id can be invalid (for replica not assign to unit)
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(unit_stat_map_.locate(unit_id, u))) {
    LOG_WARN("locate unit stat failed", K(ret), K(unit_id));
  } else if (NULL == u) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unit stat", K(ret));
  } else if (OB_FAIL(server_stat_map_.locate(server, s))) {
    LOG_WARN("locate server stat failed", K(ret), K(server));
  } else if (NULL == s) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null server stat", K(ret));
  } else {
    r.unit_ = &u->v_;
    r.server_ = &s->v_;
    if (OB_FAIL(zone_mgr_->get_region(r.zone_, r.region_))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // The replica may be an invalid replica,
        // and the zone where the replica is located has been deleted,
        // There is no need to collect such replica information.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail get region name", "zone", r.zone_, K(ret));
      }
    } else if (OB_FAIL(all_replica_.push_back(r))) {
      LOG_WARN("push replica to all_replica failed", K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::add_flag_replica(
    const ObPartitionReplica::Member& member, const ObReplicaType replica_type, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!member.server_.is_valid() || REPLICA_TYPE_MAX == replica_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(member), K(replica_type));
  } else {
    Replica r;
    r.member_time_us_ = member.timestamp_;
    r.zone_ = zone;
    r.role_ = FOLLOWER;
    r.set_in_member_list(true);
    r.replica_status_ = REPLICA_STATUS_FLAG;
    r.data_version_ = -1;
    r.rebuild_ = false;
    r.replica_type_ = replica_type;
    r.readonly_at_all_ = false;
    r.quorum_ = OB_INVALID_COUNT;
    const uint64_t unit_id = 0;
    if (OB_FAIL(add_replica(r, unit_id, member.server_))) {
      LOG_WARN("add replica failed", K(ret), K(r), K(unit_id), K(member));
    }
  }
  return ret;
}

int TenantBalanceStat::estimate_part_and_replica_cnt(
    const common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas,
    const common::ObIArray<const ObTablegroupSchema*>& tg_schemas, int64_t& part_cnt, int64_t& replica_cnt)
{
  int ret = OB_SUCCESS;
  int64_t zone_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone_count(zone_count))) {
    LOG_WARN("fail to get zone count", K(ret));
  } else {
    part_cnt = 0;
    replica_cnt = 0;
    ObClusterType cluster_type = ObClusterInfoGetter::get_cluster_type_v2();
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); ++i) {
      int64_t replica_num = OB_INVALID_COUNT;
      const ObSimpleTableSchemaV2* t = table_schemas.at(i);
      if (NULL == t) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else if (!((t)->has_self_partition())) {
        // has no partition, ignore
      } else if (STANDBY_CLUSTER == cluster_type) {
        replica_num = zone_count;
      } else if (OB_FAIL((t)->get_paxos_replica_num(*schema_guard_, replica_num))) {
        LOG_WARN("fail to get paxos replica num", K(ret));
      }

      int64_t part_num = 0;
      if (OB_FAIL(ret)) {
      } else if (!((t)->has_self_partition())) {
        // by pass
      } else if (OB_UNLIKELY(replica_num <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("paxos replica num error",
            K(ret),
            K(replica_num),
            "table_id",
            (t)->get_table_id(),
            K(zone_count),
            K(cluster_type));
      } else if (OB_FAIL(t->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all part num", K(ret), KPC(t));
      } else {
        part_cnt += part_num;
        replica_cnt += part_num * replica_num;
      }
    }

    for (int64_t i = 0; i < tg_schemas.count() && OB_SUCC(ret); ++i) {
      int64_t replica_num = OB_INVALID_COUNT;
      const ObTablegroupSchema* t = tg_schemas.at(i);
      if (NULL == t) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else if (!((t)->has_self_partition())) {
        // has no partition, ignore
      } else if (STANDBY_CLUSTER == cluster_type) {
        replica_num = zone_count;
      } else if (OB_FAIL((t)->get_paxos_replica_num(*schema_guard_, replica_num))) {
        LOG_WARN("fail to get paxos replica num", K(ret));
      }

      int64_t part_num = 0;
      if (OB_FAIL(ret)) {
      } else if (!((t)->has_self_partition())) {
        // by pass
      } else if (OB_UNLIKELY(replica_num <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("paxos replica num error",
            K(ret),
            K(replica_num),
            "tg_id",
            (t)->get_tablegroup_id(),
            K(zone_count),
            K(cluster_type));
      } else if (OB_FAIL(t->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all part num", K(ret), KPC(t));
      } else {
        part_cnt += part_num;
        replica_cnt += part_num * replica_num;
      }
    }
  }
  return ret;
}

int TenantBalanceStat::fill_standalone_table_partitions(
    const common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2* table = table_schemas.at(i);
    if (OB_UNLIKELY(nullptr == table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema ptr is null", K(ret));
    } else if (!table->has_self_partition()) {
      // bypass
    } else if (OB_FAIL(fill_partition_entity(table->get_table_id(), *table))) {
      LOG_WARN("fail to fill partition entity", K(ret), "table_id", table->get_table_id());
    }
  }
  return ret;
}

int TenantBalanceStat::fill_binding_tablegroup_partitions(const common::ObIArray<const ObTablegroupSchema*>& tg_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tg_schemas.count() && OB_SUCC(ret); ++i) {
    const ObTablegroupSchema* tablegroup = tg_schemas.at(i);
    if (OB_UNLIKELY(nullptr == tablegroup)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablegroup schema ptr is null", K(ret));
    } else if (!tablegroup->has_self_partition()) {
      // bypass
    } else if (OB_FAIL(fill_partition_entity(tablegroup->get_tablegroup_id(), *tablegroup))) {
      LOG_WARN("fail to fill partition entity", K(ret), "tg_id", tablegroup->get_tablegroup_id());
    }
  }
  return ret;
}

int TenantBalanceStat::fill_partitions()
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2*> tables;
  ObArray<const ObTablegroupSchema*> tablegroups;
  int64_t part_cnt = 0;
  int64_t replica_cnt = 0;
  all_table_.reuse();
  all_partition_.reuse();
  all_replica_.reuse();
  all_failmsg_.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schemas_in_tenant(tenant_id_, tables))) {
    LOG_WARN("get tenant table schemas failed", K(ret), K_(tenant_id));
  } else if (schema_guard_->is_tenant_schema_valid(tenant_id_) &&
             OB_FAIL(schema_guard_->get_tablegroup_schemas_in_tenant(tenant_id_, tablegroups))) {
    LOG_WARN("fail to get tenant tablegroup schemas", K(ret), K_(tenant_id));
  } else if (OB_FAIL(fill_tables(tables, tablegroups))) {
    LOG_WARN("fail fill tables", K(ret));
  } else if (OB_FAIL(estimate_part_and_replica_cnt(tables, tablegroups, part_cnt, replica_cnt))) {
    LOG_WARN("fail to estimate part and replica cnt", K(ret));
  } else if (OB_FAIL(all_partition_.reserve(part_cnt))) {
    LOG_WARN("fail to reserver array", K(ret), K(part_cnt));
  } else if (OB_FAIL(all_replica_.reserve(replica_cnt * 3 / 2))) {
    LOG_WARN("fail to reserve array", K(ret), "reserve_replica_cnt", replica_cnt * 3 / 2);
  } else if (OB_FAIL(fill_standalone_table_partitions(tables))) {
    LOG_WARN("fail to fill standalone table partitions", K(ret));
  } else if (OB_FAIL(fill_binding_tablegroup_partitions(tablegroups))) {
    LOG_WARN("fail to fill binding tablegroup partitions", K(ret));
  }
  return ret;
}

int TenantBalanceStat::fill_tables(
    const ObIArray<const ObSimpleTableSchemaV2*>& tables, const ObIArray<const ObTablegroupSchema*>& tablegroups)
{
  int ret = OB_SUCCESS;
  if (tables.count() > 0 || tablegroups.count() > 0) {
    if (OB_FAIL(all_table_.reserve(tables.count() + tablegroups.count()))) {
      LOG_WARN("fail reserver", "count", tables.count(), K(ret));
    }
    ARRAY_FOREACH_X(tables, idx, cnt, OB_SUCC(ret))
    {
      uint64_t table_id = tables.at(idx)->get_table_id();
      if (!(tables.at(idx)->has_self_partition())) {
        // has no partition, ignore
      } else if (OB_FAIL(all_table_.push_back(table_id))) {
        LOG_WARN("fail push table_id", K(table_id), K(ret));
      }
    }
    ARRAY_FOREACH_X(tablegroups, idx, cnt, OB_SUCC(ret))
    {
      uint64_t tablegroup_id = tablegroups.at(idx)->get_tablegroup_id();
      if (!(tablegroups.at(idx)->has_self_partition())) {
        // has no partition, ignore
      } else if (OB_FAIL(all_table_.push_back(tablegroup_id))) {
        LOG_WARN("fail push table_id", K(tablegroup_id), K(ret));
      }
    }
  }
  return ret;
}

int TenantBalanceStat::calc_resource_weight(
    const LoadFactor& ru_usage, const LoadFactor& ru_capacity, ObResourceWeight& resource_weight)
{
  int ret = OB_SUCCESS;
  bool valid_config = false;
  if (GCONF.enable_unit_balance_resource_weight) {
    if (OB_FAIL(ObResourceWeightParser::parse(GCONF.unit_balance_resource_weight, resource_weight))) {
      LOG_WARN("fail parse unit_balance_resource_weight", "conf_str", GCONF.unit_balance_resource_weight.str(), K(ret));
    } else {
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1440) {
        if (!resource_weight.is_valid()) {
          LOG_ERROR("Resource weight sum does not equal to 1. "
                    "Use auto calc weight instead. Please fix system parameters and retry.",
              K(resource_weight),
              "weight_sum",
              resource_weight.sum());
        } else {
          valid_config = true;
        }
      } else {
        if (!resource_weight.is_valid()) {
          LOG_ERROR("Resource weight sum does not equal to 1. "
                    "Use auto calc weight instead. Please fix system parameters and retry.",
              K(resource_weight),
              "weight_sum",
              resource_weight.sum());
        } else {
          valid_config = true;
          if (resource_weight.iops_weight_ > common::OB_DOUBLE_EPSINON) {
            // Prompt the administrator to update parameters through alarm
            LOG_ERROR("iops weight is deprecated,"
                      "please update unit_balance_resource_weight config",
                K(resource_weight));
          }
          // Recalculate weight, remove iops
          resource_weight.iops_weight_ = 0;
          resource_weight.cpu_weight_ = 0;
          resource_weight.memory_weight_ = 0;
          resource_weight.normalize();
        }
      }
    }
    ret = OB_SUCCESS;  // will fallback to auto calc weight
  }
  if (!valid_config || !GCONF.enable_unit_balance_resource_weight) {
    LOG_DEBUG("resource usage total", K(ru_usage));
    LOG_DEBUG("resource usage total limit", K(ru_capacity));
    double cpu_usage = ru_usage.get_cpu_usage() / ru_capacity.get_cpu_capacity();
    double disk_usage = ru_usage.get_disk_usage() / ru_capacity.get_disk_capacity();
    double iops_usage = ru_usage.get_iops_usage() / ru_capacity.get_iops_capacity();
    double memory_usage = ru_usage.get_memory_usage() / ru_capacity.get_memory_capacity();

    // Based on the current algorithm, memory usage may exceed 1,
    // forcibly set to 1, as an approximation
    memory_usage = memory_usage > 1 ? 1 : memory_usage;

    double sum = cpu_usage + disk_usage + iops_usage + memory_usage;
    if (sum > 0) {
      resource_weight.disk_weight_ = disk_usage / sum;
      resource_weight.cpu_weight_ = cpu_usage / sum;
      resource_weight.iops_weight_ = iops_usage / sum;
      resource_weight.memory_weight_ = memory_usage / sum;
    } else {
      resource_weight.disk_weight_ = 0;
      resource_weight.cpu_weight_ = 0;
      resource_weight.iops_weight_ = 0;
      resource_weight.memory_weight_ = 0;
    }
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_1440) {
      resource_weight.cpu_weight_ = 0;
      resource_weight.iops_weight_ = 0;
      resource_weight.memory_weight_ = 0;
      resource_weight.normalize();
    }
    LOG_DEBUG("resource usage rate", K(cpu_usage), K(disk_usage), K(iops_usage), K(memory_usage));
  }
  LOG_DEBUG("resource weight", K(resource_weight));
  return ret;
}

int TenantBalanceStat::fill_servers()
{
  int ret = OB_SUCCESS;
  ObServerManager::ObServerStatusArray server_statuses;
  ObZone zone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (OB_FAIL(server_mgr_->get_server_statuses(zone, server_statuses))) {
    LOG_WARN("get all server status failed", K(ret));
  } else {
    FOREACH_X(s, server_statuses, OB_SUCCESS == ret)
    {
      ServerStatMap::Item* item = NULL;
      bool zone_active = false;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (OB_UNLIKELY(nullptr == s)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server ptr is null", K(ret));
      } else if (OB_FAIL(server_stat_map_.locate(s->server_, item))) {
        LOG_WARN("locate server status failed", K(ret));
      } else if (NULL == item) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL item", K(ret));
      } else if (OB_FAIL(zone_mgr_->check_zone_active(s->zone_, zone_active))) {
        LOG_WARN("fail to check zone active", K(ret), "zone", s->zone_);
      } else {
        item->v_.online_ = s->is_alive() && s->in_service();
        item->v_.alive_ = s->is_alive();
        item->v_.permanent_offline_ = s->is_permanent_offline();
        item->v_.active_ = !s->is_permanent_offline() && ObServerStatus::OB_SERVER_ADMIN_NORMAL == s->admin_status_;
        item->v_.blocked_ = s->is_migrate_in_blocked();
        item->v_.stopped_ = s->is_stopped() || !zone_active;
        item->v_.resource_info_ = s->resource_info_;
      }
    }
  }
  return ret;
}

int TenantBalanceStat::get_gts_switch(bool& on)
{
  int ret = OB_SUCCESS;
  bool tmp_on = false;
  int64_t gts_value = 0;
  bool is_restore = false;
  if (OB_UNLIKELY(NULL == schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard ptr is null", KR(ret));
  } else if (OB_FAIL(schema_guard_->check_tenant_is_restore(tenant_id_, is_restore))) {
    LOG_WARN("fail to to check tenant is restore", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard_->get_timestamp_service_type(tenant_id_, gts_value))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      if (GCTX.is_standby_cluster() || is_restore) {
        // When building a tenant in the standby cluster,
        // the tenant-level system table partition needs to be supplemented by RS,
        // in order to make the process run through.
        //
        // At this time, it is considered that GTS is not turned on,
        // and the load capacity of all units is equal.
        // After the subsequent tenant system table data is synchronized and successfully refreshed,
        // the actual GTS switch can be perceived
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get tenant system variable failed", KR(ret));
      }
    }
  } else {
    tmp_on = (transaction::TS_SOURCE_GTS == gts_value);
  }
  if (OB_SUCC(ret)) {
    on = tmp_on;
  } else {
    LOG_WARN("get gts switch failed", KR(ret), K(tenant_id_));
  }
  return ret;
}

int TenantBalanceStat::get_primary_partition_key(const int64_t all_pg_idx, common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(all_pg_idx < 0 || all_pg_idx >= all_pg_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(all_pg_idx), "all_pg_cnt", all_pg_.count());
  } else {
    const Partition* primary_part = nullptr;
    const PartitionGroup& this_pg = all_pg_.at(all_pg_idx);
    const int64_t part_idx = this_pg.begin_;
    if (OB_UNLIKELY(part_idx < 0 || part_idx >= sorted_partition_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part idx unexpected", K(ret), K(part_idx), "all_part_cnt", sorted_partition_.count());
    } else if (OB_UNLIKELY(nullptr == (primary_part = sorted_partition_.at(part_idx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary part is null", K(ret));
    } else if (OB_FAIL(gen_partition_key(*primary_part, pkey))) {
      LOG_WARN("fail to gen partition key", K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::fill_units()
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> all_unit;
  const share::schema::ObTenantSchema* tenant_schema = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty schema guard", K(ret), KP(schema_guard_));
  } else if (OB_FAIL(schema_guard_->get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema is null", K(ret), KP(tenant_schema));
  } else if (gts_partition_pos_ < 0 || gts_partition_pos_ >= all_partition_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gts partition pos unexpected", K(ret), K(gts_partition_pos_));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(*tenant_schema, all_unit))) {
    LOG_WARN("get tenant resource pool failed", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else if (all_unit.count() > 0) {
    Partition& gts_service_partition = all_partition_.at(gts_partition_pos_);
    FOREACH_X(u, all_unit, OB_SUCCESS == ret)
    {
      const bool alloc_empty = true;
      ZoneUnit* zu = NULL;
      UnitStatMap::Item* item = NULL;
      if (REPLICA_TYPE_LOGONLY == u->unit_.replica_type_ && filter_logonly_unit_) {
        // fillter logonly unit; nothing todo
      } else if (OB_FAIL(locate_zone(u->unit_.zone_, zu, alloc_empty))) {
        LOG_WARN("locate zone failed", K(ret), "zone", u->unit_.zone_);
      } else if (NULL == zu) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL zone unit", K(ret));
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (OB_FAIL(unit_stat_map_.locate(u->unit_.unit_id_, item))) {
        LOG_WARN("locate unit failed", K(ret), "unit_id", u->unit_.unit_id_);
      } else if (NULL == item) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL item", K(ret));
      } else if (OB_FAIL(item->v_.info_.assign(*u))) {
        LOG_WARN("failed to assign data", K(ret));
      } else {
        item->v_.in_pool_ = true;
        item->v_.capacity_.set_disk_capacity(item->v_.info_.config_.max_disk_size_);
        item->v_.capacity_.set_cpu_capacity(item->v_.info_.config_.max_cpu_);
        item->v_.capacity_.set_memory_capacity(item->v_.info_.config_.max_memory_);
        item->v_.capacity_.set_iops_capacity(static_cast<double>(item->v_.info_.config_.max_iops_));
        if (OB_FAIL(set_unit_capacity_ratio(item->v_, gts_service_partition))) {
          LOG_WARN("fail to set unit", K(ret));
        } else if (OB_FAIL(zu->all_unit_.push_back(&item->v_))) {
          LOG_WARN("add unit stat item failed", K(ret));
        } else {
          ru_capacity_ += item->v_.capacity_;
        }
      }
    }  // end iterate all_unit
  }

  // update server status of all unit
  if (OB_SUCC(ret)) {
    FOREACH_X(u, unit_stat_map_.get_hash_table(), OB_SUCCESS == ret)
    {
      ServerStatMap::Item* s = NULL;
      if (OB_FAIL(server_stat_map_.locate(u->v_.info_.unit_.server_, s))) {
        LOG_WARN("get server stat failed", K(ret), "server", u->v_.info_.unit_.server_);
      } else if (NULL == s) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL server stat", K(ret));
      } else {
        const_cast<UnitStat&>(u->v_).server_ = &s->v_;
        // Currently (v1.4.x) the value set in all_unit_config is arbitrarily set by the DBA,
        // arbitrarily to correct the above disk_capacity value, taking the disk size of the server.
        // After v2.0 disk isolation is completed, remove the following line of patch
        const_cast<UnitStat&>(u->v_).capacity_.set_disk_capacity(s->v_.resource_info_.disk_total_);
      }
    }
  }

  return ret;
}

int TenantBalanceStat::set_unit_capacity_ratio(UnitStat& unit_stat, Partition& gts_service_partition)
{
  int ret = OB_SUCCESS;
  bool gts_on = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ALL_DUMMY_TID != extract_pure_id(gts_service_partition.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(gts_service_partition));
  } else if (OB_FAIL(get_gts_switch(gts_on))) {
    LOG_WARN("fail to get gts switch", K(ret));
  }
  if (OB_SUCC(ret)) {
    bool find_gts_service_unit = false;
    FOR_BEGIN_END_E(r, gts_service_partition, all_replica_, !find_gts_service_unit && OB_SUCC(ret))
    {
      if (!r->is_in_service() || !ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
        // bypass, since this not in leader member list
      } else if (REPLICA_TYPE_LOGONLY == r->replica_type_) {
        // L replica will not provide GTS service as leader
      } else if (r->zone_ != unit_stat.info_.unit_.zone_) {
        // not in the same zone
      } else if (OB_UNLIKELY(NULL == r->server_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica server stat ptr is null", K(ret));
      } else if (r->server_->server_ == unit_stat.info_.unit_.server_ ||
                 r->server_->server_ == unit_stat.info_.unit_.migrate_from_server_) {
        find_gts_service_unit = true;
      } else {
      }  // go on next replica
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(unit_stat.set_capacity_ratio(get_tenant_id(), find_gts_service_unit && gts_on))) {
      LOG_WARN("fail to set capacity ratio", K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::fill_sorted_partitions()
{
  int ret = OB_SUCCESS;
  sorted_partition_.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sorted_partition_.reserve(all_partition_.count()))) {
    LOG_WARN("array reserve failed", K(ret), "count", all_partition_.count());
  }
  FOREACH_X(p, all_partition_, OB_SUCCESS == ret)
  {
    if (OB_FAIL(sorted_partition_.push_back(&(*p)))) {
      LOG_WARN("push back sorted partition failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    std::sort(sorted_partition_.begin(), sorted_partition_.end(), ObPartitionGroupOrder(ret));
    if (OB_FAIL(ret)) {
      LOG_WARN("partition compare failed", K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::update_partition_statistics()
{
  int ret = OB_SUCCESS;
  const Partition* prep = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  FOREACH_X(iter, sorted_partition_, OB_SUCCESS == ret)
  {
    if (NULL == *iter) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL partition", K(ret));
      break;
    }
    Partition& p = *(*iter);
    if (NULL == prep || prep->tablegroup_id_ != p.tablegroup_id_ || prep->partition_idx_ != p.partition_idx_) {
      p.primary_ = true;
    }
    FOR_BEGIN_END_E(r, p, all_replica_, OB_SUCCESS == ret)
    {
      if (r->is_valid_paxos_member()) {
        p.valid_member_cnt_++;
      }
      if (REPLICA_STATUS_UNMERGED == r->additional_replica_status_) {
        p.can_balance_ = false;
      }
      if (r->is_leader_by_election()) {
        p.has_leader_ = true;
      }
      if ((r->is_in_service() && ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) ||
          r->only_in_member_list()) {
        if (OB_ISNULL(r->server_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL server stat", K(ret));
        } else {
          r->server_->in_member_replica_cnt_++;
        }
      }
    }
    prep = &p;
  }

  if (OB_SUCC(ret)) {
    FOREACH_X(s, server_stat_map_.get_hash_table(), OB_SUCC(ret))
    {
      if (s->v_.in_member_replica_cnt_ > 0) {
        if (OB_FAIL(replica_count_mgr_.accumulate(s->v_.server_, s->v_.in_member_replica_cnt_))) {
          LOG_WARN("accumulate server in member replica count failed",
              K(ret),
              "server",
              s->v_.server_,
              "count",
              s->v_.in_member_replica_cnt_);
        }
      }
    }
  }

  return ret;
}

int TenantBalanceStat::fill_partition_groups()
{
  int ret = OB_SUCCESS;
  all_pg_.reuse();
  PartitionGroup* pg = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(all_pg_.reserve(sorted_partition_.count()))) {
    LOG_WARN("array reserve failed", K(ret), "count", sorted_partition_.count());
  }
  FOREACH_X(p, sorted_partition_, OB_SUCCESS == ret)
  {
    if (NULL == *p) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL partition", K(ret));
      break;
    }
    if ((*p)->is_primary()) {
      PartitionGroup new_pg;
      new_pg.table_id_ = (*p)->table_id_;
      new_pg.partition_idx_ = (*p)->partition_idx_;
      new_pg.tablegroup_id_ = (*p)->tablegroup_id_;
      new_pg.begin_ = p - sorted_partition_.begin();
      if (OB_FAIL(all_pg_.push_back(new_pg))) {
        LOG_WARN("add partition group failed", K(ret));
      } else {
        pg = &all_pg_.at(all_pg_.count() - 1);
      }
    }

    if (OB_SUCC(ret)) {
      (*p)->all_pg_idx_ = all_pg_.count() - 1;
      pg->end_ = p - sorted_partition_.begin() + 1;
      pg->load_factor_ += (*p)->get_max_load_factor(resource_weight_, all_replica_);
    }
  }
  return ret;
}

int TenantBalanceStat::get_partition_locality(const Partition& partition,
    const balancer::HashIndexCollection& hash_index_collection,
    common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality)
{
  int ret = OB_SUCCESS;
  zone_locality.reset();
  common::ObSEArray<share::ObZoneReplicaAttrSet, 7> actual_zone_locality;
  bool compensate_readonly_all_server = false;
  if (OB_UNLIKELY(nullptr == schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema guard", K(ret), K_(schema_guard));
  } else if (!is_tablegroup_id(partition.table_id_)) {
    const ObTableSchema* table_schema = nullptr;
    const uint64_t table_id = partition.table_id_;
    if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get invalid table schema", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(*schema_guard_, actual_zone_locality))) {
      LOG_WARN("fail to get zone replica attr array inherit", K(ret));
    } else if (OB_FAIL(table_schema->check_is_duplicated(*schema_guard_, compensate_readonly_all_server))) {
      LOG_WARN("fail to check duplicate scope cluter", K(ret), K(table_id));
    }
  } else {
    compensate_readonly_all_server = false;
    const ObTablegroupSchema* tg_schema = nullptr;
    const uint64_t tablegroup_id = partition.table_id_;
    if (OB_FAIL(schema_guard_->get_tablegroup_schema(tablegroup_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
    } else if (OB_UNLIKELY(nullptr == tg_schema)) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN("get invalid tg schema", K(ret), K(tablegroup_id));
    } else if (OB_FAIL(tg_schema->get_zone_replica_attr_array_inherit(*schema_guard_, actual_zone_locality))) {
      LOG_WARN("fail to get zone replica attr array inherit", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLocalityUtil::generate_designated_zone_locality(compensate_readonly_all_server,
            partition.tablegroup_id_,
            partition.all_pg_idx_,
            hash_index_collection,
            *this,
            actual_zone_locality,
            zone_locality))) {
      LOG_WARN("fail to get generate designated zone locality", K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::do_fill_partition_flag_replicas(
    const Partition& partition, common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality)
{
  int ret = OB_SUCCESS;
  FOR_BEGIN_END_E(r, partition, all_replica_, OB_SUCC(ret))
  {
    if (ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_) && REPLICA_STATUS_FLAG != r->replica_status_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < designated_zone_locality.count(); ++i) {
        share::ObReplicaAttrSet& replica_set = designated_zone_locality.at(i).replica_attr_set_;
        const common::ObIArray<common::ObZone>& zone_set = designated_zone_locality.at(i).zone_set_;
        if (OB_UNLIKELY(1 != zone_set.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone set count unexpected", K(ret), K(partition), K(designated_zone_locality));
        } else if (!has_exist_in_array(zone_set, r->zone_)) {
          // bypass
        } else if (REPLICA_TYPE_FULL == r->replica_type_) {
          if (!replica_set.has_this_replica(r->replica_type_, r->memstore_percent_)) {
            // bypass
          } else if (OB_FAIL(replica_set.sub_full_replica_num(ReplicaAttr(1, r->memstore_percent_)))) {
            LOG_WARN("fail to sub full replica num", K(ret));
          }
          break;
        } else if (REPLICA_TYPE_LOGONLY == r->replica_type_) {
          if (!replica_set.has_this_replica(r->replica_type_, r->memstore_percent_)) {
            // bypass
          } else if (OB_FAIL(replica_set.sub_logonly_replica_num(ReplicaAttr(1, r->memstore_percent_)))) {
            LOG_WARN("fail to sub logonly replica num", K(ret));
          }
          break;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support replica_type", K(ret), K(partition));
        }
      }
    }
  }
  FOR_BEGIN_END_E(r, partition, all_replica_, OB_SUCC(ret))
  {
    if (REPLICA_STATUS_FLAG == r->replica_status_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < designated_zone_locality.count(); ++i) {
        share::ObReplicaAttrSet& replica_set = designated_zone_locality.at(i).replica_attr_set_;
        const common::ObIArray<common::ObZone>& zone_set = designated_zone_locality.at(i).zone_set_;
        if (OB_UNLIKELY(1 != zone_set.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone set count unexpected", K(ret), K(partition), K(designated_zone_locality));
        } else if (!has_exist_in_array(zone_set, r->zone_)) {
          if (r->zone_.is_empty()) {
            r->replica_type_ = REPLICA_TYPE_FULL;
            r->memstore_percent_ = 100;
            break;
          }
        } else if (replica_set.has_this_replica(REPLICA_TYPE_FULL, 100)) {
          r->replica_type_ = REPLICA_TYPE_FULL;
          r->memstore_percent_ = 100;
          if (OB_FAIL(replica_set.sub_full_replica_num(ReplicaAttr(1, 100)))) {
            LOG_WARN("fail to sub full replica num", K(ret));
          }
          break;
        } else if (replica_set.has_this_replica(REPLICA_TYPE_FULL, 0)) {
          r->replica_type_ = REPLICA_TYPE_FULL;
          r->memstore_percent_ = 0;
          if (OB_FAIL(replica_set.sub_full_replica_num(ReplicaAttr(1, 0)))) {
            LOG_WARN("fail to sub full replica num", K(ret));
          }
          break;
        } else if (replica_set.has_this_replica(REPLICA_TYPE_LOGONLY, 100)) {
          r->replica_type_ = REPLICA_TYPE_LOGONLY;
          r->memstore_percent_ = 100;
          if (OB_FAIL(replica_set.sub_logonly_replica_num(ReplicaAttr(1, 100)))) {
            LOG_WARN("fail to sub logonly replica num", K(ret));
          }
          break;
        } else {
          LOG_WARN("redundant replica in member list", K(ret), K(*r));
        }
      }
    }
  }
  return ret;
}

int TenantBalanceStat::fill_flag_replicas(const balancer::HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    common::ObSEArray<share::ObZoneReplicaAttrSet, 7> zone_locality;
    FOREACH_X(p, all_partition_, OB_SUCC(ret))
    {
      zone_locality.reset();
      if (!p->has_flag_replica_) {
        // bypass
      } else if (OB_FAIL(get_partition_locality(*p, hash_index_collection, zone_locality))) {
        LOG_WARN("fail to get partition locality", K(ret));
      } else if (OB_FAIL(do_fill_partition_flag_replicas(*p, zone_locality))) {
        LOG_WARN("fail to  do fill partition flag replicas", K(ret));
      }
    }
  }
  return ret;
}

int TenantBalanceStat::fill_tablegroups()
{
  int ret = OB_SUCCESS;
  all_tg_.reuse();
  TableGroup* tg = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(all_tg_.reserve(all_pg_.count()))) {
    LOG_WARN("array reserve", K(ret), "count", all_pg_.count());
  }
  FOREACH_X(pg, all_pg_, OB_SUCCESS == ret)
  {
    if (NULL == tg || !is_same_tg(all_pg_.at(tg->begin_), *pg)) {
      TableGroup new_tg;
      new_tg.tablegroup_id_ = pg->tablegroup_id_;
      new_tg.begin_ = pg - all_pg_.begin();
      if (OB_FAIL(all_tg_.push_back(new_tg))) {
        LOG_WARN("add table group failed", K(ret));
      } else {
        tg = &all_tg_.at(all_tg_.count() - 1);
      }
    }

    if (OB_SUCC(ret)) {
      tg->end_ = pg - all_pg_.begin() + 1;
      pg->all_tg_idx_ = all_tg_.count() - 1;  // Reverse index tg from pg
    }
  }

  return ret;
}

int TenantBalanceStat::calc_load()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  this->ru_total_.reset();

  FOREACH_X(iter, sorted_partition_, OB_SUCCESS == ret)
  {
    if (NULL == *iter) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL partition", K(ret));
      break;
    }
    Partition& p = *(*iter);
    FOR_BEGIN_END_E(r, p, all_replica_, OB_SUCCESS == ret)
    {
      if (REPLICA_STATUS_NORMAL == r->replica_status_) {
        r->unit_->load_factor_ += r->load_factor_;  // accumulate resource usage of the unit
        this->ru_total_ += r->load_factor_;         // accumulate resource usage of the tenant
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_resource_weight(ru_total_, ru_capacity_, resource_weight_))) {
      LOG_WARN("failed to calc resource weight");
    }
  }

  // update all zone statistics
  // FIXME : not all units, only units in resource pool
  ObStatisticsCalculator load_calc;
  ObStatisticsCalculator cpu_calc;
  ObStatisticsCalculator disk_calc;
  ObStatisticsCalculator iops_calc;
  ObStatisticsCalculator memory_calc;
  FOREACH_X(zu, all_zone_unit_, OB_SUCCESS == ret)
  {  // foreach zone
    LoadFactor real;
    LoadFactor spec;
    load_calc.reset();
    cpu_calc.reset();
    disk_calc.reset();
    iops_calc.reset();
    memory_calc.reset();

    FOREACH_X(u, zu->all_unit_, OB_SUCCESS == ret)
    {  // foreach unit of the zone
      if (NULL == *u) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unit stat", K(ret));
      } else {
        UnitStat& us = *(*u);  // the unit
        real += us.load_factor_;
        if (us.server_->active_) {
          zu->active_unit_cnt_++;
          spec += us.capacity_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1440) {
        // Use global in compatibility mode resource weight
        zu->resource_weight_ = resource_weight_;
      } else {
        // The latest version uses independent resource weight
        if (OB_FAIL(calc_resource_weight(real, spec, zu->resource_weight_))) {
          LOG_WARN("failed to calc resource weight", K(ret));
        }
      }
    }

    FOREACH_X(u, zu->all_unit_, OB_SUCCESS == ret)
    {  // foreach unit of the zone
      if (NULL == *u) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unit stat", K(ret));
      } else {
        UnitStat& us = *(*u);  // the unit
        // calc load of the unit
        double cpu_usage_rate = us.get_cpu_usage_rate();
        double disk_usage_rate = us.get_disk_usage_rate();
        double iops_usage_rate = us.get_iops_usage_rate();
        double memory_usage_rate = us.get_memory_usage_rate();
        us.load_ = cpu_usage_rate * zu->resource_weight_.cpu_weight_ +
                   disk_usage_rate * zu->resource_weight_.disk_weight_ +
                   iops_usage_rate * zu->resource_weight_.iops_weight_ +
                   memory_usage_rate * zu->resource_weight_.memory_weight_;
        if (OB_FAIL(load_calc.add_value(us.load_))) {
          break;
        } else if (OB_FAIL(cpu_calc.add_value(cpu_usage_rate))) {
          break;
        } else if (OB_FAIL(disk_calc.add_value(disk_usage_rate))) {
          break;
        } else if (OB_FAIL(iops_calc.add_value(iops_usage_rate))) {
          break;
        } else if (OB_FAIL(memory_calc.add_value(memory_usage_rate))) {
          break;
        }
      }
    }  // for each unit of the zone

    if (OB_SUCC(ret)) {
      zu->load_imbalance_ = load_calc.get_standard_deviation();
      zu->load_avg_ = load_calc.get_avg();
      zu->cpu_imbalance_ = cpu_calc.get_standard_deviation();
      zu->cpu_avg_ = cpu_calc.get_avg();
      zu->disk_imbalance_ = disk_calc.get_standard_deviation();
      zu->disk_avg_ = disk_calc.get_avg();
      zu->iops_imbalance_ = iops_calc.get_standard_deviation();
      zu->iops_avg_ = iops_calc.get_avg();
      zu->memory_imbalance_ = memory_calc.get_standard_deviation();
      zu->memory_avg_ = memory_calc.get_avg();
    }
  }  // for each zone

  return ret;
}

bool TenantBalanceStat::has_replica_locality(const Replica& replica)
{
  bool bool_ret = true;
  int err_ret = OB_SUCCESS;

  common::ObZone server_zone;
  const share::schema::ObTenantSchema* tenant_schema = nullptr;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality_array;
  if (OB_ISNULL(schema_guard_)) {
    err_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard ptr is null", K(err_ret), KP(schema_guard_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    err_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", K(err_ret), K(tenant_id_));
  } else if (OB_ISNULL(server_mgr_)) {
    err_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone mgr ptr is null", K(err_ret), KP(server_mgr_));
  } else if (OB_SUCCESS != (err_ret = schema_guard_->get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(err_ret));
  } else if (OB_ISNULL(tenant_schema)) {
    err_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema ptr is null", K(err_ret));
  } else if (OB_ISNULL(replica.server_)) {
    err_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica server is unexpected", K(err_ret));
  } else if (OB_SUCCESS != (err_ret = tenant_schema->get_zone_replica_attr_array(zone_locality_array))) {
    LOG_WARN("fail to tenant schema get zone replica attr array", K(err_ret));
  } else if (OB_SUCCESS != (err_ret = server_mgr_->get_server_zone(replica.server_->server_, server_zone))) {
    LOG_WARN("fail to get server zone", K(err_ret), "server", replica.server_->server_);
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < zone_locality_array.count(); ++i) {
      share::ObZoneReplicaAttrSet& locality_attr = zone_locality_array.at(i);
      found = has_exist_in_array(locality_attr.zone_set_, server_zone);
    }
    if (!found) {
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int TenantBalanceStat::fill_filter_partitions(TenantBalanceStat& output_stat)
{
  int ret = OB_SUCCESS;
  UnitStatMap::Item* u = NULL;
  ServerStatMap::Item* s = NULL;
  if (!output_stat.inited_ || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(output_stat.all_table_.reserve(all_table_.count()))) {
    LOG_WARN("fail to reserver", K(ret), K(all_table_));
  } else if (OB_FAIL(output_stat.all_table_.assign(all_table_))) {
    LOG_WARN("fail to assign", K(ret), K(all_table_));
  }
  for (int64_t i = 0; i < all_partition_.count() && OB_SUCC(ret); i++) {
    Partition& p = all_partition_.at(i);
    Partition new_partition = p;
    new_partition.begin_ = output_stat.all_replica_.count();
    FOR_BEGIN_END_E(r, p, all_replica_, OB_SUCC(ret))
    {
      if (OB_ISNULL(r) || OB_ISNULL(r->unit_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid unit", K(ret));
      } else if (REPLICA_TYPE_LOGONLY == r->unit_->info_.unit_.replica_type_) {
        new_partition.filter_logonly_count_++;
        // nothing todo
      } else if (OB_FAIL(output_stat.unit_stat_map_.locate(r->unit_->info_.unit_.unit_id_, u))) {
        LOG_WARN("locate unit stat failed", K(ret), "unit_id", r->unit_->info_.unit_.unit_id_);
      } else if (OB_ISNULL(u)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null unit stat", K(ret));
      } else if (OB_FAIL(output_stat.server_stat_map_.locate(r->server_->server_, s))) {
        LOG_WARN("locate server stat failed", K(ret), "server", r->server_->server_);
      } else if (NULL == s) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null server stat", K(ret));
      } else {
        Replica replica = *r;
        replica.unit_ = &u->v_;
        replica.server_ = &s->v_;
        if (OB_FAIL(output_stat.all_replica_.push_back(replica))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }  // end FOR_BEGIN_END_E(r, p
    if (OB_SUCC(ret)) {
      new_partition.end_ = output_stat.all_replica_.count();
      ObPartitionKey part_key = new_partition.get_key();
      if (OB_FAIL(output_stat.all_partition_.push_back(new_partition))) {
        LOG_WARN("fail to push back", K(ret), K(new_partition));
      } else if (OB_FAIL(output_stat.partition_map_.set_refactored(part_key, output_stat.all_partition_.count() - 1))) {
        LOG_WARN("fail to add into map", K(ret), K(new_partition));
      } else if (extract_pure_id(part_key.get_table_id()) == OB_ALL_DUMMY_TID) {
        output_stat.gts_partition_pos_ = output_stat.all_partition_.count() - 1;
      } else {
      }  // by pass
    }
  }
  return ret;
}

int TenantBalanceStat::gather_filter_stat(
    TenantBalanceStat& output_stat, const balancer::HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  bool filter_logonly_unit = true;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(output_stat.init(*unit_mgr_,
                 *server_mgr_,
                 *pt_operator_,
                 *remote_pt_operator_,
                 *zone_mgr_,
                 *task_mgr_,
                 *check_stop_provider_,
                 *sql_proxy_,
                 filter_logonly_unit))) {
    LOG_WARN("fail to init output stat", K(ret));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stat", K(ret));
  } else {
    output_stat.reuse();
    output_stat.tenant_id_ = tenant_id_;
    output_stat.schema_guard_ = schema_guard_;
    output_stat.min_source_replica_version_ = min_source_replica_version_;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(output_stat.all_zone_paxos_info_.assign(all_zone_paxos_info_))) {
      LOG_WARN("fail to assign", K(ret));
    }
  }

  // fill partition to %all_partition_ and %all_replica_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_filter_partitions(output_stat))) {
      LOG_WARN("fill tenant all partitions failed", K(ret), K_(tenant_id));
    }
  }
  // fill server
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.fill_servers())) {
      LOG_WARN("fill servers failed", K(ret));
    }
  }

  // fill unit
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.fill_units())) {
      LOG_WARN("fill units failed", K(ret));
    }
  }

  // fill sorted_partition_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.fill_sorted_partitions())) {
      LOG_WARN("fill sorted partitions failed", K(ret));
    }
  }

  // update statistics
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.reuse_replica_count_mgr())) {
      LOG_WARN("fail to reuse replica count mgr", K(ret));
    } else if (OB_FAIL(output_stat.update_partition_statistics())) {
      LOG_WARN("update statistics failed", K(ret));
    } else if (OB_FAIL(output_stat.calc_load())) {
      LOG_WARN("failed to calc unit load", K(ret));
    }
  }

  // fill all_pg_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.fill_partition_groups())) {
      LOG_WARN("fill partition groups failed", K(ret));
    }
  }

  // fill all_tg_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.fill_tablegroups())) {
      LOG_WARN("fill tablegroups failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(output_stat.fill_flag_replicas(hash_index_collection))) {
      LOG_WARN("fill flag replicas failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(output_stat.check_valid())) {
      LOG_WARN("check valid failed", K(ret));
    } else if (!valid_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("invalid tenant statistic", K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::fill_paxos_info()
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema guard", K(ret), K_(schema_guard));
  } else if (OB_FAIL(schema_guard_->get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fial to get tenant info", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("get invalid tenant_schema", K(ret), K_(tenant_id));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count() && OB_SUCC(ret); i++) {
      ZonePaxosInfo info;
      info.zone_ = zone_locality.at(i).zone_;
      info.logonly_replica_num_ = zone_locality.at(i).get_logonly_replica_num();
      info.full_replica_num_ = zone_locality.at(i).get_full_replica_num();
      if (OB_FAIL(all_zone_paxos_info_.push_back(info))) {
        LOG_WARN("fail to push back", K(ret), K(info), K_(tenant_id));
      }
    }
  }
  return ret;
}

int TenantBalanceStat::gather_stat(const uint64_t tenant_id, ObSchemaGetterGuard* schema_guard,
    const balancer::HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || NULL == schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(schema_guard));
  } else {
    reuse();
    tenant_id_ = tenant_id;
    schema_guard_ = schema_guard;
  }

  if (OB_SUCC(ret) && GCTX.is_standby_cluster()) {
    ObGlobalStatProxy global_proxy(*sql_proxy_);
    int64_t schema_version = 0;
    int64_t frozen_version = 0;
    if (OB_FAIL(global_proxy.get_schema_snapshot_version(schema_version, frozen_version))) {
      LOG_WARN("fail to set schema snapshot versoin", KR(ret), K(schema_version));
    } else {
      if (frozen_version == 0) {
        min_source_replica_version_ = 0;
      } else {
        min_source_replica_version_ = frozen_version;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_paxos_info())) {
      LOG_WARN("fail to fill paxos info", K(ret), K_(tenant_id));
    }
  }
  // fill partition to %all_partition_ and %all_replica_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_partitions())) {
      LOG_WARN("fill tenant all partitions failed", K(ret), K_(tenant_id));
    }
  }

  // fill server
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_servers())) {
      LOG_WARN("fill servers failed", K(ret));
    }
  }

  // fill unit
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_units())) {
      LOG_WARN("fill units failed", K(ret));
    }
  }

  // fill sorted_partition_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_sorted_partitions())) {
      LOG_WARN("fill sorted partitions failed", K(ret));
    }
  }

  // update statistics
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(update_partition_statistics())) {
      LOG_WARN("update statistics failed", K(ret));
    } else if (OB_FAIL(calc_load())) {
      LOG_WARN("failed to calc unit load", K(ret));
    }
  }

  // fill all_pg_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_partition_groups())) {
      LOG_WARN("fill partition groups failed", K(ret));
    }
  }

  // fill all_tg_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_tablegroups())) {
      LOG_WARN("fill tablegroups failed", K(ret));
    }
  }

  // fill flag replica
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(fill_flag_replicas(hash_index_collection))) {
      LOG_WARN("fill flag replicas failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_valid())) {
      LOG_WARN("check valid failed", K(ret));
    } else if (!valid_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("invalid tenant statistic", K(ret));
    }
  }

  return ret;
}

int TenantBalanceStat::check_valid()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id_ || NULL == schema_guard_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id or NULL schema mgr", K(ret), K_(tenant_id), KP_(schema_guard));
  } else {
    valid_ = true;

    FOREACH_CNT_X(p, all_partition_, valid_)
    {
      if (p->begin_ < 0 || p->begin_ > p->end_ || p->end_ > all_replica_.count()) {
        LOG_WARN("invalid replica index in partition", "partition", *p, "replica count", all_replica_.count());
        valid_ = false;
      }
    }

    FOREACH_CNT_X(r, all_replica_, valid_)
    {
      if (OB_ISNULL(r->server_) || (OB_ISNULL(r->unit_))) {
        LOG_WARN("invalid replica statistics", "replica", *r);
        valid_ = false;
      }
    }

    FOREACH_CNT_X(p, sorted_partition_, valid_)
    {
      if (OB_ISNULL(*p)) {
        LOG_WARN("NULL partition pointer in sorted_partition");
        valid_ = false;
      }
    }

    FOREACH_CNT_X(pg, all_pg_, valid_)
    {
      if (pg->begin_ < 0 || pg->begin_ > pg->end_ || pg->end_ > sorted_partition_.count()) {
        LOG_WARN("invalid sorted partition array index in partition group",
            "partition group",
            *pg,
            "sorted partition count",
            sorted_partition_.count());
        valid_ = false;
      }
    }

    FOREACH_CNT_X(tg, all_tg_, valid_)
    {
      if (tg->begin_ < 0 || tg->begin_ > tg->end_ || tg->end_ > all_pg_.count()) {
        LOG_WARN("invalid partition group array index in table group",
            "table group",
            *tg,
            "partition group count",
            all_pg_.count());
      }
    }

    FOREACH_CNT_X(zu, all_zone_unit_, valid_)
    {
      FOREACH_CNT_X(u, zu->all_unit_, valid_)
      {
        if (OB_ISNULL(*u) || OB_ISNULL((*u)->server_)) {
          LOG_WARN("NULL unit stat or has NULL pointer members");
          valid_ = false;
        }
      }
    }
  }
  return ret;
}

int TenantBalanceStat::locate_zone(const ObZone& zone, ZoneUnit*& zone_unit, const bool alloc_empty /* = false */)
{
  int ret = OB_SUCCESS;
  zone_unit = NULL;
  // zone can be empty, for flag replica
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH(zu, all_zone_unit_)
    {
      if (zu->zone_ == zone) {
        zone_unit = &(*zu);
      }
    }
    if (NULL == zone_unit && alloc_empty) {
      ZoneUnit zu;
      zu.zone_ = zone;
      if (OB_FAIL(all_zone_unit_.push_back(zu))) {
        LOG_WARN("push back zone unit to all unit array failed", K(ret));
      } else {
        zone_unit = &all_zone_unit_.at(all_zone_unit_.count() - 1);
      }
    }

    if (OB_SUCC(ret) && NULL == zone_unit) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int TenantBalanceStat::check_valid_replica_exist_on_dest_server(
    const Partition& partition, OnlineReplica& dest, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  FOR_BEGIN_END_E(r, partition, all_replica_, OB_SUCCESS == ret && !exist)
  {
    if (OB_UNLIKELY(NULL == r->server_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica server stat ptr is null", K(ret));
    } else if (r->is_in_service() && r->server_->server_ == dest.member_.get_server() &&
               r->replica_type_ == dest.member_.get_replica_type()) {
      LOG_WARN("already have same paxos replcia exist", K(*r), K(dest), K(partition));
      exist = true;
    }
  }
  return ret;
}

int TenantBalanceStat::check_valid_replica_exist_on_dest_zone(
    const Partition& partition, OnlineReplica& dst, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  FOR_BEGIN_END_E(r, partition, all_replica_, OB_SUCCESS == ret && !exist)
  {
    if (r->is_in_service() && r->zone_ == dst.zone_ && r->replica_type_ == dst.member_.get_replica_type()) {
      LOG_WARN("already have same paxos replcia exist", K(*r), K(dst), K(partition));
      exist = true;
    }
  }
  return ret;
}

int TenantBalanceStat::gen_partition_key(const Partition& partition, ObPartitionKey& pkey) const
{
  return gen_partition_key(partition.table_id_, partition.partition_id_, pkey);
}

int TenantBalanceStat::gen_partition_key(int64_t table_id, int64_t partition_id, ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  if (!ObIPartitionTable::is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self_valid", is_valid(), K(table_id), K(partition_id));
  } else {
    if (!is_tablegroup_id(table_id)) {
      const ObSimpleTableSchemaV2* table_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(table_id));
      } else if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not found", K(ret), K(table_id));
      } else if (OB_FAIL(pkey.init(table_id, partition_id, table_schema->get_partition_cnt()))) {
        LOG_WARN("init partition key failed", K(ret), K(table_id), K(partition_id));
      }
    } else {
      const ObTablegroupSchema* tg_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_tablegroup_schema(table_id, tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", table_id);
      } else if (OB_UNLIKELY(NULL == tg_schema)) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("tg not found", K(ret), "tg_id", table_id);
      } else if (OB_FAIL(pkey.init(table_id, partition_id, tg_schema->get_partition_cnt()))) {
        LOG_WARN("init partition key failed", K(ret), "tg_id", table_id, K(partition_id));
      }
    }
  }
  return ret;
}

int TenantBalanceStat::update_tg_pg_stat(const int64_t sorted_partition_index)
{
  int ret = OB_SUCCESS;
  int64_t idx = sorted_partition_index;
  if (!is_valid() || idx < 0 || idx >= sorted_partition_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), "sorted partition count", sorted_partition_.count());
  } else {
    while (idx >= 1 && is_same_tg(*sorted_partition_.at(idx - 1), *sorted_partition_.at(idx))) {
      idx--;
    }
    FOREACH(zu, all_zone_unit_)
    {
      zu->tg_pg_cnt_ = 0;
      FOREACH(u, zu->all_unit_)
      {
        (*u)->tg_pg_cnt_ = 0;
      }
    }

    // Traverse the whole table_group
    const Partition& first = *sorted_partition_.at(idx);
    for (; idx < sorted_partition_.count() && is_same_tg(first, *sorted_partition_.at(idx)); ++idx) {
      // Because the number of partitions of the table in the table group may be different,
      // so: Choose a partition to represent the entire partition group.
      // In the current implementation,
      // statistics are based on the partition of the table with the smallest table_id in the table group
      if (sorted_partition_.at(idx)->is_primary()) {
        FOR_BEGIN_END(r, *sorted_partition_.at(idx), all_replica_)
        {
          if (r->is_in_service()) {
            r->unit_->tg_pg_cnt_++;
          }
        }
      }
    }

    FOREACH(zu, all_zone_unit_)
    {
      FOREACH(u, zu->all_unit_)
      {
        zu->tg_pg_cnt_ += (*u)->tg_pg_cnt_;
      }
    }
  }

  return ret;
}

// The original intention of the unit assign function:
// The all_meta_table table can be emptied
// and has the ability to rebuild all_meta_table through reporting.
//
// However, there will be no unit information in the reported information,
// so it is necessary to re-assign the unit information to these meta information through the unit assign logic
int TenantBalanceStat::unit_assign()
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *this;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  }
  const Partition* prev = NULL;  // previous tablegroup stat updated partition
  Partition* pp = NULL;          // pp: primary partition
  FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
  {
    ObHashSet<int64_t> units_assigned;
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if ((*p)->is_primary()) {
      pp = *p;
      // Initialize the tg_pg_cnt of the entire tablegroup
      if (NULL == prev || !is_same_tg(*prev, *(*p))) {
        if (OB_FAIL(ts.update_tg_pg_stat(p - ts.sorted_partition_.begin()))) {
          LOG_WARN("update tenant group stat failed", K(ret));
        } else {
          prev = *p;
        }
      }
    } else if (!(*p)->is_primary() && NULL == pp) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("no primary partition, this should never happen", K(ret), "partition", *p);
    }
    if (OB_FAIL(ret)) {
    } else if (!(*p)->in_physical_restore() && !pp->has_leader_) {
      // has no leader
    } else if (OB_FAIL(units_assigned.create(MAX_REPLICA_COUNT_TOTAL))) {
      LOG_WARN("hash set init fail", K(ret));
    } else if (OB_FAIL(try_assign_unit_on_same_server(p, units_assigned))) {
      LOG_WARN("try to assign unit on same server fail", K(ret), K(*p));
    } else if (!(*p)->is_primary() && OB_FAIL(try_assign_unit_by_ppr(p, pp, units_assigned))) {
      LOG_WARN("try to assign unit on by ppr fail", K(ret), K(*p), K(*pp));
    } else if (OB_FAIL(try_assign_unit_on_random_server(p, units_assigned))) {
      LOG_WARN("try to assign unit on random server fail", K(ret), K(*p));
    }
  }
  return ret;
}

// Roughly calculate the load of the unit after the change
int TenantBalanceStat::calc_unit_load(UnitStat*& us)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // calc load of the unit
    double cpu_usage_rate = us->get_cpu_usage_rate();
    double disk_usage_rate = us->get_disk_usage_rate();
    double iops_usage_rate = us->get_iops_usage_rate();
    double memory_usage_rate = us->get_memory_usage_rate();
    us->load_ = cpu_usage_rate * resource_weight_.cpu_weight_ + disk_usage_rate * resource_weight_.disk_weight_ +
                iops_usage_rate * resource_weight_.iops_weight_ + memory_usage_rate * resource_weight_.memory_weight_;
  }
  return ret;
}

// Try to select a unit with a smaller load for the replica
int TenantBalanceStat::try_assign_unit_on_random_server(
    const ObArray<Partition*>::iterator& p, ObHashSet<int64_t>& units_assigned)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *this;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  } else {
    FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
    {
      if (REPLICA_STATUS_NORMAL != r->replica_status_) {
        continue;
      }
      if (r->unit_->in_pool_) {
        if (OB_FAIL(units_assigned.set_refactored(r->unit_->info_.unit_.unit_id_))) {
          LOG_WARN("insert unit_id into hashset fail", K(ret), K(r->unit_->info_.unit_.unit_id_));
        }
        continue;
      }
      ZoneUnit* zu = NULL;
      if (OB_FAIL(ts.locate_zone(r->zone_, zu))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("locate zone failed", K(ret), "zone", r->zone_);
          break;
        }
      } else if (NULL == zu) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL zone unit", K(ret));
        break;
      }
      if (OB_ENTRY_NOT_EXIST == ret || 0 == zu->active_unit_cnt_) {
        ret = OB_SUCCESS;
        LOG_WARN("replica unit not in resource_pool, but can't assign new one",
            "partition",
            *(*p),
            "zone",
            r->zone_,
            "unit_id",
            r->unit_->info_.unit_.unit_id_);
        continue;
        // FIXME : tenant's unit migrate from one zone to another?
        // find zone with unit and this partition has no replica on this zone.
      }
      UnitStat* dest = NULL;
      // Strategy: Unit with the lowest load in the zone
      if (OB_SUCCESS == ret && NULL == dest) {
        if (OB_FAIL(find_dest_unit_for_unit_assigned(r->replica_type_, *zu, dest, units_assigned))) {
          LOG_WARN("find dest unit failed", K(ret));
          if (OB_RESOURCE_OUT == ret) {
            LOG_WARN("no resource for replica", K(ret), "partition", *(*p), "zone", zu->zone_);
            ret = OB_SUCCESS;
          }
        } else {
          if (NULL == dest) {
            LOG_DEBUG("can't find dest unit", "partition", *(*p), "replica", *r, "zone", zu->zone_);
          } else {
            LOG_INFO("assign unit_id by unit workload in zone", K(ret), K(*r), K(dest->info_));
          }
        }
      }
      if (OB_SUCCESS == ret && NULL != dest) {
        if (OB_FAIL(pt_operator_->set_unit_id(
                (*p)->table_id_, (*p)->partition_id_, r->server_->server_, dest->info_.unit_.unit_id_))) {
          LOG_WARN("update unit id failed", K(ret));
        } else if (OB_FAIL(units_assigned.set_refactored(dest->info_.unit_.unit_id_))) {
          LOG_WARN("insert unit_id into hashset fail", K(ret), K(dest->info_.unit_.unit_id_));
        } else {
          r->unit_ = dest;
          dest->load_factor_ += r->load_factor_;
          if ((*p)->is_primary()) {
            dest->tg_pg_cnt_++;
          }
          if (OB_FAIL(calc_unit_load(dest))) {
            LOG_WARN("calc unit load fail", K(ret));
          } else if (OB_FAIL(check_stop())) {
            LOG_WARN("balancer stop", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// For the replica of the non primary partition,
// try to align with the replica of the corresponding primary partition
int TenantBalanceStat::try_assign_unit_by_ppr(
    const ObArray<Partition*>::iterator& p, Partition*& pp, ObHashSet<int64_t>& units_assigned)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *this;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  } else {
    FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
    {
      if (REPLICA_STATUS_NORMAL != r->replica_status_) {
        continue;
      }
      if (r->unit_->in_pool_) {
        if (OB_FAIL(units_assigned.set_refactored(r->unit_->info_.unit_.unit_id_))) {
          LOG_WARN("insert unit_id into hashset fail", K(ret), K(r->unit_->info_.unit_.unit_id_));
        }
        continue;
      }
      UnitStat* dest = NULL;
      // assign to primary partition replica's unit of the same zone.
      FOR_BEGIN_END_E(ppr, *pp, ts.all_replica_, OB_SUCCESS == ret)
      {
        if (ppr->zone_ == r->zone_ && ppr->unit_->in_pool_ && ppr->is_in_service() && r->is_in_service() &&
            ppr->replica_type_ == r->replica_type_) {
          int ret_tmp = units_assigned.exist_refactored(ppr->unit_->info_.unit_.unit_id_);
          if (OB_HASH_NOT_EXIST == ret_tmp) {
            dest = ppr->unit_;
            LOG_INFO("assign unit_id by ppr", K(ret), K(*r), K(dest->info_));
            break;
          } else if (OB_HASH_EXIST != ret_tmp) {
            ret = ret_tmp;
            LOG_WARN("check hashset key fail", K(ret));
          }
        }
      }  // end primary partition replica iterator
      if (OB_SUCCESS == ret && NULL != dest) {
        if (OB_FAIL(pt_operator_->set_unit_id(
                (*p)->table_id_, (*p)->partition_id_, r->server_->server_, dest->info_.unit_.unit_id_))) {
          LOG_WARN("update unit id failed", K(ret));
        } else if (OB_FAIL(units_assigned.set_refactored(dest->info_.unit_.unit_id_))) {
          LOG_WARN("insert unit_id into hashset fail", K(ret), K(dest->info_.unit_.unit_id_));
        } else {
          r->unit_ = dest;
          dest->load_factor_ += r->load_factor_;
          if (OB_FAIL(calc_unit_load(dest))) {
            LOG_WARN("calc unit load fail", K(ret));
          } else if (OB_FAIL(check_stop())) {
            LOG_WARN("balancer stop", K(ret));
          }
        }
      }
    }  // end replica iterator
  }
  return ret;
}

// Try to assign the unit where the corresponding server is located to the replica
int TenantBalanceStat::try_assign_unit_on_same_server(
    const ObArray<Partition*>::iterator& p, ObHashSet<int64_t>& units_assigned)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = *this;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", ts.is_valid());
  } else {
    FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
    {
      if (REPLICA_STATUS_NORMAL != r->replica_status_) {
        continue;
      }
      ZoneUnit* zu = NULL;
      ret = ts.locate_zone(r->zone_, zu);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("zone not exist", K(*r), "zone", r->zone_);
      } else if (OB_SUCCESS != ret) {
        LOG_WARN("fail to locate zone", K(ret), "zone", r->zone_);
      } else if (NULL == zu) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zu ptr is null", K(ret), KP(zu), "zone", r->zone_);
      } else if (0 == zu->active_unit_cnt_) {
        LOG_WARN("no active unit exist", "zone", r->zone_);
      } else if (r->unit_->in_pool_) {
        // Deal with the problem that the server where the copy is located does not match the unit when the shrink unit
        // is rolled back. In this case, set the unit id of the copy to the unit id on the server
        if (r->unit_->info_.unit_.server_ == r->server_->server_ ||
            r->unit_->info_.unit_.migrate_from_server_ == r->server_->server_) {
          // do nothing, unit of r and server of r match
          if (OB_FAIL(units_assigned.set_refactored(r->unit_->info_.unit_.unit_id_))) {
            LOG_WARN("insert unit_id into hashset fail", K(ret), K(r->unit_->info_.unit_.unit_id_));
          }
        } else {
          UnitStat* dest = NULL;
          FOREACH_X(u, zu->all_unit_, NULL == dest)
          {
            if (r->server_->server_ == (*u)->info_.unit_.server_) {
              dest = *u;
            } else if (r->server_->server_ == (*u)->info_.unit_.migrate_from_server_) {
              dest = *u;
            }
          }
          if (NULL != dest) {
            if (OB_FAIL(pt_operator_->set_unit_id(
                    (*p)->table_id_, (*p)->partition_id_, r->server_->server_, dest->info_.unit_.unit_id_))) {
              LOG_WARN("update unit id failed", K(ret));
            } else if (OB_FAIL(units_assigned.set_refactored(dest->info_.unit_.unit_id_))) {
              LOG_WARN("insert unit_id into hashset failed", K(ret), K(dest->info_.unit_.unit_id_));
            } else {
              UnitStat* prev = r->unit_;
              r->unit_ = dest;
              dest->load_factor_ += r->load_factor_;
              prev->load_factor_ -= r->load_factor_;
              if ((*p)->is_primary()) {
                dest->tg_pg_cnt_++;
                prev->tg_pg_cnt_--;
              }
              if (OB_FAIL(calc_unit_load(dest))) {
                LOG_WARN("fail to calc unit load", K(ret));
              } else if (OB_FAIL(calc_unit_load(prev))) {
                LOG_WARN("fail to calc unit load", K(ret));
              } else if (OB_FAIL(check_stop())) {
                LOG_WARN("balancer stop", K(ret));
              }
              LOG_INFO("reassign unit by replica server", K(ret), K(prev->info_), K(dest->info_), K(*r));
            }
          } else {
            if (OB_FAIL(units_assigned.set_refactored(r->unit_->info_.unit_.unit_id_))) {
              LOG_WARN("insert unit_id into hashset fail", K(ret), K(r->unit_->info_.unit_.unit_id_));
            }
          }
        }
      } else {
        UnitStat* dest = NULL;
        FOREACH_X(u, zu->all_unit_, NULL == dest)
        {
          if ((*u)->info_.unit_.server_ == r->server_->server_) {
            dest = *u;
          }
        }
        FOREACH_X(u, zu->all_unit_, NULL == dest)
        {
          if ((*u)->info_.unit_.migrate_from_server_ == r->server_->server_) {
            dest = *u;
          }
        }
        if (OB_SUCCESS == ret && NULL != dest) {
          if (OB_FAIL(pt_operator_->set_unit_id(
                  (*p)->table_id_, (*p)->partition_id_, r->server_->server_, dest->info_.unit_.unit_id_))) {
            LOG_WARN("update unit id failed", K(ret));
          } else if (OB_FAIL(units_assigned.set_refactored(dest->info_.unit_.unit_id_))) {
            LOG_WARN("insert unit_id into hashset fail", K(ret), K(dest->info_.unit_.unit_id_));
          } else {
            r->unit_ = dest;
            dest->load_factor_ += r->load_factor_;
            if ((*p)->is_primary()) {
              dest->tg_pg_cnt_++;
            }
            if (OB_FAIL(calc_unit_load(dest))) {
              LOG_WARN("calc unit load fail", K(ret));
            } else if (OB_FAIL(check_stop())) {
              LOG_WARN("balancer stop", K(ret));
            }
            LOG_INFO("assign unit_id by replica server", K(ret), K(*r), K(dest->info_));
          }
        }
      }
    }  // end replica iterator
  }
  return ret;
}

int TenantBalanceStat::find_dest_unit_for_unit_assigned(
    const ObReplicaType& replica_type, ZoneUnit& zone_unit, UnitStat*& us, ObHashSet<int64_t>& excluded_units)
{
  int ret = OB_SUCCESS;
  us = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!this->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", this->is_valid());
  } else if (zone_unit.active_unit_cnt_ <= 0) {
    ret = OB_RESOURCE_OUT;
    LOG_WARN("no active unit", K(ret), "zone", zone_unit.zone_);
  } else {
    FOREACH(u, zone_unit.all_unit_)
    {
      if (!(*u)->server_->active_ || !(*u)->server_->online_ || (*u)->server_->blocked_ ||
          (REPLICA_TYPE_LOGONLY == (*u)->info_.unit_.replica_type_ && REPLICA_TYPE_LOGONLY != replica_type)) {
        // The logonly unit does not participate in the selection of the scale-down target;
        // currently we only support scale-down non-logonly units;
        continue;
      }
      int ret_tmp = excluded_units.exist_refactored((*u)->info_.unit_.unit_id_);
      if (OB_HASH_NOT_EXIST == ret_tmp) {
      } else if (OB_HASH_EXIST == ret_tmp) {
        continue;
      } else {
        ret = ret_tmp;
        LOG_WARN("check hashset key fail", K(ret));
      }
      if (NULL == us) {
        us = (*u);
      } else {
        if (us->tg_pg_cnt_ > (*u)->tg_pg_cnt_) {
          us = (*u);
        } else if (us->tg_pg_cnt_ == (*u)->tg_pg_cnt_ && us->get_load() > (*u)->get_load()) {
          us = (*u);
        }
      }
    }
  }
  return ret;
}

int TenantBalanceStat::find_dest_unit(ZoneUnit& zone_unit, UnitStat*& us)
{
  int ret = OB_SUCCESS;
  us = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!this->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tenant stat valid", this->is_valid());
  } else if (zone_unit.active_unit_cnt_ <= 0) {
    ret = OB_RESOURCE_OUT;
    LOG_WARN("no active unit", K(ret), "zone", zone_unit.zone_);
  } else {
    FOREACH(u, zone_unit.all_unit_)
    {
      if (!(*u)->server_->active_ || !(*u)->server_->online_ || (*u)->server_->blocked_) {
        continue;
      }
      if (NULL == us) {
        us = (*u);
      } else {
        if (us->tg_pg_cnt_ > (*u)->tg_pg_cnt_) {
          us = (*u);
        } else if (us->tg_pg_cnt_ == (*u)->tg_pg_cnt_ && us->get_load() > (*u)->get_load()) {
          us = (*u);
        }
      }
    }
  }
  return ret;
}

int TenantBalanceStat::choose_data_source(const Partition& p, const ObReplicaMember& dest_member,
    const ObReplicaMember& src_member, bool is_rebuild, ObReplicaMember& data_source, int64_t& data_size,
    int64_t task_type, int64_t& cluster_id, const common::ObAddr* hint_addr_p)
{
  int ret = OB_SUCCESS;
  ObArray<Replica> replica_infos;
  FOR_BEGIN_END_E(r, p, this->all_replica_, OB_SUCC(ret))
  {
    if (OB_FAIL(replica_infos.push_back(*r))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ret = choose_data_source(zone_mgr_,
        server_mgr_,
        remote_pt_operator_,
        task_mgr_,
        this,
        replica_infos,
        p,
        dest_member,
        src_member,
        is_rebuild,
        data_source,
        data_size,
        task_type,
        cluster_id,
        *sql_proxy_,
        min_source_replica_version_,
        hint_addr_p);
  }
  return ret;
}
/*
 * Select data source rules
 * 1. same server (server status changed)
 * 2. same zone
 * 3. same region
 * 4. random F
 *
 * Copy sstable data, regardless of whether it is READONLY or FULL
 */
int TenantBalanceStat::choose_data_source(ObZoneManager* zone_mgr, ObServerManager* server_mgr,
    ObRemotePartitionTableOperator* remote_pt_operator, ObRebalanceTaskMgr* task_mgr, const TenantBalanceStat* self,
    const ObIArray<Replica>& replica_infos, const Partition& p, const ObReplicaMember& dest_member,
    const ObReplicaMember& src_member, bool is_rebuild, ObReplicaMember& data_source, int64_t& data_size,
    int64_t task_type, int64_t& cluster_id, common::ObMySQLProxy& sql_proxy, const int64_t min_source_replica_version,
    const common::ObAddr* hint_addr_p)
{
  UNUSED(cluster_id);
  UNUSED(data_size);
  UNUSED(sql_proxy);
  UNUSED(min_source_replica_version);
  UNUSED(remote_pt_operator);
  int ret = OB_SUCCESS;
  data_source.reset();
  ObZone dest_zone;
  ObRegion dest_region;
  ObDataSourceCandidateChecker type_checker(dest_member.get_replica_type());
  const Replica* src = NULL;
  if (OB_ISNULL(zone_mgr) || OB_ISNULL(server_mgr) || OB_ISNULL(remote_pt_operator) || OB_ISNULL(task_mgr) ||
      !dest_member.is_valid() || OB_INVALID_ID == p.table_id_ || (!p.can_balance_ && !is_rebuild) ||
      (!p.is_leader_valid() && !is_rebuild && ObRebalanceTaskType::COPY_SSTABLE != task_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_member), "partition", p);
  } else if (OB_FAIL(server_mgr->get_server_zone(dest_member.get_server(), dest_zone))) {
    LOG_WARN("get server zone failed", K(ret), "server", dest_member.get_server());
    ret = OB_SUCCESS;  // ignore this error
  } else if (OB_FAIL(zone_mgr->get_region(dest_zone, dest_region))) {
    LOG_WARN("get region failed", K(dest_zone), "server", dest_member.get_server(), K(ret));
    ret = OB_SUCCESS;  // ignore this error
  }

  // TODO: whether a readonly replica can be the data source
  if (OB_SUCC(ret)) {
    // If you pass in hint,
    // give priority to using hint,
    // mainly for batch migration
    if (NULL != hint_addr_p) {
      const ObAddr& hint_addr = *hint_addr_p;
      if (OB_UNLIKELY(!hint_addr.is_valid())) {
        // print a log warn and go on the choose data src from below
        LOG_INFO("invalid hint addr, ignore", K(hint_addr));
      } else {
        FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret))
        {
          if (REPLICA_STATUS_NORMAL == r->replica_status_ && r->server_->online_ && !r->server_->stopped_ &&
              type_checker.is_candidate(r->replica_type_) && r->server_->server_ == hint_addr &&
              (nullptr == src || src->get_memstore_percent() < r->get_memstore_percent()) &&
              !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
            src = &(*r);
          }
        }
      }
    }
    // try task.offline_
    if (nullptr == src) {
      FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret) && src_member.is_valid())
      {
        if (REPLICA_STATUS_NORMAL == r->replica_status_ && r->server_->online_ && !r->server_->stopped_ &&
            type_checker.is_candidate(r->replica_type_) && r->server_->server_ == src_member.get_server() &&
            (nullptr == src || src->get_memstore_percent() < r->get_memstore_percent()) &&
            !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
          src = &(*r);
        }
      }
    }
    // try same zone replica
    if (nullptr == src) {
      FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret) && !dest_zone.is_empty())
      {
        if (REPLICA_STATUS_NORMAL == r->replica_status_ && r->server_->online_ && !r->server_->stopped_ &&
            r->zone_ == dest_zone && type_checker.is_candidate(r->replica_type_) &&
            r->server_->server_ != dest_member.get_server() &&
            (nullptr == src || src->get_memstore_percent() < r->get_memstore_percent()) &&
            !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
          src = &(*r);
        }
      }
    }
    // try same region
    if (nullptr == src) {
      FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret) && !dest_zone.is_empty() && !dest_region.is_empty())
      {
        if (REPLICA_STATUS_NORMAL == r->replica_status_ && r->server_->online_ && !r->server_->stopped_ &&
            r->region_ == dest_region && type_checker.is_candidate(r->replica_type_) &&
            r->server_->server_ != dest_member.get_server() &&
            (nullptr == src || src->get_memstore_percent() < r->get_memstore_percent()) &&
            !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
          src = &(*r);
        }
      }
    }

    // try to choose any qualified replica
    if (NULL == src) {
      ObSEArray<const Replica*, 32> candidates;
      FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret))
      {
        if (dest_member.get_server() != r->server_->server_) {
          if (REPLICA_STATUS_NORMAL == r->replica_status_ && r->server_->online_ && !r->server_->stopped_ &&
              type_checker.is_candidate(r->replica_type_) &&
              !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
            if (OB_FAIL(candidates.push_back(&(*r)))) {
              LOG_WARN("fail save replica to candidates list", K(*r), K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && candidates.count() > 0) {
        ObSEArray<ObAddr, 32> addrs;
        FOREACH_X(r, candidates, OB_SUCC(ret))
        {
          if (OB_FAIL(addrs.push_back((*r)->server_->server_))) {
            LOG_WARN("fail push addr", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObSEArray<int64_t, 32> idxs;
          // Record eligible addrs subscripts
          if (OB_FAIL(task_mgr->get_min_out_data_size_server(addrs, idxs))) {
            LOG_WARN("fail get min in sched out cnt server", K(addrs), K(ret));
          } else if (idxs.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid addr idx", K(idxs), "max_cnt", addrs.count());
          } else {
            int64_t addr_idx = idxs.at(random() % idxs.count());
            if (addr_idx < 0 || addr_idx >= candidates.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid addr idx", K(addr_idx), "max_cnt", candidates.count());
            } else {
              src = candidates.at(addr_idx);
            }
          }
        }
      }
    }

    // try to choose replica data_version equal or greater than last merged version
    if (OB_SUCC(ret) && (NULL == src || src_member.get_server() != src->server_->server_)) {
      int64_t last_merged_version = 0;
      if (OB_FAIL(zone_mgr->get_global_last_merged_version(last_merged_version))) {
        LOG_WARN("get global last merged version failed", K(ret), K(last_merged_version));
      } else {
        if (NULL == src || src->data_version_ < last_merged_version) {
          FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret))
          {
            if (REPLICA_STATUS_NORMAL == r->replica_status_ && type_checker.is_candidate(r->replica_type_) &&
                r->server_->online_ && !r->server_->stopped_ && r->server_->server_ != dest_member.get_server() &&
                r->data_version_ >= last_merged_version &&
                !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
              src = &(*r);
              break;
            }
          }
        }
      }
    }

    // try to choose replica with max data_version (for rebuild replica)
    if (OB_SUCC(ret) && is_rebuild) {
      FOREACH_CNT_X(r, replica_infos, OB_SUCC(ret))
      {
        if (r->data_version_ > 0 && REPLICA_STATUS_NORMAL == r->replica_status_ &&
            (NULL == src || r->data_version_ > src->data_version_) && type_checker.is_candidate(r->replica_type_) &&
            r->server_->online_ && !r->server_->stopped_ && dest_member.get_server() != r->server_->server_ &&
            !r->is_in_blacklist(task_type, dest_member.get_server(), self)) {
          src = &(*r);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL != src) {
        data_source = ObReplicaMember(
            src->server_->server_, src->member_time_us_, src->replica_type_, src->get_memstore_percent());
        data_size = src->load_factor_.get_disk_used();
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no valid source candidates, may in black list", K(ret));
      }
    }
  }
  return ret;
}

int TenantBalanceStat::get_remote_cluster_data_source(ObRemotePartitionTableOperator* remote_pt_operator,
    const Partition& p, share::ObPartitionReplica& partition_replica, ObDataSourceCandidateChecker& type_checker,
    const int64_t min_source_replica_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(remote_pt_operator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(remote_pt_operator));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    share::ObPartitionInfo partition_info;
    partition_info.set_allocator(&allocator);
    if (OB_FAIL(remote_pt_operator->get(p.table_id_, p.partition_id_, partition_info))) {
      LOG_WARN("failed to get partition", K(ret), K(p));
    } else {
      const share::ObPartitionReplica* replica = NULL;
      for (int64_t i = 0; i < partition_info.replica_count(); i++) {
        const share::ObPartitionReplica& tmp_replica = partition_info.get_replicas_v2().at(i);
        if (!tmp_replica.is_in_service() || !type_checker.is_candidate(tmp_replica.replica_type_)) {
          // nothing todo
        } else if (tmp_replica.data_version_ < min_source_replica_version) {
          // nothing todo
        } else if (OB_ISNULL(replica)) {
          replica = &tmp_replica;
        } else if (replica->data_version_ < tmp_replica.data_version_) {
          replica = &tmp_replica;
        }
      }  // end for
      if (OB_SUCC(ret) && OB_NOT_NULL(replica)) {
        if (OB_FAIL(partition_replica.assign(*replica))) {
          LOG_WARN("failed to assign partitionreplica", K(ret), K(replica));
        }
      }
    }
  }
  return ret;
}

bool TenantBalanceStat::can_rereplicate_readonly(const Partition& p) const
{
  // If there is a copy that the restore has not completed, the R copy is not allowed
  bool bret = true;
  FOR_BEGIN_END(r, p, all_replica_)
  {
    if (OB_ISNULL(r)) {
      LOG_WARN("replica is null", K(p));
    } else if (r->is_standby_restore()) {
      bret = false;
      break;
    }
  }
  return bret;
}

// Determine whether there will be no ownership or minority
// if the member of the replica on src changes
//
// Four conditions:
//(1) There is currently a master;
//(2) At least two Fs exist when migrating F;
//(3) The number of paxos replicas is greater than the majority;
//(4) Split or minor is complete
bool TenantBalanceStat::has_leader_while_member_change(
    const Partition& p, const ObAddr& src, const bool ignore_is_in_spliting) const
{
  bool bret = false;
  const TenantBalanceStat& ts = *this;
  int64_t full_replica_count = 0;
  int64_t paxos_replica_num = 0;
  ObReplicaType migrate_type = REPLICA_TYPE_MAX;
  FOR_BEGIN_END(r, p, ts.all_replica_)
  {
    if (!r->is_in_service()) {
      // nothing todo
    } else {
      if (r->server_->server_ == src) {
        migrate_type = r->replica_type_;
      }
      if (ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
        paxos_replica_num++;
        if (REPLICA_TYPE_FULL == r->replica_type_) {
          full_replica_count++;
        }
      }
    }
  }
  if (p.is_in_spliting() && !ignore_is_in_spliting) {
    // The partition that is being split
    // and the source partition has not been dumped cannot be changed unless it is forced to delete.
    // In the case of offline replica, it can be changed.
    bret = false;
  } else {
    if (!ObReplicaTypeCheck::is_paxos_replica_V2(migrate_type)) {
      bret = true;
    } else if (p.has_leader()) {  // migrate paxos replica
      bret = true;
      if (REPLICA_TYPE_FULL == migrate_type) {
        // If one f is migrated; another F is required to exist
        bret = (full_replica_count >= 2);
      }
      if (bret) {
        // The current number of paxos replicas is required to be greater than majory
        bret = (p.filter_logonly_count_ + paxos_replica_num - 1 >= majority(p.schema_replica_cnt_));
      }
    } else {
      // no leader
      bret = false;
    }
  }
  if (!bret) {
    LOG_WARN("may has no leader while member change", K(p), K(src), K(paxos_replica_num));
  }
  return bret;
}

// Verify whether the partition has a replica on dest
bool TenantBalanceStat::has_replica(const Partition& p, const ObAddr& dest) const
{
  bool bret = false;
  const TenantBalanceStat& ts = *this;
  FOR_BEGIN_END(r, p, ts.all_replica_)
  {
    if (!r->is_in_service()) {
      // nothing todo
    } else if (r->server_->server_ == dest) {
      bret = true;
      break;
    }
  }
  return bret;
}

int TenantBalanceStat::get_full_replica_num(const Partition& p, int64_t& count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const uint64_t schema_id = p.table_id_;
    int64_t full_count = 0;
    if (!is_tablegroup_id(schema_id)) {
      const ObTableSchema* table_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_table_schema(schema_id, table_schema))) {
        LOG_WARN("fail to get table schema", "table_id", schema_id);
      } else if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not found", K(ret), "table_id", schema_id);
      } else if (OB_FAIL(table_schema->get_full_replica_num(*schema_guard_, full_count))) {
        LOG_WARN("fail to get full replica num", K(ret));
      } else if (full_count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid paxos full_count", K(ret));
      } else {
        count = full_count;
      }
    } else {
      const ObTablegroupSchema* tg_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_tablegroup_schema(schema_id, tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", schema_id);
      } else if (OB_UNLIKELY(NULL == tg_schema)) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("tg not found", K(ret), "tg_id", schema_id);
      } else if (OB_FAIL(tg_schema->get_full_replica_num(*schema_guard_, full_count))) {
        LOG_WARN("fail to get full replica num", K(ret));
      } else if (full_count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid paxos full_count", K(ret));
      } else {
        count = full_count;
      }
    }
  }
  return ret;
}
// when get quorum can't simply get paxos replica num from the current schema,
// the following disaster recovery scenarios need to be considered.
// The locality of a certain partiton is F@z1,F@z2,F@z3;
// The server where the z2 replica is located is removed from the member list due to permanent offline.
// At this time, the replica distribution of partition is z1 one F, z3 one F.
// Begin to change locality, and change the locality of the partition to F@z1, F@z2.
// The RS rebalance task first add the replica on z2.
// If used directly, the supplementary copy is passed to the clog layer with a quorum value of 2,
// but at this time there are already two members on z1 and z3 in the clog layer and cannot be added.
// So the add replica failed.
// When calculating quorum, the distribution of existing copies needs to be considered at the same time.
int TenantBalanceStat::get_schema_quorum_size(const Partition& p, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  quorum = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const uint64_t schema_id = p.table_id_;
    int64_t paxos_count = 0;
    if (!is_tablegroup_id(schema_id)) {
      const ObTableSchema* table_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_table_schema(schema_id, table_schema))) {
        LOG_WARN("fail to get table schema", "table_id", schema_id);
      } else if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not found", K(ret), "table_id", schema_id);
      } else if (OB_FAIL(table_schema->get_paxos_replica_num(*schema_guard_, paxos_count))) {
        LOG_WARN("fail to get full replica num", K(ret));
      } else if (paxos_count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid paxos full_count", K(ret));
      } else {
        quorum = paxos_count;
      }
    } else {
      const ObTablegroupSchema* tg_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_tablegroup_schema(schema_id, tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", schema_id);
      } else if (OB_UNLIKELY(NULL == tg_schema)) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("tg not found", K(ret), "tg_id", schema_id);
      } else if (OB_FAIL(tg_schema->get_paxos_replica_num(*schema_guard_, paxos_count))) {
        LOG_WARN("fail to get full replica num", K(ret));
      } else if (paxos_count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid paxos full_count", K(ret));
      } else {
        quorum = paxos_count;
      }
    }
  }
  return ret;
}

int TenantBalanceStat::get_migrate_replica_quorum_size(const Partition& p, int64_t& quorum) const
{
  // The migration task will not adjust the quorum value,
  // just take the quorum value reported by the observer.
  int ret = OB_SUCCESS;
  quorum = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_guard_) || OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard or unit_mgr is null", K(ret), KP(schema_guard_), KP(unit_mgr_));
  } else if (OB_INVALID_ID == p.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(p));
  } else if (!p.is_valid_quorum()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum", K(ret), K(p));
  } else if (p.in_physical_restore()) {
    // skip
  } else {
    ObQuorumGetter quorum_getter(*schema_guard_, *unit_mgr_, *zone_mgr_);
    if (OB_FAIL(quorum_getter.get_migrate_replica_quorum_size(p.table_id_, p.quorum_, quorum))) {
      LOG_WARN("fail to get migrate replica quorum size", K(ret), K(p));
    }
  }
  return ret;
}

int TenantBalanceStat::get_add_replica_quorum_size(
    const Partition& p, const ObZone& zone, const ObReplicaType type, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  quorum = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_guard_) || OB_ISNULL(unit_mgr_) || OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard or unit_mgr or zone_mgr_ is null", K(ret), KP(schema_guard_), KP(unit_mgr_), KPC(zone_mgr_));
  } else if (OB_INVALID_ID == p.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(p));
  } else if (!p.is_valid_quorum()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum", K(ret), K(p));
  } else if (p.in_physical_restore()) {
    // skip
  } else {
    ObQuorumGetter quorum_getter(*schema_guard_, *unit_mgr_, *zone_mgr_);
    if (OB_FAIL(quorum_getter.get_add_replica_quorum_size(p.table_id_, p.quorum_, zone, type, quorum))) {
      LOG_WARN("fail to get add replica quorum size", K(p), K(zone), K(type));
    }
  }
  return ret;
}

int TenantBalanceStat::get_transform_quorum_size(const Partition& p, const ObZone& zone, const ObReplicaType src_type,
    const ObReplicaType dst_type, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  quorum = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_guard_) || OB_ISNULL(unit_mgr_) || OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard or unit_mgr or zone_mgr_ is null", K(ret), KP(schema_guard_), KP(unit_mgr_), KPC(zone_mgr_));
  } else if (OB_INVALID_ID == p.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(p));
  } else if (!p.is_valid_quorum()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum", K(ret), K(p));
  } else if (p.in_physical_restore()) {
    // skip
  } else {
    ObQuorumGetter quorum_getter(*schema_guard_, *unit_mgr_, *zone_mgr_);
    if (OB_FAIL(quorum_getter.get_transform_quorum_size(p.table_id_, p.quorum_, zone, src_type, dst_type, quorum))) {
      LOG_WARN("fail to get transform quorum size", K(p), K(zone), K(src_type), K(dst_type));
    }
  }
  return ret;
}

int TenantBalanceStat::get_remove_replica_quorum_size(
    const Partition& p, const ObZone& zone, const ObReplicaType type, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_guard_) || OB_ISNULL(unit_mgr_) || OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard or unit_mgr or zone_mgr_ is null", K(ret), KP(schema_guard_), KP(unit_mgr_), KPC(zone_mgr_));
  } else if (OB_INVALID_ID == p.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(p));
  } else if (!p.is_valid_quorum()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum", K(ret), K(p));
  } else if (p.in_physical_restore()) {
    // skip
  } else {
    ObQuorumGetter quorum_getter(*schema_guard_, *unit_mgr_, *zone_mgr_);
    if (OB_FAIL(quorum_getter.get_remove_replica_quorum_size(p.table_id_, p.quorum_, zone, type, quorum))) {
      LOG_WARN("fail to get remove replica quorum size", K(p), K(zone), K(type));
    }
  }
  return ret;
}

void TenantBalanceStat::print_stat()
{
  LOG_INFO("================ BALANCE INFO ================", "tenant", tenant_id_);
  print_unit_load();
  // print_replica_load();
}

void TenantBalanceStat::print_unit_load()
{
  LOG_INFO("================ UNIT LOAD ================");
  // Zone -> Unit
  int64_t zone_idx = 0;
  FOREACH(zone, all_zone_unit_)
  {
    LOG_INFO(
        "|--", "idx", zone_idx, "zone", zone->zone_, "#unit", zone->active_unit_cnt_, "load", zone->get_avg_load());
    int64_t unit_idx = 0;
    FOREACH(unit, zone->all_unit_)
    {
      LOG_INFO(
          "  |--", "idx", unit_idx, "unit", (*unit)->info_, "load", (*unit)->get_load(), "server", *((*unit)->server_));
      ++unit_idx;
    }
    ++zone_idx;
  }
}

void TenantBalanceStat::print_replica_load()
{
  LOG_INFO("================ REPLICA LOAD ================");
  FOREACH(part, all_partition_)
  {
    LOG_INFO("|--",
        "table_id",
        part->table_id_,
        "part_id",
        part->partition_id_,
        "def_replica#",
        part->schema_replica_cnt_,
        "valid_replica#",
        part->valid_member_cnt_);
    for (int64_t i = part->begin_; i < part->end_ && i < all_replica_.count(); ++i) {
      const Replica& r = all_replica_.at(i);
      LOG_INFO("  |--",
          "unit",
          r.unit_->info_.unit_.unit_id_,
          "type",
          r.replica_type_,
          "role",
          r.role_,
          "load",
          r.load_factor_,
          "zone",
          r.zone_,
          "server",
          r.server_->server_);
    }
  }
}

/////////////////
// Data required for load balancing, filled in at runtime
int TenantBalanceStat::gather_unit_balance_stat(ZoneUnit& zu, UnitStat& u)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_unit_pg_by_load(zu, u))) {
    LOG_WARN("fail sort unit pg by load", K(zu), K(ret));
  }
  // XXX more runtime gathering goes here
  return ret;
}

int TenantBalanceStat::sort_unit_pg_by_load(ZoneUnit& zu, UnitStat& u)
{
  int ret = OB_SUCCESS;

  sorted_pg_.reset();

  if (OB_FAIL(sorted_pg_.reserve(all_pg_.count()))) {
    LOG_WARN("fail reserve memory", K(ret));
  } else {
    // Because load balancing is in-zone balancing
    // Therefore, to calculate the load factor of pg, only the current zone is considered.
    FOREACH_X(pg, all_pg_, OB_SUCC(ret))
    {
      FOR_BEGIN_END_E(r, *sorted_partition_.at(pg->begin_), all_replica_, OB_SUCC(ret))
      {
        if (r->unit_ == &u && r->zone_ == zu.zone_) {
          if (OB_FAIL(sorted_pg_.push_back(*pg))) {
            LOG_WARN("fail push pg", K(ret));
          }
          break;
        }
      }
    }
    FOREACH_X(pg, sorted_pg_, OB_SUCC(ret))
    {
      pg->load_factor_.reset();
      for (int64_t i = pg->begin_; i < pg->end_; ++i) {
        FOR_BEGIN_END_E(r, *sorted_partition_.at(i), all_replica_, OB_SUCC(ret))
        {
          if (r->unit_ == &u && r->zone_ == zu.zone_) {
            pg->load_factor_ += r->load_factor_;
            break;
          }
        }
      }
    }
  }
  for (int64_t rest_idx = 0; rest_idx < sorted_pg_.count() && OB_SUCC(ret); ++rest_idx) {
    int64_t max_idx = rest_idx;
    double max_load = u.calc_load(zu.resource_weight_, sorted_pg_.at(max_idx).load_factor_);
    for (int64_t pg_idx = rest_idx + 1; pg_idx < sorted_pg_.count(); ++pg_idx) {
      double load = u.calc_load(zu.resource_weight_, sorted_pg_.at(pg_idx).load_factor_);
      if (load > max_load) {
        max_load = load;
        max_idx = pg_idx;
      }
    }
    // swap
    if (max_idx != rest_idx) {
      PartitionGroup pg = sorted_pg_.at(rest_idx);
      sorted_pg_.at(rest_idx) = sorted_pg_.at(max_idx);
      sorted_pg_.at(max_idx) = pg;
    }
  }
  return ret;
}

/////////////////
bool Replica::operator==(const Replica& that) const
{
  return (unit_ == that.unit_ && server_ == that.server_ && region_ == that.region_ && zone_ == that.zone_ &&
          member_time_us_ == that.member_time_us_ && in_member_list_ == that.in_member_list_ && role_ == that.role_ &&
          data_version_ == that.data_version_ && replica_status_ == that.replica_status_ &&
          replica_type_ == that.replica_type_ && readonly_at_all_ == that.readonly_at_all_ &&
          modify_time_us_ == that.modify_time_us_ && quorum_ == that.quorum_ &&
          additional_replica_status_ == that.additional_replica_status_);
}

int TenantBalanceStat::get_all_pg_idx(const common::ObPartitionKey& pkey, const int64_t all_tg_idx, int64_t& all_pg_idx)
{
  int ret = OB_SUCCESS;
  if (all_tg_idx < 0 || all_tg_idx >= all_tg_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(all_tg_idx), "tg_count", all_tg_.count());
  } else {
    bool found = false;
    const TableGroup& tg = all_tg_.at(all_tg_idx);
    for (int64_t i = tg.begin_; !found && OB_SUCC(ret) && i < tg.end_; ++i) {
      PartitionGroup& pg = all_pg_.at(i);
      for (int64_t j = pg.begin_; !found && OB_SUCC(ret) && j < pg.end_; ++j) {
        Partition* partition = sorted_partition_.at(j);
        if (nullptr == partition) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition ptr is null", K(ret));
        } else if (partition->table_id_ == pkey.get_table_id() && partition->partition_id_ == pkey.get_partition_id()) {
          all_pg_idx = partition->all_pg_idx_;
          found = true;
        } else {
          // not match, go on get next
        }
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find partition in sorted partition", K(ret), K(pkey));
    }
  }
  return ret;
}

int TenantBalanceStat::get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_INVALID_ID != tablegroup_id) {
    ARRAY_FOREACH_X(all_tg_, idx, cnt, !found)
    {
      if (all_tg_.at(idx).tablegroup_id_ == tablegroup_id) {
        all_tg_idx = idx;
        found = true;
      }
    }
  } else {
    ARRAY_FOREACH_X(all_pg_, idx, cnt, !found)
    {
      if (all_pg_.at(idx).table_id_ == table_id) {
        all_tg_idx = all_pg_.at(idx).all_tg_idx_;
        found = true;
      }
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find tablegroup in all_tg", K(tablegroup_id), K(table_id), K(ret));
  }
  return ret;
}

int TenantBalanceStat::get_primary_partition_unit(
    const ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, uint64_t& unit_id)
{
  int ret = OB_SUCCESS;

  if (all_tg_idx < 0 || all_tg_idx >= all_tg_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(all_tg_idx), "total", all_tg_.count(), K(ret));
  } else {
    // Take the unit_id of the unit where the first table partition of the part_idx pg in the table group is located,
    // and can get the unit of the primary partition
    bool found = false;
    unit_id = OB_INVALID_ID;
    TableGroup& tg = all_tg_.at(all_tg_idx);
    FOR_BEGIN_END_E(pg, tg, all_pg_, !found)
    {
      if (pg->partition_idx_ == part_idx) {
        // Just look at the first partition in pg (primary partition)
        Partition& primary_partition = *sorted_partition_.at(pg->begin_);
        FOR_BEGIN_END_E(r, primary_partition, all_replica_, !found)
        {
          if (r->zone_ == zone && OB_INVALID_ID != r->unit_->info_.unit_.unit_id_ &&
              REPLICA_STATUS_NORMAL == r->replica_status_) {
            unit_id = r->unit_->info_.unit_.unit_id_;
            found = true;
          }
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("partition not found in zone", K(zone), K(tg), K(part_idx), K(all_tg_idx), K(ret));
    }
  }
  return ret;
}

int TenantBalanceStat::get_partition_group_data_size(
    const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, int64_t& data_size)
{
  int ret = OB_SUCCESS;
  if (all_tg_idx < 0 || all_tg_idx >= all_tg_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(all_tg_idx), "total", all_tg_.count(), K(ret));
  } else {
    bool found = false;
    data_size = 0;
    TableGroup& tg = all_tg_.at(all_tg_idx);
    FOR_BEGIN_END_E(pg, tg, all_pg_, !found)
    {
      if (pg->partition_idx_ == part_idx) {
        FOR_BEGIN_END(p, *pg, sorted_partition_)
        {
          FOR_BEGIN_END(r, **p, all_replica_)
          {
            if (r->zone_ == zone) {
              data_size += r->load_factor_.get_disk_used();
            }
          }
        }
        found = true;
      }
    }
  }
  return ret;
}

int TenantBalanceStat::get_partition_entity_ids_by_tg_idx(
    const int64_t tablegroup_idx, common::ObIArray<uint64_t>& tids)
{
  int ret = OB_SUCCESS;
  if (tablegroup_idx < 0 || tablegroup_idx >= all_tg_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tablegroup_idx), "count", all_tg_.count(), K(ret));
  } else {
    TableGroup& tg = all_tg_.at(tablegroup_idx);
    if (tg.begin_ < tg.end_) {
      PartitionGroup& primary_pg = all_pg_.at(tg.begin_);
      FOR_BEGIN_END_E(p, primary_pg, sorted_partition_, OB_SUCC(ret))
      {
        if (OB_FAIL(tids.push_back((*p)->table_id_))) {
          LOG_WARN("fail push back table ids for primary partitions", K(**p), K(ret));
        }
      }
    } else {
      // empty tg
    }
  }
  return ret;
}

int TenantBalanceStat::get_leader_addr(const Partition& partition, ObAddr& leader, bool& valid_leader)
{
  int ret = OB_SUCCESS;
  leader.reset();
  valid_leader = false;
  FOR_BEGIN_END_E(r, partition, all_replica_, OB_SUCC(ret))
  {
    if (OB_ISNULL(r)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("replica is null", K(ret), K(partition));
    } else if (r->is_leader_by_election() && r->is_in_member_list() && r->quorum_ == partition.quorum_ &&
               !OB_ISNULL(r->server_)) {
      leader = r->server_->server_;
      valid_leader = true;
      break;
    }
  }
  return ret;
}

int TenantBalanceStat::get_leader_and_member_list(
    const Partition& partition, const Replica*& leader, ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  leader = NULL;
  FOR_BEGIN_END_E(r, partition, all_replica_, OB_SUCC(ret))
  {
    if (OB_ISNULL(r)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("replica is null", K(ret), K(partition));
    } else if (r->is_in_member_list()) {
      if (r->is_leader_by_election() && r->quorum_ == partition.quorum_) {
        leader = r;
      }
      if (OB_ISNULL(r->server_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server is null", K(ret), K(partition));
      } else if (OB_FAIL(member_list.add_server(r->server_->server_))) {
        LOG_WARN("fail to add member", K(ret), K(r->server_));
      }
    }
  }
  return ret;
}

// check if table locality str changed
int TenantBalanceStat::check_table_locality_changed(
    share::schema::ObSchemaGetterGuard& guard, const uint64_t table_id, bool& locality_changed) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_sys_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  int64_t new_version = OB_INVALID_VERSION;
  int64_t pre_version = OB_INVALID_VERSION;
  locality_changed = true;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(table_id));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", K(ret));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, new_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (OB_FAIL(schema_guard_->get_schema_version(tenant_id, pre_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (new_version == pre_version) {
    locality_changed = false;
  } else {
    const share::schema::ObSchemaType schema_type =
        (is_new_tablegroup_id(table_id) ? ObSchemaType::TABLEGROUP_SCHEMA : ObSchemaType::TABLE_SCHEMA);
    const ObPartitionSchema* new_partition_schema = nullptr;
    const ObPartitionSchema* old_partition_schema = nullptr;
    const ObString* new_locality_str = nullptr;
    const ObString* old_locality_str = nullptr;
    if (OB_FAIL(ObPartMgrUtils::get_partition_schema(guard, table_id, schema_type, new_partition_schema)) ||
        OB_FAIL(ObPartMgrUtils::get_partition_schema(*schema_guard_, table_id, schema_type, old_partition_schema))) {
      LOG_WARN("fail to get schema", K(ret), K(table_id));
    } else if (OB_ISNULL(new_partition_schema) || OB_ISNULL(old_partition_schema)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (OB_FAIL(new_partition_schema->get_locality_str_inherit(guard, new_locality_str)) ||
               OB_FAIL(old_partition_schema->get_locality_str_inherit(*schema_guard_, old_locality_str))) {
      LOG_WARN("fail to get locality str", K(ret));
    } else if (OB_ISNULL(new_locality_str) || OB_ISNULL(old_locality_str)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality str is null", K(ret), K(table_id));
    } else if (0 == new_locality_str->case_compare(*old_locality_str)) {
      locality_changed = false;
    } else {
      LOG_INFO("locality changed", K(ret), K(new_version), K(pre_version), K(table_id));
    }
  }
  return ret;
}

int TenantBalanceStat::check_table_schema_changed(
    share::schema::ObSchemaGetterGuard& guard, const uint64_t table_id, bool& schema_changed) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_sys_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  int64_t new_version = OB_INVALID_VERSION;
  int64_t pre_version = OB_INVALID_VERSION;
  schema_changed = true;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(table_id));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", K(ret));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, new_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (OB_FAIL(schema_guard_->get_schema_version(tenant_id, pre_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (new_version == pre_version) {
    schema_changed = false;
  } else {
    const share::schema::ObSchemaType schema_type =
        (is_new_tablegroup_id(table_id) ? ObSchemaType::TABLEGROUP_SCHEMA : ObSchemaType::TABLE_SCHEMA);
    const ObPartitionSchema* new_partition_schema = nullptr;
    const ObPartitionSchema* old_partition_schema = nullptr;
    const ObString* new_locality_str = nullptr;
    const ObString* old_locality_str = nullptr;
    if (OB_FAIL(ObPartMgrUtils::get_partition_schema(guard, table_id, schema_type, new_partition_schema)) ||
        OB_FAIL(ObPartMgrUtils::get_partition_schema(*schema_guard_, table_id, schema_type, old_partition_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (OB_ISNULL(new_partition_schema) || OB_ISNULL(old_partition_schema)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema", K(ret), K(table_id));
    } else if (new_partition_schema->get_tablegroup_id() != old_partition_schema->get_tablegroup_id()) {
      schema_changed = true;
      LOG_INFO("tablegroup changed after gather stat",
          K(ret),
          K(new_version),
          K(pre_version),
          K(table_id),
          "old_tablegroup_id",
          old_partition_schema->get_tablegroup_id(),
          "new_tablegroup_id",
          new_partition_schema->get_tablegroup_id());
    } else if (OB_FAIL(new_partition_schema->get_locality_str_inherit(guard, new_locality_str)) ||
               OB_FAIL(old_partition_schema->get_locality_str_inherit(*schema_guard_, old_locality_str))) {
      LOG_WARN("fail to get locality str", K(ret));
    } else if (OB_ISNULL(new_locality_str) || OB_ISNULL(old_locality_str)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality str is null", K(ret), K(table_id));
    } else if (0 == new_locality_str->case_compare(*old_locality_str)) {
      schema_changed = false;
    } else {
      LOG_INFO("locality changed", K(ret), K(new_version), K(pre_version), K(table_id));
    }
  }
  return ret;
}

void TenantBalanceStat::update_last_run_timestamp()
{
  static_cast<ObRootBalancer*>(check_stop_provider_)->update_last_run_timestamp();
}

int ObCanMigratePartitionChecker::can_migrate_out(ObZoneManager& zone_mgr, UnitStat& src_unit, bool& need_balance)
{
  int ret = OB_SUCCESS;
  need_balance = false;
  // When the load reaches the emergency_load or more,
  // balance will be done even in daily merge
  double emergency_load = static_cast<double>(GCONF.balancer_emergency_percentage) / 100;
  ObZone& zone = src_unit.info_.unit_.zone_;
  HEAP_VAR(ObZoneInfo, zone_info)
  {
    int64_t global_broadcast_version = OB_INVALID_VERSION;
    if (OB_FAIL(zone_mgr.get_global_broadcast_version(global_broadcast_version))) {
      LOG_WARN("fail get global broadcast version", K(ret));
    } else if (OB_FAIL(zone_mgr.get_zone(zone, zone_info))) {
      LOG_WARN("fail get zone info", K(ret));
    } else {
      // In the absence of a disk burst, the unmerged zone does not participate in load balancing
      bool zone_merged = zone_info.is_merged(global_broadcast_version);
      if (!zone_merged && src_unit.get_load() < emergency_load) {
        need_balance = false;
        LOG_WARN("zone not merged, skip load balance",
            K(zone_merged),
            K(zone),
            "max_load_unit_id",
            src_unit.get_unit_id(),
            "load",
            src_unit.get_load());
      } else if (!zone_merged && src_unit.get_load() >= emergency_load) {
        need_balance = true;
        LOG_WARN("zone not merged, but unit load is in emergency mode. do load balance anyway",
            K(zone_merged),
            K(zone),
            "max_load_unit_id",
            src_unit.get_unit_id(),
            "load",
            src_unit.get_load());
      } else {
        need_balance = true;
      }
    }
  }
  return ret;
}

ObReplicaTask::ObReplicaTask()
    : tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      partition_id_(OB_INVALID_PARTITION_ID),
      src_(),
      replica_type_(REPLICA_TYPE_MAX),
      memstore_percent_(100),
      dst_(),
      dst_replica_type_(REPLICA_TYPE_MAX),
      dst_memstore_percent_(100),
      cmd_type_(ObRebalanceTaskType::MAX_TYPE),
      comment_(NULL)
{}

void ObReplicaTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_PARTITION_ID;
  src_.reset();
  replica_type_ = REPLICA_TYPE_MAX;
  memstore_percent_ = 100;
  zone_.reset();
  region_.reset();
  dst_.reset();
  dst_replica_type_ = REPLICA_TYPE_MAX;
  dst_memstore_percent_ = 100;
  cmd_type_ = ObRebalanceTaskType::MAX_TYPE;
  comment_ = NULL;
}

bool ObReplicaTask::is_valid()
{
  return (OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != table_id_ &&
          OB_INVALID_PARTITION_ID != partition_id_ && ObRebalanceTaskType::MAX_TYPE != cmd_type_ && NULL != comment_);
}
