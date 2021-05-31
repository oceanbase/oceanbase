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

#define USING_LOG_PREFIX RS
#include "ob_all_rebalance_replica_stat.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllRebalanceReplicaStat::ObAllRebalanceReplicaStat() : cur_partition_idx_(-1), cur_replica_idx_(-1)
{
  ip_buf_[0] = '\0';
}

ObAllRebalanceReplicaStat::~ObAllRebalanceReplicaStat()
{}

int ObAllRebalanceReplicaStat::init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
    share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  return impl_.init(
      schema_service, unit_mgr, server_mgr, pt_operator, remote_pt_operator, zone_mgr, task_mgr, check_stop_provider);
}

int ObAllRebalanceReplicaStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(impl_.tenant_balance_stat_.reuse_replica_count_mgr())) {
    LOG_WARN("reuse_replica_count_mgr failed", K(ret));
  } else if (OB_FAIL(impl_.get_all_tenant())) {
  } else if (OB_FAIL(impl_.get_table_schema(OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TID))) {
  } else {
    impl_.cur_tenant_idx_ = -1;
    cur_partition_idx_ = -1;
    cur_replica_idx_ = -1;
  }
  return ret;
}

int ObAllRebalanceReplicaStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!impl_.inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(next())) {
    } else if (OB_FAIL(get_row())) {
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllRebalanceReplicaStat::next()
{
  int ret = OB_SUCCESS;
  bool loop = false;
  do {
    loop = false;
    ++cur_replica_idx_;  // next unit
    if (cur_partition_idx_ < 0) {
      cur_partition_idx_ = 0;  // first zone
    }
    if (cur_partition_idx_ >= impl_.tenant_balance_stat_.all_partition_.count()) {  // end of partition
      ++impl_.cur_tenant_idx_;                                                      // next tenant
      if (impl_.cur_tenant_idx_ >= impl_.all_tenants_.count()) {
        ret = OB_ITER_END;
      } else {
        if (OB_FAIL(impl_.init_tenant_balance_stat(impl_.all_tenants_.at(impl_.cur_tenant_idx_)))) {
        } else {
          cur_partition_idx_ = -1;
          cur_replica_idx_ = -1;
          loop = true;
        }
      }
    } else {  // valid partition
      const Partition& partition = impl_.tenant_balance_stat_.all_partition_.at(cur_partition_idx_);

      if (cur_replica_idx_ >= partition.get_replica_count()) {  // end of replica
        cur_partition_idx_++;                                   // next partition
        cur_replica_idx_ = -1;
        loop = true;
      }
    }
  } while (loop);
  return ret;
}

int ObAllRebalanceReplicaStat::get_row()
{
  int ret = OB_SUCCESS;
  impl_.columns_.reuse();
  if (OB_FAIL(get_full_row(impl_.table_schema_, impl_.columns_))) {
    LOG_WARN("fail to get full row", K(ret));
  } else if (OB_FAIL(project_row(impl_.columns_, cur_row_))) {
    LOG_WARN("fail to project row", K_(impl_.columns), K(ret));
  }
  return ret;
}

int ObAllRebalanceReplicaStat::get_full_row(
    const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (cur_partition_idx_ >= impl_.tenant_balance_stat_.all_partition_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch");
  } else if (cur_replica_idx_ >= impl_.tenant_balance_stat_.all_partition_.at(cur_partition_idx_).get_replica_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch");
  } else {
    const Partition& partition = impl_.tenant_balance_stat_.all_partition_.at(cur_partition_idx_);
    const Replica& replica = impl_.tenant_balance_stat_.all_replica_.at(cur_partition_idx_);
    // primary key
    (void)replica.server_->server_.ip_to_string(ip_buf_, MAX_IP_ADDR_LENGTH);
    ADD_COLUMN(set_int, table, "tenant_id", impl_.tenant_balance_stat_.tenant_id_, columns);
    ADD_COLUMN(set_int, table, "table_id", partition.table_id_, columns);
    ADD_COLUMN(set_int, table, "partition_id", partition.partition_id_, columns);
    ADD_COLUMN(set_varchar, table, "svr_ip", ip_buf_, columns);
    ADD_COLUMN(set_int, table, "svr_port", replica.server_->server_.get_port(), columns);
    // other columns
    ADD_COLUMN(set_double, table, "cpu_usage", replica.load_factor_.get_cpu_usage(), columns);
    ADD_COLUMN(set_double, table, "disk_usage", replica.load_factor_.get_disk_usage(), columns);
    ADD_COLUMN(set_double, table, "iops_usage", replica.load_factor_.get_iops_usage(), columns);
    ADD_COLUMN(set_double, table, "memory_usage", replica.load_factor_.get_memory_usage(), columns);
    ADD_COLUMN(set_double, table, "net_packet_usage", replica.load_factor_.get_net_packet_usage(), columns);
    ADD_COLUMN(set_double, table, "net_throughput_usage", replica.load_factor_.get_net_throughput_usage(), columns);

    ADD_COLUMN(set_int, table, "unit_id", replica.unit_->info_.unit_.unit_id_, columns);
    ADD_COLUMN(set_varchar, table, "zone", replica.zone_.str(), columns);
  }
  return ret;
}
