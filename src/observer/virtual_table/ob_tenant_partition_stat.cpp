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

#define USING_LOG_PREFIX SERVER

#include "ob_tenant_partition_stat.h"

#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace observer {
ObTenantPartitionStat::PartitionStat::PartitionStat()
{
  reset();
}

void ObTenantPartitionStat::PartitionStat::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;
  row_count_ = 0;
  diff_percentage_ = 0;
}

bool ObTenantPartitionStat::PartitionStat::is_valid() const
{
  return OB_INVALID_ID != table_id_ && partition_id_ >= 0 && row_count_ >= 0;
}

ObTenantPartitionStat::ObTenantPartitionStat()
    : inited_(false),
      partition_iter_(),
      table_schema_(NULL),
      table_id_(OB_INVALID_ID),
      partition_stats_(),
      partition_idx_(0),
      prefetch_info_(),
      tenant_end_(false)
{}

ObTenantPartitionStat::~ObTenantPartitionStat()
{}

int ObTenantPartitionStat::init(
    ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool ignore_row_checksum = true;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(partition_iter_.init(pt_operator, schema_service, tenant_id, ignore_row_checksum))) {
    LOG_WARN("tenant partition iter init failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_PARTITION_STAT_TID), table_schema_))) {
    LOG_WARN("get_table_schema failed", K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema_ is null", KP_(table_schema), K(ret));
  } else {
    table_id_ = OB_INVALID_ID;
    partition_stats_.reset();
    partition_idx_ = 0;
    prefetch_info_.reuse();
    tenant_end_ = false;
    inited_ = true;
  }
  return ret;
}

int ObTenantPartitionStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!start_to_read_) {
    start_to_read_ = true;
  }

  if (OB_SUCC(ret)) {
    ObArray<Column> columns;
    PartitionStat partition_stat;
    if (OB_FAIL(get_next_partition_stat(partition_stat))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_partition_stat failed", K(ret));
      }
    } else if (OB_FAIL(get_full_row(table_schema_, partition_stat, columns))) {
      LOG_WARN("get_full_row failed", "table_schema", *table_schema_, K(partition_stat), K(ret));
    } else if (OB_FAIL(project_row(columns, cur_row_))) {
      LOG_WARN("project_row failed", K(columns), K(ret));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObTenantPartitionStat::get_next_partition_stat(PartitionStat& partition_stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (partition_idx_ == partition_stats_.count()) {
      if (OB_FAIL(gen_next_table_stat())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get_next_table_stat failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (partition_idx_ < 0 || partition_idx_ >= partition_stats_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partition_idx", K_(partition_idx), "partition_stat count", partition_stats_.count(), K(ret));
      } else {
        partition_stat = partition_stats_[partition_idx_];
        ++partition_idx_;
      }
    }
  }
  return ret;
}

int ObTenantPartitionStat::gen_next_table_stat()
{
  int ret = OB_SUCCESS;
  partition_stats_.reuse();
  ObArray<ObPartitionInfo> partition_infos;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_end_) {
    ret = OB_ITER_END;
  } else if (OB_INVALID_ID != prefetch_info_.get_table_id()) {
    table_id_ = prefetch_info_.get_table_id();
    if (OB_FAIL(partition_infos.push_back(prefetch_info_))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret)) {
      prefetch_info_.reuse();
      if (OB_FAIL(partition_iter_.next(prefetch_info_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("partition_iter next failed", K(ret));
        }
      } else {
        if (OB_INVALID_ID == table_id_) {
          table_id_ = prefetch_info_.get_table_id();
        }
        if (table_id_ == prefetch_info_.get_table_id()) {
          if (OB_FAIL(partition_infos.push_back(prefetch_info_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        } else if (table_id_ > prefetch_info_.get_table_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition table iterator partition not in sorted order",
              K_(table_id),
              "prefetch table_id",
              prefetch_info_.get_table_id(),
              K(ret));
        } else {
          break;
        }
      }
    }

    if (OB_ITER_END == ret) {
      tenant_end_ = true;
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(build_table_stat(partition_infos))) {
        LOG_WARN("build_table_stat failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTenantPartitionStat::build_table_stat(const ObIArray<ObPartitionInfo>& partitions)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (partitions.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partitions is empty", K(partitions), K(ret));
  } else {
    int64_t sum_count = 0;
    FOREACH_CNT_X(partition, partitions, OB_SUCCESS == ret)
    {
      const ObPartitionReplica* leader_replica = NULL;
      if (OB_FAIL(get_leader_replica(*partition, leader_replica))) {
        LOG_WARN("get_leader_replica failed", "partition", *partition, K(ret));
      } else if (NULL == leader_replica) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader_replica is null", KP(leader_replica), K(ret));
      } else {
        sum_count += leader_replica->row_count_;
      }
    }
    if (OB_SUCC(ret)) {
      partition_stats_.reuse();
      partition_idx_ = 0;
      const int64_t avg_count = sum_count / partitions.count();
      PartitionStat partition_stat;
      FOREACH_CNT_X(partition, partitions, OB_SUCCESS == ret)
      {
        partition_stat.reset();
        if (partition->get_replicas_v2().count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition has no replica", "partition", *partition, K(ret));
        } else {
          const ObPartitionReplica& replica = partition->get_replicas_v2().at(0);
          partition_stat.table_id_ = replica.table_id_;
          partition_stat.partition_id_ = replica.partition_id_;
          partition_stat.partition_cnt_ = replica.partition_cnt_;
          partition_stat.row_count_ = replica.row_count_;
          if (0 == avg_count) {
            partition_stat.diff_percentage_ = 0;
          } else {
            partition_stat.diff_percentage_ = (replica.row_count_ - avg_count) / avg_count * 100;
          }
          if (OB_FAIL(partition_stats_.push_back(partition_stat))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantPartitionStat::get_leader_replica(
    const ObPartitionInfo& partition_info, const ObPartitionReplica*& leader_replica)
{
  int ret = OB_SUCCESS;
  leader_replica = NULL;
  if (!partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(partition_info), K(ret));
  } else if (OB_FAIL(partition_info.find_leader_v2(leader_replica))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find leader replica failed", K(ret));
    } else {
      if (OB_FAIL(partition_info.find_latest_leader(leader_replica))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("find latest leader failed", K(ret), K(partition_info));
        }
      } else if (NULL == leader_replica) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader_replica is null", KP(leader_replica), K(ret));
      }
    }
  } else if (NULL == leader_replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader_replica is null", KP(leader_replica), K(ret));
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("partition doesn't have leader", K(partition_info), K(ret));
  }

  return ret;
}

int ObTenantPartitionStat::get_full_row(
    const ObTableSchema* table, const PartitionStat& partition_stat, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (!partition_stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_stat is invalid", K(partition_stat), K(ret));
  } else {
    ADD_COLUMN(set_int, table, "table_id", static_cast<int64_t>(partition_stat.table_id_), columns);
    ADD_COLUMN(set_int, table, "partition_id", partition_stat.partition_id_, columns);
    ADD_COLUMN(set_int, table, "partition_cnt", partition_stat.partition_cnt_, columns);
    ADD_COLUMN(set_int, table, "row_count", partition_stat.row_count_, columns);
    ADD_COLUMN(set_int, table, "diff_percentage", partition_stat.diff_percentage_, columns);
  }
  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
