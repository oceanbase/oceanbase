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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_restore_stat.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObRestoreStat::ObRestoreStat(RestoreJob& job_info, common::ObMySQLProxy* sql_proxy)
    : job_info_(job_info), partition_task_(), restore_op_(), sql_proxy_(sql_proxy)
{}

ObRestoreStat::~ObRestoreStat()
{}

int ObRestoreStat::gather_stat()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_op_.init(sql_proxy_))) {
    LOG_WARN("fail init restore op", K(ret));
  } else if (OB_FAIL(fill_partitions())) {
    LOG_WARN("fail fill partition restore info", K(ret));
  }
  return ret;
}

int ObRestoreStat::fill_partitions()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_op_.get_tasks(job_info_.job_id_, partition_task_))) {
    LOG_WARN("fail get tasks", K(ret));
  } else {
    FOREACH_X(p, partition_task_, OB_SUCC(ret))
    {
      if (p->job_id_ == job_info_.job_id_) {
        p->job_ = &job_info_;
      }
    }
  }
  return ret;
}

ObPhysicalRestoreStat::ObPhysicalRestoreStat(share::schema::ObMultiVersionSchemaService& schema_service,
    common::ObMySQLProxy& sql_proxy, share::ObPartitionTableOperator& pt_operator, const ObPhysicalRestoreJob& job_info,
    bool only_sys, volatile bool& stop)
    : job_info_(job_info),
      partitions_(),
      replicas_(),
      statistic_(),
      only_sys_(only_sys),
      sql_proxy_(sql_proxy),
      guard_(),
      schema_service_(schema_service),
      pt_operator_(pt_operator),
      stop_(stop)
{}

ObPhysicalRestoreStat::~ObPhysicalRestoreStat()
{}

int ObPhysicalRestoreStat::gather_stat()
{
  int ret = OB_SUCCESS;
  if (only_sys_) {
    if (OB_FAIL(fill_sys_partitions())) {
      LOG_WARN("fail fill sys partition restore info", K(ret));
    }
  } else {
    if (OB_FAIL(fill_user_partitions())) {
      LOG_WARN("fail fill sys partition restore info", K(ret));
    }
  }
  return ret;
}

int ObPhysicalRestoreStat::fill_user_partitions()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info_.tenant_id_;
  ObArray<const ObSimpleTableSchemaV2*> tables;
  ObArray<const ObTablegroupSchema*> tablegroups;
  partitions_.reset();
  replicas_.reset();
  statistic_.reset();

  if (only_sys_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mode is not match", K(ret), K_(only_sys));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, guard_))) {
    LOG_WARN("fail to get tenant_schema_guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard_.get_user_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("get tenant table schemas failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard_.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    LOG_WARN("fail to get tenant tablegroup schemas", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); ++i) {
      const ObSimpleTableSchemaV2* table = tables.at(i);
      int64_t table_cnt = 1;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stop", K(ret));
      } else if (OB_UNLIKELY(nullptr == table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (!table->has_self_partition()) {
        // bypass
      } else if (is_inner_table(table->get_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not be inner table", K(ret), "table_id", table->get_table_id());
      } else if (OB_FAIL(add_partition(table->get_table_id(), table_cnt, *table))) {
        LOG_WARN("fail to fill partition entity", K(ret), "table_id", table->get_table_id());
      } else {
        LOG_DEBUG("restore stat add table",
            K(ret),
            "table_id",
            table->get_table_id(),
            "partition_cnt",
            table->get_all_part_num());
      }
    }
    for (int64_t i = 0; i < tablegroups.count() && OB_SUCC(ret); ++i) {
      const ObTablegroupSchema* tablegroup = tablegroups.at(i);
      ObArray<const ObSimpleTableSchemaV2*> tables;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stop", K(ret));
      } else if (OB_UNLIKELY(nullptr == tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema ptr is null", K(ret));
      } else if (!tablegroup->has_self_partition()) {
        // bypass
      } else if (OB_FAIL(guard_.get_table_schemas_in_tablegroup(
                     tablegroup->get_tenant_id(), tablegroup->get_tablegroup_id(), tables))) {
        LOG_WARN("fail to get tables in tablegroup", K(ret), KPC(tablegroup));
      } else if (OB_FAIL(add_partition(tablegroup->get_tablegroup_id(), tables.count(), *tablegroup))) {
        LOG_WARN("fail to fill partition entity", K(ret), "tablegroup_id", tablegroup->get_tablegroup_id());
      } else {
        LOG_DEBUG("restore stat add tablegroup", K(ret), "tablegroup_id", tablegroup->get_tablegroup_id());
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreStat::fill_sys_partitions()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info_.tenant_id_;
  ObArray<const ObSimpleTableSchemaV2*> tables;
  ObArray<const ObTablegroupSchema*> tablegroups;
  partitions_.reset();
  replicas_.reset();
  statistic_.reset();
  if (!only_sys_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mode is not match", K(ret), K_(only_sys));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard_))) {
    LOG_WARN("fail to get tenant_schema_guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard_.get_inner_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("get tenant table schemas failed", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); ++i) {
      const ObSimpleTableSchemaV2* table = tables.at(i);
      int64_t table_cnt = 1;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stop", K(ret));
      } else if (OB_UNLIKELY(nullptr == table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (!table->has_self_partition()) {
        // bypass
      } else if (!is_inner_table(table->get_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not be user table", K(ret), "table_id", table->get_table_id());
      } else if (OB_FAIL(add_partition(table->get_table_id(), table_cnt, *table))) {
        LOG_WARN("fail to fill partition entity", K(ret), "table_id", table->get_table_id());
      } else {
        LOG_DEBUG("restore stat add table",
            K(ret),
            "table_id",
            table->get_table_id(),
            "partition_cnt",
            table->get_all_part_num());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (partitions_.count() <= 0 || replicas_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys table partition is empty",
          K(ret),
          "partition_cnt",
          partitions_.count(),
          "replica_cnt",
          replicas_.count());
    }
  }
  return ret;
}

int ObPhysicalRestoreStat::add_partition(const int64_t partition_cnt, const int64_t paxos_replica_cnt,
    const int64_t table_cnt, const share::ObPartitionInfo& info)
{
  int ret = OB_SUCCESS;
  PhysicalRestorePartition p;
  const ObPartitionReplica* leader_replica = NULL;
  const uint64_t schema_id = info.get_table_id();
  if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stop", K(ret));
  } else if (paxos_replica_cnt <= 0 || !info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    p.schema_id_ = schema_id;
    p.partition_id_ = info.get_partition_id();
    p.partition_cnt_ = partition_cnt;
    p.schema_paxos_cnt_ = paxos_replica_cnt;
    p.begin_ = replicas_.count();
    p.table_cnt_ = table_cnt;
    FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is null", K(ret), K(info));
      } else if (OB_FAIL(replicas_.push_back(*r))) {
        LOG_WARN("push replica to all_replica failed", K(ret), "replica", *r);
      } else {
        LOG_DEBUG("restore add replica", K(ret), "replica", *r);
      }
    }
    if (OB_SUCC(ret)) {
      p.end_ = replicas_.count();
      statistic_.total_pg_cnt_ += 1;
      statistic_.total_partition_cnt_ += table_cnt;
      if (OB_FAIL(partitions_.push_back(p))) {
        LOG_WARN("fail to push back partition", K(ret), "partition", p);
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreStat::check_stop()
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("restore scheduler stopped", K(ret));
  }
  return ret;
}
