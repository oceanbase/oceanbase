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

#include "ob_all_tenant_stat.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array_iterator.h"
#include "common/ob_role.h"
#include "common/ob_partition_key.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {

ObAllTenantStat::PartitionStat::PartitionStat()
{
  reset();
}

int ObAllTenantStat::PartitionStat::check_same_partition(
    const PartitionStat& lhs, const PartitionStat& rhs, bool& same_partition)
{
  int ret = OB_SUCCESS;
  same_partition = false;
  if (!lhs.is_valid() || !rhs.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(lhs), K(rhs), K(ret));
  } else {
    same_partition = (lhs.table_id_ == rhs.table_id_ && lhs.partition_id_ == rhs.partition_id_ &&
                      lhs.partition_cnt_ == rhs.partition_cnt_);
  }
  return ret;
}

int ObAllTenantStat::PartitionStat::cmp_version(const PartitionStat& lhs, const PartitionStat& rhs, int64_t& cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = -1;
  if (!lhs.is_valid() || !rhs.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(lhs), K(rhs), K(ret));
  } else if (lhs.major_version_ != rhs.major_version_) {
    if (lhs.major_version_ > rhs.major_version_) {
      cmp_ret = 1;
    } else {
      cmp_ret = -1;
    }
  } else {
    if (lhs.minor_version_ == rhs.minor_version_) {
      cmp_ret = 0;
    } else if (lhs.minor_version_ < rhs.minor_version_) {
      cmp_ret = 1;
    } else {
      cmp_ret = -1;
    }
  }

  return ret;
}

bool ObAllTenantStat::PartitionStat::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_INDEX != partition_id_ && occupy_size_ >= 0 && row_count_ >= 0 &&
         major_version_ >= 0 && minor_version_ >= 0;
}

void ObAllTenantStat::PartitionStat::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;
  occupy_size_ = 0;
  row_count_ = 0;
  major_version_ = 0;
  minor_version_ = 0;
}

bool ObAllTenantStat::ComparePartition::operator()(const PartitionStat& l, const PartitionStat& r)
{
  ObPartitionKey l_key(l.table_id_, l.partition_id_, l.partition_cnt_);
  ObPartitionKey r_key(r.table_id_, r.partition_id_, l.partition_cnt_);

  return l_key < r_key;
}

ObAllTenantStat::TenantStat::TenantStat()
{
  reset();
}

bool ObAllTenantStat::TenantStat::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && table_count_ >= 0 && row_count_ >= 0 && total_size_ >= 0;
}

void ObAllTenantStat::TenantStat::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_count_ = 0;
  row_count_ = 0;
  total_size_ = 0;
}

int ObAllTenantStat::TenantStat::add_partition_stat(const PartitionStat& partition_stat)
{
  int ret = OB_SUCCESS;
  if (!partition_stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition_stat", K(partition_stat), K(ret));
  } else {
    if (OB_INVALID_ID == tenant_id_) {
      tenant_id_ = extract_tenant_id(partition_stat.table_id_);
    }
    if (tenant_id_ != extract_tenant_id(partition_stat.table_id_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant_id not match",
          K_(tenant_id),
          "partition_stat tenant_id",
          extract_tenant_id(partition_stat.table_id_),
          K(ret));
    } else {
      row_count_ += partition_stat.row_count_;
      total_size_ += partition_stat.occupy_size_;
      if (0 == partition_stat.partition_id_) {
        ++table_count_;
      }
    }
  }
  return ret;
}

ObAllTenantStat::ObAllTenantStat() : inited_(false), schema_service_(NULL), proxy_(NULL)
{}

ObAllTenantStat::~ObAllTenantStat()
{}

int ObAllTenantStat::init(ObMultiVersionSchemaService& schema_service, ObMySQLProxy& proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    proxy_ = &proxy;
    inited_ = true;
  }

  return ret;
}

int ObAllTenantStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (!start_to_read_) {
    ObArray<TenantStat> tenant_stats;
    const ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_STAT_TID);
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(ret));
    } else if (OB_FAIL(get_all_tenant_stats(tenant_stats))) {
      LOG_WARN("get_all_tenant_stats failed", K(ret));
    } else {
      ObArray<Column> columns;
      FOREACH_CNT_X(tenant_stat, tenant_stats, OB_SUCCESS == ret)
      {
        columns.reuse();
        if (OB_FAIL(get_full_row(table_schema, *tenant_stat, columns))) {
          LOG_WARN("fail to get full row", "table_schema", *table_schema, K(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("fail to project row", K(ret));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("fail to add row", K(cur_row_), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllTenantStat::get_all_partition_stats(ObIArray<PartitionStat>& partition_stats)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(sql.append_fmt("SELECT table_id, partition_id, partition_cnt, "
                                      "occupy_size, row_count, major_version, minor_version "
                                      "FROM %s WHERE role = '%d' ",
                   OB_ALL_VIRTUAL_STORAGE_STAT_TNAME,
                   LEADER))) {
      LOG_WARN("assign sql string failed", K(ret));
    } else if (OB_FAIL(proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else if (OB_FAIL(retrieve_partition_stats(*result, partition_stats))) {
      LOG_WARN("retrieve partition stat failed", K(ret));
    }
  }
  return ret;
}

int ObAllTenantStat::get_all_tenant_stats(ObIArray<TenantStat>& tenant_stats)
{
  int ret = OB_SUCCESS;
  TenantStat tenant_stat;
  ObArray<PartitionStat> all_partition_stats;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_all_partition_stats(all_partition_stats))) {
    LOG_WARN("get all partition stats failed", K(ret));
  } else if (all_partition_stats.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no partition exist", "partition_stat_count", all_partition_stats.count(), K(ret));
  } else {
    ComparePartition compare;
    std::sort(all_partition_stats.begin(), all_partition_stats.end(), compare);
    PartitionStat max = all_partition_stats.at(0);
    tenant_stat.tenant_id_ = extract_tenant_id(max.table_id_);
    for (int64_t i = 1; OB_SUCC(ret) && i < all_partition_stats.count(); ++i) {
      const PartitionStat& tmp = all_partition_stats.at(i);
      bool same_partition = false;
      int64_t cmp_ret = -1;
      if (OB_FAIL(PartitionStat::check_same_partition(max, tmp, same_partition))) {
        LOG_WARN("check_same_partition failed", K(max), K(tmp), K(ret));
      } else if (same_partition) {
        if (OB_FAIL(PartitionStat::cmp_version(tmp, max, cmp_ret))) {
          LOG_WARN("cmp_version failed", K(tmp), K(max), K(ret));
        } else if (cmp_ret > 0) {
          max = tmp;
        } else {
        }
      } else {
        if (tenant_stat.tenant_id_ != extract_tenant_id(max.table_id_)) {
          if (OB_FAIL(tenant_stats.push_back(tenant_stat))) {
            LOG_WARN("push back tenant stat failed", K(ret));
          } else {
            tenant_stat.reset();
            tenant_stat.tenant_id_ = extract_tenant_id(max.table_id_);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(tenant_stat.add_partition_stat(max))) {
            LOG_WARN("add partition stat failed", K(ret));
          } else {
            max = tmp;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (tenant_stat.tenant_id_ != extract_tenant_id(max.table_id_)) {
        if (OB_FAIL(tenant_stats.push_back(tenant_stat))) {
          LOG_WARN("push back tenant stat failed", K(ret));
        } else {
          tenant_stat.reset();
          tenant_stat.tenant_id_ = extract_tenant_id(max.table_id_);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tenant_stat.add_partition_stat(max))) {
          LOG_WARN("add partition stat failed", K(ret));
        } else if (tenant_stats.push_back(tenant_stat)) {
          LOG_WARN("add tenant stat failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllTenantStat::retrieve_partition_stats(
    sqlclient::ObMySQLResult& result, ObIArray<PartitionStat>& partition_stats)
{
  int ret = OB_SUCCESS;
  PartitionStat partition_stat;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
      } else if (OB_FAIL(fill_partition_stat(result, partition_stat))) {
        LOG_WARN("fill partition_stat failed", K(ret));
      } else if (OB_FAIL(partition_stats.push_back(partition_stat))) {
        LOG_WARN("push back partition stat failed", K(ret));
      }
    }
  }

  return ret;
}

int ObAllTenantStat::fill_partition_stat(const sqlclient::ObMySQLResult& result, PartitionStat& partition_stat)
{
  int ret = OB_SUCCESS;
  partition_stat.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_id, partition_stat, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, partition_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_cnt, partition_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, occupy_size, partition_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, row_count, partition_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, major_version, partition_stat, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, minor_version, partition_stat, int64_t);
  }

  return ret;
}

int ObAllTenantStat::get_full_row(
    const share::schema::ObTableSchema* table, const TenantStat& tenant_stat, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (!tenant_stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_stat", K(tenant_stat), K(ret));
  } else {
    ADD_COLUMN(set_int, table, "tenant_id", tenant_stat.tenant_id_, columns);
    ADD_COLUMN(set_int, table, "table_count", tenant_stat.table_count_, columns);
    ADD_COLUMN(set_int, table, "row_count", tenant_stat.row_count_, columns);
    ADD_COLUMN(set_int, table, "total_size", tenant_stat.total_size_, columns);
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
