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

#include "share/ob_pg_partition_meta_table_operator.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

void ObPGPartitionMTUpdateItem::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  svr_ip_.reset();
  role_ = 0;
  row_count_ = 0;
  data_size_ = 0;
  data_version_ = 0;
  required_size_ = 0;
  status_ = REPLICA_STATUS_NORMAL;
  replica_type_ = REPLICA_TYPE_FULL;
  data_checksum_ = 0;
}

bool ObPGPartitionMTUpdateItem::is_key_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != table_id_ && svr_ip_.is_valid();
}

bool ObPGPartitionMTUpdateItem::is_valid() const
{
  return is_key_valid() && row_count_ >= 0 && data_size_ >= 0 && data_version_ >= 0 and required_size_ >= 0;
}

int ObPGPartitionMTUpdateOperator::batch_update(
    const ObIArray<ObPGPartitionMTUpdateItem>& items, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items));
  } else {
    uint64_t tenant_id = OB_SYS_TENANT_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      const ObPGPartitionMTUpdateItem& item = items.at(i);
      dml.reuse();
      if (OB_FAIL(fill_one_item_(item, dml))) {
        LOG_WARN("fail to fill one item", K(ret), K(item));
      } else {
        if (0 == i) {
          if (OB_FAIL(dml.splice_column_names(columns))) {
            LOG_WARN("fail to splice column names", K(ret));
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                         OB_ALL_TENANT_PARTITION_META_TABLE_TNAME,
                         columns.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          } else {
            tenant_id = item.tenant_id_;
          }
        }

        if (OB_SUCC(ret)) {
          values.reset();
          if (OB_FAIL(dml.splice_values(values))) {
            LOG_WARN("fail to splice values", K(ret));
          } else if (OB_FAIL(sql.append_fmt("%s(%s)", 0 == i ? " " : " , ", values.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && items.count() > 0) {
      if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
        LOG_WARN("fail to append sql string", K(ret));
      } else if (OB_FAIL(sql.append(" role = values(role)")) ||
                 OB_FAIL(sql.append(", row_count = values(row_count)")) ||
                 OB_FAIL(sql.append(", data_size = values(data_size)")) ||
                 OB_FAIL(sql.append(", data_version = values(data_version)")) ||
                 OB_FAIL(sql.append(", required_size = values(required_size)")) ||
                 OB_FAIL(sql.append(", status = values(status)")) ||
                 OB_FAIL(sql.append(", replica_type = values(replica_type)")) ||
                 OB_FAIL(sql.append(", data_checksum = values(data_checksum)"))) {
        LOG_WARN("fail to append sql string", K(ret));
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_SUCC(ret)) {
        affected_rows = 0;
        if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret));
        } else if (OB_UNLIKELY(affected_rows > 2 * items.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(items.count()));
        } else {
          LOG_INFO("batch update partition meta table", K(sql));
        }
      }
    }
  }
  return ret;
}

int ObPGPartitionMTUpdateOperator::batch_delete(
    const ObIArray<ObPGPartitionMTUpdateItem>& items, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (0 == items.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (OB_FAIL(remove_one_item_(items.at(i), sql_proxy))) {
        LOG_WARN("fail to remove one item", K(ret), K(items.at(i)));
      }
    }
  }
  return ret;
}

int ObPGPartitionMTUpdateOperator::remove_one_item_(const ObPGPartitionMTUpdateItem& item, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE];
  ObDMLSqlSplicer dml;
  if (!item.is_key_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.svr_ip_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, fail to convert ip to string", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("table_id", item.table_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(dml.add_pk_column("svr_ip", ip)) ||
             OB_FAIL(dml.add_pk_column("svr_port", item.svr_ip_.get_port()))) {
    LOG_WARN("fail to add column", K(ret), K(item));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_proxy, item.tenant_id_);
    if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_PARTITION_META_TABLE_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret), K(item));
    }
  }
  return ret;
}

int ObPGPartitionMTUpdateOperator::fill_one_item_(const ObPGPartitionMTUpdateItem& item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!item.is_key_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.svr_ip_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("table_id", item.table_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(dml.add_pk_column("svr_ip", ip)) ||
             OB_FAIL(dml.add_pk_column("svr_port", item.svr_ip_.get_port())) ||
             OB_FAIL(dml.add_column("role", item.role_)) || OB_FAIL(dml.add_column("row_count", item.row_count_)) ||
             OB_FAIL(dml.add_column("data_size", item.data_size_)) ||
             OB_FAIL(dml.add_column("data_version", item.data_version_)) ||
             OB_FAIL(dml.add_column("required_size", item.required_size_)) ||
             OB_FAIL(dml.add_column("status", ob_replica_status_str(item.status_))) ||
             OB_FAIL(dml.add_column("replica_type", item.replica_type_)) ||
             OB_FAIL(dml.add_column("data_checksum", item.data_checksum_))) {
    LOG_WARN("fail to fill pg partition update info", K(ret));
  } else {
    // do nothing
  }

  return ret;
}
