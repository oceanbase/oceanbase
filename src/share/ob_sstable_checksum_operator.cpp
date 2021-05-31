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

#include "ob_sstable_checksum_operator.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_column_schema.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/container/ob_array_iterator.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

ObSSTableDataChecksumItem::ObSSTableDataChecksumItem()
    : tenant_id_(OB_INVALID_ID),
      data_table_id_(OB_INVALID_ID),
      sstable_id_(OB_INVALID_ID),
      partition_id_(-1),
      sstable_type_(-1),
      server_(),
      row_checksum_(0),
      data_checksum_(0),
      row_count_(0),
      snapshot_version_(0),
      replica_type_(-1)
{}

void ObSSTableDataChecksumItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  data_table_id_ = OB_INVALID_ID;
  sstable_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  sstable_type_ = -1;
  server_.reset();
  row_checksum_ = 0;
  data_checksum_ = 0;
  row_count_ = 0;
  snapshot_version_ = 0;
  replica_type_ = -1;
}

bool ObSSTableDataChecksumItem::is_key_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != data_table_id_ && OB_INVALID_ID != sstable_id_ &&
         partition_id_ >= 0 && sstable_type_ >= 0 && server_.is_valid();
}

bool ObSSTableDataChecksumItem::is_valid() const
{
  return is_key_valid() && row_count_ >= 0 && snapshot_version_ >= 0;
}

bool ObSSTableDataChecksumItem::is_same_table(const ObSSTableDataChecksumItem& other) const
{
  return tenant_id_ == other.tenant_id_ && data_table_id_ == other.data_table_id_ && sstable_id_ == other.sstable_id_ &&
         partition_id_ == other.partition_id_ && sstable_type_ == other.sstable_type_;
}

int ObSSTableDataChecksumOperator::get_checksum(ObSSTableDataChecksumItem& item, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  ObArray<ObSSTableDataChecksumItem> items;
  if (OB_UNLIKELY(!item.is_key_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid ip", K(ret), K(item.server_));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s "
                 "WHERE tenant_id = %ld AND data_table_id = %ld AND sstable_id = %ld AND partition_id = %ld "
                 "AND sstable_type = %d AND svr_ip = '%s' AND svr_port = %d",
                 OB_ALL_SSTABLE_CHECKSUM_TNAME,
                 item.tenant_id_,
                 item.data_table_id_,
                 item.sstable_id_,
                 item.partition_id_,
                 item.sstable_type_,
                 ip,
                 item.server_.get_port()))) {
    LOG_WARN("fail to assign sql", K(ret), K(item));
  } else if (OB_FAIL(get_checksum(sql, items, sql_proxy))) {
    LOG_WARN("fail to get checksum", K(ret));
  } else if (1 != items.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, item count is invalid", K(ret), K(items.count()));
  } else {
    item = items.at(0);
  }
  return ret;
}

int ObSSTableDataChecksumOperator::get_checksum(const uint64_t data_table_id, const uint64_t sstable_id,
    const int64_t partition_id, const int sstable_type, common::ObIArray<ObSSTableDataChecksumItem>& items,
    ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == data_table_id || OB_INVALID_ID == sstable_id || OB_INVALID_ID == partition_id ||
                  -1 == sstable_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_id), K(sstable_id), K(partition_id), K(sstable_type));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s "
                 "WHERE tenant_id = %ld AND data_table_id = %ld AND sstable_id = %ld AND partition_id = %ld "
                 "AND sstable_type = %d",
                 OB_ALL_SSTABLE_CHECKSUM_TNAME,
                 extract_tenant_id(data_table_id),
                 data_table_id,
                 sstable_id,
                 partition_id,
                 sstable_type))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(get_checksum(sql, items, sql_proxy))) {
    LOG_WARN("fail to get checksum", K(ret), K(sql));
  }
  return ret;
}

int ObSSTableDataChecksumOperator::need_verify_checksum(const ObTableSchema& table_schema,
    const int64_t global_snapshot_version, int64_t& snapshot_version, bool& need_verify, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObArray<ObSSTableDataChecksumItem> items;
  need_verify = false;
  const uint64_t data_table_id =
      table_schema.is_global_index_table() ? table_schema.get_data_table_id() : table_schema.get_table_id();
  const uint64_t sstable_id = table_schema.get_table_id();
  if (OB_UNLIKELY(OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * from %s "
                                    "WHERE tenant_id = %ld AND data_table_id = %ld AND sstable_id = %ld AND "
                                    "replica_type = 0 AND sstable_type = 1 order by snapshot_version",
                 OB_ALL_SSTABLE_CHECKSUM_TNAME,
                 extract_tenant_id(data_table_id),
                 data_table_id,
                 sstable_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(get_checksum(sql, items, sql_proxy))) {
    LOG_WARN("fail to get checksum", K(ret));
  } else if (0 == items.count()) {
    ret = OB_EAGAIN;
  } else {
    const int64_t item_cnt = items.count();
    const int64_t first_snapshot_version = items.at(0).snapshot_version_;
    const int64_t last_snapshot_version = items.at(item_cnt - 1).snapshot_version_;
    if (first_snapshot_version > global_snapshot_version) {
      need_verify = false;
    } else if (first_snapshot_version == global_snapshot_version) {
      if (last_snapshot_version > global_snapshot_version) {
        need_verify = false;
      } else {
        need_verify = true;
        snapshot_version = last_snapshot_version;
      }
    } else {
      if (last_snapshot_version > global_snapshot_version) {
        need_verify = false;
      } else {
        // when each partition has at lease one replica which finished compaction with version global_snapshot_version,
        // we will validate checksum
        bool check_dropped_schema = true;
        ObTablePartitionKeyIter part_iter(table_schema, check_dropped_schema);
        const int64_t part_num = part_iter.get_partition_num();
        ObPartitionKey pkey;
        ObArray<int64_t> partition_ids;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
          if (OB_FAIL(part_iter.next_partition_key_v2(pkey))) {
            STORAGE_LOG(WARN, "fail to get next partition key", K(ret));
          } else if (OB_FAIL(partition_ids.push_back(pkey.get_partition_id()))) {
            STORAGE_LOG(WARN, "fail to push back partition key", K(ret));
          }
        }
        ObHashMap<int64_t, bool> reported_partition_ids;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(reported_partition_ids.create(part_num, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
            LOG_WARN("fail to create reported partition id map", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
          if (items.at(i).snapshot_version_ == global_snapshot_version) {
            if (OB_FAIL(reported_partition_ids.set_refactored(items.at(i).partition_id_, true, true /*overwrite*/))) {
              STORAGE_LOG(WARN, "fail to set to hashmap", K(ret), K(items.at(i)));
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
          bool value = false;
          if (OB_FAIL(reported_partition_ids.get_refactored(partition_ids.at(i), value))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_EAGAIN;
              need_verify = false;
              if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
                LOG_WARN("partition has not reported",
                    K(ret),
                    K(table_schema),
                    K(partition_ids),
                    K(reported_partition_ids.size()),
                    K(partition_ids.at(i)));
              }
            } else {
              LOG_WARN("fail to check partition id exist", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          need_verify = true;
        }

        if (reported_partition_ids.created()) {
          reported_partition_ids.destroy();
        }
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::get_major_checksums(const uint64_t start_tenant_id,
    const uint64_t start_data_table_id, const uint64_t start_sstable_id, const int64_t start_partition_id,
    const int64_t batch_cnt, common::ObIArray<ObSSTableDataChecksumItem>& items, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == start_data_table_id || OB_INVALID_ID == start_sstable_id ||
                  OB_INVALID_ID == start_partition_id || OB_INVALID_ID == start_tenant_id || batch_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
        K(ret),
        K(start_tenant_id),
        K(start_data_table_id),
        K(start_sstable_id),
        K(start_partition_id),
        K(batch_cnt));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s "
                                    "WHERE (tenant_id, data_table_id, sstable_id, partition_id) > (%ld, %ld, %ld, %ld) "
                                    "AND sstable_type = 1 ORDER BY tenant_id, data_table_id, sstable_id, partition_id, "
                                    "svr_ip, svr_port limit %ld",
                 OB_ALL_SSTABLE_CHECKSUM_TNAME,
                 start_tenant_id,
                 start_data_table_id,
                 start_sstable_id,
                 start_partition_id,
                 batch_cnt))) {
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_checksum(sql, items, sql_proxy))) {
      LOG_WARN("fail to get checksum", K(ret), K(sql));
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::get_replicas(const uint64_t data_table_id, const uint64_t sstable_id,
    const int64_t partition_id, const int sstable_type, common::ObIArray<ObAddr>& replicas, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTableDataChecksumItem> items;
  if (OB_UNLIKELY(
          OB_INVALID_ID == data_table_id || OB_INVALID_ID == sstable_id || partition_id < 0 || -1 == sstable_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_id), K(sstable_id), K(partition_id), K(sstable_type));
  } else if (OB_FAIL(get_checksum(data_table_id, sstable_id, partition_id, sstable_type, items, sql_proxy))) {
    LOG_WARN("fail to get checksum", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (OB_FAIL(replicas.push_back(items.at(i).server_))) {
        LOG_WARN("fail to push back replica", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::get_checksum(
    const ObSqlString& sql, common::ObIArray<ObSSTableDataChecksumItem>& items, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
    int port = 0;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObSSTableDataChecksumItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          int64_t tmp_real_str_len = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", item.tenant_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "data_table_id", item.data_table_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "sstable_id", item.sstable_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", item.partition_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "sstable_type", item.sstable_type_, int);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
          if (!item.server_.set_ip_addr(ip, port)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, fail to set server addr", K(ret), K(ip), K(port));
          }
          UNUSED(tmp_real_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, "row_checksum", item.row_checksum_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "data_checksum", item.data_checksum_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "row_count", item.row_count_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", item.snapshot_version_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "replica_type", item.replica_type_, int);
          if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("fail to push back item", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::check_table_checksum(
    const uint64_t data_table_id, const uint64_t index_id, common::ObISQLClient& sql_proxy, bool& is_checksum_valid)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  ObArray<ObSSTableDataChecksumItem> checksum_items;
  const int major_sstable_type = 1L;
  is_checksum_valid = true;
  if (OB_UNLIKELY(OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_id), K(index_id));
  } else if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s "
                                           "WHERE tenant_id = %ld AND data_table_id = %ld AND sstable_id = %ld "
                                           "AND sstable_type = %d order by partition_id, snapshot_version desc",
                 OB_ALL_SSTABLE_CHECKSUM_TNAME,
                 extract_tenant_id(data_table_id),
                 data_table_id,
                 index_id,
                 major_sstable_type))) {
    LOG_WARN("fail to assign sql string", K(ret));
  } else if (OB_FAIL(get_checksum(sql_string, checksum_items, sql_proxy))) {
    LOG_WARN("fail to get checksum", K(ret));
  } else {
    const ObSSTableDataChecksumItem* check_item = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_checksum_valid && i < checksum_items.count(); ++i) {
      const ObSSTableDataChecksumItem& item = checksum_items.at(i);
      if (NULL == check_item || check_item->partition_id_ != item.partition_id_) {
        check_item = &item;
      } else {
        if (item.snapshot_version_ == check_item->snapshot_version_) {
          is_checksum_valid =
              check_item->data_checksum_ == item.data_checksum_ && check_item->row_count_ == item.row_count_;
          if (!is_checksum_valid) {
            LOG_ERROR("table data checksum is not equal", K(index_id), "left_item", *check_item, "right_item", item);
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::batch_report_checksum(
    const ObIArray<ObSSTableDataChecksumItem>& items, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObMySQLTransaction trans;
  ObDMLSqlSplicer dml;
  const int64_t BATCH_CNT = 500;
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else {
    int64_t report_idx = 0;
    while (OB_SUCC(ret) && report_idx < items.count()) {
      sql.reuse();
      columns.reuse();
      const int64_t remain_cnt = items.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        const ObSSTableDataChecksumItem& item = items.at(report_idx + i);
        dml.reuse();
        if (OB_FAIL(fill_one_item(item, dml))) {
          LOG_WARN("fail to fill one item", K(ret), K(item));
        } else {
          if (0 == i) {
            if (OB_FAIL(dml.splice_column_names(columns))) {
              LOG_WARN("fail to splice column names", K(ret));
            } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                           OB_ALL_SSTABLE_CHECKSUM_TNAME,
                           columns.ptr()))) {
              LOG_WARN("fail to assign sql string", K(ret));
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

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
          LOG_WARN("fail to append sql string", K(ret));
        } else if (OB_FAIL(sql.append(" row_checksum = values(row_checksum)")) ||
                   OB_FAIL(sql.append(", data_checksum = values(data_checksum)")) ||
                   OB_FAIL(sql.append(", row_count = values(row_count)")) ||
                   OB_FAIL(sql.append(", snapshot_version = values(snapshot_version)")) ||
                   OB_FAIL(sql.append(", replica_type = values(replica_type)"))) {
          LOG_WARN("fail to append sql string", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret));
        } else if (OB_UNLIKELY(affected_rows > 2 * cur_batch_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(cur_batch_cnt));
        } else {
          report_idx += cur_batch_cnt;
          LOG_INFO("batch report checksum", K(sql));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*commit*/))) {
        LOG_WARN("failed to abort trans", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::batch_remove_checksum(
    const ObIArray<ObSSTableDataChecksumItem>& items, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (0 == items.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (OB_FAIL(remove_one_item(items.at(i), sql_proxy))) {
        LOG_WARN("fail to remove one item", K(ret), K(items.at(i)));
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::remove_one_item(const ObSSTableDataChecksumItem& item, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE];
  ObDMLSqlSplicer dml;
  if (OB_INVALID_ID == item.tenant_id_ || OB_INVALID_ID == item.data_table_id_ || item.partition_id_ < 0 ||
      !item.server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, fail to convert ip to string", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("data_table_id", item.data_table_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(dml.add_pk_column("svr_ip", ip)) ||
             OB_FAIL(dml.add_pk_column("svr_port", item.server_.get_port()))) {
    LOG_WARN("fail to add column", K(ret));
  } else {
    ObDMLExecHelper exec(sql_proxy, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_SSTABLE_CHECKSUM_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    }
  }
  return ret;
}

int ObSSTableDataChecksumOperator::fill_one_item(const ObSSTableDataChecksumItem& item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("data_table_id", item.data_table_id_)) ||
             OB_FAIL(dml.add_pk_column("sstable_id", item.sstable_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(dml.add_pk_column("sstable_type", item.sstable_type_)) ||
             OB_FAIL(dml.add_pk_column("svr_ip", ip)) ||
             OB_FAIL(dml.add_pk_column("svr_port", item.server_.get_port())) ||
             OB_FAIL(dml.add_column("row_checksum", item.row_checksum_)) ||
             OB_FAIL(dml.add_column("data_checksum", item.data_checksum_)) ||
             OB_FAIL(dml.add_column("row_count", item.row_count_)) ||
             OB_FAIL(dml.add_column("snapshot_version", item.snapshot_version_)) ||
             OB_FAIL(dml.add_column("replica_type", item.replica_type_))) {
    LOG_WARN("fail to fill checksum info", K(ret));
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::batch_report_checksum(
    const ObIArray<ObSSTableColumnChecksumItem>& items, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  ObMySQLTransaction trans;
  const int64_t BATCH_CNT = 500;
  if (OB_UNLIKELY(items.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    int64_t report_idx = 0;
    const int64_t tenant_id = items.at(0).tenant_id_;
    const int64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    while (OB_SUCC(ret) && report_idx < items.count()) {
      sql.reuse();
      columns.reuse();
      const int64_t remain_cnt = items.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        const ObSSTableColumnChecksumItem& item = items.at(report_idx + i);
        dml.reuse();
        if (OB_FAIL(fill_one_item(item, dml))) {
          LOG_WARN("fail to fill one item", K(ret), K(item));
        } else {
          if (0 == i) {
            if (OB_FAIL(dml.splice_column_names(columns))) {
              LOG_WARN("fail to splice column names", K(ret));
            } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                           OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TNAME,
                           columns.ptr()))) {
              LOG_WARN("fail to assign sql string", K(ret));
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

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
          LOG_WARN("fail to append sql string", K(ret));
        } else if (OB_FAIL(sql.append(" column_checksum = values(column_checksum)")) ||
                   OB_FAIL(sql.append(", snapshot_version = values(snapshot_version)")) ||
                   OB_FAIL(sql.append(", checksum_method = values(checksum_method)")) ||
                   OB_FAIL(sql.append(", replica_type = values(replica_type)")) ||
                   OB_FAIL(sql.append(", major_version = values(major_version)"))) {
          LOG_WARN("fail to append sql string", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql_proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          if (OB_SCHEMA_EAGAIN == ret) {
            LOG_INFO("fail to execute sql, try again", K(ret), K(tenant_id));
          } else {
            LOG_WARN("fail to execute sql", K(ret));
          }
        } else if (OB_UNLIKELY(affected_rows > 2 * cur_batch_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(cur_batch_cnt));
        } else {
          report_idx += cur_batch_cnt;
          LOG_INFO("batch report column checksum", K(sql));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*commit*/))) {
        LOG_WARN("failed to abort trans", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::fill_one_item(const ObSSTableColumnChecksumItem& item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(item.tenant_id_);
    if (OB_FAIL(
            dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, item.tenant_id_))) ||
        OB_FAIL(dml.add_pk_column(
            "data_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, item.data_table_id_))) ||
        OB_FAIL(dml.add_pk_column("index_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, item.index_id_))) ||
        OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
        OB_FAIL(dml.add_pk_column("sstable_type", item.sstable_type_)) ||
        OB_FAIL(dml.add_pk_column("column_id", item.column_id_)) || OB_FAIL(dml.add_pk_column("svr_ip", ip)) ||
        OB_FAIL(dml.add_pk_column("svr_port", item.server_.get_port())) ||
        OB_FAIL(dml.add_column("column_checksum", item.column_checksum_)) ||
        OB_FAIL(dml.add_column("checksum_method", item.checksum_method_)) ||
        OB_FAIL(dml.add_column("snapshot_version", item.snapshot_version_)) ||
        OB_FAIL(dml.add_column("replica_type", item.replica_type_)) ||
        OB_FAIL(dml.add_column("major_version", item.major_version_))) {
      LOG_WARN("fail to fill column checksum info", K(ret));
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::batch_remove_checksum(
    const ObIArray<ObSSTableColumnChecksumItem>& items, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (0 == items.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(items.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (OB_FAIL(remove_one_item(items.at(i), sql_proxy))) {
        LOG_WARN("fail to remove one item", K(ret), K(items.at(i)));
      }
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::remove_one_item(const ObSSTableColumnChecksumItem& item, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE];
  if (OB_INVALID_ID == item.tenant_id_ || OB_INVALID_ID == item.data_table_id_ || item.partition_id_ < 0 ||
      !item.server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item));
  } else if (!item.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, fail to convert ip to string", K(ret));
  } else {
    const uint64_t tenant_id = item.tenant_id_;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id=%ld AND data_table_id=%ld AND partition_id=%ld AND "
                               "svr_ip = '%s' AND svr_port=%d",
            OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, item.data_table_id_),
            item.partition_id_,
            ip,
            item.server_.get_port()))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret));
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::get_checksum(const uint64_t tenant_id, const ObSqlString& sql,
    common::ObIArray<ObSSTableColumnChecksumItem>& items, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
    int port = 0;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObSSTableColumnChecksumItem item;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          int64_t tmp_real_str_len = 0;
          item.tenant_id_ = tenant_id;
          EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(*result, "data_table_id", item.data_table_id_, tenant_id);
          EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(*result, "index_id", item.index_id_, tenant_id);
          EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", item.partition_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "sstable_type", item.sstable_type_, int);
          EXTRACT_INT_FIELD_MYSQL(*result, "column_id", item.column_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
          if (OB_SUCC(ret) && !item.server_.set_ip_addr(ip, port)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, fail to set server addr", K(ret), K(ip), K(port));
          }
          UNUSED(tmp_real_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, "column_checksum", item.column_checksum_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "checksum_method", item.checksum_method_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", item.snapshot_version_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "replica_type", item.replica_type_, int);
          EXTRACT_INT_FIELD_MYSQL(*result, "major_version", item.major_version_, int);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(items.push_back(item))) {
              LOG_WARN("fail to push back item", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::get_checksum_method(const uint64_t data_table_id, const uint64_t index_id,
    const int64_t major_version, int64_t& checksum_method, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObSSTableColumnChecksumItem> items;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_id || major_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_id), K(index_id), K(major_version));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s "
                 "WHERE tenant_id = %ld AND data_table_id = %ld AND index_id = %ld AND major_version = %ld "
                 "AND sstable_type = 1",
                 OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TNAME,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, data_table_id),
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, index_id),
                 major_version))) {
    LOG_WARN("fail to generate sql", K(ret));
  } else if (OB_FAIL(get_checksum(extract_tenant_id(data_table_id), sql, items, sql_proxy))) {
    LOG_WARN("fail to get checksum", K(ret));
  } else if (items.count() < 1) {
    ret = OB_EAGAIN;
    LOG_WARN("data table sstable column checksum method is not available", K(ret));
  } else {
    const int64_t first_checksum_method = items.at(0).checksum_method_;
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (items.at(i).checksum_method_ != first_checksum_method) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, checksum method is not equal", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      checksum_method = first_checksum_method;
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::get_table_column_checksum(const share::schema::ObTableSchema& data_table_schema,
    const share::schema::ObTableSchema& index_table_schema, const int64_t global_snapshot_version,
    common::ObMySQLProxy& sql_proxy, int64_t& snapshot_version, ObHashMap<int64_t, int64_t>& column_checksum_map)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    hash::ObHashMap<int64_t, bool> reported_partition_ids;
    snapshot_version = 0;
    if (OB_UNLIKELY(!data_table_schema.is_valid() || !index_table_schema.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(data_table_schema), K(index_table_schema));
    } else {
      bool check_dropped_schema = false;
      ObTablePartitionKeyIter part_iter(index_table_schema, check_dropped_schema);
      const int64_t part_num = part_iter.get_partition_num();
      ObPartitionKey pkey;
      ObArray<int64_t> partition_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_FAIL(part_iter.next_partition_key_v2(pkey))) {
          STORAGE_LOG(WARN, "fail to get next partition key", K(ret));
        } else if (OB_FAIL(partition_ids.push_back(pkey.get_partition_id()))) {
          STORAGE_LOG(WARN, "fail to push back partition key", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(reported_partition_ids.create(part_num, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
          LOG_WARN("fail to create reported partition id map", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        const int64_t data_table_id = data_table_schema.get_table_id();
        std::sort(partition_ids.begin(), partition_ids.end());
        const uint64_t tenant_id = extract_tenant_id(data_table_schema.get_table_id());
        const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
        if (OB_FAIL(sql.assign_fmt("SELECT column_id, partition_id, column_checksum, snapshot_version FROM %s "
                                   "WHERE tenant_id='%ld' AND data_table_id='%ld' AND index_id='%ld' order by "
                                   "column_id, partition_id, snapshot_version",
                OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TNAME,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, data_table_schema.get_table_id()),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, index_table_schema.get_table_id())))) {
          LOG_WARN("fail to append sql string", K(ret));
        } else if (OB_FAIL(sql_proxy.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, query result must not be NULL", K(ret));
        } else {
          int64_t last_column_id = -1;
          int64_t last_partition_id = -1;
          int64_t last_column_checksum = -1;
          int64_t last_snapshot_version = -1;
          int64_t curr_column_id_checksum_sum = 0;
          int64_t curr_column_id = 0;
          int64_t curr_partition_id = 0;
          int64_t curr_column_checksum = 0;
          int64_t curr_snapshot_version = 0;
          int64_t tmp_partition_id = 0;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                if (curr_column_id != 0) {
                  if (OB_FAIL(reported_partition_ids.set_refactored(
                          curr_partition_id, true /*value*/, true /*overwrite*/))) {
                    LOG_WARN("fail to push back partition id", K(ret));
                  } else if (OB_FAIL(column_checksum_map.set_refactored(
                                 curr_column_id, curr_column_id_checksum_sum, true /*overwrite*/))) {
                    LOG_WARN("fail to set column checksum to map", K(ret));
                  }
                } else {
                  ret = OB_SUCCESS;
                }
                break;
              }
            } else {
              EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", tmp_partition_id, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", curr_snapshot_version, int64_t);
              LOG_DEBUG("get next row before", K(data_table_id), K(tmp_partition_id));
              if (has_exist_in_array(partition_ids, tmp_partition_id) &&
                  curr_snapshot_version == global_snapshot_version) {
                curr_partition_id = tmp_partition_id;
                EXTRACT_INT_FIELD_MYSQL(*result, "column_id", curr_column_id, int64_t);
                EXTRACT_INT_FIELD_MYSQL(*result, "column_checksum", curr_column_checksum, int64_t);
                if (-1 == last_column_id) {
                  last_partition_id = curr_partition_id;
                  last_column_id = curr_column_id;
                  last_column_checksum = curr_column_checksum;
                  last_snapshot_version = curr_snapshot_version;
                  curr_column_id_checksum_sum = curr_column_checksum;
                }
                LOG_DEBUG("get next row",
                    K(data_table_id),
                    K(curr_partition_id),
                    K(curr_column_id),
                    K(curr_column_checksum),
                    K(curr_snapshot_version),
                    K(last_partition_id),
                    K(last_column_id),
                    K(last_column_checksum),
                    K(last_snapshot_version));
                if (OB_FAIL(ret)) {
                } else if (last_column_id == curr_column_id) {
                  if (last_partition_id == curr_partition_id) {
                    // need checksum column checksum
                    snapshot_version = curr_snapshot_version;
                    if (last_snapshot_version == curr_snapshot_version &&
                        last_column_checksum != curr_column_checksum) {
                      ret = OB_CHECKSUM_ERROR;
                      LOG_ERROR("column checksum is not equal between replicas",
                          K(ret),
                          K(index_table_schema),
                          K(curr_partition_id),
                          K(curr_column_id),
                          K(curr_column_checksum),
                          K(last_column_checksum));
                    }
                  } else {
                    if (OB_FAIL(reported_partition_ids.set_refactored(
                            last_partition_id, true /*value*/, true /*overwrite*/))) {
                      LOG_WARN("fail to push back partition id", K(ret));
                    } else {
                      last_partition_id = curr_partition_id;
                      last_column_checksum = curr_column_checksum;
                      last_snapshot_version = curr_snapshot_version;
                      curr_column_id_checksum_sum += curr_column_checksum;
                    }
                  }
                } else {
                  if (OB_FAIL(column_checksum_map.set_refactored(last_column_id, curr_column_id_checksum_sum))) {
                    LOG_WARN("fail to set column checksum to map", K(ret));
                  } else if (OB_FAIL(reported_partition_ids.set_refactored(
                                 last_partition_id, true /*value*/, true /*overwrite*/))) {
                    LOG_WARN("fail to push back partition id", K(ret));
                  } else {
                    last_column_id = curr_column_id;
                    last_partition_id = curr_partition_id;
                    last_column_checksum = curr_column_checksum;
                    last_snapshot_version = curr_snapshot_version;
                    curr_column_id_checksum_sum = curr_column_checksum;
                  }
                }
              }
            }
          }
        }
      }
      if (partition_ids.count() != reported_partition_ids.size()) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_WARN("fail to check column checksum",
              K(ret),
              K(index_table_schema),
              K(partition_ids),
              K(reported_partition_ids.size()));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
          bool value = false;
          if (OB_FAIL(reported_partition_ids.get_refactored(partition_ids.at(i), value))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_EAGAIN;
              if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
                LOG_WARN("fail to check column checksum",
                    K(ret),
                    K(index_table_schema),
                    K(partition_ids),
                    K(reported_partition_ids.size()));
              }
            } else {
              LOG_WARN("fail to check partition id exist", K(ret));
            }
          }
        }
      }
    }
    if (reported_partition_ids.created()) {
      reported_partition_ids.destroy();
    }
  }
  return ret;
}

int ObSSTableColumnChecksumOperator::check_column_checksum(const share::schema::ObTableSchema& data_table_schema,
    const share::schema::ObTableSchema& index_table_schema, const int64_t global_snapshot_version,
    ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, int64_t> data_table_column_checksums;
  int64_t data_table_snapshot_version = 0L;
  int64_t index_table_snapshot_version = 0L;
  bool verify_checksum = false;
  hash::ObHashMap<int64_t, int64_t> index_table_column_checksums;
  if (OB_UNLIKELY(!data_table_schema.is_valid() || !index_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_schema), K(index_table_schema));
  } else if (OB_FAIL(data_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(
                 index_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(ObSSTableDataChecksumOperator::need_verify_checksum(index_table_schema,
                 global_snapshot_version,
                 index_table_snapshot_version,
                 verify_checksum,
                 sql_proxy))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need verify checksum", K(ret));
    }
  } else if (!verify_checksum) {
    LOG_INFO("do not need verify checksum", K(index_table_schema.get_table_id()));
  } else if (OB_FAIL(get_table_column_checksum(data_table_schema,
                 index_table_schema,
                 global_snapshot_version,
                 sql_proxy,
                 index_table_snapshot_version,
                 index_table_column_checksums))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to get table column checksum", K(ret));
    } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("fail to get table column checksums", K(ret), K(index_table_schema));
    }
  } else if (OB_FAIL(ObSSTableDataChecksumOperator::need_verify_checksum(data_table_schema,
                 global_snapshot_version,
                 data_table_snapshot_version,
                 verify_checksum,
                 sql_proxy))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to get major snapshot version", K(ret));
    }
  } else if (!verify_checksum) {
    LOG_INFO("do not need verify checksum", K(index_table_schema.get_table_id()));
  } else if (data_table_snapshot_version != index_table_snapshot_version) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("snapshot version is not equal, skip check",
          "data_table_id",
          data_table_schema.get_table_id(),
          "index_table_id",
          index_table_schema.get_table_id());
    }
  } else if (OB_FAIL(get_table_column_checksum(data_table_schema,
                 data_table_schema,
                 global_snapshot_version,
                 sql_proxy,
                 data_table_snapshot_version,
                 data_table_column_checksums))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to get data table column checksums", K(ret));
    } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("fail to get table column checksums", K(ret), K(index_table_schema));
    }
  } else if (data_table_snapshot_version != index_table_snapshot_version) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("snapshot version is not equal, skip check",
          "data_table_id",
          data_table_schema.get_table_id(),
          "index_table_id",
          index_table_schema.get_table_id(),
          K(ret));
    }
  } else {
    for (hash::ObHashMap<int64_t, int64_t>::const_iterator iter = index_table_column_checksums.begin();
         OB_SUCC(ret) && iter != index_table_column_checksums.end();
         ++iter) {
      int64_t data_table_column_checksum = 0;
      // is_virtual_generated_column is only tag in data table
      const ObColumnSchemaV2* column_schema = data_table_schema.get_column_schema(iter->first);
      if (NULL == column_schema) {
        column_schema = index_table_schema.get_column_schema(iter->first);
      }
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column schema must not be NULL", K(ret));
      } else if (column_schema->is_shadow_column() || !column_schema->is_column_stored_in_sstable()) {
        STORAGE_LOG(INFO, "column do not need to compare checksum", K(iter->first));
      } else if (OB_FAIL(data_table_column_checksums.get_refactored(iter->first, data_table_column_checksum))) {
        LOG_WARN("fail to get data table column checksum", K(ret), "column_id", iter->first);
      } else if (data_table_column_checksum != iter->second) {
        ret = OB_CHECKSUM_ERROR;
        LOG_ERROR("data table and index table column checksum is not equal",
            K(ret),
            "data_table_id",
            data_table_schema.get_table_id(),
            "index_table_id",
            index_table_schema.get_table_id(),
            "column_id",
            iter->first,
            K(data_table_column_checksum),
            "index_table_column_checksum",
            iter->second);
      }
    }
  }
  if (index_table_column_checksums.created()) {
    index_table_column_checksums.destroy();
  }
  if (data_table_column_checksums.created()) {
    data_table_column_checksums.destroy();
  }
  return ret;
}

void ObSSTableChecksumItem::reset()
{
  column_checksum_.reset();
}
