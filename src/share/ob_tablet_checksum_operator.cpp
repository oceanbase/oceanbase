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

#define USING_LOG_PREFIX SHARE

#include "share/ob_tablet_checksum_operator.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/ob_freeze_info_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_smart_var.h"
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::transaction;
using namespace schema;

void ObTabletChecksumItem::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_id_.reset();
  ls_id_.reset();
  data_checksum_ = -1;
  row_count_ = 0;
  compaction_scn_.reset();
  column_meta_.reset();
}

bool ObTabletChecksumItem::is_valid() const
{
  return (tablet_id_.is_valid()) && (ls_id_.is_valid());
}

ObTabletChecksumItem &ObTabletChecksumItem::operator=(const ObTabletChecksumItem &other)
{
  tenant_id_ = other.tenant_id_;
  tablet_id_ = other.tablet_id_;
  ls_id_ = other.ls_id_;
  data_checksum_ = other.data_checksum_;
  row_count_ = other.row_count_;
  compaction_scn_ = other.compaction_scn_;
  column_meta_.assign(other.column_meta_);
  return *this;
}

bool ObTabletChecksumItem::is_same_tablet(const ObTabletChecksumItem &item) const
{
  return (tenant_id_ == item.tenant_id_) && (tablet_id_ == item.tablet_id_) 
    && (ls_id_ == item.ls_id_);
}

int ObTabletChecksumItem::compare_tablet(const ObTabletReplicaChecksumItem &replica_item) const
{
  int ret = 0;
  if (tablet_id_.id() < replica_item.tablet_id_.id()) {
    ret = -1;
  } else if (tablet_id_.id() > replica_item.tablet_id_.id()) {
    ret = 1;
  } else {
    if (ls_id_.id() < replica_item.ls_id_.id()) {
      ret = -1;
    } else if (ls_id_.id() > replica_item.ls_id_.id()) {
      ret = 1;
    }
  }
  return ret;
}

int ObTabletChecksumItem::verify_tablet_column_checksum(const ObTabletReplicaChecksumItem &replica_item) const
{
  int ret = OB_SUCCESS;

  if ((tablet_id_ != replica_item.tablet_id_)
      || (ls_id_ != replica_item.ls_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_item), K(*this));
  } else {
    // Only compare row_count and column_checksum in same compaction_scn, do not compare data_checksum.
    // Cuz data_checksum may be different in primary and restore tenants, after introducing medium compation.
    if (compaction_scn_ == replica_item.compaction_scn_) {
      bool is_same_column_checksum = false;
      if (OB_FAIL(column_meta_.check_equal(replica_item.column_meta_, is_same_column_checksum))) {
        LOG_WARN("fail to check column_meta equal", KR(ret), K(replica_item));
      } else if ((row_count_ != replica_item.row_count_) || !is_same_column_checksum) {
        ret = OB_CHECKSUM_ERROR;
        LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "fatal checksum error", KR(ret), K(is_same_column_checksum), K(replica_item), K(*this));
      }
    } 
  }
  return ret;
}

int ObTabletChecksumItem::assign(const ObTabletReplicaChecksumItem &replica_item)
{
  int ret = OB_SUCCESS;
  if (!replica_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_item));
  } else {
    tenant_id_ = replica_item.tenant_id_;
    tablet_id_ = replica_item.tablet_id_;
    ls_id_ = replica_item.ls_id_;
    data_checksum_ = replica_item.data_checksum_;
    row_count_ = replica_item.row_count_;
    compaction_scn_ = replica_item.compaction_scn_;
    if (OB_FAIL(column_meta_.assign(replica_item.column_meta_))) {
      LOG_WARN("fail to assign column meta", KR(ret), K(replica_item));
    }
  }
  return ret;
}

int ObTabletChecksumItem::assign(const ObTabletChecksumItem &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    data_checksum_ = other.data_checksum_;
    row_count_ = other.row_count_;
    compaction_scn_ = other.compaction_scn_;
    if (OB_FAIL(column_meta_.assign(other.column_meta_))) {
      LOG_WARN("fail to assign column meta", KR(ret), K(other));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObTabletChecksumOperator::load_tablet_checksum_items(
    ObISQLClient &sql_client,
    const ObTabletLSPair &start_pair,
    const int64_t batch_cnt,
    const uint64_t tenant_id,
    const SCN &compaction_scn,
    ObIArray<ObTabletChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || (!compaction_scn.is_valid())
      || (batch_cnt < 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(batch_cnt), K(compaction_scn));
  } 
  int64_t remain_cnt = batch_cnt;
  int64_t ori_items_cnt = 0;
  ObSqlString sql;
  while (OB_SUCC(ret) && (remain_cnt > 0)) {
    sql.reuse();
    const int64_t limit_cnt = ((remain_cnt >= MAX_BATCH_COUNT) ? MAX_BATCH_COUNT : remain_cnt);
    ori_items_cnt = items.count();

    ObTabletLSPair last_pair;
    if (remain_cnt == batch_cnt) {
      last_pair = start_pair;
    } else {
      const int64_t items_cnt = items.count();
      if (items_cnt > 0) {
        const ObTabletChecksumItem &last_item = items.at(items_cnt - 1);
        if (OB_FAIL(last_pair.init(last_item.tablet_id_, last_item.ls_id_))) {
          LOG_WARN("fail to init last tablet_ls_pair", KR(ret), K(last_item));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("err unexpected, about tablet items count", KR(ret), K(tenant_id), K(start_pair),
          K(batch_cnt), K(remain_cnt), K(items_cnt));
      }
    }

    if (FAILEDx(construct_load_sql_str_(tenant_id, last_pair, limit_cnt, compaction_scn, sql))) {
      LOG_WARN("fail to construct load sql", KR(ret), K(tenant_id), K(last_pair), K(limit_cnt), 
        K(compaction_scn));
    } else if (OB_FAIL(load_tablet_checksum_items(sql_client, sql, tenant_id, items))) {
      LOG_WARN("fail to load tablet checksum items", KR(ret), K(tenant_id), K(sql));
    } else {
      const int64_t curr_items_cnt = items.count();
      if (curr_items_cnt - ori_items_cnt == limit_cnt) {
        remain_cnt -= limit_cnt;
      } else {
        remain_cnt = 0;
      }
    }
  }
  return ret;
}

int ObTabletChecksumOperator::load_tablet_checksum_items(
    ObISQLClient &sql_client,
    const ObIArray<ObTabletLSPair> &pairs,
    const uint64_t tenant_id,
    const SCN &compaction_scn,
    ObIArray<ObTabletChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  const int64_t pairs_cnt = pairs.count();
  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, pairs_cnt);
  ObSqlString sql;
  while (OB_SUCC(ret) && (start_idx < end_idx)) {
    sql.reuse();
    if (OB_FAIL(construct_load_sql_str_(tenant_id, pairs, start_idx, end_idx, compaction_scn, sql))) {
      LOG_WARN("fail to construct load sql", KR(ret), K(tenant_id), K(pairs_cnt), 
        K(start_idx), K(end_idx));
    } else if (OB_FAIL(load_tablet_checksum_items(sql_client, sql, tenant_id, items))) {
      LOG_WARN("fail to load tablet checksum items", KR(ret), K(tenant_id), K(sql));
    } else {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_cnt);
    }
  }
  return ret;
}

int ObTabletChecksumOperator::load_tablet_checksum_items(
    ObISQLClient &sql_client,
    const ObSqlString &sql,
    const uint64_t tenant_id,
    ObIArray<ObTabletChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_UNLIKELY(!sql.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", KR(ret), K(sql), K(tenant_id));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", KR(ret), K(sql), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          ObTabletChecksumItem item;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", KR(ret), K(tenant_id));
            }
          } else {
            int64_t ls_id = ObLSID::INVALID_LS_ID;
            int64_t tenant_id = -1;
            int64_t tablet_id = -1;
            uint64_t compaction_scn_val = 0;
            ObString column_meta_str;
            EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
            EXTRACT_UINT_FIELD_MYSQL(*result, "compaction_scn", compaction_scn_val, uint64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", tablet_id, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "data_checksum", item.data_checksum_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "row_count", item.row_count_, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "column_checksums", column_meta_str);

            if (FAILEDx(item.compaction_scn_.convert_for_inner_table_field(compaction_scn_val))) {
              LOG_WARN("fail to convert val to SCN", KR(ret), K(compaction_scn_val));
            } else {
              item.tenant_id_ = (uint64_t)tenant_id;
              item.tablet_id_ = (uint64_t)tablet_id;
              item.ls_id_ = ls_id;
              if (OB_FAIL(ObTabletReplicaChecksumOperator::set_column_meta_with_hex_str(column_meta_str, item.column_meta_))) {
                LOG_WARN("fail to deserialize column meta from hex str", KR(ret));
              } else if (OB_FAIL(items.push_back(item))) {
                LOG_WARN("fail to push back item", KR(ret), K(item));
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTabletChecksumOperator::construct_load_sql_str_(
    const uint64_t tenant_id,
    const ObTabletLSPair &start_pair,
    const int64_t batch_cnt,
    const SCN &compaction_scn,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const uint64_t extract_tenant_id = 0;
  if ((batch_cnt < 1) || (!compaction_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(batch_cnt), K(compaction_scn));
  } else {
    if (compaction_scn > SCN::min_scn()) {
      if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' and compaction_scn = %lu "
          "and tablet_id > '%lu' ", OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id,
          compaction_scn.get_val_for_inner_table_field(), start_pair.get_tablet_id().id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(start_pair), K(compaction_scn));
      } else if (OB_FAIL(sql.append_fmt(" ORDER BY tenant_id, tablet_id limit %ld", batch_cnt))) {
        LOG_WARN("fail to assign sql string", KR(ret), K(tenant_id), K(batch_cnt), K(compaction_scn));
      }
    } else { // compaction_scn == 0: get records with all compaction_scn
      if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' and tablet_id > '%lu' ",
          OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id, start_pair.get_tablet_id().id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(start_pair), K(compaction_scn));
      } else if (OB_FAIL(sql.append_fmt(" ORDER BY tenant_id, tablet_id, compaction_scn limit %ld",
          batch_cnt))) {
        LOG_WARN("fail to assign sql string", KR(ret), K(tenant_id), K(batch_cnt), K(compaction_scn));
      }
    }
  }
  return ret;
}

int ObTabletChecksumOperator::construct_load_sql_str_(
    const uint64_t tenant_id,
    const common::ObIArray<ObTabletLSPair> &pairs,
    const int64_t start_idx,
    const int64_t end_idx,
    const SCN &compaction_scn,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const uint64_t extract_tenant_id = 0;
  const int64_t pairs_cnt = pairs.count();
  if ((start_idx < 0) || (end_idx > pairs_cnt) || 
      (start_idx > end_idx) || (pairs_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(end_idx), K(pairs_cnt));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' and compaction_scn = "
      "%lu and (tablet_id, ls_id) IN (", OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id,
      compaction_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    ObSqlString order_by_sql;
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletLSPair &pair = pairs.at(idx);
      if (OB_UNLIKELY(!pair.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet_ls_pair", KR(ret), K(tenant_id), K(pair), K(idx));
      } else if (OB_FAIL(sql.append_fmt(
          "(%ld,%ld)%s",
          pair.get_tablet_id().id(),
          pair.get_ls_id().id(),
          ((idx == end_idx - 1) ? ")" : ", ")))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(pair));
      } else if (OB_FAIL(order_by_sql.append_fmt(
          ",%ld",
          pair.get_tablet_id().id()))) {
        SHARE_LOG(WARN, "fail to assign sql", KR(ret), K(tenant_id), K(pair));
      }
    }
    if (FAILEDx(sql.append_fmt(" ORDER BY FIELD(tablet_id%s)", order_by_sql.string().ptr()))) {
      SHARE_LOG(WARN, "fail to assign sql string", KR(ret), K(tenant_id), K(compaction_scn), K(pairs_cnt));
    }
  }
  return ret;
}

int ObTabletChecksumOperator::insert_tablet_checksum_item(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObTabletChecksumItem &item)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletChecksumItem> items;
  if (OB_FAIL(items.push_back(item))) {
    LOG_WARN("fail to add item into array", KR(ret), K(item));
  } else if (OB_FAIL(insert_tablet_checksum_items(sql_client, tenant_id, items))) {
    LOG_WARN("fail to insert tablet checksum items", KR(ret), K(item));
  }
  return ret;
}

int ObTabletChecksumOperator::insert_tablet_checksum_items(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObTabletChecksumItem> &items)
{
  return insert_or_update_tablet_checksum_items_(sql_client, tenant_id, items, false/*is_update*/);
}

int ObTabletChecksumOperator::update_tablet_checksum_items(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObTabletChecksumItem> &items)
{
  return insert_or_update_tablet_checksum_items_(sql_client, tenant_id, items, true/*is_update*/);
}

int ObTabletChecksumOperator::insert_or_update_tablet_checksum_items_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObTabletChecksumItem> &items,
    const bool is_update)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLTransaction trans;
  const uint64_t extract_tenant_id = 0;
  const int64_t item_cnt = items.count();
  if (OB_UNLIKELY(item_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(item_cnt), K(tenant_id));
  } else if (OB_FAIL(trans.start(&sql_client, tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret), K(tenant_id));
  } else {
    int64_t remain_cnt = item_cnt; 
    int64_t report_idx = 0;
    while (OB_SUCC(ret) && (remain_cnt > 0)) {
      sql.reuse();
      if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, compaction_scn, tablet_id, ls_id, data_checksum, "
          "row_count, column_checksums, gmt_modified, gmt_create) VALUES", OB_ALL_TABLET_CHECKSUM_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      } else {
        ObArenaAllocator allocator;

        int64_t cur_batch_cnt = ((remain_cnt < MAX_BATCH_COUNT) ? remain_cnt : MAX_BATCH_COUNT);
        int64_t bias = item_cnt - remain_cnt;
        for (int64_t i = 0; OB_SUCC(ret) && (i < cur_batch_cnt); ++i) {
          const ObTabletChecksumItem &item = items.at(bias + i);
          const uint64_t compaction_scn_val = item.compaction_scn_.get_val_for_inner_table_field();
          ObString hex_column_meta;

          if (OB_FAIL(ObTabletReplicaChecksumOperator::get_hex_column_meta(
              item.column_meta_, allocator, hex_column_meta))) {
            LOG_WARN("fail to serialize column meta to hex str", KR(ret), K(item));
          } else if (OB_FAIL(sql.append_fmt("('%lu', %lu, '%lu', %ld, %ld, %ld, '%.*s', now(6), now(6))%s",
              extract_tenant_id, compaction_scn_val, item.tablet_id_.id(), item.ls_id_.id(),
              item.data_checksum_, item.row_count_, hex_column_meta.length(), hex_column_meta.ptr(),
              ((i == cur_batch_cnt - 1) ? " " : ", ")))) {
            LOG_WARN("fail to assign sql", KR(ret), K(i), K(bias), K(item));
          }
        }

        if (OB_SUCC(ret) && is_update) {
          if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
            LOG_WARN("fail to append sql string", KR(ret), K(sql));
          } else if (OB_FAIL(sql.append(" data_checksum = values(data_checksum)"))
                    || OB_FAIL(sql.append(", row_count = values(row_count)"))
                    || OB_FAIL(sql.append(", column_checksums = values(column_checksums)"))) {
            LOG_WARN("fail to append sql string", KR(ret), K(sql));
          }
        }

        if (OB_SUCC(ret)) {
          int64_t affected_rows = 0;
          if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", KR(ret), K(sql));
          } else if (!is_update) {  // do not check affected_rows, when is_update = true
            if (OB_UNLIKELY(affected_rows != cur_batch_cnt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid affected rows", KR(ret), K(tenant_id), K(affected_rows), K(cur_batch_cnt));
            }
          }
          if (OB_SUCC(ret)) {
            remain_cnt -= cur_batch_cnt;
          }
        }
      }
    } // end loop while

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit trans", KR(ret), K(tenant_id));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /*commit*/))) {
        LOG_WARN("fail to abort trans", KR(ret), K(tmp_ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObTabletChecksumOperator::delete_tablet_checksum_items(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN &gc_compaction_scn,
    const int64_t limit_cnt,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  affected_rows = 0;
  const uint64_t extract_tenant_id = 0;
  const uint64_t gc_scn_val = gc_compaction_scn.is_valid() ? gc_compaction_scn.get_val_for_inner_table_field() : 0;
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)))
      || (!gc_compaction_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(gc_compaction_scn));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND compaction_scn <= %lu"
    " AND NOT (tablet_id=%ld AND ls_id=%ld) limit %ld", OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id,
    gc_scn_val, ObTabletID::MIN_VALID_TABLET_ID, ObLSID::SYS_LS_ID, limit_cnt))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(gc_compaction_scn), K(limit_cnt));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else {
    LOG_INFO("succ to delete tablet checksum items", K(tenant_id), K(gc_compaction_scn), K(affected_rows), K(limit_cnt));
  }
  return ret;
}

int ObTabletChecksumOperator::delete_special_tablet_checksum_items(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN &gc_compaction_scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t extract_tenant_id = 0;
  const uint64_t gc_scn_val = gc_compaction_scn.is_valid() ? gc_compaction_scn.get_val_for_inner_table_field() : 0;
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)))
      || (!gc_compaction_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(gc_compaction_scn));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND compaction_scn <= %lu"
    " AND tablet_id=%ld AND ls_id=%ld", OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id,
    gc_scn_val, ObTabletID::MIN_VALID_TABLET_ID, ObLSID::SYS_LS_ID))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(gc_compaction_scn));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else {
    LOG_INFO("succ to delete special tablet checksum items", K(tenant_id), K(gc_compaction_scn), K(affected_rows));
  }
  return ret;
}

int ObTabletChecksumOperator::delete_tablet_checksum_items(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObTabletChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t items_cnt = items.count();
  const uint64_t extract_tenant_id = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || items_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(items_cnt));
  } else if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE tenant_id = '%lu' and (tablet_id, ls_id, compaction_scn)"
             " IN (", OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < items_cnt); ++i) {
      const ObTabletChecksumItem &tmp_item = items.at(i);
      const uint64_t compaction_scn_val = tmp_item.compaction_scn_.get_val_for_inner_table_field();
      if (OB_FAIL(sql.append_fmt(
          "(%ld, %ld, %lu)%s",
          tmp_item.tablet_id_.id(),
          tmp_item.ls_id_.id(),
          compaction_scn_val,
          (i == items_cnt - 1) ? ")" : ", "))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(tmp_item));
      }
    }

    if (FAILEDx(sql.append_fmt(") ORDER BY tenant_id, tablet_id, ls_id, compaction_scn"))) {
      LOG_WARN("fail to assign sql string", KR(ret), K(tenant_id), K(items_cnt));
    } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else {
      LOG_TRACE("succ to delete tablet checksum items", K(tenant_id), K(affected_rows),
        K(items_cnt));
    }
  }
  return ret;
}

int ObTabletChecksumOperator::load_all_compaction_scn(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<SCN> &compaction_scn_arr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t estimated_timeout_us = 0;
  ObTimeoutCtx timeout_ctx;
  int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      // set trx_timeout and query_timeout based on tablet_cnt
      if (OB_FAIL(ObTabletChecksumOperator::get_estimated_timeout_us(sql_client, tenant_id,
                                            estimated_timeout_us))) {
        LOG_WARN("fail to get estimated_timeout_us", KR(ret), K(tenant_id));
      } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(estimated_timeout_us))) {
        LOG_WARN("fail to set trx timeout", KR(ret), K(estimated_timeout_us));
      } else if (OB_FAIL(timeout_ctx.set_timeout(estimated_timeout_us))) {
        LOG_WARN("fail to set abs timeout", KR(ret), K(estimated_timeout_us));
      } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT compaction_scn as dis_compaction_scn FROM %s"
          " WHERE tenant_id = 0 ORDER BY compaction_scn ASC", OB_ALL_TABLET_CHECKSUM_TNAME))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          uint64_t compaction_scn_val = 0;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", KR(ret), K(tenant_id));
            }
          } else {
            EXTRACT_UINT_FIELD_MYSQL(*result, "dis_compaction_scn", compaction_scn_val, uint64_t);
          }

          SCN tmp_compaction_scn;
          if (FAILEDx(tmp_compaction_scn.convert_for_inner_table_field(compaction_scn_val))) {
            LOG_WARN("fail to convert val to SCN", KR(ret), K(compaction_scn_val));
          } else if (OB_UNLIKELY(!tmp_compaction_scn.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid compaction_scn", KR(ret), K(tmp_compaction_scn), K(tenant_id), K(sql));
          } else if (OB_FAIL(compaction_scn_arr.push_back(tmp_compaction_scn))) {
            LOG_WARN("fail to push back", KR(ret), K(tmp_compaction_scn), K(tenant_id));
          }
        } // end for while

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;
  LOG_INFO("finish to load all compaction_scn", KR(ret), K(tenant_id), K(cost_time_us),
           K(estimated_timeout_us), K(sql), K(compaction_scn_arr));
  return ret;
}

int ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN &compaction_scn,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !compaction_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(compaction_scn));
  }
  if (OB_SUCC(ret)) {
    is_exist = false;
    const uint64_t extract_tenant_id = 0;
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      uint64_t compaction_scn_val = compaction_scn.get_val_for_inner_table_field();
      if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS cnt FROM %s WHERE tenant_id = '%lu' AND "
            "compaction_scn >= %lu AND tablet_id = %lu AND ls_id = %ld", OB_ALL_TABLET_CHECKSUM_TNAME,
            extract_tenant_id, compaction_scn_val, ObTabletID::MIN_VALID_TABLET_ID, ObLSID::SYS_LS_ID))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
      } else {
        int64_t cnt = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
        if (OB_SUCC(ret)) {
          if (cnt >= 1) {
            is_exist = true;
          } else if (0 == cnt) {
            is_exist = false;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected count of first tablet in sys ls", KR(ret), K(tenant_id), K(sql), K(cnt));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletChecksumOperator::is_all_tablet_checksum_sync(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<uint64_t> &frozen_scn_vals,
    bool &is_sync)
{
  int ret = OB_SUCCESS;
  int64_t frozen_scn_vals_cnt = frozen_scn_vals.count();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (frozen_scn_vals_cnt <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(frozen_scn_vals_cnt));
  } else {
    is_sync = false;
    const uint64_t extract_tenant_id = 0;
    // split into several batches, so as to avoid the sql too long
    const int64_t batch_cnt = 100;
    int64_t start_idx = 0;
    int64_t end_idx = min(batch_cnt, frozen_scn_vals_cnt);
    while (OB_SUCC(ret) && !is_sync && (start_idx < end_idx)) {
      ObSqlString sql;
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql.append_fmt("SELECT COUNT(*) AS cnt FROM %s WHERE tenant_id = '%lu' AND "
              "compaction_scn IN (", OB_ALL_TABLET_CHECKSUM_TNAME, extract_tenant_id))) {
          LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
        } else {
          for (int64_t i = start_idx; (i < end_idx) && OB_SUCC(ret); ++i) {
            if (OB_FAIL(sql.append_fmt("%lu%s", frozen_scn_vals.at(i),
                        (i == (end_idx - 1)) ? "" : ","))) {
              LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(sql.append_fmt(") AND tablet_id = %lu AND ls_id = %ld",
              ObTabletID::MIN_VALID_TABLET_ID, ObLSID::SYS_LS_ID))) {
          LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
        } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
        } else {
          int64_t cnt = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
          if (OB_SUCC(ret)) {
            if (cnt >= 1) {
              is_sync = true;
            } else if (0 == cnt) {
              is_sync = false;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected count", KR(ret), K(tenant_id), K(sql), K(cnt));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        start_idx = end_idx;
        end_idx = min(start_idx + batch_cnt, frozen_scn_vals_cnt);
      }
    }
  }
  LOG_INFO("finish to check is all tablet checksum sync", KR(ret), K(is_sync),
           K(tenant_id), K(frozen_scn_vals));
  return ret;
}

int ObTabletChecksumOperator::get_tablet_cnt(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    int64_t &tablet_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.append_fmt("SELECT COUNT(*) as cnt from %s", OB_ALL_TABLET_CHECKSUM_TNAME))) {
        LOG_WARN("failed to append fmt", K(ret), K(tenant_id));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", tablet_cnt, int64_t);
      }
    }
  }
  return ret;
}

int ObTabletChecksumOperator::get_estimated_timeout_us(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    int64_t &estimated_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t tablet_cnt = 0;
  if (OB_FAIL(ObTabletChecksumOperator::get_tablet_cnt(sql_client, tenant_id, tablet_cnt))) {
    LOG_WARN("fail to get tablet replica cnt", KR(ret), K(tenant_id));
  } else {
    estimated_timeout_us = tablet_cnt * 1000L; // 1ms for each tablet
    const int64_t default_timeout_us = 9 * 1000 * 1000L;
    estimated_timeout_us = MAX(estimated_timeout_us, default_timeout_us);
    estimated_timeout_us = MIN(estimated_timeout_us, 3600 * 1000 * 1000L);
    estimated_timeout_us = MAX(estimated_timeout_us, GCONF.rpc_timeout);
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
