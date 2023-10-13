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

#include "share/ob_tablet_replica_checksum_operator.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_param.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_column_schema.h"
#include "observer/ob_server_struct.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/config/ob_server_config.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;

ObTabletReplicaReportColumnMeta::ObTabletReplicaReportColumnMeta()
  : compat_version_(0),
    checksum_method_(0),
    checksum_bytes_(0),
    column_checksums_(),
    is_inited_(false)
{}

ObTabletReplicaReportColumnMeta::~ObTabletReplicaReportColumnMeta()
{
  reset();
}

void ObTabletReplicaReportColumnMeta::reset()
{
  is_inited_ = false;
  compat_version_ = 0;
  checksum_method_ = 0;
  checksum_bytes_ = 0;
  column_checksums_.reset();
}

bool ObTabletReplicaReportColumnMeta::is_valid() const
{
  return is_inited_ && column_checksums_.count() > 0;
}

int ObTabletReplicaReportColumnMeta::init(const ObIArray<int64_t> &column_checksums)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletReplicaReportColumnMeta inited twice", KR(ret), K(*this));
  } else if (column_checksums.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(column_checksums_.assign(column_checksums))) {
    LOG_WARN("fail to assign column_checksums", KR(ret));
  } else {
    checksum_bytes_ = (sizeof(int16_t) + sizeof(int64_t) + sizeof(int8_t)) * 2;
    checksum_method_ = 0; // TODO
    is_inited_ = true;
  }
  return ret;
}

int ObTabletReplicaReportColumnMeta::assign(const ObTabletReplicaReportColumnMeta &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (other.column_checksums_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
    } else if (OB_FAIL(column_checksums_.assign(other.column_checksums_))) {
      LOG_WARN("fail to assign column_checksums", KR(ret));
    } else {
      compat_version_ = other.compat_version_;
      checksum_method_ = other.checksum_method_;
      checksum_bytes_ = other.checksum_bytes_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletReplicaReportColumnMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = get_serialize_size();
  if (OB_UNLIKELY(NULL == buf) || (serialize_size > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), KR(ret), K(serialize_size), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, MAGIC_NUMBER))) {
    LOG_WARN("fail to encode magic number", KR(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, compat_version_))) {
    LOG_WARN("fail to encode compat version", KR(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, checksum_method_))) {
    LOG_WARN("fail to encode checksum method", KR(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, checksum_bytes_))) {
    LOG_WARN("fail to encode checksum bytes", KR(ret));
  } else if (OB_FAIL(column_checksums_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize column_checksums", KR(ret));
  }
  return ret;
}

int64_t ObTabletReplicaReportColumnMeta::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(MAGIC_NUMBER);
  len += serialization::encoded_length_i8(compat_version_);
  len += serialization::encoded_length_i8(checksum_method_);
  len += serialization::encoded_length_i8(checksum_bytes_);
  len += column_checksums_.get_serialize_size();
  return len;
}

int ObTabletReplicaReportColumnMeta::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t magic_number = 0;
  if (OB_ISNULL(buf) || (buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", KR(ret), K(buf), K(buf_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &magic_number))) {
    LOG_WARN("fail to encode magic number", KR(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, buf_len, pos, &compat_version_))) {
    LOG_WARN("fail to deserialize compat version", KR(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, buf_len, pos, &checksum_method_))) {
    LOG_WARN("fail to deserialize checksum method", KR(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, buf_len, pos, &checksum_bytes_))) {
    LOG_WARN("fail to deserialize checksum bytes", KR(ret));
  } else if (OB_FAIL(column_checksums_.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize column checksums", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int64_t ObTabletReplicaReportColumnMeta::get_string_length() const
{
  int64_t len = 0;
  len += sizeof("magic:%lX,");
  len += sizeof("compat:%d,");
  len += sizeof("method:%d,");
  len += sizeof("bytes:%d,");
  len += sizeof("colcnt:%d,");
  len += sizeof("%d:%ld,") * column_checksums_.count();
  len += get_serialize_size();
  return len;
}

int64_t ObTabletReplicaReportColumnMeta::get_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int32_t column_cnt = static_cast<int32_t>(column_checksums_.count());
  common::databuff_printf(buf, buf_len, pos, "magic:%lX,", MAGIC_NUMBER);
  common::databuff_printf(buf, buf_len, pos, "compat:%d,", compat_version_);
  common::databuff_printf(buf, buf_len, pos, "method:%d,", checksum_method_);
  common::databuff_printf(buf, buf_len, pos, "bytes:%d,", checksum_bytes_);
  common::databuff_printf(buf, buf_len, pos, "colcnt:%d,", column_cnt);

  for (int32_t i = 0; i < column_cnt; ++i) {
    if (column_cnt - 1 != i) {
      common::databuff_printf(buf, buf_len, pos, "%d:%ld,", i, column_checksums_.at(i));
    } else {
      common::databuff_printf(buf, buf_len, pos, "%d:%ld", i, column_checksums_.at(i));
    }
  }
  return pos;
}

int ObTabletReplicaReportColumnMeta::check_checksum(
    const ObTabletReplicaReportColumnMeta &other,
    const int64_t pos, bool &is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const int64_t col_ckm_cnt = column_checksums_.count();
  const int64_t other_col_ckm_cnt = other.column_checksums_.count();
  if ((pos < 0) || (pos > col_ckm_cnt) || (pos > other_col_ckm_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(pos), K(col_ckm_cnt), K(other_col_ckm_cnt),
      K(column_checksums_), K(other.column_checksums_));
  } else if (column_checksums_.at(pos) != other.column_checksums_.at(pos)) {
    is_equal = false;
    LOG_WARN("column checksum is not equal!", K(pos), "col_ckm", column_checksums_.at(pos),
      "other_col_ckm", other.column_checksums_.at(pos), K(col_ckm_cnt), K(other_col_ckm_cnt),
      K(column_checksums_), K(other.column_checksums_));
  }
  return ret;
}

int ObTabletReplicaReportColumnMeta::check_all_checksums(
    const ObTabletReplicaReportColumnMeta &other, 
    bool &is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (column_checksums_.count() != other.column_checksums_.count()) {
    is_equal = false;
    LOG_WARN("column cnt is not equal!", "cur_cnt", column_checksums_.count(),
      "other_cnt", other.column_checksums_.count(), K(*this), K(other));
  } else {
    const int64_t column_ckm_cnt = column_checksums_.count();
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && (i < column_ckm_cnt); ++i) {
      if (OB_FAIL(check_checksum(other, i, is_equal))) {
        LOG_WARN("fail to check checksum", KR(ret), K(i), K(column_ckm_cnt));
      }
    }
  }
  return ret;
}

int ObTabletReplicaReportColumnMeta::check_equal(
    const ObTabletReplicaReportColumnMeta &other, 
    bool &is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (compat_version_ != other.compat_version_) {
    is_equal = false;
    LOG_WARN("compat version is not equal !", K(*this), K(other));
  } else if (checksum_method_ != other.checksum_method_) {
    is_equal = false;
    LOG_WARN("checksum method is different !", K(*this), K(other));
  } else if (OB_FAIL(check_all_checksums(other, is_equal))) {
    LOG_WARN("fail to check all checksum", KR(ret), K(*this), K(other));
  }
  return ret;
}

int ObTabletReplicaReportColumnMeta::get_column_checksum(const int64_t pos, int64_t &checksum) const
{
  int ret = OB_SUCCESS;
  checksum = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletReplicaReportColumnMeta is not inited", KR(ret));
  } else if (pos < 0 || pos >= column_checksums_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(pos), K(column_checksums_));
  } else {
    checksum = column_checksums_.at(pos);
  }
  return ret;
}


/****************************** ObTabletReplicaChecksumItem ******************************/

ObTabletReplicaChecksumItem::ObTabletReplicaChecksumItem()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    server_(),
    row_count_(0),
    compaction_scn_(),
    data_checksum_(0),
    column_meta_()
{}

void ObTabletReplicaChecksumItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  server_.reset();
  row_count_ = 0;
  compaction_scn_.reset();
  data_checksum_ = 0;
  column_meta_.reset();
}

bool ObTabletReplicaChecksumItem::is_key_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && tablet_id_.is_valid_with_tenant(tenant_id_)
      && server_.is_valid();
}

bool ObTabletReplicaChecksumItem::is_valid() const
{
  return is_key_valid() && column_meta_.is_valid();
}

bool ObTabletReplicaChecksumItem::is_same_tablet(const ObTabletReplicaChecksumItem &other) const
{
  return is_key_valid()
      && other.is_key_valid()
      && tenant_id_ == other.tenant_id_
      && ls_id_ == other.ls_id_
      && tablet_id_ == other.tablet_id_;
}

int ObTabletReplicaChecksumItem::verify_checksum(const ObTabletReplicaChecksumItem &other) const
{
  int ret = OB_SUCCESS;
  if (compaction_scn_ == other.compaction_scn_) {
    bool column_meta_equal = false;
    if (OB_FAIL(column_meta_.check_equal(other.column_meta_, column_meta_equal))) {
      LOG_WARN("fail to check column meta equal", KR(ret), K(other), K(*this));
    } else if (!column_meta_equal) {
      ret = OB_CHECKSUM_ERROR;
    } else if ((row_count_ != other.row_count_) || (data_checksum_ != other.data_checksum_)) {
      ret = OB_CHECKSUM_ERROR;
    }
  } else {
    LOG_INFO("no need to check data checksum", K(other), K(*this));
  }
  return ret;
}

int ObTabletReplicaChecksumItem::assign_key(const ObTabletReplicaChecksumItem &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_key_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    server_ = other.server_;
  }
  return ret;
}

int ObTabletReplicaChecksumItem::assign(const ObTabletReplicaChecksumItem &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(column_meta_.assign(other.column_meta_))) {
      LOG_WARN("fail to assign column meta", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      tablet_id_ = other.tablet_id_;
      ls_id_ = other.ls_id_;
      server_ = other.server_;
      row_count_ = other.row_count_;
      compaction_scn_ = other.compaction_scn_;
      data_checksum_ = other.data_checksum_;
    }
  }
  return ret;
}

ObTabletReplicaChecksumItem &ObTabletReplicaChecksumItem::operator=(const ObTabletReplicaChecksumItem &other)
{
  assign(other);
  return *this;
}

/****************************** ObTabletReplicaChecksumOperator ******************************/

int ObTabletReplicaChecksumOperator::batch_remove_with_trans(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObIArray<share::ObTabletReplica> &tablet_replicas)
{
  int ret = OB_SUCCESS;
  const int64_t replicas_count = tablet_replicas.count();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || replicas_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "tablet_replica cnt", replicas_count);
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, replicas_count);
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_remove_by_sql_(tenant_id, tablet_replicas, start_idx, end_idx, trans))) {
        LOG_WARN("fail to inner batch remove", KR(ret), K(tenant_id), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, replicas_count);
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_remove_by_sql_(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObTabletReplica> &tablet_replicas,
    const int64_t start_idx,
    const int64_t end_idx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || (tablet_replicas.count() <= 0)
      || (start_idx < 0)
      || (start_idx > end_idx)
      || (end_idx > tablet_replicas.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(start_idx), K(end_idx),
             "tablet_replica cnt", tablet_replicas.count());
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND (tablet_id, svr_ip, svr_port, ls_id) IN(",
                OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    } else {
      char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
      for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
        if (OB_UNLIKELY(!tablet_replicas.at(idx).get_server().ip_to_string(ip, sizeof(ip)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("convert server ip to string failed", KR(ret), "server", tablet_replicas.at(idx).get_server());
        } else if (OB_FAIL(sql.append_fmt("('%lu', '%s', %d, %ld)%s",
            tablet_replicas.at(idx).get_tablet_id().id(),
            ip,
            tablet_replicas.at(idx).get_server().get_port(),
            tablet_replicas.at(idx).get_ls_id().id(),
            ((idx == end_idx - 1) ? ")" : ", ")))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(idx), K(start_idx), K(end_idx));
        }
      }

      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      if (FAILEDx(trans.write(meta_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(meta_tenant_id), K(sql));
      } else {
        LOG_INFO("will batch delete tablet replica checksum", K(affected_rows));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::remove_residual_checksum(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObAddr &server,
    const int64_t limit,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || is_virtual_tenant_id(tenant_id)
                  || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(server));
  } else if (OB_UNLIKELY(!server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", KR(ret), K(server));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND svr_ip = '%s' AND"
             " svr_port = %d limit %ld", OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id, ip,
             server.get_port(), limit))) {
    LOG_WARN("assign sql string failed", KR(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql), K(sql_tenant_id));
  } else if (affected_rows > 0) {
    LOG_INFO("finish to remove residual checksum", KR(ret), K(tenant_id), K(affected_rows));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::batch_get(
    const uint64_t tenant_id,
    const ObTabletLSPair &start_pair,
    const SCN &compaction_scn,
    ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(construct_batch_get_sql_str_(tenant_id, start_pair, MAX_BATCH_COUNT,
                                           compaction_scn, sql))) {
    LOG_WARN("fail to construct load sql", KR(ret), K(tenant_id), K(start_pair), K(compaction_scn));
  } else if (OB_FAIL(batch_get(tenant_id, sql, sql_proxy, items))) {
    LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K(tenant_id), K(sql));
  } else {
    const int64_t items_cnt = items.count();
    if (MAX_BATCH_COUNT == items_cnt) {
      // in case the checksum of three replica belong to one tablet, were split into two batch-get,
      // we will remove the last several item which belong to one tablet
      // if current round item's count less than limit_cnt, it means already to the end, no need to handle.
      int64_t tmp_items_cnt = items_cnt;
      ObTabletReplicaChecksumItem tmp_item;
      if (OB_FAIL(tmp_item.assign_key(items.at(tmp_items_cnt - 1)))) {
        LOG_WARN("fail to assign key", KR(ret), "tmp_item", items.at(tmp_items_cnt - 1));
      }
      while (OB_SUCC(ret) && (tmp_items_cnt > 0)) {
        if (tmp_item.is_same_tablet(items.at(tmp_items_cnt - 1))) {
          if (OB_FAIL(items.remove(tmp_items_cnt - 1))) {
            LOG_WARN("fail to remove item from array", KR(ret), K(tmp_items_cnt), K(items));
          } else {
            --tmp_items_cnt;
          }
        } else {
          break;
        }
      }

      if (OB_SUCC(ret)) {
        if (tmp_items_cnt == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err about item count", KR(ret), K(tmp_item), K(items_cnt));
        }
      }
    }
  }

  return ret;
}

int ObTabletReplicaChecksumOperator::get_specified_tablet_checksum(
      const uint64_t tenant_id,
      const int64_t ls_id,
      const int64_t tablet_id,
      const int64_t snapshot_version,
      common::ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY((OB_INVALID_TENANT_ID == tenant_id)
      || (ls_id <= 0)
      || (tablet_id <= 0)
      || (snapshot_version < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(snapshot_version));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' and tablet_id = '%ld' "
      "and ls_id = '%ld' and compaction_scn = '%ld'",
      OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME,
      tenant_id,
      tablet_id,
      ls_id,
      snapshot_version))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(ls_id), K(snapshot_version));
  } else if (OB_FAIL(batch_get(tenant_id, sql, *GCTX.sql_proxy_, items))) {
    LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::batch_get(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &pairs,
    const SCN &compaction_scn,
    ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items,
    const bool include_larger_than)
{
  int ret = OB_SUCCESS;
  items.reset();
  const int64_t pairs_cnt = pairs.count();
  hash::ObHashMap<ObTabletLSPair, bool> pair_map;
  if (OB_UNLIKELY(pairs_cnt < 1 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pairs_cnt));
  }
  // Step 1: check repeatable ObTabletLSPair by hash map
  if (FAILEDx(inner_init_tablet_pair_map_(pairs, pair_map))) {
    LOG_WARN("fail to init tablet_ls_pair map", KR(ret));
  }
  // Step 2: cut tablet replica checksum items into small batches
  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, pairs_cnt);
  ObSqlString sql;
  while (OB_SUCC(ret) && (start_idx < end_idx)) {
    sql.reuse();
    if (OB_FAIL(construct_batch_get_sql_str_(tenant_id, compaction_scn, pairs, start_idx, end_idx,
                                             sql, include_larger_than))) {
      LOG_WARN("fail to construct batch get sql", KR(ret), K(tenant_id), K(compaction_scn), K(pairs),
        K(start_idx), K(end_idx));
    } else if (OB_FAIL(inner_batch_get_by_sql_(tenant_id, sql, sql_proxy, items))) {
      LOG_WARN("fail to inner batch get by sql", KR(ret), K(tenant_id), K(sql));
    } else {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_cnt);
    }
  }
  // Step 3: check tablet replica checksum item and set flag in map
  if (OB_SUCC(ret)) {
    int overwrite_flag = 1;
    ARRAY_FOREACH_N(items, idx, cnt) {
      ObTabletLSPair tmp_pair;
      if (OB_FAIL(tmp_pair.init(items.at(idx).tablet_id_, items.at(idx).ls_id_))) {
        LOG_WARN("fail to init tablet_ls_pair", KR(ret), K(items.at(idx)), K(idx));
      } else if (OB_FAIL(pair_map.set_refactored(tmp_pair, true, overwrite_flag))) {
        LOG_WARN("fail to set_fefactored", KR(ret), K(tenant_id), K(tmp_pair));
      }
    }
    // print tablet_ls_pair which not exist in tablet replica checksum table
    if (OB_SUCC(ret)) {
      FOREACH_X(iter, pair_map, OB_SUCC(ret)) {
        if (!iter->second) {
          LOG_TRACE("tablet replica checksum item not exist in tablet replica checksum table",
            KR(ret),  K(tenant_id), "tablet_ls_pair", iter->first);
        }
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_init_tablet_pair_map_(
    const ObIArray<ObTabletLSPair> &pairs,
    hash::ObHashMap<ObTabletLSPair, bool> &pair_map)
{
  int ret = OB_SUCCESS;
  const int64_t pairs_cnt = pairs.count();
  if (FAILEDx(pair_map.create(hash::cal_next_prime(pairs_cnt * 2),
      ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("fail to create pair_map", KR(ret), K(pairs_cnt));
  } else {
    ARRAY_FOREACH_N(pairs, idx, cnt) {
      // if same talet_id exist, return error
      if (OB_FAIL(pair_map.set_refactored(pairs.at(idx), false))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("exist repeatable tablet_ls_pair", KR(ret), K(pairs), K(idx));
        } else {
          LOG_WARN("fail to set refactored", KR(ret), K(pairs), K(idx));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (pair_map.size() != pairs_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid pair_map size", "size", pair_map.size(), K(pairs_cnt));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::batch_get(
    const uint64_t tenant_id,
    const ObSqlString &sql,
    ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  return inner_batch_get_by_sql_(tenant_id, sql, sql_proxy, items);
}

int ObTabletReplicaChecksumOperator::construct_batch_get_sql_str_(
    const uint64_t tenant_id,
    const ObTabletLSPair &start_pair,
    const int64_t batch_cnt,
    const SCN &compaction_scn,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if ((batch_cnt < 1) || (!compaction_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(batch_cnt), K(compaction_scn));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' and tablet_id > '%lu' "
      "and compaction_scn = %lu", OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id,
      start_pair.get_tablet_id().id(), compaction_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(start_pair), K(compaction_scn));
  } else if (OB_FAIL(sql.append_fmt(" ORDER BY tenant_id, tablet_id, svr_ip, svr_port limit %ld",
      batch_cnt))) {
    LOG_WARN("fail to assign sql string", KR(ret), K(tenant_id), K(batch_cnt));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::construct_batch_get_sql_str_(
    const uint64_t tenant_id,
    const SCN &compaction_scn,
    const ObIArray<ObTabletLSPair> &pairs,
    const int64_t start_idx,
    const int64_t end_idx,
    ObSqlString &sql,
    const bool include_larger_than)
{
  int ret = OB_SUCCESS;
  const int64_t pairs_cnt = pairs.count();
  if (start_idx < 0 || end_idx > pairs_cnt || start_idx > end_idx ||
      pairs_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(end_idx), K(pairs_cnt));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' AND compaction_scn %s %ld"
      " AND (tablet_id, ls_id) IN ((", OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id,
      include_larger_than ? ">=" : "=", compaction_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(compaction_scn));
  } else {
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletLSPair &pair = pairs.at(idx);
      if (OB_UNLIKELY(!pair.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet_ls_pair", KR(ret), K(tenant_id), K(pair));
      } else if (OB_FAIL(sql.append_fmt(
          "'%lu', %ld%s",
          pair.get_tablet_id().id(),
          pair.get_ls_id().id(),
          ((idx == end_idx - 1) ? ")" : "), (")))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(pair));
      }
    }
  }

  if (FAILEDx(sql.append_fmt(") ORDER BY tenant_id, tablet_id, ls_id, svr_ip, svr_port"))) {
    LOG_WARN("fail to assign sql string", KR(ret), K(tenant_id), K(compaction_scn), K(pairs_cnt));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_get_by_sql_(
    const uint64_t tenant_id,
    const ObSqlString &sql,
    ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy.read(result, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(construct_tablet_replica_checksum_items_(*result.get_result(), items))) {
        LOG_WARN("fail to construct tablet checksum items", KR(ret), K(items));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::construct_tablet_replica_checksum_items_(
    sqlclient::ObMySQLResult &res,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObTabletReplicaChecksumItem item;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next result", KR(ret));
      }
    } else {
      item.reset();
      if (OB_FAIL(construct_tablet_replica_checksum_item_(res, item))) {
        LOG_WARN("fail to construct tablet checksum item", KR(ret));
      } else if (OB_FAIL(items.push_back(item))) {
        LOG_WARN("fail to push back checksum item", KR(ret), K(item));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::construct_tablet_replica_checksum_item_(
    sqlclient::ObMySQLResult &res,
    ObTabletReplicaChecksumItem &item)
{
  int ret = OB_SUCCESS;
  int64_t int_tenant_id = -1;
  int64_t int_tablet_id = -1;
  ObString ip;
  int64_t port = OB_INVALID_INDEX;
  int64_t ls_id = OB_INVALID_ID;
  uint64_t compaction_scn_val = 0;
  ObString column_meta_hex_str;

  (void)GET_COL_IGNORE_NULL(res.get_int, "tenant_id", int_tenant_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "tablet_id", int_tablet_id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "svr_ip", ip);
  (void)GET_COL_IGNORE_NULL(res.get_int, "svr_port", port);
  (void)GET_COL_IGNORE_NULL(res.get_int, "ls_id", ls_id);
  (void)GET_COL_IGNORE_NULL(res.get_uint, "compaction_scn", compaction_scn_val);
  (void)GET_COL_IGNORE_NULL(res.get_int, "row_count", item.row_count_);
  (void)GET_COL_IGNORE_NULL(res.get_int, "data_checksum", item.data_checksum_);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "b_column_checksums", column_meta_hex_str);

  if (OB_FAIL(item.compaction_scn_.convert_for_inner_table_field(compaction_scn_val))) {
    LOG_WARN("fail to convert val to SCN", KR(ret), K(compaction_scn_val));
  } else {
    item.tenant_id_ = (uint64_t)int_tenant_id;
    item.tablet_id_ = (uint64_t)int_tablet_id;
    item.ls_id_ = ls_id;
    if (OB_UNLIKELY(!item.server_.set_ip_addr(ip, static_cast<int32_t>(port)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set ip_addr", KR(ret), K(item), K(ip), K(port));
    } else if (OB_FAIL(set_column_meta_with_hex_str(column_meta_hex_str, item.column_meta_))) {
      LOG_WARN("fail to deserialize column meta from hex str", KR(ret));
    }
  }

  LOG_TRACE("construct tablet checksum item", KR(ret), K(item));
  return ret;
}


int ObTabletReplicaChecksumOperator::batch_update_with_trans(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const common::ObIArray<ObTabletReplicaChecksumItem> &items)
{
  return batch_insert_or_update_with_trans_(tenant_id, items, trans, true);
}

int ObTabletReplicaChecksumOperator::batch_insert_or_update_with_trans_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    common::ObMySQLTransaction &trans,
    const bool is_update)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((OB_INVALID_TENANT_ID == tenant_id) || (items.count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "items count", items.count());
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, items.count());
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_insert_or_update_by_sql_(tenant_id, items, start_idx,
          end_idx, trans, is_update))) {
        LOG_WARN("fail to inner batch insert", KR(ret), K(tenant_id), K(start_idx), K(is_update));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, items.count());
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_insert_or_update_by_sql_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    const int64_t start_idx,
    const int64_t end_idx,
    ObISQLClient &sql_client,
    const bool is_update)
{
  int ret = OB_SUCCESS;
  const int64_t item_cnt = items.count();
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id))
      || (item_cnt <= 0)
      || (start_idx < 0)
      || (start_idx >= end_idx)
      || (end_idx > item_cnt))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "items count", item_cnt, 
      K(start_idx), K(end_idx));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, tablet_id, ls_id, svr_ip, svr_port, row_count, "
          "compaction_scn, data_checksum, column_checksums, b_column_checksums, gmt_modified, gmt_create) VALUES",
          OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    } else {
      ObArenaAllocator allocator;
      char *hex_buf = NULL;

      for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
        const ObTabletReplicaChecksumItem &cur_item = items.at(idx);
        const uint64_t compaction_scn_val = cur_item.compaction_scn_.get_val_for_inner_table_field();
        char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
        ObString visible_column_meta;
        ObString hex_column_meta;

        if (OB_UNLIKELY(!cur_item.server_.ip_to_string(ip, sizeof(ip)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to convert server ip to string", KR(ret), "server", cur_item.server_);
        } else if (OB_FAIL(get_visible_column_meta(cur_item.column_meta_, allocator, visible_column_meta))) {
          LOG_WARN("fail to get visible column meta str", KR(ret));
        } else if (OB_FAIL(get_hex_column_meta(cur_item.column_meta_, allocator, hex_column_meta))) {
          LOG_WARN("fail to get hex column meta str", KR(ret));
        } else if (OB_FAIL(sql.append_fmt("('%lu', '%lu', %ld, '%s', %d, %ld, %lu, %ld, "
                  "'%.*s', '%.*s', now(6), now(6))%s", cur_item.tenant_id_, cur_item.tablet_id_.id(),
                  cur_item.ls_id_.id(), ip, cur_item.server_.get_port(), cur_item.row_count_,
                  compaction_scn_val, cur_item.data_checksum_, visible_column_meta.length(),
                  visible_column_meta.ptr(), hex_column_meta.length(), hex_column_meta.ptr(),
                  ((idx == end_idx - 1) ? " " : ", ")))) {
          LOG_WARN("fail to assign sql", KR(ret), K(idx), K(end_idx), K(cur_item));
        }
      }

      if (is_update) {
        if (FAILEDx(sql.append_fmt(" ON DUPLICATE KEY UPDATE "))) {
          LOG_WARN("fail to append sql string", KR(ret), K(sql));
        } else if (OB_FAIL(sql.append(" row_count = values(row_count)"))
            || OB_FAIL(sql.append(", compaction_scn = values(compaction_scn)"))
            || OB_FAIL(sql.append(", data_checksum = values(data_checksum)"))
            || OB_FAIL(sql.append(", column_checksums = values(column_checksums)"))
            || OB_FAIL(sql.append(", b_column_checksums = values(b_column_checksums)"))) {
          LOG_WARN("fail to append sql string", KR(ret), K(sql));
        }
      }
    }
    
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (FAILEDx(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::check_tablet_replica_checksum(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &pairs,
    const SCN &compaction_scn,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || pairs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(pairs), K(compaction_scn));
  } else {
    SMART_VAR(ObArray<ObTabletReplicaChecksumItem>, items) {
      if (OB_FAIL(batch_get(tenant_id, pairs, compaction_scn, sql_proxy, items))) {
        LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K(tenant_id), K(compaction_scn));
      } else if (0 == items.count()) {
        ret = OB_ITEM_NOT_MATCH;
        LOG_WARN("fail to get tablet replica checksum items", KR(ret), K(tenant_id), K(compaction_scn), K(pairs));
      } else if (OB_FAIL(innner_verify_tablet_replica_checksum(items))) {
        LOG_WARN("fail to execute tablet replica checksum verification", KR(ret), K(items));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::innner_verify_tablet_replica_checksum(
    const ObIArray<ObTabletReplicaChecksumItem> &ckm_items)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  if (OB_UNLIKELY(ckm_items.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ckm_items));
  } else {
    const int64_t item_cnt = ckm_items.count();
    ObTabletReplicaChecksumItem prev_item;
    ObTabletReplicaChecksumItem curr_item;
    for (int64_t i = 0; OB_SUCC(ret) && (i < item_cnt); ++i) {
      curr_item.reset();
      if (OB_FAIL(curr_item.assign(ckm_items.at(i)))) {
        LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(i), "item", ckm_items.at(i));
      } else if (prev_item.is_key_valid()) {
        if (curr_item.is_same_tablet(prev_item)) { // same tablet
          if (OB_FAIL(curr_item.verify_checksum(prev_item))) {
            if (OB_CHECKSUM_ERROR == ret) {
              LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "checksum error in tablet replica checksum", KR(ret),
                K(curr_item), K(prev_item));
              check_ret = ret;
              ret = OB_SUCCESS; // continue checking next checksum
            } else {
              LOG_WARN("unexpected error in tablet replica checksum", KR(ret), K(curr_item), K(prev_item));
            }
          }
        } else if (OB_FAIL(prev_item.assign(curr_item))) { // next tablet
          LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(i), K(curr_item));
        }
      } else if (OB_FAIL(prev_item.assign(curr_item))) {
        LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(i), K(curr_item));
      }
    }
  }
  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = OB_CHECKSUM_ERROR;
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_index_and_data_table_schema(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const uint64_t index_table_id,
    const uint64_t data_table_id,
    const ObTableSchema *&index_table_schema,
    const ObTableSchema *&data_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(index_table_id));
  } else if (OB_ISNULL(index_table_schema)) {
    LOG_WARN("index_table_schema is null", K(tenant_id), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    LOG_WARN("data_table_schema is null", K(tenant_id), K(data_table_id));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::check_column_checksum(
    const uint64_t tenant_id,
    const ObSimpleTableSchemaV2 &data_simple_schema,
    const ObSimpleTableSchemaV2 &index_simple_schema,
    const SCN &compaction_scn,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t index_table_id = index_simple_schema.get_table_id();
  const uint64_t data_table_id = data_simple_schema.get_table_id();
  const ObTableSchema *index_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  // destruct the schema_guard after validating index column checksum to free memory
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !data_simple_schema.is_valid()
      || !index_simple_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(data_simple_schema), K(index_simple_schema));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_index_and_data_table_schema(schema_guard, tenant_id, index_table_id,
                     data_table_id, index_table_schema, data_table_schema))) {
    LOG_WARN("fail to get index and data table schema", KR(ret), K(index_simple_schema), K(data_simple_schema));
  } else if (OB_ISNULL(index_table_schema) || OB_ISNULL(data_table_schema)) {
    // table schemas are changed, and index_table or data_table does not exist in new table schemas.
    // no need to check index column checksum.
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), KP(index_table_schema), KP(data_table_schema),
             K(index_table_id), K(data_table_id));
  } else {
    const bool is_global_index = index_simple_schema.is_global_index_table();
    if (is_global_index) {
      if (OB_FAIL(check_global_index_column_checksum(tenant_id, *data_table_schema, *index_table_schema,
          compaction_scn, sql_proxy))) {
        LOG_WARN("fail to check global index column checksum", KR(ret), K(tenant_id), K(compaction_scn));
      }
    } else if (OB_UNLIKELY(index_simple_schema.is_spatial_index())) {
      // do nothing
      // spatial index column is different from data table column
    } else {
      if (OB_FAIL(check_local_index_column_checksum(tenant_id, *data_table_schema, *index_table_schema,
          compaction_scn, sql_proxy))) {
        LOG_WARN("fail to check local index column checksum", KR(ret), K(tenant_id), K(compaction_scn));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::check_global_index_column_checksum(
    const uint64_t tenant_id,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    const SCN &compaction_scn,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t default_column_cnt = ObTabletReplicaReportColumnMeta::DEFAULT_COLUMN_CNT;
  int64_t check_cnt = 0;
  bool need_verify = false;
  int64_t index_ckm_tablet_cnt = 0;
  int64_t data_ckm_tablet_cnt = 0;
  bool is_match = true;
  uint64_t index_table_id = UINT64_MAX;
  uint64_t data_table_id = UINT64_MAX;
  // map element: <column_id, checksum_sum>
  hash::ObHashMap<int64_t, int64_t> data_column_ckm_sum_map;
  hash::ObHashMap<int64_t, int64_t> index_column_ckm_sum_map;

  SMART_VARS_2((ObArray<ObTabletReplicaChecksumItem>, data_table_ckm_items),
               (ObArray<ObTabletReplicaChecksumItem>, index_table_ckm_items)) {
    SMART_VARS_2((ObArray<ObTabletLSPair>, data_table_tablets),
                 (ObArray<ObTabletLSPair>, index_table_tablets)) {
      if (OB_FAIL(data_column_ckm_sum_map.create(default_column_cnt, ObModIds::OB_CHECKSUM_CHECKER))) {
        LOG_WARN("fail to create data table column ckm_sum map", KR(ret), K(default_column_cnt));
      } else if (OB_FAIL(index_column_ckm_sum_map.create(default_column_cnt, ObModIds::OB_CHECKSUM_CHECKER))) {
        LOG_WARN("fail to create index table column ckm_sum map", KR(ret), K(default_column_cnt));
      } else {
        index_table_id = index_table_schema.get_table_id();
        data_table_id = data_table_schema.get_table_id();
        ObTabletID unused_tablet_id;
        ObColumnChecksumErrorInfo ckm_error_info(tenant_id, compaction_scn, true, data_table_id, index_table_id,
          unused_tablet_id, unused_tablet_id);

        if (OB_FAIL(get_tablet_replica_checksum_items_(tenant_id, sql_proxy, index_table_schema, compaction_scn,
            index_table_tablets, index_table_ckm_items))) {
          LOG_WARN("fail to get index table tablet replica ckm_items", KR(ret), K(tenant_id), K(compaction_scn),
            K(index_table_id));
        } else if (OB_FAIL(get_column_checksum_sum_map_(index_table_schema, compaction_scn,
            index_column_ckm_sum_map, index_table_ckm_items))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("fail to get index table column checksum_sum map", KR(ret), K(index_table_id), K(data_table_id),
              K(compaction_scn));
          } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("fail to get index table tablet checksum items", KR(ret), K(index_table_schema));
          }
        } else if (OB_FAIL(get_tablet_replica_checksum_items_(tenant_id, sql_proxy, data_table_schema, compaction_scn,
            data_table_tablets, data_table_ckm_items))) {
          LOG_WARN("fail to get data table tablet replica ckm_items", KR(ret), K(tenant_id), K(compaction_scn),
            K(data_table_id));
        } else if (OB_FAIL(get_column_checksum_sum_map_(data_table_schema, compaction_scn,
            data_column_ckm_sum_map, data_table_ckm_items))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("fail to get data table column checksum_sum map", KR(ret), K(data_table_id), K(index_table_id),
              K(compaction_scn));
          } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("fail to get data table tablet checksum items", KR(ret), K(data_table_schema));
          }
        } else if (need_verify_checksum_(compaction_scn, index_table_schema, index_table_ckm_items,
                                         need_verify, index_ckm_tablet_cnt)) {
          LOG_WARN("fail to check need verfy checksum", KR(ret), K(compaction_scn), K(index_table_id), K(data_table_id));
        } else if (!need_verify) {
          LOG_INFO("do not need verify checksum", K(index_table_id), K(data_table_id), K(compaction_scn));
        } else if (need_verify_checksum_(compaction_scn, data_table_schema, data_table_ckm_items,
                                         need_verify, data_ckm_tablet_cnt)) {
          LOG_WARN("fail to check need verfy checksum", KR(ret), K(compaction_scn), K(index_table_id), K(data_table_id));
        } else if (!need_verify) {
          LOG_INFO("do not need verify checksum", K(index_table_id), K(data_table_id), K(compaction_scn));
        } else if (OB_FAIL(compare_column_checksum_(data_table_schema, index_table_schema, data_column_ckm_sum_map,
            index_column_ckm_sum_map, check_cnt, ckm_error_info))) {
          if (OB_CHECKSUM_ERROR == ret) {
            LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "data table and global index table column checksum are not equal",
                          KR(ret), K(ckm_error_info), K(index_ckm_tablet_cnt), "index_schema_tablet_cnt",
                          index_table_tablets.count(), K(data_ckm_tablet_cnt), "data_schema_tablet_cnt",
                          data_table_tablets.count());
            if (OB_TMP_FAIL(ObColumnChecksumErrorOperator::insert_column_checksum_err_info(sql_proxy, tenant_id,
                ckm_error_info))) {
              LOG_WARN("fail to insert global index column checksum error info", KR(tmp_ret), K(ckm_error_info));
            }
          }
        }
      }
    } // end smart_var
  } // end smart_var

  ret = ((OB_SUCCESS == ret) ? tmp_ret : ret);

  if (data_column_ckm_sum_map.created()) {
    data_column_ckm_sum_map.destroy();
  }
  if (index_column_ckm_sum_map.created()) {
    index_column_ckm_sum_map.destroy();
  }
  LOG_INFO("finish verify global index table columns checksum", KR(ret), KR(tmp_ret), K(tenant_id), K(compaction_scn),
    K(data_table_id), K(index_table_id), K(check_cnt));

  return ret;
}

int ObTabletReplicaChecksumOperator::check_local_index_column_checksum(
    const uint64_t tenant_id,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    const SCN &compaction_scn,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_verify = false;
  int64_t index_ckm_tablet_cnt = 0;
  int64_t data_ckm_tablet_cnt = 0;
  bool is_match = true;
  const uint64_t index_table_id = index_table_schema.get_table_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const int64_t default_column_cnt = ObTabletReplicaReportColumnMeta::DEFAULT_COLUMN_CNT;
  int64_t check_cnt = 0;

  SMART_VARS_2((ObArray<ObTabletReplicaChecksumItem>, data_table_ckm_items),
               (ObArray<ObTabletReplicaChecksumItem>, index_table_ckm_items)) {
    SMART_VARS_4((ObArray<ObTabletLSPair>, data_table_tablets),
                 (ObArray<ObTabletLSPair>, index_table_tablets),
                 (ObArray<ObTabletID>, index_table_tablet_ids),
                 (ObArray<ObTabletID>, data_table_tablet_ids)) {
      if (OB_FAIL(get_tablet_replica_checksum_items_(tenant_id, sql_proxy, index_table_schema, compaction_scn,
          index_table_tablets, index_table_ckm_items))) {
        LOG_WARN("fail to get index table tablet replica ckm_items", KR(ret), K(tenant_id), K(compaction_scn),
          K(index_table_id));
      } else if (OB_FAIL(get_tablet_replica_checksum_items_(tenant_id, sql_proxy, data_table_schema, compaction_scn,
          data_table_tablets, data_table_ckm_items))) {
        LOG_WARN("fail to get data table tablet replica ckm_items", KR(ret), K(tenant_id), K(compaction_scn),
          K(data_table_id));
      } else if (OB_UNLIKELY(data_table_tablets.count() != index_table_tablets.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet count of local index table is not same with data table", KR(ret), "data_table_tablet_cnt",
          data_table_tablets.count(), "index_table_tablet_cnt", index_table_tablets.count());
      } else if (need_verify_checksum_(compaction_scn, index_table_schema, index_table_ckm_items,
                                       need_verify, index_ckm_tablet_cnt)) {
        LOG_WARN("fail to check need verfy checksum", KR(ret), K(compaction_scn), K(index_table_id), K(data_table_id));
      } else if (!need_verify) {
        LOG_INFO("do not need verify checksum", K(index_table_id), K(data_table_id), K(compaction_scn));
      } else if (need_verify_checksum_(compaction_scn, data_table_schema, data_table_ckm_items,
                                       need_verify, data_ckm_tablet_cnt)) {
        LOG_WARN("fail to check need verfy checksum", KR(ret), K(compaction_scn), K(index_table_id), K(data_table_id));
      } else if (!need_verify) {
        LOG_INFO("do not need verify checksum", K(index_table_id), K(data_table_id), K(compaction_scn));
      } else if (OB_FAIL(get_table_all_tablet_ids_(index_table_schema, index_table_tablet_ids))) {
        LOG_WARN("fail to get index table all tablet ids", KR(ret), K(index_table_schema));
      } else if (OB_FAIL(get_table_all_tablet_ids_(data_table_schema, data_table_tablet_ids))) {
        LOG_WARN("fail to get data table all tablet ids", KR(ret), K(data_table_schema));
      } else if (OB_UNLIKELY((data_table_tablet_ids.count() != index_table_tablet_ids.count())
                             || (data_table_tablets.count() != data_table_tablet_ids.count()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_ids count or tablet_ls_pair count", KR(ret), "data_table_tablet_id_cnt",
          data_table_tablet_ids.count(), "index_table_tablet_id_cnt", index_table_tablet_ids.count(),
          "data_table_tablet_ls_pair_count", data_table_tablets.count());
      } else {
        // map elemant: <tablet_id, tablet_ls_pair>
        hash::ObHashMap<ObTabletID, ObTabletLSPair> data_tablet_ls_pair_map;
        hash::ObHashMap<ObTabletID, ObTabletLSPair> index_tablet_ls_pair_map;
        // map element: <column_id, checksum>
        hash::ObHashMap<int64_t, int64_t> data_column_ckm_map;
        hash::ObHashMap<int64_t, int64_t> index_column_ckm_map;
        if (OB_FAIL(data_tablet_ls_pair_map.create(500, ObModIds::OB_CHECKSUM_CHECKER,
                                                   ObModIds::OB_CHECKSUM_CHECKER, tenant_id))) {
          LOG_WARN("fail to create data tablet ls pair map", KR(ret), K(default_column_cnt), K(tenant_id));
        } else if (OB_FAIL(index_tablet_ls_pair_map.create(500, ObModIds::OB_CHECKSUM_CHECKER,
                                                   ObModIds::OB_CHECKSUM_CHECKER, tenant_id))) {
          LOG_WARN("fail to create index tablet ls pair map", KR(ret), K(default_column_cnt), K(tenant_id));
        } else if (OB_FAIL(convert_array_to_map(data_table_tablets, data_tablet_ls_pair_map))) {
          LOG_WARN("fail to convert array to map", KR(ret));
        } else if (OB_FAIL(convert_array_to_map(index_table_tablets, index_tablet_ls_pair_map))) {
          LOG_WARN("fail to convert array to map", KR(ret));
        } else if (OB_FAIL(data_column_ckm_map.create(default_column_cnt,
                           ObModIds::OB_CHECKSUM_CHECKER, ObModIds::OB_CHECKSUM_CHECKER, tenant_id))) {
          LOG_WARN("fail to create data table column ckm map", KR(ret), K(default_column_cnt), K(tenant_id));
        } else if (OB_FAIL(index_column_ckm_map.create(default_column_cnt,
                           ObModIds::OB_CHECKSUM_CHECKER, ObModIds::OB_CHECKSUM_CHECKER, tenant_id))) {
          LOG_WARN("fail to create index table column ckm map", KR(ret), K(default_column_cnt), K(tenant_id));
        }

        // One tablet of local index table is mapping to one tablet of data table
        const int64_t tablet_cnt = data_table_tablets.count();
        for (int64_t i = 0; (i < tablet_cnt) && OB_SUCC(ret); ++i) {
          ObTabletID &tmp_data_tablet_id = data_table_tablet_ids.at(i);
          ObTabletID &tmp_index_tablet_id = index_table_tablet_ids.at(i);
          ObTabletLSPair data_tablet_pair;
          ObTabletLSPair index_tablet_pair;
          if (OB_FAIL(data_column_ckm_map.clear())) {
            LOG_WARN("fail to clear hash map", KR(ret), K(default_column_cnt));
          } else if (OB_FAIL(index_column_ckm_map.clear())) {
            LOG_WARN("fail to clear hash map", KR(ret), K(default_column_cnt));
          } else if (OB_FAIL(data_tablet_ls_pair_map.get_refactored(tmp_data_tablet_id, data_tablet_pair))) {
            LOG_WARN("fail to get refactored", KR(ret), K(tmp_data_tablet_id));
          } else if (OB_FAIL(index_tablet_ls_pair_map.get_refactored(tmp_index_tablet_id, index_tablet_pair))) {
            LOG_WARN("fail to get refactored", KR(ret), K(tmp_index_tablet_id));
          } else {
            int64_t data_tablet_idx = OB_INVALID_INDEX;
            int64_t index_tablet_idx = OB_INVALID_INDEX;
            if (OB_FAIL(find_checksum_item_(data_tablet_pair, data_table_ckm_items, compaction_scn, data_tablet_idx))) {
              LOG_WARN("fail to find checksum item by tablet_id", KR(ret), K(data_tablet_pair), K(compaction_scn));
            } else if (OB_FAIL(find_checksum_item_(index_tablet_pair, index_table_ckm_items, compaction_scn, index_tablet_idx))) {
              LOG_WARN("fail to find checksum item by tablet_id", KR(ret), K(index_tablet_pair), K(compaction_scn));
            } else {
              // compare column checksum of index schema tablet and data schema tablet
              check_cnt = 0;
              const ObTabletReplicaChecksumItem &data_ckm_item = data_table_ckm_items.at(data_tablet_idx);
              const ObTabletReplicaChecksumItem &index_ckm_item = index_table_ckm_items.at(index_tablet_idx);

              ObColumnChecksumErrorInfo ckm_error_info(tenant_id, compaction_scn, false, data_table_id, index_table_id,
                data_tablet_pair.get_tablet_id(), index_tablet_pair.get_tablet_id());

              if (OB_FAIL(get_column_checksum_map_(data_table_schema, compaction_scn,
                  data_column_ckm_map, data_ckm_item))) {
                LOG_WARN("fail to get column ckm map of one data table tablet", KR(ret), K(data_ckm_item));
              } else if (OB_FAIL(get_column_checksum_map_(index_table_schema, compaction_scn,
                  index_column_ckm_map, index_ckm_item))) {
                LOG_WARN("fail to get column ckm map of one index table tablet", KR(ret), K(index_ckm_item));
              } else if (OB_FAIL(compare_column_checksum_(data_table_schema, index_table_schema, data_column_ckm_map,
                  index_column_ckm_map, check_cnt, ckm_error_info))) {
                if (OB_CHECKSUM_ERROR == ret) {
                  LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "data table and local index table column checksum are not equal",
                                KR(ret), K(ckm_error_info), K(index_ckm_tablet_cnt), "index_schema_tablet_cnt",
                                index_table_tablets.count(), K(data_ckm_tablet_cnt), "data_schema_tablet_cnt",
                                data_table_tablets.count());
                  if (OB_TMP_FAIL(ObColumnChecksumErrorOperator::insert_column_checksum_err_info(sql_proxy, tenant_id,
                      ckm_error_info))) {
                    LOG_WARN("fail to insert local index column checksum error info", KR(tmp_ret), K(ckm_error_info));
                  }
                }
              }
            }
          }
        } // end loop

        ret = ((OB_SUCCESS == ret) ? tmp_ret : ret);

        if (data_column_ckm_map.created()) {
          data_column_ckm_map.destroy();
        }
        if (index_column_ckm_map.created()) {
          index_column_ckm_map.destroy();
        }
      }
      LOG_INFO("finish verify local index table columns checksum", KR(ret), KR(tmp_ret), K(tenant_id), K(compaction_scn),
        K(data_table_id), K(index_table_id), K(check_cnt), K(data_table_tablets.count()));
    }
  }

  return ret;
}

int ObTabletReplicaChecksumOperator::get_column_checksum_sum_map_(
    const ObTableSchema &table_schema,
    const SCN &compaction_scn,
    hash::ObHashMap<int64_t, int64_t> &column_ckm_sum_map,
    const ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_descs;
  if (items.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(items.count()));
  } else if (OB_FAIL(table_schema.get_multi_version_column_descs(column_descs))) {
    LOG_WARN("fail to get multi version column descs", KR(ret), K(table_schema));
  } else {
    const int64_t column_descs_cnt = column_descs.count();
    const int64_t items_cnt = items.count();
    const int64_t column_checksums_cnt = items.at(0).column_meta_.column_checksums_.count();

    if (column_checksums_cnt > column_descs_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema column id count must not less than column checksum count exclude hidden column",
        KR(ret), K(column_checksums_cnt), K(column_descs_cnt));
    }

    uint64_t pre_tablet_id = OB_INVALID_ID;
    // items are order by tablet_id
    for (int64_t i = 0; (i < items_cnt) && OB_SUCC(ret); ++i) {
      const ObTabletReplicaChecksumItem &cur_item = items.at(i);
      if (cur_item.compaction_scn_ == compaction_scn) {
        const ObTabletReplicaReportColumnMeta &cur_column_meta = cur_item.column_meta_;
        if (pre_tablet_id == OB_INVALID_ID) {
          for (int64_t j = 0; (j < column_checksums_cnt) && OB_SUCC(ret); ++j) {
            const int64_t cur_column_id = column_descs.at(j).col_id_;
            const int64_t cur_column_checksum = cur_column_meta.column_checksums_.at(j);
            if (OB_FAIL(column_ckm_sum_map.set_refactored(cur_column_id, cur_column_checksum))) {
              LOG_WARN("fail to set column ckm_sum to map", KR(ret), K(cur_column_id), 
                K(cur_column_checksum), K(column_checksums_cnt));
            } 
          }
        } else {
          if (cur_item.tablet_id_.id() != pre_tablet_id) { // start new tablet
            for (int64_t j = 0; (j < column_checksums_cnt) && OB_SUCC(ret); ++j) {
              const int64_t cur_column_id = column_descs.at(j).col_id_;
              const int64_t cur_column_checksum = cur_column_meta.column_checksums_.at(j);
              int64_t last_column_checksum_sum = 0;
              if (OB_FAIL(column_ckm_sum_map.get_refactored(cur_column_id, last_column_checksum_sum))) {
                LOG_WARN("fail to get column ckm_sum from map", KR(ret), K(cur_column_id));
              } else if (OB_FAIL(column_ckm_sum_map.set_refactored(cur_column_id, 
                  cur_column_checksum + last_column_checksum_sum, true))) {
                LOG_WARN("fail to set column ckm_sum to map", KR(ret), K(cur_column_id), K(cur_column_checksum),
                  "cur_column_ckm_sum", (cur_column_checksum + last_column_checksum_sum));
              } 
            }
          } 
        }
        pre_tablet_id = cur_item.tablet_id_.id();
      }
    } // end for loop
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_column_checksum_map_(
      const ObTableSchema &table_schema,
      const SCN &compaction_scn,
      hash::ObHashMap<int64_t, int64_t> &column_ckm_map,
      const ObTabletReplicaChecksumItem &item)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_descs;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else if (OB_FAIL(table_schema.get_multi_version_column_descs(column_descs))) {
    LOG_WARN("fail to get multi version column descs", KR(ret), K(table_schema));
  } else {
    const int64_t column_descs_cnt = column_descs.count();
    const int64_t column_checksums_cnt = item.column_meta_.column_checksums_.count();

    // TODO donglou, make sure whether they are equal or not;
    if (column_checksums_cnt > column_descs_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema column id count must not less than column checksum count exclude hidden column",
        KR(ret), K(column_checksums_cnt), K(column_descs_cnt));
    }

    if (item.compaction_scn_ == compaction_scn) {
      const ObTabletReplicaReportColumnMeta &column_meta = item.column_meta_;
      for (int64_t i = 0; (i < column_checksums_cnt) && OB_SUCC(ret); ++i) {
        const int64_t cur_column_id = column_descs.at(i).col_id_;
        const int64_t cur_column_checksum = column_meta.column_checksums_.at(i);
        if (OB_FAIL(column_ckm_map.set_refactored(cur_column_id, cur_column_checksum))) {
          LOG_WARN("fail to set column checksum to map", KR(ret), K(cur_column_id), 
            K(cur_column_checksum), K(column_checksums_cnt));
        } 
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("snapshot_version mismtach", KR(ret), K(item.compaction_scn_), K(compaction_scn));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::find_checksum_item_(
    const ObTabletLSPair &pair,
    ObIArray<ObTabletReplicaChecksumItem> &items,
    const SCN &compaction_scn,
    int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  const int64_t item_cnt = items.count();
  if (OB_UNLIKELY(!pair.is_valid() || (item_cnt < 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pair), K(item_cnt));
  } else {
    for (int64_t i = 0; i < item_cnt; ++i) {
      if ((items.at(i).tablet_id_ == pair.get_tablet_id())
          && (items.at(i).ls_id_ == pair.get_ls_id())
          && (items.at(i).compaction_scn_ == compaction_scn)) {
        idx = i;
        break;
      }
    }

    if (idx == OB_INVALID_INDEX) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(
    const uint64_t tenant_id,
    const ObSimpleTableSchemaV2 &simple_schema,
    ObMySQLProxy &sql_proxy,
    ObIArray<ObTabletLSPair> &pairs)
{
  int ret = OB_SUCCESS;
  if ((!is_valid_tenant_id(tenant_id)) || (!simple_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
      if (OB_FAIL(get_table_all_tablet_ids_(simple_schema, tablet_ids))) {
        LOG_WARN("fail to get table all tablet ids", KR(ret), K(simple_schema));
      } else if (tablet_ids.count() > 0) {
        const uint64_t table_id = simple_schema.get_table_id();
        if (OB_FAIL(get_tablet_ls_pairs(tenant_id, table_id, sql_proxy, tablet_ids, pairs))) {
          LOG_WARN("fail to get tablet_ls_pairs", KR(ret), K(tenant_id), K(table_id));
        }
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObMySQLProxy &sql_proxy,
    const ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObTabletLSPair> &pairs)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || (tablet_ids.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids.count()));
  } else {
    SMART_VAR(ObArray<ObLSID>, ls_ids) {
      // sys_table's tablet->ls relation won't be written into __all_tablet_to_ls
      if (is_sys_tenant(tenant_id) || is_sys_table(table_id)) {
        for (int64_t i = 0; (i < tablet_ids.count()) && OB_SUCC(ret); ++i) {
          ObLSID tmp_ls_id(ObLSID::SYS_LS_ID);
          if (OB_FAIL(ls_ids.push_back(tmp_ls_id))) {
            LOG_WARN("fail to push back ls_id", KR(ret), K(tenant_id), K(table_id));
          }
        }
        const int64_t ls_id_cnt = ls_ids.count();
        for (int64_t i = 0; (i < ls_id_cnt) && OB_SUCC(ret); ++i) {
          ObTabletLSPair cur_pair;
          const ObTabletID &cur_tablet_id = tablet_ids.at(i);
          const ObLSID &cur_ls_id = ls_ids.at(i);
          if (OB_FAIL(cur_pair.init(cur_tablet_id, cur_ls_id))) {
            LOG_WARN("fail to init tablet_ls_pair", KR(ret), K(i), K(cur_tablet_id), K(cur_ls_id));
          } else if (OB_FAIL(pairs.push_back(cur_pair))) {
            LOG_WARN("fail to push back pair", KR(ret), K(cur_pair));
          }
        }
      } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_tablet_ls_pairs(sql_proxy, tenant_id,
                                                                              tablet_ids, pairs))) {
        LOG_WARN("fail to batch get ls", KR(ret), K(tenant_id), K(tablet_ids));
      }
    }

    if (OB_FAIL(ret)){
    } else if (OB_UNLIKELY(pairs.count() != tablet_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some unexpected err about tablet_ls_pair count", KR(ret), "tablet_id_cnt",
               tablet_ids.count(), "pair_cnt", pairs.count());
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_tablet_replica_checksum_items_(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    const ObSimpleTableSchemaV2 &simple_schema,
    const SCN &compaction_scn,
    ObIArray<ObTabletLSPair> &tablet_pairs,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if ((!is_valid_tenant_id(tenant_id)) || (!simple_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const uint64_t table_id = simple_schema.get_table_id();
    if (OB_FAIL(get_tablet_ls_pairs(tenant_id, simple_schema, sql_proxy, tablet_pairs))) {
      LOG_WARN("fail to get tablet_ls_pairs", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_get(tenant_id, tablet_pairs, compaction_scn,
        sql_proxy, items, true/*include_larger_than*/))) {
      LOG_WARN("fail to batch get tablet checksum item", KR(ret), K(tenant_id), K(compaction_scn),
                "pairs_count", tablet_pairs.count());
    } else if (0 == items.count()) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("fail to get tablet replica checksum items", KR(ret), K(tenant_id), K(compaction_scn), K(items));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_table_all_tablet_ids_(
    const ObSimpleTableSchemaV2 &simple_schema,
    ObIArray<ObTabletID> &schema_tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!simple_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(simple_schema));
  } else {
    if (simple_schema.has_tablet()) {
      if (OB_FAIL(simple_schema.get_tablet_ids(schema_tablet_ids))) {
        LOG_WARN("fail to get tablet_ids from table schema", KR(ret), K(simple_schema));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::check_table_all_tablets_ckm_status_(
    const uint64_t tenant_id,
    ObIArray<ObTabletLSPair> &tablet_pairs,
    bool &exist_error_status)
{
  int ret = OB_SUCCESS;
  exist_error_status = false;
  if (tablet_pairs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObArray<ObTabletReplica::ScnStatus> status_arr;
    if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_unique_status(tenant_id, tablet_pairs, status_arr))) {
      LOG_WARN("fail to get unique scn status", KR(ret), K(tenant_id), "pair_cnt", tablet_pairs.count());
    } else if (status_arr.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get unique scn status", KR(ret), K(tenant_id), K(tablet_pairs));
    } else {
      for (int64_t i = 0; (i < status_arr.count()) && (!exist_error_status); ++i) {
        if (status_arr.at(i) == ObTabletReplica::ScnStatus::SCN_STATUS_ERROR) {
          exist_error_status = true;
        }
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::need_verify_checksum_(
    const SCN &compaction_scn,
    const ObSimpleTableSchemaV2 &simple_schema,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    bool &need_verify,
    int64_t &ckm_tablet_cnt)
{
  int ret = OB_SUCCESS;
  need_verify = false;
  hash::ObHashSet<uint64_t> tablet_id_set;  // record all tablet_ids in @items
  const int64_t item_cnt = items.count();
  SMART_VAR(ObArray<ObTabletID>, schema_tablet_ids) {
    if (item_cnt <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(item_cnt));
    } else if (OB_FAIL(get_table_all_tablet_ids_(simple_schema, schema_tablet_ids))) {
      LOG_WARN("fail to get table all tablet ids", KR(ret), K(simple_schema));
    } else if (OB_FAIL(tablet_id_set.create(item_cnt))) {
      LOG_WARN("fail to create tablet_id set", KR(ret), K(item_cnt));
    } else {
      SCN min_compaction_scn = SCN::max_scn();
      SCN max_compaction_scn = SCN::min_scn();
      // step 1. obtain min_compaction_scn/max_compaction_scn and tablet_ids of checksum_items
      for (int64_t i = 0; i < item_cnt && OB_SUCC(ret); ++i) {
        const SCN &cur_compaction_scn = items.at(i).compaction_scn_;
        if (cur_compaction_scn < min_compaction_scn) {
          min_compaction_scn = cur_compaction_scn;
        }
        if (cur_compaction_scn > max_compaction_scn) {
          max_compaction_scn = cur_compaction_scn;
        }
        const ObTabletID &cur_tablet_id = items.at(i).tablet_id_;
        if (OB_FAIL(tablet_id_set.set_refactored(cur_tablet_id.id()))) {
          LOG_WARN("fail to set refactored", KR(ret), K(cur_tablet_id));
        }
      }
      // step 2. check if tablet_cnt in table_schema is equal to tablet_cnt in checksum_items
      bool need_check = true; // record if need to perform the check in step 3 and step 4
      if (OB_SUCC(ret)) {
        ckm_tablet_cnt = tablet_id_set.size();
        if (schema_tablet_ids.count() != ckm_tablet_cnt) { // may be caused by truncate table
          need_check = false;
          need_verify = false;
          LOG_INFO("no need to verify checksum, cuz tablet_cnt in table_schema is not equal to "
            "tablet_cnt in checksum_items", "tablet_id_in_table_schema", schema_tablet_ids,
            "tablet_id_in_checksum_items", tablet_id_set);
        }
      }
      // step 3. check if each tablet in table_schema has tablet_replica_checksum_items
      FOREACH_CNT_X(tablet_id, schema_tablet_ids, (OB_SUCCESS == ret) && need_check) {
        if (OB_ISNULL(tablet_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet_id is null", KR(ret), K(schema_tablet_ids));
        } else if (OB_FAIL(tablet_id_set.exist_refactored(tablet_id->id()))) {
          if (OB_HASH_NOT_EXIST == ret) { // may be caused by truncate table
            need_check = false;
            need_verify = false;
            ret = OB_SUCCESS;
            LOG_INFO("no need to verify checksum, cuz tablet in table_schema has no checksum_item",
                  "table_id", simple_schema.get_table_id(), "tablet_id", tablet_id->id(), K(items));
          } else if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to check tablet_id exist", KR(ret), K(tablet_id));
          }
        }
      }
      // step 4. check if exists checksum_items with compaction_scn larger than compaction_scn of major merge
      if (OB_SUCC(ret) && need_check) {
        if ((min_compaction_scn == compaction_scn) && (max_compaction_scn == compaction_scn)) {
          need_verify = true;
        } else if ((min_compaction_scn >= compaction_scn) && (max_compaction_scn > compaction_scn)) {
          // This means another medium compaction is launched. Thus, no need to verify checksum.
          need_verify = false;
          LOG_INFO("no need to verify checksum, cuz max_compaction_scn of checksum_items is larger "
                   "than compaction_scn of major compaction", K(min_compaction_scn),
                   K(max_compaction_scn), K(compaction_scn));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected compaction_scn of tablet_replica_checksum_items", KR(ret),
                    K(min_compaction_scn), K(max_compaction_scn), K(compaction_scn));
        }
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::compare_column_checksum_(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    const hash::ObHashMap<int64_t, int64_t> &data_column_ckm_map,
    const hash::ObHashMap<int64_t, int64_t> &index_column_ckm_map,
    int64_t &check_cnt,
    ObColumnChecksumErrorInfo &ckm_error_info)
{
  int ret = OB_SUCCESS;
  check_cnt = 0;

  if ((data_column_ckm_map.size() < 1) || (index_column_ckm_map.size() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_column_ckm_map.size()), K(data_column_ckm_map.size()));
  }

  for (hash::ObHashMap<int64_t, int64_t>::const_iterator iter = index_column_ckm_map.begin();
       OB_SUCC(ret) && (iter != index_column_ckm_map.end()); ++iter) {
    if ((iter->first == OB_HIDDEN_TRANS_VERSION_COLUMN_ID) || (iter->first == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID)) {
      // there not exists a promise: these two hidden columns checksums in data table and index table are equal。
      // thus, we skip them
    } else {
      int64_t data_table_column_ckm = 0;
      // is_virtual_generated_column is only tag in data table
      const ObColumnSchemaV2 *column_schema = data_table_schema.get_column_schema(iter->first);
      if (NULL == column_schema) {
        column_schema = index_table_schema.get_column_schema(iter->first);
      }
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column schema must not be NULL", KR(ret));
      } else if ((column_schema->is_shadow_column()) || (!column_schema->is_column_stored_in_sstable())) {
        // shadow column only exists in index table; if not stored in sstable, means virtual column/ROWID fake column
        LOG_INFO("column do not need to compare checksum", K(iter->first), K(column_schema->is_shadow_column()));
      } else if (OB_FAIL(data_column_ckm_map.get_refactored(iter->first, data_table_column_ckm))) {
        LOG_WARN("fail to get column ckm_sum, cuz this column_id not exist in data_table", KR(ret), "column_id",
          iter->first, K(data_table_schema), K(index_table_schema));
      } else {
        ++check_cnt;
        if (data_table_column_ckm != iter->second) {
          ret = OB_CHECKSUM_ERROR;
          ckm_error_info.column_id_ = iter->first;
          ckm_error_info.index_column_checksum_ = iter->second;
          ckm_error_info.data_column_checksum_ = data_table_column_ckm; 
        }
      }
    }
  } // end loop
  return ret;
}

void ObTabletReplicaChecksumOperator::print_detail_tablet_replica_checksum(
    const ObIArray<ObTabletReplicaChecksumItem> &items)
{
  const int64_t item_cnt = items.count();
  for (int64_t i = 0; i < item_cnt; ++i) {
    const ObTabletReplicaChecksumItem &cur_item = items.at(i);
    FLOG_WARN_RET(OB_SUCCESS, "detail tablet replica checksum", K(i), K(cur_item));
  }
}

int ObTabletReplicaChecksumOperator::set_column_meta_with_hex_str(
    const common::ObString &hex_str,
    ObTabletReplicaReportColumnMeta &column_meta)
{
  int ret = OB_SUCCESS;
  const int64_t hex_str_len = hex_str.length();
  if (hex_str_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hex_str_len), K(hex_str));
  } else {
    const int64_t deserialize_size = ObTabletReplicaReportColumnMeta::MAX_OCCUPIED_BYTES;
    int64_t deserialize_pos = 0;
    char *deserialize_buf = NULL;
    ObArenaAllocator allocator;

    if (OB_ISNULL(deserialize_buf = static_cast<char *>(allocator.alloc(deserialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(deserialize_size));
    } else if (OB_FAIL(hex_to_cstr(hex_str.ptr(), hex_str_len, deserialize_buf, deserialize_size))) {
      LOG_WARN("fail to get cstr from hex", KR(ret), K(hex_str_len), K(deserialize_size));
    } else if (OB_FAIL(column_meta.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
      LOG_WARN("fail to deserialize from str to build column meta", KR(ret), "column_meta_str", hex_str.ptr());
    } else if (deserialize_pos > deserialize_size) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("deserialize size overflow", KR(ret), K(deserialize_pos), K(deserialize_size));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_visible_column_meta(
    const ObTabletReplicaReportColumnMeta &column_meta,
    common::ObIAllocator &allocator,
    common::ObString &column_meta_visible_str)
{
  int ret = OB_SUCCESS;
  char *column_meta_str = NULL;
  const int64_t length = column_meta.get_string_length() * 2;
  int64_t pos = 0;

  if (OB_UNLIKELY(!column_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column meta is not valid", KR(ret), K(column_meta));
  } else if (OB_UNLIKELY(length > OB_MAX_LONGTEXT_LENGTH + 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column meta too long", KR(ret), K(length), K(column_meta));
  } else if (OB_ISNULL(column_meta_str = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(length));
  } else if (FALSE_IT(pos = column_meta.get_string(column_meta_str, length))) {
    //nothing
  } else if (OB_UNLIKELY(pos >= length)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(pos), K(length));
  } else {
    column_meta_visible_str.assign(column_meta_str, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_hex_column_meta(
    const ObTabletReplicaReportColumnMeta &column_meta,
    common::ObIAllocator &allocator,
    common::ObString &column_meta_hex_str)
{
  int ret = OB_SUCCESS;
  char *serialize_buf = NULL;
  const int64_t serialize_size = column_meta.get_serialize_size();
  int64_t serialize_pos = 0;
  char *hex_buf = NULL;
  const int64_t hex_size = 2 * serialize_size;
  int64_t hex_pos = 0;
  if (OB_UNLIKELY(!column_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_meta is invlaid", KR(ret), K(column_meta));
  } else if (OB_UNLIKELY(hex_size > OB_MAX_LONGTEXT_LENGTH + 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("format str is too long", KR(ret), K(hex_size), K(column_meta));
  } else if (OB_ISNULL(serialize_buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(serialize_size));
  } else if (OB_FAIL(column_meta.serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("failed to serialize column meta", KR(ret), K(column_meta), K(serialize_size), K(serialize_pos));
  } else if (OB_UNLIKELY(serialize_pos > serialize_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize error", KR(ret), K(serialize_pos), K(serialize_size));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator.alloc(hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(hex_size));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, hex_buf, hex_size, hex_pos))) {
    LOG_WARN("fail to print hex", KR(ret), K(serialize_pos), K(hex_size), K(serialize_buf));
  } else if (OB_UNLIKELY(hex_pos > hex_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("encode error", KR(ret), K(hex_pos), K(hex_size));
  } else {
    column_meta_hex_str.assign_ptr(hex_buf, static_cast<int32_t>(hex_size));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::convert_array_to_map(
    const ObArray<ObTabletLSPair> &tablet_ls_pairs,
    hash::ObHashMap<ObTabletID, ObTabletLSPair> &tablet_ls_pair_map)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_ls_pair_cnt = tablet_ls_pairs.count();
  for (int64_t i = 0; (i < tablet_ls_pair_cnt) && OB_SUCC(ret); ++i) {
    const ObTabletLSPair &pair = tablet_ls_pairs.at(i);
    const ObTabletID &tablet_id = pair.get_tablet_id();
    if (OB_FAIL(tablet_ls_pair_map.set_refactored(tablet_id, pair, false/*overwrite*/))) {
      LOG_WARN("fail to set_refactored", KR(ret), K(tablet_id), K(pair));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::is_higher_ver_tablet_rep_ckm_exist(
    const uint64_t tenant_id,
    const SCN &compaction_scn,
    const uint64_t tablet_id,
    common::ObISQLClient &sql_proxy,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !compaction_scn.is_valid() || (tablet_id <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(compaction_scn), K(tablet_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      uint64_t compaction_scn_val = compaction_scn.get_val_for_inner_table_field();
      if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS cnt FROM %s WHERE tenant_id = '%lu' AND "
            "tablet_id = %lu AND compaction_scn > %lu", OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME,
            tenant_id, tablet_id, compaction_scn_val))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(meta_tenant_id), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(meta_tenant_id), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", KR(ret), K(meta_tenant_id), K(tenant_id), K(sql));
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
            LOG_WARN("unexpected count", KR(ret), K(meta_tenant_id), K(tenant_id), K(sql), K(cnt));
          }
        }
      }
    }
  }
  return ret;
}

} // share
} // oceanbase
