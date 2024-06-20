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
#include "share/schema/ob_column_schema.h"
#include "observer/ob_server_struct.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/config/ob_server_config.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

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
    if (OB_FAIL(set_tenant_id(other.tenant_id_))) {
      LOG_WARN("failed to set tenant id", KR(ret), K(other));
    } else if (OB_FAIL(column_meta_.assign(other.column_meta_))) {
      LOG_WARN("fail to assign column meta", KR(ret), K(other));
    } else {
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

int ObTabletReplicaChecksumItem::set_tenant_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    column_meta_.column_checksums_.set_attr(ObMemAttr(tenant_id, "RepCkmItem"));
  }
  return ret;
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

int ObTabletReplicaChecksumOperator::get_tablets_replica_checksum(
    const uint64_t tenant_id,
    const ObIArray<compaction::ObTabletCheckInfo> &pairs,
    ObIArray<ObTabletReplicaChecksumItem> &tablet_replica_checksum_items)
{
  int ret = OB_SUCCESS;
  int64_t tablet_items_cnt = 0;
  tablet_replica_checksum_items.reuse();
  const int64_t pairs_cnt = pairs.count();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || pairs_cnt <= 0 || NULL == GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(pairs), K(GCTX.sql_proxy_));
  } else if (OB_FAIL(tablet_replica_checksum_items.reserve(pairs.count()))) {
    LOG_WARN("failed to reserve array", K(ret), K(pairs.count()));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, pairs_cnt);
    ObSqlString sql;
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      int64_t tmp_tablet_items_cnt = 0;
      sql.reuse();
      if (OB_FAIL(construct_batch_get_sql_str_(tenant_id, SCN(), pairs, start_idx, end_idx, sql,
                                              false/*include_larger_than*/, false/*with_compaction_scn*/,
                                              false/*with_order_by_field*/))) {
        LOG_WARN("fail to construct batch get sql", KR(ret), K(tenant_id), K(pairs),
          K(start_idx), K(end_idx));
      } else if (OB_FAIL(inner_batch_get_by_sql_(
                     tenant_id, sql, share::OBCG_DEFAULT /*group_id*/,
                     *GCTX.sql_proxy_, tablet_replica_checksum_items,
                     tmp_tablet_items_cnt))) {
        LOG_WARN("fail to inner batch get by sql", KR(ret), K(tenant_id), K(sql));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_cnt);
        tablet_items_cnt += tmp_tablet_items_cnt;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("success to get tablet replica checksum items", KR(ret), K(pairs_cnt), K(tablet_items_cnt));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::batch_get(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &pairs,
    const SCN &compaction_scn,
    ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items,
    int64_t &tablet_items_cnt,
    const bool include_larger_than,
    const int32_t group_id,
    const bool with_order_by_field)
{
  int ret = OB_SUCCESS;
  items.reset();
  tablet_items_cnt = 0;
  const int64_t pairs_cnt = pairs.count();
  if (OB_UNLIKELY(pairs_cnt < 1 || OB_INVALID_TENANT_ID == tenant_id || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pairs_cnt), K(group_id));
  }
  // Step 2: cut tablet replica checksum items into small batches
  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, pairs_cnt);
  ObSqlString sql;
  while (OB_SUCC(ret) && (start_idx < end_idx)) {
    sql.reuse();
    int64_t tmp_tablet_items_cnt = 0;
    if (OB_FAIL(construct_batch_get_sql_str_(tenant_id, compaction_scn, pairs, start_idx, end_idx,
                                             sql, include_larger_than, true/*with_compaction_scn*/,
                                             with_order_by_field))) {
      LOG_WARN("fail to construct batch get sql", KR(ret), K(tenant_id), K(compaction_scn), K(pairs),
        K(start_idx), K(end_idx));
    } else if (OB_FAIL(inner_batch_get_by_sql_(tenant_id, sql, group_id, sql_proxy, items, tmp_tablet_items_cnt))) {
      LOG_WARN("fail to inner batch get by sql", KR(ret), K(tenant_id), K(sql));
    } else {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_cnt);
      tablet_items_cnt += tmp_tablet_items_cnt;
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && pairs.at(0).get_tablet_id().id() > ObTabletID::MIN_USER_TABLET_ID) {
    ret = OB_E(EventTable::EN_RS_CANT_GET_ALL_TABLET_CHECKSUM) ret;
    if (OB_FAIL(ret)) {
      tablet_items_cnt--;
      LOG_INFO("ERRSIM EN_RS_CANT_GET_ALL_TABLET_CHECKSUM", K(ret), K(tablet_items_cnt), K(pairs.count()), K(pairs.at(0)));
    }
  }
#endif
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_get_by_sql_(
    const uint64_t tenant_id,
    const ObSqlString &sql,
    const int32_t group_id,
    ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items,
    int64_t &tablet_items_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(group_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy.read(result, meta_tenant_id, sql.ptr(), group_id))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(construct_tablet_replica_checksum_items_(*result.get_result(), items, tablet_items_cnt))) {
        LOG_WARN("fail to construct tablet checksum items", KR(ret), K(items));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::construct_tablet_replica_checksum_items_(
    sqlclient::ObMySQLResult &res,
    ObIArray<ObTabletReplicaChecksumItem> &items,
    int64_t &tablet_items_cnt)
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
      } else if (items.empty()
        || item.tablet_id_ != items.at(items.count() - 1).tablet_id_) {
        ++tablet_items_cnt;
      }
      if (FAILEDx(items.push_back(item))) {
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
  } else if (OB_FAIL(item.set_tenant_id((uint64_t)int_tenant_id))) {
    LOG_WARN("failed to set tenant id", KR(ret), K(int_tenant_id));
  } else {
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

int ObTabletReplicaChecksumOperator::get_tablet_replica_checksum_items(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    const SCN &compaction_scn,
    const ObIArray<ObTabletLSPair> &tablet_pairs,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    int64_t tablet_items_cnt = 0;
    if (OB_FAIL(batch_get(tenant_id, tablet_pairs, compaction_scn,
        sql_proxy, items, tablet_items_cnt, false/*include_larger_than*/,
        share::OBCG_DEFAULT, true/*with_order_by_field*/))) {
      LOG_WARN("fail to batch get tablet checksum item", KR(ret), K(tenant_id), K(compaction_scn),
                "pairs_count", tablet_pairs.count());
    } else if (tablet_items_cnt < tablet_pairs.count()) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("fail to get tablet replica checksum items", KR(ret), K(tenant_id), K(compaction_scn),
        K(tablet_items_cnt), K(items));
    }
  }
  return ret;
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

} // share
} // oceanbase
