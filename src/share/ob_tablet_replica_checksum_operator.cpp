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
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/compaction/ob_shared_storage_compaction_util.h"
#endif
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
    column_meta_(),
    data_checksum_type_(ObDataChecksumType::DATA_CHECKSUM_MAX)
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
  data_checksum_type_ = ObDataChecksumType::DATA_CHECKSUM_MAX;
}

bool ObTabletReplicaChecksumItem::is_key_valid() const
{
#ifdef OB_BUILD_SHARED_STORAGE
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && tablet_id_.is_valid_with_tenant(tenant_id_);
#else
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && tablet_id_.is_valid_with_tenant(tenant_id_)
      && server_.is_valid();
#endif
}

bool ObTabletReplicaChecksumItem::is_valid() const
{
  return is_key_valid()
       && column_meta_.is_valid()
       && is_valid_data_checksum_type(data_checksum_type_);
}

bool ObTabletReplicaChecksumItem::is_same_tablet(const ObTabletReplicaChecksumItem &other) const
{
  return is_key_valid()
      && other.is_key_valid()
      && tenant_id_ == other.tenant_id_
      && ls_id_ == other.ls_id_
      && tablet_id_ == other.tablet_id_;
}

int ObTabletReplicaChecksumItem::check_data_checksum_type(bool &is_cs_replica) const
{
  int ret = OB_SUCCESS;
  is_cs_replica = false;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid checksum item", K(ret), KPC(this));
  } else if (ObDataChecksumType::DATA_CHECKSUM_COLUMN_STORE == data_checksum_type_) {
    is_cs_replica = true;
  }
  return ret;
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

int ObTabletReplicaChecksumItem::verify_column_checksum(const ObTabletReplicaChecksumItem &other) const
{
  int ret = OB_SUCCESS;
  bool column_meta_equal = false;
  bool is_cs_replica_flag1 = false;
  bool is_cs_replica_flag2 = false;
  if (OB_UNLIKELY(compaction_scn_ != other.compaction_scn_)) {
    // do nothing
  } else if (OB_FAIL(column_meta_.check_equal(other.column_meta_, column_meta_equal))) {
    LOG_WARN("fail to check column meta equal", KR(ret), K(other), K(*this));
  } else if (column_meta_equal) {
    // do nothing
  } else if (OB_FAIL(check_data_checksum_type(is_cs_replica_flag1))) {
    LOG_WARN("fail to check data checksum type", KR(ret), KPC(this));
  } else if (OB_FAIL(other.check_data_checksum_type(is_cs_replica_flag2))) {
    LOG_WARN("fail to check data checksum type", KR(ret), K(other));
  } else if (is_cs_replica_flag1 == is_cs_replica_flag2) {
    ret = OB_CHECKSUM_ERROR; // compaction between the same replica type can be compared
  } else if (OB_FAIL(verify_column_checksum_between_diffrent_replica(other))) {
    LOG_WARN("fail to verify column checksum between diffrent replica", KR(ret), K(other), K(*this));
  }
  return ret;
}

int ObTabletReplicaChecksumItem::verify_column_checksum_between_diffrent_replica(const ObTabletReplicaChecksumItem &other) const
{
  int ret = OB_SUCCESS;
  ObFreezeInfo boundary_freeze_info;
  ObFreezeInfo to_check_freeze_info;
  if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(compaction_scn_.get_val_for_tx(), boundary_freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get boundary freeze info", K(ret), K_(compaction_scn));
    }
  } else if (boundary_freeze_info.is_valid() && boundary_freeze_info.data_version_ >= DATA_VERSION_4_3_5_0) {
    ret = OB_CHECKSUM_ERROR; // it is compacted in lob column checksum fixed version
    LOG_WARN("failed to check column checksum", K(ret), K(boundary_freeze_info));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_by_snapshot_version(compaction_scn_.get_val_for_tx(), to_check_freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get freeze info", K(ret), K_(compaction_scn));
    }
  } else if (!to_check_freeze_info.is_valid()) {
  } else if (to_check_freeze_info.data_version_ >= DATA_VERSION_4_3_5_0) {
    ret = OB_CHECKSUM_ERROR; // it is compacted in lob column checksum fixed version
    LOG_WARN("failed to check column checksum", K(ret), K(to_check_freeze_info));
  } else {
    // only freeze_info of this round compaction reserved can we get the correct table schema to check lob column
    bool is_all_large_text_column = false;
    ObSEArray<int64_t, 8> column_idxs;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < column_meta_.column_checksums_.count(); ++idx) {
      if (column_meta_.column_checksums_.at(idx) == other.column_meta_.column_checksums_.at(idx)) {
        // do nothing
      } else if (OB_FAIL(column_idxs.push_back(idx))) {
        LOG_WARN("failed to add column idx", K(ret), K(idx));
      }
    }

    if (FAILEDx(compaction::ObCSReplicaChecksumHelper::check_column_type(tablet_id_,
                                                                         to_check_freeze_info,
                                                                         column_idxs,
                                                                         is_all_large_text_column))) {
      LOG_WARN("failed to check column type for cs replica", K(ret), KPC(this), K(other));
    } else if (is_all_large_text_column) {
      // do nothing
    } else {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("failed to check column checksum", K(ret), K(to_check_freeze_info));
    }
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
      data_checksum_type_ = other.data_checksum_type_;
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
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || (tablet_replicas.count() <= 0)
      || (start_idx < 0)
      || (start_idx > end_idx)
      || (end_idx > tablet_replicas.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(start_idx), K(end_idx),
             "tablet_replica cnt", tablet_replicas.count());
  } else if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND (tablet_id, ls_id) IN(",
                OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    } else {
      for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
        if (OB_FAIL(sql.append_fmt("('%lu', %ld)%s",
                                   tablet_replicas.at(idx).get_tablet_id().id(),
                                   tablet_replicas.at(idx).get_ls_id().id(),
                                   ((idx == end_idx - 1) ? ")" : ", ")))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(idx), K(start_idx), K(end_idx));
        }
      }
    }
#endif
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND (tablet_id, svr_ip, svr_port, ls_id) IN(",
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
  }

  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (FAILEDx(trans.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(meta_tenant_id), K(sql));
  } else {
    LOG_INFO("will batch delete tablet replica checksum", K(affected_rows));
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
    ObReplicaCkmArray &tablet_replica_checksum_items)
{
  int ret = OB_SUCCESS;
  const int64_t pairs_cnt = pairs.count();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || pairs_cnt <= 0 || NULL == GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(pairs), K(GCTX.sql_proxy_));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, pairs_cnt);
    ObSqlString sql;
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      sql.reuse();
      if (OB_FAIL(construct_batch_get_sql_str_(tenant_id, SCN(), pairs, start_idx, end_idx, sql,
                                              false/*include_larger_than*/, false/*with_compaction_scn*/))) {
        LOG_WARN("fail to construct batch get sql", KR(ret), K(tenant_id), K(pairs),
          K(start_idx), K(end_idx));
      } else if (OB_FAIL(inner_batch_get_by_sql_(
                     tenant_id, sql, share::OBCG_DEFAULT /*group_id*/,
                     *GCTX.sql_proxy_, tablet_replica_checksum_items))) {
        LOG_WARN("fail to inner batch get by sql", KR(ret), K(tenant_id), K(sql));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_cnt);
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("success to get tablet replica checksum items", KR(ret), K(pairs_cnt));
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::batch_get(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &pairs,
    const SCN &compaction_scn,
    ObISQLClient &sql_proxy,
    ObReplicaCkmArray &items,
    const bool include_larger_than,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  items.reset();
  const int64_t pairs_cnt = pairs.count();
  if (OB_UNLIKELY(pairs_cnt < 1 || OB_INVALID_TENANT_ID == tenant_id || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pairs_cnt), K(group_id));
  } else if (OB_FAIL(items.reserve(pairs_cnt))) {
    LOG_WARN("failed to reserve ckm array", KR(ret), K(tenant_id), K(pairs_cnt), K(group_id));
  }
  // Step 2: cut tablet replica checksum items into small batches
  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, pairs_cnt);
  ObSqlString sql;
  while (OB_SUCC(ret) && (start_idx < end_idx)) {
    sql.reuse();
    if (OB_FAIL(construct_batch_get_sql_str_(tenant_id, compaction_scn, pairs, start_idx, end_idx,
                                             sql, include_larger_than, true/*with_compaction_scn*/))) {
      LOG_WARN("fail to construct batch get sql", KR(ret), K(tenant_id), K(compaction_scn), K(pairs),
        K(start_idx), K(end_idx));
    } else if (OB_FAIL(inner_batch_get_by_sql_(tenant_id, sql, group_id, sql_proxy, items))) {
      LOG_WARN("fail to inner batch get by sql", KR(ret), K(tenant_id), K(sql));
    } else {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_cnt);
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_get_by_sql_(
    const uint64_t tenant_id,
    const ObSqlString &sql,
    const int32_t group_id,
    ObISQLClient &sql_proxy,
    ObReplicaCkmArray &items)
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
      } else if (OB_FAIL(construct_tablet_replica_checksum_items_(*result.get_result(), items))) {
        LOG_WARN("fail to construct tablet checksum items", KR(ret), K(items));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::construct_tablet_replica_checksum_items_(
    sqlclient::ObMySQLResult &res,
    ObReplicaCkmArray &items)
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
#ifdef ERRSIM
      } else if (item.get_tablet_id().id() > ObTabletID::MIN_USER_TABLET_ID) {
          ret = OB_E(EventTable::EN_RS_CANT_GET_ALL_TABLET_CHECKSUM) ret;
          if (OB_FAIL(ret)) { // skip push item
            LOG_INFO("ERRSIM EN_RS_CANT_GET_ALL_TABLET_CHECKSUM", K(ret), K(items), K(item));
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("fail to push back checksum item", KR(ret), K(item));
          }
#endif
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

int ObTabletReplicaChecksumOperator::construct_tablet_replica_checksum_items_(
    sqlclient::ObMySQLResult &res,
    common::ObIArray<ObTabletReplicaChecksumItem> &items,
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
  uint64_t min_data_version = 0;
  const int64_t tenant_id = MTL_ID();
  int64_t data_checksum_type = 0;
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

  if (FAILEDx(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret), K(tenant_id));
  } else if (min_data_version < DATA_VERSION_4_3_3_0) {
    item.data_checksum_type_ = ObDataChecksumType::DATA_CHECKSUM_NORMAL;
  } else {
    (void)GET_COL_IGNORE_NULL(res.get_int, "data_checksum_type", data_checksum_type);
    if (is_valid_data_checksum_type(data_checksum_type)) {
      item.data_checksum_type_ = static_cast<ObDataChecksumType>(data_checksum_type);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data checksum type", KR(ret), K(data_checksum_type));
    }
  }

  if (FAILEDx(item.compaction_scn_.convert_for_inner_table_field(compaction_scn_val))) {
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
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((OB_INVALID_TENANT_ID == tenant_id) || (items.count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "items count", items.count());
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, items.count());
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_insert_or_update_by_sql_(tenant_id, items, start_idx,
          end_idx, trans))) {
        LOG_WARN("fail to inner batch insert", KR(ret), K(tenant_id), K(start_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, items.count());
      }
    } // while
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_insert_or_update_by_sql_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    const int64_t start_idx,
    const int64_t end_idx,
    ObISQLClient &sql_client)
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
    ObDMLSqlSplicer dml_splicer;
    uint64_t min_data_version = 0;

    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret), K(tenant_id));
    } else {
      ObArenaAllocator allocator;
      char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
      int port = 0;
      for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
        const ObTabletReplicaChecksumItem &cur_item = items.at(idx);
        const uint64_t compaction_scn_val = cur_item.compaction_scn_.get_val_for_inner_table_field();
        if (OB_UNLIKELY(!cur_item.server_.ip_to_string(ip, sizeof(ip)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to convert server ip to string", KR(ret), "server", cur_item.server_);
        }
        port = cur_item.server_.get_port();
        ObString visible_column_meta;
        ObString hex_column_meta;

        if (FAILEDx(get_visible_column_meta(cur_item.column_meta_, allocator, visible_column_meta))) {
          LOG_WARN("fail to get visible column meta str", KR(ret));
        } else if (OB_FAIL(get_hex_column_meta(cur_item.column_meta_, allocator, hex_column_meta))) {
          LOG_WARN("fail to get hex column meta str", KR(ret));
        } else if (OB_FAIL(dml_splicer.add_gmt_modified())
                || OB_FAIL(dml_splicer.add_gmt_create())
                || OB_FAIL(dml_splicer.add_pk_column("tenant_id", cur_item.tenant_id_))
                || OB_FAIL(dml_splicer.add_pk_column("tablet_id", cur_item.tablet_id_.id()))
                || OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip))
                || OB_FAIL(dml_splicer.add_pk_column("svr_port", port))
                || OB_FAIL(dml_splicer.add_pk_column("ls_id", cur_item.ls_id_.id()))
                || OB_FAIL(dml_splicer.add_column("compaction_scn", compaction_scn_val))
                || OB_FAIL(dml_splicer.add_column("row_count", cur_item.row_count_))
                || OB_FAIL(dml_splicer.add_column("data_checksum", cur_item.data_checksum_))
                || OB_FAIL(dml_splicer.add_column("column_checksums", visible_column_meta))
                || OB_FAIL(dml_splicer.add_column("b_column_checksums", hex_column_meta))
                || (min_data_version >= DATA_VERSION_4_3_3_0 && OB_FAIL(dml_splicer.add_column("data_checksum_type", cur_item.data_checksum_type_)))
                ) {
          LOG_WARN("fail to fill dml splicer", KR(ret), K(min_data_version), K(idx), K(end_idx), K(cur_item));
        } else if (OB_FAIL(dml_splicer.finish_row())) {
          LOG_WARN("fail to finish row", KR(ret), K(idx), K(end_idx), K(cur_item));
        }
      }

      if (FAILEDx(dml_splicer.splice_batch_insert_update_sql(OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, sql))) {
        LOG_WARN("fail to splice batch insert update sql", KR(ret), K(sql));
      }
    }

    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (FAILEDx(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE

int ObTabletReplicaChecksumOperator::gene_select_sql_(
  const uint64_t tenant_id,
      const ObIArray<ObTabletReplicaChecksumItem> &items,
      const int64_t start_idx,
      const int64_t end_idx,
      ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObSqlString tablet_sql;
  for (int64_t idx = 0; OB_SUCC(ret) && (idx < items.count()); ++idx) {
    const ObTabletID &tablet_id = items.at(idx).tablet_id_;
    if (OB_FAIL(tablet_sql.append_fmt("%s %ld", 0 == idx ? "" : ",", tablet_id.id()))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
    }
  } // end for
  if (FAILEDx(sql.assign_fmt(
          "SELECT tenant_id, tablet_id, ls_id, compaction_scn FROM %s WHERE "
          "tenant_id=%lu AND svr_ip = '%s' AND svr_port = %d AND tablet_id in (%s) FOR UPDATE",
          OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id, compaction::SHARED_STORAGE_REPLICA_CKM_SVR_IP,
          compaction::SHARED_STORAGE_REPLICA_CKM_SVR_PORT,
          tablet_sql.ptr()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::build_ckm_info_map_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    const int64_t start_idx,
    const int64_t end_idx,
    ObISQLClient &sql_client,
    ObTabletSimpleCkmInfoMap &map)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult *res = NULL;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_FAIL(gene_select_sql_(tenant_id, items, start_idx, end_idx, sql))) {
    LOG_WARN("failed to gene select sql", KR(ret), K(tenant_id), K(items), K(start_idx), K(end_idx));
  }
  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    if (FAILEDx(sql_client.read(result, meta_tenant_id, sql.ptr(), share::OBCG_DEFAULT))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), "sql", sql.ptr());
    } else if (OB_ISNULL(res = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), "sql", sql.ptr());
    } else {
      ObSimpleCkmInfo info;
      int64_t int_tablet_id  = 0;
      int64_t int_ls_id  = 0;
      uint64_t compaction_scn_val = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(res->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", KR(ret));
          }
        } else {
          info.reset();
          (void)GET_COL_IGNORE_NULL(res->get_int, "tablet_id", int_tablet_id);
          (void)GET_COL_IGNORE_NULL(res->get_int, "ls_id", int_ls_id);
          (void)GET_COL_IGNORE_NULL(res->get_uint, "compaction_scn", compaction_scn_val);
          info.ls_id_ = ObLSID(int_ls_id);
          info.compaction_scn_ = int64_t(compaction_scn_val);
          if (FAILEDx(map.set_refactored(ObTabletID(int_tablet_id), info))) {
            LOG_WARN("fail to push back checksum item", KR(ret), K(int_tablet_id), K(info));
          }
        }
      } // end of while
      ret = (OB_ITER_END == ret ? OB_SUCCESS : ret);
    }
  }
  LOG_TRACE("build ckm info map", KR(ret), K(map.size()));
  return ret;
}

int ObTabletReplicaChecksumOperator::batch_select_and_update_with_trans(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((OB_INVALID_TENANT_ID == tenant_id) || (items.count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "items count", items.count());
  } else {
    ObTabletSimpleCkmInfoMap map;
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, items.count());
    if (OB_FAIL(map.create(MAX_BATCH_COUNT, "TabletUpdMap",
                           ObModIds::OB_HASH_NODE, tenant_id))) {
      LOG_WARN("fail to create replica map", KR(ret));
    }
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
        LOG_WARN("fail to start transaction", KR(ret), K(tenant_id));
      } else if (OB_FAIL(inner_batch_select_and_update_by_sql_(tenant_id, items, start_idx,
          end_idx, trans, map))) {
        LOG_WARN("fail to inner batch insert", KR(ret), K(tenant_id), K(start_idx), K(map.size()));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, items.count());
        map.reuse(); // clear map before next batch
      }
      if (trans.is_started()) {
        int trans_ret = trans.end(OB_SUCCESS == ret);
        if (OB_SUCCESS != trans_ret) {
          LOG_WARN("fail to end transaction", KR(trans_ret));
          ret = ((OB_SUCCESS == ret) ? trans_ret : ret);
        }
      }
    } // while
  }
  return ret;
}

enum OpType
{
  OP_INSERT = 0,
  OP_UPDATE,
  OP_MAX
};

int ObTabletReplicaChecksumOperator::update_with_map_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    const int64_t start_idx,
    const int64_t end_idx,
    ObISQLClient &sql_client,
    ObTabletSimpleCkmInfoMap &map)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSimpleCkmInfo info;
  OpType op_type = OP_MAX;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = -1;
  ObSqlString sql;
  ObArenaAllocator allocator;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  ObString visible_column_meta;
  ObString hex_column_meta;
  for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
    const ObTabletReplicaChecksumItem &item = items.at(idx);
    op_type = OP_MAX;
    sql.reuse();
    dml.reuse();
    if (OB_TMP_FAIL(map.get_refactored(item.tablet_id_, info))) {
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        op_type = OP_INSERT;
      } else {
        ret = tmp_ret;
        LOG_WARN("failed to get from map", KR(ret), K(item));
      }
    } else if (info.ls_id_ != item.ls_id_ || info.compaction_scn_ < item.compaction_scn_.get_val_for_tx()) {
      op_type = OP_UPDATE;
    }
    if (OB_FAIL(ret) || OP_MAX == op_type) {
    } else if (OB_TMP_FAIL(get_visible_column_meta(item.column_meta_, allocator, visible_column_meta))) {
      LOG_WARN("fail to get visible column meta str", KR(tmp_ret));
    } else if (OB_TMP_FAIL(get_hex_column_meta(item.column_meta_, allocator, hex_column_meta))) {
      LOG_WARN("fail to get hex column meta str", KR(tmp_ret));
    } else if (OB_TMP_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
      LOG_WARN("failed to add pk column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_pk_column("tablet_id", item.tablet_id_.id()))) {
      LOG_WARN("failed to add pk column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_pk_column("ls_id", item.ls_id_.id()))) {
      LOG_WARN("failed to add pk column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_pk_column("svr_ip", compaction::SHARED_STORAGE_REPLICA_CKM_SVR_IP))) {
      LOG_WARN("failed to add pk column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_pk_column("svr_port", compaction::SHARED_STORAGE_REPLICA_CKM_SVR_PORT))) {
      LOG_WARN("failed to add pk column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_column("compaction_scn", item.compaction_scn_.get_val_for_tx()))) {
      LOG_WARN("failed to add pk column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_column("row_count", item.row_count_))) {
      LOG_WARN("failed to add column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_column("data_checksum", item.data_checksum_))) {
      LOG_WARN("failed to add column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_column("column_checksums", visible_column_meta))) {
      LOG_WARN("failed to add column", KR(tmp_ret), K(item));
    } else if (OB_TMP_FAIL(dml.add_column("b_column_checksums", hex_column_meta))) {
      LOG_WARN("failed to add column", KR(tmp_ret), K(item));
    } else if (OP_INSERT == op_type && OB_TMP_FAIL(dml.splice_insert_update_sql(OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, sql))) {
      LOG_WARN("failed to splice sql", KR(tmp_ret));
    } else if (OP_UPDATE == op_type && OB_TMP_FAIL(dml.splice_update_sql(OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, sql))) {
      LOG_WARN("failed to splice sql", KR(tmp_ret));
    } else if (OB_TMP_FAIL(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to write", KR(tmp_ret), K(sql));
    }
  } // for
  return ret;
}

int ObTabletReplicaChecksumOperator::inner_batch_select_and_update_by_sql_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplicaChecksumItem> &items,
    const int64_t start_idx,
    const int64_t end_idx,
    ObISQLClient &sql_client,
    ObTabletSimpleCkmInfoMap &map)
{
  int ret = OB_SUCCESS;
  const int64_t item_cnt = items.count();
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id))
      || (item_cnt <= 0)
      || (start_idx < 0)
      || (start_idx >= end_idx)
      || (end_idx > item_cnt)
      || (!map.created()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "items count", item_cnt,
      K(start_idx), K(end_idx), K(map.created()));
  } else if (OB_FAIL(build_ckm_info_map_(tenant_id, items, start_idx, end_idx, sql_client, map))) {
    LOG_WARN("fail to build replca info map", KR(ret), K(tenant_id), K(items), K(start_idx), K(end_idx));
  } else if (OB_FAIL(update_with_map_(tenant_id, items, start_idx, end_idx, sql_client, map))) {
    LOG_WARN("fail to update", KR(ret), K(tenant_id), K(items), K(start_idx), K(end_idx));
  }
  return ret;
}
#endif

int ObTabletReplicaChecksumOperator::batch_update_compaction_scn(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObTabletID> &tablet_array,
    const int64_t compaction_scn,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_cnt = tablet_array.count();

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_cnt < 1 || !ls_id.is_valid() || compaction_scn <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablet_cnt), K(ls_id), K(compaction_scn));
  }

  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, tablet_cnt);
  while (OB_SUCC(ret) && start_idx < end_idx) {
    int64_t affected_rows = 0;
    ObSqlString sql;

    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET compaction_scn = %lu WHERE tenant_id = %lu AND ls_id = %ld AND compaction_scn < %lu AND tablet_id IN (",
                OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, compaction_scn, tenant_id, ls_id.id(), compaction_scn))) {
      LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(compaction_scn), K(ls_id));
    }

    for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
      const ObTabletID &tablet_id = tablet_array.at(idx);
      if (OB_FAIL(sql.append_fmt("%lu %s", tablet_id.id(), idx == end_idx - 1 ? " )" : ", " ))) {
        LOG_WARN("failed to assign sql", KR(ret), K(idx), K(end_idx), K(tablet_id));
      }
    }

    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (FAILEDx(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(compaction_scn), K(sql));
    } else {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, tablet_cnt);
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_tablet_replica_checksum_items(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    const SCN &compaction_scn,
    const ObIArray<ObTabletLSPair> &tablet_pairs,
    ObReplicaCkmArray &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(batch_get(tenant_id, tablet_pairs, compaction_scn,
        sql_proxy, items, false/*include_larger_than*/,
        share::OBCG_DEFAULT))) {
    LOG_WARN("fail to batch get tablet checksum item", KR(ret), K(tenant_id), K(compaction_scn),
      "pairs_count", tablet_pairs.count());
  } else if (items.get_tablet_cnt() < tablet_pairs.count()) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("fail to get tablet replica checksum items", KR(ret), K(tenant_id), K(compaction_scn),
      K(items));
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

int ObTabletReplicaChecksumOperator::range_get_(
    const uint64_t tenant_id,
    const ObSqlString &tablet_sql,
    const int32_t group_id,
    const int64_t range_size,
    common::ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items,
    int64_t &tablet_cnt)
{
  int ret = OB_SUCCESS;
  items.reset();

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || group_id < 0 || !tablet_sql.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(group_id), K(tablet_sql));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      ObSqlString tmp_sql;
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt(
          "SELECT * FROM %s WHERE tenant_id = %lu and %s ORDER BY tablet_id asc",
          OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME,
          tenant_id,
          tablet_sql.ptr()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (range_size > 0 && OB_FAIL(tmp_sql.append_fmt("%s LIMIT %ld", sql.ptr(), range_size))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql), K(range_size));
      } else if (OB_FAIL(sql_proxy.read(result, meta_tenant_id, range_size > 0 ? tmp_sql.ptr() : sql.ptr(), group_id))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql), K(tmp_sql), K(range_size));
      } else if (OB_FAIL(construct_tablet_replica_checksum_items_(*result.get_result(), items, tablet_cnt))) {
        LOG_WARN("fail to construct tablet checksum items", KR(ret), K(items));
      }
    }
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::range_get(
      const uint64_t tenant_id,
      const common::ObTabletID &start_tablet_id,
      const common::ObTabletID &end_tablet_id,
      const int64_t compaction_scn,
      common::ObISQLClient &sql_proxy,
      ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t tablet_cnt = 0;
  if (OB_UNLIKELY(start_tablet_id > end_tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_tablet_id), K(end_tablet_id));
  } else if (OB_FAIL(sql.append_fmt("tablet_id >= %lu AND tablet_id <= %lu AND compaction_scn = %ld ",
                                    start_tablet_id.id(),
                                    end_tablet_id.id(),
                                    compaction_scn))) {
    LOG_WARN("fail to assign sql", KR(ret), K(start_tablet_id), K(end_tablet_id));
  } else if (OB_FAIL(range_get_(tenant_id, sql, 0/*OBCG_DEFAULT*/, 0/*range_size*/, sql_proxy, items, tablet_cnt))) {
    LOG_WARN("fail to get ckm", KR(ret), K(start_tablet_id), K(end_tablet_id));
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::range_get(
      const uint64_t tenant_id,
      const common::ObTabletID &start_tablet_id,
      const int64_t range_size,
      const int32_t group_id,
      common::ObISQLClient &sql_proxy,
      ObIArray<ObTabletReplicaChecksumItem> &items,
      int64_t &tablet_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(" tablet_id > %lu", start_tablet_id.id()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(start_tablet_id));
  } else if (OB_FAIL(range_get_(tenant_id, sql, group_id, range_size, sql_proxy,
                                items, tablet_cnt))) {
    LOG_WARN("fail to get ckm", KR(ret), K(start_tablet_id), K(range_size));
  } else if (OB_UNLIKELY(items.count() > range_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get too much tablets", KR(ret), K(sql), K(range_size), "items count", items.count());
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::construct_tablet_id_list(const ObIArray<ObTabletID> &tablet_ids, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && (idx < tablet_ids.count()); ++idx) {
    const ObTabletID &tablet_id = tablet_ids.at(idx);
    if (OB_FAIL(sql.append_fmt("%s %ld", 0 == idx ? "" : ",", tablet_id.id()))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
    }
  } // end for
  return ret;
}

int ObTabletReplicaChecksumOperator::multi_get(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t compaction_scn,
    common::ObISQLClient &sql_proxy,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString tablet_ids_sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || compaction_scn < 0 || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(compaction_scn), K(tablet_ids));
  } else if (OB_FAIL(construct_tablet_id_list(tablet_ids, tablet_ids_sql))) {
    LOG_WARN("failed to construct tablet id list", KR(ret), K(tablet_ids));
  } else {
    int64_t tablet_cnt = 0;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt(
          "SELECT * FROM %s WHERE tenant_id = %lu AND compaction_scn=%ld AND tablet_id in (%s) ORDER BY tablet_id asc",
          OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME,
          tenant_id, compaction_scn,
          tablet_ids_sql.ptr()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy.read(result, meta_tenant_id, sql.ptr(), 0/*OBCG_DEFAULT*/))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_tablet_replica_checksum_items_(*result.get_result(), items, tablet_cnt))) {
        LOG_WARN("fail to construct tablet checksum items", KR(ret), K(items));
      }
    } // SMART_VAR
  }
  return ret;
}

int ObTabletReplicaChecksumOperator::get_min_compaction_scn(
    const uint64_t tenant_id,
    SCN &min_compaction_scn)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    const int64_t estimated_timeout_us = 10 * 60 * 1000 * 1000L;
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(timeout_ctx.set_trx_timeout_us(estimated_timeout_us))) {
      LOG_WARN("fail to set trx timeout", KR(ret), K(estimated_timeout_us));
    } else if (OB_FAIL(timeout_ctx.set_timeout(estimated_timeout_us))) {
      LOG_WARN("fail to set abs timeout", KR(ret), K(estimated_timeout_us));
    } else {
      ObSqlString sql;
      SMART_VAR(ObISQLClient::ReadResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql.assign_fmt("SELECT MIN(compaction_scn) as value FROM %s WHERE tenant_id ="
                                   " '%ld' ", OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME, tenant_id))) {
          LOG_WARN("failed to append fmt", K(ret), K(tenant_id));
        } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
        } else {
          uint64_t min_compaction_scn_val = UINT64_MAX;
          EXTRACT_UINT_FIELD_MYSQL(*result, "value", min_compaction_scn_val, uint64_t);
          if (FAILEDx(min_compaction_scn.convert_for_inner_table_field(min_compaction_scn_val))) {
            LOG_WARN("fail to convert uint64_t to SCN", KR(ret), K(min_compaction_scn_val));
          }
        }
      }
    }
    LOG_INFO("finish to get min_compaction_scn", KR(ret), K(tenant_id), K(min_compaction_scn),
             "cost_time_us", ObTimeUtil::current_time() - start_time_us, K(estimated_timeout_us));
  }
  return ret;
}

// ----------------------- ObTabletDataChecksumChecker -----------------------
ObTabletDataChecksumChecker::ObTabletDataChecksumChecker()
  : normal_ckm_item_(nullptr)
{}

ObTabletDataChecksumChecker::~ObTabletDataChecksumChecker()
{
  reset();
}

void ObTabletDataChecksumChecker::reset()
{
  normal_ckm_item_ = nullptr;
}

int ObTabletDataChecksumChecker::check_data_checksum(const ObTabletReplicaChecksumItem& curr_item)
{
  int ret = OB_SUCCESS;
  bool is_cs_replica = false;
  if (OB_FAIL(curr_item.check_data_checksum_type(is_cs_replica))) {
    LOG_WARN("fail to check data checksum type", KR(ret), K(curr_item));
  } else if (is_cs_replica) {
    // skip checking data checksum between cs replicas
  } else {
    if (OB_ISNULL(normal_ckm_item_)) {
      normal_ckm_item_ = &curr_item;
    } else if (normal_ckm_item_->compaction_scn_ != curr_item.compaction_scn_) {
      LOG_INFO("no need to check data checksum", K(curr_item), KPC(this));
    } else if (normal_ckm_item_->data_checksum_ != curr_item.data_checksum_) {
      ret = OB_CHECKSUM_ERROR;
    }
  }
  return ret;
}

int ObTabletDataChecksumChecker::set_data_checksum(const ObTabletReplicaChecksumItem& curr_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(normal_ckm_item_)) {
    bool is_cs_replica = false;
    if (OB_FAIL(curr_item.check_data_checksum_type(is_cs_replica))) {
      LOG_WARN("fail to check data checksum type", KR(ret), K(curr_item));
    } else if (!is_cs_replica) {
      normal_ckm_item_ = &curr_item;
    }
  }
  return ret;
}

} // share
} // oceanbase
