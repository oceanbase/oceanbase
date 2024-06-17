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

#define USING_LOG_PREFIX TABLELOCK

#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_define.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{

using namespace common;
namespace transaction
{

namespace tablelock
{
constexpr const char ObSimpleIteratorModIds::OB_OBJ_LOCK[];
constexpr const char ObSimpleIteratorModIds::OB_OBJ_LOCK_MAP[];

bool is_deadlock_avoid_enabled(const bool is_from_sql, const int64_t timeout_us)
{
  return (!is_from_sql && timeout_us >= MIN_DEADLOCK_AVOID_TIMEOUT_US);
}

OB_SERIALIZE_MEMBER(ObTableLockOp, lock_id_,
                    lock_mode_,
                    owner_id_,
                    create_trans_id_,
                    op_type_,
                    lock_op_status_,
                    lock_seq_no_,
                    commit_version_,
                    commit_scn_,
                    create_timestamp_,
                    create_schema_version_);

DEFINE_SERIALIZE(ObLockID)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, (int8_t)obj_type_))) {
    LOG_WARN("serialize obj_type_ failed, ", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, obj_id_))) {
    LOG_WARN("serialize obj_id_ failed, ", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLockID)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t *)&obj_type_))) {
    LOG_WARN("deserialize obj_type_ failed.", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos,
      reinterpret_cast<int64_t *>(&obj_id_)))) {
    LOG_WARN("deserialize obj_id_ failed.", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    hash_value_ = inner_hash();
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLockID)
{
  int64_t size = 0;
  size += serialization::encoded_length_i8((int8_t)obj_type_);
  size += serialization::encoded_length_i64(obj_id_);
  return size;
}

int ObLockID::convert_to(common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  common::ObTabletID tmp_id(obj_id_);
  if (!is_tablet_lock() || !tmp_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not convert to", K(ret), K_(obj_type), K_(obj_id));
  } else {
    tablet_id = tmp_id;
  }
  return ret;
}

int ObLockID::set(const ObLockOBJType &type, const uint64_t obj_id)
{
  int ret = OB_SUCCESS;

  if (!is_lock_obj_type_valid(type) || !is_valid_id(obj_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init fail", K(ret), K(type), K(obj_id));
  } else {
    obj_type_ = type;
    obj_id_ = obj_id;
  }
  hash_value_ = inner_hash();
  return ret;
}

int get_lock_id(const ObLockOBJType obj_type,
                const uint64_t obj_id,
                ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_lock_obj_type_valid(obj_type) || !is_valid_id(obj_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(ret), K(obj_type), K(obj_id));
  } else if (OB_FAIL(lock_id.set(obj_type, obj_id))) {
    LOG_WARN("create lock id failed.", K(ret));
  }
  return ret;
}

int get_lock_id(const uint64_t table_id,
                ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_id(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(ret), K(table_id));
  } else if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_TABLE,
                                 table_id))) {
    LOG_WARN("create lock id failed.", K(ret));
  }
  return ret;
}

int get_lock_id(const ObTabletID &tablet,
                ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  if (!tablet.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(ret), K(tablet));
  } else if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_TABLET,
                                 tablet.id()))) {
    LOG_WARN("create lock id failed.", K(ret), K(tablet));
  }
  return ret;
}

ObTableLockOwnerID ObTableLockOwnerID::default_owner()
{
  ObTableLockOwnerID owner;
  owner.set_default();
  return owner;
}

ObTableLockOwnerID ObTableLockOwnerID::get_owner_by_value(const int64_t packed_id)
{
  ObTableLockOwnerID owner;
  owner.convert_from_value(packed_id);
  return owner;
}

int ObTableLockOwnerID::convert_from_value(const int64_t packed_id)
{
  int ret = OB_SUCCESS;
  pack_ = packed_id;
  return ret;
}

int ObTableLockOwnerID::convert_from_value(const ObLockOwnerType owner_type,
                                           const int64_t raw_owner_id)
{
  int ret = OB_SUCCESS;
  if (!is_lock_owner_type_valid(owner_type) || MAX_VALID_RAW_OWNER_ID < raw_owner_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(owner_type), K(raw_owner_id));
  } else {
    pack_ = 0;
    type_ = static_cast<unsigned char>(owner_type);
    id_ = raw_owner_id;
  }
  return ret;
}

int ObTableLockOwnerID::convert_from_client_sessid(const uint32_t client_sessid, const uint64_t client_sess_create_ts)
{
  int ret = OB_SUCCESS;
  pack_ = 0;
  type_ = static_cast<unsigned char>(ObLockOwnerType::SESS_ID_OWNER_TYPE);
  int64_t client_unique_id = client_sess_create_ts & CLIENT_SESS_CREATE_TS_MASK;
  id_ = (static_cast<int64_t>(client_sessid)) | (client_unique_id << CLIENT_SESS_ID_BIT);
  return ret;
}

int ObTableLockOwnerID::convert_to_sessid(uint32_t &sessid) const
{
  int ret = OB_SUCCESS;
  if (type_ != static_cast<int64_t>(ObLockOwnerType::SESS_ID_OWNER_TYPE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this lock owner id cannot be converted to session id", K(ret), K_(type));
  } else {
    sessid = static_cast<uint32_t>(id_ & CLIENT_SESS_ID_MASK);
  }
  return ret;
}

int ObTableLockOwnerID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, pack_))) {
    LOG_WARN("serialize ID failed", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObTableLockOwnerID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &pack_))) {
    LOG_WARN("deserialize ID failed", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObTableLockOwnerID::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(pack_);
  return size;
}

void ObTableLockOp::set(
    const ObLockID &lock_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID &owner_id,
    const ObTransID &trans_id,
    const ObTableLockOpType type,
    const ObTableLockOpStatus lock_op_status,
    const ObTxSEQ seq_no,
    const int64_t create_timestamp,
    const int64_t create_schema_version)
{
  lock_id_ = lock_id;
  lock_mode_ = lock_mode;
  owner_id_ = owner_id;
  create_trans_id_ = trans_id;
  op_type_ = type;
  lock_op_status_ = lock_op_status;
  lock_seq_no_ = seq_no;
  // here, ensure lock-callback was dispatched to single callback-list
  // forcedly set the seq_no's branch to zero
  if (lock_seq_no_.get_branch() != 0) {
    lock_seq_no_.set_branch(0);
  }
  create_timestamp_ = create_timestamp;
  create_schema_version_ = create_schema_version;
}

bool ObTableLockOp::is_valid() const
{
  bool is_valid = false;
  if (is_out_trans_lock_op() && owner_id_.id() == 0) {
    is_valid = false;
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "owner_id should not be 0 in out_trans lock", K_(owner_id));
  } else {
    is_valid = lock_id_.is_valid() &&
               is_lock_mode_valid(lock_mode_) &&
               create_trans_id_.is_valid() &&
               is_op_type_valid(op_type_) &&
               lock_op_status_ != UNKNOWN_STATUS;
  }
  return is_valid;
}

bool ObTableLockOp::need_replay_or_recover(const ObTableLockOp &other) const
{
  return !(lock_seq_no_ == other.lock_seq_no_ &&
           lock_id_ == other.lock_id_ &&
           create_trans_id_ == other.create_trans_id_ &&
           owner_id_ == other.owner_id_ &&
           lock_mode_ == other.lock_mode_ &&
           op_type_ == other.op_type_);
}

void ObTableLockInfo::reset()
{
  table_lock_ops_.reset();
  max_durable_scn_.reset();
}

OB_SERIALIZE_MEMBER(ObTableLockInfo,
                    table_lock_ops_,
                    max_durable_scn_);

} // tablelock
} // transaction
} // oceanbase
