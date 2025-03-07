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
#include "ob_table_lock_rpc_struct.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace common;
namespace transaction
{
namespace tablelock
{
#define GET_REAL_TYPE_LOCK_REQ(T, allocator, lock_req)      \
  void *ptr = static_cast<T *>(allocator.alloc(sizeof(T))); \
  if (OB_ISNULL(ptr)) {                                     \
    ret = OB_ALLOCATE_MEMORY_FAILED;                        \
    LOG_WARN("failed to allocate lock_req", KR(ret));       \
  } else {                                                  \
    lock_req = new (ptr) T();                               \
  }

OB_SERIALIZE_MEMBER(ObLockParam,
                    lock_id_,
                    lock_mode_,
                    owner_id_,
                    op_type_,
                    is_deadlock_avoid_enabled_,
                    is_try_lock_,
                    expired_time_,
                    schema_version_,
                    is_for_replace_,
                    lock_priority_,
                    is_two_phase_lock_);

OB_SERIALIZE_MEMBER_INHERIT(ObReplaceLockParam, ObLockParam,
                            new_lock_mode_,
                            new_owner_id_);

OB_SERIALIZE_MEMBER(ObLockRequest,
                    type_,
                    owner_id_,
                    lock_mode_,
                    op_type_,
                    timeout_us_,
                    is_from_sql_,
                    lock_priority_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockObjRequest, ObLockRequest,
                            obj_type_,
                            obj_id_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockObjsRequest, ObLockRequest,
                            objs_,
                            detect_func_no_,
                            detect_param_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockTableRequest, ObLockRequest,
                            table_id_,
                            detect_func_no_,
                            detect_param_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockPartitionRequest, ObLockTableRequest,
                            part_object_id_,
                            is_sub_part_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockTabletRequest, ObLockTableRequest,
                            tablet_id_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockTabletsRequest, ObLockTableRequest,
                            tablet_ids_);

OB_SERIALIZE_MEMBER_INHERIT(ObLockAloneTabletRequest, ObLockTabletsRequest,
                            ls_id_);

OB_SERIALIZE_MEMBER(ObReplaceLockRequest,
                    new_lock_mode_,
                    new_lock_owner_,
                    *unlock_req_);

OB_SERIALIZE_MEMBER(ObTableLockTaskResult,
                    ret_code_,
                    tx_result_ret_code_,
                    tx_result_,
                    can_retry_,
                    success_pos_);

OB_DEF_SERIALIZE_SIZE(ObTableLockTaskRequest)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx_desc should not be null", K(ret), KP(tx_desc_));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                task_type_,
                lsid_,
                param_,
                *tx_desc_);
  }
  return len;
}

OB_DEF_SERIALIZE(ObTableLockTaskRequest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx_desc should not be null", K(ret), KP(tx_desc_));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                task_type_,
                lsid_,
                param_,
                *tx_desc_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableLockTaskRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              task_type_,
              lsid_,
              param_);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(TxDescHelper::deserialize_tx_desc(buf, data_len, pos, tx_desc_))) {
    LOG_WARN("acquire tx by deserialize fail", K(data_len), K(pos), K(ret));
  } else {
    need_release_tx_ = true;
    LOG_TRACE("deserialize txDesc", KPC_(tx_desc));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObReplaceAllLocksRequest)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t count = 0;
  if (OB_ISNULL(lock_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock_req should not be null", K(ret), KP(lock_req_));
  } else {
    OB_UNIS_ADD_LEN(*lock_req_);
    count = unlock_req_list_.count();
    OB_UNIS_ADD_LEN(count);
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      if (OB_ISNULL(unlock_req_list_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unlock_req should not be null", K(ret));
      } else {
        OB_UNIS_ADD_LEN(*unlock_req_list_.at(i));
      }
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObReplaceAllLocksRequest)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(lock_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock_req should not be null", K(ret), KP(lock_req_));
  } else {
    OB_UNIS_ENCODE(*lock_req_);
    count = unlock_req_list_.count();
    OB_UNIS_ENCODE(count);
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      if (OB_ISNULL(unlock_req_list_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unlock_req should not be null", K(ret));
      } else {
        OB_UNIS_ENCODE(*unlock_req_list_.at(i));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObReplaceAllLocksRequest)
{
  int ret = OB_SUCCESS;
  ObLockRequest lock_req;
  int64_t cnt = 0;
  int64_t tmp_pos = 0;
  void *ptr = nullptr;
  ObUnLockRequest unlock_req;

  if (OB_FAIL(common::serialization::decode(buf, data_len, tmp_pos, lock_req))) {
    LOG_WARN("deserialize lock_req failed", K(ret), K(data_len), K(tmp_pos), KPHEX(buf, data_len));
  } else {
    switch(lock_req.type_) {
    case ObLockRequest::ObLockMsgType::LOCK_OBJ_REQ: {
      GET_REAL_TYPE_LOCK_REQ(ObLockObjsRequest, allocator_, lock_req_);
      break;
    }
    case ObLockRequest::ObLockMsgType::LOCK_TABLE_REQ: {
      GET_REAL_TYPE_LOCK_REQ(ObLockTableRequest, allocator_, lock_req_);
      break;
    }
    case ObLockRequest::ObLockMsgType::LOCK_PARTITION_REQ: {
      GET_REAL_TYPE_LOCK_REQ(ObLockPartitionRequest, allocator_, lock_req_);
      break;
    }
    case ObLockRequest::ObLockMsgType::LOCK_TABLET_REQ: {
      GET_REAL_TYPE_LOCK_REQ(ObLockTabletsRequest, allocator_, lock_req_);
      break;
    }
    case ObLockRequest::ObLockMsgType::LOCK_ALONE_TABLET_REQ: {
      GET_REAL_TYPE_LOCK_REQ(ObLockAloneTabletRequest, allocator_, lock_req_);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("meet unexpected type of lock_req in ObReplaceAllLocksRequest", K(lock_req));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lock_req_->deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize for lock_req failed", K(ret));
    allocator_.free(lock_req_);
    lock_req_ = nullptr;
  } else {
    OB_UNIS_DECODE(cnt);
    for (int64_t i = 0; i < cnt && OB_SUCC(ret); i++) {
      unlock_req.reset();
      tmp_pos = pos;
      ObLockRequest *unlock_req_ptr = nullptr;
      if (OB_FAIL(common::serialization::decode(buf, data_len, tmp_pos, unlock_req))) {
        LOG_WARN("deserialize lock_req failed", K(ret));
      } else {
        switch (unlock_req.type_) {
        case ObLockRequest::ObLockMsgType::UNLOCK_OBJ_REQ: {
          GET_REAL_TYPE_LOCK_REQ(ObUnLockObjsRequest, allocator_, unlock_req_ptr);
          break;
        }
        case ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ: {
          GET_REAL_TYPE_LOCK_REQ(ObUnLockTableRequest, allocator_, unlock_req_ptr);
          break;
        }
        case ObLockRequest::ObLockMsgType::UNLOCK_PARTITION_REQ: {
          GET_REAL_TYPE_LOCK_REQ(ObLockPartitionRequest, allocator_, unlock_req_ptr);
          break;
        }
        case ObLockRequest::ObLockMsgType::UNLOCK_TABLET_REQ: {
          GET_REAL_TYPE_LOCK_REQ(ObLockTabletsRequest, allocator_, unlock_req_ptr);
          break;
        }
        case ObLockRequest::ObLockMsgType::UNLOCK_ALONE_TABLET_REQ: {
          GET_REAL_TYPE_LOCK_REQ(ObLockAloneTabletRequest, allocator_, unlock_req_ptr);
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("meet unexpected type of unlock_req in ObReplaceAllLocksRequest", K(unlock_req));
        }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(unlock_req_ptr->deserialize(buf, data_len, pos))) {
        LOG_WARN("deserialize for unlock_req failed", K(ret));
        allocator_.free(unlock_req_ptr);
      } else if (OB_FAIL(unlock_req_list_.push_back(unlock_req_ptr))) {
        LOG_WARN("push unlock_req into list failed", K(ret));
        allocator_.free(unlock_req_ptr);
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int TxDescHelper::deserialize_tx_desc(DESERIAL_PARAMS, ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTransService is null");
  } else if (OB_FAIL(txs->acquire_tx(buf, data_len, pos, tx_desc))) {
    LOG_WARN("acquire tx by deserialize fail", K(data_len), K(pos), K(ret));
  }
  return ret;
}

int TxDescHelper::release_tx_desc(ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTransService is null");
  } else if (OB_FAIL(txs->release_tx(tx_desc))) {
    LOG_WARN("acquire tx by deserialize fail", K(ret));
  }
  return ret;
}

void ObLockParam::reset()
{
  lock_id_.reset();
  lock_mode_ = NO_LOCK;
  owner_id_.reset();
  op_type_ = UNKNOWN_TYPE;
  is_deadlock_avoid_enabled_ = false;
  is_try_lock_ = true;
  expired_time_ = 0;
  schema_version_ = -1;
  is_for_replace_ = false;
  lock_priority_ = ObTableLockPriority::NORMAL;
  is_two_phase_lock_ = false;
}

int ObLockParam::set(
    const ObLockID &lock_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID &owner_id,
    const ObTableLockOpType type,
    const int64_t schema_version,
    const bool is_deadlock_avoid_enabled,
    const bool is_try_lock,
    const int64_t expired_time,
    const bool is_for_replace)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_id.is_valid()) ||
      OB_UNLIKELY(!is_lock_mode_valid(lock_mode)) ||
      OB_UNLIKELY(!is_op_type_valid(type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_id), K(lock_mode), K(owner_id),
             K(type), K(is_try_lock), K(expired_time));
  } else {
    lock_id_ = lock_id;
    lock_mode_ = lock_mode;
    owner_id_ = owner_id;
    op_type_ = type;
    is_deadlock_avoid_enabled_ = is_deadlock_avoid_enabled;
    is_try_lock_ = is_try_lock;
    expired_time_ = expired_time;
    schema_version_ = schema_version;
    is_for_replace_ = is_for_replace;
  }
  return ret;
}

bool ObLockParam::is_valid() const
{
  return (lock_id_.is_valid() &&
          is_lock_mode_valid(lock_mode_) &&
          is_op_type_valid(op_type_) &&
          !(ObTableLockPriority::NORMAL != lock_priority_ && !is_two_phase_lock_) &&
          (schema_version_ >= 0 ||
           (ObLockOBJType::OBJ_TYPE_COMMON_OBJ == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_TENANT == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_LS == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_EXTERNAL_TABLE_REFRESH == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLE == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLET == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_DATABASE_NAME == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_OBJECT_NAME == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_DBMS_LOCK == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_MATERIALIZED_VIEW == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC == lock_id_.obj_type_
            || ObLockOBJType::OBJ_TYPE_REFRESH_VECTOR_INDEX == lock_id_.obj_type_)));
}

void ObLockRequest::reset()
{
  owner_id_.set_default();
  lock_mode_ = NO_LOCK;
  op_type_ = UNKNOWN_TYPE;
  timeout_us_ = 0;
  is_from_sql_ = false;
  lock_priority_ = ObTableLockPriority::NORMAL;
}

bool ObLockRequest::is_valid() const
{
  return (is_lock_mode_valid(lock_mode_) &&
          is_op_type_valid(op_type_));
}

bool ObLockRequest::is_lock_thread_enabled() const
{
  const int64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  return ((min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_1_4 && min_cluster_version < CLUSTER_VERSION_4_2_2_0)
          || (min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_3_0 && min_cluster_version < CLUSTER_VERSION_4_3_0_0)
          || (min_cluster_version >= CLUSTER_VERSION_4_3_0_0));
}

void ObLockRequest::set_to_unlock_type()
{
  if (is_unlock_request()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "it has been unlock request, no need to change", KPC(this));
  } else {
    switch (type_) {
    case ObLockMsgType::LOCK_OBJ_REQ: {
      type_ = ObLockMsgType::UNLOCK_OBJ_REQ;
      break;
    }
    case ObLockMsgType::LOCK_TABLE_REQ: {
      type_ = ObLockMsgType::UNLOCK_TABLE_REQ;
      break;
    }
    case ObLockMsgType::LOCK_PARTITION_REQ: {
      type_ = ObLockMsgType::UNLOCK_PARTITION_REQ;
      break;
    }
    case ObLockMsgType::LOCK_TABLET_REQ: {
      type_ = ObLockMsgType::UNLOCK_TABLET_REQ;
      break;
    }
    case ObLockMsgType::LOCK_ALONE_TABLET_REQ: {
      type_ = ObLockMsgType::UNLOCK_ALONE_TABLET_REQ;
      break;
    }
    default: {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "meet unexpected request type", KPC(this));
    }
    }
  }
}

void ObReplaceLockParam::reset()
{
  ObLockParam::reset();
  new_owner_id_.reset();
  new_lock_mode_ = NO_LOCK;
}

bool ObReplaceLockParam::is_valid() const
{
  return (ObLockParam::is_valid() &&
          is_lock_mode_valid(new_lock_mode_) &&
          new_owner_id_.is_valid());
}

void ObLockObjRequest::reset()
{
  ObLockRequest::reset();
  obj_type_ = ObLockOBJType::OBJ_TYPE_INVALID;
  obj_id_ = 0;
}

bool ObLockObjRequest::is_valid() const
{
  return (ObLockMsgType::LOCK_OBJ_REQ == type_ &&
          ObLockRequest::is_valid() &&
          is_lock_obj_type_valid(obj_type_) &&
          is_valid_id(obj_id_));
}

ObUnLockObjRequest::ObUnLockObjRequest() :
      ObLockObjRequest()
      {
        if (!is_lock_thread_enabled()) {
          type_ = ObLockMsgType::LOCK_OBJ_REQ;
        } else {
          type_ = ObLockMsgType::UNLOCK_OBJ_REQ;
        }
}

bool ObUnLockObjRequest::is_valid() const
{
  bool valid = true;
  valid = (ObLockRequest::is_valid() &&
           is_lock_obj_type_valid(obj_type_) &&
           is_valid_id(obj_id_));

  valid = valid && (ObLockMsgType::LOCK_OBJ_REQ == type_ ||
                    ObLockMsgType::UNLOCK_OBJ_REQ == type_);
  return valid;
}

void ObLockObjsRequest::reset()
{
  ObLockRequest::reset();
  objs_.reset();
  detect_func_no_ = INVALID_DETECT_TYPE;
  detect_param_.reset();
}

bool ObLockObjsRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = (ObLockMsgType::LOCK_OBJ_REQ == type_ &&
              ObLockRequest::is_valid());
  for (int64_t i = 0; i < objs_.count() && is_valid; i++) {
    is_valid = is_valid && objs_.at(i).is_valid();
  }
  return is_valid;
}

int ObLockObjsRequest::assign(const ObLockObjRequest &arg)
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;
  owner_id_ = arg.owner_id_;
  lock_mode_ = arg.lock_mode_;
  op_type_ = arg.op_type_;
  timeout_us_ = arg.timeout_us_;
  is_from_sql_ = arg.is_from_sql_;
  if (OB_FAIL(lock_id.set(arg.obj_type_, arg.obj_id_))) {
    LOG_WARN("set lock_id failed", K(ret), K(arg));
  } else if (OB_FAIL(objs_.push_back(lock_id))) {
    LOG_WARN("append lock_id into array failed", K(ret), K(arg));
  }
  return ret;
}

ObUnLockObjsRequest::ObUnLockObjsRequest()
  : ObLockObjsRequest()
{
  if (!is_lock_thread_enabled()) {
    type_ = ObLockMsgType::LOCK_OBJ_REQ;
  } else {
    type_ = ObLockMsgType::UNLOCK_OBJ_REQ;
  }
}

bool ObUnLockObjsRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = (ObLockRequest::is_valid());
  for (int64_t i = 0; i < objs_.count() && is_valid; i++) {
    is_valid = is_valid && objs_.at(i).is_valid();
  }
  is_valid = is_valid && (ObLockMsgType::LOCK_OBJ_REQ == type_ ||
                          ObLockMsgType::UNLOCK_OBJ_REQ == type_);
  return is_valid;
}

void ObLockTableRequest::reset()
{
  ObLockRequest::reset();
  table_id_ = 0;
  detect_func_no_ = INVALID_DETECT_TYPE;
  detect_param_.reset();
}

bool ObLockTableRequest::is_valid() const
{
  return (ObLockMsgType::LOCK_TABLE_REQ == type_ &&
          ObLockRequest::is_valid() &&
          is_valid_id(table_id_));
}

ObUnLockTableRequest::ObUnLockTableRequest() : ObLockTableRequest()
{
  if (!is_lock_thread_enabled()) {
    type_ = ObLockMsgType::LOCK_TABLE_REQ;
  } else {
    type_ = ObLockMsgType::UNLOCK_TABLE_REQ;
  }
}

bool ObUnLockTableRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = (ObLockRequest::is_valid() &&
              is_valid_id(table_id_) &&
              (ObLockMsgType::LOCK_TABLE_REQ == type_ ||
               ObLockMsgType::UNLOCK_TABLE_REQ == type_));
  return is_valid;
}

void ObLockPartitionRequest::reset()
{
  ObLockTableRequest::reset();
  part_object_id_ = 0;
}

bool ObLockPartitionRequest::is_valid() const
{
  return (ObLockMsgType::LOCK_PARTITION_REQ == type_ &&
          ObLockRequest::is_valid() &&
          is_valid_id(table_id_) &&
          is_valid_id(part_object_id_));
}

ObUnLockPartitionRequest::ObUnLockPartitionRequest()
  : ObLockPartitionRequest()
{
  if (!is_lock_thread_enabled()) {
    type_ = ObLockMsgType::LOCK_PARTITION_REQ;
  } else {
    type_ = ObLockMsgType::UNLOCK_PARTITION_REQ;
  }
}

bool ObUnLockPartitionRequest::is_valid() const
{
  return ((ObLockMsgType::LOCK_PARTITION_REQ == type_ ||
           ObLockMsgType::UNLOCK_PARTITION_REQ == type_) &&
          ObLockRequest::is_valid() &&
          is_valid_id(table_id_) &&
          is_valid_id(part_object_id_));
}

void ObLockTabletRequest::reset()
{
  ObLockTableRequest::reset();
  tablet_id_.reset();
}

bool ObLockTabletRequest::is_valid() const
{
  return (ObLockMsgType::LOCK_TABLET_REQ == type_ &&
          ObLockRequest::is_valid() &&
          is_valid_id(table_id_) &&
          tablet_id_.is_valid());
}

ObUnLockTabletRequest::ObUnLockTabletRequest() : ObLockTabletRequest()
{
  if (!is_lock_thread_enabled()) {
    type_ = ObLockMsgType::LOCK_TABLET_REQ;
  } else {
    type_ = ObLockMsgType::UNLOCK_TABLET_REQ;
  }
}

bool ObUnLockTabletRequest::is_valid() const
{
  return ((ObLockMsgType::LOCK_TABLET_REQ == type_ ||
           ObLockMsgType::UNLOCK_TABLET_REQ == type_) &&
          ObLockRequest::is_valid() &&
          is_valid_id(table_id_) &&
          tablet_id_.is_valid());
}

void ObLockTabletsRequest::reset()
{
  ObLockTableRequest::reset();
  tablet_ids_.reset();
}

bool ObLockTabletsRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = (ObLockMsgType::LOCK_TABLET_REQ == type_ &&
              ObLockRequest::is_valid() &&
              is_valid_id(table_id_));
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = is_valid && tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

int ObLockTabletsRequest::assign(const ObLockTabletRequest &arg)
{
  int ret = OB_SUCCESS;
  owner_id_ = arg.owner_id_;
  lock_mode_ = arg.lock_mode_;
  op_type_ = arg.op_type_;
  timeout_us_ = arg.timeout_us_;
  is_from_sql_ = arg.is_from_sql_;
  if (OB_FAIL(tablet_ids_.push_back(arg.tablet_id_))) {
    LOG_WARN("append tablet_id into array failed", K(arg), K(ret));
  }
  return ret;
}

ObUnLockTabletsRequest::ObUnLockTabletsRequest() : ObLockTabletsRequest()
{
  if (!is_lock_thread_enabled()) {
    type_ = ObLockMsgType::LOCK_TABLET_REQ;
  } else {
    type_ = ObLockMsgType::UNLOCK_TABLET_REQ;
  }
}

bool ObUnLockTabletsRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = ((ObLockMsgType::LOCK_TABLET_REQ == type_ ||
               ObLockMsgType::UNLOCK_TABLET_REQ == type_) &&
              ObLockRequest::is_valid() &&
              is_valid_id(table_id_));
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = is_valid && tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

void ObLockAloneTabletRequest::reset()
{
  ObLockTabletsRequest::reset();
  ls_id_.reset();
}

bool ObLockAloneTabletRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = (ObLockMsgType::LOCK_ALONE_TABLET_REQ == type_ &&
              ObLockRequest::is_valid() &&
              ls_id_.is_valid());
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = is_valid && tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

ObUnLockAloneTabletRequest::ObUnLockAloneTabletRequest() : ObLockAloneTabletRequest()
{
  if (!is_lock_thread_enabled()) {
    type_ = ObLockMsgType::LOCK_ALONE_TABLET_REQ;
  } else {
    type_ = ObLockMsgType::UNLOCK_ALONE_TABLET_REQ;
  }
}

bool ObUnLockAloneTabletRequest::is_valid() const
{
  bool is_valid = true;
  is_valid = ((ObLockMsgType::LOCK_ALONE_TABLET_REQ == type_ ||
               ObLockMsgType::UNLOCK_ALONE_TABLET_REQ == type_) &&
              ObLockRequest::is_valid() &&
              ls_id_.is_valid());
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = is_valid && tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

void ObReplaceLockRequest::reset()
{
  unlock_req_ = nullptr;
  new_lock_mode_ = MAX_LOCK_MODE;
  new_lock_owner_.reset();
}

bool ObReplaceLockRequest::is_valid() const
{
  return unlock_req_->is_valid() && new_lock_owner_.is_valid() && is_lock_mode_valid(new_lock_mode_);
}

int ObReplaceLockRequest::deserialize_and_check_header(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  CHECK_VERSION_LENGTH(ObReplaceLockRequest, version, len);
  return ret;
}

int ObReplaceLockRequest::deserialize_new_lock_mode_and_owner(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, new_lock_mode_, new_lock_owner_);
  return ret;
}

void ObReplaceAllLocksRequest::reset()
{
  if (OB_NOT_NULL(lock_req_)) {
    allocator_.free(lock_req_);
    lock_req_ = nullptr;
  }
  for (int64_t i = 0; i < unlock_req_list_.count(); i++) {
    ObLockRequest *unlock_req_ptr = unlock_req_list_.at(i);
    if (OB_NOT_NULL(unlock_req_ptr)) {
      allocator_.free(unlock_req_ptr);
    }
  }
}

bool ObReplaceAllLocksRequest::is_valid() const
{
  bool is_valid = false;
  if (OB_NOT_NULL(lock_req_)) {
    is_valid = lock_req_->is_valid();
  } else {
    is_valid = false;
  }

  if (is_valid) {
    if (unlock_req_list_.count() == 0) {
      is_valid = false;
    } else {
      for (int64_t i = 0; i < unlock_req_list_.count() && is_valid; i++) {
        ObLockRequest *unlock_req_ptr = unlock_req_list_.at(i);
        if (OB_NOT_NULL(unlock_req_ptr)) {
          is_valid = is_valid && unlock_req_ptr->is_valid();
        } else {
          is_valid = false;
        }
      }
    }
  }
  return is_valid;
}

int ObTableLockTaskRequest::set(
  const ObTableLockTaskType task_type,
  const share::ObLSID &lsid,
  const ObLockParam &param,
  transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!(task_type < MAX_TASK_TYPE)) ||
      OB_UNLIKELY(!lsid.is_valid()) ||
      OB_UNLIKELY(!param.is_valid()) ||
      OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_type), K(lsid), K(param), KP(tx_desc));
  } else {
    task_type_ = task_type;
    lsid_ = lsid;
    param_ = param;
    tx_desc_ = tx_desc;
  }
  return ret;
}

int ObTableLockTaskRequest::assign(const ObTableLockTaskRequest &arg)
{
  int ret = OB_SUCCESS;
  task_type_ = arg.task_type_;
  lsid_ = arg.lsid_;
  param_ = arg.param_;
  tx_desc_ = arg.tx_desc_;
  return ret;
}

ObTableLockTaskRequest::~ObTableLockTaskRequest()
{
  reset();
}

void ObTableLockTaskRequest::reset()
{
  if (OB_NOT_NULL(tx_desc_)) {
    if (need_release_tx_) {
      LOG_TRACE("free txDesc", KPC_(tx_desc));
      TxDescHelper::release_tx_desc(*tx_desc_);
    }
  }
  task_type_ = INVALID_LOCK_TASK_TYPE;
  lsid_.reset();
  param_.reset();
  tx_desc_ = nullptr;
  need_release_tx_ = false;
}

bool ObTableLockTaskRequest::is_timeout() const
{
  return common::ObTimeUtility::current_time() >= param_.expired_time_;
}

OB_SERIALIZE_MEMBER(ObOutTransLockTableRequest, table_id_, lock_mode_, lock_owner_, timeout_us_);

int ObOutTransLockTableRequest::assign(const ObOutTransLockTableRequest &arg)
{
  int ret = OB_SUCCESS;
  table_id_ = arg.table_id_;
  lock_mode_ = arg.lock_mode_;
  lock_owner_ = arg.lock_owner_;
  timeout_us_ = arg.timeout_us_;
  return ret;
}

ObInTransLockTableRequest::~ObInTransLockTableRequest()
{
}

OB_SERIALIZE_MEMBER(ObInTransLockTableRequest, type_, table_id_, lock_mode_, timeout_us_);

int ObInTransLockTableRequest::assign(const ObInTransLockTableRequest &arg)
{
  int ret = OB_SUCCESS;
  table_id_ = arg.table_id_;
  lock_mode_ = arg.lock_mode_;
  timeout_us_ = arg.timeout_us_;
  return ret;
}

OB_SERIALIZE_MEMBER_INHERIT(ObInTransLockTabletRequest, ObInTransLockTableRequest, tablet_id_);

int ObInTransLockTabletRequest::assign(const ObInTransLockTabletRequest &arg)
{
  int ret = OB_SUCCESS;
  ret = ObInTransLockTableRequest::assign(arg);
  tablet_id_ = arg.tablet_id_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminRemoveLockOpArg, tenant_id_, ls_id_, lock_op_);

int ObAdminRemoveLockOpArg::set(const uint64_t tenant_id,
                                const share::ObLSID &ls_id,
                                const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id ||
                  !ls_id.is_valid() ||
                  !lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(lock_op));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    lock_op_ = lock_op;
  }
  return ret;
}

int ObAdminRemoveLockOpArg::assign(const ObAdminRemoveLockOpArg &arg)
{
  int ret = OB_SUCCESS;
  tenant_id_ = arg.tenant_id_;
  ls_id_ = arg.ls_id_;
  lock_op_ = arg.lock_op_;
  return ret;
}

bool ObAdminRemoveLockOpArg::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_ &&
          ls_id_.is_valid() &&
          lock_op_.is_valid());
}

OB_SERIALIZE_MEMBER(ObAdminUpdateLockOpArg, tenant_id_, ls_id_, lock_op_,
                    commit_version_, commit_scn_);

int ObAdminUpdateLockOpArg::set(const uint64_t tenant_id,
                                const share::ObLSID &ls_id,
                                const ObTableLockOp &lock_op,
                                const share::SCN &commit_version,
                                const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id ||
                  !ls_id.is_valid() ||
                  !lock_op.is_valid() ||
                  !commit_version.is_valid() ||
                  !commit_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(lock_op),
             K(commit_version), K(commit_scn));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    lock_op_ = lock_op;
    commit_version_ = commit_version;
    commit_scn_ = commit_scn;
  }
  return ret;
}

int ObAdminUpdateLockOpArg::assign(const ObAdminUpdateLockOpArg &arg)
{
  int ret = OB_SUCCESS;
  tenant_id_ = arg.tenant_id_;
  ls_id_ = arg.ls_id_;
  lock_op_ = arg.lock_op_;
  commit_version_ = arg.commit_version_;
  commit_scn_ = arg.commit_scn_;
  return ret;
}

bool ObAdminUpdateLockOpArg::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_ &&
          ls_id_.is_valid() &&
          lock_op_.is_valid() &&
          commit_version_.is_valid() &&
          commit_scn_.is_valid());
}
    }
}
}
