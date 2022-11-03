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
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace common;
namespace transaction
{
namespace tablelock
{

OB_SERIALIZE_MEMBER(ObLockParam,
                    lock_id_,
                    lock_mode_,
                    owner_id_,
                    op_type_,
                    is_deadlock_avoid_enabled_,
                    is_try_lock_,
                    expired_time_,
                    schema_version_);

OB_SERIALIZE_MEMBER(ObTableLockTaskResult,
                    ret_code_,
                    tx_result_ret_code_,
                    tx_result_);

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
  ObTransService *txs = MTL(transaction::ObTransService*);
  LST_DO_CODE(OB_UNIS_DECODE,
              task_type_,
              lsid_,
              param_);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(txs->acquire_tx(buf, data_len, pos, tx_desc_))) {
    LOG_WARN("acquire tx by deserialize fail", K(data_len), K(pos), K(ret));
  } else {
    need_release_tx_ = true;
    LOG_TRACE("deserialize txDesc", KPC_(tx_desc));
  }
  return ret;
}

int ObLockParam::set(
    const ObLockID &lock_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID &owner_id,
    const ObTableLockOpType type,
    const int64_t schema_version,
    const bool is_deadlock_avoid_enabled,
    const bool is_try_lock,
    const int64_t expired_time)
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
  }
  return ret;
}

bool ObLockParam::is_valid() const
{
  return (lock_id_.is_valid() &&
          is_lock_mode_valid(lock_mode_) &&
          is_op_type_valid(op_type_) &&
          schema_version_ >= 0);
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
  auto txs = MTL(transaction::ObTransService*);
  if (OB_NOT_NULL(tx_desc_)) {
    if (need_release_tx_) {
      LOG_TRACE("free txDesc", KPC_(tx_desc));
      txs->release_tx(*tx_desc_);
    }
  }
  task_type_ = INVALID_LOCK_TASK_TYPE;
  lsid_.reset();
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

}
}
}
