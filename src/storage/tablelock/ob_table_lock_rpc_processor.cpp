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
#include "storage/tablelock/ob_table_lock_rpc_processor.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tx/ob_clog_encrypt_info.h" // TODO: remove with old trans interface
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace transaction;
using namespace transaction::tablelock;

namespace observer
{

int ObTableLockTaskP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // lock/unlock process:
  // 1. get ls
  // 2. get store ctx
  // 3. lock/unlock
  // 4. collect tx exec result.

  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg_));
  } else {
    ObTransService *tx_srv = MTL(ObTransService *);
    switch (arg_.task_type_) {
      case ObTableLockTaskType::PRE_CHECK_TABLET: {
        // NOTE: yanyuan.cxf pre check should not check timeout
        ObAccessService *access_srv = MTL(ObAccessService *);
        if (OB_FAIL(access_srv->pre_check_lock(arg_.lsid_,
                                               *(arg_.tx_desc_),
                                               arg_.param_))) {
          LOG_WARN("failed to exec pre_check_lock operation",
                      K(ret), K(arg_));
        }
        break;
      }
      case ObTableLockTaskType::LOCK_TABLE:
      case ObTableLockTaskType::LOCK_TABLET: {
        ObAccessService *access_srv = MTL(ObAccessService *);
        if (arg_.is_timeout()) {
          ret = OB_TIMEOUT;
          LOG_WARN("table lock task timeout", K(ret), K(arg_));
        } else if (OB_FAIL(access_srv->lock_obj(arg_.lsid_,
                                                *(arg_.tx_desc_),
                                                arg_.param_))) {
          LOG_WARN("failed to exec lock obj operation",
                      K(ret), K(arg_));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid task type", K(ret), K(arg_));
        break;
      } // default
    } // switch

    if (OB_SUCCESS != (tmp_ret = tx_srv->
                       get_tx_exec_result(*(arg_.tx_desc_),
                                          result_.get_tx_result()))) {
      result_.tx_result_ret_code_ = tmp_ret;
      LOG_WARN("get trans_result fail", KR(tmp_ret), K(arg_.tx_desc_));
    }
  }

  result_.ret_code_ = ret;
  LOG_DEBUG("ObTableLockTaskP::process", KR(ret), K(result_), K(arg_));
  ret = OB_SUCCESS;

  return ret;
}

int ObHighPriorityTableLockTaskP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // lock/unlock process:
  // 1. get ls
  // 2. get store ctx
  // 3. lock/unlock
  // 4. collect tx exec result.

  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg_));
  } else {
    ObTransService *tx_srv = MTL(ObTransService *);
    switch (arg_.task_type_) {
      case ObTableLockTaskType::UNLOCK_TABLE:
      case ObTableLockTaskType::UNLOCK_TABLET: {
        ObAccessService *access_srv = MTL(ObAccessService *);
        if (arg_.is_timeout()) {
          ret = OB_TIMEOUT;
          LOG_WARN("table lock task timeout", K(ret), K(arg_));
        } else if (OB_FAIL(access_srv->unlock_obj(arg_.lsid_,
                                                  *(arg_.tx_desc_),
                                                  arg_.param_))) {
          LOG_WARN("failed to exec unlock obj operation",
                      K(ret), K(arg_));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid task type", K(ret), K(arg_));
        break;
      } // default
    } // switch

    if (OB_SUCCESS != (tmp_ret = tx_srv->
                       get_tx_exec_result(*(arg_.tx_desc_),
                                          result_.get_tx_result()))) {
      result_.tx_result_ret_code_ = tmp_ret;
      LOG_WARN("get trans_result fail", KR(tmp_ret), K(arg_.tx_desc_));
    }
  }

  result_.ret_code_ = ret;
  LOG_DEBUG("ObHighPriorityTableLockTaskP::process", KR(ret), K(result_), K(arg_));
  ret = OB_SUCCESS;

  return ret;
}

int ObOutTransLockTableP::process()
{
  int ret = OB_SUCCESS;
  ObTableLockService *table_lock_service = MTL(ObTableLockService *);
  if (OB_FAIL(table_lock_service->lock_table(arg_.table_id_,
                                            arg_.lock_mode_,
                                            arg_.lock_owner_,
                                            arg_.timeout_us_))) {
    LOG_WARN("lock_table failed", K(ret), K(arg_));
  }
  return ret;
}

int ObOutTransUnlockTableP::process()
{
  int ret = OB_SUCCESS;
  ObTableLockService *table_lock_service = MTL(ObTableLockService *);
  if (OB_FAIL(table_lock_service->unlock_table(arg_.table_id_,
                                               arg_.lock_mode_,
                                               arg_.lock_owner_,
                                               arg_.timeout_us_))) {
    LOG_WARN("unlock_table failed", K(ret), K(arg_));
  }
  return ret;
}

} // observer
} // oceanbase
