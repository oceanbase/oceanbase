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
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx/ob_clog_encrypt_info.h" // TODO: remove with old trans interface
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace transaction;
using namespace transaction::tablelock;

namespace observer
{

int check_exist(const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLELOCK_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else {
    // do nothing
  }
  return ret;
}

int check_exist(const ObLockTaskBatchRequest &arg,
                const common::ObTabletID  &tablet_id,
                ObLSHandle ls_handle)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTabletStatus::Status tablet_status = ObTabletStatus::MAX;
  ObTabletCreateDeleteMdsUserData data;
  bool is_commited = false;
  if (ObTableLockTaskType::LOCK_ALONE_TABLET == arg.task_type_ ||
      ObTableLockTaskType::UNLOCK_ALONE_TABLET == arg.task_type_) {
    // alone tablet does not check exist
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet(tablet_id,
                                    tablet_handle,
                                    0,
                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet with timeout failed", K(ret), K(ls->get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_latest_tablet_status(
      data, is_commited))) {
    LOG_WARN("failed to get CreateDeleteMdsUserData", KR(ret));
  } else if (FALSE_IT(tablet_status = data.get_tablet_status())) {
  } else if (ObTabletStatus::NORMAL == tablet_status
             || ObTabletStatus::TRANSFER_OUT == tablet_status
             || ObTabletStatus::TRANSFER_IN == tablet_status) {
    // do nothing
  } else if (ObTabletStatus::DELETED == tablet_status
             || ObTabletStatus::TRANSFER_OUT_DELETED == tablet_status) {
    // tablet shell
    ret = OB_TABLET_NOT_EXIST;
    LOG_INFO("tablet is already deleted", KR(ret), K(tablet_id));
  } else {
    // do nothing
  }
  return ret;
}

#define BATCH_PROCESS(arg, func_name, result)                           \
  ({                                                                    \
    int ret = OB_SUCCESS;                                               \
    ObAccessService *access_srv = MTL(ObAccessService *);               \
    ObLSHandle ls_handle;                                               \
    common::ObTabletID tablet_id;                                       \
    if (OB_FAIL(check_exist(arg.lsid_, ls_handle))) {                   \
      LOG_WARN("check ls failed", K(ret), K(arg));                      \
      if (OB_LS_NOT_EXIST == ret) {                                     \
        result.can_retry_ = true;                                       \
      }                                                                 \
    } else {                                                            \
      for (int i = 0; i < arg.params_.count() && OB_SUCC(ret); i++) {   \
        if (arg.params_[i].lock_id_.is_tablet_lock()) {                 \
          if (OB_FAIL(arg.params_[i].lock_id_.convert_to(tablet_id))) { \
            LOG_WARN("convert lock id to tablet id failed", K(ret),     \
                     K(arg.params_[i].lock_id_));                       \
          } else if (OB_FAIL(check_exist(arg,                           \
                                         tablet_id,                     \
                                         ls_handle))) {                 \
            LOG_WARN("check tablet failed", K(ret), K(tablet_id),       \
                     K(arg.params_[i].expired_time_), K(ls_handle));    \
            if (OB_TABLET_NOT_EXIST == ret) {                           \
              result.can_retry_ = true;                                 \
            }                                                           \
          }                                                             \
        }                                                               \
        if (OB_FAIL(ret)) {                                             \
        } else if (OB_FAIL(access_srv->func_name(arg.lsid_,             \
                                                 *(arg.tx_desc_),       \
                                                 arg.params_[i]))) {    \
          LOG_WARN("failed to exec", K(ret), K(arg.params_[i]));        \
        } else if (arg.params_[i].lock_id_.is_tablet_lock() &&          \
                   OB_FAIL(check_exist(arg,                             \
                                       tablet_id,                       \
                                       ls_handle))) {                   \
          LOG_WARN("check tablet failed", K(ret), K(tablet_id),         \
                   K(arg.params_[i].expired_time_), K(ls_handle));      \
        } else {                                                        \
          result.success_pos_ = i;                                      \
        }                                                               \
      }                                                                 \
    }                                                                   \
    ret;                                                                \
  })

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
      case ObTableLockTaskType::LOCK_PARTITION:
      case ObTableLockTaskType::LOCK_SUBPARTITION:
      case ObTableLockTaskType::LOCK_TABLET:
      case ObTableLockTaskType::LOCK_OBJECT: {
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
      case ObTableLockTaskType::UNLOCK_PARTITION:
      case ObTableLockTaskType::UNLOCK_SUBPARTITION:
      case ObTableLockTaskType::UNLOCK_TABLET:
      case ObTableLockTaskType::UNLOCK_OBJECT: {
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

int ObBatchLockTaskP::process()
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
        ret = BATCH_PROCESS(arg_, pre_check_lock, result_);
        break;
      }
      case ObTableLockTaskType::LOCK_TABLE:
      case ObTableLockTaskType::LOCK_PARTITION:
      case ObTableLockTaskType::LOCK_SUBPARTITION:
      case ObTableLockTaskType::LOCK_TABLET:
      case ObTableLockTaskType::LOCK_OBJECT:
      case ObTableLockTaskType::LOCK_ALONE_TABLET: {
        if (OB_FAIL(BATCH_PROCESS(arg_, lock_obj, result_))) {
          LOG_WARN("failed to exec lock obj operation", K(ret), K(arg_));
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
  LOG_DEBUG("ObBatchLockTaskP::process", KR(ret), K(result_), K(arg_));
  ret = OB_SUCCESS;

  return ret;
}

int ObHighPriorityBatchLockTaskP::process()
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
      case ObTableLockTaskType::UNLOCK_PARTITION:
      case ObTableLockTaskType::UNLOCK_SUBPARTITION:
      case ObTableLockTaskType::UNLOCK_TABLET:
      case ObTableLockTaskType::UNLOCK_OBJECT:
      case ObTableLockTaskType::UNLOCK_ALONE_TABLET: {
        if (OB_FAIL(BATCH_PROCESS(arg_, unlock_obj, result_))) {
          LOG_WARN("failed to exec unlock obj operation", K(ret), K(arg_));
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
  LOG_DEBUG("ObHighPriorityBatchLockTaskP::process", KR(ret), K(result_), K(arg_));
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

int ObAdminRemoveLockP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObAdminRemoveLockP::process", K(arg_));
  uint64_t tenant_id = arg_.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  if (OB_SUCC(ret)) {
    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_,
                                          ls_handle,
                                          ObLSGetMod::TABLELOCK_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(arg_));
    } else if (OB_FAIL(ls->admin_remove_lock_op(arg_.lock_op_))) {
      LOG_WARN("admin remove lock op failed", KR(ret), K(arg_));
    }
  }
  return ret;
}

int ObAdminUpdateLockP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObAdminUpdateLockP::process", K(arg_));
  uint64_t tenant_id = arg_.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  if (OB_SUCC(ret)) {
    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mtl ObLSService should not be null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_,
                                          ls_handle,
                                          ObLSGetMod::TABLELOCK_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(arg_));
    } else if (OB_FAIL(ls->admin_update_lock_op(arg_.lock_op_,
                                                arg_.commit_version_,
                                                arg_.commit_scn_,
                                                arg_.lock_op_.lock_op_status_))) {
      LOG_WARN("admin update lock op failed", KR(ret), K(arg_));
    }
  }
  return ret;
}


} // observer
} // oceanbase
