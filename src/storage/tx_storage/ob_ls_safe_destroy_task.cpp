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

#define USING_LOG_PREFIX STORAGE
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_safe_destroy_task.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObLSSafeDestroyTask::ObLSSafeDestroyTask()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_handle_(),
    ls_service_(nullptr)
{
  type_ = ObSafeDestroyTask::ObSafeDestroyTaskType::LS;
}

ObLSSafeDestroyTask::~ObLSSafeDestroyTask()
{
  destroy();
}

int ObLSSafeDestroyTask::init(const uint64_t tenant_id,
                              const ObLSHandle &ls_handle,
                              ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls safe destroy task init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(tenant_id)) ||
             OB_UNLIKELY(!ls_handle.is_valid()) ||
             OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_handle), KP(ls_svr));
  } else {
    // record the ls at ls service to make sure
    // ls service will not release before ls.
    ls_svr->inc_ls_safe_destroy_task_cnt();
    ls_service_ = ls_svr;
    tenant_id_ = tenant_id;
    ls_handle_ = ls_handle;
    is_inited_ = true;
  }
  return ret;
}

bool ObLSSafeDestroyTask::safe_to_destroy()
{
  int ret = OB_SUCCESS;
  bool is_safe = false;
  ObLS *ls = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  // there is no ls protected. safe to destroy
  if (IS_NOT_INIT) {
    is_safe = true;
  } else if (OB_FAIL(guard.switch_to(tenant_id_, false))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!ls_handle_.is_valid())) {
    is_safe = true;
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
    // the ls is not safe to destroy.
  } else if (OB_LIKELY(!ls->safe_to_destroy())) {
    // do nothing
  } else {
    is_safe = true;
  }
  return is_safe;
}

void ObLSSafeDestroyTask::destroy()
{
  int ret = OB_SUCCESS;
  // 1. reset ls_handle.
  // 2. tell the ls service, a ls is released.
  if (is_inited_) {
    // the ls tablet reset may use some MTL component we must switch to this tenant.
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_FAIL(guard.switch_to(tenant_id_, false))) {
      LOG_WARN("switch to tenant failed, we cannot release the ls, because it is may core",
               K(ret), KPC(this));
    } else {
      ls_handle_.reset();
      ls_service_->dec_ls_safe_destroy_task_cnt();
      tenant_id_ = OB_INVALID_ID;
      ls_service_ = nullptr;
      is_inited_ = false;
    }
  }
}

int ObLSSafeDestroyTask::get_ls_id(ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  // there is no ls protected. safe to destroy
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret));
  } else if (OB_FAIL(guard.switch_to(tenant_id_, false))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!ls_handle_.is_valid())) {
    LOG_WARN("ls handle invalid", K(ret), K(tenant_id_), K_(ls_handle));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
    // the ls is not safe to destroy.
  } else {
    ls_id = ls->get_ls_id();
  }
  return ret;
}

bool ObSafeDestroyCheckLSExist::operator()(const ObSafeDestroyTask *task,
                                           bool &need_requeue)
{
  int ret = OB_SUCCESS;
  bool need_continue = true;
  const ObLSSafeDestroyTask *ls_task = nullptr;
  ObLSID ls_id;
  need_requeue = true;
  if (OB_ISNULL(task)) {
    // just skip it.
    need_requeue = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else {
    need_requeue = true;
    if (task->get_type() != ObSafeDestroyTask::ObSafeDestroyTaskType::LS) {
      // just skip it.
    } else {
      ls_task = reinterpret_cast<const ObLSSafeDestroyTask *>(task);
      if (OB_FAIL(ls_task->get_ls_id(ls_id))) {
        LOG_WARN("get ls id failed", KP(ls_task), K(*ls_task));
      } else if (ls_id == ls_id_) {
        exist_ = true;
        need_continue = false;
      } else {
        // do nothing
      }
      if ((OB_SUCCESS == err_code_) && OB_FAIL(ret)) {
        err_code_ = ret;
      }
    }
  }
  return need_continue;
}

} // storage
} // oceanbase
