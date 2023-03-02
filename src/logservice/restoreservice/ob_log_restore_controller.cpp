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

#define USING_LOG_PREFIX CLOG
#include <algorithm>
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_debug_sync.h"        // DEBUG
#include "logservice/ob_log_service.h"
#include "ob_log_restore_controller.h"

namespace oceanbase
{
namespace logservice
{
ObLogRestoreController::ObLogRestoreController() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  log_service_(NULL),
  available_capacity_(0),
  last_refresh_ts_(common::OB_INVALID_TIMESTAMP)
{}

ObLogRestoreController::~ObLogRestoreController()
{
  destroy();
}

int ObLogRestoreController::init(const uint64_t tenant_id, ObLogService *log_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLogRestoreController init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(log_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(log_service));
  } else {
    tenant_id_ = tenant_id;
    log_service_ = log_service;
    inited_ = true;
  }
  return ret;
}

void ObLogRestoreController::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  log_service_ = NULL;
  available_capacity_ = 0;
  last_refresh_ts_ = OB_INVALID_TIMESTAMP;
}

int ObLogRestoreController::get_quota(const int64_t size, bool &succ)
{
  int ret = OB_SUCCESS;
  succ = false;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (size > ATOMIC_LOAD(&available_capacity_)) {
    succ = false;
  } else {
    ATOMIC_SAF(&available_capacity_, size);
    succ = true;
  }
  return ret;
}

int ObLogRestoreController::update_quota()
{
  int ret = OB_SUCCESS;
  int64_t used_size = 0;
  int64_t total_size = 0;
  palf::PalfOptions palf_opt;

  auto get_palf_used_size_func = [&](const palf::PalfHandle &palf_handle) -> int {
    int ret = OB_SUCCESS;
    int64_t palf_id = -1;
    palf::LSN base_lsn;
    palf::LSN end_lsn;
    palf_handle.get_palf_id(palf_id);
    if (OB_FAIL(palf_handle.get_base_lsn(base_lsn))) {
      CLOG_LOG(WARN, "get palf base_lsn failed", K(palf_id));
    } else if (OB_FAIL(palf_handle.get_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "get palf end_lsn failed", K(palf_id));
    } else if (OB_UNLIKELY(base_lsn > end_lsn)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "unexpected base_lsn or end_lsn", K(palf_id), K(base_lsn), K(end_lsn));
    } else {
      used_size += static_cast<int64_t>(end_lsn - base_lsn);
    }
    return ret;
  };

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreController not init", K(tenant_id_));
  } else if (! need_update_()) {
  } else if (OB_FAIL(log_service_->get_palf_options(palf_opt))) {
    LOG_WARN("get palf option failed", K(tenant_id_));
  } else if (OB_FAIL(log_service_->iterate_palf(get_palf_used_size_func))) {
    LOG_WARN("fail to get palf_options", K(tenant_id_));
  } else {
    total_size = palf_opt.disk_options_.log_disk_usage_limit_size_;
    const int64_t capacity = std::max(total_size / 100 * palf_opt.disk_options_.log_disk_utilization_threshold_ - used_size, 0L);
    ATOMIC_SET(&available_capacity_, capacity);
    last_refresh_ts_ = common::ObTimeUtility::fast_current_time();
    LOG_TRACE("update log restore quota succ", K(tenant_id_), K(used_size), K(total_size), K(capacity), K(last_refresh_ts_));
  }
  return ret;
}

bool ObLogRestoreController::need_update_() const
{
  return common::ObTimeUtility::fast_current_time() - last_refresh_ts_ >= LOG_RESTORE_CONTROL_REFRESH_INTERVAL;
}

} // namespace logservice
} // namespace oceanbase
