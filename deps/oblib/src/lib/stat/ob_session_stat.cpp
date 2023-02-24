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

#include "lib/stat/ob_session_stat.h"
#include "lib/ob_lib_config.h"

namespace oceanbase
{
namespace common
{

ObSessionDIBuffer::ObSessionDIBuffer()
  : tenant_cache_(),
    local_session_collect_(),
    session_collect_(NULL),
    sys_tenant_collect_(tenant_cache_.get_sys_tenant_node()),
    curr_tenant_collect_(sys_tenant_collect_),
    not_sys_tenant_collect_(sys_tenant_collect_)
{
}

ObSessionDIBuffer::~ObSessionDIBuffer()
{
}

/**
 *--------------------------------------------------------ObSessionStatEstGuard---------------------------------------------
 */
ObSessionStatEstGuard::ObSessionStatEstGuard(const uint64_t tenant_id, const uint64_t session_id, const bool is_multi_thread_plan)
  : prev_tenant_id_(OB_SYS_TENANT_ID),
    prev_session_id_(0)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    buffer_ = GET_TSI(ObSessionDIBuffer);
    if (NULL != buffer_) {
      prev_tenant_id_ = buffer_->get_tenant_id();
      if (NULL != (buffer_->get_curr_session())) {
        prev_session_id_ = buffer_->get_curr_session()->session_id_;
      }
      if (0 < tenant_id && 0 < session_id) {
        buffer_->switch_both(tenant_id, session_id, is_multi_thread_plan);
      }
    }
  } else {
    buffer_ = nullptr;
  }
}

ObSessionStatEstGuard::~ObSessionStatEstGuard()
{
  if (NULL != buffer_) {
    buffer_->switch_tenant(prev_tenant_id_);
    if (0 != prev_session_id_) {
      buffer_->switch_session(prev_session_id_);
    } else {
      buffer_->reset_session();
    }
  }
}

} /* namespace common */
} /* namespace oceanbase */
