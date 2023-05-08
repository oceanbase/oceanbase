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

#ifndef OB_SESSION_STAT_H_
#define OB_SESSION_STAT_H_

#include "lib/stat/ob_di_cache.h"
#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase
{
namespace common
{
struct ObSysSessionIds
{
  enum ObSysSessionIdEnum
  {
    DEFAULT = 1,
    MERGE,
    DUMP,
    MIGRATE,
    BLOOM_FILTER_BUILD,
    NETWORK,
    OMT,
    MAX_RESERVED
  };
};

class ObSessionDIBuffer
{
public:
  ObSessionDIBuffer();
  virtual ~ObSessionDIBuffer();
  int switch_both(const uint64_t tenant_id, const uint64_t session_id, const bool is_multi_thread_plan);
  int switch_session(const uint64_t session_id, const bool is_multi_thread_plan = false);
  int switch_tenant(const uint64_t tenant_id);
  uint64_t get_tenant_id();
  void reset_session();
  inline ObDISessionCollect *get_curr_session() {return session_collect_;}
  inline ObDITenantCollect *get_curr_tenant() {return curr_tenant_collect_;}
  inline ObDIThreadTenantCache &get_tenant_cache() {return tenant_cache_;}
private:
  ObDIThreadTenantCache tenant_cache_;
  ObDISessionCollect local_session_collect_;
  ObDISessionCollect *session_collect_;
  ObDITenantCollect *sys_tenant_collect_;
  ObDITenantCollect *curr_tenant_collect_;
  ObDITenantCollect *not_sys_tenant_collect_;
};

class ObSessionStatEstGuard
{
public:
  ObSessionStatEstGuard(const uint64_t tenant_id = OB_SYS_TENANT_ID,
                        const uint64_t session_id = ObSysSessionIds::DEFAULT,
                        const bool is_multi_thread_plan = false);
  virtual ~ObSessionStatEstGuard();
private:
  uint64_t prev_tenant_id_;
  uint64_t prev_session_id_;
  ObSessionDIBuffer *buffer_;
};

class ObTenantStatEstGuard
{
public:
  explicit ObTenantStatEstGuard(uint64_t tenant_id = OB_SYS_TENANT_ID)
      : prev_tenant_id_(OB_SYS_TENANT_ID)
  {
    if (oceanbase::lib::is_diagnose_info_enabled()) {
      buffer_ = GET_TSI(ObSessionDIBuffer);
      if (NULL != buffer_) {
        prev_tenant_id_ = buffer_->get_tenant_id();
        if (0 < tenant_id) {
          if (OB_UNLIKELY(OB_SUCCESS != buffer_->switch_tenant(tenant_id))) {
            buffer_ = nullptr;
          }
        }
      }
    } else {
      buffer_ = nullptr;
    }
  }
  virtual ~ObTenantStatEstGuard()
  {
    if (NULL != buffer_) {
      buffer_->switch_tenant(prev_tenant_id_);
    }
  }
private:
  uint64_t prev_tenant_id_;
  ObSessionDIBuffer *buffer_;
};

inline int ObSessionDIBuffer::switch_both(const uint64_t tenant_id,
    const uint64_t session_id, const bool is_multi_thread_plan)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = switch_session(session_id, is_multi_thread_plan))) {
  } else {
    ret = switch_tenant(tenant_id);
  }
  return ret;
}

inline int ObSessionDIBuffer::switch_session(const uint64_t session_id, const bool is_multi_thread_plan)
{
  int ret = OB_SUCCESS;
  if (NULL != session_collect_ && session_id == session_collect_->session_id_) {
  } else {
    if (NULL != session_collect_) {
      if (session_collect_ != &local_session_collect_) {
        session_collect_->lock_.unlock();
      } else {
        local_session_collect_.clean();
      }
    }
    if (is_multi_thread_plan) {
      local_session_collect_.session_id_ = session_id;
      session_collect_ = &local_session_collect_;
    } else {
      ret = ObDISessionCache::get_instance().get_node(session_id, session_collect_);
    }
  }
  return ret;
}

inline int ObSessionDIBuffer::switch_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    curr_tenant_collect_ = sys_tenant_collect_;
  } else {
    if (OB_NOT_NULL(not_sys_tenant_collect_)
        && tenant_id == not_sys_tenant_collect_->tenant_id_) {
    } else {
      ret = tenant_cache_.get_node(tenant_id, not_sys_tenant_collect_);
    }
    curr_tenant_collect_ = not_sys_tenant_collect_;
  }
  return ret;
}

inline uint64_t ObSessionDIBuffer::get_tenant_id()
{
  uint64_t tenant_id = 0;
  if (NULL != curr_tenant_collect_) {
    tenant_id = curr_tenant_collect_->tenant_id_;
  }
  return tenant_id;
}

inline void ObSessionDIBuffer::reset_session()
{
  if (NULL != session_collect_) {
    // Summarize the thread-local session collect into the session cache
    if (session_collect_ == &local_session_collect_) {
      const uint64_t session_id = session_collect_->session_id_;
      ObDISessionCollect *collect = NULL;
      ObDISessionCache::get_instance().get_node(session_id, collect);
      if (NULL != collect) {
        collect->base_value_.add(session_collect_->base_value_);
        collect->lock_.unlock();
        local_session_collect_.clean();
      }
    } else {
      session_collect_->lock_.unlock();
    }
    session_collect_ = NULL;
  }
}


} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_SESSION_STAT_H_ */
