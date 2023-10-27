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
#include "lib/thread/thread_mgr.h"
#include "lib/thread/threads.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "storage/ob_file_system_router.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "observer/omt/ob_tenant_mtl_helper.h"

namespace oceanbase
{
namespace lib
{
bool mtl_is_mini_mode()
{
  return MTL_CTX() != nullptr && MTL_IS_MINI_MODE();
}
}

namespace share
{
using namespace oceanbase::common;

#define INIT_BIND_FUNC_TMP(IDX) \
  ObTenantBase::new_m##IDX##_func_name ObTenantBase::new_m##IDX##_func = nullptr; \
  ObTenantBase::init_m##IDX##_func_name ObTenantBase::init_m##IDX##_func = nullptr; \
  ObTenantBase::start_m##IDX##_func_name ObTenantBase::start_m##IDX##_func = nullptr; \
  ObTenantBase::stop_m##IDX##_func_name ObTenantBase::stop_m##IDX##_func = nullptr; \
  ObTenantBase::wait_m##IDX##_func_name ObTenantBase::wait_m##IDX##_func = nullptr; \
  ObTenantBase::destroy_m##IDX##_func_name ObTenantBase::destroy_m##IDX##_func = nullptr;
#define INIT_BIND_FUNC(UNUSED, IDX) INIT_BIND_FUNC_TMP(IDX)
LST_DO2(INIT_BIND_FUNC, (), MTL_MEMBERS);

#define CONSTRUCT_MEMBER(T, IDX) m##IDX##_()
ObTenantBase::ObTenantBase(const uint64_t id, bool enable_tenant_ctx_check)
    : LST_DO2(CONSTRUCT_MEMBER, (,), MTL_MEMBERS),
    id_(id),
    inited_(false),
    created_(false),
    mtl_init_ctx_(nullptr),
    tenant_role_value_(share::ObTenantRole::Role::INVALID_TENANT),
    unit_max_cpu_(0),
    unit_min_cpu_(0),
    unit_memory_size_(0),
    cgroups_(nullptr),
    enable_tenant_ctx_check_(enable_tenant_ctx_check),
    thread_count_(0),
    mini_mode_(false)
{
}
#undef CONSTRUCT_MEMBER

ObTenantBase &ObTenantBase::operator=(const ObTenantBase &ctx)
{
  if (this == &ctx) {
    return *this;
  }
  id_ = ctx.id_;
  mtl_init_ctx_ = ctx.mtl_init_ctx_;
  tenant_role_value_ = ctx.tenant_role_value_;
#define CONSTRUCT_MEMBER_TMP2(IDX) \
  m##IDX##_ = ctx.m##IDX##_;
#define CONSTRUCT_MEMBER2(UNUSED, IDX) CONSTRUCT_MEMBER_TMP2(IDX)
  LST_DO2(CONSTRUCT_MEMBER2, (), MTL_MEMBERS);
#undef CONSTRUCT_MEMBER_TMP2
#undef CONSTRUCT_MEMBER2
  return *this;
}

class FuncWrapper
{
public:
  std::function<void ()> func_;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    int64_t pos = 0;
    return pos;
  }
};

// TODO 待参数调整
int ObTenantBase::init(ObCgroupCtrl *cgroup)
{
  int ret = OB_SUCCESS;

  ObMemAttr attr(id_, "DynamicFactor");
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice error", K(ret));
  } else if (OB_FAIL(tg_set_.create(1024))) {
    LOG_WARN("fail to create tg set", K(ret));
  } else if (OB_FAIL(thread_dynamic_factor_map_.create(1024, attr))) {
    LOG_WARN("fail to create thread dynamic_factor_map", K(ret));
  } else {
    if (cgroup == nullptr) {
      LOG_WARN("ObTenantBase init cgroup is null", K(id_));
    } else {
      cgroups_ = cgroup;
    }
    inited_ = true;
  }

  return ret;
}

int ObTenantBase::create_mtl_module()
{
  int ret = OB_SUCCESS;

  if (created_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("create twice error", K(ret));
  }

  LOG_INFO("create_mtl_module", K(id_));
  #define CREATE_TMP(IDX)                                                                   \
    if (OB_SUCC(ret)) {                                                                     \
      void *mtl_ptr = nullptr;                                                              \
      if (nullptr == ObTenantBase::new_m##IDX##_func) {                                     \
      } else if (OB_FAIL(ObTenantBase::new_m##IDX##_func(m##IDX##_))) {                     \
        LOG_WARN("mtl create failed", K(ret), "type", typeid(m##IDX##_).name());            \
      } else if (get_mtl_ptr(m##IDX##_, mtl_ptr)) {                                         \
        LOG_INFO("finish create mtl"#IDX, "type", typeid(m##IDX##_).name(), KP(mtl_ptr));   \
      }                                                                                     \
    }
  #define CREATE(UNUSED, IDX) CREATE_TMP(IDX)
    LST_DO2(CREATE, (), MTL_MEMBERS);

  if (OB_SUCC(ret)) {
    created_ = true;
  }

  return ret;
}

int ObTenantBase::init_mtl_module()
{
  int ret = OB_SUCCESS;
  LOG_INFO("init_mtl_module", K(id_));
  #define INIT_TMP(IDX)                                                                          \
    if (OB_SUCC(ret)) {                                                                          \
      int64_t start_time_us = ObTimeUtility::current_time();                                     \
      if (nullptr == ObTenantBase::init_m##IDX##_func) {                                         \
      } else if (OB_FAIL(ObTenantBase::init_m##IDX##_func(m##IDX##_))) {                         \
        LOG_WARN("mtl init failed", K(ret), "type", typeid(m##IDX##_).name());                   \
      }                                                                                          \
      int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;                      \
      LOG_INFO("finish init mtl"#IDX, K(cost_time_us), "type", typeid(m##IDX##_).name());        \
    }
  #define INIT(UNUSED, IDX) INIT_TMP(IDX)
    LST_DO2(INIT, (), MTL_MEMBERS);

  return ret;
}
int ObTenantBase::start_mtl_module()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start_mtl_module", K(id_));

  #define START_TMP(IDX)                                                                       \
    if (OB_SUCC(ret)) {                                                                        \
      int64_t start_time_us = ObTimeUtility::current_time();                                   \
      if (nullptr == ObTenantBase::start_m##IDX##_func) {                                      \
      } else if (OB_FAIL(ObTenantBase::start_m##IDX##_func(m##IDX##_))) {                      \
        LOG_WARN("mtl start failed", K(ret), "type", typeid(m##IDX##_).name());                \
      }                                                                                        \
      int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;                    \
      LOG_INFO("finish start mtl"#IDX, K(cost_time_us), "type", typeid(m##IDX##_).name());     \
    }
  #define START(UNUSED, IDX) START_TMP(IDX)
    LST_DO2(START, (), MTL_MEMBERS);

  return ret;

}

void ObTenantBase::stop_mtl_module()
{
  LOG_INFO("stop_mtl_module", K(id_));
  ObSEArray<FuncWrapper, 100> func_arr;
#define STOP_TMP(IDX)                                                                               \
  if (ObTenantBase::stop_m##IDX##_func != nullptr) {                                                \
    FuncWrapper fw;                                                                                 \
    fw.func_ = [this] () {                                                                          \
      int64_t start_time_us = ObTimeUtility::current_time();                                        \
      this->stop_m##IDX##_func(this->m##IDX##_);                                                    \
      int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;                         \
      FLOG_INFO("finish stop mtl"#IDX, K(cost_time_us), "type", typeid(this->m##IDX##_).name());    \
    };                                                                                              \
    func_arr.push_back(fw);                                                                         \
  }
#define STOP(UNUSED, IDX) STOP_TMP(IDX)
  LST_DO2(STOP, (), MTL_MEMBERS);
  int count = int(func_arr.count());
  for (int i = count - 1; i >= 0; i--) {
      func_arr.at(i).func_();
  }
}

void ObTenantBase::wait_mtl_module()
{
  LOG_INFO("wait_mtl_module", K(id_));
  ObSEArray<FuncWrapper, 100> func_arr;
#define WAIT_TMP(IDX)                                                                             \
  if (ObTenantBase::wait_m##IDX##_func != nullptr) {                                              \
    FuncWrapper fw;                                                                               \
    fw.func_ = [this] () {                                                                        \
      int64_t start_time_us = ObTimeUtility::current_time();                                      \
      this->wait_m##IDX##_func(this->m##IDX##_);                                                  \
      int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;                       \
      FLOG_INFO("finish wait mtl"#IDX, K(cost_time_us), "type", typeid(this->m##IDX##_).name());  \
    };                                                                                            \
    func_arr.push_back(fw);                                                                       \
  }
#define WAIT(UNUSED, IDX) WAIT_TMP(IDX)
  LST_DO2(WAIT, (), MTL_MEMBERS);
  int count = int(func_arr.count());
  for (int i = count - 1; i >= 0; i--) {
      func_arr.at(i).func_();
  }
}

void ObTenantBase::destroy()
{
  if (tg_set_.size() > 0) {
    TGSetDumpFunc tg_set_dump_func;
    tg_set_.foreach_refactored(tg_set_dump_func);
    _OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                "tg thread not execute tg_destory make tg_id leak, tg_size=%ld, tg_set=[%s]",
                tg_set_.size(), tg_set_dump_func.buf_);
  }
  tg_set_.destroy();
  thread_dynamic_factor_map_.destroy();
  OB_ASSERT(thread_list_.get_size() == 0);
  inited_ = false;
}


void ObTenantBase::destroy_mtl_module()
{
  LOG_INFO("destroy_mtl_module", K(id_));
   ObSEArray<FuncWrapper, 100> func_arr;
#define DESTROY_TMP(IDX)                                                                                           \
  if (ObTenantBase::destroy_m##IDX##_func != nullptr) {                                                            \
    FuncWrapper fw;                                                                                                \
    fw.func_ = [this] () {                                                                                         \
      int ret = OB_SUCCESS;                                                                                        \
      void *mtl_ptr = nullptr;                                                                                     \
      if (get_mtl_ptr(this->m##IDX##_, mtl_ptr)) {                                                                 \
        if (nullptr == mtl_ptr) {                                                                                  \
         LOG_WARN("mtl is nullptr before destroy", "type", typeid(this->m##IDX##_).name());                        \
        }                                                                                                          \
      }                                                                                                            \
      int64_t start_time_us = ObTimeUtility::current_time();                                                       \
      this->destroy_m##IDX##_func(this->m##IDX##_);                                                                \
      int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;                                        \
      FLOG_INFO("finish destroy mtl"#IDX, K(cost_time_us), KP(mtl_ptr), "type", typeid(this->m##IDX##_).name());   \
      if (get_mtl_ptr(this->m##IDX##_, mtl_ptr)) {                                                                 \
        if (nullptr != mtl_ptr) {                                                                                  \
          LOG_WARN("mtl is not nullptr after destroy", "type", typeid(this->m##IDX##_).name());                    \
        }                                                                                                          \
      }                                                                                                            \
    };                                                                                                             \
    func_arr.push_back(fw);                                                                                        \
  }
#define DESTROY(UNUSED, IDX) DESTROY_TMP(IDX)
  LST_DO2(DESTROY, (), MTL_MEMBERS);
  int count = int(func_arr.count());
  for (int i = count - 1; i >= 0; i--) {
      func_arr.at(i).func_();
  }

  created_ = false;
}

ObCgroupCtrl *ObTenantBase::get_cgroup()
{
  ObCgroupCtrl *cgroup_ctrl = nullptr;
  cgroup_ctrl = cgroups_;
  return cgroup_ctrl;
}

int ObTenantBase::pre_run()
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(this);
  ObCgroupCtrl *cgroup_ctrl = get_cgroup();
  if (cgroup_ctrl != nullptr && cgroup_ctrl->is_valid()) {
    ret = cgroup_ctrl->add_self_to_cgroup(id_);
  }
  {
    ThreadListNode *node = lib::Thread::current().get_thread_list_node();
    lib::ObMutexGuard guard(thread_list_lock_);
    if (!thread_list_.add_last(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("add to thread list fail", K(ret));
    }
  }
  ATOMIC_INC(&thread_count_);
  LOG_INFO("tenant thread pre_run", K(MTL_ID()), K(ret), K(thread_count_));
  return ret;
}

int ObTenantBase::end_run()
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(nullptr);
  ObCgroupCtrl *cgroup_ctrl = get_cgroup();
  if (cgroup_ctrl != nullptr && cgroup_ctrl->is_valid()) {
    ret = cgroup_ctrl->remove_self_from_cgroup(id_);
  }
  {
    ThreadListNode *node = lib::Thread::current().get_thread_list_node();
    lib::ObMutexGuard guard(thread_list_lock_);
    thread_list_.remove(node);
  }
  ATOMIC_DEC(&thread_count_);
  LOG_INFO("tenant thread end_run", K(id_), K(ret), K(thread_count_));
  return ret;
}

void ObTenantBase::tg_create_cb(int tg_id)
{
  tg_set_.set_refactored(tg_id);
}

void ObTenantBase::tg_destroy_cb(int tg_id)
{
  tg_set_.erase_refactored(tg_id);
}

int ObTenantBase::register_module_thread_dynamic(double dynamic_factor, int tg_id)
{
  int ret = OB_SUCCESS;
  if (dynamic_factor <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ThreadDynamicNode node(tg_id);
    ret = thread_dynamic_factor_map_.set_refactored(node, dynamic_factor);
  }
  return ret;
}

int ObTenantBase::unregister_module_thread_dynamic(int tg_id)
{
  ThreadDynamicNode node(tg_id);
  return thread_dynamic_factor_map_.erase_refactored(node);
}

int ObTenantBase::register_module_thread_dynamic(double dynamic_factor, lib::Threads *th)
{
  int ret = OB_SUCCESS;
  if (dynamic_factor <= 0 || th == nullptr) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ThreadDynamicNode node(th);
    ret = thread_dynamic_factor_map_.set_refactored(node, dynamic_factor);
  }
  return ret;
}

int ObTenantBase::unregister_module_thread_dynamic(lib::Threads *th)
{
  ThreadDynamicNode node(th);
  return thread_dynamic_factor_map_.erase_refactored(th);
}

int ObTenantBase::register_module_thread_dynamic(double dynamic_factor, ThreadDynamicImpl *dynamic_impl)
{
  int ret = OB_SUCCESS;
  if (dynamic_factor <= 0 || dynamic_impl == nullptr) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ThreadDynamicNode node(dynamic_impl);
    ret = thread_dynamic_factor_map_.set_refactored(node, dynamic_factor);
  }
  return ret;
}

int ObTenantBase::unregister_module_thread_dynamic(ThreadDynamicImpl *dynamic_impl)
{
  ThreadDynamicNode node(dynamic_impl);
  return thread_dynamic_factor_map_.erase_refactored(dynamic_impl);
}

int64_t ObTenantBase::get_max_session_num(const int64_t rl_max_session_num)
{
  int64_t max_session_num = 0;
  if (rl_max_session_num != 0) {
    max_session_num = rl_max_session_num;
  } else {
    /* As test, one session occupies 100K bytes*/
    max_session_num = max(100, (unit_memory_size_ * 5 / 100) / (100<<10));
  }
  return max_session_num;
}

int ObTenantBase::update_thread_cnt(double tenant_unit_cpu)
{
  int64_t old_thread_count = ATOMIC_LOAD(&thread_count_);
  int ret = OB_SUCCESS;
  if (tenant_unit_cpu <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_thread_cnt", K(tenant_unit_cpu), K(id()), K(ret));
  }
  if (OB_SUCC(ret)) {
    for (ThreadDynamicFactorMap::iterator it = thread_dynamic_factor_map_.begin(); it != thread_dynamic_factor_map_.end(); it++) {
      int cnt = it->second * tenant_unit_cpu;
      if (cnt < 1) {
        cnt = 1;
      }
      int tmp_ret = OB_SUCCESS;
      if (it->first.get_type() == ThreadDynamicNode::TG) {
        tmp_ret = TG_SET_THREAD_CNT(it->first.get_tg_id(), cnt);
      } else if (it->first.get_type() == ThreadDynamicNode::USER_THREAD) {
        tmp_ret = it->first.get_user_thread()->do_set_thread_count(cnt);
      } else if (it->first.get_type() == ThreadDynamicNode::DYNAMIC_IMPL) {
        tmp_ret = it->first.get_dynamic_impl()->set_thread_cnt(cnt);
      }
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("update_thread_cnt", K(it->first), K(cnt), K(tmp_ret), K(it->second));
      }
    }
  }
  int64_t new_thread_count = ATOMIC_LOAD(&thread_count_);
  LOG_INFO("update_thread_cnt", K(tenant_unit_cpu), K(old_thread_count), K(new_thread_count));
  return ret;
}

void ObTenantEnv::set_tenant(ObTenantBase *ctx)
{
  if (ctx != nullptr && ctx->id_ == OB_INVALID_TENANT_ID) {
    LOG_ERROR_RET(OB_ERROR, "ObTenantEnv::set_tenant", KP(ctx));
    ob_abort();
  }
  get_tenant() = ctx;
  if (ctx == nullptr) {
    ObTenantBase ctx_tmp(OB_INVALID_TENANT_ID);
    *get_tenant_local() = ctx_tmp;
    ob_get_tenant_id() = 0;
  } else {
    *get_tenant_local() = *ctx;
    ob_get_tenant_id() = ctx->id();
  }
  lib::set_tenant_tg_helper(ctx);
  // 跳过系统租户的检查是因为系统租户的启动有特殊性
  if (ctx != nullptr && ctx->id() != OB_SYS_TENANT_ID && ctx->enable_tenant_ctx_check_) {
    lib::Threads::get_expect_run_wrapper() = ctx;
  } else {
    lib::Threads::get_expect_run_wrapper() = nullptr;
  }
}

ObTenantSwitchGuard::ObTenantSwitchGuard(ObTenantBase *ctx)
{
  if (ctx != nullptr && ctx->id() != MTL_ID()) {
    on_switch_ = true;
    stash_tenant_ = ObTenantEnv::get_tenant();
    ObTenantEnv::set_tenant(ctx);
  } else {
    on_switch_ = false;
    stash_tenant_ = nullptr;
  }
}

int ObTenantSwitchGuard::switch_to(ObTenantBase *ctx)
{
  int ret = OB_SUCCESS;
  if (ctx != nullptr && ctx->id() != MTL_ID()) {
    on_switch_ = true;
    stash_tenant_ = ObTenantEnv::get_tenant();
    ObTenantEnv::set_tenant(ctx);
    ta_guard_.switch_to(ctx->id());
  } else {
    on_switch_ = false;
    stash_tenant_ = nullptr;
  }
  return ret;
}

bool check_allow_switch(uint64_t src_tenant, uint64_t dest_tenant)
{
  bool allow = true;
  if (src_tenant > OB_USER_TENANT_ID && dest_tenant > OB_USER_TENANT_ID) {
    if (is_user_tenant(src_tenant)) {
      if (gen_meta_tenant_id(src_tenant) != dest_tenant) {
        allow = false;
      }
    } else if (is_meta_tenant(src_tenant)) {
      if (gen_user_tenant_id(src_tenant) != dest_tenant) {
        allow = false;
      }
    }
  }
  return allow;
}

int ObTenantSwitchGuard::switch_to(uint64_t tenant_id, bool need_check_allow)
{
  int ret = OB_SUCCESS;

  if (!common::is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant id to switch", K(ret), K(tenant_id));
  } else if (tenant_id == MTL_ID()) {
    // no need to switch
  } else if (is_virtual_tenant_id(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_ERROR("can't switch to virtual tenant", K(ret), K(tenant_id));
  } else if (on_switch_) {
    // release current tenant lock
    if (release_cb_ != nullptr) {
      release_cb_(lock_handle_);
      release_cb_ = nullptr;
    }
    // switch to dest tenant
    ObTenantBase *switch_tenant = nullptr;
    if (OB_SUCC(get_tenant_base_with_lock(tenant_id, lock_handle_, switch_tenant, release_cb_))) {
      ObTenantEnv::set_tenant(switch_tenant);
    }
  } else if (need_check_allow && !check_allow_switch(MTL_ID(), tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_ERROR("forbid switch in normal tenant", K(tenant_id), K(MTL_ID()), K(ret));
  } else {
    on_switch_ = true;
    ObTenantBase *switch_tenant = nullptr;
    stash_tenant_ = ObTenantEnv::get_tenant();
    if (OB_SUCC(get_tenant_base_with_lock(tenant_id, lock_handle_, switch_tenant, release_cb_))) {
      ObTenantEnv::set_tenant(switch_tenant);
    }
  }
  if (OB_FAIL(ret)) { // convert to one error code
    if (ret == OB_IN_STOP_STATE) {
      ret = OB_TENANT_NOT_IN_SERVER;
    }
    LOG_WARN("switch tenant fail", K(tenant_id), K(ret), K(lbt()));
  }
  if (OB_SUCC(ret)) {
    ta_guard_.switch_to(tenant_id);
  }
  return ret;
}

void ObTenantSwitchGuard::release()
{
  if (on_switch_) {
    if (release_cb_ != nullptr) {
      release_cb_(lock_handle_);
      release_cb_ = nullptr;
    }
    ObTenantEnv::set_tenant(stash_tenant_);

    reset();
  }
}

} // end of namespace share
} // end of namespace oceanbase
