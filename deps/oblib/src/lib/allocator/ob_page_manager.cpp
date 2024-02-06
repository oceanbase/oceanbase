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

#include "lib/allocator/ob_page_manager.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::lib;
namespace oceanbase
{
namespace common
{
ObPageManagerCenter::ObPageManagerCenter()
{
  mutex_.enable_record_stat(false);
}

ObPageManagerCenter &ObPageManagerCenter::get_instance()
{
  static ObPageManagerCenter THE_ONE;
  return THE_ONE;
}

int ObPageManagerCenter::register_pm(ObPageManager &pm)
{
  int ret = OB_SUCCESS;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  lib::ObMutexGuard guard(mutex_);
  rb_tree_.insert(&pm);
  pm.has_register_ = true;
  OB_LOG(INFO, "register pm finish", K(ret), KP(&pm), K(pm.get_tid()),
         "tenant_id", pm.get_tenant_id());
  return ret;
}

void ObPageManagerCenter::unregister_pm(ObPageManager &pm)
{
  ObDisableDiagnoseGuard disable_diagnose_guard;
  lib::ObMutexGuard guard(mutex_);
  pm.has_register_ = false;
  rb_tree_.remove(&pm);
  OB_LOG(INFO, "unregister pm finish", KP(&pm), K(pm.get_tid()));
}

bool ObPageManagerCenter::has_register(ObPageManager &pm) const
{
  return pm.has_register_;
}

int ObPageManagerCenter::print_tenant_stat(int64_t tenant_id, char *buf,
                                           int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  lib::ObMutexGuard guard(mutex_);
  int64_t sum_used = 0;
  int64_t sum_hold = 0;
  if (OB_SUCC(print_tenant_stat(tenant_id, sum_used, sum_hold, buf, len, pos)) &&
      sum_hold > 0) {
    ret = databuff_printf(buf, len, pos,
        "[MEMORY][PM] tid=%10s used=%'15ld hold=%'15ld\n", "summary", sum_used, sum_hold);
  }
  return ret;
}

AChunk *ObPageManagerCenter::alloc_from_thread_local_cache(int64_t tenant_id, int64_t ctx_id)
{
  int tmpret = OB_SUCCESS;
  AChunk *ret = nullptr;
  const int RETRY_LIMIT = 10;
  ObDisableDiagnoseGuard disable_diagnose_guard;
  for (int retry = 0; retry < RETRY_LIMIT && OB_EAGAIN == (tmpret = mutex_.trylock()); ++retry) {
    sched_yield();
  }
  if (OB_SUCCESS == tmpret) {
    ret = alloc_from_thread_local_cache_(tenant_id, ctx_id);
    if (OB_SUCCESS != (tmpret = mutex_.unlock())) {
      OB_LOG_RET(ERROR, tmpret, "unlock failed", K(tmpret));
    }
  }
  return ret;
}

int ObPageManagerCenter::print_tenant_stat(int64_t tenant_id,
    int64_t &sum_used, int64_t &sum_hold, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char cmp_buf[sizeof(ObPageManager)];
  ObPageManager *cmp_node = (ObPageManager*)cmp_buf;
  cmp_node->tenant_id_ = tenant_id - 1;
  cmp_node->id_ = INT64_MAX;
  ObPageManager *start = nullptr;
  rb_tree_.nsearch(cmp_node, start);
  struct Arg
  {
    int *ret_;
    char *buf_;
    int64_t len_;
    int64_t *pos_;
    int64_t *sum_used_;
    int64_t *sum_hold_;
    int64_t tenant_id_;
  } arg{&ret, buf, len, &pos, &sum_used, &sum_hold, tenant_id};
  auto &&cb = [] (decltype(rb_tree_) *, ObPageManager *pm, void *p) {
    Arg *arg = (Arg*)p;
    ObPageManager *return_ret = nullptr;
    if (!pm->less_than(arg->tenant_id_, INT64_MAX)) {
      // iter over
      return_ret = pm;
    } else if (pm->get_hold() > 0) {
      *arg->ret_ = databuff_printf(arg->buf_, arg->len_, *arg->pos_,
          "[MEMORY][PM] tid=%10ld used=%'15ld hold=%'15ld pm=%14p ctx_name=%s\n", pm->get_tid(),
          pm->get_used(), pm->get_hold(), pm,
          get_global_ctx_info().get_ctx_name(pm->get_ctx_id()));
      *arg->sum_used_ += pm->get_used();
      *arg->sum_hold_ += pm->get_hold();
    }
    return return_ret;
  };
  if (NULL != start) {
    rb_tree_.iter_rbtree(&rb_tree_, start, cb, &arg);
  }
  return ret;
}

AChunk *ObPageManagerCenter::alloc_from_thread_local_cache_(int64_t tenant_id, int64_t ctx_id)
{
  AChunk * ret = nullptr;

  char cmp_buf[sizeof(ObPageManager)];
  ObPageManager *cmp_node = (ObPageManager*)cmp_buf;
  cmp_node->tenant_id_ = tenant_id - 1;
  cmp_node->id_ = INT64_MAX;
  ObPageManager *start = nullptr;
  rb_tree_.nsearch(cmp_node, start);
  struct Arg
  {
    AChunk *&ret_;
    int64_t tenant_id_;
    int64_t ctx_id_;
  } arg{ret, tenant_id, ctx_id};
  auto &&cb = [] (decltype(rb_tree_) *, ObPageManager *pm, void *p) {
    Arg *arg = (Arg*)p;
    ObPageManager *return_ret = nullptr;
    if (!pm->less_than(arg->tenant_id_, INT64_MAX)) {
      // iter over
      return_ret = pm;
    } else if (pm->get_ctx_id() == arg->ctx_id_) {
      BlockSet::LockGuard lock(pm->bs_.cache_shared_lock_);
      arg->ret_ = pm->bs_.chunk_free_list_.pop();
      if (OB_NOT_NULL(arg->ret_)) {
        UNUSED(ATOMIC_FAA(&(pm->bs_.total_hold_), -arg->ret_->hold()));
        // iter over
        return_ret = pm;
      }
    }
    return return_ret;
  };
  rb_tree_.iter_rbtree(&rb_tree_, start, cb, &arg);

  return ret;
}

void ObPageManager::reset()
{
  ctx_id_ = ObCtxIds::GLIBC;
  bs_.reset();
  used_ = 0;
  is_inited_ = false;
}

_RLOCAL(ObPageManager *, ObPageManager::tl_instance_);

int64_t ObPageManager::global_id_ = 0;

} // end of namespace common
} // end of namespace oceanbase
