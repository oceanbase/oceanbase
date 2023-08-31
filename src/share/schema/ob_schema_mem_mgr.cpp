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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_schema_mem_mgr.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_force_print_log.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
void ObSchemaMemory::reset() {
  pos_ = OB_INVALID_INDEX;
  tenant_id_ = OB_INVALID_TENANT_ID;
  mem_used_ = OB_INVALID_COUNT;
  mem_total_ = OB_INVALID_COUNT;
  used_schema_mgr_cnt_ = OB_INVALID_COUNT;
  free_schema_mgr_cnt_ = OB_INVALID_COUNT;
  allocator_idx_ = OB_INVALID_INDEX;
}

void ObSchemaMemory::init(const int64_t pos, const uint64_t &tenant_id,
                          const int64_t &mem_used, const int64_t &mem_total,
                          const int64_t &used_schema_mgr_cnt,
                          const int64_t &free_schema_mgr_cnt,
                          const int64_t &allocator_idx) {
  pos_ = pos;
  tenant_id_ = tenant_id;
  mem_used_ = mem_used;
  mem_total_ = mem_total;
  used_schema_mgr_cnt_ = used_schema_mgr_cnt;
  free_schema_mgr_cnt_ = free_schema_mgr_cnt;
  allocator_idx_ = allocator_idx;
}
namespace share
{
namespace schema
{

ObSchemaMemMgr::ObSchemaMemMgr()
  : pos_(0),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObSchemaMemMgr::~ObSchemaMemMgr()
{
}

int ObSchemaMemMgr::init(const char *label, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  auto attr = SET_USE_500(label, ObCtxIds::SCHEMA_SERVICE);
  //FIXME: The memory split of the subsequent 500 tenants is then set to the corresponding tenant_id
  new(&allocator_[0]) ObArenaAllocator(attr, OB_MALLOC_BIG_BLOCK_SIZE);
  new(&allocator_[1]) ObArenaAllocator(attr, OB_MALLOC_BIG_BLOCK_SIZE);
  all_ptrs_[0] = 0;
  all_ptrs_[1] = 0;
  ptrs_[0].set_attr(attr);
  ptrs_[1].set_attr(attr);
  tenant_id_ = tenant_id;
  is_inited_ = true;

  return ret;
}

bool ObSchemaMemMgr::check_inner_stat() const
{
  bool ret = true;
  if (!is_inited_ || (pos_ != 0 && pos_ != 1)) {
    ret = false;
    LOG_WARN("inner stat error", K(is_inited_), K(pos_));
  }
  return ret;
}

int ObSchemaMemMgr::alloc_(const int size, void *&ptr,
                          ObIAllocator **allocator)
{
  int ret = OB_SUCCESS;
  ptr = NULL;
  if (NULL != allocator) {
    *allocator = NULL;
  }
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else {
    ObIAllocator &cur_allocator = allocator_[pos_];
    void *tmp_ptr = cur_allocator.alloc(size);
    if (NULL == tmp_ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc mem failed", K(ret), K(size), K(pos_));
    } else if (FALSE_IT(all_ptrs_[pos_]++)) {
    } else if (OB_FAIL(ptrs_[pos_].push_back(tmp_ptr))) {
      LOG_WARN("push back ptr failed", K(ret), K(pos_));
    } else {
      ptr = tmp_ptr;
      LOG_INFO("alloc schema mgr", K(tmp_ptr), KP(tmp_ptr));
      if (NULL != allocator) {
        *allocator = &cur_allocator;
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_ptr)) {
      LOG_WARN("alloc ptr failed", KR(ret), K(tenant_id_), K(size), K(pos_));
      int tmp_ret = OB_SUCCESS;
      if(OB_TMP_FAIL(free_(tmp_ptr))) {
        FLOG_ERROR("fail to free tmp_ptr", KR(ret), KR(tmp_ret));
      } else {
        ptr = NULL;
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_ALLOC_SCHEMA_MGR);
int ObSchemaMemMgr::alloc_schema_mgr(ObSchemaMgr *&schema_mgr, bool alloc_for_liboblog)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = NULL;
  ObIAllocator *allocator = NULL;
  SpinWLockGuard guard(schema_mem_rwlock_);
  if (OB_UNLIKELY(ERRSIM_ALLOC_SCHEMA_MGR)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("turn on error injection ERRSIM_ALLOC_SCHEMA_MGR", KR(ret));
  } else if (OB_FAIL(alloc_(sizeof(ObSchemaMgr), tmp_ptr, &allocator))) {
    LOG_WARN("alloc memory for schema_mgr falied", KR(ret));
  } else if (OB_ISNULL(allocator) || OB_ISNULL(tmp_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tmp ptr or allocator is null", KR(ret), K(tmp_ptr), K(allocator));
  } else if (alloc_for_liboblog) {
    schema_mgr = new (tmp_ptr) ObSchemaMgr();
  } else {
    schema_mgr = new (tmp_ptr) ObSchemaMgr(*allocator);
    schema_mgr->set_allocator_idx(pos_);
  }
  if (OB_SUCC(ret) && OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new schema mgr is NULL", KR(ret));
  }
  return ret;
}

int ObSchemaMemMgr::find_ptr(const void *ptr, const int ptrs_pos, int &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  if (0 != ptrs_pos && 1 != ptrs_pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr", K(ret));
  } else {
    ObIArray<void *> &ptrs =  ptrs_[ptrs_pos];
    int tmp_idx = -1;
    for (int i = 0; i < ptrs.count() && OB_SUCC(ret) && -1 == tmp_idx; ++i) {
      void *cur_ptr = ptrs.at(i);
      if (OB_ISNULL(cur_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(cur_ptr));
      } else if (cur_ptr == ptr) {
        tmp_idx = i;
      }
    }
    if (OB_SUCC(ret)) {
      idx = tmp_idx;
    }
  }
  return ret;
}

int ObSchemaMemMgr::in_current_allocator(const void *ptr, bool &in_curr_allocator)
{
  int ret = OB_SUCCESS;
  in_curr_allocator = false;
  int tmp_idx = -1;

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret));
  } else if (OB_FAIL(find_ptr(ptr, pos_, tmp_idx))) {
    LOG_WARN("failed to find ptr", K(ret));
  } else if (-1 == tmp_idx) {
    in_curr_allocator = false;
  } else {
    in_curr_allocator = true;
  }
  return ret;
}

int ObSchemaMemMgr::free_(void *ptr)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ptr));
  } else {
    int idx1 = -1;
    int idx2 = -1;
    if (OB_FAIL(find_ptr(ptr, pos_, idx1))) {
      LOG_WARN("find ptr failed", K(pos_), K(ptr), K(idx1));
    } else if (OB_FAIL(find_ptr(ptr, 1 - pos_, idx2))) {
      LOG_WARN("find ptr failed", K(1 - pos_), K(ptr), K(idx2));
    }
    if (OB_SUCC(ret)) {
      if (-1 == idx1 && -1 == idx2) {
        // do-nothing
      } else if (-1 != idx1 && -1 != idx2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ptr should be allocated only once", K(ret), K(ptr), K(idx1), K(idx2));
      } else {
        if (-1 != idx1) {
          if (OB_FAIL(ptrs_[pos_].remove(idx1))) {
            LOG_WARN("failed to remove ptr", K(idx1), K(ret));
          }
        } else {
          if (OB_FAIL(ptrs_[1 - pos_].remove(idx2))) {
            LOG_WARN("failed to remove ptr", K(idx2), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_FREE_SCEHMA_MGR);
int ObSchemaMemMgr::free_schema_mgr(ObSchemaMgr *&schema_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ERRSIM_FREE_SCEHMA_MGR)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("turn on error injection ERRSIM_FREE_SCEHMA_MGR", KR(ret));
  } else if (OB_NOT_NULL(schema_mgr)) {
    const uint64_t tenant_id = schema_mgr->get_tenant_id();
    const int64_t timestamp_in_slot = schema_mgr->get_timestamp_in_slot();
    const int64_t schema_version = schema_mgr->get_schema_version();
    schema_mgr->~ObSchemaMgr();
    FLOG_INFO("[SCHEMA_RELEASE] free schema mgr", K(tenant_id), K(schema_version), K(timestamp_in_slot));
    SpinWLockGuard guard(schema_mem_rwlock_);
    if (OB_FAIL(free_(static_cast<void *>(schema_mgr)))) {
      LOG_ERROR("free schema_mgr failed", KR(ret), K(tenant_id), K(schema_version));
    } else {
      schema_mgr = NULL;
    }
  }
  return ret;
}

int ObSchemaMemMgr::get_cur_alloc_cnt(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    cnt = all_ptrs_[pos_];
  }

  return ret;
}

int ObSchemaMemMgr::get_all_alloc_info(common::ObIArray<ObSchemaMemory> &schema_mem_infos)
{
  int ret = OB_SUCCESS;
  schema_mem_infos.reset();

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    ObSchemaMemory schema_mem;
    int64_t mem_used = OB_INVALID_COUNT;
    int64_t mem_total = OB_INVALID_COUNT;
    int64_t used_schema_mgr_cnt = OB_INVALID_COUNT;
    int64_t free_schema_mgr_cnt = OB_INVALID_COUNT;
    int64_t tenant_id = OB_INVALID_TENANT_ID;

    tenant_id = tenant_id_;
    mem_used = allocator_[pos_].used();
    mem_total = allocator_[pos_].total();
    used_schema_mgr_cnt = ptrs_[pos_].count();
    free_schema_mgr_cnt = all_ptrs_[pos_] - used_schema_mgr_cnt;
    schema_mem.init(0, tenant_id, mem_used, mem_total, used_schema_mgr_cnt, free_schema_mgr_cnt, pos_);
    if (OB_FAIL(schema_mem_infos.push_back(schema_mem))) {
      LOG_WARN("fail to push back schema_mem", KR(ret));
    } else {
      mem_used = allocator_[1 - pos_].used();
      mem_total = allocator_[1 - pos_].total();
      used_schema_mgr_cnt = ptrs_[1 - pos_].count();
      free_schema_mgr_cnt = all_ptrs_[1 - pos_] - used_schema_mgr_cnt;
      schema_mem.init(1, tenant_id, mem_used, mem_total, used_schema_mgr_cnt, free_schema_mgr_cnt, 1 - pos_);
      if (OB_FAIL(schema_mem_infos.push_back(schema_mem))) {
        LOG_WARN("fail to push back schema_mem", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaMemMgr::is_same_allocator(const void *p1, const void * p2, bool &is_same_allocator)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(schema_mem_rwlock_);

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    is_same_allocator = false;
    bool p1_found = false;
    bool p2_found = false;
    bool found_one = false;
    {
      ObIArray<void *> &ptrs =  ptrs_[pos_];
      for (int i = 0; i < ptrs.count() && OB_SUCC(ret) && (!p1_found || !p2_found); ++i) {
        void *cur_ptr = ptrs.at(i);
        if (OB_ISNULL(cur_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(cur_ptr));
        } else if (cur_ptr == p1) {
          p1_found = true;
        } else if (cur_ptr == p2) {
          p2_found = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (p1_found != p2_found) { //one not found, must be different
          is_same_allocator = false;
          found_one = true;
          LOG_INFO("found one ptr in allocator", K(pos_), K(p1), K(p2), K(p1_found), K(p2_found));
        } else if (p1_found) { //both found
          is_same_allocator = true;
          LOG_INFO("both found in allocator", K(pos_), K(p1), K(p2));
        }
      }
    }
    if (OB_SUCC(ret) && !found_one && !is_same_allocator) { //not found one, continue find
      ObIArray<void *> &ptrs =  ptrs_[1 - pos_];
      for (int i = 0; i < ptrs.count() && OB_SUCC(ret) && (!p1_found || !p2_found); ++i) {
        void *cur_ptr = ptrs.at(i);
        if (OB_ISNULL(cur_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(cur_ptr));
        } else if (cur_ptr == p1) {
          p1_found = true;
        } else if (cur_ptr == p2) {
          p2_found = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (p1_found != p2_found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("p1 or p2 not found", K(p1_found), K(p2_found), K(ret), K(p1), K(p2));
        } else if (p1_found) { //both found
          is_same_allocator = true;
          LOG_INFO("both found in second allocator", K(1-pos_), K(p1), K(p2), K(p1_found), K(p2_found));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("p1 and p2 not found", K(1-pos_), K(p1), K(p2), K(p1_found), K(p2_found), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaMemMgr::check_can_switch_allocator(const int64_t &switch_cnt, bool &can_switch) const
{
  int ret = OB_SUCCESS;
  can_switch = false;
  int64_t cur_alloc_cnt = 0;
  bool alloc_not_in_use = false;
  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    alloc_not_in_use = 0 == ptrs_[1 - pos_].count();
    cur_alloc_cnt = all_ptrs_[pos_];
    can_switch = alloc_not_in_use && cur_alloc_cnt > switch_cnt;
  }
  LOG_TRACE("check can switch", KR(ret), K_(tenant_id), K_(pos), K(can_switch), K(cur_alloc_cnt), K(switch_cnt));
  return ret;
}

int ObSchemaMemMgr::switch_allocator()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (0 != ptrs_[1 - pos_].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
    dump_without_lock_();
  } else {
    allocator_[1 - pos_].reset();
    all_ptrs_[1 - pos_] = 0;
    pos_ = 1 - pos_;
    FLOG_INFO("[SCHEMA_RELEASE] switch_allocator", K_(tenant_id), K_(pos));
  }
  return ret;
}

int ObSchemaMemMgr::switch_back_allocator()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    pos_ = 1 - pos_;
    FLOG_WARN("[SCHEMA_RELEASE] schema mgr encounters something wrong, it needs to switch_back_allocator", K_(tenant_id), K_(pos));
  }

  return ret;
}

void ObSchemaMemMgr::dump() const
{
  SpinRLockGuard guard(schema_mem_rwlock_);
  dump_without_lock_();
}

void ObSchemaMemMgr::dump_without_lock_() const
{
  FLOG_INFO("[SCHEMA_STATISTICS] cur allocator",
            K_(tenant_id),
            "cur_pos", pos_,
            "mem_used", allocator_[pos_].used(),
            "mem_total", allocator_[pos_].total(),
            "cur_ptrs_cnt", ptrs_[pos_].count(),
            "all_ptrs_cnt", all_ptrs_[pos_],
            "ptrs", ptrs_[pos_]);
  FLOG_INFO("[SCHEMA_STATISTICS] another allocator",
            K_(tenant_id),
            "another_pos", 1 - pos_,
            "mem_used", allocator_[1 - pos_].used(),
            "mem_total", allocator_[1 - pos_].total(),
            "cur_ptrs_cnt", ptrs_[1 - pos_].count(),
            "all_ptrs_cnt", all_ptrs_[1 - pos_],
            "ptrs", ptrs_[1 - pos_]);
}

int ObSchemaMemMgr::check_can_release(bool &can_release) const
{
  int ret = OB_SUCCESS;
  can_release = false;

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    can_release = (0 != allocator_[1 - pos_].used()
                   || 0 != allocator_[pos_].used());
  }
  return ret;
}

int ObSchemaMemMgr::try_reset_allocator()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (0 != ptrs_[pos_].count()) {
    LOG_INFO("allocator is not empty, just skip", K_(tenant_id));
    dump_without_lock_();
  } else {
    all_ptrs_[pos_] = 0;
    allocator_[pos_].reset();
    FLOG_INFO("[SCHEMA_RELEASE] reset schema_mem_mgr", K_(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (0 != ptrs_[1 - pos_].count()) {
    LOG_INFO("another allocator is not empty, just skip", K_(tenant_id));
    dump_without_lock_();
  } else {
    all_ptrs_[1 - pos_] = 0;
    allocator_[1 - pos_].reset();
    FLOG_INFO("[SCHEMA_RELEASE] reset another schema_mem_mgr", K_(tenant_id));
  }
  return ret;
}

int ObSchemaMemMgr::try_reset_another_allocator()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (0 != ptrs_[1 - pos_].count()) {
    LOG_INFO("another allocator is not empty, just skip", K_(tenant_id));
    dump_without_lock_();
  } else {
    all_ptrs_[1 - pos_] = 0;
    allocator_[1 - pos_].reset();
    FLOG_INFO("[SCHEMA_RELEASE] reset another allocator", K_(tenant_id), "pos", 1 - pos_);
  }
  return ret;
}

int ObSchemaMemMgr::get_another_ptrs(common::ObArray<void *> &ptrs)
{
  int ret = OB_SUCCESS;
  ptrs.reset();

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    common::ObArray<void *> &another_ptrs = ptrs_[1 - pos_];
    for (int64_t i = 0; OB_SUCC(ret) && i < another_ptrs.count(); i++) {
      if (OB_FAIL(ptrs.push_back(another_ptrs.at(i)))) {
        LOG_WARN("fail to push back ptr", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSchemaMemMgr::get_current_ptrs(common::ObArray<void *> &ptrs)
{
  int ret = OB_SUCCESS;
  ptrs.reset();

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    common::ObArray<void *> &current_ptrs = ptrs_[pos_];
    for (int64_t i = 0; OB_SUCC(ret) && i < current_ptrs.count(); i++) {
      if (OB_FAIL(ptrs.push_back(current_ptrs.at(i)))) {
        LOG_WARN("fail to push back ptr", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSchemaMemMgr::get_all_ptrs(common::ObArray<void *> &ptrs)
{
  int ret = OB_SUCCESS;
  ptrs.reset();

  SpinRLockGuard guard(schema_mem_rwlock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    common::ObArray<void *> &current_ptrs = ptrs_[pos_];
    common::ObArray<void *> &another_ptrs = ptrs_[1 - pos_];
    for (int64_t i = 0; OB_SUCC(ret) && i < current_ptrs.count(); i++) {
      if (OB_FAIL(ptrs.push_back(current_ptrs.at(i)))) {
        LOG_WARN("fail to push back current ptr", KR(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < another_ptrs.count(); i++) {
      if (OB_FAIL(ptrs.push_back(another_ptrs.at(i)))) {
        LOG_WARN("fail to push back another ptr", KR(ret), K(i));
      }
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
