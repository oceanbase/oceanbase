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

namespace oceanbase {
using namespace common;

namespace share {
namespace schema {

ObSchemaMemMgr::ObSchemaMemMgr() : pos_(0), is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{}

ObSchemaMemMgr::~ObSchemaMemMgr()
{}

int ObSchemaMemMgr::init(const char* label, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  allocator_[0].set_label(label);
  // FIXME: The memory split of the subsequent 500 tenants is then set to the corresponding tenant_id
  allocator_[0].set_tenant_id(OB_SERVER_TENANT_ID);
  allocator_[1].set_label(label);
  allocator_[1].set_tenant_id(OB_SERVER_TENANT_ID);
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

int ObSchemaMemMgr::alloc(const int size, void*& ptr, ObIAllocator** allocator)
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
    ObIAllocator& cur_allocator = allocator_[pos_];
    void* tmp_ptr = cur_allocator.alloc(size);
    if (NULL == tmp_ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc mem failed", K(ret), K(size), K(pos_));
    } else if (OB_FAIL(all_ptrs_[pos_].push_back(tmp_ptr))) {
      LOG_WARN("push back ptr failed", K(ret), K(pos_));
    } else if (OB_FAIL(ptrs_[pos_].push_back(tmp_ptr))) {
      LOG_WARN("push back ptr failed", K(ret), K(pos_));
    } else {
      ptr = tmp_ptr;
      LOG_INFO("alloc schema mgr", K(tmp_ptr), KP(tmp_ptr), K(lbt()));
      if (NULL != allocator) {
        *allocator = &cur_allocator;
      }
    }
  }

  return ret;
}

int ObSchemaMemMgr::find_ptr(const void* ptr, const int ptrs_pos, int& idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  if (0 != ptrs_pos && 1 != ptrs_pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr", K(ret));
  } else {
    ObIArray<void*>& ptrs = ptrs_[ptrs_pos];
    int tmp_idx = -1;
    for (int i = 0; i < ptrs.count() && OB_SUCC(ret) && -1 == tmp_idx; ++i) {
      void* cur_ptr = ptrs.at(i);
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

int ObSchemaMemMgr::in_current_allocator(const void* ptr, bool& in_curr_allocator)
{
  int ret = OB_SUCCESS;
  in_curr_allocator = false;
  int tmp_idx = -1;
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

int ObSchemaMemMgr::free(void* ptr)
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

int ObSchemaMemMgr::get_cur_alloc_cnt(int64_t& cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    cnt = all_ptrs_[pos_].count();
  }

  return ret;
}

int ObSchemaMemMgr::is_same_allocator(const void* p1, const void* p2, bool& is_same_allocator)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    is_same_allocator = false;
    bool p1_found = false;
    bool p2_found = false;
    bool found_one = false;
    {
      ObIArray<void*>& ptrs = ptrs_[pos_];
      for (int i = 0; i < ptrs.count() && OB_SUCC(ret) && (!p1_found || !p2_found); ++i) {
        void* cur_ptr = ptrs.at(i);
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
        if (p1_found != p2_found) {  // one not found, must be different
          is_same_allocator = false;
          found_one = true;
          LOG_INFO("found one ptr in allocator", K(pos_), K(p1), K(p2), K(p1_found), K(p2_found));
        } else if (p1_found) {  // both found
          is_same_allocator = true;
          LOG_INFO("both found in allocator", K(pos_), K(p1), K(p2));
        }
      }
    }
    if (OB_SUCC(ret) && !found_one && !is_same_allocator) {  // not found one, continue find
      ObIArray<void*>& ptrs = ptrs_[1 - pos_];
      for (int i = 0; i < ptrs.count() && OB_SUCC(ret) && (!p1_found || !p2_found); ++i) {
        void* cur_ptr = ptrs.at(i);
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
        } else if (p1_found) {  // both found
          is_same_allocator = true;
          LOG_INFO("both found in second allocator", K(1 - pos_), K(p1), K(p2), K(p1_found), K(p2_found));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("p1 and p2 not found", K(1 - pos_), K(p1), K(p2), K(p1_found), K(p2_found), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaMemMgr::check_can_switch_allocator(bool& can_switch) const
{
  int ret = OB_SUCCESS;
  can_switch = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    can_switch = 0 == ptrs_[1 - pos_].count();
  }

  return ret;
}

int ObSchemaMemMgr::switch_allocator()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (0 != ptrs_[1 - pos_].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
    dump();
  } else {
    allocator_[1 - pos_].reset();
    all_ptrs_[1 - pos_].reset();
    pos_ = 1 - pos_;
    LOG_INFO("[SCHEMA_RELEASE] switch_allocator", K_(tenant_id), K_(pos));
  }

  return ret;
}

void ObSchemaMemMgr::dump() const
{
  LOG_INFO("[SCHEMA_STATISTICS] cur allocator",
      K_(tenant_id),
      "cur_pos",
      pos_,
      "mem_used",
      allocator_[pos_].used(),
      "mem_total",
      allocator_[pos_].total(),
      "cur_ptrs_cnt",
      ptrs_[pos_].count(),
      "used_ptrs_cnt",
      all_ptrs_[pos_].count(),
      "all_ptrs",
      all_ptrs_[pos_],
      "ptrs",
      ptrs_[pos_]);
  LOG_INFO("[SCHEMA_STATISTICS] another allocator",
      K_(tenant_id),
      "another_pos",
      1 - pos_,
      "mem_used",
      allocator_[1 - pos_].used(),
      "mem_total",
      allocator_[1 - pos_].total(),
      "cur_ptrs_cnt",
      ptrs_[1 - pos_].count(),
      "used_ptrs_cnt",
      all_ptrs_[1 - pos_].count(),
      "all_ptrs",
      all_ptrs_[1 - pos_],
      "ptrs",
      ptrs_[1 - pos_]);
}

int ObSchemaMemMgr::check_can_release(bool& can_release) const
{
  int ret = OB_SUCCESS;
  can_release = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    can_release = (0 != allocator_[1 - pos_].used() || 0 != allocator_[pos_].used());
  }
  return ret;
}

int ObSchemaMemMgr::try_reset_allocator()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (0 != ptrs_[pos_].count()) {
    LOG_INFO("allocator is not empty, just skip", K_(tenant_id));
    dump();
  } else {
    all_ptrs_[pos_].reset();
    allocator_[pos_].reset();
    LOG_INFO("[SCHEMA_RELEASE] reset schema_mem_mgr", K_(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (0 != ptrs_[1 - pos_].count()) {
    LOG_INFO("another allocator is not empty, just skip", K_(tenant_id));
    dump();
  } else {
    all_ptrs_[1 - pos_].reset();
    allocator_[1 - pos_].reset();
    LOG_INFO("[SCHEMA_RELEASE] reset another schema_mem_mgr", K_(tenant_id));
  }
  return ret;
}

int ObSchemaMemMgr::try_reset_another_allocator()
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (0 != ptrs_[1 - pos_].count()) {
    LOG_INFO("another allocator is not empty, just skip", K_(tenant_id));
    dump();
  } else if (0 != all_ptrs_[1 - pos_].count()) {
    all_ptrs_[1 - pos_].reset();
    allocator_[1 - pos_].reset();
    LOG_INFO("[SCHEMA_RELEASE] reset another allocator", K_(tenant_id), "pos", 1 - pos_);
  }
  return ret;
}

int ObSchemaMemMgr::get_another_ptrs(common::ObArray<void*>& ptrs)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    common::ObArray<void*>& another_ptrs = ptrs_[1 - pos_];
    for (int64_t i = 0; OB_SUCC(ret) && i < another_ptrs.count(); i++) {
      if (OB_FAIL(ptrs.push_back(another_ptrs.at(i)))) {
        LOG_WARN("fail to push back ptr", K(ret), K(i));
      }
    }
  }
  return ret;
}

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
