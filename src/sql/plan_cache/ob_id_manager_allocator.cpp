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

#define USING_LOG_PREFIX SQL_PC

#include "sql/plan_cache/ob_id_manager_allocator.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

ObIdManagerAllocator::ObIdManagerAllocator()
    : small_alloc_(),
      m_alloc_(),
      object_size_(0),
      inited_(false)
{
}

ObIdManagerAllocator::~ObIdManagerAllocator()
{
  reset();
}

void ObIdManagerAllocator::reset()
{
  if (inited_) {
    // destroy() print enough errmsg
    (void) small_alloc_.destroy();
    m_alloc_.reset();
  }
  object_size_ = 0;
  inited_ = false;
}

int ObIdManagerAllocator::init(const int64_t sz, const char *label, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sz <= 0)
      || OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sz), K(label), K(tenant_id), K(ret));
  } else {
    object_size_ = sz + EXTEND_SIZE;
    if (OB_UNLIKELY(inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("ObIdManagerAllocator init twice", K(ret));
    } else if (OB_FAIL(small_alloc_.init(object_size_,
                                         label,
                                         tenant_id,
                                         BLOCK_SIZE))) {
      ret = OB_INIT_FAIL;
      LOG_WARN("ObSmallAllocator init failed", K(object_size_), K(label), K(tenant_id), K(ret));
    } else {
      m_alloc_.set_label(label);
      mem_attr_.label_ = label;
      mem_attr_.tenant_id_ = tenant_id;
      inited_ = true;
    }
  }
  return ret;
}

void *ObIdManagerAllocator::alloc_(const int64_t sz)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  int64_t *tmp = NULL;
  int64_t size = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIdManagerAllocator not inited", K(ret));
  } else if (OB_UNLIKELY(sz <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sz), K(ret));
  } else if (sz <= object_size_ - EXTEND_SIZE) {
    if (OB_UNLIKELY(NULL == (buf = small_alloc_.alloc()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory by ObSmallAllocator failed", K(sz), K(object_size_), K(ret));
    } else {
      tmp = static_cast<int64_t *>(buf);
      *tmp = ALLOC_MAGIC;
      tmp++;
      *tmp = SMALL_ALLOC_SYMBOL;
    }
  } else {
    size = sz + EXTEND_SIZE;
    if (OB_UNLIKELY(NULL == (buf = m_alloc_.alloc(size, mem_attr_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory by ObMalloc failed", K(sz), K(object_size_), K(ret));
    } else {
      tmp = static_cast<int64_t *>(buf);
      *tmp = ALLOC_MAGIC;
      tmp++;
      *tmp = M_ALLOC_SYMBOL;
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(NULL != buf)) {
    tmp = static_cast<int64_t *>(buf);
    tmp += 2;
    buf = tmp;
  }

  return buf;
}

void ObIdManagerAllocator::free_(void *ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIdManagerAllocator not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(ptr), K(ret));
  } else {
    int64_t *tmp_magic = static_cast<int64_t *>(ptr);
    int64_t *tmp_symbol = static_cast<int64_t *>(ptr);
    tmp_magic -= 2;
    tmp_symbol -= 1;
    if (OB_UNLIKELY(ALLOC_MAGIC != *tmp_magic)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid magic", KP(ptr), K(*tmp_magic), K(ret));
    } else {
      switch (*tmp_symbol) {
      case SMALL_ALLOC_SYMBOL: {
          small_alloc_.free(static_cast<void *>(tmp_magic));
          break;
        }
      case M_ALLOC_SYMBOL: {
          m_alloc_.free(static_cast<void *>(tmp_magic));
          break;
        }
      default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid symbol magic", KP(ptr), K(*tmp_symbol), K(ret));
          break;
        }
      }
    }
    tmp_magic = NULL;
    tmp_symbol = NULL;
  }
}

} // namespace sql
} // namespace oceanbase
