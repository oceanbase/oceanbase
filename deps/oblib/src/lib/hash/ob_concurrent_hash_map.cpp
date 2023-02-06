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

#include <lib/hash/ob_concurrent_hash_map.h>
#include <lib/allocator/ob_malloc.h>

namespace oceanbase
{
namespace common
{

int HashAlloc::init(const lib::ObLabel &label, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(label), K(tenant_id));
  } else {
    label_ = label;
    tenant_id_ = tenant_id;
  }
  return ret;
}

void *HashAlloc::alloc(const int64_t sz)
{
  void *p = NULL;
  int ret = OB_SUCCESS;
  if (sz <= 0) {
    COMMON_LOG(WARN, "invalid argument", K(sz));
    ret = OB_INVALID_ARGUMENT;
  } else {
    while(!ATOMIC_LOAD(&is_inited_)) {
      if (ATOMIC_BCAS(&size_, 0, sz)) {
        if (0 != (ret = alloc_.init(sz, label_, tenant_id_))) {
          ATOMIC_STORE(&size_, 0);
          COMMON_LOG(WARN, "init small_allocator fail", K(sz));
          break;
        } else {
          ATOMIC_STORE(&is_inited_, true);
        }
      }
    }
    if (OB_SUCC(ret)) {
      p = alloc_.alloc();
    }
  }
  return p;
}

void HashAlloc::free(void *p)
{
  if (p != NULL) {
    alloc_.free(p);
    p = NULL;
  }
}

int ArrayAlloc::init(const lib::ObLabel &label, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(label), K(tenant_id));
  } else {
    label_ = label;
    tenant_id_ = tenant_id;
  }
  return ret;
}

void *ArrayAlloc::alloc(const int64_t sz)
{
  void *ptr = NULL;
  if (sz <= 0) {
    COMMON_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument", K(sz));
  } else {
    ObMemAttr attr(tenant_id_, label_);
    ptr = ob_malloc(sz, attr);
  }
  return ptr;
}

void ArrayAlloc::free(void *p)
{
  if (p != NULL) {
    ob_free(p);
    p = NULL;
  }
}

} // namespace common
} // namespace oceanbase
