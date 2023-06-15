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

#include "lib/allocator/ob_malloc.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace common
{
template <typename PageAllocatorT, typename PageArenaT>
const int64_t ObStringBufT<PageAllocatorT, PageArenaT>::DEF_MEM_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

template <typename PageAllocatorT, typename PageArenaT>
const int64_t ObStringBufT<PageAllocatorT, PageArenaT>::MIN_DEF_MEM_BLOCK_SIZE =
    OB_MALLOC_NORMAL_BLOCK_SIZE;

template <typename PageAllocatorT, typename PageArenaT>
ObStringBufT<PageAllocatorT, PageArenaT>::ObStringBufT(const lib::ObMemAttr &attr,
                                                       const int64_t block_size /*= DEF_MEM_BLOCK_SIZE*/)
    : local_arena_(block_size < MIN_DEF_MEM_BLOCK_SIZE ? MIN_DEF_MEM_BLOCK_SIZE : block_size,
                   PageAllocatorT(attr)),
      arena_(local_arena_)
{
}

template <typename PageAllocatorT, typename PageArenaT>
ObStringBufT<PageAllocatorT, PageArenaT>::ObStringBufT(PageArenaT &arena)
    : local_arena_(),
      arena_(arena)
{
}

template <typename PageAllocatorT, typename PageArenaT>
ObStringBufT<PageAllocatorT, PageArenaT> :: ~ObStringBufT()
{
  reset();
}

template <typename PageAllocatorT, typename PageArenaT>
int ObStringBufT<PageAllocatorT, PageArenaT> :: reset()
{
  local_arena_.free();
  return OB_SUCCESS;
}

template <typename PageAllocatorT, typename PageArenaT>
int ObStringBufT<PageAllocatorT, PageArenaT> :: reuse()
{
  local_arena_.reuse();
  return OB_SUCCESS;
}

template <typename PageAllocatorT, typename PageArenaT>
int ObStringBufT<PageAllocatorT, PageArenaT> :: write_string(const ObString &str,
                                                             ObString *stored_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stored_str)) {
    ret = OB_BAD_NULL_ERROR;
    _OB_LOG(WARN, "stored str is null, ret=%d", ret);
  } else if (OB_UNLIKELY(0 == str.length() || NULL == str.ptr())) {
    stored_str->assign(NULL, 0);
  } else {
    int64_t str_length = str.length();
    char *str_clone = arena_.dup(str.ptr(), str_length);
    if (OB_UNLIKELY(NULL == str_clone)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG(WARN, "failed to dup string, ret=%d", ret);
    } else {
      stored_str->assign(str_clone, static_cast<int32_t>(str_length));
    }
  }

  return ret;
}

template <typename PageAllocatorT, typename PageArenaT>
int ObStringBufT<PageAllocatorT, PageArenaT> :: write_number(const number::ObNumber &nmb,
                                                             number::ObNumber *stored_nmb)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stored_nmb)) {
    ret = OB_BAD_NULL_ERROR;
    _OB_LOG(WARN, "stored nmb is null, ret=%d", ret);
  } else if (OB_FAIL(stored_nmb->from(nmb, arena_))) {
    _OB_LOG(WARN, "failed to construct number, ret=%d", ret);
  }
  return ret;
}

class ObRowkey;
template <typename PageAllocatorT, typename PageArenaT>
int ObStringBufT<PageAllocatorT, PageArenaT> :: write_string(const ObRowkey &rowkey,
                                                             ObRowkey *stored_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stored_rowkey)) {
    ret = OB_BAD_NULL_ERROR;
    _OB_LOG(WARN, "stored rowkey is null, ret=%d", ret);
  } else if (OB_UNLIKELY(0 == rowkey.length() || NULL == rowkey.ptr())) {
    stored_rowkey->assign(NULL, 0);
  } else {
    int64_t str_length = rowkey.get_deep_copy_size();
    char *buf = arena_.alloc(str_length);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG(WARN, "no memory, ret=%d", ret);
    } else {
      ObRawBufAllocatorWrapper allocator(buf, str_length);
      if (OB_FAIL(rowkey.deep_copy(*stored_rowkey, allocator))) {
        _OB_LOG(WARN, "failed to deep copy rowkey, ret=%d", ret);
      } else {}
    }
  }
  return ret;
}

template <typename PageAllocatorT, typename PageArenaT>
int ObStringBufT<PageAllocatorT, PageArenaT> :: write_obj(const ObObj &obj, ObObj *stored_obj)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stored_obj)) {
    ret = OB_BAD_NULL_ERROR;
    _OB_LOG(WARN, "stored obj is null, ret=%d", ret);
  } else if (OB_FAIL(ob_write_obj(arena_, obj, *stored_obj))) {
    LIB_LOG(WARN, "write obj failed", K(ret), K(obj));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
