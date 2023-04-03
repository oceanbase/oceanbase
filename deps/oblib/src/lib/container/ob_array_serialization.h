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

#ifndef _OB_ARRAY_SERIALIZATION_H
#define _OB_ARRAY_SERIALIZATION_H 1

#include "lib/container/ob_array.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_2d_array.h"
#include "lib/utility/ob_serialization_helper.h"

namespace oceanbase
{
namespace common
{

template<typename T, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false, typename CallBack = ObArrayDefaultCallBack<T>, typename ItemEncode = DefaultItemEncode<T> >
using ObSArray = ObArray<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>;

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::serialize(char *buf, const int64_t buf_len,
                                                                 int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, count()))) {
    _OB_LOG(WARN, "fail to encode ob array count:ret[%d]", ret);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); i ++) {
    if (OB_SUCCESS != (ret = ItemEncode::encode_item(buf, buf_len, pos, at(i)))) {
      _OB_LOG(WARN, "fail to encode item[%ld]:ret[%d]", i, ret);
    }
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::deserialize(const char *buf,
                                                                   int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  reset();
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    _OB_LOG(WARN, "fail to decode ob array count:ret[%d]", ret);
  } else if (OB_SUCCESS != (ret = prepare_allocate(count))) {
    OB_LOG(WARN, "fail to allocate memory", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    T &item = at(i);
    if (OB_SUCCESS != (ret = ItemEncode::decode_item(buf, data_len, pos, item))) {
      _OB_LOG(WARN, "fail to decode array item:ret[%d]", ret);
    }
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int64_t ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(count());
  for (int64_t i = 0; i < count(); i ++) {
    size += ItemEncode::encoded_length_item(at(i));
  }
  return size;
}

template<typename T, typename BlockAllocatorT>
int ObFixedArrayImpl<T, BlockAllocatorT>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(data_, count_);
  return ret;
}

template<typename T, typename BlockAllocatorT>
int ObFixedArrayImpl<T, BlockAllocatorT>::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && count > 0) {
    if (OB_FAIL(prepare_allocate(count))) {
      _OB_LOG(WARN, "fail to init ob array item:ret[%d]", ret);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
        OB_UNIS_DECODE(at(i));
      }
    }
  }
  return ret;
}

template<typename T, typename BlockAllocatorT>
int64_t ObFixedArrayImpl<T, BlockAllocatorT>::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(data_, count_);
  return len;
}

template <typename T,
          int max_block_size,
          typename BlockAllocatorT, bool auto_free,
          typename BlockPointerArrayT>
int Ob2DArray<T, max_block_size,
              BlockAllocatorT, auto_free,
              BlockPointerArrayT>::serialize(
                                             char *buf,
                                             const int64_t buf_len,
                                             int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, count()))) {
    _OB_LOG(WARN, "failed to encode ob array count:ret[%d]", ret);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); i++) {
    if (OB_SUCCESS != (ret = serialization::encode(buf, buf_len, pos, at(i)))) {
      _OB_LOG(WARN, "failed to encode item[%ld]:ret[%d]", i, ret);
    }
  }
  return ret;
}

template <typename T,
          int max_block_size,
          typename BlockAllocatorT, bool auto_free,
          typename BlockPointerArrayT>
int64_t Ob2DArray<T,
                  max_block_size,
                  BlockAllocatorT,
                  auto_free,
                  BlockPointerArrayT>::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(count());
  for (int64_t i = 0; i < count(); i++) {
    len += serialization::encoded_length(at(i));
  }
  return len;
}

template <typename T,
          int max_block_size,
          typename BlockAllocatorT, bool auto_free,
          typename BlockPointerArrayT>
int Ob2DArray<T,
              max_block_size,
              BlockAllocatorT,
              auto_free,
              BlockPointerArrayT>::deserialize(const char *buf,
                                               int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  reset();

  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    _OB_LOG(WARN, "failed to decode ob array count:ret[%d]", ret);
  } else if (OB_SUCCESS != (ret = prepare_allocate(count))) {
    OB_LOG(WARN, "failed to prepare allocate array space", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    T &item = at(i);
    if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
      _OB_LOG(WARN, "failed to decode array item:ret[%d]", ret);
    }
  }
  return ret;
}
}
}

#endif /* _OB_ARRAY_SERIALIZATION_H */
