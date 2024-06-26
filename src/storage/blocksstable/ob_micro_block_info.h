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

#ifndef SRC_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_INFO_H_
#define SRC_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_INFO_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObMicroBlockInfo
{
  static const int64_t SF_BIT_OFFSET = 24;
  static const int64_t MAX_OFFSET = (1 << (SF_BIT_OFFSET - 1)) - 1;
  static const int64_t SF_BIT_SIZE = 24;
  static const int64_t MAX_SIZE = (1 << (SF_BIT_SIZE - 1)) - 1;
  static const int64_t SF_BIT_MARK_DELETION = 1;
  static const uint64_t MAX_MARK_DELETION = (1 << (SF_BIT_MARK_DELETION)) - 1;
  static const int64_t SF_BIT_RESERVED = 15;

  struct
  {
    int32_t offset_ : SF_BIT_OFFSET;
    int32_t size_ : SF_BIT_SIZE;
    bool mark_deletion_ : SF_BIT_MARK_DELETION;
    uint16_t reserved_: SF_BIT_RESERVED;
  };
  ObMicroBlockInfo() : offset_(0), size_(0), mark_deletion_(false), reserved_(0) {}

  int64_t get_block_size() const
  { return size_; }
  int64_t get_block_offset() const
  { return  offset_; }
  void reset() { offset_ = 0; size_ = 0; mark_deletion_ = false; reserved_ = 0; }
  int set(const int32_t offset, const int32_t size, bool mark_deletion = false)
  {
    int ret = common::OB_SUCCESS;
    if (!is_offset_valid(offset) || !is_size_valid(size)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Setting invalid value, it may be caused by overflow when converting type",
          K(ret), K(offset), K(size));
    } else {
      offset_ = offset & MAX_OFFSET;
      size_ = size & MAX_SIZE;
      mark_deletion_ = mark_deletion & MAX_MARK_DELETION;
    }
    return ret;
  }
  bool is_offset_valid(const int32_t offset) const
  {
    return offset >= 0 && offset <= MAX_OFFSET;
  }
  bool is_size_valid(const int32_t size) const
  {
    return size > 0 && size <= MAX_SIZE;
  }
  bool is_valid() const { return offset_ >=0 && size_ > 0; }
  TO_STRING_KV(K_(offset), K_(size), K_(mark_deletion));
};
}
}

#endif /* SRC_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_INFO_H_ */
