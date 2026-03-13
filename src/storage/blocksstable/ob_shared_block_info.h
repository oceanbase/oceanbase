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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_SHARED_BLOCK_INFO_H_
#define OCEANBASE_BLOCKSSTABLE_OB_SHARED_BLOCK_INFO_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObBlockInfo
{
public:
  ObBlockInfo()
    : nested_size_(OB_DEFAULT_MACRO_BLOCK_SIZE), nested_offset_(0), macro_id_()
  {
  }
  ObBlockInfo(const ObBlockInfo &other)
    : nested_size_(other.nested_size_), nested_offset_(other.nested_offset_), macro_id_(other.macro_id_)
  {
  }
  ~ObBlockInfo();
  void reset();
  bool is_valid() const;
  bool is_small_sstable() const;
  ObBlockInfo& operator =(const ObBlockInfo &other)
  {
    nested_size_ = other.nested_size_;
    nested_offset_ = other.nested_offset_;
    macro_id_ = other.macro_id_;
    return *this;
  }
  TO_STRING_KV(K_(nested_size), K_(nested_offset), K_(macro_id));
public:
  int64_t nested_size_;
  int64_t nested_offset_;
  MacroBlockId macro_id_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif //OCEANBASE_BLOCKSSTABLE_OB_SHARED_BLOCK_INFO_H_