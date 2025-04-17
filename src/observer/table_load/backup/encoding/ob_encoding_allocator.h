/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"
#include "lib/objectpool/ob_pool.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "ob_raw_decoder.h"
#include "ob_dict_decoder.h"
#include "ob_rle_decoder.h"
#include "ob_const_decoder.h"
#include "ob_integer_base_diff_decoder.h"
#include "ob_string_diff_decoder.h"
#include "ob_hex_string_decoder.h"
#include "ob_string_prefix_decoder.h"
#include "ob_column_equal_decoder.h"
#include "ob_inter_column_substring_decoder.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

template <typename EncodingItem>
struct ObEncodingPool
{
  static const int64_t MAX_FREE_ITEM_CNT = 64;
  ObEncodingPool(const int64_t item_size, const char *label);
  ~ObEncodingPool();

  template <typename T>
  inline int alloc(T *&item);
  inline void free(EncodingItem *item);

  EncodingItem* free_items_[MAX_FREE_ITEM_CNT];
  int64_t free_cnt_;
  common::ObPool<common::ObMalloc, common::ObNullLock> pool_;
};

extern int64_t decoder_sizes[];

template <typename EncodingItem>
class ObEncodingAllocator
{
public:
  typedef ObEncodingPool<EncodingItem> Pool;
  ObEncodingAllocator(const int64_t *size_array, const char *label);
  virtual ~ObEncodingAllocator() {}
  int init();
  bool is_inited() const { return inited_; }
  template<typename T>
  inline int alloc(T *&item);
  inline void free(EncodingItem *item);
private:
  int add_pool(Pool *pool);
  bool inited_;
  int64_t size_index_;
  Pool raw_pool_;
  Pool dict_pool_;
  Pool rle_pool_;
  Pool const_pool_;
  Pool int_diff_pool_;
  Pool str_diff_pool_;
  Pool hex_str_pool_;
  Pool str_prefix_pool_;
  Pool column_equal_pool_;
  Pool column_substr_pool_;
  Pool *pools_[ObColumnHeader::MAX_TYPE];
  int64_t pool_cnt_;
};

#include "ob_encoding_allocator.ipp"

typedef ObEncodingAllocator<ObIColumnDecoder> ObDecoderAllocator;

class ObDecoderAllocatorShell
{
public:
  ObDecoderAllocatorShell():
  allocator_(nullptr)
  {
    int ret = OB_SUCCESS;
    if (nullptr == (allocator_ = OB_NEW(ObDecoderAllocator, "Decoder",
        decoder_sizes, "Decoder"))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate ObDecoderAllocator failed", K(ret));
    } else if (OB_FAIL(allocator_->init())) {
      STORAGE_LOG(WARN, "allocator init failed", K(ret));
      common::ob_delete(allocator_);
    }
  }
  ~ObDecoderAllocatorShell()
  {
    if (nullptr != allocator_) {
      common::ob_delete(allocator_);
    }
  }
  ObDecoderAllocator *get_allocator()
  {
    return allocator_;
  }
private:
  ObDecoderAllocator *allocator_;
};

OB_INLINE ObDecoderAllocator *get_decoder_allocator()
{
  RLOCAL_INLINE(ObDecoderAllocatorShell, allcator_shell);
  return (&allcator_shell)->get_allocator();
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
