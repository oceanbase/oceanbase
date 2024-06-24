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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_ENCODING_ALLOCATOR_H_
#define OCEANBASE_BLOCKSSTABLE_OB_ENCODING_ALLOCATOR_H_

#include "lib/objectpool/ob_pool.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_raw_encoder.h"
#include "ob_dict_encoder.h"
#include "ob_rle_encoder.h"
#include "ob_const_encoder.h"
#include "ob_integer_base_diff_encoder.h"
#include "ob_string_diff_encoder.h"
#include "ob_hex_string_encoder.h"
#include "ob_string_prefix_encoder.h"
#include "ob_column_equal_encoder.h"
#include "ob_inter_column_substring_encoder.h"
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
namespace blocksstable
{

template <typename EncodingItem>
struct ObEncodingPool
{
  static const int64_t MAX_FREE_ITEM_CNT = 64;
  ObEncodingPool(const int64_t item_size, const ObMemAttr &attr);
  ~ObEncodingPool();

  template <typename T>
  inline int alloc(T *&item);
  inline void free(EncodingItem *item);

  EncodingItem* free_items_[MAX_FREE_ITEM_CNT];
  int64_t free_cnt_;
  common::ObPool<common::ObMalloc, common::ObNullLock> pool_;
};

extern int64_t encoder_sizes[];
extern int64_t decoder_sizes[];

template <typename EncodingItem>
class ObEncodingAllocator
{
public:
  typedef ObEncodingPool<EncodingItem> Pool;
  ObEncodingAllocator(const int64_t *size_array, const ObMemAttr &attr);
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

typedef ObEncodingAllocator<ObIColumnEncoder> ObEncoderAllocator;

}//end namespace blocksstable
}//end namespace oceanbase

#endif //OCEANBASE_ENCODING_OB_ENCODING_ALLOCATOR_H_
