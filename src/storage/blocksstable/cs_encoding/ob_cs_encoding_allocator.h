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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_CS_ENCODING_ALLOCATOR_H_
#define OCEANBASE_BLOCKSSTABLE_OB_CS_ENCODING_ALLOCATOR_H_

#include "lib/objectpool/ob_pool.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "ob_column_encoding_struct.h"
#include "ob_icolumn_cs_encoder.h"
#include "ob_icolumn_cs_decoder.h"
#include "ob_integer_column_encoder.h"
#include "ob_string_column_encoder.h"
#include "ob_int_dict_column_encoder.h"
#include "ob_str_dict_column_encoder.h"
#include "ob_integer_column_decoder.h"
#include "ob_string_column_decoder.h"
#include "ob_int_dict_column_decoder.h"
#include "ob_str_dict_column_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

template <typename EncodingItem>
struct ObCSEncodingPool
{
  static const int64_t MAX_FREE_ITEM_CNT = 64;
  ObCSEncodingPool(const int64_t item_size, const ObMemAttr &attr);
  ~ObCSEncodingPool();

  template <typename T>
  inline int alloc(T *&item);
  inline void free(EncodingItem *item);

  EncodingItem* free_items_[MAX_FREE_ITEM_CNT];
  int64_t free_cnt_;
  common::ObPool<common::ObMalloc, common::ObNullLock> pool_;
};

extern int64_t cs_encoder_sizes[];
extern int64_t cs_decoder_sizes[];

template <typename EncodingItem>
class ObCSEncodingAllocator
{
public:
  typedef ObCSEncodingPool<EncodingItem> Pool;
  ObCSEncodingAllocator(const int64_t *size_array, const ObMemAttr &attr);
  virtual ~ObCSEncodingAllocator() {}
  int init();
  bool is_inited() const { return inited_; }
  template<typename T>
  inline int alloc(T *&item);
  inline void free(EncodingItem *item);
private:
  int add_pool(Pool *pool);
  bool inited_;
  int64_t size_index_;
  Pool integer_pool_;
  Pool string_pool_;
  Pool int_dict_pool_;
  Pool str_dict_pool_;
  Pool *pools_[ObCSColumnHeader::MAX_TYPE];
  int64_t pool_cnt_;
};

#include "ob_cs_encoding_allocator.ipp"

typedef ObCSEncodingAllocator<ObIColumnCSEncoder> ObCSEncoderAllocator;

}//end namespace blocksstable
}//end namespace oceanbase

#endif //OCEANBASE_ENCODING_OB_CS_ENCODING_ALLOCATOR_H_
