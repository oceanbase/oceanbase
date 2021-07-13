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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_MICRO_BLOCK_INDEX_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_MICRO_BLOCK_INDEX_READER_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace blocksstable {

struct ObLobMicroIndexSizeItem {
  TO_STRING_KV(K_(char_size), K_(byte_size));
  uint64_t char_size_;  // micro block char size
  uint64_t byte_size_;  // uncompressed micro block byte size
};

struct ObLobMicroIndexItem {
  TO_STRING_KV(K_(offset), K_(data_size), K_(size_item));
  int32_t offset_;     // micro block data offset
  int32_t data_size_;  // micro block size after compressed
  struct ObLobMicroIndexSizeItem size_item_;
};

class ObLobMicroBlockIndexReader {
public:
  ObLobMicroBlockIndexReader();
  virtual ~ObLobMicroBlockIndexReader() = default;
  int transform(const char* index_buf, const int32_t size_array_offset, const int64_t micro_block_cnt);
  int get_next_index_item(ObLobMicroIndexItem& next_item);

private:
  const int32_t* offset_array_;
  const ObLobMicroIndexSizeItem* size_array_;
  int64_t micro_block_cnt_;
  int64_t cursor_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_MICRO_BLOCK_INDEX_READER_H_
