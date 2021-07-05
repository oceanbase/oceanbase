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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOCK_INDEX_INTERMEDIATE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOCK_INDEX_INTERMEDIATE_H_
#include "ob_block_sstable_struct.h"
#include "ob_data_buffer.h"
#include "storage/ob_i_store.h"
#include "ob_macro_block.h"

namespace oceanbase {
namespace common {
class ObObj;
}
namespace storage {
class ObStoreRow;
}
namespace blocksstable {

struct ObBlockIntermediateHeader {
  static const int64_t SF_BIT_VERSION = 8;
  static const int64_t SF_BIT_OFFSET = 24;
  static const int64_t SF_BIT_SIZE = 23;
  static const int64_t SF_BIT_IS_LEAF_NODE = 1;
  static const int64_t SF_BIT_RESERVED = 8;
  static const int64_t MAX_VERSION = (1 << SF_BIT_VERSION) - 1;
  static const int64_t MAX_OFFSET = (1 << SF_BIT_OFFSET) - 1;
  static const int64_t MAX_SIZE = (1 << SF_BIT_SIZE) - 1;
  static const int64_t MAX_IS_LEAF_NODE = (1 << SF_BIT_IS_LEAF_NODE) - 1;
  static const int64_t INTERMEDIATE_HEADER_VERSION = 1;
  union {
    int64_t desc_;
    struct {
      uint32_t version_ : SF_BIT_VERSION;
      uint32_t offset_ : SF_BIT_OFFSET;  // lead_node:1: micro block index offset, lead_node:0: micro block data offset
      uint32_t size_ : SF_BIT_SIZE;
      uint32_t is_leaf_node_ : SF_BIT_IS_LEAF_NODE;
      uint32_t reserved_ : SF_BIT_RESERVED;  // reserved
    };
  };
  int64_t row_count_;
  int64_t macro_version_;
  int64_t macro_logic_id_;

  TO_STRING_KV(K_(version), K_(offset), K_(size), K_(is_leaf_node), K_(reserved), K_(row_count), K_(macro_version),
      K_(macro_logic_id));
  ObBlockIntermediateHeader();
  ObBlockIntermediateHeader(const int64_t macro_version, const int64_t macro_logic_id, const int32_t offset,
      const int32_t size, const int64_t row_count, const bool is_leaf_node);
  virtual ~ObBlockIntermediateHeader();
  OB_INLINE void reset()
  {
    desc_ = 0;
    version_ = INTERMEDIATE_HEADER_VERSION;
    row_count_ = 0;
    macro_version_ = 0;
    macro_logic_id_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return INTERMEDIATE_HEADER_VERSION == version_ && row_count_ > 0 && size_ > 0;
  }
  OB_INLINE void set_offset(const int32_t offset)
  {
    offset_ = offset & MAX_OFFSET;
  }
  OB_INLINE void set_size(const int32_t size)
  {
    size_ = size & MAX_SIZE;
  }
  OB_INLINE void set_leaf_node(const bool is_leaf_node)
  {
    is_leaf_node_ = is_leaf_node & MAX_IS_LEAF_NODE;
  }
  OB_INLINE bool is_leaf_node() const
  {
    return is_leaf_node_ != 0;
  }
};

class ObBlockIntermediateBuilder {
public:
  ObBlockIntermediateBuilder();
  virtual ~ObBlockIntermediateBuilder();
  void reset();
  void reuse();
  int init(const int64_t rowkey_column_count);
  int build_intermediate_row(const ObBlockIntermediateHeader& header, const storage::ObStoreRow*& row);
  int set_rowkey(const ObStoreRowkey& rowkey);
  int set_rowkey(const ObDataStoreDesc& desc, const ObString& s_rowkey);

private:
  ObBlockIntermediateHeader header_;
  storage::ObStoreRow intermediate_row_;  // rowkey_, buf_
  common::ObArenaAllocator allocator_;
  ObFlatRowReader row_reader_;
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj)];
  int64_t rowkey_column_count_;
  bool is_inited_;
};

class ObBlockIntermediateRowParser {
public:
  ObBlockIntermediateRowParser();
  virtual ~ObBlockIntermediateRowParser();
  int init(const int64_t rowkey_column_count, const storage::ObStoreRow& row);
  int get_intermediate_header(const ObBlockIntermediateHeader*& header);

private:
  storage::ObStoreRow* intermediate_row_;
  ObBlockIntermediateHeader* header_;
  int64_t rowkey_column_count_;
  bool is_inited_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
