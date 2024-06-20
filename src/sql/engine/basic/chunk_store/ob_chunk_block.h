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

#ifndef OCEANBASE_BASIC_OB_CHUNK_BLOCK_H_
#define OCEANBASE_BASIC_OB_CHUNK_BLOCK_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace storage {
  class ObColumnSchemaItem;
}
namespace sql
{

struct ChunkRowHeader final
{
public:
  ChunkRowHeader() : offset_len_(4), has_null_(false), reserved_(0) {}
public:
  uint32_t row_size_;
  union {
    struct {
      uint32_t offset_len_    : 3;
      uint32_t has_null_      : 1;
      uint32_t reserved_      : 28;
    };
    uint32_t flag_;
  };
};

struct ChunkRowMeta final
{
  OB_UNIS_VERSION_V(1);
public:
  ChunkRowMeta(common::ObIAllocator &alloc) : allocator_(alloc), col_cnt_(0), extra_size_(0),
                                         fixed_cnt_(0), fixed_offsets_(NULL), projector_(NULL),
                                         nulls_off_(0), var_offsets_off_(0), extra_off_(0),
                                         fix_data_off_(0), var_data_off_(0),
                                         column_length_(alloc), column_offset_(alloc)
  {
  }
  int init(const ObExprPtrIArray &exprs, const int32_t extra_size);
  int init(const ObIArray<storage::ObColumnSchemaItem> &col_array, const int32_t extra_size);
  int32_t get_row_fixed_size() const { return sizeof(ChunkRowHeader) + var_data_off_; }
  int32_t get_var_col_cnt() const { return col_cnt_ - fixed_cnt_; }
  int32_t get_fixed_length(const int64_t idx) const
  {
    return fixed_offsets_[idx + 1] - fixed_offsets_[idx];
  }

  // todo: make allocator as pointer and free memory when row_meta destroyed

public:
  common::ObIAllocator &allocator_;
  int32_t col_cnt_;
  int32_t extra_size_;

  int32_t fixed_cnt_;
  int32_t *fixed_offsets_;

  int32_t *projector_;

  // start pos of those offset is payload
  int32_t nulls_off_;
  int32_t var_offsets_off_;
  int32_t extra_off_;
  int32_t fix_data_off_;
  int32_t var_data_off_;

  ObFixedArray<int32_t, ObIAllocator> column_length_;
  ObFixedArray<int32_t, ObIAllocator> column_offset_;
};

class WriterBufferHandler final
{
  // used for DefaultWriter and CompactWriter.
  // provide API to add data to buffer, and manage memory;
public:
  WriterBufferHandler(ObTempBlockStore *store = nullptr) : store_(store),  buf_(nullptr), buf_size_(0), cur_pos_(0), row_cnt_(0), inner_alloc_() {};
  virtual ~WriterBufferHandler() { reset(); };

  int init(const int64_t buf_size);
  int resize(const int64_t size);

  int write_data(char *data, const int64_t data_size);

  void reuse()
  {
    MEMSET(buf_, 0, buf_size_);
    cur_pos_ = 0;
    row_cnt_ = 0;
  }

  void reset()
  {
    free_buffer();
    buf_size_ = 0;
    cur_pos_ = 0;
    row_cnt_ = 0;
    store_ = nullptr;
  }

  void free_buffer();
  int64_t get_remain() { return buf_size_ - cur_pos_; }
  char* get_buf() { return buf_; }
  char* get_cur_data() { return buf_ + cur_pos_; }
  int64_t& get_cur_pos() { return cur_pos_; }
  int64_t get_size() { return cur_pos_; }
  int64_t get_max_size() { return buf_size_; }
  int64_t get_row_cnt() { return row_cnt_; }
  bool is_empty() { return cur_pos_ == 0; }
  inline int advance(const int64_t size)
  {
    int ret = common::OB_SUCCESS;
    if (size < -cur_pos_) {
      //overflow
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(size), K_(cur_pos));
    } else if (size > get_remain()) {
      ret = common::OB_BUF_NOT_ENOUGH;
      SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", get_remain());
    } else {
      cur_pos_ += size;
      row_cnt_++;
    }
    return ret;
  };

private:
  ObTempBlockStore *store_;
  char * buf_;
  int64_t buf_size_;
  int64_t cur_pos_;
  int64_t row_cnt_;
  ObArenaAllocator inner_alloc_;
};

struct BatchCtx final
{
public:
  BatchCtx() : datums_(nullptr), stored_rows_(nullptr), row_size_array_(nullptr), selector_(nullptr) {}
  ~BatchCtx() {}
public:
  const ObDatum **datums_;
  ObChunkDatumStore::StoredRow **stored_rows_;
  uint32_t *row_size_array_;
  uint16_t *selector_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_CHUNK_BLOCK_H_
