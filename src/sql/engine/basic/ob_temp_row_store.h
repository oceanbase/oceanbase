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

#ifndef OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_
#define OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_

#include "share/ob_define.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace sql
{

class ObTempRowStore : public ObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
public:
  class Iterator;
  // RowBlock provides functions for writing and reading, and does not occupy memory
  struct RowBlock : public Block
  {
    int add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row);
    int get_store_row(int64_t &cur_pos, const ObCompactRow *&sr) const;
  };

  const static int64_t BLOCK_SIZE = (64L << 10);
  class Iterator : public ObTempBlockStore::BlockReader
  {
  public:
    friend struct RowBlock;
    friend class ObTempRowStore;
    Iterator() : ObTempBlockStore::BlockReader(), row_store_(NULL), cur_blk_(NULL),
                 cur_blk_id_(0), row_idx_(0), read_pos_(0) {}
    virtual ~Iterator() {}

    int init(ObTempRowStore *store);
    bool is_valid() { return nullptr != row_store_; }
    inline bool has_next() const { return cur_blk_id_ < get_row_cnt(); }
    inline int64_t get_row_cnt() const { return row_store_->get_row_cnt(); }
    int get_next_row(const ObCompactRow *&stored_rows);
    void reset()
    {
      cur_blk_id_ = 0;
      row_idx_ = 0;
      read_pos_ = 0;
      cur_blk_ = NULL;
      ObTempBlockStore::BlockReader::reset();
    }

  private:
    int next_block();

  private:
    ObTempRowStore *row_store_;
    const RowBlock *cur_blk_;
    int64_t cur_blk_id_; // block id(row_id) for iterator, from 0 to row_cnt_
    int32_t row_idx_; // current row index in reader block
    int64_t read_pos_; // current memory read position in reader block
  };

public:
  explicit ObTempRowStore(common::ObIAllocator *alloc = nullptr)
   : ObTempBlockStore(alloc), cur_blk_(NULL)
  {
  }
  virtual ~ObTempRowStore()
  {
    destroy();
  }

  void destroy()
  {
    reset();
  }

  void reset()
  {
    ObTempBlockStore::reset();
  }

  int init(const int64_t mem_limit,
           bool enable_dump,
           const uint64_t tenant_id,
           const int64_t mem_ctx_id,
           const char *label,
           const common::ObCompressorType compressor_type /*NONE_COMPRESSOR*/,
           const bool enable_trunc /*false*/);


  int begin(Iterator &it)
  {
    return it.init(this);
  }


  int add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row);
  inline int ensure_write_blk(const int64_t mem_size)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == cur_blk_ || mem_size > cur_blk_->remain()) {
      if (OB_FAIL(new_block(mem_size))) {
        SQL_ENG_LOG(WARN, "fail to new block", K(ret), K(mem_size));
      } else {
        cur_blk_ = static_cast<RowBlock *>(blk_);
      }
    }
    return ret;
  }

  inline int64_t get_row_cnt() const { return block_id_cnt_; }
  inline int64_t get_row_cnt_on_disk() const { return dumped_block_id_cnt_; }
  inline int64_t get_row_cnt_in_memory() const { return get_row_cnt() - get_row_cnt_on_disk(); }

  INHERIT_TO_STRING_KV("ObTempBlockStore", ObTempBlockStore,
                       K_(mem_attr));


private:
  lib::ObMemAttr mem_attr_;
  RowBlock *cur_blk_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_
