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

#ifndef OCEANBASE_COMMON_OB_ROW_STORE_H
#define OCEANBASE_COMMON_OB_ROW_STORE_H
#include <stdint.h>
#include <utility>
#include "common/row/ob_row.h"
#include "common/row/ob_row_iterator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/string/ob_string.h"
#include "common/cell/ob_cell_writer.h"

namespace oceanbase
{
namespace common
{
class ObRowStore
{
public:
  struct BlockInfo;
  //What is reserved_cells_ for?
  //In order by c1 and other calculations, not only must all the columns be written to the ObRowStore, but also need to be sorted according to c1 after the writing is completed
  //If reserved_cells_ is not set, then the entire StoreRow needs to be deserialized to obtain c1
  //Adding reserved_cells_ is an optimization
  struct StoredRow
  {
    int32_t compact_row_size_;
    int32_t reserved_cells_count_;
    int64_t payload_;
    common::ObObj reserved_cells_[0];
    // ... compact_row
    const common::ObString get_compact_row() const;
    void *get_compact_row_ptr();
    const void *get_compact_row_ptr() const;
    TO_STRING_KV(N_PAYLOAD, payload_,
                 N_CELLS, ObArrayWrap<ObObj>(reserved_cells_, reserved_cells_count_));
  };
  class Iterator : public ObOuterRowIterator
  {
  public:
    friend class ObRowStore;
    Iterator();
    /// @param row [in/out] row.size_ is used to verify the data
    int get_next_row(ObNewRow &row);
    int get_next_row(ObNewRow &row, common::ObString *compact_row, StoredRow **stored_row);
    int get_next_row(ObNewRow *&row, StoredRow **stored_row);
    int get_next_stored_row(StoredRow *&stored_row);
    void reset();
    bool is_valid() const { return row_store_ != NULL; }
    void set_invalid();
    // @pre is_valid
    bool has_next() const;
  private:
    explicit Iterator(const ObRowStore *row_store);
    int next_iter_pos(const BlockInfo *&iter_block, int64_t &iter_pos);
  protected:
    const ObRowStore *row_store_;
    const BlockInfo *cur_iter_block_;
    int64_t cur_iter_pos_;
  };
public:
  ObRowStore(ObIAllocator &alloc,
             const lib::ObLabel &label = ObModIds::OB_SQL_ROW_STORE,
             const uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
             bool use_compact_row = true);
  ObRowStore(const lib::ObLabel &label = ObModIds::OB_SQL_ROW_STORE,
             const uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
             bool use_compact_row = true);
  ~ObRowStore();
  void set_label(const lib::ObLabel &label) { label_ = label; }
  lib::ObLabel get_label() const { return label_; }
  void set_ctx_id(const int64_t ctx_id) { ctx_id_ = ctx_id; }

  /// set normal block size
  void set_block_size(int64_t block_size = NORMAL_BLOCK_SIZE) { block_size_ = block_size; }
  int64_t get_block_size() const { return block_size_; }

  /// add columns to be reserved as ObObj
  int add_reserved_column(int64_t index);
  int init_reserved_column_count(int64_t count)
  {
    return reserved_columns_.init(count);
  }
  int64_t get_reserved_column_count() const { return reserved_columns_.count(); }

  void reset();
  void reuse() { reset(); }
  void reuse_hold_one_block();
  /**
   * clear rows only, keep reserved columns info
   */
  void clear_rows();
  /**
   * add row into the store, if row has projector, will store cell with projector
   *
   * @param row [in]
   * @param sort_row [out] stored row
   * @param payload [in] payload data interpreted by user
   * @param by_projector[in], store cell by row projector index
   *
   * @return error code
   */
  int add_row(const ObNewRow &row, const StoredRow *&stored_row, int64_t payload, bool by_projector);
  /**
   * add row into the store
   * @param row
   * @param by_projector, if true, and the row has projector, will store cell with projector index
   * otherwise, will store cell by real cell index
   */
  int add_row(const ObNewRow &row, bool by_projector = true);
  /**
   * rollback the last added row
   *
   * @note can only be called once.
   * @retval OB_NOT_SUPPORTED if there's no row can be rollbacked
   */
  int rollback_last_row();
  /**
   * Peek the lasted added row.
   * If the added row was rollbacked, the previous last row can also be got
   * @param stored_row
   *
   * @retval OB_ENTRY_NOT_EXIST if the store is empty
   */
  int get_last_row(const StoredRow *&stored_row) const;

  int assign_block(char *buf, int64_t block_size);

  bool is_empty() const { return blocks_.is_empty(); }
  bool is_read_only() const { return is_read_only_; }
  bool use_compact_row() const { return use_compact_row_; }
  inline int64_t get_used_mem_size() const { return blocks_.get_used_mem_size(); }
  inline int64_t get_block_count() const { return blocks_.get_block_count(); }
  inline int64_t get_data_size() const { return data_size_; }
  inline int64_t get_row_count() const { return row_count_; }
  inline int64_t get_col_count() const { return col_count_; }
  int set_col_count(int64_t col_count)
  {
    int ret = OB_SUCCESS;
    if(col_count < 1) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "column count is less than 1", K(col_count));
    } else {
      col_count_ = col_count;
    }
    return ret;
  }
  /// get iterator
  Iterator begin() const;

  //@{  KVStoreCache Component, @see ob_kv_storecache.h
  /// size needed by KVStoreCache to copy this
  int32_t get_copy_size() const;
  /// size needed by KVStoreCache to copy this meta
  int32_t get_meta_size() const { return static_cast<int32_t>(sizeof(*this)); }
  /**
   * clone this row store into buffer
   * @pre 0 == reserved_columns_.count()
   * @param buffer [out] where to store the new clone
   *
   * @return the new clone
   */
  ObRowStore *clone(char *buffer, int64_t buffer_length) const;
  //@}

  int assign(const ObRowStore &other_store);
  template<typename T>
  int append_row_store(const T &other_store);
  /// dump all data for debug purpose
  void dump() const;

  // set tenant id
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }

  void set_use_compact(bool opt) { use_compact_row_ = opt; }



  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(N_BLOCK_SIZE, block_size_,
               N_DATA_SIZE, data_size_,
               N_BLOCK_NUM, blocks_.get_block_count(),
               N_ROW_COUNT, row_count_,
               N_COLUMN_COUNT, col_count_,
               N_READ_ONLY, is_read_only_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObRowStore);

  static const int64_t BIG_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t NORMAL_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

  // non-circular doubly linked list
  class BlockList
  {
  public:
    BlockList();
    void reset();
    int add_last(BlockInfo *block);
    BlockInfo *pop_last();
    BlockInfo *get_first() { return first_; }
    BlockInfo *get_last() { return last_; }
    const BlockInfo *get_first() const { return first_; }
    const BlockInfo *get_last() const { return last_; }
    int64_t get_block_count() const { return count_; }
    int64_t get_used_mem_size() const { return used_mem_size_; }
    bool is_empty() const { return 0 == count_; }
  private:
    BlockInfo *first_;
    BlockInfo *last_;
    int64_t count_;
    int64_t used_mem_size_;  // bytes of all blocks
  };
private:
  static int64_t get_reserved_cells_size(const int64_t reserved_columns_count);
  int add_row_by_projector(const ObNewRow &row, const StoredRow *&stored_row, int64_t payload);
  int init_pre_project_row(int64_t count);
  int pre_project(const ObNewRow &row);
  BlockInfo* new_block(int64_t block_size);
  BlockInfo* new_block();
  int64_t get_compact_row_min_size(const int64_t row_columns_count) const;
  // @return OB_SIZE_OVERFLOW if buffer not enough
  int append_row(const ObNewRow &row, BlockInfo &block, StoredRow &stored_row);
  inline int append_extend_cell(ObCellWriter &cell_writer, const ObObj &cell);
  void free_rollbacked_block(BlockInfo *iter);
  BlockInfo *find_pre_block();
  void del_block_list(BlockInfo *del_block);
  int adjust_row_cells_reference();
private:
  // xiyu@TODO: add control for tenant_id
  DefaultPageAllocator inner_alloc_;
  ObFixedArray<int64_t, common::ObIAllocator> reserved_columns_;
  BlockList blocks_;  // ASSERT: all linked blocks has at least one row stored
  int64_t block_size_;  // normal block size
  int64_t data_size_;   // bytes of all rows
  int64_t row_count_;
  int64_t col_count_;
  int64_t last_last_row_size_;  // for rollback & get_last_row
  int64_t last_row_size_;     // for get_last_row, -1 means invalid
  uint64_t tenant_id_; // the tenant who owns the store
  lib::ObLabel label_;
  int64_t ctx_id_;
  bool is_read_only_;
  bool has_big_block_;
  bool use_compact_row_;
  common::ObIAllocator &alloc_;
  ObObj *pre_project_buf_;  //if compact row not used, we need to project row before deep copy
  ObNewRow pre_project_row_;
  bool pre_alloc_block_;
};

inline int64_t ObRowStore::get_reserved_cells_size(const int64_t reserved_columns_count)
{
  return sizeof(StoredRow) + (sizeof(common::ObObj) * reserved_columns_count);
}

//Every time I see this calculation, I feel entangled, I always feel unreasonable, and the calculation result is wrong.
//But in fact, this calculation result is just a reference to judge whether the remaining space in the current block is enough.
//Even if the judgment is passed this time, subsequent writing can still fail due to insufficient space, and it will try again at this time;
//so, let it go.
inline int64_t ObRowStore::get_compact_row_min_size(const int64_t row_columns_count) const
{
  // 4 ==  SUM( len(TypeAttr) = 1, len(int8) = 1, len(column id) = 2 )
  // 8 is a padding value/magic number,
  // try to avoid a useless deserialization when reaching the end of a block
  return 4 * row_columns_count + 8;
}

inline void *ObRowStore::StoredRow::get_compact_row_ptr()
{
  return reinterpret_cast<void*>(&reserved_cells_[reserved_cells_count_]);
}

inline const void *ObRowStore::StoredRow::get_compact_row_ptr() const
{
  return reinterpret_cast<const void*>(&reserved_cells_[reserved_cells_count_]);
}

inline const common::ObString ObRowStore::StoredRow::get_compact_row() const
{
  common::ObString ret;
  ret.assign_ptr(reinterpret_cast<const char *>(get_compact_row_ptr()), compact_row_size_);
  return ret;
}

template<typename T>
int ObRowStore::append_row_store(const T &other_store)
{
  int ret = OB_SUCCESS;
  if (other_store.get_row_count() > 0) {
    ObRowStore::Iterator row_store_it = other_store.begin();
    ObNewRow cur_row;
    int64_t col_count = other_store.get_col_count();
    ObObj *cell = NULL;
    if (OB_ISNULL(cell = static_cast<ObObj *>(alloca(sizeof(ObObj) * col_count)))) {
      COMMON_LOG(WARN, "fail to alloc obj array", K(ret));
    } else {
      cur_row.cells_ = cell;
      cur_row.count_ = col_count;
      while (true) {
        if (OB_FAIL(row_store_it.get_next_row(cur_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            COMMON_LOG(WARN, "fail to get next row", K(ret));
          }
          break;
        } else if (OB_FAIL(add_row(cur_row))) {
          COMMON_LOG(WARN, "fail to add row", K(ret));
          break;
        }
      }
    }
  }
  return ret;
}
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_ROW_STORE_H */
