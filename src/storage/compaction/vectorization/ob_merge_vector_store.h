/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_COMPACTION_VECTORIZATION_OB_MERGE_VECTOR_STORE_H_
#define OB_STORAGE_COMPACTION_VECTORIZATION_OB_MERGE_VECTOR_STORE_H_

#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "common/object/ob_obj_type.h"
#include "storage/compaction/vectorization/ob_compaction_vector.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/compaction/ob_i_compaction_filter.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIMicroBlockRowScanner;
class ObIMicroBlockReader;
}
namespace compaction
{
struct ObStaticMergeParam;
struct ObMergeVectorStoreLayoutParam
{
  ObMergeVectorStoreLayoutParam();
  virtual ~ObMergeVectorStoreLayoutParam() { destroy(); }
  void destroy();
  bool is_valid() const;
  int64_t get_compaction_batch_size() const;
  int64_t get_merge_snapshot_version() const;

  int64_t column_count_;
  int64_t rowkey_column_cnt_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const compaction::ObStaticMergeParam *static_param_;

  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
};

class ObMergeVectorStore
{
public:
  ObMergeVectorStore();
  ~ObMergeVectorStore();

  int init(const ObMergeVectorStoreLayoutParam &layout_param,
           const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const bool is_continuous = true,
           const blocksstable::ObDatumRow *default_row = nullptr);

  void reset();
  void reuse();

  int append_row(
      const blocksstable::ObDatumRow &row,
      const bool need_check_order = true,
      const common::ObIArrayWrap<uint16_t> *cols = nullptr,
      const bool is_incremental = false);
  int append_batch(
      const ObMergeVectorStore &other,
      int64_t &batch_idx,
      const common::ObIArrayWrap<uint16_t> *cols = nullptr,
      ObCompactionFilterHandle *filter_handle = nullptr,
      const bool is_incremental = false);
  int fill_rows(
      const common::ObIArrayWrap<uint16_t> *cols,
      blocksstable::ObIMicroBlockRowScanner &scanner,
      int64_t &begin_index,
      const int64_t end_index);
  int fill_rows_from_reader(
      const common::ObIArrayWrap<uint16_t> *cols,
      blocksstable::ObIMicroBlockReader &reader,
      int64_t &begin_index,
      const int64_t end_index);
  int get_batch_datum_rows(blocksstable::ObBatchDatumRows &batch_rows);
  int get_datum_row(const int64_t row_idx, blocksstable::ObDatumRow &datum_row, const common::ObIArrayWrap<uint16_t> *cols = nullptr) const;

  OB_INLINE int64_t get_row_count() const { return has_single_row() ? 1 : row_count_; }
  OB_INLINE int64_t get_column_count() const { return nullptr == layout_param_ ? 0 : layout_param_->column_count_; }
  OB_INLINE bool is_full() const { return row_count_ >= get_batch_capacity() || total_mem_usage_ > mem_limit_; }
  OB_INLINE bool is_empty() const { return row_count_ == 0; } // only means no rows in vectors, not single row
  OB_INLINE bool is_continuous() const { return mem_limit_ != UINT64_MAX; }
  OB_INLINE int64_t get_batch_capacity() const { return layout_param_->get_compaction_batch_size(); }
  OB_INLINE bool need_flush() const { return need_force_flush_ || is_full(); }
  OB_INLINE void set_need_flush() { need_force_flush_ = true; }
  OB_INLINE int64_t get_incremental_row_count() const { return incremental_row_count_; }

  // Single-row accessors: used when batch scan is not available and only one row is returned.
  int set_single_row(const blocksstable::ObDatumRow *row);
  OB_INLINE const blocksstable::ObDatumRow *get_single_row() const { return single_row_; }
  OB_INLINE bool has_single_row() const { return nullptr != single_row_; }
  TO_STRING_KV(K_(is_inited), KPC_(layout_param), K_(row_count),
               K_(total_mem_usage), K_(mem_limit),
               K_(need_force_flush), KPC_(single_row));

public:
  static const int64_t DEFAULT_BATCH_BUFFER_LIMIT = 1L * 1024 * 1024; // 1M // TODO
  typedef common::ObSEArray<compaction::ObCompactionVector *, 16> VectorArray;

private:
  OB_INLINE const blocksstable::ObStorageDatum &datum_or_default(
      const blocksstable::ObStorageDatum &src_datum,
      const int64_t default_idx) const
  {
    const bool has_default = default_datums_.count() == get_column_count();
    return (has_default && src_datum.is_nop()) ? default_datums_.at(default_idx) : src_datum;
  }
  OB_INLINE const common::ObIArray<blocksstable::ObStorageDatum> *get_default_datums() const
  {
    return default_datums_.count() == get_column_count() ? &default_datums_ : nullptr;
  }
  int init_vectors(const common::ObIArray<share::schema::ObColDesc> &col_descs, const bool is_continuous);
  int init_default_datums(const blocksstable::ObDatumRow *default_row);
  int append_datum_to_vector(const int64_t col_idx, const blocksstable::ObStorageDatum &datum);

  int fill_last_rowkey(const int64_t append_rows = 1);
  int check_order(const blocksstable::ObDatumRow &cur_row, const blocksstable::ObDatumRowkey &last_rowkey);
  int get_row_ids(
      const int64_t begin_index,
      const int64_t row_count);
  int get_batch_row_count(
      int64_t begin_index,
      const int64_t end_index,
      int64_t &row_count);
  int inner_fill_rows_from_reader(
      const common::ObIArrayWrap<uint16_t> *cols,
      blocksstable::ObIMicroBlockReader *reader,
      const int64_t begin_index,
      const int64_t row_cap);
  int is_row_filtered(const int64_t idx, ObCompactionFilterHandle *filter_handle, bool &is_filtered);

private:
  bool is_inited_;
  bool need_force_flush_; // Whether the store contains shallow pointers that must be flushed before switching micro blocks.
  const blocksstable::ObDatumRow *single_row_; // Non-null when a single row is returned instead of a vector batch.
  const ObMergeVectorStoreLayoutParam *layout_param_;
  int64_t row_count_; // row count usage
  int64_t total_mem_usage_; // mem usage
  int64_t mem_limit_; // mem limit
  int64_t incremental_row_count_; // incremental row count
  // for check_order: previous row's schema rowkey shallow refs (see fill_last_rowkey)
  blocksstable::ObDatumRow last_row_;

  VectorArray vectors_;

  // Memory management
  compaction::ObLocalArena allocator_;
  compaction::ObLocalArena vector_allocator_; // only for vectors

  // Temporary buffers for batch operations
  const char **cell_data_ptrs_;
  uint32_t *len_array_;
  int32_t *row_ids_;
  blocksstable::ObDatumRow row_buf_;
  common::ObFixedArray<blocksstable::ObStorageDatum, common::ObIAllocator> default_datums_;

  DISALLOW_COPY_AND_ASSIGN(ObMergeVectorStore);
};

// A small helper that groups the read/write vector stores used by batch merge.
// - read_store: typically "not continuous" to avoid large continuous allocations
// - write_store: typically "continuous" for efficient append/write
class ObMergeVectorStorePair
{
public:
  ObMergeVectorStorePair() : read_store_(), write_store_() {}
  ~ObMergeVectorStorePair() { reset(); }

  int init(const ObMergeVectorStoreLayoutParam &layout_param,
           const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const blocksstable::ObDatumRow *default_row = nullptr);
  void reset();

  OB_INLINE ObMergeVectorStore &read_store() { return read_store_; }
  OB_INLINE ObMergeVectorStore &write_store() { return write_store_; }

  TO_STRING_KV(K_(read_store), K_(write_store));
private:
  ObMergeVectorStore read_store_;
  ObMergeVectorStore write_store_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeVectorStorePair);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_VECTORIZATION_OB_MERGE_VECTOR_STORE_H_
