/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_CG_ROW_TMP_FILE_H_
#define OCEANBASE_STORAGE_OB_CG_ROW_TMP_FILE_H_

#include "storage/ob_storage_schema.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "sql/engine/basic/ob_temp_column_store.h"

namespace oceanbase
{
namespace storage
{

struct ObDDLChunk;
struct ObChunk;

struct ObCGRowFile
{
public:
  ObCGRowFile() :
    is_opened_(false),
    is_start_iterate_(false),
    tablet_id_(),
    slice_idx_(-1),
    cg_idx_(-1),
    column_count_(0),
    allocator_(ObMemAttr(MTL_ID(), "CGRowFile")),
    store_(),
    bdrs_(),
    brs_(),
    iter_()
  {
    bdrs_.vectors_.set_block_allocator(ModulePageAllocator(allocator_));
  }
  ~ObCGRowFile()
  {
    if (is_opened_) {
      IGNORE_RETURN close();
    }
  }
  int open(const ObIArray<ObColumnSchemaItem>  &all_column_schema_its,
           const ObTabletID &tablet_id,
           const int64_t slice_idx,
           const int64_t cg_idx,
           const ObStorageColumnGroupSchema &cg_schema,
           const int64_t max_batch_size,
           const int64_t memory_limit = INT64_MAX,
           const int64_t dir_id = 0);
  int close();
  int append_batch(const blocksstable::ObBatchDatumRows &brs);
  int get_next_batch(blocksstable::ObBatchDatumRows *&bdrs);
  int dump(const bool all_dump, const int64_t target_dump_size = INT64_MAX);
  int finish_append_batch(bool need_dump);
  OB_INLINE int64_t get_mem_hold() const { return store_.get_mem_hold(); }
  OB_INLINE int64_t get_cg_idx() const { return cg_idx_; }
  OB_INLINE bool is_opened() const { return is_opened_; }
  TO_STRING_KV(K(is_opened_), K(tablet_id_), K(slice_idx_), K(cg_idx_), K(is_start_iterate_), K(column_count_), K(brs_));

private:
  int begin(sql::ObTempColumnStore::Iterator &iter, const bool async = true);
  DISALLOW_COPY_AND_ASSIGN(ObCGRowFile);

private:
  bool is_opened_;
  bool is_start_iterate_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  int64_t cg_idx_;
  int64_t column_count_;
  ObArenaAllocator allocator_;
  sql::ObTempColumnStore store_;
  blocksstable::ObBatchDatumRows bdrs_;
  sql::ObBatchRows brs_;
  sql::ObTempColumnStore::Iterator iter_;
};

class ObCGRowFilesGenerater
{
public:
  ObCGRowFilesGenerater() :
    is_inited_(false),
    is_generation_sync_output_(false),
    is_sorted_table_load_with_column_store_replica_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    storage_schema_(nullptr),
    max_batch_size_(0),
    cg_row_file_memory_limit_(INT64_MAX),
    all_column_schema_its_(),
    cg_row_file_arr_(),
    cg_row_file_arr_for_output_(nullptr),
    sync_chunk_data_(nullptr)
  {
    cg_row_file_arr_.set_attr(ObMemAttr(MTL_ID(), "CGRowFileArr"));
  }
  ~ObCGRowFilesGenerater()
  {
    reset();
  }
  void reset();
  int init(const ObTabletID &tablet_id,
           const int64_t slice_idx,
           ObStorageSchema *storage_schema,
           const int64_t max_batch_size,
           const int64_t cg_row_file_memory_limit,
           const ObIArray<ObColumnSchemaItem> &all_column_schema_its,
           const bool is_sync_generation,
           const bool is_sorted_table_load_with_column_store_replica);
  int append_batch(const blocksstable::ObBatchDatumRows &bdrs,
                   const bool is_slice_end,
                   ObDDLChunk &output_chunk);
  int try_generate_output_chunk(const bool is_slice_end,
                                ObDDLChunk &output_chunk);

public:
  static const int64_t CG_ROW_FILE_MEMORY_LIMIT = 512L * 1024L;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCGRowFilesGenerater);

private:
  bool is_inited_;
  bool is_generation_sync_output_;
  bool is_sorted_table_load_with_column_store_replica_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObStorageSchema *storage_schema_;
  int64_t max_batch_size_;
  int64_t cg_row_file_memory_limit_;
  ObArray<ObColumnSchemaItem> all_column_schema_its_;
  ObArray<ObCGRowFile *> cg_row_file_arr_;
  ObArray<ObCGRowFile *> *cg_row_file_arr_for_output_;
  ObChunk *sync_chunk_data_;
};

} // end namespace storage
} // end namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_CG_ROW_TMP_FILE_H_
