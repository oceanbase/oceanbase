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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_cg_row_tmp_file.h"
#include "sql/engine/ob_bit_vector.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_tablet_context.h"

namespace oceanbase
{
using namespace sql;
namespace storage
{
/**
* -----------------------------------ObCGRowFile-----------------------------------
*/
int ObCGRowFile::open(const ObIArray<ObColumnSchemaItem> &all_column_schema_its,
                      const ObTabletID &tablet_id,
                      const int64_t slice_idx,
                      const int64_t cg_idx,
                      const ObStorageColumnGroupSchema &cg_schema,
                      const int64_t max_batch_size,
                      const int64_t memory_limit,
                      const int64_t dir_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSchemaItem> column_schema_its;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObCGRowFile is opened already", K(ret));
  } else if (OB_UNLIKELY(all_column_schema_its.empty() ||
                         !tablet_id.is_valid() ||
                         slice_idx < 0 ||
                         cg_idx < 0 ||
                         cg_schema.get_column_count() <= 0 ||
                         max_batch_size <= 0 ||
                         memory_limit <= 0 ||
                         dir_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(all_column_schema_its.count()), K(tablet_id),
        K(slice_idx), K(cg_idx), K(cg_schema.get_column_count()), K(max_batch_size), K(memory_limit), K(dir_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_schema.get_column_count(); ++i) {
    const int64_t column_idx = cg_schema.get_column_idx(i);
    if (OB_UNLIKELY(column_idx < 0 || column_idx >= all_column_schema_its.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the column index is invalid",
          K(ret), K(column_idx), K(all_column_schema_its.count()));
    } else {
      const ObColumnSchemaItem &column_schema_it = all_column_schema_its.at(column_idx);
      if (OB_FAIL(column_schema_its.push_back(column_schema_it))) {
        LOG_WARN("fail to push back column schema item", K(ret), K(column_schema_it));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObCompressorType compressor_type = NONE_COMPRESSOR;
    const int64_t skip_size = ObBitVector::memory_size(max_batch_size);
    void *skip_mem = nullptr;
    int64_t tempstore_read_alignment_size = ObTempBlockStore::get_read_alignment_size_config(MTL_ID());
    if (OB_FAIL(ObTempColumnStore::init_vectors(column_schema_its, allocator_, bdrs_.vectors_))) {
      LOG_WARN("fail to initialize vectors", K(ret), K(column_schema_its));
    } else if (OB_FAIL(store_.init(bdrs_.vectors_,
                                   max_batch_size,
                                   ObMemAttr(MTL_ID(), "CGRowFileStore"),
                                   memory_limit,
                                   true/*enable_dump*/,
                                   compressor_type,
                                   tempstore_read_alignment_size))) {
      LOG_WARN("fail to initialize temp column store", K(ret));
    } else if (OB_ISNULL(skip_mem = allocator_.alloc(skip_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc skip buffer", K(ret), K(skip_size));
    } else {
      tablet_id_ = tablet_id;
      slice_idx_ = slice_idx;
      cg_idx_ = cg_idx;
      column_count_ = cg_schema.get_column_count();
      store_.set_dir_id(dir_id);
      store_.get_inner_allocator().set_tenant_id(MTL_ID());
      brs_.skip_ = to_bit_vector(skip_mem);
      brs_.skip_->reset(max_batch_size);
      brs_.size_ = 0;
      brs_.set_all_rows_active(true);
      bdrs_.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
      is_opened_ = true;
    }
  }
  return ret;
}

int ObCGRowFile::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFile is not opened", K(ret));
  } else {
    is_opened_ = false;
    column_count_ = 0;
    is_start_iterate_ = false;
    iter_.reset();
    store_.reset();
    bdrs_.reset();
    if (nullptr != brs_.skip_) {
      brs_.skip_->~ObBitVector();
      allocator_.free(brs_.skip_);
      brs_.skip_ = nullptr;
    }
    allocator_.reset();
  }
  return ret;
}

int ObCGRowFile::append_batch(const blocksstable::ObBatchDatumRows &bdrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFile is not opened", K(ret));
  } else if (OB_UNLIKELY(bdrs.vectors_.count() != column_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the column count is not equal to the batch datum rows's vector count",
        K(ret), K(bdrs.vectors_.count()), K(column_count_));
  } else {
    int64_t stored_row_count = 0;
    brs_.size_ = bdrs.row_count_;
    if (OB_FAIL(store_.add_batch(bdrs.vectors_, brs_, stored_row_count))) {
      LOG_WARN("fail to add batch", K(ret));
    } else if (OB_UNLIKELY(stored_row_count != bdrs.row_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the stored row count is not equal to the brs's row count",
          K(ret), K(stored_row_count), K(bdrs.row_count_));
    } else {
      is_start_iterate_ = false;
    }
  }
  return ret;
}

int ObCGRowFile::get_next_batch(blocksstable::ObBatchDatumRows *&bdrs)
{
  int ret = OB_SUCCESS;
  int64_t read_row_count = 0;
  bdrs = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFile is not opened", K(ret));
  } else if (!is_start_iterate_ && begin(iter_)) {
    LOG_WARN("fail to begin iterating", K(ret));
  } else if (OB_FAIL(iter_.get_next_batch(bdrs_.vectors_, read_row_count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next batch", KR(ret));
    }
  } else {
    bdrs_.row_count_ = read_row_count;
    bdrs = &bdrs_;
  }
  return ret;
}

int ObCGRowFile::dump(const bool all_dump, const int64_t target_dump_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFile is not opened", K(ret));
  } else if (OB_FAIL(store_.dump(all_dump, target_dump_size))) {
    LOG_WARN("fail to dump", K(ret), K(all_dump), K(target_dump_size));
  }
  return ret;
}

int ObCGRowFile::finish_append_batch(bool need_dump)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFile is not opened", K(ret));
  } else if (OB_FAIL(store_.finish_add_row(need_dump))) {
    LOG_WARN("fail to finish add row", K(ret));
  }
  return ret;
}

// private function
int ObCGRowFile::begin(sql::ObTempColumnStore::Iterator &iter, const bool async)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_.begin(iter, async))) {
    LOG_WARN("fail to begin iterating", K(ret));
  } else {
    is_start_iterate_ = true;
  }
  return ret;
}

/**
* -----------------------------------ObCGRowFilesGenerater-----------------------------------
*/
void ObCGRowFilesGenerater::reset()
{
  is_inited_ = false;
  tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  slice_idx_ = -1;
  storage_schema_ = nullptr;
  max_batch_size_ = 0;
  cg_row_file_memory_limit_ = 0;
  all_column_schema_its_.reset();
  for (int64_t i = 0; i < cg_row_file_arr_.count(); ++i) {
    ObCGRowFile *&cg_row_file = cg_row_file_arr_.at(i);
    if (nullptr != cg_row_file) {
      cg_row_file->~ObCGRowFile();
      ob_free(cg_row_file);
      cg_row_file = nullptr;
    }
  }
  cg_row_file_arr_.reset();
  if (is_generation_sync_output_ && nullptr != cg_row_file_arr_for_output_) {
    for (int64_t i = 0; i < cg_row_file_arr_for_output_->count(); ++i) {
      ObCGRowFile *&cg_row_file = cg_row_file_arr_for_output_->at(i);
      if (nullptr != cg_row_file) {
        cg_row_file->~ObCGRowFile();
        ob_free(cg_row_file);
        cg_row_file = nullptr;
      }
    }
    cg_row_file_arr_for_output_->~ObArray<ObCGRowFile *>();
    ob_free(cg_row_file_arr_for_output_);
    cg_row_file_arr_for_output_ = nullptr;
  }
  if (is_generation_sync_output_ && nullptr != sync_chunk_data_) {
    sync_chunk_data_->cg_block_file_arr_ = nullptr;
    sync_chunk_data_->~ObChunk();
    ob_free(sync_chunk_data_);
    sync_chunk_data_ = nullptr;
  }
}

int ObCGRowFilesGenerater::init(
    const ObTabletID &tablet_id,
    const int64_t slice_idx,
    ObStorageSchema *storage_schema,
    const int64_t max_batch_size,
    const int64_t cg_row_file_memory_limit,
    const ObIArray<ObColumnSchemaItem> &all_column_schema_its,
    const bool is_generation_sync_output,
    const bool is_sorted_table_load_with_column_store_replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObCGRowFilesGenerater has been initialized", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() ||
                          slice_idx < 0 ||
                          nullptr == storage_schema ||
                          max_batch_size <= 0 ||
                          cg_row_file_memory_limit <= 0 ||
                          all_column_schema_its.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument",
        K(ret), K(tablet_id), K(slice_idx), KP(storage_schema),
        K(max_batch_size), K(cg_row_file_memory_limit), K(all_column_schema_its));
  } else {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    storage_schema_ = storage_schema;
    max_batch_size_ = max_batch_size;
    is_generation_sync_output_ = is_generation_sync_output;
    cg_row_file_memory_limit_ = cg_row_file_memory_limit;
    is_sorted_table_load_with_column_store_replica_ = is_sorted_table_load_with_column_store_replica;
    const int64_t cg_count = storage_schema->get_column_group_count();
    if (OB_UNLIKELY(cg_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the column group count is invalid", K(ret), K(cg_count));
    } else if (OB_FAIL(cg_row_file_arr_.prepare_allocate(cg_count))) {
      LOG_WARN("fail to prepare allocate cg row file arr", K(ret), K(cg_count));
    } else if (OB_FAIL(all_column_schema_its_.assign(all_column_schema_its))) {
      LOG_WARN("fail to assign all column schema its", K(ret), K(all_column_schema_its));
    } else {
      for (int64_t cg_idx = 0; cg_idx < cg_row_file_arr_.count(); ++cg_idx) {
        cg_row_file_arr_.at(cg_idx) = nullptr;
      }
      if (is_generation_sync_output) {
        cg_row_file_arr_for_output_ = OB_NEW(ObArray<ObCGRowFile *>, ObMemAttr(MTL_ID(), "CGRFArrOutput"));
        sync_chunk_data_ = OB_NEW(ObChunk, ObMemAttr(MTL_ID(), "ChunkDataOutput"));
        if (OB_UNLIKELY(nullptr == cg_row_file_arr_for_output_ || nullptr == sync_chunk_data_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate cg row file arr for output",
              K(ret), KP(cg_row_file_arr_for_output_), KP(sync_chunk_data_));
        }
        FLOG_INFO("the ObCGRowFilesGenerater is generation sync output mode",
            K(ret), K(is_generation_sync_output_));
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObCGRowFilesGenerater::append_batch(
    const blocksstable::ObBatchDatumRows &bdrs,
    const bool is_slice_end,
    ObDDLChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFilesGenerater is not initialized", K(ret));
  } else if (OB_UNLIKELY((!is_slice_end && bdrs.row_count_ <= 0) ||
                          bdrs.row_count_ > max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the are invalid arguments",
        K(ret), K(is_slice_end), K(bdrs), K(max_batch_size_));
  } else if (is_slice_end && bdrs.row_count_ <= 0) {
    // by pass
  } else {
    blocksstable::ObBatchDatumRows cg_rows;
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema_->get_column_groups();
    output_chunk.reset();
    cg_rows.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
    cg_rows.mvcc_row_flag_ = bdrs.mvcc_row_flag_;
    cg_rows.row_count_ = bdrs.row_count_;
    cg_rows.trans_id_ = bdrs.trans_id_;

    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_schemas.count(); ++cg_idx) {
      ObCGRowFile *cg_row_file = nullptr;
      const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(cg_idx);
      if (OB_UNLIKELY(cg_idx >= cg_row_file_arr_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg idx is invalid", K(ret), K(cg_idx), K(cg_row_file_arr_.count()));
      } else {
        cg_row_file = cg_row_file_arr_.at(cg_idx);
        if (nullptr == cg_row_file) {
          cg_row_file = OB_NEW(ObCGRowFile, ObMemAttr(MTL_ID(), "CGRowFile"));
          if (OB_UNLIKELY(nullptr == cg_row_file)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate cg row file obj", K(ret));
          } else if (OB_FAIL(cg_row_file->open(all_column_schema_its_,
                                               tablet_id_,
                                               slice_idx_,
                                               cg_idx,
                                               cg_schema,
                                               max_batch_size_,
                                               cg_row_file_memory_limit_))) {
            LOG_WARN("fail to open cg block file", K(ret), K(tablet_id_), K(slice_idx_), K(cg_idx));
          } else {
            cg_row_file_arr_.at(cg_idx) = cg_row_file;
          }
          if (OB_FAIL(ret) && nullptr != cg_row_file) {
            cg_row_file->~ObCGRowFile();
            ob_free(cg_row_file);
            cg_row_file = nullptr;
          }
        }
      }
      if (OB_SUCC(ret)) {
        cg_rows.vectors_.reuse();
        for (int64_t j = 0; OB_SUCC(ret) && j < cg_schema.get_column_count(); ++j) {
          const int64_t column_idx = cg_schema.get_column_idx(j);
          if (OB_UNLIKELY(column_idx < 0 || column_idx >= bdrs.vectors_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("this is invalid column idx", K(ret), K(column_idx), K(cg_schema));
          } else if (OB_FAIL(cg_rows.vectors_.push_back(bdrs.vectors_.at(column_idx)))) {
            LOG_WARN("push back vector failed", K(ret), K(column_idx));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cg_row_file->append_batch(cg_rows))) {
            LOG_WARN("fail to append batch", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(try_generate_output_chunk(is_slice_end, output_chunk))) {
    LOG_WARN("fail to generate ddl output chunk", K(ret));
  }
  return ret;
}

int ObCGRowFilesGenerater::try_generate_output_chunk(
    const bool is_slice_end,
    ObDDLChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  ObChunk *chunk_data = nullptr;
  ObArray<ObCGRowFile *> *cg_row_files_ptr = nullptr;
  output_chunk.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFilesGenerater is not initialized", K(ret));
  } else {
    if (is_generation_sync_output_) {
      cg_row_file_arr_for_output_->reuse();
    }
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_row_file_arr_.count(); ++cg_idx) {
      ObCGRowFile *&cg_row_file = cg_row_file_arr_.at(cg_idx);
      if (nullptr != cg_row_file &&
          ((cg_row_file->get_mem_hold() > cg_row_file_memory_limit_ &&
            !is_sorted_table_load_with_column_store_replica_) || is_slice_end)) {
        if (OB_FAIL(cg_row_file->dump(true))) {
          LOG_WARN("fail to dump cg row file", K(ret), KPC(cg_row_file));
        } else if (OB_FAIL(cg_row_file->finish_append_batch(true/*need_dump*/))) {
          LOG_WARN("fail to finish add row", K(ret), KPC(cg_row_file));
        } else if (is_generation_sync_output_) {
          if (OB_UNLIKELY(nullptr == cg_row_file_arr_for_output_ ||
                          nullptr == sync_chunk_data_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cg row file arr for output is null",
                K(ret), KP(cg_row_file_arr_for_output_), KP(sync_chunk_data_));
          } else {
            if (OB_FAIL(cg_row_file_arr_for_output_->push_back(cg_row_file))) {
              LOG_WARN("fail to push back cg row file", K(ret), KPC(cg_row_file));
            } else {
              cg_row_file = nullptr;
            }
          }
        } else {
          if (nullptr == chunk_data || nullptr == cg_row_files_ptr) {
            chunk_data = OB_NEW(ObChunk, ObMemAttr(MTL_ID(), "CGRFG_Chunk"));
            cg_row_files_ptr = OB_NEW(ObArray<ObCGRowFile *>, ObMemAttr(MTL_ID(), "CGRFG_CGRFiles"));
            if (OB_UNLIKELY(nullptr == chunk_data || nullptr == cg_row_files_ptr)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret), KP(chunk_data), KP(cg_row_files_ptr));
            }
          }
          if (FAILEDx(cg_row_files_ptr->push_back(cg_row_file))) {
            LOG_WARN("fail to push back cg row file", K(ret), KPC(cg_row_file));
          } else {
            cg_row_file = nullptr;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      output_chunk.tablet_id_ = tablet_id_;
      output_chunk.slice_idx_ = slice_idx_;
      output_chunk.is_slice_end_ = is_slice_end;
      if (is_generation_sync_output_ &&
          nullptr != sync_chunk_data_ &&
          nullptr != cg_row_file_arr_for_output_ &&
          !cg_row_file_arr_for_output_->empty()) { // has ouput data
        sync_chunk_data_->cg_row_file_arr_ = cg_row_file_arr_for_output_;
        sync_chunk_data_->type_ = ObChunk::CG_ROW_TMP_FILES;
        output_chunk.chunk_data_ = sync_chunk_data_;
      } else if (!is_generation_sync_output_ &&
                 nullptr != chunk_data &&
                 nullptr != cg_row_files_ptr &&
                 !cg_row_files_ptr->empty()) { // has ouput data
        chunk_data->cg_row_file_arr_ = cg_row_files_ptr;
        chunk_data->type_ = ObChunk::CG_ROW_TMP_FILES;
        output_chunk.chunk_data_ = chunk_data;
      }
    } else { // fail to generate output chunk
      if (nullptr != chunk_data) {
        chunk_data->~ObChunk();
        ob_free(chunk_data);
        chunk_data = nullptr;
      }
      if (nullptr != cg_row_files_ptr) {
        for (int64_t i = 0; i < cg_row_files_ptr->count(); ++i) {
          ObCGRowFile *&cg_row_file = cg_row_files_ptr->at(i);
          if (OB_LIKELY(nullptr != cg_row_file)) {
            cg_row_file->~ObCGRowFile();
            ob_free(cg_row_file);
            cg_row_file = nullptr;
          }
        }
        cg_row_files_ptr->~ObArray<ObCGRowFile *>();
        ob_free(cg_row_files_ptr);
        cg_row_files_ptr = nullptr;
      }
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase