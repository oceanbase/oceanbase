/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 * This file is for define of plugin vector index util
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_co_merge_log_operator.h"
#include "storage/column_store/ob_co_merge_ctx.h"

namespace oceanbase
{
namespace compaction
{
/**
 * -------------------------------------------------------ObCOMergeLogFileWriter-------------------------------------------------------------
 */
int ObCOMergeLogFileWriter::init(ObIAllocator &allocator, ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = ctx.get_schema()->get_column_groups();
  ObCOMergeLogFileMgr *mgr = nullptr;
  ObCOMergeLogFile *log_file = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(co_ctx.get_merge_log_mgr(idx, mgr))) {
    LOG_WARN("failed to get mgr", K(ret), K(idx));
  } else if (OB_ISNULL(mgr) || !mgr->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file mgr is null", K(ret), K(mgr));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (FALSE_IT(mgr_ = mgr)) {
  } else if (FALSE_IT(cg_count_ = mgr_->get_row_file_count())) {
  } else if (cg_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cg count", K(ret), K(cg_count_));
  } else if (OB_ISNULL(row_buffer_writers_ = static_cast<ObCOMergeLogBufferWriter **>(
      allocator_->alloc(sizeof(ObCOMergeLogBufferWriter *) * cg_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(mgr_->get_log_file(log_file))) {
    LOG_WARN("failed to get log file", K(ret));
  } else if (OB_FAIL(init_buffer_writer(log_buffer_writer_, *log_file, nullptr/*projector*/))) {
    LOG_WARN("failed to init log buffer writer", K(ret));
  } else if (OB_FAIL(init_row_buffer_writers(cg_array))) {
    LOG_WARN("failed to init row buffer writers", K(ret));
  } else {
    log_id_ = 1;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObCOMergeLogFileWriter::init_row_buffer_writers(const common::ObIArray<ObStorageColumnGroupSchema> &cg_array)
{
  int ret = OB_SUCCESS;
  ObCOMergeLogFile *file = nullptr;
  ObCOMergeProjector *projector = nullptr;
  ObCOMergeLogBufferWriter *buffer_writer = nullptr;
  if (cg_count_ > cg_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg count not match", K(ret), K(cg_count_), "cg_array count", cg_array.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_count_; ++i) {
    const ObStorageColumnGroupSchema &cg_schema = cg_array.at(i);
    projector = nullptr;
    buffer_writer = nullptr;
    if (OB_FAIL(mgr_->get_row_file(i, file))) {
      LOG_WARN("failed to get row file fd", K(ret), K(i));
    } else if (!file->is_valid()) { // no row file // skip current cg
      // check rowkey cg or all cg
      if (cg_schema.is_all_column_group() || cg_schema.is_rowkey_column_group()) {
        // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cg schema", K(ret), K(file), K(i), K(cg_schema));
      }
    } else {
      if (1 == cg_count_) { // row store // no need projector
      } else if (OB_ISNULL(projector = OB_NEWx(ObCOMergeProjector, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to new projector", K(ret), K(i));
      } else if (OB_FAIL(projector->init(cg_schema, *allocator_))) {
        LOG_WARN("failed to init projector", K(ret), K(i), K(cg_schema));
      }
      if (FAILEDx(init_buffer_writer(buffer_writer, *file, projector))) {
        LOG_WARN("failed to init buffer writer", K(ret), K(i), K(cg_schema));
      }
    }
    if (OB_SUCC(ret)) {
      row_buffer_writers_[i] = buffer_writer;
    } else{
      if (OB_NOT_NULL(projector)) {
        projector->~ObCOMergeProjector();
        allocator_->free(projector);
        projector = nullptr;
      }
      if (OB_NOT_NULL(buffer_writer)) {
        buffer_writer->~ObCOMergeLogBufferWriter();
        allocator_->free(buffer_writer);
        buffer_writer = nullptr;
      }
    }
  }
  return ret;
}

int ObCOMergeLogFileWriter::init_buffer_writer(
    ObCOMergeLogBufferWriter *&buffer_writer,
    ObCOMergeLogFile &file,
    ObCOMergeProjector *projector)
{
  int ret = OB_SUCCESS;
  buffer_writer = nullptr;
  if (OB_ISNULL(buffer_writer = OB_NEWx(ObCOMergeLogBufferWriter, allocator_,
      projector, file, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new ObCOMergeLogBufferWriter", K(ret));
  } else if (OB_FAIL(buffer_writer->init())) {
    LOG_WARN("failed to init ObCOMergeLogBufferWriter", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buffer_writer)) {
    buffer_writer->~ObCOMergeLogBufferWriter();
    allocator_->free(buffer_writer);
    buffer_writer = nullptr;
  }
  return ret;
}

void ObCOMergeLogFileWriter::reset()
{
  is_inited_ = false;
  for (int64_t i = 0; i < cg_count_; ++i) {
    if (OB_NOT_NULL(row_buffer_writers_) && OB_NOT_NULL(row_buffer_writers_[i])) {
      row_buffer_writers_[i]->~ObCOMergeLogBufferWriter();
      allocator_->free(row_buffer_writers_[i]);
      row_buffer_writers_[i] = nullptr;
    }
  }
  if (OB_NOT_NULL(log_buffer_writer_)) {
    log_buffer_writer_->~ObCOMergeLogBufferWriter();
    allocator_->free(log_buffer_writer_);
    log_buffer_writer_ = nullptr;
  }
  if (OB_NOT_NULL(row_buffer_writers_)) {
    allocator_->free(row_buffer_writers_);
    row_buffer_writers_ = nullptr;
  }
  log_id_ = 0;
  cg_count_ = 0;
}

int ObCOMergeLogFileWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogFileWriter has not been inited", K(ret));
  } else if (OB_FAIL(log_buffer_writer_->close())) {
    LOG_WARN("failed to flush log file", K(ret), K(log_buffer_writer_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_count_; ++i) {
      if (OB_NOT_NULL(row_buffer_writers_[i])) {
        if (OB_FAIL(row_buffer_writers_[i]->close())) {
          LOG_WARN("failed to flush row file", K(ret), K(i), K(row_buffer_writers_[i]));
        }
      }
    }
    if (OB_SUCC(ret)) {
      mgr_->set_readable();
    }
  }
  return ret;
}

int ObCOMergeLogFileWriter::write_merge_log(const ObMergeLog &log, const blocksstable::ObDatumRow *full_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogFileWriter has not been inited", K(ret));
  } else if (!log.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge log", K(ret), K(log));
  } else {
    for (int64_t i = 0; ObMergeLog::REPLAY != log.op_ && OB_SUCC(ret) && i < cg_count_; i++) {
      if (OB_ISNULL(full_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("full row is null", K(ret));
      } else if (OB_FAIL(inner_row_write(i, *full_row))) {
        LOG_WARN("failed to write row", K(ret), K(i), K(full_row));
      }
    }
    if (FAILEDx(inner_log_write(log))) {
      LOG_WARN("failed to write log file", K(ret), K(log));
    } else {
      log_id_++;
    }
  }
  return ret;
}

int ObCOMergeLogFileWriter::inner_log_write(const ObMergeLog &log)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_buffer_writer_->write(log_id_, log))) {
    LOG_WARN("failed to write log", K(ret), K(log));
  }
  return ret;
}

int ObCOMergeLogFileWriter::inner_row_write(const int64_t idx, const blocksstable::ObDatumRow &full_row)
{
  int ret = OB_SUCCESS;
  if (nullptr == row_buffer_writers_[idx]) { // skip row write
    // do nothing
  } else if (OB_FAIL(row_buffer_writers_[idx]->write(log_id_, full_row))) {
    LOG_WARN("failed to write row file", K(ret), K(idx), K(*row_buffer_writers_[idx]));
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeLogFileReader--------------------------------------------------------------
 */
int ObCOMergeLogFileReader::init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  ObCOMergeLogFileMgr *mgr = nullptr;
  ObCOMergeLogFile *log_file = nullptr;
  ObCOMergeLogFile *row_file = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(co_ctx.get_merge_log_mgr(idx, mgr))) {
    LOG_WARN("failed to get mgr", K(ret), K(idx));
  } else if (OB_ISNULL(mgr) || !mgr->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file mgr is null", K(ret), K(mgr));
  } else if (0 > cg_idx || mgr->get_row_file_count() <= cg_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cg idx", K(ret), K(cg_idx), K(mgr->get_row_file_count()));
  } else if (!mgr->is_readable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file mgr is not readable", K(ret), K(*mgr));
  } else if (FALSE_IT(cg_idx_ = cg_idx)) {
  } else if (OB_FAIL(mgr->get_log_file(log_file))) {
    LOG_WARN("failed to get log file", K(ret), K(*mgr));
  } else if (OB_FAIL(mgr->get_row_file(cg_idx_, row_file))) {
    LOG_WARN("failed to get row file", K(ret), K(*mgr), K(cg_idx_));
  } else if (OB_ISNULL(row_file)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row file is null", K(ret), K(*mgr), K(cg_idx_));
  } else if (OB_FAIL(init_buffer_reader(log_buffer_reader_, *log_file))) {
    LOG_WARN("failed to init log buffer reader", K(ret));
  } else if (OB_FAIL(init_buffer_reader(row_buffer_reader_, *row_file))) {
    LOG_WARN("failed to init row buffer reader", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObCOMergeLogFileReader::reset()
{
  is_inited_ = false;
  cg_idx_ = -1;
  merge_log_.reset();
  curr_row_.reset();
  if (OB_NOT_NULL(log_buffer_reader_)) {
    log_buffer_reader_->~ObCOMergeLogBufferReader();
    allocator_.free(log_buffer_reader_);
    log_buffer_reader_ = nullptr;
  }
  if (OB_NOT_NULL(row_buffer_reader_)) {
    row_buffer_reader_->~ObCOMergeLogBufferReader();
    allocator_.free(row_buffer_reader_);
    row_buffer_reader_ = nullptr;
  }
  FLOG_INFO("ObCOMergeLogFileReader reset", K_(cost_time));
  cost_time_ = 0;
}

int ObCOMergeLogFileReader::init_buffer_reader(
    ObCOMergeLogBufferReader *&buffer_reader,
    ObCOMergeLogFile &file)
{
  int ret = OB_SUCCESS;
  buffer_reader = nullptr;
  if (OB_ISNULL(buffer_reader = OB_NEWx(ObCOMergeLogBufferReader, &allocator_, file, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new ObCOMergeLogBufferReader", K(ret));
  } else if (OB_FAIL(buffer_reader->init())) {
    LOG_WARN("failed to init buffer reader", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buffer_reader)) {
    buffer_reader->~ObCOMergeLogBufferReader();
    allocator_.free(buffer_reader);
    buffer_reader = nullptr;
  }
  return ret;
}

int ObCOMergeLogFileReader::inner_log_read(int64_t &log_id)
{
  int ret = OB_SUCCESS;
  merge_log_.reset();
  if (OB_FAIL(log_buffer_reader_->read_next_log(log_id, merge_log_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read log", K(ret), K(merge_log_));
    }
  }
  return ret;
}

int ObCOMergeLogFileReader::inner_row_read(int64_t &log_id)
{
  int ret = OB_SUCCESS;
  curr_row_.reuse();
  if (OB_FAIL(row_buffer_reader_->read_next_log(log_id, curr_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read row", K(ret), K(curr_row_));
    }
  }
  return ret;
}

int ObCOMergeLogFileReader::get_next_log(ObMergeLog &mergelog, const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t log_id = 0;
  int64_t row_log_id = 0;
  row = nullptr;
  mergelog.reset();
  const int64_t start_time = common::ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_log_read(log_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read log", K(ret));
    }
  } else if (!merge_log_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge log", K(ret), K(merge_log_));
  } else if (FALSE_IT(mergelog = merge_log_)) {
  } else if (ObMergeLog::REPLAY != merge_log_.op_) {
    if (OB_FAIL(inner_row_read(row_log_id))) {
      LOG_WARN("failed to read row", K(ret));
    } else if (row_log_id != log_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log id mismatch", K(ret), K(log_id), K(row_log_id));
    } else {
      row = &curr_row_;
    }
  }
  cost_time_ += common::ObTimeUtility::current_time() - start_time;
  return ret;
}

}
}