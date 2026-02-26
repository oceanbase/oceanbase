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
#include "ob_co_merge_log.h"

namespace oceanbase
{
namespace compaction
{
OB_SERIALIZE_MEMBER(ObMergeLog, op_, major_idx_, row_id_);

/**
 * ---------------------------------------------------------ObCOMergeProjector--------------------------------------------------------------
 */
int ObCOMergeProjector::init(const ObStorageColumnGroupSchema &cg_schema, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!cg_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid cg schema", K(ret), K(cg_schema));
  } else {
    allocator_ = &allocator;
    projector_ = nullptr;
    project_row_.reset();
    projector_count_ = cg_schema.column_cnt_;
    if (OB_ISNULL(projector_ = static_cast<uint16_t *>(allocator_->alloc(sizeof(uint16_t) * projector_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc projector", K(ret), K(projector_count_));
    } else {
      MEMSET(projector_, 0, sizeof(uint16_t) * projector_count_);
    }
    for (uint16_t i = 0; OB_SUCC(ret) && i < projector_count_; i++) {
      projector_[i] = cg_schema.get_column_idx(i);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(project_row_.init(projector_count_))) {
      STORAGE_LOG(WARN, "failed to init project row", K(ret), K(projector_count_));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }

  return ret;
}

void ObCOMergeProjector::reset()
{
  if (OB_NOT_NULL(projector_)) {
    allocator_->free(projector_);
    projector_ = nullptr;
  }
  projector_count_ = 0;
  project_row_.reset();
  allocator_ = nullptr;
  is_inited_ = false;
}

int ObCOMergeProjector::project(const blocksstable::ObDatumRow &row)
{
  bool is_all_nop = false;
  clean_project_row();
  return project(row, project_row_, is_all_nop);
}

int ObCOMergeProjector::project(const blocksstable::ObDatumRow &row, blocksstable::ObDatumRow &result_row, bool &is_all_nop) const
{
  int ret = OB_SUCCESS;
  is_all_nop = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObProjector is not init", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid() || result_row.count_ != projector_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(row));
  } else {
    result_row.row_flag_ = row.row_flag_;

    for (int64_t i = 0; OB_SUCC(ret) && i < projector_count_; i++) {
      const uint16_t idx = projector_[i];
      if (idx < 0 || idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected idx", K(ret), K(i), K(idx), K(row.count_));
      } else {
        result_row.storage_datums_[i] = row.storage_datums_[idx];
        if (!row.storage_datums_[idx].is_nop()) {
          is_all_nop = false;
        }
      }
    }
  }

  return ret;
}

void ObCOMergeProjector::clean_project_row()
{
  project_row_.row_flag_.reset();
  project_row_.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_NOT_EXIST);
}

/**
 * ---------------------------------------------------------ObCOMergeLogConsumer--------------------------------------------------------------
 */
template<typename CallbackImpl>
int ObCOMergeLogConsumer<CallbackImpl>::consume_all_merge_log(ObCOMergeLogIterator &iter, CallbackImpl &callback)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(CONSUME_ALL_MERGE_LOG_PROCESS);
  bool need_consume = false;
  ObMergeLog curr_log;
  ObMergeLog compact_log;
  const blocksstable::ObDatumRow *row = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!MERGE_SCHEDULER_PTR->could_major_merge_start())) {
      ret = OB_CANCELED;
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("merge_errsim", "cancel_merge", "reason", "co_merge_paused");
#endif
      LOG_WARN("major merge has been paused, cancel co merge", K(ret));
    } else if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("fail to yield", K(ret));
#ifdef ERRSIM
    } else if (OB_FAIL(ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PARTITION_LONG_TIME) OB_SUCCESS)) {
      if (REACH_THREAD_TIME_INTERVAL(ObPartitionMergeProgress::UPDATE_INTERVAL)) {
        LOG_INFO("ERRSIM EN_COMPACTION_CO_MERGE_PARTITION_LONG_TIME", K(ret));
      }
      ret = OB_SUCCESS;
      continue;
#endif
    } else if (OB_FAIL(iter.get_next_log(curr_log, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next log", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (compact_log.is_valid() && !curr_log.is_continuous(compact_log)) {
      if (OB_FAIL(callback.consume(compact_log, row/*no used*/))) {
        LOG_WARN("failed to consume compact merge log", K(ret), K(compact_log));
      } else {
        compact_log.reset();
      }
    }
    // TODO statistic merge progress
    if (OB_FAIL(ret)) {
    } else if (curr_log.op_ == ObMergeLog::OpType::REPLAY) {
      compact_log = curr_log;
    } else if (OB_FAIL(callback.consume(curr_log, row))) {
      LOG_WARN("failed to consume merge log", K(ret), K(curr_log), KPC(row));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (compact_log.is_valid() && OB_FAIL(callback.consume(compact_log, row/*no used*/))) {
    LOG_WARN("failed to consume merge log", K(ret), K(compact_log));
  } else if (OB_FAIL(iter.close())) {
    LOG_WARN("failed to close merge log iter", K(ret));
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeLogFile--------------------------------------------------------------
 */
void ObCOMergeLogFile::destroy()
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(MTL_ID(), file_fd_))) {
      LOG_ERROR("remove file failed", K(ret), K(*this));
    } else {
      LOG_INFO("close file success", K(ret), K(*this));
    }
  }
  file_fd_ = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  dir_id_ = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
  block_count_ = 0;
  block_size_ = 0;
}

int ObCOMergeLogFile::open(const int64_t dir_id, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(*this));
  } else if (dir_id < 0 || block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dir_id), K(block_size));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(MTL_ID(), file_fd_, dir_id))) {
    LOG_WARN("failed to open tmp file", K(ret), K(dir_id));
  } else {
    dir_id_ = dir_id;
    block_size_ = block_size;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObCOMergeLogFile::get_io_info(
    char *buf,
    tmp_file::ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  io_info.reset();
  io_info.fd_ = file_fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = buf;
  io_info.size_ = block_size_;
  io_info.io_timeout_ms_ = TIMEOUT_MS;
  return ret;
}

int ObCOMergeLogFile::append_block(ObCOMergeLogBlock &block)
{
  int ret = OB_SUCCESS;
  tmp_file::ObTmpFileIOInfo io_info;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogFile is invalid", K(ret), K(*this));
  } else if (OB_ISNULL(block.header_) || block.block_size_ != block_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block), K(block_size_));
  } else if (OB_FAIL(get_io_info(reinterpret_cast<char*>(block.header_), io_info))) {
    LOG_WARN("get io info failed", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(MTL_ID(), io_info))) {
    OB_LOG(WARN, "failed to write to tmp file", K(ret), K(block), K(io_info));
  } else {
    block_count_++;
  }
  return ret;
}

int ObCOMergeLogFile::read_block(
    ObCOMergeLogBuffer &buffer,
    const int64_t start_block_idx)
{
  int ret = OB_SUCCESS;
  tmp_file::ObTmpFileIOInfo io_info;
  io_info.prefetch_ = true;
  tmp_file::ObTmpFileIOHandle handle;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogFile is invalid", K(ret), K(*this));
  } else if (start_block_idx < 0 || start_block_idx >= block_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buffer), K(start_block_idx), K(*this));
  } else if (OB_FAIL(buffer.reserve(block_size_))) {
    LOG_WARN("failed to reserve buffer", K(ret), K(block_size_));
  } else if (OB_FAIL(get_io_info(buffer.data(), io_info))) {
    LOG_WARN("failed to get io info", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info, start_block_idx * block_size_, handle))) {
    LOG_WARN("failed to pread from tmp file", K(ret), K(io_info), K(start_block_idx), K(*this));
  } else if (OB_FAIL(buffer.set_current_block(block_size_))) {
    LOG_WARN("failed to construct blocks", K(ret), K(buffer), K(block_size_));
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeLogBuffer--------------------------------------------------------------
 */
int ObCOMergeLogBuffer::init(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block_count", K(ret), K(capacity));
  } else if (OB_ISNULL(data_ = static_cast<char*>(allocator_.alloc(capacity)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(capacity));
  } else {
    capacity_ = capacity;
    MEMSET(data_, 0, capacity_);
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObCOMergeLogBuffer::reserve(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (size > capacity_) {
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(size));
    } else {
      allocator_.free(data_);
      data_ = buf;
      capacity_ = size;
      MEMSET(data_, 0, capacity_);
    }
  }
  return ret;
}

int ObCOMergeLogBuffer::set_current_block(const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (block_size > capacity_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP_(data), K(block_size), K(capacity_));
  } else {
    current_block_.block_size_ = block_size;
    current_block_.header_ = reinterpret_cast<ObCOMergeLogBlockHeader*>(data_);
    if (current_block_.header_->magic_num_ != ObCOMergeLogBlockHeader::MAGIC_NUM) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid magic number", K(ret), K(current_block_.header_->magic_num_));
    }
  }
  return ret;
}

void ObCOMergeLogBuffer::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(data_)) {
    allocator_.free(data_);
    data_ = nullptr;
  }
  if (OB_NOT_NULL(overflow_buffer_)) {
    allocator_.free(overflow_buffer_);
    overflow_buffer_ = nullptr;
  }
  overflow_buffer_size_ = 0;
}

void ObCOMergeLogBuffer::free_overflow_buffer()
{
  if (OB_NOT_NULL(overflow_buffer_)) {
    allocator_.free(overflow_buffer_);
    overflow_buffer_ = nullptr;
    overflow_buffer_size_ = 0;
  }
}

int ObCOMergeLogBuffer::alloc_overflow_buffer(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (size > overflow_buffer_size_) {
    free_overflow_buffer();
    overflow_buffer_ = static_cast<char*>(allocator_.alloc(size));
    if (OB_ISNULL(overflow_buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc overflow buffer", K(ret), K(size));
    } else {
      overflow_buffer_size_ = size;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(overflow_buffer_)) {
    MEMSET(overflow_buffer_, 0, overflow_buffer_size_);
  }
  return ret;
}
/**
 * ---------------------------------------------------------ObCOMergeLogBufferWriter--------------------------------------------------------------
 */
void ObCOMergeLogBufferWriter::reset()
{
  if (OB_NOT_NULL(projector_)) {
    projector_->~ObCOMergeProjector();
    allocator_.free(projector_);
    projector_ = nullptr;
  }
  pos_ = 0;
  ObCOMergeLogBuffer::reset();
}

int ObCOMergeLogBufferWriter::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCOMergeLogBuffer::init(file_.get_block_size()))) {
    LOG_WARN("failed to init ObCOMergeLogBuffer", K(ret));
  } else {
    reuse_current_block();
  }
  return ret;
}

bool ObCOMergeLogBufferWriter::can_reserved()
{
  return write_remain() < MAX(write_capacity() * MAX_RESERVED_RATIO, MAX_RESERVED_SIZE);
}

void ObCOMergeLogBufferWriter::set_current_piece_header(
    const int64_t log_id,
    const int64_t piece_length,
    const int64_t log_length,
    const int64_t piece_id,
    const int64_t total_pieces)
{
  ObCOMergeLogPieceHeader *piece_header = reinterpret_cast<ObCOMergeLogPieceHeader*>(current());
  piece_header->log_id_ = log_id;
  piece_header->length_ = piece_length;
  piece_header->log_length_ = log_length;
  piece_header->piece_id_ = piece_id;
  piece_header->total_pieces_ = total_pieces;
}

void ObCOMergeLogBufferWriter::reuse_current_block()
{
  reuse_buffer();
  current_block_.block_size_ = file_.get_block_size();
  current_block_.header_ = reinterpret_cast<ObCOMergeLogBlockHeader *>(data_);
  current_block_.header_->magic_num_ = ObCOMergeLogBlockHeader::MAGIC_NUM;
}

int ObCOMergeLogBufferWriter::write(const int64_t log_id, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *write_row = &row;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogBufferWriter has not been inited", K(ret));
  } else if (nullptr != projector_) {
    if (OB_FAIL(projector_->project(row))) {
      LOG_WARN("failed to project row", K(ret), K(row));
    } else {
      write_row = &projector_->get_project_row();
    }
  }
  if (FAILEDx(write_piece(log_id, *write_row))) {
    LOG_WARN("failed to write row", K(ret), K(*write_row));
  }
  return ret;
}

int ObCOMergeLogBufferWriter::write(const int64_t log_id, const ObMergeLog &log)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogBufferWriter has not been inited", K(ret));
  } else if (OB_FAIL(write_piece(log_id, log))) {
    LOG_WARN("failed to write log", K(ret), K(log));
  }
  return ret;
}

int ObCOMergeLogBufferWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogBufferWriter has not been inited", K(ret));
  } else if (current_block_.header_->length_ > 0) {
    if (OB_FAIL(file_.append_block(current_block_))) {
      LOG_WARN("failed to append block", K(ret));
    }
  }
  reset();
  return ret;
}

int ObCOMergeLogBufferWriter::flush_current_block()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_.append_block(current_block_))) {
    LOG_WARN("failed to append block", K(ret));
  } else {
    reuse_current_block();
  }
  return ret;
}

int64_t ObCOMergeLogBufferWriter::calc_piece_count(const int64_t serialize_size, const int64_t header_size)
{
  int64_t piece_count = 0;
  int64_t left_size = serialize_size;
  if (write_remain() > header_size) {
    piece_count++;
    left_size -= (write_remain() - header_size);
  }
  while (left_size > 0) {
    piece_count++;
    left_size -= (write_capacity() - header_size);
  }
  return piece_count;
}

int ObCOMergeLogBufferWriter::write_pieces(const int64_t log_id, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = sizeof(ObCOMergeLogPieceHeader);
  int64_t total_pieces = calc_piece_count(buf_len, header_size);
  int64_t writed_size = 0;
  int64_t piece_id = 0;
  while (OB_SUCC(ret) && writed_size < buf_len) {
    const int64_t piece_len = MIN((write_remain() - header_size), buf_len - writed_size);
    if (piece_len <= 0) {
      if (OB_FAIL(flush_current_block())) {
        STORAGE_LOG(WARN, "failed to move next or flush", K(ret));
      }
    } else {
      // set header
      set_current_piece_header(log_id, piece_len, buf_len, ++piece_id, total_pieces);
      current_block_.header_->length_ += header_size;
      pos_ += header_size;
      // set data
      MEMCPY(current(), buf + writed_size, piece_len);
      current_block_.header_->length_ += piece_len;
      pos_ += piece_len;
      // finish
      current_block_.header_->piece_count_++;
      writed_size += piece_len;
    }
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeLogBufferReader--------------------------------------------------------------
 */
void ObCOMergeLogBufferReader::reset()
{
  ObCOMergeLogBuffer::reset();
  block_read_piece_count_ = 0;
  total_read_block_count_ = 0;
}

int ObCOMergeLogBufferReader::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCOMergeLogBuffer::init(file_.get_block_size()))) {
    LOG_WARN("failed to init ObCOMergeLogBuffer", K(ret), K(file_));
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObCOMergeLogBufferReader::read_next_block()
{
  int ret = OB_SUCCESS;
  if (total_read_block_count_ >= file_.get_block_count()) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(reuse_buffer())) {
  } else if (OB_FAIL(file_.read_block(*this, total_read_block_count_))) {
    LOG_WARN("failed to read buffer", K(ret), K(total_read_block_count_), K(*this));
  } else {
    total_read_block_count_++;
    block_read_piece_count_ = 0;
  }
  return ret;
}

int ObCOMergeLogBufferReader::read_next_piece(ObCOMergeLogPieceHeader *&header)
{
  int ret = OB_SUCCESS;
  if (total_read_block_count_ <= 0 // first time read file
      || block_read_piece_count_ >= current_block_.header_->piece_count_) {
    if (OB_FAIL(read_next_block())) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to read next block", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    header = reinterpret_cast<ObCOMergeLogPieceHeader *>(current());
    pos_ += (sizeof(ObCOMergeLogPieceHeader) + header->length_);
    block_read_piece_count_++;
    if (OB_ISNULL(header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "header is null", K(ret), K(pos_));
    } else if (pos_ > current_block_.block_max_readable_size()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read piece pos is out of block", K(ret), K(pos_), K(*header), K(*current_block_.header_));
    }
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeLogFileMgr--------------------------------------------------------------
 */
int ObCOMergeLogFileMgr::init(
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    const int64_t file_block_size,
    const bool skip_base_cg)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCOMergeLogFileMgr has been inited", K(ret));
  } else if (FALSE_IT(row_file_count_ = cg_array.empty() ? 1 : cg_array.count())) {
  } else if (OB_ISNULL(buf = allocator_.alloc(
        (sizeof(ObCOMergeLogFile*) + sizeof(ObCOMergeLogFile)) * row_file_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc row file fds", K(ret));
  } else if (FALSE_IT(row_files_ = static_cast<ObCOMergeLogFile**>(buf))) {
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id_))) {
    LOG_WARN("failed to alloc dir", K(ret));
  } else if (OB_FAIL(log_file_.open(dir_id_, file_block_size))) {
    LOG_WARN("failed to open tmp file", K(ret), K(dir_id_));
  } else {
    ObCOMergeLogFile* objects = reinterpret_cast<ObCOMergeLogFile*>(
      static_cast<char*>(buf) + sizeof(ObCOMergeLogFile*) * row_file_count_);
    for (int64_t i = 0; OB_SUCC(ret) && i < row_file_count_; ++i) {
      row_files_[i] = new (objects + i) ObCOMergeLogFile();
      if (skip_base_cg && row_file_count_ > 1 &&
          (cg_array.at(i).is_all_column_group() || cg_array.at(i).is_rowkey_column_group())) {
        // skip base column group
      } else if (OB_FAIL(row_files_[i]->open(dir_id_, file_block_size))) {
        LOG_WARN("failed to open tmp file", K(ret), K(dir_id_));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(row_files_[i])) {
        row_files_[i]->destroy();
        row_files_[i] = nullptr;
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

void ObCOMergeLogFileMgr::reset()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  readable_ = false;
  log_file_.destroy();
  for (int64_t i = 0; OB_NOT_NULL(row_files_) && i < row_file_count_; i++) {
    if (OB_NOT_NULL(row_files_[i])) {
      row_files_[i]->destroy();
      row_files_[i] = nullptr;
    }
  }
  if (OB_NOT_NULL(row_files_)){
    allocator_.free(row_files_);
    row_files_ = nullptr;
  }
  row_file_count_ = 0;
  dir_id_ = -1;
}

int ObCOMergeLogFileMgr::close_part(const int64_t start_cg_idx, const int64_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogFileMgr not init", K(ret));
  } else if (start_cg_idx >= end_cg_idx || end_cg_idx > row_file_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_file_count_), K(start_cg_idx), K(end_cg_idx));
  } else {
    for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; i++) {
      if (OB_ISNULL(row_files_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row file is null", K(ret), K(i), K(row_file_count_));
      } else {
        row_files_[i]->destroy();
        row_files_[i] = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to close part of files", K(ret), KPC(this), K(start_cg_idx), K(end_cg_idx));
    }
  }
  return ret;
}

int ObCOMergeLogFileMgr::check_could_release(bool &could_release)
{
  int ret = OB_SUCCESS;
  could_release = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogFileMgr not init", K(ret));
  } else {
    for (int64_t i = 0; could_release && i < row_file_count_; ++i) {
      if (nullptr != row_files_[i]) {
        could_release = false;
      }
    }
  }
  return ret;
}

int ObCOMergeLogFileMgr::get_row_file(const int64_t idx, ObCOMergeLogFile *&file)
{
  int ret = OB_SUCCESS;
  file = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (idx < 0 || idx >= row_file_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(row_file_count_));
  } else {
    file = row_files_[idx];
  }
  return ret;
}

int ObCOMergeLogFileMgr::get_log_file(ObCOMergeLogFile *&file)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    file = &log_file_;
  }
  return ret;
}
}
}