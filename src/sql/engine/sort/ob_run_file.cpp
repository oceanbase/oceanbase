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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_run_file.h"
#include "common/cell/ob_cell_reader.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObRunFile::ObRunFile()
    : file_appender_(),
      file_reader_(),
      buckets_last_run_trailer_(),
      cur_run_trailer_(NULL),
      run_blocks_(),
      cur_run_row_count_(0)
{}

ObRunFile::~ObRunFile()
{
  for (int32_t i = 0; i < run_blocks_.count(); ++i) {
    run_blocks_.at(i).free_buffer();
  }  // end for
  run_blocks_.reset();
}

int ObRunFile::open(const ObString& filename)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_appender_.is_opened() || file_reader_.is_opened())) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("file is opened");
  } else if (OB_FAIL(file_appender_.open(filename, true, true, true))) {
    LOG_WARN("failed to open file", K(ret), K(filename));
  } else if (OB_FAIL(file_reader_.open(filename, true))) {
    file_appender_.close();
    LOG_WARN("reader failed to open file", K(ret), K(filename));
  } else {
    LOG_INFO("open run file", K(filename));
  }
  return ret;
}

int ObRunFile::close()
{
  int ret = OB_SUCCESS;
  file_appender_.close();
  file_reader_.close();
  for (int32_t i = 0; i < run_blocks_.count(); ++i) {
    run_blocks_.at(i).free_buffer();
  }  // end for
  run_blocks_.reset();
  cur_run_trailer_ = NULL;
  buckets_last_run_trailer_.reset();
  return ret;
}

bool ObRunFile::is_opened() const
{
  return file_appender_.is_opened() && file_reader_.is_opened();
}

int ObRunFile::find_last_run_trailer(const int64_t bucket_idx, RunTrailer*& bucket_info)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < buckets_last_run_trailer_.count(); ++i) {
    if (bucket_idx == buckets_last_run_trailer_.at(i).bucket_idx_) {
      bucket_info = &buckets_last_run_trailer_.at(i);
      ret = OB_SUCCESS;
    }
  }  // end for
  return ret;
}

int ObRunFile::begin_append_run(const int64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  if (!is_opened()) {
    ret = OB_NOT_INIT;
    LOG_WARN("run file not opened", K(ret));
  } else if (OB_FAIL(find_last_run_trailer(bucket_idx, cur_run_trailer_))) {
    RunTrailer run_trailer;
    run_trailer.bucket_idx_ = bucket_idx;
    run_trailer.prev_run_trailer_pos_ = -1;  // we are the first run of this bucket
    run_trailer.curr_run_size_ = 0;
    if (OB_FAIL(buckets_last_run_trailer_.push_back(run_trailer))) {
      LOG_WARN("failed to push back to array", K(ret));
    } else {
      cur_run_trailer_ = &buckets_last_run_trailer_.at(static_cast<int32_t>(buckets_last_run_trailer_.count() - 1));
      LOG_INFO("begin append run", K(bucket_idx));
    }
  } else {
    cur_run_trailer_->curr_run_size_ = 0;
    cur_run_row_count_ = 0;
    LOG_INFO("begin append run", K(bucket_idx));
  }
  return ret;
}

int ObRunFile::append_row(const common::ObString& compact_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_appender_.append(compact_row.ptr(), compact_row.length(), false))) {
    LOG_WARN("failed to append file", K(ret));
  } else {
    ++cur_run_row_count_;
    cur_run_trailer_->curr_run_size_ += compact_row.length();
  }
  return ret;
}

int ObRunFile::end_append_run()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_appender_.fsync())) {
    LOG_WARN("failed to fsync run file", K(ret));
  } else {
    int64_t trailer_pos = file_appender_.get_file_pos();
    if (OB_FAIL(file_appender_.append(cur_run_trailer_, sizeof(*cur_run_trailer_), false))) {
      LOG_WARN("failed to write run trailer", K(ret));
    } else if (OB_FAIL(file_appender_.fsync())) {
      LOG_WARN("failed to fsync run file", K(ret));
    } else {
      LOG_INFO("end append run",
          "bucket",
          cur_run_trailer_->bucket_idx_,
          "row_count",
          cur_run_row_count_,
          "run_trailer_pos",
          trailer_pos,
          "run_size",
          cur_run_trailer_->curr_run_size_);
      cur_run_trailer_->prev_run_trailer_pos_ = trailer_pos;
      cur_run_trailer_->curr_run_size_ = 0;
      cur_run_trailer_ = NULL;
      cur_run_row_count_ = 0;
    }
  }
  return ret;
}

ObRunFile::RunBlock::RunBlock() : run_end_offset_(0), block_offset_(0), block_data_size_(0), next_row_pos_(0)
{}

ObRunFile::RunBlock::~RunBlock()
{
  reset();
}

void ObRunFile::RunBlock::reset()
{
  buffer_ = NULL;  // don't free the buffer
  buffer_size_ = 0;
  base_pos_ = 0;
  run_end_offset_ = 0;
  block_offset_ = 0;
  block_data_size_ = 0;
  next_row_pos_ = 0;
}

void ObRunFile::RunBlock::free_buffer()
{
  if (NULL != buffer_) {
    ::free(buffer_);
    buffer_ = NULL;
  }
}

bool ObRunFile::RunBlock::is_end_of_run() const
{
  return (next_row_pos_ >= block_data_size_) && (block_offset_ + block_data_size_ >= run_end_offset_);
}

int ObRunFile::begin_read_bucket(const int64_t bucket_idx, int64_t& run_count)
{
  int ret = OB_SUCCESS;
  run_count = 0;
  RunTrailer* run_trailer = NULL;
  if (0 != run_blocks_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("run blocks is not empty", K(ret));
  } else if (OB_FAIL(find_last_run_trailer(bucket_idx, run_trailer))) {
    LOG_ERROR("no run for this bucket", K(bucket_idx));
  } else {
    // read the first block of every run
    int64_t run_trailer_pos = run_trailer->prev_run_trailer_pos_;
    LOG_INFO("last run trailer", K(bucket_idx), K(run_trailer_pos));
    RunBlock run_block;
    int64_t read_size = 0;
    while (OB_SUCC(ret) && run_trailer_pos > 0) {
      if (OB_FAIL(run_block.assign(run_block.BLOCK_SIZE, FileComponent::DirectFileReader::DEFAULT_ALIGN_SIZE))) {
        LOG_ERROR("failed to alloc block", K(ret));
      }
      // read the run trailer
      else if (OB_FAIL(file_reader_.pread(sizeof(RunTrailer), run_trailer_pos, run_block, read_size)) ||
               sizeof(RunTrailer) != read_size) {
        ret = OB_IO_ERROR;
        LOG_WARN("failed to read run file", K(ret), K(run_trailer_pos), K(read_size));
      } else {
        run_trailer = reinterpret_cast<RunTrailer*>(run_block.get_buffer() + run_block.get_base_pos());
        if (MAGIC_NUMBER != run_trailer->magic_number_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid run trailer, data corrupted", K(ret), "magic", run_trailer->magic_number_);
        } else if (bucket_idx != run_trailer->bucket_idx_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid bucket", K(ret), "idx", run_trailer->bucket_idx_, "expected", bucket_idx);
        } else if (run_trailer_pos < run_trailer->curr_run_size_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR(
              "invalid run size", K(ret), "trailer_pos", run_trailer_pos, "run_size", run_trailer->curr_run_size_);
        } else {
          run_block.run_end_offset_ = run_trailer_pos;
          run_block.block_offset_ = run_trailer_pos - run_trailer->curr_run_size_;
          run_block.block_data_size_ =
              run_trailer->curr_run_size_ < run_block.BLOCK_SIZE ? run_trailer->curr_run_size_ : run_block.BLOCK_SIZE;
          run_block.next_row_pos_ = 0;

          run_trailer_pos = run_trailer->prev_run_trailer_pos_;  // update the previous run trailer
          LOG_INFO("run trailer",
              "bucket_idx",
              bucket_idx,
              "prev_trailer_pos",
              run_trailer_pos,
              "curr_run_size",
              run_trailer->curr_run_size_);
          // read the first data block
          if (OB_FAIL(file_reader_.pread(run_block.block_data_size_, run_block.block_offset_, run_block, read_size)) ||
              read_size != run_block.block_data_size_) {
            ret = OB_IO_ERROR;
            LOG_WARN("failed to read run file",
                K(ret),
                "offset",
                run_block.block_offset_,
                K(read_size),
                "data_size",
                run_block.block_data_size_);
          } else if (OB_FAIL(run_blocks_.push_back(run_block))) {
            LOG_WARN("failed to push back to array", K(ret));
          } else {
            LOG_INFO("read run first block", "offset", run_block.block_offset_, K(read_size));
            ++run_count;
            run_block.reset();
          }
        }
      }
    }  // end while
  }
  return ret;
}

/// @return OB_ITER_END when reaching the end of this run
int ObRunFile::get_next_row(const int64_t run_idx, ObNewRow& row)
{
  int ret = OB_SUCCESS;
  const int64_t run_count = run_blocks_.count();
  if (OB_UNLIKELY(run_idx >= run_count || 0 >= run_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid run", K(run_idx), K(run_count));
  } else {
    RunBlock& run_block = run_blocks_.at(static_cast<int32_t>(run_count - run_idx - 1));
    if (run_block.is_end_of_run()) {
      LOG_INFO("reach end of run", K(run_idx));
      ret = OB_ITER_END;
    } else if (OB_FAIL(block_get_next_row(run_block, row))) {
      LOG_WARN("failed to get the next row from the block", K(ret), K(run_idx));
    }
  }
  return ret;
}

int ObRunFile::block_get_next_row(RunBlock& run_block, ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (run_block.next_row_pos_ >= run_block.block_data_size_) {
    if (OB_FAIL(read_next_run_block(run_block))) {
      LOG_WARN("failed to read next block", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString compact_row;
    if (OB_FAIL(parse_row(run_block.get_buffer() + run_block.get_base_pos() + run_block.next_row_pos_,
            run_block.block_data_size_ - run_block.next_row_pos_,
            compact_row,
            row))) {
      if (OB_BUF_NOT_ENOUGH == ret || OB_ITER_END == ret) {
        if (OB_FAIL(read_next_run_block(run_block))) {
          LOG_WARN("failed to read next block", K(ret));
        } else if (OB_FAIL(parse_row(run_block.get_buffer() + run_block.get_base_pos() + run_block.next_row_pos_,
                       run_block.block_data_size_ - run_block.next_row_pos_,
                       compact_row,
                       row))) {
          LOG_WARN("failed to get next row after read new block", K(ret));
        }
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      run_block.next_row_pos_ += compact_row.length();
    }
  }
  return ret;
}

int ObRunFile::parse_row(const char* buf, const int64_t buf_len, ObString& compact_row, ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObCellReader cell_reader;
  ObString input_buffer;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or buf_len < 0", K(ret), K(buf), K(buf_len));
  } else {
    input_buffer.assign_ptr(const_cast<char*>(buf), static_cast<int32_t>(buf_len));
    cell_reader.init(input_buffer.ptr(), input_buffer.length(), DENSE);
    uint64_t column_id = OB_INVALID_ID;
    ObObj cell;
    bool is_row_finished = false;
    int64_t cell_idx = 0;
    while (OB_SUCC(ret) && !is_row_finished && OB_SUCC(cell_reader.next_cell())) {
      if (OB_FAIL(cell_reader.get_cell(column_id, cell, &is_row_finished, &compact_row))) {
        LOG_WARN("failed to get cell", K(ret));
      } else if (is_row_finished) {
        // nothing.
      } else {
        row.cells_[cell_idx++] = cell;
      }
    }
    if (OB_SUCC(ret) && is_row_finished) {
      if (cell_idx != row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("corrupted row data", K_(row.count), K(cell_idx));
      }
    }
  }
  return ret;
}

int ObRunFile::read_next_run_block(RunBlock& run_block)
{
  int ret = OB_SUCCESS;
  const int64_t read_offset = run_block.block_offset_ + run_block.next_row_pos_;
  const int64_t count = (run_block.run_end_offset_ - read_offset < run_block.BLOCK_SIZE)
                            ? (run_block.run_end_offset_ - read_offset)
                            : run_block.BLOCK_SIZE;
  run_block.block_data_size_ = count;
  run_block.block_offset_ = read_offset;
  run_block.next_row_pos_ = 0;
  // read the next data block
  int64_t read_size = 0;
  if (OB_FAIL(file_reader_.pread(run_block.block_data_size_, run_block.block_offset_, run_block, read_size)) ||
      read_size != run_block.block_data_size_) {
    ret = OB_IO_ERROR;
    LOG_WARN("failed to read run file",
        K(ret),
        "offset",
        run_block.block_offset_,
        K(read_size),
        "data_size",
        run_block.block_data_size_);
  } else {
    LOG_DEBUG("read block", K(read_size), "offset", run_block.block_offset_);
  }
  return ret;
}

int ObRunFile::end_read_bucket()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < run_blocks_.count(); ++i) {
    run_blocks_.at(i).free_buffer();
  }  // end for
  run_blocks_.reset();
  return ret;
}
