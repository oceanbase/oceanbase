/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_BATCH_RECORD_
#define OCEANBASE_LOG_MINER_BATCH_RECORD_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string_buffer.h"
#include "ob_log_miner_record.h"
#include "ob_log_miner_file_manager.h"
#include <cstdint>

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerRecordConverter;

class ObLogMinerBatchRecord: public ObLogMinerRecyclableTask
{
public:
  static const int64_t BATCH_RECORD_THRESHOLD = 2L * 1024 * 1024; // 2MB
private:
  ObLogMinerBatchRecord():
      ObLogMinerRecyclableTask(TaskType::BATCH_RECORD),
      alloc_(nullptr),
      converter_(nullptr),
      range_(),
      begin_record_(),
      buf_(),
      trans_data_record_count_(0),
      total_data_record_count_(0),
      written_record_count_(0),
      first_access_ts_(OB_INVALID_TIMESTAMP),
      last_trans_end_ts_(OB_INVALID_TIMESTAMP),
      seq_no_(-1),
      file_id_(-1),
      file_offset_(-1),
      is_file_end_(false),
      freezed_(false)
      {}
public:
  ObLogMinerBatchRecord(ObIAllocator *alloc, ILogMinerRecordConverter *converter):
      ObLogMinerBatchRecord()
  {
    set_allocator(alloc);
    set_record_converter(converter);
  }
  ~ObLogMinerBatchRecord()
  {
    reset();
    converter_ = nullptr;
    alloc_ = nullptr;
  }

  void reset();

  void set_allocator(ObIAllocator *alloc)
  {
    alloc_ = alloc;
    buf_.set_allocator(alloc_);
  }

  int init_last_trans_end_ts(const int64_t commit_ts);

  int64_t get_last_trans_end_ts() const
  {
    return last_trans_end_ts_;
  }

  void set_record_converter(ILogMinerRecordConverter *converter)
  {
    converter_ = converter;
  }

  int append_record(const ObLogMinerRecord &record);
  void get_data(const char *&data, int64_t &data_len) const {
    data = buf_.ptr();
    data_len = buf_.length();
  }

  int64_t get_total_record_count() const {
    return total_data_record_count_;
  }

  int64_t get_cur_trans_record_count() const {
    return trans_data_record_count_;
  }

   int64_t get_written_record_count() const {
    return written_record_count_;
  }

  const ObLogMinerProgressRange &get_progress_range() const {
    return range_;
  }
  int64_t get_first_access_ts() const {
    return first_access_ts_;
  }
  bool is_freezed() const {
    return freezed_;
  }

  void set_seq_no(const int64_t seq_no) {
    seq_no_ = seq_no;
  }

  int64_t get_seq_no() const {
    return seq_no_;
  }

  void set_file_id(const int64_t file_id) {
    file_id_ = file_id;
  }

  int64_t get_file_id() const {
    return file_id_;
  }

  void set_file_offset(const int64_t file_offset) {
    file_offset_ = file_offset;
  }

  int64_t get_file_offset() const {
    return file_offset_;
  }

  void set_file_end() {
    is_file_end_ = true;
  }

  bool is_file_end() const {
    return is_file_end_;
  }

  TO_STRING_KV(
    K(range_),
    K(begin_record_),
    K(trans_data_record_count_),
    K(total_data_record_count_),
    K(written_record_count_),
    K(first_access_ts_),
    K(seq_no_),
    K(file_id_),
    K(file_offset_),
    K(is_file_end_),
    K(freezed_)
  );

private:
  int handle_begin_record_(const ObLogMinerRecord &record);

  int handle_dml_record_(const ObLogMinerRecord &record);

  int handle_ddl_record_(const ObLogMinerRecord &record);

  int handle_commit_record_(const ObLogMinerRecord &record);

  int handle_heartbeat_record_(const ObLogMinerRecord &record);

  int write_record_(const ObLogMinerRecord &record);

  void update_progress_range_(const int64_t progress);

private:
  common::ObIAllocator *alloc_;
  ILogMinerRecordConverter *converter_;

  ObLogMinerProgressRange range_;
  ObLogMinerRecord begin_record_;
  ObStringBuffer buf_;
  int64_t trans_data_record_count_;
  int64_t total_data_record_count_;
  int64_t written_record_count_;
  int64_t first_access_ts_; // in milliseconds

  int64_t last_trans_end_ts_; // in milliseconds

  int64_t seq_no_;
  int64_t file_id_;
  int64_t file_offset_;
  bool is_file_end_;

  bool freezed_;
};

}
}

#endif