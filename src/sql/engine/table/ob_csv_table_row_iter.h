/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CSV_TABLE_ROW_ITER_H
#define OB_CSV_TABLE_ROW_ITER_H

#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_external_file_access.h"
#include "sql/engine/table/ob_csv_prefetch_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/hash_func/murmur_hash.h"

namespace oceanbase {
namespace sql {

class ObCSVIteratorState : public ObExternalIteratorState
{
public:
  static const int64_t MIN_EXTERNAL_TABLE_FILE_ID = 1;
  static const int64_t MIN_EXTERNAL_TABLE_LINE_NUMBER = 1;
  ObCSVIteratorState() :
    buf_(nullptr),
    buf_len_(OB_MALLOC_NORMAL_BLOCK_SIZE),
    pos_(nullptr),
    data_end_(nullptr),
    escape_buf_(nullptr),
    escape_buf_end_(nullptr),
    skip_lines_(0),
    cur_file_name_(),
    line_count_limit_(INT64_MAX),
    ip_port_buf_(NULL),
    ip_port_len_(0),
    need_expand_buf_(false),
    duration_(0),
    cur_file_size_(0),
    has_escape_(true),
    bounded_start_pos_(0),
    bounded_end_pos_(INT64_MAX),
    already_read_size_(0),
    is_scan_full_file_(true),
    chunk_idx_(OB_INVALID_INDEX) {}

  virtual void reuse() override
  {
    ObExternalIteratorState::reuse();
    // cur_file_id_ = MIN_EXTERNAL_TABLE_FILE_ID;
    // cur_line_number_ = MIN_EXTERNAL_TABLE_LINE_NUMBER;
    pos_ = buf_;
    data_end_ = buf_;
    skip_lines_ = 0;
    cur_file_name_.reset();
    line_count_limit_ = INT64_MAX;
    ip_port_len_ = 0;
    need_expand_buf_ = false;
    duration_ = 0;
    cur_file_size_ = 0;
    has_escape_ = true;
    bounded_start_pos_ = 0;
    bounded_end_pos_ = INT64_MAX;
    already_read_size_ = 0;
    is_scan_full_file_ = true;
    chunk_idx_ = OB_INVALID_INDEX;
  }
  DECLARE_VIRTUAL_TO_STRING;
  char *buf_;
  int64_t buf_len_;
  const char *pos_;
  const char *data_end_;
  char *escape_buf_;
  char *escape_buf_end_;
  int64_t skip_lines_;
  ObString cur_file_name_;
  int64_t line_count_limit_;
  char *ip_port_buf_;
  int ip_port_len_;
  bool need_expand_buf_;
  int64_t duration_;
  int64_t cur_file_size_;
  bool has_escape_;
  int64_t bounded_start_pos_;
  int64_t bounded_end_pos_;
  int64_t already_read_size_;
  bool is_scan_full_file_;
  int64_t chunk_idx_;
};

class ObCSVTableRowIterator : public ObExternalTableRowIterator {
public:
  static const int64_t MIN_EXTERNAL_TABLE_FILE_ID = 1;
  static const int64_t MIN_EXTERNAL_TABLE_LINE_NUMBER = 1;
  static const int max_ipv6_port_length = 100;
  ObCSVTableRowIterator() : bit_vector_cache_(NULL),
                            is_bad_file_enabled_(false),
                            max_buffer_size_(1024 * 1024 * 1024),
                            enable_prefetch_(false),
                            prefetch_mgr_(),
                            storage_type_(OB_STORAGE_MAX_TYPE) {}
  virtual ~ObCSVTableRowIterator();
  virtual int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }

  virtual void reset() override;
  virtual bool is_diagnosis_supported() const override { return true; }
  virtual int64_t get_cur_line_num() const override { return state_.batch_first_row_line_num_; }
  virtual ObString get_cur_file_url() const override { return state_.cur_file_name_; }

private:
  int expand_buf();
  int load_next_buf();
  int open_next_file();
  int get_next_file_scan_info(const int64_t task_idx,
                               common::ObString &file_url,
                               int64_t &file_id,
                               int64_t &part_id,
                               int64_t &start_line,
                               int64_t &end_line,
                               int64_t &bounded_start_pos,
                               int64_t &bounded_end_pos,
                               int64_t &chunk_idx);
  int skip_lines();
  void release_buf();
  void dump_error_log(common::ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs);
  int handle_error_msgs(common::ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs);
  static int handle_bad_file_line(ObCSVTableRowIterator *csv_iter,
                                  ObEvalCtx &eval_ctx,
                                  ObCSVGeneralParser::HandleOneLineParam &param);
  bool is_end_of_file();
  OB_INLINE bool check_row_sample_filtered(const int64_t row_num) const
  {
    uint64_t hash_value = murmurhash(&row_num, sizeof(row_num), row_sample_seed_);
    return hash_value > row_sample_cut_off_;
  }
private:
  ObCSVIteratorState state_;
  ObBitVector *bit_vector_cache_;
  common::ObMalloc malloc_alloc_; //for internal data buffers
  common::ObArenaAllocator arena_alloc_;
  ObCSVGeneralParser parser_;
  ObExternalStreamFileReader file_reader_;
  ObSqlString url_;
  ObExpr *file_name_expr_;
  bool is_bad_file_enabled_;
  bool use_handle_batch_lines_ = false;
  int64_t max_buffer_size_;
  bool enable_prefetch_;
  ObCSVPrefetchMgr prefetch_mgr_;
  common::ObStorageType storage_type_;
  int64_t global_row_counter_ = 0;
  bool is_row_sample_ = false;
  uint64_t row_sample_seed_ = 0;
  uint64_t row_sample_cut_off_ = 0;
};


}
}

#endif // OB_CSV_TABLE_ROW_ITER_H