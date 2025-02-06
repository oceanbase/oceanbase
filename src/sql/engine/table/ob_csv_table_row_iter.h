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

#ifndef OB_CSV_TABLE_ROW_ITER_H
#define OB_CSV_TABLE_ROW_ITER_H

#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"

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
    cur_file_size_(0) {}

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
};

class ObCSVTableRowIterator : public ObExternalTableRowIterator {
public:
  static const int64_t MIN_EXTERNAL_TABLE_FILE_ID = 1;
  static const int64_t MIN_EXTERNAL_TABLE_LINE_NUMBER = 1;
  static const int max_ipv6_port_length = 100;
  ObCSVTableRowIterator() : bit_vector_cache_(NULL) {}
  virtual ~ObCSVTableRowIterator();
  virtual int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }

  virtual void reset() override;

private:
  int expand_buf();
  int load_next_buf();
  int open_next_file();
  int get_next_file_and_line_number(const int64_t task_idx,
                                    common::ObString &file_url,
                                    int64_t &file_id,
                                    int64_t &part_id,
                                    int64_t &start_line,
                                    int64_t &end_line);
  int skip_lines();
  void release_buf();
  void dump_error_log(common::ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs);
private:
  ObCSVIteratorState state_;
  ObBitVector *bit_vector_cache_;
  common::ObMalloc malloc_alloc_; //for internal data buffers
  common::ObArenaAllocator arena_alloc_;
  ObCSVGeneralParser parser_;
  ObExternalStreamFileReader file_reader_;
  ObSqlString url_;
  ObExpr *file_name_expr_;
};


}
}

#endif // OB_CSV_TABLE_ROW_ITER_H