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


#ifndef OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_
#define OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_


#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"


namespace oceanbase
{
namespace common
{
}

namespace sql {

class ObExternalDataAccessDriver
{
public:
  ObExternalDataAccessDriver() : storage_type_(common::OB_STORAGE_MAX_TYPE), device_handle_(nullptr) {}
  ~ObExternalDataAccessDriver();
  int init(const common::ObString &location, const ObString &access_info);
  int open(const common::ObString &url);
  bool is_opened();
  int get_file_size(const common::ObString &url, int64_t &file_size);

  int get_file_sizes(const ObString &location, const ObIArray<ObString> &urls, ObIArray<int64_t> &file_sizes);
  int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
  int get_file_list(const ObString &path,
                    ObIArray<ObString> &file_urls,
                    ObIAllocator &allocator);
  static int resolve_storage_type(const ObString &location, common::ObStorageType &device_type);
  common::ObStorageType get_storage_type() { return storage_type_; }
  void close();

  const char dummy_empty_char = '\0';
private:
  common::ObStorageType storage_type_;
  ObIODOpt opts_[1];
  ObIODOpts iod_opts_;
  share::ObBackupStorageInfo access_info_;
  ObIODevice* device_handle_;
  ObIOFd fd_;
};


class ObExternalTableRowIterator : public common::ObNewRowIterator {
public:
  ObExternalTableRowIterator() : scan_param_(nullptr) {}
  virtual int init(const storage::ObTableScanParam *scan_param) {
    scan_param_ = scan_param;
    return common::OB_SUCCESS;
  }
protected:
  const storage::ObTableScanParam *scan_param_;
};

class ObExternalTableAccessService : public common::ObITabletScan
{
public:
  ObExternalTableAccessService() {}
  virtual int table_scan(common::ObVTableScanParam &param, ObNewRowIterator *&result) override;
  virtual int table_rescan(ObVTableScanParam &param, ObNewRowIterator *result) override;
  virtual int reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter) override;
  virtual int revert_scan_iter(ObNewRowIterator *iter) override;

private:

  DISALLOW_COPY_AND_ASSIGN(ObExternalTableAccessService);
};

class ObCSVTableRowIterator : public ObExternalTableRowIterator {
public:
  static const int64_t MIN_EXTERNAL_TABLE_FILE_ID = 1;
  static const int64_t MIN_EXTERNAL_TABLE_LINE_NUMBER = 1;

public:
  struct StateValues {
    StateValues() :
      buf_(nullptr), buf_len_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      pos_(nullptr), data_end_(nullptr), escape_buf_(nullptr), escape_buf_end_(nullptr),
      is_end_file_(true), file_idx_(0), file_offset_(0), file_size_(0), skip_lines_(0),
      cur_file_id_(MIN_EXTERNAL_TABLE_FILE_ID), cur_line_number_(MIN_EXTERNAL_TABLE_LINE_NUMBER),
      line_count_limit_(INT64_MAX) {}
    char *buf_;
    int64_t buf_len_;
    const char *pos_;
    const char *data_end_;
    char *escape_buf_;
    char *escape_buf_end_;
    bool is_end_file_;
    int64_t file_idx_;
    int64_t file_offset_;
    int64_t file_size_;
    int64_t skip_lines_;
    common::ObString cur_file_name_;
    int64_t cur_file_id_;
    int64_t cur_line_number_;
    int64_t line_count_limit_;
    void reuse() {
      pos_ = buf_;
      data_end_ = buf_;
      is_end_file_ = true;
      file_idx_ = 0;
      file_offset_ = 0;
      file_size_ = 0;
      skip_lines_ = 0;
      cur_file_name_.reset();
      cur_file_id_ = MIN_EXTERNAL_TABLE_FILE_ID;
      cur_line_number_ = MIN_EXTERNAL_TABLE_LINE_NUMBER;
      line_count_limit_ = INT64_MAX;
    }
    TO_STRING_KV(KP(buf_), K(buf_len_), KP(pos_), KP(data_end_), K(is_end_file_), K(file_idx_),
                 K(file_offset_), K(file_size_), K(skip_lines_), K(line_count_limit_),
                 K(cur_file_name_), K(cur_file_id_), K(cur_line_number_), K(line_count_limit_));
  };

  ObCSVTableRowIterator() : bit_vector_cache_(NULL), line_number_expr_(NULL), file_id_expr_(NULL) {}
  virtual ~ObCSVTableRowIterator();
  int init(const storage::ObTableScanParam *scan_param) override;
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
                                    int64_t &start_line,
                                    int64_t &end_line);
  int skip_lines();
  void release_buf();
  void dump_error_log(common::ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs);
  int init_exprs(const storage::ObTableScanParam *scan_param);
private:
  ObBitVector *bit_vector_cache_;
  StateValues state_;
  common::ObMalloc allocator_;
  ObCSVGeneralParser parser_;
  ObExternalDataAccessDriver data_access_driver_;
  ObSqlString url_;
  ObSEArray<ObExpr*, 16> column_exprs_;
  ObExpr *line_number_expr_;
  ObExpr *file_id_expr_;
};

}


}

#endif // OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_
