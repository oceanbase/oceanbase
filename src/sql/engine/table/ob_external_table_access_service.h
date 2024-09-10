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
class ObExprRegexpSessionVariables;
class ObDecompressor;

class ObExternalDataAccessDriver
{
public:
  ObExternalDataAccessDriver() : storage_type_(common::OB_STORAGE_MAX_TYPE), device_handle_(nullptr) {}
  ~ObExternalDataAccessDriver();
  int init(const common::ObString &location, const ObString &access_info);
  int open(const char *url);
  bool is_opened() const;
  int get_file_size(const common::ObString &url, int64_t &file_size);

  int get_file_sizes(const ObString &location, const ObIArray<ObString> &urls, ObIArray<int64_t> &file_sizes);
  int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
  int get_file_list(const common::ObString &path,
                    const common::ObString &pattern,
                    const ObExprRegexpSessionVariables &regexp_vars,
                    common::ObIArray<common::ObString> &file_urls,
                    common::ObIArray<int64_t> &file_sizes,
                    common::ObIAllocator &allocator);
  common::ObStorageType get_storage_type() { return storage_type_; }
  void close();

  const char dummy_empty_char = '\0';
private:
  common::ObStorageType storage_type_;
  share::ObBackupStorageInfo access_info_;
  ObIODevice* device_handle_;
  ObIOFd fd_;
};

class ObExternalStreamFileReader final
{
public:
  // ObExternalStreamFileReader();
  ~ObExternalStreamFileReader();
  void reset();

  int init(const common::ObString &location,
           const ObString &access_info,
           ObLoadCompressionFormat compression_format,
           ObIAllocator &allocator);

  int open(const ObString &filename);
  void close();

  /**
   * read data into buffer. decompress source data if need
   */
  int read(char *buf, int64_t buf_len, int64_t &read_size);
  bool eof();

  common::ObStorageType get_storage_type() { return data_access_driver_.get_storage_type(); }

  ObExternalDataAccessDriver &get_data_access_driver() { return data_access_driver_; }

private:
  int read_from_driver(char *buf, int64_t buf_len, int64_t &read_size);
  int read_decompress(char *buf, int64_t buf_len, int64_t &read_size);
  int read_compressed_data(); // read data from driver into compressed buffer

  /**
   * create the decompressor if need
   *
   * It's no need to create new decompressor if the compression_format is the seem as decompressor's.
   */
  int create_decompressor(ObLoadCompressionFormat compression_format);

private:
  ObExternalDataAccessDriver data_access_driver_;
  bool    is_file_end_ = true;
  int64_t file_offset_ = 0;
  int64_t file_size_   = 0;

  ObDecompressor *decompressor_ = nullptr;
  char *  compressed_data_      = nullptr; /// compressed data buffer
  int64_t compress_data_size_   = 0;       /// the valid data size in compressed data buffer
  int64_t consumed_data_size_   = 0;       /// handled buffer size in the compressed data buffer
  int64_t uncompressed_size_    = 0;       /// decompressed size from compressed data

  ObIAllocator *allocator_ = nullptr;

  /// the compression format specified in `create external table` statement
  ObLoadCompressionFormat compression_format_ = ObLoadCompressionFormat::NONE;

  static const char * MEMORY_LABEL;
  static const int64_t COMPRESSED_DATA_BUFFER_SIZE;
};

class ObExternalTableRowIterator : public common::ObNewRowIterator {
public:
  ObExternalTableRowIterator() :
    scan_param_(nullptr), line_number_expr_(NULL), file_id_expr_(NULL), file_name_expr_(NULL)
  {}
  virtual int init(const storage::ObTableScanParam *scan_param);
protected:
  int init_exprs(const storage::ObTableScanParam *scan_param);
  int gen_ip_port(common::ObIAllocator &allocator);
  int calc_file_partition_list_value(const int64_t part_id, common::ObIAllocator &allocator, common::ObNewRow &value);
  int fill_file_partition_expr(ObExpr *expr, common::ObNewRow &value, const int64_t row_count);
protected:
  const storage::ObTableScanParam *scan_param_;
  //external table column exprs
  common::ObSEArray<ObExpr*, 16> column_exprs_;
  //hidden columns
  ObExpr *line_number_expr_;
  ObExpr *file_id_expr_;
  ObExpr *file_name_expr_;
  common::ObString ip_port_;
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
  static const int max_ipv6_port_length = 100;
public:
  struct StateValues {
    StateValues() :
      buf_(nullptr), buf_len_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      pos_(nullptr), data_end_(nullptr), escape_buf_(nullptr), escape_buf_end_(nullptr),
      file_idx_(0), skip_lines_(0),
      cur_file_id_(MIN_EXTERNAL_TABLE_FILE_ID), cur_line_number_(MIN_EXTERNAL_TABLE_LINE_NUMBER),
      line_count_limit_(INT64_MAX), part_id_(0), part_list_val_(), ip_port_buf_(NULL), ip_port_len_(0), file_with_url_() {}
    char *buf_;
    int64_t buf_len_;
    const char *pos_;
    const char *data_end_;
    char *escape_buf_;
    char *escape_buf_end_;
    int64_t file_idx_;
    int64_t skip_lines_;
    common::ObString cur_file_name_;
    int64_t cur_file_id_;
    int64_t cur_line_number_;
    int64_t line_count_limit_;
    int64_t part_id_;
    ObNewRow part_list_val_;
    char *ip_port_buf_;
    int ip_port_len_;
    ObString file_with_url_;
    void reuse() {
      pos_ = buf_;
      data_end_ = buf_;
      file_idx_ = 0;
      skip_lines_ = 0;
      cur_file_name_.reset();
      cur_file_id_ = MIN_EXTERNAL_TABLE_FILE_ID;
      cur_line_number_ = MIN_EXTERNAL_TABLE_LINE_NUMBER;
      line_count_limit_ = INT64_MAX;
      part_id_ = 0;
      part_list_val_.reset();
      ip_port_len_ = 0;
      file_with_url_.reset();
    }
    TO_STRING_KV(KP(buf_), K(buf_len_), KP(pos_), KP(data_end_), K(file_idx_),
                 K(skip_lines_), K(line_count_limit_),
                 K(cur_file_name_), K(cur_file_id_), K(cur_line_number_), K(line_count_limit_), K_(part_id), K_(ip_port_len), K_(file_with_url));
  };

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
  ObBitVector *bit_vector_cache_;
  StateValues state_;
  common::ObMalloc malloc_alloc_; //for internal data buffers
  common::ObArenaAllocator arena_alloc_;
  ObCSVGeneralParser parser_;
  ObExternalStreamFileReader file_reader_;
  ObSqlString url_;
  ObExpr *file_name_expr_;
};


}
}

#endif // OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_
