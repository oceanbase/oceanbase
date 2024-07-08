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

#ifndef OB_PARQUET_TABLE_ROW_ITER_H
#define OB_PARQUET_TABLE_ROW_ITER_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase {
namespace sql {

class ObArrowMemPool : public ::arrow::MemoryPool
{
public:
  ObArrowMemPool() : total_alloc_size_(0) {}
  void init(uint64_t tenant_id);
  virtual arrow::Status Allocate(int64_t size, uint8_t** out) override;

  virtual arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override;

  virtual void Free(uint8_t* buffer, int64_t size) override;

  virtual void ReleaseUnused() override;

  virtual int64_t bytes_allocated() const override;

  virtual int64_t max_memory() const override { return -1; }

  virtual std::string backend_name() const override { return "Arrow"; }
private:
  common::ObArenaAllocator alloc_;
  common::ObMemAttr mem_attr_;
  arrow::internal::MemoryPoolStats stats_;
  int64_t total_alloc_size_;
};


class ObArrowFile : public arrow::io::RandomAccessFile {
public:
  ObArrowFile(ObExternalDataAccessDriver &file_reader, const char*file_name, arrow::MemoryPool *pool)
    : file_reader_(file_reader), file_name_(file_name), pool_(pool)
  {}
  ~ObArrowFile() override {
    file_reader_.close();
  }

  int open();

  virtual arrow::Status Close() override;

  virtual bool closed() const override;

  virtual arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
  virtual arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;


  virtual arrow::Status Seek(int64_t position) override;
  virtual arrow::Result<int64_t> Tell() const override;
  virtual arrow::Result<int64_t> GetSize() override;
private:
  ObExternalDataAccessDriver &file_reader_;
  const char* file_name_;
  arrow::MemoryPool *pool_;
  int64_t position_;
};

class ObParquetTableRowIterator : public ObExternalTableRowIterator {
public:
  struct StateValues {
    StateValues() :
      file_idx_(0),
      part_id_(0),
      row_group_idx_(0),
      cur_file_id_(0),
      cur_row_group_idx_(0),
      end_row_group_idx_(-1),
      cur_row_group_read_row_count_(0),
      cur_row_group_row_count_(0),
      cur_line_number_(0) {}

    void reuse() {
      file_idx_ = 0;
      part_id_ = 0;
      row_group_idx_ = 0;
      cur_file_id_ = 0;
      cur_row_group_idx_ = 0;
      end_row_group_idx_ = -1;
      cur_row_group_read_row_count_ = 0;
      cur_row_group_row_count_ = 0;
      cur_line_number_ = 0;
      cur_file_url_.reset();
      part_list_val_.reset();
    }

    int64_t file_idx_;
    int64_t part_id_;
    int64_t row_group_idx_;
    int64_t cur_file_id_;
    int64_t cur_row_group_idx_;
    int64_t end_row_group_idx_;
    int64_t cur_row_group_read_row_count_;
    int64_t cur_row_group_row_count_;
    int64_t cur_line_number_;
    ObString cur_file_url_;
    ObNewRow part_list_val_;
  };
public:
  ObParquetTableRowIterator() :
    read_props_(&arrow_alloc_),
    file_column_exprs_(allocator_),
    file_meta_column_exprs_(allocator_),
    bit_vector_cache_(NULL) {}
  virtual ~ObParquetTableRowIterator();

  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }

  virtual void reset() override;
private:
  // load vec data from parquet file to expr mem
  struct DataLoader {
    DataLoader(ObEvalCtx &eval_ctx,
               ObExpr *file_col_expr,
               parquet::ColumnReader *reader,
               common::ObIArrayWrap<int16_t> &def_levels_buf,
               common::ObIArrayWrap<int16_t> &rep_levels_buf,
               const int64_t batch_size,
               int64_t &row_count):
      eval_ctx_(eval_ctx),
      file_col_expr_(file_col_expr),
      reader_(reader),
      batch_size_(batch_size),
      row_count_(row_count),
      def_levels_buf_(def_levels_buf),
      rep_levels_buf_(rep_levels_buf)
    {}
    typedef int (DataLoader::*LOAD_FUNC)();
    static LOAD_FUNC select_load_function(const ObDatumMeta &datum_type,
                                          const parquet::ColumnDescriptor *col_desc);
    int16_t get_max_def_level();
    int load_data_for_col(LOAD_FUNC &func);

    int load_int64_to_int64_vec();
    int load_int32_to_int64_vec();
    int load_int32_to_int32_vec();
    int load_string_col();
    int load_fixed_string_col();
    int load_decimal_any_col();
    //[TODO EXTERNAL TABLE] float16
    int load_date_col_to_datetime();
    int load_time_millis_col();
    int load_time_nanos_col();
    int load_timestamp_millis_col();
    int load_timestamp_micros_col();
    int load_timestamp_nanos_col();
    int load_timestamp_hive();
    int load_float();
    int load_double();

    int to_numeric(const int64_t idx, const int64_t int_value);
    int to_numeric(const int64_t idx, const char *str, const int32_t length);
    int to_numeric_hive(const int64_t idx, const char *str, const int32_t length, char *buf, const int64_t data_len);
    int64_t calc_tz_adjust_us();
    bool check_char_len(const char *ptr, int32_t len);

    static bool is_ob_type_store_utc(const ObDatumMeta &meta);
    static bool is_parquet_store_utc(const parquet::LogicalType *logtype);

    ObEvalCtx &eval_ctx_;
    ObExpr *file_col_expr_;
    parquet::ColumnReader *reader_;
    const int64_t batch_size_;
    int64_t &row_count_;
    common::ObIArrayWrap<int16_t> &def_levels_buf_;
    common::ObIArrayWrap<int16_t> &rep_levels_buf_;
  };
private:
  int next_file();
  int next_row_group();
  int calc_exprs_for_rowid(const int64_t read_count);
  int calc_pseudo_exprs(const int64_t read_count);
private:
  StateValues state_;
  lib::ObMemAttr mem_attr_;
  ObArenaAllocator allocator_;
  ObArrowMemPool arrow_alloc_;
  parquet::ReaderProperties read_props_;
  ObExternalDataAccessDriver data_access_driver_;
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::shared_ptr<parquet::FileMetaData> file_meta_;
  ExprFixedArray file_column_exprs_; //column value from parquet file
  ExprFixedArray file_meta_column_exprs_; //column value from file meta
  common::ObArrayWrap<int> column_indexs_;
  common::ObArrayWrap<std::shared_ptr<parquet::ColumnReader>> column_readers_;
  common::ObArrayWrap<DataLoader::LOAD_FUNC> load_funcs_;
  ObSqlString url_;
  ObBitVector *bit_vector_cache_;
  common::ObArrayWrap<int16_t> def_levels_buf_;
  common::ObArrayWrap<int16_t> rep_levels_buf_;
  common::ObArrayWrap<char *> file_url_ptrs_; //for file url expr
  common::ObArrayWrap<ObLength> file_url_lens_; //for file url expr
};

}
}

#endif // OB_PARQUET_TABLE_ROW_ITER_H
