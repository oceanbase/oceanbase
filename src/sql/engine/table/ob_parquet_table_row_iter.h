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
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "storage/access/ob_sstable_index_filter.h"

namespace oceanbase {
namespace sql {

class ObParquetIteratorState : public ObExternalIteratorState {
public:
  ObParquetIteratorState() :
    row_group_idx_(0),
    cur_row_group_idx_(0),
    end_row_group_idx_(-1),
    cur_row_group_read_row_counts_(),
    cur_row_group_row_count_(0) {}
  int init(const int64_t column_cnt, ObIAllocator &alloc) {
    cur_row_group_read_row_counts_.set_allocator(&alloc);
    return cur_row_group_read_row_counts_.prepare_allocate(0 == column_cnt ? 1 : column_cnt, 0);
  }
  virtual void reuse() override
  {
    ObExternalIteratorState::reuse();
    row_group_idx_ = 0;
    cur_row_group_idx_ = 0;
    end_row_group_idx_ = -1;
    memset(pointer_cast<char *> (&cur_row_group_read_row_counts_.at(0)),
           0, sizeof(int64_t) * cur_row_group_read_row_counts_.count());
    cur_row_group_row_count_ = 0;
  }
  DECLARE_VIRTUAL_TO_STRING;
  int64_t row_group_idx_;
  int64_t cur_row_group_idx_;
  int64_t end_row_group_idx_;
  common::ObFixedArray<int64_t, common::ObIAllocator> cur_row_group_read_row_counts_;
  int64_t cur_row_group_row_count_;
};

class ObParquetTableRowIterator :
    public ObExternalTableRowIterator, public ObExternalTablePushdownFilter {
public:
  ObParquetTableRowIterator() :
    read_props_(&arrow_alloc_),
    file_column_exprs_(allocator_),
    mapping_column_exprs_(allocator_),
    mapping_column_ids_(allocator_),
    file_meta_column_exprs_(allocator_),
    bit_vector_cache_(NULL),
    options_(),
    file_prebuffer_(data_access_driver_),
    column_range_slices_(allocator_) {}
  virtual ~ObParquetTableRowIterator();

  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;
  static bool is_parquet_store_utc(const parquet::LogicalType *logtype);

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }
  virtual void reset() override;
  static int read_min_max_datum(const std::string &min_val, const std::string &max_val,
                         const parquet::Type::type physical_type, const parquet::LogicalType* log_type,
                         const ObDatumMeta &datum_type, const int64_t max_length,
                         const parquet::SortOrder::type sort_order, const bool has_lob_header,
                         ObEvalCtx &eval_ctx, blocksstable::ObStorageDatum &min_datum,
                         blocksstable::ObStorageDatum &max_datum, ObIAllocator &tmp_alloc);
private:
  class ParquetMinMaxFilterParamBuilder : public MinMaxFilterParamBuilder
  {
  public:
    explicit ParquetMinMaxFilterParamBuilder(ObParquetTableRowIterator *row_iter,
                                             std::shared_ptr<parquet::RowGroupReader> rg_reader,
                                             std::shared_ptr<parquet::FileMetaData> file_meta,
                                             ObIAllocator &tmp_alloc)
                                             : row_iter_(row_iter), rg_reader_(rg_reader),
                                               file_meta_(file_meta), tmp_alloc_(tmp_alloc) {}
    virtual ~ParquetMinMaxFilterParamBuilder() {}
    int build(const int64_t ext_table_id, const ObExpr *expr,
              blocksstable::ObMinMaxFilterParam &param) override;
  private:
    ObParquetTableRowIterator *row_iter_;
    std::shared_ptr<parquet::RowGroupReader> rg_reader_;
    std::shared_ptr<parquet::FileMetaData> file_meta_;
    ObIAllocator &tmp_alloc_;
  };
  // load vec data from parquet file to expr mem
  struct DataLoader {
    DataLoader(ObEvalCtx &eval_ctx,
               ObExpr *file_col_expr,
               parquet::ColumnReader *reader,
               common::ObIArrayWrap<int16_t> &def_levels_buf,
               common::ObIArrayWrap<int16_t> &rep_levels_buf,
               common::ObIAllocator &str_res_mem,
               const int64_t batch_size,
               const int64_t row_offset,
               int64_t &row_count):
      eval_ctx_(eval_ctx),
      file_col_expr_(file_col_expr),
      reader_(reader),
      batch_size_(batch_size),
      row_offset_(row_offset),
      row_count_(row_count),
      def_levels_buf_(def_levels_buf),
      rep_levels_buf_(rep_levels_buf),
      str_res_mem_(str_res_mem)
    {}
    typedef int (DataLoader::*LOAD_FUNC)();
    static LOAD_FUNC select_load_function(const ObDatumMeta &datum_type,
                                          const parquet::ColumnDescriptor *col_desc);
    int16_t get_max_def_level();
    int load_data_for_col(LOAD_FUNC &func);

    int load_int64_to_int64_vec();
    int load_int32_to_int64_vec();
    int load_uint32_to_int64_vec();
    int load_int32_to_int32_vec();
    int load_bool_to_int64_vec();
    int load_date_to_mysql_date();
    int load_string_col();
    int load_fixed_string_col();
    int load_decimal_any_col();
    //[TODO EXTERNAL TABLE] float16
    int load_date_col_to_datetime();
    int load_date_col_to_mysql_datetime();
    int load_year_col();
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
    static int64_t calc_tz_adjust_us(const parquet::LogicalType *logtype, const ObDatumMeta &meta, ObSQLSessionInfo *session);
    bool check_char_len(const char *ptr, int32_t len);

    static bool is_ob_type_store_utc(const ObDatumMeta &meta);

    ObEvalCtx &eval_ctx_;
    ObExpr *file_col_expr_;
    parquet::ColumnReader *reader_;
    const int64_t batch_size_;
    const int64_t row_offset_;
    int64_t &row_count_;
    common::ObIArrayWrap<int16_t> &def_levels_buf_;
    common::ObIArrayWrap<int16_t> &rep_levels_buf_;
    common::ObIAllocator &str_res_mem_;
  };
private:
  int next_file();
  int next_row_group();
  int calc_pseudo_exprs(const int64_t read_count);
  ObExternalTableAccessOptions& make_external_table_access_options(stmt::StmtType stmt_type);
  static int convert_timestamp_datum(const ObDatumMeta &datum_type, int64_t adjusted_min_value,
                              int64_t adjusted_max_value, blocksstable::ObStorageDatum &min_datum,
                              blocksstable::ObStorageDatum &max_datum);
  static int to_numeric_hive(const char *str, const int32_t length, char *buf, const int64_t data_len,
                             const ObDatumMeta &meta, common::ObIAllocator &alloc, blocksstable::ObStorageDatum &datum);
  int pre_buffer(std::shared_ptr<parquet::RowGroupReader> rg_reader);
  int compute_column_id_by_index_type(int index, int &file_col_id);
private:
  ObParquetIteratorState state_;
  lib::ObMemAttr mem_attr_;
  ObArenaAllocator allocator_;
  ObArenaAllocator str_res_mem_;
  ObArrowMemPool arrow_alloc_;
  parquet::ReaderProperties read_props_;
  ObExternalFileAccess data_access_driver_;
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::shared_ptr<parquet::FileMetaData> file_meta_;
  ExprFixedArray file_column_exprs_; //column value from parquet file
  ExprFixedArray mapping_column_exprs_;
  ObFixedArray<uint64_t, ObIAllocator> mapping_column_ids_;
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
  ObExternalTableAccessOptions options_;
  ObFilePreBuffer file_prebuffer_;
  common::ObFixedArray<ObFilePreBuffer::ColumnRangeSlices *, ObIAllocator> column_range_slices_;
};

}
}

#endif // OB_PARQUET_TABLE_ROW_ITER_H
