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
#include <parquet/page_index.h>
#include <parquet/stream_reader.h>

namespace oceanbase {
namespace sql {

struct ParquetStatInfo {
  ParquetStatInfo() : projected_eager_cnt_(0), projected_lazy_cnt_(0), cross_page_cnt_(0), in_page_cnt_(0) {}
  int64_t projected_eager_cnt_;
  int64_t projected_lazy_cnt_;
  int64_t cross_page_cnt_;
  int64_t in_page_cnt_;
  TO_STRING_KV(K_(projected_eager_cnt), K_(projected_lazy_cnt), K_(cross_page_cnt), K_(in_page_cnt));
};

enum FilterCalcMode {
  DYNAMIC_EAGER_CALC,
  DYNAMIC_LAZY_CALC,
  FORCE_EAGER_CALC,
  FORCE_LAZY_CALC,
};

struct ReadPages
{
  ReadPages(int64_t &cur_col_id,
            int64_t &cur_eager_id,
            common::ObIArray<int64_t> &read_row_counts,
            common::ObIArray<ObArray<std::pair<int64_t, int64_t>> *> &page_skip_ranges,
            ObLakeTableParquetReaderMetrics &reader_metrics)
            : cur_col_id_(cur_col_id),
              cur_eager_id_(cur_eager_id),
              read_row_counts_(read_row_counts),
              page_skip_ranges_(page_skip_ranges),
              reader_metrics_(reader_metrics) {}
  bool operator() (const parquet::DataPageStats& stats)
  {
    bool can_skip = false;
    int64_t begin = read_row_counts_.at(cur_eager_id_);
    int64_t end = begin + stats.num_values;
    for (int64_t i = 0; !can_skip && i < page_skip_ranges_.at(cur_col_id_)->count(); ++i) {
      if (begin >= page_skip_ranges_.at(cur_col_id_)->at(i).first
          && end <= page_skip_ranges_.at(cur_col_id_)->at(i).second) {
        can_skip = true;
      }
    }
    if (!can_skip) {
      ++reader_metrics_.selected_page_count_;
      //ObTaskController::get().allow_next_syslog();
      //LOG_INFO("print can not skip", K(cur_col_id_), K(state_.cur_row_group_idx_), K(begin), K(end));
    } else {
      ++reader_metrics_.skipped_page_count_;
      //ObTaskController::get().allow_next_syslog();
      //LOG_INFO("print can skip", K(cur_col_id_), K(state_.cur_row_group_idx_), K(begin), K(end));
      read_row_counts_.at(cur_eager_id_) += stats.num_values; /*TODO: check if is correct*/
    }
    return can_skip;
  }
  int64_t &cur_col_id_;
  int64_t &cur_eager_id_;
  common::ObIArray<int64_t> &read_row_counts_;
  common::ObIArray<ObArray<std::pair<int64_t, int64_t>> *> &page_skip_ranges_;
  ObLakeTableParquetReaderMetrics &reader_metrics_;
};


class ObParquetIteratorState : public ObExternalIteratorState {
public:
  ObParquetIteratorState() :
    row_group_idx_(0),
    cur_row_group_idx_(0),
    cur_row_group_begin_row_id_(0),
    end_row_group_idx_(-1),
    read_row_counts_(),
    eager_read_row_counts_(),
    cur_row_group_row_count_(0),
    logical_read_row_count_(0),
    logical_eager_read_row_count_(0) {}
  int init(const int64_t column_cnt, const int64_t eager_cnt, ObIAllocator &alloc) {
    int ret = OB_SUCCESS;
    read_row_counts_.set_allocator(&alloc);
    eager_read_row_counts_.set_allocator(&alloc);
    OZ (read_row_counts_.prepare_allocate(0 == column_cnt ? 1 : column_cnt, 0));
    OZ (eager_read_row_counts_.prepare_allocate(0 == eager_cnt ? 1 : eager_cnt, 0));
    return ret;
  }
  virtual void reuse() override
  {
    ObExternalIteratorState::reuse();
    row_group_idx_ = 0;
    cur_row_group_idx_ = 0;
    cur_row_group_begin_row_id_ = 0;
    end_row_group_idx_ = -1;
    if (read_row_counts_.count() > 0) {
      memset(pointer_cast<char *> (&read_row_counts_.at(0)),
           0, sizeof(int64_t) * read_row_counts_.count());
    }
    if (eager_read_row_counts_.count() > 0) {
      memset(pointer_cast<char *> (&eager_read_row_counts_.at(0)),
           0, sizeof(int64_t) * eager_read_row_counts_.count());
    }
    cur_row_group_row_count_ = 0;
    logical_read_row_count_ = 0;
    logical_eager_read_row_count_ = 0;
  }
  DECLARE_VIRTUAL_TO_STRING;
  int64_t row_group_idx_;
  int64_t cur_row_group_idx_;
  int64_t cur_row_group_begin_row_id_;
  int64_t end_row_group_idx_;
  common::ObFixedArray<int64_t, common::ObIAllocator> read_row_counts_;
  common::ObFixedArray<int64_t, common::ObIAllocator> eager_read_row_counts_;
  int64_t cur_row_group_row_count_;
  int64_t logical_read_row_count_;
  int64_t logical_eager_read_row_count_;
};

class ObParquetTableRowIterator :
    public ObExternalTableRowIterator, public ObExternalTablePushdownFilter {
public:
  static const int64_t SECTOR_SIZE = 8192;
  static const constexpr double EAGER_CALC_CUT_RATIO = 0.66;
  ObParquetTableRowIterator() :
    read_props_(&arrow_alloc_),
    bit_vector_cache_(NULL),
    options_(),
    file_prebuffer_(data_access_driver_),
    eager_file_prebuffer_(eager_data_access_driver_),
    column_range_slices_(allocator_),
    cur_col_id_(-1),
    cur_eager_id_(-1),
    rg_bitmap_(nullptr),
    malloc_allocator_(),
    sector_iter_(SECTOR_SIZE, this, allocator_),
    page_index_reader_(nullptr),
    rg_page_index_reader_(nullptr),
    page_skip_ranges_(allocator_),
    stat_(),
    mode_(FilterCalcMode::DYNAMIC_EAGER_CALC),
    reader_metrics_(),
    column_index_type_(sql::ColumnIndexType::NAME),
    is_col_name_case_sensitive_(false) {}
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
                         const ObColumnMeta &column_meta, const parquet::SortOrder::type sort_order,
                         ObEvalCtx &eval_ctx, blocksstable::ObStorageDatum &min_datum,
                         blocksstable::ObStorageDatum &max_datum, ObIAllocator &tmp_alloc);
  bool is_eager_calc() const { return FilterCalcMode::DYNAMIC_EAGER_CALC == mode_ || FilterCalcMode::FORCE_EAGER_CALC == mode_; }
  bool is_lazy_calc() const { return FilterCalcMode::DYNAMIC_LAZY_CALC == mode_ || FilterCalcMode::FORCE_LAZY_CALC == mode_; }
  bool is_dynamic_calc() const { return FilterCalcMode::DYNAMIC_EAGER_CALC == mode_ || FilterCalcMode::DYNAMIC_LAZY_CALC == mode_; }
  bool has_eager_columns() const { return is_eager_calc() && eager_columns_.count() > 0; }
  int64_t get_eager_count() const { return is_eager_calc() ? eager_columns_.count() : 0; }
  int64_t get_lazy_file_count() const { return is_eager_calc() ? lazy_columns_.count() : file_column_exprs_.count(); }
  int64_t get_lazy_access_count() const { return is_eager_calc() ? lazy_columns_.count() : column_exprs_.count();  }
  int64_t get_lazy_column_id(const int64_t i) const { return is_eager_calc() ? lazy_columns_.at(i) : i; }
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
    int build(const int32_t ext_table_id, const ObColumnMeta &column_meta,
              blocksstable::ObMinMaxFilterParam &param) override;
    int next_range(const int64_t column_id, int64_t &offset, int64_t &rows) override
    {
      return OB_NOT_SUPPORTED;
    }
  private:
    ObParquetTableRowIterator *row_iter_;
    std::shared_ptr<parquet::RowGroupReader> rg_reader_;
    std::shared_ptr<parquet::FileMetaData> file_meta_;
    ObIAllocator &tmp_alloc_;
  };
  class ParquetPageMinMaxFilterParamBuilder : public MinMaxFilterParamBuilder
  {
  public:
    explicit ParquetPageMinMaxFilterParamBuilder(ObParquetTableRowIterator *row_iter,
                                             std::shared_ptr<parquet::RowGroupReader> rg_reader,
                                             std::shared_ptr<parquet::FileMetaData> file_meta,
                                             std::shared_ptr<parquet::PageIndexReader> page_index_reader,
                                             std::shared_ptr<parquet::RowGroupPageIndexReader> row_group_page_index_reader,
                                             ObIAllocator &tmp_alloc)
                                             : row_iter_(row_iter), rg_reader_(rg_reader),
                                               file_meta_(file_meta), page_index_reader_(page_index_reader),
                                               rg_page_index_reader_(row_group_page_index_reader),
                                               tmp_alloc_(tmp_alloc), page_id_(-1),
                                               last_column_id_(-1) {}
    virtual ~ParquetPageMinMaxFilterParamBuilder() {}
    int build(const int32_t ext_col_id, const ObColumnMeta &column_meta,
              blocksstable::ObMinMaxFilterParam &param) override;
    int next_range(const int64_t column_id, int64_t &offset, int64_t &rows) override;
    void rescan() override
    {
      page_id_ = -1;
    }
    bool has_page_index() const override { return nullptr != rg_page_index_reader_; }
  private:
    ObParquetTableRowIterator *row_iter_;
    std::shared_ptr<parquet::RowGroupReader> rg_reader_;
    std::shared_ptr<parquet::FileMetaData> file_meta_;
    std::shared_ptr<parquet::PageIndexReader> page_index_reader_;
    std::shared_ptr<parquet::RowGroupPageIndexReader> rg_page_index_reader_;
    ObIAllocator &tmp_alloc_;
    int64_t page_id_;
    int64_t last_column_id_;
  };
  // load vec data from parquet file to expr mem
  struct DataLoader {
    DataLoader(ObEvalCtx &eval_ctx,
               ObExpr *file_col_expr,
               ObCollectionTypeBase *arr_type,
               parquet::ColumnReader *reader,
               parquet::internal::RecordReader *record_reader,
               common::ObIArrayWrap<int16_t> &def_levels_buf,
               common::ObIArrayWrap<int16_t> &rep_levels_buf,
               common::ObIAllocator &str_res_mem,
               const int64_t batch_size,
               const int64_t row_offset,
               int64_t &row_count,
               int64_t cur_row_group_row_cnt,
               ObColumnDefaultValue &col_def,
               int64_t &read_progress,
               const bool cross_page,
               ParquetStatInfo &stat):
      eval_ctx_(eval_ctx),
      file_col_expr_(file_col_expr),
      arr_type_(arr_type),
      reader_(reader),
      record_reader_(record_reader),
      batch_size_(batch_size),
      row_offset_(row_offset),
      row_count_(row_count),
      def_levels_buf_(def_levels_buf),
      rep_levels_buf_(rep_levels_buf),
      str_res_mem_(str_res_mem),
      cur_row_group_row_cnt_(cur_row_group_row_cnt),
      col_def_(col_def),
      read_progress_(read_progress),
      cross_page_(cross_page),
      stat_(stat)
    {}
    typedef int (DataLoader::*LOAD_FUNC)();
    static LOAD_FUNC select_load_function(const ObDatumMeta &datum_type,
                                          const parquet::ColumnDescriptor *col_desc,
                                          const ::parquet::schema::Node* node,
                                          const uint16_t type_id);
    static bool check_array_column_schema(const ::parquet::schema::Node* node);
    int16_t get_max_def_level();
    int load_data_for_col(LOAD_FUNC &func);

    int load_int64_to_int64_vec();
    int load_int32_to_int64_vec();
    int load_uint32_to_int64_vec();
    int load_int32_to_int32_vec();
    int load_bool_to_int64_vec();
    int load_date_to_mysql_date();
    int load_string_col();
    int load_default();
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
    int load_float_to_double();
    int get_offset_nulls_from_levels(const ObArrayWrap<int16_t> &def_levels_buf,
                                     const ObArrayWrap<int16_t> &rep_levels_buf,
                                     const int64_t parent_def_level, const int64_t max_rep_level,
                                     const int64_t max_def_level, const int64_t levels_count,
                                     uint32_t *offsets, uint8_t *nulls);
    template <typename T>
    int set_attr_vector_payload(const uint32_t attrs_idx,
                                common::ObIArrayWrap<uint32_t> &parent_offsets, uint8_t *nulls,
                                T *values_buf);
    int set_data_attr_vector_payload_for_varchar_array(const uint32_t attrs_idx,
                                                   common::ObIArrayWrap<uint32_t> &parent_offsets,
                                                   uint8_t *nulls, uint32_t *offsets,
                                                   const ObLength &max_accuracy_len);

    template <typename SrcType, typename DstType>
    int decode_list_to_array(const ObArrayWrap<int16_t> &def_levels_buf,
                             const ObArrayWrap<int16_t> &rep_levels_buf,
                             const ::parquet::schema::Node *&node,
                             ObArrayWrap<uint32_t> &parent_offsets, ObExpr **attrs,
                             uint32_t &attrs_idx, int64_t &parent_def_level, int64_t &max_rep_level,
                             int64_t &max_def_level, const ObLength &max_accuracy_len);
    template<typename SrcType, typename DstType>
    int load_list_to_array_dispatch_type(const ObLength &max_accuracy_len);
    int load_list_to_array();
    int to_numeric(const int64_t idx, const int64_t int_value);
    int to_numeric(const int64_t idx, const char *str, const int32_t length);
    int to_numeric_hive(const int64_t idx, const char *str, const int32_t length, char *buf, const int64_t data_len);
    static int64_t calc_tz_adjust_us(const parquet::LogicalType *logtype, const ObDatumMeta &meta, ObSQLSessionInfo *session);
    bool check_char_len(const char *ptr, int32_t len);

    static bool is_ob_type_store_utc(const ObDatumMeta &meta);

    ObEvalCtx &eval_ctx_;
    ObExpr *file_col_expr_;
    ObCollectionTypeBase *arr_type_;
    parquet::ColumnReader *reader_;
    parquet::internal::RecordReader *record_reader_;
    const int64_t batch_size_;
    const int64_t row_offset_;
    int64_t &row_count_;
    common::ObIArrayWrap<int16_t> &def_levels_buf_;
    common::ObIArrayWrap<int16_t> &rep_levels_buf_;
    common::ObIAllocator &str_res_mem_;
    int64_t cur_row_group_row_cnt_;
    ObColumnDefaultValue &col_def_;
    int64_t &read_progress_;
    bool cross_page_;
    ParquetStatInfo &stat_;
  };
  class ParquetSectorIterator {
    public:
      ParquetSectorIterator(const int64_t sector_size, ObParquetTableRowIterator *iter, ObIAllocator &alloc)
        :  iter_(iter), capacity_(sector_size),
            size_(0), idx_(0), bitmap_(alloc),
            alloc_(alloc), skip_ranges_(), read_ranges_(),
            segment_count_(0), cross_pages_(alloc) {}
      int prepare_next(const int64_t group_remain_count, const int64_t batch_size,
                       ObPushdownFilterExecutor *root_filter, ObEvalCtx &eval_ctx);
      void rewind(const int64_t capacity);
      void reset();
      int fill_ranges(const int64_t batch_size, const bool has_no_skip_bits);
      int fill_ranges_one();
      int fill_eager_ranges(const ObBitVector &rg_bitmap,
                            const int64_t max_batch_size,
                            const int64_t start_idx,
                            const int64_t capacity,
                            const bool has_no_skip_bits);
      ObIArray<int64_t> &get_skip_ranges() { return skip_ranges_; }
      ObIArray<int64_t> &get_read_ranges() { return read_ranges_; }
      bool is_end() const { return 0 == size_ || idx_ >= size_; }
      bool is_empty() const { return 0 == size_; }
      void check_cross_pages(const int64_t capacity);
      bool is_cross_page(const int64_t column_id) { return cross_pages_.at(column_id); }
      // Check if a batch will cross page boundary
      // Returns true if the batch crosses page, false otherwise
      bool check_if_batch_cross_page(const int64_t column_id,
        const int64_t current_row_pos,
        const int64_t batch_size);

    private:
      bool has_no_skip_bits();

    private:
      ObParquetTableRowIterator *iter_;
      int64_t capacity_;
      int64_t size_;
      int64_t idx_;
      ObBitmap bitmap_;
      ObIAllocator &alloc_;
      ObArray<int64_t> skip_ranges_;
      ObArray<int64_t> read_ranges_;
      int64_t segment_count_;
      common::ObFixedArray<bool, common::ObIAllocator> cross_pages_;
      common::ObArrayWrap<std::shared_ptr<parquet::OffsetIndex>> offset_indexs_;
  };
  friend class ObParquetTableRowIterator::ParquetSectorIterator;
private:
  int next_file();
  int next_row_group();
  int next_sector(const int64_t capacity, ObEvalCtx &eval_ctx, int64_t &read_count);
  int calc_pseudo_exprs(const int64_t read_count);
  ObExternalTableAccessOptions& make_external_table_access_options(stmt::StmtType stmt_type);
  static int convert_timestamp_datum(const ObDatumMeta &datum_type, int64_t adjusted_min_value,
                              int64_t adjusted_max_value, blocksstable::ObStorageDatum &min_datum,
                              blocksstable::ObStorageDatum &max_datum);
  static int to_numeric_hive(const char *str, const int32_t length, char *buf, const int64_t data_len,
                             const ObDatumMeta &meta, common::ObIAllocator &alloc, blocksstable::ObStorageDatum &datum);
  int pre_buffer(std::shared_ptr<parquet::RowGroupReader> rg_reader);
  int compute_column_id_by_index_type(int index, int &file_col_id, const bool is_collection_column);
  int pre_buffer(ObFilePreBuffer &file_prebuffer,
                 ObFilePreBuffer::ColumnRangeSlicesList &column_range_slice_list);
  int create_file_reader(const ObString& data_path,
                        const ObString file_content_digest,
                        const int64_t file_size,
                        const int64_t modify_time,
                        ObExternalFileAccess& file_access_driver,
                        ObFilePreBuffer& file_prebuffer,
                        std::unique_ptr<parquet::ParquetFileReader>& file_reader,
                        std::unique_ptr<parquet::ParquetFileReader>& eager_file_reader);
  void reset_column_readers();
  int project_eager_columns(int64_t &count, int64_t capacity);
  int calc_filters(const int64_t count,
                   ObPushdownFilterExecutor *curr_filter,
                   ObPushdownFilterExecutor *parent_filter);
  int project_lazy_columns(int64_t &read_count, int64_t capacity);
  void load_lazy_read_row_count();
  int64_t SkipRowsInColumn(const int64_t column_id, const int64_t num_rows_to_skip,
                           const int64_t logical_idx, const bool is_lazy,
                           int64_t &curr_idx, parquet::ColumnReader* reader,
                           parquet::internal::RecordReader* record_reader,
                           bool is_collection_column);
  void clear_eager_flags(ObEvalCtx &eval_ctx);
  void move_next();
  void increase_read_rows(const int64_t rows, const bool only_eager);
  int prepare_page_ranges(const int64_t num_rows);
  int64_t get_real_skip_count(const int64_t curr_idx,
                              const int64_t num_rows_to_skip,
                              const int64_t column_id);
  int reorder_output(const oceanbase::common::ObBitmap &bitmap, ObEvalCtx &ctx, int64_t &read_count);
  int prepare_rg_bitmap(std::shared_ptr<parquet::RowGroupReader> rg_reader);
  int prepare_page_index(const int64_t cur_row_group,
                         std::shared_ptr<parquet::RowGroupReader> rg_reader,
                         std::shared_ptr<parquet::RowGroupReader> eager_rg_reader);
  void dynamic_switch_calc_mode();
  int calc_column_convert(const int64_t read_count, const bool is_eager, ObEvalCtx &eval_ctx);
  int calc_file_meta_column(const int64_t read_count, ObEvalCtx &eval_ctx);
  static bool is_contain_field_id(std::shared_ptr<parquet::FileMetaData> file_meta);
private:
  ObParquetIteratorState state_;
  lib::ObMemAttr mem_attr_;
  ObArenaAllocator allocator_;
  ObArenaAllocator str_res_mem_;
  ObArrowMemPool arrow_alloc_;
  parquet::ReaderProperties read_props_;
  ObExternalFileAccess data_access_driver_;
  ObExternalFileAccess eager_data_access_driver_;
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::unique_ptr<parquet::ParquetFileReader> eager_file_reader_;
  std::shared_ptr<parquet::FileMetaData> file_meta_;
  common::ObArrayWrap<int> column_indexs_;
  common::ObArrayWrap<std::shared_ptr<parquet::ColumnReader>> column_readers_;
  common::ObArrayWrap<std::shared_ptr<parquet::internal::RecordReader>> record_readers_;
  common::ObArrayWrap<std::shared_ptr<parquet::ColumnReader>> eager_column_readers_;
  common::ObArrayWrap<std::shared_ptr<parquet::internal::RecordReader>> eager_record_readers_;
  common::ObArrayWrap<DataLoader::LOAD_FUNC> load_funcs_;
  ObSqlString url_;
  ObBitVector *bit_vector_cache_;
  common::ObArrayWrap<int16_t> def_levels_buf_;
  common::ObArrayWrap<int16_t> rep_levels_buf_;
  common::ObArrayWrap<char *> file_url_ptrs_; //for file url expr
  common::ObArrayWrap<ObLength> file_url_lens_; //for file url expr
  ObExternalTableAccessOptions options_;
  ObFilePreBuffer file_prebuffer_;
  ObFilePreBuffer eager_file_prebuffer_;
  common::ObFixedArray<ObFilePreBuffer::ColumnRangeSlices *, ObIAllocator> column_range_slices_;
  int64_t cur_col_id_;
  int64_t cur_eager_id_;
  ObBitVector *rg_bitmap_;
  common::ObFIFOAllocator malloc_allocator_;
  ParquetSectorIterator sector_iter_;
  std::shared_ptr<parquet::PageIndexReader> page_index_reader_;
  std::shared_ptr<parquet::RowGroupPageIndexReader> rg_page_index_reader_;
  common::ObFixedArray<ObArray<std::pair<int64_t, int64_t>> *, ObIAllocator> page_skip_ranges_;
  ParquetStatInfo stat_;
  FilterCalcMode mode_;
  ObLakeTableParquetReaderMetrics reader_metrics_;
  sql::ColumnIndexType column_index_type_;
  bool is_col_name_case_sensitive_;
};

}
}

#endif // OB_PARQUET_TABLE_ROW_ITER_H
