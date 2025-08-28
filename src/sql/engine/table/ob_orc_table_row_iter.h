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

#ifndef OB_ORC_TABLE_ROW_ITER_H
#define OB_ORC_TABLE_ROW_ITER_H

#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/engine/table/ob_external_file_access.h"
#include <orc/OrcFile.hh>
#include <orc/MemoryPool.hh>
#include <orc/Writer.hh>
#include <orc/Reader.hh>
#include <orc/Int128.hh>
#include <orc/Common.hh>
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/table/ob_file_prebuffer.h"
#include "storage/access/ob_sstable_index_filter.h"

namespace oceanbase {
namespace sql {
  class ObOrcFileAccess : public orc::InputStream {
    public:
      ObOrcFileAccess(ObExternalFileAccess &file_reader, const char *file_name, int64_t len) :
        file_reader_(file_reader), file_name_(file_name), total_length_(len), fileName_(file_name),
        timeout_ts_(INT64_MAX), file_prebuffer_(nullptr)
      {}

      uint64_t getLength() const override {
        return total_length_;
      }

      uint64_t getNaturalReadSize() const override {
        return 128 * 1024; // 128KB
      }

      void read(void* buf, uint64_t length, uint64_t offset) override {
        int ret = OB_SUCCESS;
        int64_t bytesRead = 0;
        bool is_hit_cache = false;
        const int64_t io_timeout_ms = MAX(0, (timeout_ts_ - ObTimeUtility::current_time()) / 1000);
        ObExternalReadInfo read_info(offset, buf, length, io_timeout_ms);
        if (OB_FAIL(read_from_cache(offset, length, buf, is_hit_cache))) {
          SERVER_LOG(WARN, "failed to read from cache", K(ret));
        } else if (!is_hit_cache && OB_FAIL(file_reader_.pread(read_info, bytesRead))) {
          SERVER_LOG(WARN, "fail to read file", K(ret), K(length));
        }
        if (OB_FAIL(ret)) {
          throw ObErrorCodeException(ret);
        }
        SERVER_LOG(TRACE, "read file access", K(file_name_), K(bytesRead), K(offset), K(length));
      }

      const std::string& getName() const override {
        return fileName_;
      }
      void set_timeout_timestamp(const int64_t timeout_ts)
      {
        timeout_ts_ = timeout_ts;
      }
      void set_file_prebuffer(ObFilePreBuffer *file_prebuffer)
      {
        if (nullptr != file_prebuffer) {
          file_prebuffer->reset();
        }
        file_prebuffer_ = file_prebuffer;
      }

      int read_from_cache(int64_t position, int64_t nbytes, void *out, bool &is_hit)
      {
        int ret = OB_SUCCESS;
        is_hit = false;
        if (nullptr == file_prebuffer_) {
        } else if (OB_FAIL(file_prebuffer_->read(position, nbytes, out))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            SERVER_LOG(WARN, "failed to read from prebuffer", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          is_hit = true;
        }
        return ret;
      }

    private:
      ObExternalFileAccess &file_reader_;
      const char* file_name_;
      uint64_t total_length_;
      std::string fileName_;
      int64_t timeout_ts_;
      ObFilePreBuffer *file_prebuffer_;
  };

  class ObOrcIteratorState : public ObExternalIteratorState {
  public:
    ObOrcIteratorState() { reset_cur_file_state(); }

    virtual void reuse() override
    {
      ObExternalIteratorState::reuse();
      reset_cur_file_state();
    }
    void reset_cur_file_state()
    {
      cur_stripe_idx_ = 0;
      end_stripe_idx_ = -1;
      next_stripe_first_row_id_ = 0;
      cur_row_range_idx_ = 0;
      end_row_range_idx_ = -1;
      cur_range_read_row_count_ = 0;
      cur_range_row_count_ = 0;
      orc_reader_cur_row_id_ = 0;
    }
    OB_INLINE bool has_stripe() const
    {
      return cur_stripe_idx_ <= end_stripe_idx_;
    }
    OB_INLINE bool next_stripe()
    {
      ++cur_stripe_idx_;
      cur_row_range_idx_ = 0;
      end_row_range_idx_ = -1;
      return has_stripe();
    }
    OB_INLINE bool has_row_range() const
    {
      return cur_row_range_idx_ <= end_row_range_idx_;
    }
    OB_INLINE bool next_row_range()
    {
      ++cur_row_range_idx_;
      orc_reader_cur_row_id_ += cur_range_read_row_count_;
      cur_range_read_row_count_ = 0;
      cur_range_row_count_ = 0;
      return has_row_range();
    }
    OB_INLINE bool has_rows_in_row_range() const
    {
      return cur_range_read_row_count_ < cur_range_row_count_;
    }
    OB_INLINE int64_t remain_rows_in_range() const
    {
      return cur_range_row_count_ - cur_range_read_row_count_;
    }

    DECLARE_VIRTUAL_TO_STRING;
    int64_t cur_stripe_idx_;
    int64_t end_stripe_idx_;
    int64_t next_stripe_first_row_id_;
    int64_t cur_row_range_idx_;
    int64_t end_row_range_idx_;
    int64_t cur_range_read_row_count_;
    int64_t cur_range_row_count_;
    int64_t orc_reader_cur_row_id_;
  };

  class ObOrcTableRowIterator :
    public ObExternalTableRowIterator, public ObExternalTablePushdownFilter {
  private:
    static const PushdownLevel STRIPE_LEVEL = PushdownLevel::ROW_GROUP;
    static const PushdownLevel ROW_INDEX_LEVEL = PushdownLevel::PAGE;

  private:
    class OrcMinMaxFilterParamBuilder : public MinMaxFilterParamBuilder
    {
    public:
      explicit OrcMinMaxFilterParamBuilder(ObOrcTableRowIterator *orc_row_iter,
                                           const orc::Statistics *orc_stat)
            : orc_row_iter_(orc_row_iter), orc_stat_(orc_stat) {}
      virtual ~OrcMinMaxFilterParamBuilder() {}
      int build(const int32_t ext_tbl_col_id, const ObColumnMeta &column_meta,
                blocksstable::ObMinMaxFilterParam &param) override;
      int next_range(const int64_t column_id, int64_t &offset, int64_t &rows) override
      {
        return OB_NOT_SUPPORTED;
      }
    private:
      ObOrcTableRowIterator *orc_row_iter_;
      const orc::Statistics *orc_stat_;
    };

    class OrcFilterColumnLoader : public ObFilterColumnLoader {
    public:
      OrcFilterColumnLoader(ObOrcTableRowIterator *orc_row_iter) : orc_row_iter_(orc_row_iter) {}
      virtual ~OrcFilterColumnLoader() {}
      int load(const common::ObIArray<uint64_t> &col_ids) override;
    private:
      ObOrcTableRowIterator *orc_row_iter_;
    };

    class RowIndexStatisticsWrapper : public orc::Statistics
    {
    public:
      RowIndexStatisticsWrapper(const orc::StripeStatistics &stripe_statistics) :
        row_index_(0), stripe_statistics_(stripe_statistics) {}

      virtual ~RowIndexStatisticsWrapper() {}
      OB_INLINE void set_row_index(const int64_t row_index) { row_index_ = row_index; }
      OB_INLINE const orc::ColumnStatistics* getColumnStatistics(uint32_t column_id) const override
      {
        return stripe_statistics_.getRowIndexStatistics(column_id, row_index_);
      }
      OB_INLINE uint32_t getNumberOfColumns() const override
      { return stripe_statistics_.getNumberOfColumns(); }

    private:
      int64_t row_index_;
      const orc::StripeStatistics &stripe_statistics_;
    };

    struct SelectedRowRange {
      SelectedRowRange() { reset(); }

      void reset()
      {
        num_rows = 0;
        first_row_id = 0;
      }

      bool is_empty() const { return num_rows <= 0; }

      /// \brief Number of rows in the range
      int64_t num_rows;
      /// \brief Index of first row of the stripe
      int64_t first_row_id;
      TO_STRING_KV(K(num_rows), K(first_row_id));
    };

  public:
    ObOrcTableRowIterator() :
      query_flag_(0), inner_sector_reader_(nullptr), sector_reader_(nullptr), bit_vector_cache_(NULL),
      options_(), file_prebuffer_(data_access_driver_), reader_metrics_(),
      file_contains_attribute_key_(false)
    {}
    virtual ~ObOrcTableRowIterator()
    {
      file_prebuffer_.destroy();
      reader_profile_.dump_metrics();
      if (nullptr != inner_sector_reader_) {
        inner_sector_reader_->~SectorReader();
        inner_sector_reader_ = nullptr;
      }
    }

    int init(const storage::ObTableScanParam *scan_param) override;
    virtual int get_next_row(ObNewRow *&row) override
    {
      UNUSED(row);
      return common::OB_ERR_UNEXPECTED;
    }

    int get_next_row() override;
    int get_next_rows(int64_t &count, int64_t capacity) override;
    virtual void reset() override;
private:
  // load vec data from orc file to expr mem
  struct DataLoader {
    DataLoader() { reset(); }
    ~DataLoader() { reset(); }
    int init(ObExpr *file_col_expr, const orc::ColumnVectorBatch *batch, const orc::Type *col_type);
    int init(ObExpr *file_col_expr, const ObColumnDefaultValue *col_def);
    void reset()
    {
      file_col_expr_ = nullptr;
      batch_ = nullptr;
      col_type_ = nullptr;
      load_func_ = nullptr;
      col_def_ = nullptr;
    }

    bool has_load_func() const { return load_func_ != nullptr; }

    int load_data_for_col(ObEvalCtx &eval_ctx);

    static int64_t calc_tz_adjust_us(const orc::Type *orc_type, const ObObjType ob_type,
                                     const ObSQLSessionInfo *session);

   private:
    typedef int (DataLoader::*LOAD_FUNC)(ObEvalCtx &eval_ctx);
    static LOAD_FUNC select_load_function(const ObDatumMeta &datum_type,
                                          const orc::Type &type);
    int load_string_col(ObEvalCtx &eval_ctx);
    int load_year_vec(ObEvalCtx &eval_ctx);
    int load_int32_vec(ObEvalCtx &eval_ctx);
    int load_int64_vec(ObEvalCtx &eval_ctx);
    int load_timestamp_vec(ObEvalCtx &eval_ctx);
    int load_date_to_time_or_stamp(ObEvalCtx &eval_ctx);
    int load_float(ObEvalCtx &eval_ctx);
    int load_double(ObEvalCtx &eval_ctx);
    int load_dec128_vec(ObEvalCtx &eval_ctx);
    int load_dec64_vec(ObEvalCtx &eval_ctx);
    int load_dec64_to_dec128_vec(ObEvalCtx &eval_ctx);
    int load_int64_to_number_vec(ObEvalCtx &eval_ctx);
    int load_default(ObEvalCtx &eval_ctx);
    static bool is_orc_read_utc(const orc::Type *type);
    static bool is_ob_type_store_utc(const ObObjType &type);
    int64_t calc_tz_adjust_us(ObEvalCtx &eval_ctx);
    ObExpr *file_col_expr_;
    const orc::ColumnVectorBatch *batch_;
    const orc::Type *col_type_;
    const ObColumnDefaultValue *col_def_;
    LOAD_FUNC load_func_;
  };

  // wrapper struct of orc row reader and batch
  struct OrcRowReader {
    OrcRowReader() : row_reader_(nullptr), orc_batch_(nullptr), data_loaders_(), row_id_(0) {}

    ~OrcRowReader() {
      row_reader_.reset();
      orc_batch_.reset();
      data_loaders_.reset();
    }

    template<typename T>
    void init(int64_t capacity, const std::list<T>& include_columns, orc::Reader *reader);
    void init_for_hive_table(int64_t capacity,
                            const std::list<uint64_t>& include_columns,
                            orc::Reader *reader);

    OB_INLINE bool next_batch(const int64_t capacity)
    {
      // Iceberg table may have no row_reader, just load data by default value
      bool has_rows = true;
      if (row_reader_) {
        orc_batch_->capacity = capacity;
        has_rows = row_reader_->next(*orc_batch_);
      }
      return has_rows;
    }

    std::unique_ptr<orc::RowReader> row_reader_;
    std::unique_ptr<orc::ColumnVectorBatch> orc_batch_;
    common::ObArrayWrap<DataLoader> data_loaders_;
    int64_t row_id_;
  };

  class SectorReader {
    // keep max sector row size same as default orc row index group size to avoid unnecessary skip
    // within row group.
    static const int64_t MAX_SECTOR_ROW_COUNT = 10000;
  public:
    SectorReader(ObIAllocator &allocator) :
      allocator_(allocator), orc_row_iter_(nullptr), eager_reader_(), bitmap_(allocator),
      skip_(nullptr), next_(nullptr), sector_begin_(0), sector_end_(0), sector_size_(0) {}
    virtual ~SectorReader()
    {
      if (skip_ != nullptr) {
        allocator_.free(skip_);
        skip_ = nullptr;
      }
      if (next_ != nullptr) {
        allocator_.free(next_);
        next_ = nullptr;
      }
    }

    int init(ObOrcTableRowIterator *orc_row_iter);

    int next(int64_t &count, int64_t capacity);

    OB_INLINE bool is_finished() const
    {
      return sector_begin_ >= sector_end_;
    }

    OB_INLINE void seek_to_row(const int64_t row_id)
    {
      if (eager_reader_.row_reader_) {
        eager_reader_.row_reader_->seekToRow(row_id);
      }
      eager_reader_.row_id_ = row_id;
    }

    OB_INLINE bool has_eager_reader() const
    {
      return eager_reader_.row_reader_ != nullptr;
    }

    OrcRowReader &get_eager_reader() { return eager_reader_; }

  private:
    OB_INLINE void reset_sector_state()
    {
      sector_begin_ = 0;
      sector_end_ = 0;
      sector_size_ = 0;
    }

    int build_sector_bitmap_by_filter(bool &has_active_row);
    int popcnt_rows_by_filter(bool &has_active_row);
    int next_sector_range(const int64_t capacity, orc::ProjectArgument &arg);
    int merge_bitmap_with_delete_bitmap(ObBitmap *bitmap, const int64_t eval_count,
                                        const int64_t sector_start_row_id);

  private:
    ObIAllocator &allocator_;
    ObOrcTableRowIterator *orc_row_iter_;
    OrcRowReader eager_reader_;
    int64_t max_batch_size_;
    ObBitmap bitmap_;
    uint16_t *skip_;
    uint16_t *next_;
    int64_t sector_begin_;
    int64_t sector_end_;
    int64_t sector_size_;
  };

  private:
    int init_query_flag();
    int next_file();
    int next_stripe();
    int next_row_range();
    int build_type_name_id_map(const orc::Type* type, ObIArray<ObString> &col_names);
    int build_iceberg_id_to_type_map(const orc::Type* type);
    int prepare_read_orc_file();
    int init_data_loader(int64_t i, int64_t orc_col_id, const orc::Type *type,
                        OrcRowReader &reader, ObColumnDefaultValue *default_value);
    int compute_column_id_by_index_type(int64_t index, int64_t &orc_col_id);
    int to_dot_column_path(ObIArray<ObString> &col_names, ObString &path);
    int get_data_column_batch(const orc::Type *type, const orc::StructVectorBatch *root_batch,
                              const int col_id, orc::ColumnVectorBatch *&batch);
    ObExternalTableAccessOptions& make_external_table_access_options(stmt::StmtType stmt_type);
    int create_row_readers();
    int init_selected_columns();
    int filter_file(const int64_t task_idx);
     // the select row ranges stored in `row_ranges_`
    int select_row_ranges(const int64_t stripe_idx);
    int select_row_ranges_by_pushdown_filter(const int64_t stripe_idx,
                                             const int64_t stripe_first_row_id,
                                             const int64_t stripe_num_rows,
                                             bool &build_whole_stripe_range);
    int init_column_range_slices();
    int filter_by_statistic(const PushdownLevel filter_level,
                            const orc::Statistics *orc_stat,
                            bool &skipped);
    int pre_buffer(const bool row_index);
    int convert_orc_statistics(const orc::ColumnStatistics* orc_stat,
                               const orc::Type *orc_type,
                               const ObColumnMeta &column_meta,
                               blocksstable::ObMinMaxFilterParam &param);
    int init_sector_reader();
    int fill_file_meta_column(ObEvalCtx &eval_ctx, ObExpr *meta_expr, const int64_t read_count);
    // project column dependent expr to column expr.
    int project_column(ObEvalCtx &eval_ctx, const ObExpr *from, const ObExpr *to,
                       const int64_t read_count);
    int load_filter_column(const common::ObIArray<uint64_t> &col_ids);
    int next_batch(int64_t &read_count, const int64_t capacity);

    int compute_column_id_by_table_type(int64_t index, int64_t &orc_col_id);
    int create_file_reader(const ObString& data_file_path,
                          ObExternalFileAccess& file_access_driver,
                          ObFilePreBuffer& file_prebuffer,
                          const int64_t file_size,
                          std::unique_ptr<orc::Reader>& delete_reader);
    void clear_filter_expr_evaluated_flag()
    {
      scan_param_->op_->clear_evaluated_flag();
      FOREACH(filter_expr_rel, filter_expr_rels_) {
        filter_expr_rel->second.projected_ = false;
      }
    }
    bool is_contain_attribute_key(const orc::Type *type);
  private:
    ObOrcIteratorState state_;
    lib::ObMemAttr mem_attr_;
    ObArenaAllocator allocator_;
    ObOrcMemPool orc_alloc_;
    union {
      struct {
        uint16_t is_count_aggr_ : 1;
        uint16_t is_count_aggr_with_filter_ : 1;
        uint16_t is_file_meta_filter_ : 1; // if only file meta filter
        uint16_t has_skip_index_filter_ : 1;
        uint16_t has_eager_column_ : 1;
        uint16_t need_pre_buffer_index_ : 1;
        uint16_t reserved_ : 10;
      };
      uint16_t query_flag_;
    };
    std::unique_ptr<orc::Reader> reader_;
    orc::CachedReaderContext reader_ctx_;
    std::unique_ptr<orc::RowReader> all_row_reader_;
    OrcRowReader project_reader_;
    SectorReader *inner_sector_reader_;
    SectorReader *sector_reader_;
    common::ObArrayWrap<bool> selected_columns_;
    ObExternalFileAccess data_access_driver_;
    common::ObArrayWrap<int> column_indexs_; //for getting statistics
    common::ObArrayWrap<SelectedRowRange> row_ranges_;
    ObSqlString url_;
    ObBitVector *bit_vector_cache_;
    common::ObArrayWrap<char *> file_url_ptrs_; //for file url expr
    common::ObArrayWrap<ObLength> file_url_lens_; //for file url expr
    hash::ObHashMap<int64_t, const orc::Type*, common::hash::NoPthreadDefendMode> id_to_type_;
    hash::ObHashMap<ObString, int64_t, common::hash::NoPthreadDefendMode> name_to_id_;
    hash::ObHashMap<int64_t, const orc::Type*, common::hash::NoPthreadDefendMode> iceberg_id_to_type_;
    ObExternalTableAccessOptions options_;
    ObFilePreBuffer file_prebuffer_;
    common::ObArenaAllocator temp_allocator_; // used for lob filter pushdown
    common::ObArrayWrap<ObFilePreBuffer::ColumnRangeSlices *> column_range_slices_;
    ObLakeTableReaderMetrics reader_metrics_;
    bool file_contains_attribute_key_;
};

}
}

#endif
