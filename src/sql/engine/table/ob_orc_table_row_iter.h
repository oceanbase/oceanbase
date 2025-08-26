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

      void read(void* buf,
                uint64_t length,
                uint64_t offset) override  {
        int ret = OB_SUCCESS;
        int64_t bytesRead = 0;
        bool is_hit_cache = false;
        const int64_t io_timeout_ms = (timeout_ts_ - ObTimeUtility::current_time()) / 1000;
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
      int build(const int64_t ext_tbl_col_id, const ObExpr *expr,
                blocksstable::ObMinMaxFilterParam &param) override;
    private:
      ObOrcTableRowIterator *orc_row_iter_;
      const orc::Statistics *orc_stat_;
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
        start_row_group_idx = 0;
        end_row_group_idx = 0;
        num_rows = 0;
        first_row_id = 0;
      }

      bool is_empty() const { return num_rows <= 0; }

      /// \brief The start row group index of range in the stripe.
      int64_t start_row_group_idx;
      /// \brief The end row group index of range in the stripe.
      int64_t end_row_group_idx;
      /// \brief Number of rows in the range
      int64_t num_rows;
      /// \brief Index of first row of the stripe
      int64_t first_row_id;
      TO_STRING_KV(K(start_row_group_idx), K(end_row_group_idx), K(num_rows), K(first_row_id));
    };

    struct ObOrcReaderMetrics
    {
      ObOrcReaderMetrics() { reset(); }
      void reset()
      {
        selected_file_count = 0;
        skipped_file_count = 0;
        selected_stripe_count = 0;
        skipped_stripe_count = 0;
        selected_row_group_count = 0;
        skipped_row_group_count = 0;
        read_rows_count = 0;
      }
      TO_STRING_KV(K(selected_file_count), K(skipped_file_count), K(selected_stripe_count),
              K(skipped_stripe_count), K(selected_row_group_count), K(skipped_row_group_count),
              K(read_rows_count));
      int64_t selected_file_count;
      int64_t skipped_file_count;
      int64_t selected_stripe_count;
      int64_t skipped_stripe_count;
      int64_t selected_row_group_count;
      int64_t skipped_row_group_count;
      int64_t read_rows_count;
    };

  public:
    ObOrcTableRowIterator() :
      file_column_exprs_(allocator_), mapping_column_exprs_(allocator_), mapping_column_ids_(allocator_),
      file_meta_column_exprs_(allocator_), bit_vector_cache_(NULL),
      options_(), file_prebuffer_(data_access_driver_), reader_metrics_() {}
    virtual ~ObOrcTableRowIterator()
    {
      file_prebuffer_.destroy();
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
    DataLoader(ObEvalCtx &eval_ctx,
               ObExpr *file_col_expr,
               std::unique_ptr<orc::ColumnVectorBatch> &batch,
               const int64_t batch_size,
               const ObIArray<int> &idxs,
               int64_t &row_count,
               const orc::Type *col_type):
      eval_ctx_(eval_ctx),
      file_col_expr_(file_col_expr),
      batch_(batch),
      batch_size_(batch_size),
      idxs_(idxs),
      row_count_(row_count),
      col_type_(col_type)
    {}
    typedef int (DataLoader::*LOAD_FUNC)();
    static LOAD_FUNC select_load_function(const ObDatumMeta &datum_type,
                                          const orc::Type &type);
    int load_data_for_col(LOAD_FUNC &func);
    int load_string_col();
    int load_year_vec();
    int load_int32_vec();
    int load_int64_vec();
    int load_timestamp_vec();
    int load_date_to_time_or_stamp();
    int load_float();
    int load_double();
    int load_dec128_vec();
    int load_dec64_vec();
    int load_int64_to_number_vec();
    int to_numeric(const int64_t idx, const int64_t int_value);

    static bool is_orc_read_utc(const orc::Type *type);
    static bool is_ob_type_store_utc(const ObObjType &type);
    static int64_t calc_tz_adjust_us(const orc::Type *orc_type, const ObObjType ob_type,
                                     const ObSQLSessionInfo *session);
    int64_t calc_tz_adjust_us();
    ObEvalCtx &eval_ctx_;
    ObExpr *file_col_expr_;
    std::unique_ptr<orc::ColumnVectorBatch> &batch_;
    const int64_t batch_size_;
    const ObIArray<int> &idxs_;
    int64_t &row_count_;
    const orc::Type *col_type_;
  };
  private:
    int next_file();
    int next_stripe();
    int next_row_range();
    int build_type_name_id_map(const orc::Type* type, ObIArray<ObString> &col_names);
    int prepare_read_orc_file();
    int compute_column_id_by_index_type(int64_t index, int64_t &orc_col_id);
    int to_dot_column_path(ObIArray<ObString> &col_names, ObString &path);
    int get_data_column_batch_idxs(const orc::Type *type, const int col_id, ObIArray<int> &idxs);
    ObExternalTableAccessOptions& make_external_table_access_options(stmt::StmtType stmt_type);
    int filter_file(const int64_t task_idx);
     // the select row ranges stored in `row_ranges_`
    int select_row_ranges(const int64_t stripe_idx);
    int select_row_ranges_by_pushdown_filter(const orc::StripeInformation &stripe,
                                             const int64_t stripe_idx,
                                             const int64_t stripe_first_row_id,
                                             const int64_t stripe_num_rows,
                                             bool &build_whole_stripe_range);
    int ensure_row_range_array(const int64_t size);
    int init_column_range_slices();
    int filter_by_statistic(const PushdownLevel filter_level,
                            const orc::Statistics *orc_stat,
                            bool &skipped);
    int pre_buffer_row_index(const orc::StripeInformation &stripe);
    int pre_buffer_data(const orc::StripeInformation &stripe);
    int convert_orc_statistics(const orc::ColumnStatistics* orc_stat,
                               const orc::Type *orc_type,
                               const ObDatumMeta &col_meta,
                               const bool has_lob_header,
                               blocksstable::ObMinMaxFilterParam &param);

  private:

    ObOrcIteratorState state_;
    lib::ObMemAttr mem_attr_;
    ObArenaAllocator allocator_;
    ObOrcMemPool orc_alloc_;
    std::unique_ptr<orc::Reader> reader_;
    std::unique_ptr<orc::RowReader> row_reader_;
    std::unique_ptr<orc::ColumnVectorBatch> orc_batch_;
    ObExternalFileAccess data_access_driver_;
    common::ObArrayWrap<int> column_indexs_; //for getting statistics
    common::ObArrayWrap<SelectedRowRange> row_ranges_;
    ExprFixedArray file_column_exprs_; //column value from orc file
    ExprFixedArray mapping_column_exprs_;
    ObFixedArray<uint64_t, ObIAllocator> mapping_column_ids_;
    ExprFixedArray file_meta_column_exprs_; //column value from file meta
    common::ObArrayWrap<DataLoader::LOAD_FUNC> load_funcs_;
    ObSqlString url_;
    ObBitVector *bit_vector_cache_;
    common::ObArrayWrap<char *> file_url_ptrs_; //for file url expr
    common::ObArrayWrap<ObLength> file_url_lens_; //for file url expr
    hash::ObHashMap<int64_t, const orc::Type*, common::hash::NoPthreadDefendMode> id_to_type_;
    hash::ObHashMap<ObString, int64_t, common::hash::NoPthreadDefendMode> name_to_id_;
    ObExternalTableAccessOptions options_;
    ObFilePreBuffer file_prebuffer_;
    ObOrcReaderMetrics reader_metrics_; // record reader info in the filter-down scenario
    common::ObArenaAllocator temp_allocator_; // used for lob filter pushdown
    common::ObArrayWrap<ObFilePreBuffer::ColumnRangeSlices *> column_range_slices_;
};

}
}

#endif