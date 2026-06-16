/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_PARQUET_MIN_MAX_ITER_H
#define OB_PARQUET_MIN_MAX_ITER_H

#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_parquet_table_row_iter.h"
#include "sql/engine/basic/ob_arrow_basic.h"

namespace oceanbase {
namespace sql {

class ObParquetMinMaxIter : public ObExternalTableRowIterator {
public:
  ObParquetMinMaxIter()
    : read_props_(&arrow_alloc_),
      file_prebuffer_(data_access_driver_),
      column_index_type_(sql::ColumnIndexType::NAME),
      is_col_name_case_sensitive_(false)
  {}
  virtual ~ObParquetMinMaxIter();

  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_rows(int64_t &count, int64_t capacity) override;
  int get_next_row() override { return common::OB_NOT_SUPPORTED; }
  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }
  virtual void reset() override;

private:
  int next_file();
  int create_file_reader(const ObString &data_path,
                         const ObString file_content_digest,
                         const int64_t file_size,
                         const int64_t modify_time);
  int compute_column_id_by_index_type(int index, int &file_col_id);

  int collect_partition_aggregate();
  int collect_aggregate_from_files();
  int collect_file_meta_statistics();
  int resolve_agg_parquet_column(ObExpr *agg_expr, const int64_t agg_idx,
                                 int &parquet_col_idx, ObColumnMeta &column_meta);
  int merge_rowgroup_statistics(ObExpr *agg_expr, const int64_t agg_idx,
                                const int64_t rg_idx, const int parquet_col_idx,
                                const ObColumnMeta &column_meta, ObEvalCtx &eval_ctx);
  int merge_agg_datum(ObExpr *agg_expr, const int64_t agg_idx,
                      const blocksstable::ObStorageDatum &rg_min_datum,
                      const blocksstable::ObStorageDatum &rg_max_datum);
  int output_aggregate_result(int64_t &count);
  int calc_file_meta_column(const int64_t read_count, ObEvalCtx &eval_ctx);
  int calc_column_convert(const int64_t read_count, ObEvalCtx &eval_ctx);

private:
  ObParquetIteratorState state_;
  lib::ObMemAttr mem_attr_;
  ObArenaAllocator allocator_;
  ObArrowMemPool arrow_alloc_;
  parquet::ReaderProperties read_props_;
  ObExternalFileAccess data_access_driver_;
  ObFilePreBuffer file_prebuffer_;
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::shared_ptr<parquet::FileMetaData> file_meta_;
  ObExternalTableAccessOptions options_;
  ObSqlString url_;
  sql::ColumnIndexType column_index_type_;
  bool is_col_name_case_sensitive_;
  common::ObSEArray<blocksstable::ObStorageDatum, 4> agg_res_datums_;
  common::ObArrayWrap<char *> file_url_ptrs_;
  common::ObArrayWrap<ObLength> file_url_lens_;
  ObBitVector *bit_vector_cache_ = nullptr;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_PARQUET_MIN_MAX_ITER_H
