/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ORC_MIN_MAX_ITER_H
#define OB_ORC_MIN_MAX_ITER_H

#include <orc/OrcFile.hh>
#include <orc/MemoryPool.hh>
#include <orc/Reader.hh>

#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/engine/table/ob_external_file_access.h"
#include "sql/engine/basic/ob_arrow_basic.h"

namespace oceanbase {
namespace sql {

class ObOrcMinMaxIter : public ObExternalTableRowIterator {
public:
  ObOrcMinMaxIter()
    : mem_attr_(),
      allocator_(),
      orc_alloc_(),
      file_prebuffer_(data_access_driver_),
      column_index_type_(sql::ColumnIndexType::NAME),
      is_col_name_case_sensitive_(false)
  {}
  virtual ~ObOrcMinMaxIter();

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
  int create_file_reader(const ObString &data_path, const int64_t file_size);
  int compute_orc_column_id(ObExpr *file_col_expr, int64_t &orc_col_id);
  int build_type_name_id_map(const orc::Type *type, common::ObIArray<ObString> &col_names);
  int build_iceberg_id_to_type_map(const orc::Type *type);
  int to_dot_column_path(common::ObIArray<ObString> &col_names, ObString &path);

  int collect_partition_aggregate();
  int collect_aggregate_from_files();
  int collect_file_meta_statistics();
  int merge_file_statistics(ObExpr *agg_expr, const int64_t agg_idx,
                            const int64_t orc_col_id, const ObColumnMeta &column_meta,
                            ObEvalCtx &eval_ctx);
  int read_orc_min_max_datum(const orc::ColumnStatistics *col_stat,
                             const orc::Type *orc_type,
                             const ObColumnMeta &column_meta,
                             blocksstable::ObStorageDatum &min_datum,
                             blocksstable::ObStorageDatum &max_datum);
  int merge_agg_datum(ObExpr *agg_expr, const int64_t agg_idx,
                      const blocksstable::ObStorageDatum &stripe_min_datum,
                      const blocksstable::ObStorageDatum &stripe_max_datum);
  int convert_orc_statistics(const orc::ColumnStatistics *orc_stat,
                             const orc::Type *orc_type,
                             const ObColumnMeta &col_meta,
                             blocksstable::ObMinMaxFilterParam &param);
  int output_aggregate_result(int64_t &count);
  int calc_file_meta_column(const int64_t read_count, ObEvalCtx &eval_ctx);
  int calc_column_convert(const int64_t read_count, ObEvalCtx &eval_ctx);
  static int64_t calc_tz_adjust_us(const orc::Type *orc_type, const ObObjType ob_type,
                                   const ObSQLSessionInfo *session);

private:
  ObExternalIteratorState state_;
  lib::ObMemAttr mem_attr_;
  ObArenaAllocator allocator_;
  ObOrcMemPool orc_alloc_;
  common::ObArenaAllocator temp_allocator_;
  std::unique_ptr<orc::Reader> reader_;
  hash::ObHashMap<int64_t, const orc::Type *, common::hash::NoPthreadDefendMode> iceberg_id_to_type_;
  hash::ObHashMap<ObString, int64_t, common::hash::NoPthreadDefendMode> name_to_id_;
  ObExternalFileAccess data_access_driver_;
  ObFilePreBuffer file_prebuffer_;
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

#endif  // OB_ORC_MIN_MAX_ITER_H
