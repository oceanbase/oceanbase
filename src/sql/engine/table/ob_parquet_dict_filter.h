/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_PARQUET_DICT_FILTER_H_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_PARQUET_DICT_FILTER_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_bitmap.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/vector/ob_vector_define.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr.h"

#include <parquet/api/reader.h>

namespace oceanbase
{
namespace sql
{

// Parquet字典列的数据存储
// NULL 值处理说明：
// - 使用 null_ref 概念，NULL 行的 index 设置为 dict_len_（类似 OB 内表的设计）
// - has_null_ 标志表示该列是否包含 NULL 值
// - 字典值 dict_values_ 不包含 NULL，NULL 用特殊的 ref（dict_len_）表示
struct ObParquetDictColumnData
{
  ObFixedArray<int32_t, ObIAllocator> indices_;
  int64_t indices_count_;
  common::StrDiscVec *dict_values_;
  int32_t dict_len_;
  parquet::Type::type parquet_type_;
  bool has_null_;

  ObParquetDictColumnData(common::ObIAllocator &allocator)
      : indices_(allocator), indices_count_(0), dict_values_(nullptr), dict_len_(0),
        parquet_type_(parquet::Type::BYTE_ARRAY), has_null_(false)
  {
  }

  // 获取 distinct_ref_cnt（包含 NULL ref 在内的所有可能 ref 值数量）
  int32_t get_distinct_ref_cnt() const
  {
    return has_null_ ? (dict_len_ + 1) : dict_len_;
  }

  int32_t get_null_ref() const
  {
    return dict_len_;
  }

  TO_STRING_KV(K_(indices),
               K_(indices_count),
               KP_(dict_values),
               K_(dict_len),
               K_(parquet_type),
               K_(has_null));
};

typedef hash::ObHashMap<int32_t, ObParquetDictColumnData *, common::hash::NoPthreadDefendMode>
    DictColumnsMap;

// Parquet字典优化的管理类
class ObParquetDictFilterPushdown
{
public:
  ObParquetDictFilterPushdown();
  ~ObParquetDictFilterPushdown();

  int init(int64_t column_count);

  int init_decode_exprs(const common::ObIArray<ObExpr *> &decode_exprs);

  void reset();

  int init_row_group_dict_encoding_check(std::shared_ptr<parquet::RowGroupReader> rg_reader,
                                         const common::ObArrayWrap<int> &column_indexs,
                                         const common::ObIArray<uint64_t> &eager_columns);

  int is_column_all_pages_dict_encoded(int32_t col_idx, bool &is_all_pages_dict_encoded) const;

  int save_dict_column_data(int32_t col_idx,
                            int32_t *indices,
                            int64_t indices_count,
                            const void *dict_values,
                            int32_t dict_len,
                            parquet::Type::type parquet_type,
                            bool first_batch,
                            int64_t row_count,
                            int64_t batch_size,
                            bool has_null,
                            const int16_t *def_levels,
                            int16_t max_def_level,
                            const ObExpr *file_col_expr);

  int apply_single_column_dict_filters(
      ObPushdownFilterExecutor *curr_filter,
      ObPushdownFilterExecutor *parent_filter,
      ObEvalCtx &eval_ctx,
      int64_t row_count,
      const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
      const common::ObArrayWrap<int> &column_indexs,
      bool &applied_dict_filter);

  int check_in_non_dict_filter(
      int32_t col_idx,
      ObPushdownFilterExecutor *filter_executor,
      const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
      bool &need_decode);
  int need_decode_dict_column(
      int32_t col_idx,
      ObPushdownFilterExecutor *filter_executor,
      const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
      const common::ObIArray<bool> &is_dup_project,
      bool is_eager_calc,
      bool &need_decode);

  int decode_filtered_rows_to_exprs(
      ObPushdownFilterExecutor *filter_executor,
      ObEvalCtx &eval_ctx,
      const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
      const common::ObIArray<bool> &is_dup_project,
      bool is_eager_calc);

  bool has_dict_columns() const
  {
    return dict_columns_.size() > 0;
  }

  int get_dict_data(int32_t col_idx, ObParquetDictColumnData *&dict_data, bool &has_dict_data);

  bool is_filter_processed_by_dict(const ObPushdownFilterExecutor *filter_executor);

  void clear_filter_arrays()
  {
    processed_dict_op_filters_.reuse();
    failed_dict_filters_.reuse();
  }
  template <typename ArgVec>
  static int filter_batch_by_format(common::ObIVector *col_vector,
                                    ObWhiteFilterExecutor *white_filter,
                                    ObPushdownFilterExecutor *parent_filter,
                                    const int64_t count,
                                    common::ObBitmap *result);

  static bool is_operator_supported_for_dict_filter(ObPushdownFilterExecutor *filter_executor);

  int collect_dict_filter_executor(ObPushdownFilterExecutor *filter_executor);
  bool is_failed_dict_filter(ObPushdownFilterExecutor *filter_executor);
  bool is_processed_dict_op_filter(ObPushdownFilterExecutor *filter_executor);

private:
  int apply_dict_filter_on_black_filter(ObBlackFilterExecutor *black_filter,
                                        ObEvalCtx &eval_ctx,
                                        const ObParquetDictColumnData *dict_data,
                                        common::ObBitmap &result_bitmap);

  int apply_dict_filter_on_white_filter(ObWhiteFilterExecutor *white_filter,
                                        ObEvalCtx &eval_ctx,
                                        const ObParquetDictColumnData *dict_data,
                                        common::ObBitmap &result_bitmap);

  int eval_black_filter_on_dict_values(ObBlackFilterExecutor *black_filter,
                                       ObEvalCtx &eval_ctx,
                                       const ObParquetDictColumnData *dict_data,
                                       common::ObBitmap &valid_dict_codes);

  int eval_white_filter_on_dict_values(ObWhiteFilterExecutor *white_filter,
                                       const ObParquetDictColumnData *dict_data,
                                       common::ObBitmap &valid_dict_codes);

  int filter_by_dict_codes(const common::ObBitmap &valid_dict_codes,
                           const ObIArray<int32_t> &indices,
                           int64_t indices_count,
                           common::ObBitmap &result_bitmap);

  int has_non_dict_filter_for_column(
      ObPushdownFilterExecutor *filter_executor,
      int32_t target_col_idx,
      const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
      bool &has_non_dict_filter);

  int decode_dict_column_to_expr(const ObParquetDictColumnData *dict_data,
                                 const ObPushdownFilterExecutor *filter_executor,
                                 ObExpr *file_col_expr,
                                 ObEvalCtx &eval_ctx);

  int check_all_pages_dict_encoded(int32_t col_idx,
                                   std::shared_ptr<parquet::RowGroupReader> rg_reader,
                                   const common::ObArrayWrap<int> &column_indexs,
                                   bool &is_all_pages_dict_encoded);

  bool is_dict_optimization_worthwhile(const ObParquetDictColumnData *dict_data,
                                       int64_t row_count) const;

  struct DictFilterCacheEntry
  {
    ObPushdownFilterExecutor *filter_executor_;
    common::ObBitmap *valid_dict_codes_;
    DictFilterCacheEntry() : filter_executor_(nullptr), valid_dict_codes_(nullptr)
    {
    }
    DictFilterCacheEntry(ObPushdownFilterExecutor *filter, common::ObBitmap *bitmap)
        : filter_executor_(filter), valid_dict_codes_(bitmap)
    {
    }

    TO_STRING_KV(KP_(filter_executor), KP_(valid_dict_codes));
  };

private:
  lib::ObMemAttr mem_attr_;
  ObArenaAllocator allocator_;
  // key: col_idx, value: dict column data
  DictColumnsMap dict_columns_;
  // key: col_idx, value: 是否所有page都是字典编码（row group级别缓存）
  hash::ObHashMap<int32_t, bool, common::hash::NoPthreadDefendMode> dict_encoding_check_cache_;
  hash::ObHashMap<int32_t, bool, common::hash::NoPthreadDefendMode> column_need_decode_cache_;
  common::ObSEArray<DictFilterCacheEntry, 16> valid_dict_codes_cache_;
  // target exprs for dict filter decode
  common::ObSEArray<ObExpr *, 16> decode_exprs_;
  common::ObSEArray<ObPushdownFilterExecutor *, 16> dict_filter_executors_;
  // 记录已经在字典filter路径下处理过(all_true或all_false)的logic_op_filters，避免重复处理
  common::ObSEArray<ObPushdownFilterExecutor *, 16> processed_dict_op_filters_;
  // 记录失败的字典过滤器
  common::ObSEArray<ObPushdownFilterExecutor *, 16> failed_dict_filters_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObParquetDictFilterPushdown);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_TABLE_OB_PARQUET_DICT_FILTER_H_
