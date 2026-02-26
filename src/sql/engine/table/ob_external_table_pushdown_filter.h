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


#ifndef OB_EXTERNAL_TABLE_PUSHDOWN_FILTER_H_
#define OB_EXTERNAL_TABLE_PUSHDOWN_FILTER_H_

#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_sstable_index_filter.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase
{

namespace sql {

class ObColumnMeta : public ObDatumMeta
{
public:
  OB_UNIS_VERSION(1);
public:
  ObColumnMeta()
  : ObDatumMeta(),
    max_length_(0),
    has_lob_header_(false),
    is_valid_(false)
  {}
  int from_ob_expr(const ObExpr* expr);
  int from_ob_raw_expr_res_type(const ObRawExprResType &res_type);
  int assign(const ObColumnMeta &other);
  ObColumnMeta &operator=(const ObColumnMeta &other);
  void reset() { new (this) ObColumnMeta(); }
  TO_STRING_KV(K_(type), K_(cs_type), K_(scale), K_(precision), K_(max_length), K_(has_lob_header));

  int64_t max_length_;
  bool has_lob_header_;
  bool is_valid_;
};

struct PageSkipInfo {
  PageSkipInfo(const int64_t column_id,
               const int64_t offset,
               const int64_t rows)
                 : column_id_(column_id), offset_(offset),
                   rows_(rows), skipped_(false) {}
  PageSkipInfo() : column_id_(-1), offset_(-1), rows_(0), skipped_(false) {}
  int64_t column_id_;
  int64_t offset_;
  int64_t rows_;
  bool skipped_;
  TO_STRING_KV(K_(column_id), K_(offset), K_(rows), K_(skipped));
};

class ObExternalTablePushdownFilter {
public:
  enum PushdownLevel {
    DISABLE = 0,
    FILE,
    ROW_GROUP, // aka stripe in orc format
    PAGE,      // aka row_index in orc format
    ENCODING
  };

  enum PushdownType {
    PARQUET = 0,
    ORC = 1,
  };

protected:
  static const int64_t MOCK_ROW_COUNT = INT64_MAX; // row count for filter function

  class MinMaxFilterParamBuilder
  {
  public:
    MinMaxFilterParamBuilder() {}
    virtual ~MinMaxFilterParamBuilder() {}
    virtual int build(const int32_t ext_tbl_col_id, const ObColumnMeta &column_meta,
                      blocksstable::ObMinMaxFilterParam &param) = 0;
    virtual int next_range(const int64_t column_id, int64_t &offset, int64_t &rows) = 0;
    virtual bool has_page_index() const { return false; }
    virtual void rescan() {}
  };

  // struct to record filter expr and its dependency relationships
  // the calc order is file_col_exprs => column_conv_exprs => column_exprs
  struct FilterExprRel {

    FilterExprRel() : column_expr_(nullptr), column_conv_expr_(nullptr),
        file_col_expr_index_(-1), is_file_meta_column_(false), projected_(false) {}
    virtual ~FilterExprRel() {}

    OB_INLINE bool is_valid() const {
      return column_expr_ != nullptr && ((column_conv_expr_ != nullptr &&
        file_col_expr_index_ >= 0) || is_file_meta_column_);
    }

    const ObExpr *column_expr_;  // column ref expr for filter calculation.
    const ObExpr *column_conv_expr_;  // column convert expr which mapping to column_expr_.
    int64_t file_col_expr_index_;  // the index of file column expr in file_column_exprs_.
    bool is_file_meta_column_;
    bool projected_;
  };

public:
  ObExternalTablePushdownFilter() : is_eager_column_(allocator_), is_dup_project_(allocator_),
    eager_columns_(), lazy_columns_(), skip_filter_executor_(), skipping_filter_nodes_(),
    file_filter_col_ids_(allocator_), file_filter_col_metas_(allocator_),
    pd_storage_filters_(nullptr), ext_tbl_filter_pd_level_(0),
    column_ids_(nullptr), filter_enabled_(false) {}
  virtual ~ObExternalTablePushdownFilter() {}
  virtual int init(sql::ObPushdownFilterExecutor *pd_storage_filters,
                   int64_t ext_tbl_filter_pd_level,
                   const ObIArray<uint64_t> &column_ids,
                   ObEvalCtx &eval_ctx);

  int prepare_filter_col_meta(const common::ObArrayWrap<int> &file_col_index,
                              const common::ObIArray<uint64_t> &col_ids,
                              const common::ObIArray<ObColumnMeta> &col_metas);
  int apply_skipping_index_filter(const PushdownLevel filter_level,
                                  MinMaxFilterParamBuilder &param_builder,
                                  bool &skipped,
                                  const int64_t row_count = MOCK_ROW_COUNT);
  OB_INLINE bool has_pushdown_filter() const { return filter_enabled_ && !skipping_filter_nodes_.empty(); }
  OB_INLINE bool has_skip_index_filter() const { return !skipping_filter_nodes_.empty(); }
  int apply_skipping_index_filter(const PushdownLevel filter_level,
                                  MinMaxFilterParamBuilder &param_builder,
                                  ObBitVector *rg_bitmap,
                                  const int64_t num_rows,
                                  ObIAllocator &tmp_alloc);
  int execute_skip_index(const common::hash::ObHashMap<uint64_t, ObArray<PageSkipInfo> *, common::hash::NoPthreadDefendMode> &info_map,
                         const int64_t num_rows,
                         ObBitVector *&filter_rg_bitmap,
                         ObPushdownFilterExecutor *root_filter,
                         ObIAllocator &tmp_alloc);
  int generate_bitmap(const int64_t num_rows,
                      const ObArray<PageSkipInfo> &curr_info,
                      ObBitVector *&filter_rg_bitmap,
                      ObIAllocator &tmp_alloc);
  int generate_default_bitmap(const int64_t num_rows,
                              ObBitVector *&filter_rg_bitmap,
                              ObIAllocator &tmp_alloc);
  int gather_eager_exprs(const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_col_ids,
                        ObPushdownFilterExecutor *root_filter);

  int generate_lazy_exprs(const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_col_ids,
                          const common::ObIArray<ObExpr *> &column_exprs,
                          const common::ObIArray<bool> &column_sel_mask);

protected:
  // build the filter expr relationships and mark which column is eager.
  int build_filter_expr_rels(sql::ObPushdownFilterExecutor *root,
                             const ObExternalTableRowIterator *row_iter);

private:
  int build_skipping_filter_nodes(sql::ObPushdownFilterExecutor &filter);
  int extract_skipping_filter_from_tree(sql::ObPushdownFilterExecutor &filter);
  OB_INLINE bool can_apply_filters(const PushdownLevel filter_level) const
  {
    return static_cast<int64_t>(filter_level) <= ext_tbl_filter_pd_level_;
  }

  int build_filter_expr_rels_recursive(sql::ObPushdownFilterExecutor *filter,
                                       const ObExternalTableRowIterator *row_iter);

  int build_filter_expr_rel(const uint64_t col_id, const ObExpr *col_expr,
                            const ObExternalTableRowIterator *row_iter);

  int find_ext_tbl_expr_index(const ObExpr *expr,
                              const common::ObIArray<ObExpr*> &file_column_exprs,
                              const common::ObIArray<ObExpr*> &file_meta_column_exprs,
                              int64_t &file_col_expr_index,
                              bool &is_file_meta_column);

  static uint64_t bit_or_op(const uint64_t l, const uint64_t r) { return (l | r); }
  static uint64_t bit_and_op(const uint64_t l, const uint64_t r) { return (l & r); }

protected:
  // the array to record whether the column is eager, the array size is the same as the
  // external file column count.
  common::ObFixedArray<bool, ObIAllocator> is_eager_column_;
  // the array to record whether the eager column need to be projected after filter.
  // for example, the query select c1 from tbl where c2 > 10, the c2 is eager column and it isn't
  // duplicate project column, the c1 is not duplicate project column because the c2 is not
  // eager column.
  common::ObFixedArray<bool, ObIAllocator> is_dup_project_;
  // the map from column id to filter expr rel.
  common::hash::ObHashMap<int64_t, FilterExprRel, common::hash::NoPthreadDefendMode> filter_expr_rels_;

  ObArray<uint64_t> eager_columns_;
  ObArray<uint64_t> lazy_columns_;
private:
  ObArenaAllocator allocator_;
  blocksstable::ObSkipIndexFilterExecutor skip_filter_executor_;
  common::ObSEArray<ObSkippingFilterNode, 4> skipping_filter_nodes_;
  common::ObFixedArray<int, ObIAllocator> file_filter_col_ids_;
  common::ObFixedArray<ObColumnMeta, ObIAllocator> file_filter_col_metas_;
  sql::ObPushdownFilterExecutor *pd_storage_filters_;
  int64_t ext_tbl_filter_pd_level_;
  const ObIArray<uint64_t> *column_ids_;
  bool filter_enabled_;
};

}
}

#endif // OB_EXTERNAL_TABLE_PUSHDOWN_FILTER_H_
