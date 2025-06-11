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

namespace oceanbase
{

namespace sql {

class ObExternalTablePushdownFilter {
public:
  enum PushdownLevel {
    DISABLE = 0,
    FILE,
    ROW_GROUP, // aka stripe in orc format
    PAGE,      // aka row_index in orc format
    ENCODING
  };

protected:
  static const int64_t MOCK_ROW_COUNT = INT64_MAX; // row count for filter function

  class MinMaxFilterParamBuilder
  {
  public:
    MinMaxFilterParamBuilder() {}
    virtual ~MinMaxFilterParamBuilder() {}
    virtual int build(const int64_t ext_tbl_col_id, const ObExpr *expr,
                      blocksstable::ObMinMaxFilterParam &param) = 0;
  };

public:
  ObExternalTablePushdownFilter() : skip_filter_executor_(), skipping_filter_nodes_(),
    file_filter_col_ids_(allocator_), file_filter_exprs_(allocator_),
    param_(nullptr), filter_enabled_(false) {}
  virtual ~ObExternalTablePushdownFilter() {}
  virtual int init(const storage::ObTableScanParam *scan_param);

  int prepare_filter_col_meta(const common::ObArrayWrap<int> &file_col_index,
                              const common::ObIArray<uint64_t> &col_ids,
                              const common::ObIArray<ObExpr*> &col_exprs);
  int apply_skipping_index_filter(const PushdownLevel filter_level,
                                  MinMaxFilterParamBuilder &param_builder,
                                  bool &skipped);
  OB_INLINE bool has_pushdown_filter() const { return filter_enabled_ && !skipping_filter_nodes_.empty(); }

private:
  int build_skipping_filter_nodes(sql::ObPushdownFilterExecutor &filter);
  int extract_skipping_filter_from_tree(sql::ObPushdownFilterExecutor &filter);
  OB_INLINE bool can_apply_filters(const PushdownLevel filter_level) const
  {
    return static_cast<int64_t>(filter_level) <= param_->ext_tbl_filter_pd_level_;
  }

private:
  ObArenaAllocator allocator_;
  blocksstable::ObSkipIndexFilterExecutor skip_filter_executor_;
  common::ObSEArray<ObSkippingFilterNode, 4> skipping_filter_nodes_;
  common::ObFixedArray<int, ObIAllocator> file_filter_col_ids_;
  common::ObFixedArray<ObExpr *, ObIAllocator> file_filter_exprs_;
  const storage::ObTableScanParam *param_;
  bool filter_enabled_;
};

}
}

#endif // OB_EXTERNAL_TABLE_PUSHDOWN_FILTER_H_
