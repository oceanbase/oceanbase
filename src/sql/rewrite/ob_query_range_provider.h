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

#ifndef OCEANBASE_SQL_REWRITE_QUERY_RANGE_PROVIDER_
#define OCEANBASE_SQL_REWRITE_QUERY_RANGE_PROVIDER_

#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"
#include "lib/geo/ob_s2adapter.h"
namespace oceanbase
{
namespace common
{
struct ObDataTypeCastParams;
}
namespace sql
{
struct ColumnItem;
class ObRawExpr;
typedef common::ObSEArray<common::ObNewRange *, 1> ObQueryRangeArray;
typedef common::ObSEArray<common::ObNewRange, 4, common::ModulePageAllocator, true> ObRangesArray;
typedef common::ObSEArray<ColumnItem, 16, common::ModulePageAllocator, true> ColumnArray;
static const int64_t MAX_NOT_IN_SIZE = 10; //do not extract range for not in row over this size
static const int64_t NEW_MAX_NOT_IN_SIZE = 1000; // mysql support 1000 not in range node
class ObFastFinalNLJRangeCtx;
static const double MAX_NOT_IN_SELECTIVITY_TO_CONVERT = 0.1; // do not extract not in range when selectivity is over this


class ObQueryRangeProvider
{
public:
  virtual ~ObQueryRangeProvider() {}
  virtual bool is_new_query_range() const = 0;
  virtual int get_tablet_ranges(common::ObIAllocator &allocator,
                                ObExecContext &exec_ctx,
                                ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params) const = 0;
  virtual int get_tablet_ranges(ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params) = 0;
  virtual int get_ss_tablet_ranges(common::ObIAllocator &allocator,
                                   ObExecContext &exec_ctx,
                                   ObQueryRangeArray &ss_ranges,
                                   const common::ObDataTypeCastParams &dtc_params) const = 0;
  virtual int get_tablet_ranges(common::ObIAllocator &allocator,
                                ObExecContext &exec_ctx,
                                ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params,
                                ObIArray<common::ObSpatialMBR> &mbr_filters) const = 0;
  virtual int get_fast_nlj_tablet_ranges(ObFastFinalNLJRangeCtx &fast_nlj_range_ctx,
                                         common::ObIAllocator &allocator,
                                         ObExecContext &exec_ctx,
                                         const ParamStore &param_store,
                                         int64_t range_buffer_idx,
                                         void *range_buffer,
                                         ObQueryRangeArray &ranges,
                                         const common::ObDataTypeCastParams &dtc_params) const = 0;
  virtual bool is_precise_whole_range() const = 0;
  virtual int is_get(bool &is_get) const = 0;
  virtual bool is_precise_get() const = 0;
  virtual int64_t get_column_count() const = 0;
  virtual bool has_exec_param() const = 0;
  virtual bool is_ss_range() const = 0;
  virtual int64_t get_skip_scan_offset() const = 0;
  virtual int reset_skip_scan_range() = 0;
  virtual bool has_range() const = 0;
  virtual bool is_contain_geo_filters() const = 0;
  virtual const common::ObIArray<ObRawExpr*> &get_range_exprs() const = 0;
  virtual const common::ObIArray<ObRawExpr*> &get_ss_range_exprs() const = 0;
  virtual const common::ObIArray<ObRawExpr*> &get_unprecise_range_exprs() const = 0;
  virtual int get_prefix_info(int64_t &equal_prefix_count,
                              int64_t &range_prefix_count,
                              int64_t &ss_range_prefix_count,
                              bool &contain_always_false) const = 0;

  // to string
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  virtual int get_total_range_sizes(common::ObIArray<uint64_t> &total_range_sizes) const = 0;
  virtual bool is_fast_nlj_range() const = 0;
  virtual bool enable_new_false_range() const = 0;
};

}
}
#endif //OCEANBASE_SQL_REWRITE_QUERY_RANGE_PROVIDER_
//// end of header file

