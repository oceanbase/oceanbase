/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SCALAR_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SCALAR_DEFINE_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{

struct ObDASScalarCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_SCALAR_QUERY),
      has_index_scan_(false),
      has_main_scan_(false)
  { }
  virtual ~ObDASScalarCtDef() {}

  OB_INLINE bool has_main_scan() const { return has_main_scan_; }
  OB_INLINE bool has_index_scan() const { return has_index_scan_; }
  OB_INLINE void set_has_main_scan(bool has_main_scan) { has_main_scan_ = has_main_scan; }
  OB_INLINE void set_has_index_scan(bool has_index_scan) { has_index_scan_ = has_index_scan; }

  const ObDASScalarScanCtDef* get_index_scan_ctdef() const;
  const ObDASScalarScanCtDef* get_main_scan_ctdef() const;

private:
  bool has_index_scan_;
  bool has_main_scan_;
};

struct ObDASScalarRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarRtDef()
    : ObIDASSearchRtDef(DAS_OP_SCALAR_QUERY)
  { }
  virtual ~ObDASScalarRtDef() {}

  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  const ObDASScalarCtDef* get_ctdef() const { return static_cast<const ObDASScalarCtDef *>(ctdef_); }
  ObDASScalarScanRtDef* get_index_scan_rtdef() const;
  ObDASScalarScanRtDef* get_main_scan_rtdef() const;
};

struct ObDASScalarScanCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarScanCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_SCALAR_SCAN_QUERY),
      ref_table_id_(common::OB_INVALID_ID),
      access_column_ids_(alloc),
      schema_version_(-1),
      table_param_(alloc),
      pd_expr_spec_(alloc),
      result_output_(alloc),
      rowkey_exprs_(alloc),
      table_scan_opt_(),
      pre_query_range_(alloc),
      pre_range_graph_(alloc),
      flags_(0)
  { }
  virtual ~ObDASScalarScanCtDef() {}

  const ObQueryRangeProvider& get_query_range_provider() const
  {
    return is_new_query_range_ ? static_cast<const ObQueryRangeProvider&>(pre_range_graph_)
                               : static_cast<const ObQueryRangeProvider&>(pre_query_range_);
  }

  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
                       K_(ref_table_id),
                       K_(access_column_ids),
                       K_(schema_version),
                       K_(table_param),
                       K_(pd_expr_spec),
                       K_(result_output),
                       K_(rowkey_exprs),
                       K_(table_scan_opt),
                       K_(pre_query_range),
                       K_(pre_range_graph),
                       K_(flags));

  common::ObTableID ref_table_id_;
  UIntFixedArray access_column_ids_;
  int64_t schema_version_;
  share::schema::ObTableParam table_param_;
  ObPushdownExprSpec pd_expr_spec_;
  sql::ExprFixedArray result_output_;
  sql::ExprFixedArray rowkey_exprs_;
  ObTableScanOption table_scan_opt_;
  ObQueryRange pre_query_range_;
  ObPreRangeGraph pre_range_graph_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_primary_table_scan_        : 1;
      uint64_t is_new_query_range_           : 1;
      uint64_t enable_new_false_range_       : 1;
      uint64_t is_get_                       : 1;
      uint64_t is_rowkey_order_scan_         : 1;
      uint64_t need_rowkey_order_            : 1;
      uint64_t is_search_index_              : 1;
      uint64_t reserved_                     : 57;
    };
  };
};

struct ObDASScalarScanRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScalarScanRtDef()
    : ObIDASSearchRtDef(DAS_OP_SCALAR_SCAN_QUERY),
      p_row2exprs_projector_(nullptr),
      p_pd_expr_op_(nullptr),
      tenant_schema_version_(-1),
      timeout_ts_(-1),
      tx_lock_timeout_(-1),
      sql_mode_(SMO_DEFAULT),
      scan_flag_(),
      stmt_allocator_("StmtScanAlloc"),
      scan_allocator_("TableScanAlloc"),
      key_ranges_(),
      really_need_rowkey_order_(true)
  { }
  virtual ~ObDASScalarScanRtDef() {}

  int init_pd_op(ObExecContext &exec_ctx, const ObDASScalarScanCtDef &scalar_ctdef);
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  const ObDASScalarScanCtDef* get_ctdef() const { return static_cast<const ObDASScalarScanCtDef *>(ctdef_); }

  INHERIT_TO_STRING_KV("ObIDASSearchRtDef", ObIDASSearchRtDef,
                        K_(tenant_schema_version),
                        K_(timeout_ts),
                        K_(tx_lock_timeout),
                        K_(sql_mode),
                        K_(scan_flag),
                        K_(key_ranges),
                        K_(really_need_rowkey_order));

  storage::ObRow2ExprsProjector *p_row2exprs_projector_;
  ObPushdownOperator *p_pd_expr_op_;
  int64_t tenant_schema_version_;
  int64_t timeout_ts_;
  int64_t tx_lock_timeout_;
  ObSQLMode sql_mode_;
  ObQueryFlag scan_flag_;
  common::ObWrapperAllocatorWithAttr stmt_allocator_;
  common::ObWrapperAllocatorWithAttr scan_allocator_;
  common::ObSEArray<common::ObNewRange, 1> key_ranges_;

  // whether the scalar scan really needs rowkey order, it is equal to the need_rowkey_order_
  // of the scalar ctdef by default and it can be adjusted in the runtime for optimization,
  // for example, the vector index pre-filtering scenario.
  bool really_need_rowkey_order_;

private:
  union {
    storage::ObRow2ExprsProjector row2exprs_projector_;
  };
  union {
    ObPushdownOperator pd_expr_op_;
  };
};


} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SCALAR_DEFINE_H_