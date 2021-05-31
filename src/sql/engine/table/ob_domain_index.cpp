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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_domain_index.h"
#include "sql/engine/table/ob_table_scan_with_index_back.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_ctxcat_analyzer.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_i_data_access_service.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_table_scan_iterator.h"
namespace oceanbase {
using namespace common;
using namespace storage;
namespace sql {
ObDomainIndex::ObDomainIndex(ObIAllocator& allocator)
    : ObNoChildrenPhyOperator(allocator),
      table_op_id_(OB_INVALID_ID),
      allocator_(allocator),
      /*index_scan_info_(allocator),*/
      output_column_ids_(allocator),
      flags_(0),
      index_id_(OB_INVALID_ID),
      index_projector_(NULL),
      index_projector_size_(0),
      /*search_tree_(NULL),*/
      fulltext_mode_flag_(NATURAL_LANGUAGE_MODE),
      index_range_(allocator_),
      fulltext_key_idx_(OB_INVALID_INDEX),
      fulltext_filter_(NULL)
{}

ObDomainIndex::~ObDomainIndex()
{}

int ObDomainIndex::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_FAIL(prepare_index_scan_param(ctx))) {
    LOG_WARN("prepare index scan param failed", K(ret));
  } else if (OB_FAIL(do_range_extract(ctx, false))) {
    LOG_WARN("extract index range failed", K(ret));
  } else if (OB_FAIL(do_domain_index_scan(ctx))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("scan domain index failed", K(ret));
    }
  }
  return ret;
}

int ObDomainIndex::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan no children physical operator failed", K(ret));
  } else if (OB_FAIL(do_range_extract(ctx, true))) {
    LOG_WARN("extract index range failed", K(ret));
  } else if (OB_FAIL(do_domain_index_rescan(ctx))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("rescan domain index failed", K(ret));
    }
  }
  return ret;
}

int ObDomainIndex::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObDomainIndexCtx* index_ctx = NULL;
  ObObj keyword_text;
  uint32_t next_expr_id = 0;
  ObExprOperatorFactory expr_factory(ctx.get_allocator());
  ObExprCtxCatAnalyzer ctxcat_analyzer(expr_factory);

  expr_factory.set_next_expr_id(&next_expr_id);
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret), K(get_type()), K(get_id()));
  } else if (OB_ISNULL(index_ctx = static_cast<ObDomainIndexCtx*>(op_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator context is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("init current row failed", K(ret));
  } else if (OB_FAIL(get_search_keyword_text(ctx, keyword_text))) {
    LOG_WARN("get search keyword text failed", K(ret));
  } else if (OB_FAIL(ctxcat_analyzer.parse_search_keywords(
                 keyword_text, fulltext_mode_flag_, index_ctx->search_tree_, index_ctx->keywords_))) {
    LOG_WARN("splite search keywords failed", K(ret));
  } else if (!index_ctx->keywords_.empty()) {
    if (OB_FAIL(index_ctx->index_scan_iters_.init(index_ctx->keywords_.count()))) {
      LOG_WARN("init index scan iters failed", K(ret));
    } else if (OB_FAIL(index_ctx->index_scan_params_.init(index_ctx->keywords_.count()))) {
      LOG_WARN("init index scan params failed", K(ret));
    } else if (OB_FAIL(index_ctx->iter_expr_ctx_.init(next_expr_id, &index_ctx->index_scan_iters_))) {
      LOG_WARN("init iterator expr context failed", K(ret));
    }
  }
  // create table scan param
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ctx->keywords_.count(); ++i) {
    // for each index scan info create index context
    void* ptr = ctx.get_allocator().alloc(sizeof(ObTableScanParam));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed");
    } else {
      ObTableScanParam* scan_param = new (ptr) ObTableScanParam();
      ObNewRowIterator* row_iter = NULL;
      if (OB_FAIL(index_ctx->index_scan_params_.push_back(scan_param))) {
        LOG_WARN("store table scan param failed", K(ret));
      } else if (OB_FAIL(index_ctx->index_scan_iters_.push_back(row_iter))) {
        LOG_WARN("store new row iterator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDomainIndex::do_domain_index_scan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObDomainIndexCtx* index_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObIDataAccessService* das = NULL;
  if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else {
    das = static_cast<ObIDataAccessService*>(executor_ctx->get_partition_service());
  }
  // set scan param
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ctx->index_scan_params_.count(); ++i) {
    ObTableScanParam* scan_param = index_ctx->index_scan_params_.at(i);
    if (OB_ISNULL(scan_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan_param is null");
    } else if (OB_FAIL(das->table_scan(*scan_param, index_ctx->index_scan_iters_.at(i)))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(*scan_param), K(ret));
      }
    }
  }
  return ret;
}

int ObDomainIndex::do_domain_index_rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObDomainIndexCtx* index_ctx = NULL;
  if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  }
  // set scan param
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ctx->index_scan_params_.count(); ++i) {
    ObTableScanParam* scan_param = index_ctx->index_scan_params_.at(i);
    ObNewRowIterator* scan_iter = index_ctx->index_scan_iters_.at(i);
    if (OB_ISNULL(scan_param) || OB_ISNULL(scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan_param is null", K(scan_param), K(scan_iter));
    } else if (OB_FAIL(static_cast<storage::ObTableScanIterator*>(scan_iter)->rescan(*scan_param))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to rescan table", K(*scan_param), K(ret));
      }
    }
  }
  return ret;
}

int ObDomainIndex::prepare_index_scan_param(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObDomainIndexCtx* index_ctx = NULL;
  ObTableScanWithIndexBack::ObTableScanWithIndexBackCtx* table_ctx = NULL;
  if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(table_ctx = GET_PHY_OPERATOR_CTX(
                           ObTableScanWithIndexBack::ObTableScanWithIndexBackCtx, ctx, table_op_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table scan context failed", K_(table_op_id));
  }
  // set scan param
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ctx->index_scan_params_.count(); ++i) {
    storage::ObTableScanParam* scan_param = index_ctx->index_scan_params_.at(i);
    if (OB_ISNULL(scan_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan param is null");
    } else {
      scan_param->index_id_ = index_id_;
      scan_param->timeout_ = table_ctx->scan_param_.timeout_;
      scan_param->scan_flag_.flag_ = flags_;
      scan_param->reserved_cell_count_ = column_count_;
      scan_param->sql_mode_ = table_ctx->scan_param_.sql_mode_;
      scan_param->scan_allocator_ = &ctx.get_allocator();
      scan_param->frozen_version_ = table_ctx->scan_param_.frozen_version_;
      scan_param->pkey_ = table_ctx->scan_param_.pkey_;
      scan_param->schema_version_ = table_ctx->scan_param_.schema_version_;
      scan_param->query_begin_schema_version_ = table_ctx->scan_param_.query_begin_schema_version_;
      scan_param->limit_param_.limit_ = -1;
      scan_param->limit_param_.offset_ = 0;
      scan_param->trans_desc_ = table_ctx->scan_param_.trans_desc_;
      scan_param->index_projector_ = index_projector_;
      scan_param->projector_size_ = index_projector_size_;
      ret = wrap_expr_ctx(ctx, scan_param->expr_ctx_);
    }
    // fill scan param filters
    if (OB_SUCC(ret)) {
      ObSqlExpression* fulltext_filter = NULL;
      bool need_copy_filter = index_ctx->index_scan_params_.count() > 1 || BOOLEAN_MODE == fulltext_mode_flag_;
      if (OB_FAIL(get_final_fulltext_filter(ctx, index_ctx->keywords_.at(i), fulltext_filter, need_copy_filter))) {
        LOG_WARN("get final fulltext filter failed", K(ret));
      } else if (fulltext_filter != NULL) {
        if (OB_FAIL(scan_param->filters_.push_back(fulltext_filter))) {
          LOG_WARN("store scan param index filter failed", K(ret));
        }
      }
    }
    DLIST_FOREACH(node, filter_exprs_)
    {
      ObSqlExpression* expr = const_cast<ObSqlExpression*>(node);
      if (OB_FAIL(scan_param->filters_.push_back(expr))) {
        LOG_WARN("store scan param index filter failed", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
      if (OB_FAIL(scan_param->column_ids_.push_back(output_column_ids_.at(j)))) {
        LOG_WARN("store column ids failed", K(ret), K(j));
      }
    }
  }
  return ret;
}

inline int ObDomainIndex::get_final_fulltext_filter(
    ObExecContext& ctx, const ObObj& keyword, ObSqlExpression*& fulltext_filter, bool need_deep_copy) const
{
  int ret = OB_SUCCESS;
  if (fulltext_filter_ != NULL) {
    if (OB_UNLIKELY(need_deep_copy)) {
      ObObj tmp;
      const ObSqlFixedArray<ObInfixExprItem>& expr_items = fulltext_filter_->get_expr_items();
      ObSqlExpressionFactory sql_expr_factory(ctx.get_allocator());
      if (OB_FAIL(sql_expr_factory.alloc(fulltext_filter, expr_items.count()))) {
        LOG_WARN("allocate sql expr failed", K(ret));
      } else if (FALSE_IT(fulltext_filter->set_gen_infix_expr(true))) {
        // after 2.2.3, infix expr is always valid
        // do nothing
      } else if (OB_FAIL(fulltext_filter->set_item_count(expr_items.count()))) {
        LOG_WARN("failed to set item count", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_items.count(); ++i) {
        if (IS_DATATYPE_OR_QUESTIONMARK_OP(expr_items.at(i).get_item_type())) {
          // replace the keyword with
          ObPostExprItem expr_item;
          tmp = keyword;
          if (OB_FAIL(expr_item.assign(tmp))) {
            LOG_WARN("assign expr item failed", K(ret));
          } else if (OB_FAIL(fulltext_filter->add_expr_item(expr_item))) {
            LOG_WARN("add expr item to fulltext filter failed", K(ret));
          }
        } else if (OB_FAIL(fulltext_filter->add_expr_item(expr_items.at(i)))) {
          LOG_WARN("add expr item to fulltext filter failed", K(ret));
        }
      }
    } else {
      fulltext_filter = fulltext_filter_;
    }
  }
  return ret;
}

int ObDomainIndex::do_range_extract(ObExecContext& ctx, bool is_rescan) const
{
  int ret = OB_SUCCESS;
  ObQueryRangeArray key_ranges;
  ObGetMethodArray get_method;
  ObDomainIndexCtx* index_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get operator context", K(ret));
  } else if (OB_UNLIKELY(index_ctx->keywords_.empty())) {
    // do nothing
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else {
    if (is_rescan) {
      index_ctx->index_allocator_.reuse();
    }
    ObIAllocator& allocator = is_rescan ? index_ctx->index_allocator_ : ctx.get_allocator();
    ObQueryRange final_range(allocator);
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx.get_my_session());
    if (OB_LIKELY(!index_range_.need_deep_copy())) {
      // For most queries, the query conditions are very standardized and neat.
      // We don't need to copy this condition to change the graph, and we can extract it directly.
      if (OB_FAIL(index_range_.get_tablet_ranges(
              allocator, plan_ctx->get_param_store(), key_ranges, get_method, dtc_params))) {
        LOG_WARN("get tablet range failed", K(ret));
      }
    } else if (OB_FAIL(final_range.deep_copy(index_range_))) {
      LOG_WARN("deep copy index range failed", K(ret));
    } else if (OB_FAIL(final_range.final_extract_query_range(plan_ctx->get_param_store(), dtc_params))) {
      LOG_WARN("final extract query range failed", K(ret));
    } else if (OB_FAIL(final_range.get_tablet_ranges(key_ranges, get_method, dtc_params))) {
      LOG_WARN("get tablet ranges failed", K(ret));
    }

    // in boolen mode, even if only one param in index scan param,
    // the string maybe be changed, so must copy and replace it
    bool need_copy_range = index_ctx->index_scan_params_.count() > 1 || BOOLEAN_MODE == fulltext_mode_flag_;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ctx->index_scan_params_.count(); ++i) {
      if (is_rescan) {
        index_ctx->index_scan_params_.at(i)->key_ranges_.reset();
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < key_ranges.count(); ++j) {
        ObNewRange* key_range = key_ranges.at(j);
        if (OB_ISNULL(key_range)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key range is null", K(j), K(key_ranges));
        } else if (FALSE_IT(key_range->table_id_ = index_id_)) {
          // do nothing
        } else if (OB_LIKELY(!need_copy_range)) {
          if (OB_FAIL(index_ctx->index_scan_params_.at(i)->key_ranges_.push_back(*key_range))) {
            LOG_WARN("store rowkey range failed", K(ret));
          }
        } else {
          ObNewRange final_range;
          if (OB_FAIL(deep_copy_range(allocator, *key_range, final_range))) {
            LOG_WARN("deep copy range failed", K(ret));
          } else if (OB_FAIL(replace_key_range(final_range, index_ctx->keywords_.at(i)))) {
            LOG_WARN("replace key range failed", K(ret), K(final_range));
          } else if (OB_FAIL(index_ctx->index_scan_params_.at(i)->key_ranges_.push_back(final_range))) {
            LOG_WARN("store rowkey range failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

inline int ObDomainIndex::replace_key_range(ObNewRange& key_range, const ObObj& keyword) const
{
  int ret = OB_SUCCESS;
  int64_t key_cnt = key_range.get_start_key().get_obj_cnt();
  ObObj* start = const_cast<ObObj*>(key_range.get_start_key().get_obj_ptr());
  ObObj* end = const_cast<ObObj*>(key_range.get_end_key().get_obj_ptr());
  if (OB_ISNULL(start) || OB_ISNULL(end) || OB_UNLIKELY(fulltext_key_idx_ >= key_cnt) ||
      OB_UNLIKELY(fulltext_key_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid key range", K_(fulltext_key_idx), K(key_cnt), K(start), K(end));
  }
  if (OB_SUCC(ret) && start[fulltext_key_idx_].is_string_type()) {
    start[fulltext_key_idx_] = keyword;
  }
  if (OB_SUCC(ret) && end[fulltext_key_idx_].is_string_type()) {
    end[fulltext_key_idx_] = keyword;
  }
  return ret;
}

inline int ObDomainIndex::get_search_keyword_text(ObExecContext& exec_ctx, ObObj& keyword_text) const
{
  int ret = OB_SUCCESS;
  if (search_key_param_.is_unknown()) {
    ObPhysicalPlanCtx* plan_ctx = NULL;
    if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan context is null");
    } else {
      const ParamStore& param_store = plan_ctx->get_param_store();
      int64_t param_idx = search_key_param_.get_unknown();
      if (OB_UNLIKELY(param_idx < 0) || OB_UNLIKELY(param_idx >= param_store.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param index is invalid", K(param_idx), K(param_store.count()));
      } else {
        keyword_text = param_store.at(param_idx);
      }
    }
  } else {
    keyword_text = search_key_param_;
  }
  return ret;
}

int ObDomainIndex::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // There is only one iterator for a single keyword
  ObDomainIndexCtx* index_ctx = NULL;
  if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get operator context", K(ret));
  } else if (OB_ISNULL(index_ctx->search_tree_)) {
    if (index_ctx->keywords_.empty()) {
      ret = OB_ITER_END;
    } else {
      ret = OB_NOT_INIT;
      LOG_WARN("search tree is null");
    }
  } else if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get domain index context failed", K(get_id()));
  } else if (OB_FAIL(index_ctx->search_tree_->get_next_row(index_ctx->iter_expr_ctx_, row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get current row failed", K(ret));
    }
  } else {
    index_ctx->cur_row_.cells_ = row->cells_;
    row = &index_ctx->cur_row_;
    LOG_DEBUG("get next row from domain index", K(*row));
  }
  return ret;
}

int ObDomainIndex::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObIDataAccessService* das = NULL;
  ObDomainIndexCtx* index_ctx = NULL;

  if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(das = static_cast<ObIDataAccessService*>(executor_ctx->get_partition_service()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get data access service", K(ret));
  } else {
    index_ctx->index_allocator_.reuse();
  }
  if (index_ctx != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ctx->index_scan_iters_.count(); ++i) {
      if (index_ctx->index_scan_iters_.at(i) != NULL) {
        if (OB_FAIL(das->revert_scan_iter(index_ctx->index_scan_iters_.at(i)))) {
          LOG_WARN("fail to revert row iterator", K(ret));
        } else {
          index_ctx->index_scan_iters_.at(i) = NULL;
        }
      }
    }
    for (int64_t i = 0; i < index_ctx->index_scan_params_.count(); ++i) {
      if (index_ctx->index_scan_params_.at(i) != NULL) {
        index_ctx->index_scan_params_.at(i)->~ObTableScanParam();
      }
    }
  }
  return ret;
}

int ObDomainIndex::wrap_expr_ctx(ObExecContext& exec_ctx, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObDomainIndexCtx* index_ctx = NULL;
  if (OB_FAIL(ObPhyOperator::wrap_expr_ctx(exec_ctx, expr_ctx))) {
    LOG_WARN("wrap_expr_ctx failed", K(ret));
  } else if (OB_ISNULL(index_ctx = GET_PHY_OPERATOR_CTX(ObDomainIndexCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    expr_ctx.subplan_iters_ = &(index_ctx->index_scan_iters_);
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDomainIndex)
{
  int ret = OB_SUCCESS;
  bool has_fulltext_filter = (fulltext_filter_ != NULL);
  if (OB_FAIL(ObNoChildrenPhyOperator::serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize no children physical operator failed", K(ret));
  }
  OB_UNIS_ENCODE(table_op_id_);
  OB_UNIS_ENCODE(output_column_ids_);
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(index_id_);
  OB_UNIS_ENCODE_ARRAY(index_projector_, index_projector_size_);
  OB_UNIS_ENCODE(search_key_param_);
  OB_UNIS_ENCODE(fulltext_mode_flag_);
  OB_UNIS_ENCODE(index_range_);
  OB_UNIS_ENCODE(fulltext_key_idx_);
  OB_UNIS_ENCODE(has_fulltext_filter);
  if (has_fulltext_filter) {
    OB_UNIS_ENCODE(*fulltext_filter_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDomainIndex)
{
  int64_t len = 0;
  bool has_fulltext_filter = (fulltext_filter_ != NULL);
  len += ObNoChildrenPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(table_op_id_);
  OB_UNIS_ADD_LEN(output_column_ids_);
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(index_id_);
  OB_UNIS_ADD_LEN_ARRAY(index_projector_, index_projector_size_);
  OB_UNIS_ADD_LEN(search_key_param_);
  OB_UNIS_ADD_LEN(fulltext_mode_flag_);
  OB_UNIS_ADD_LEN(index_range_);
  OB_UNIS_ADD_LEN(fulltext_key_idx_);
  OB_UNIS_ADD_LEN(has_fulltext_filter);
  if (has_fulltext_filter) {
    OB_UNIS_ADD_LEN(*fulltext_filter_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObDomainIndex)
{
  int ret = OB_SUCCESS;
  bool has_fulltext_filter = false;
  if (OB_FAIL(ObNoChildrenPhyOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize no children physical operator failed", K(ret), K(data_len), K(pos));
  }
  OB_UNIS_DECODE(table_op_id_);
  OB_UNIS_DECODE(output_column_ids_);
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(index_id_);
  OB_UNIS_DECODE(index_projector_size_);
  if (OB_SUCC(ret) && index_projector_size_ > 0) {
    if (OB_ISNULL(index_projector_ = my_phy_plan_->alloc_projector(index_projector_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for index projector failed", K_(index_projector_size));
    }
    OB_UNIS_DECODE_ARRAY(index_projector_, index_projector_size_);
  }
  OB_UNIS_DECODE(search_key_param_);
  OB_UNIS_DECODE(fulltext_mode_flag_);
  OB_UNIS_DECODE(index_range_);
  OB_UNIS_DECODE(fulltext_key_idx_);
  OB_UNIS_DECODE(has_fulltext_filter);
  if (has_fulltext_filter) {
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, fulltext_filter_))) {
      LOG_WARN("create sql expression failed", K(ret));
    }
    OB_UNIS_DECODE(*fulltext_filter_);
  }
  return ret;
}

// OB_DEF_SERIALIZE(ObDomainIndex::ObIndexScanInfo)
//{
//  int ret = OB_SUCCESS;
//  if (OB_ISNULL(fulltext_filter_) || OB_ISNULL(index_range_)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("fulltext_filter or index_range is null", K_(fulltext_filter), K_(index_range));
//  } else if (OB_FAIL(fulltext_filter_->serialize(buf, buf_len, pos))) {
//    LOG_WARN("serialize fulltext filter failed", K(ret));
//  } else if (OB_FAIL(index_range_->serialize(buf, buf_len, pos))) {
//    LOG_WARN("serialize index range failed", K(ret));
//  }
//  return ret;
//}
//
// OB_DEF_SERIALIZE_SIZE(ObDomainIndex::ObIndexScanInfo)
//{
//  int64_t len = 0;
//  if (fulltext_filter_ != NULL) {
//    len += fulltext_filter_->get_serialize_size();
//  }
//  if (index_range_ != NULL) {
//    len += index_range_->get_serialize_size();
//  }
//  return len;
//}
//
// OB_DEF_DESERIALIZE(ObDomainIndex::ObIndexScanInfo)
//{
//  int ret = OB_SUCCESS;
//  if (OB_ISNULL(fulltext_filter_) || OB_ISNULL(index_range_)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("fulltext_filter or index_range is null", K_(fulltext_filter), K_(index_range));
//  } else if (OB_FAIL(fulltext_filter_->deserialize(buf, data_len, pos))) {
//    LOG_WARN("deserialize fulltext filter failed", K(ret));
//  } else if (OB_FAIL(index_range_->deserialize(buf, data_len, pos))) {
//    LOG_WARN("deserialize index range", K(ret));
//  }
//  return ret;
//}
}  // namespace sql
}  // namespace oceanbase
