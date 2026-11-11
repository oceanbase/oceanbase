/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_mv_checker.h"

#include "lib/ob_errno.h"
#include "sql/resolver/mv/ob_mv_compat_control.h"
#include "sql/resolver/mv/ob_mv_checker_utils.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "sql/resolver/mv/ob_mv_major_refresh_checker.h"
#include "sql/resolver/mv/ob_mv_printer_checker.h"
#ifdef OB_BUILD_MV_CLOSE_MODULES
#include "sql/resolver/mv/ob_mv_relation_checker.h"
#endif
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_mview_info.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

class ObMVCheckerRouter
{
public:
  static int check(const ObSelectStmt &stmt,
                   const ObMVCheckerParams &params,
                   ObMVCheckerResult &result);

private:
  typedef int (*CheckerFunc)(const ObSelectStmt &, const ObMVCheckerParams &, ObMVCheckerResult &);

  static int run_checker(const ObSelectStmt &stmt,
                         const ObMVCheckerParams &params,
                         ObMVCheckerResult &result,
                         CheckerFunc checker_func);
  static int check_major_refresh(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result);
  static int check_normal_refresh(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result);
};

int ObMVCheckerRouter::check(const ObSelectStmt &stmt,
                             const ObMVCheckerParams &params,
                             ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(result.start_check())) {
    LOG_WARN("failed to start mv refresh-ability check", K(ret));
  } else {
    if (OB_FAIL(check_major_refresh(stmt, params, result))) {
      LOG_WARN("failed to check major refresh type", K(ret));
    } else if (OB_MV_REFRESH_INVALID != result.get_refresh_type()) {
      // major refresh checker matched
    } else if (OB_FAIL(check_normal_refresh(stmt, params, result))) {
      LOG_WARN("failed to check normal refresh type", K(ret));
    }
    if (OB_SUCCESS != (tmp_ret = result.finish_check())) {
      LOG_WARN("failed to finish mv refresh-ability check", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObMVCheckerRouter::run_checker(const ObSelectStmt &stmt,
                                   const ObMVCheckerParams &params,
                                   ObMVCheckerResult &result,
                                   CheckerFunc checker_func)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(result.start_check())) {
    LOG_WARN("failed to start mv checker", K(ret));
  } else {
    if (OB_FAIL(checker_func(stmt, params, result))) {
      LOG_WARN("failed to run mv checker", K(ret));
    } else if (OB_SUCCESS != (tmp_ret = result.finish_check())) {
      LOG_WARN("failed to finish mv checker", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

// The resulted refresh type can be:
// OB_MV_REFRESH_INVALID                - the statement does not match major refresh form
// OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV - the statement meets all the major refresh requirements.
// OB_MV_COMPLETE_REFRESH               - the statement meets the major refresh form, but dependency checks failed.
int ObMVCheckerRouter::check_major_refresh(const ObSelectStmt &stmt,
                                           const ObMVCheckerParams &params,
                                           ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  const bool for_create_mview = params.for_create_mview_;
  const ObTableSchema &container_table_schema = params.mv_container_table_schema_;
  const bool is_persisted_major_refresh = !for_create_mview && container_table_schema.mv_major_refresh();

  if (!for_create_mview && !is_persisted_major_refresh) {
    // do nothing
  } else {
    ObMVCheckerResult tmp_result;
    if (OB_FAIL(run_checker(stmt, params, tmp_result, ObMVMajorRefreshChecker::check))) {
      LOG_WARN("failed to check major refresh type", K(ret));
    } else if (OB_MV_REFRESH_INVALID != tmp_result.get_refresh_type()) {
      if (OB_FAIL(result.assign(tmp_result))) {
        LOG_WARN("failed to assign major refresh checker result", K(ret));
      }
    } else if (is_persisted_major_refresh) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("persisted major refresh mview no longer matches major refresh checker",
               K(ret),
               K(container_table_schema.get_table_name()));
    }
  }
  return ret;
}

int ObMVCheckerRouter::check_normal_refresh(const ObSelectStmt &stmt,
                                            const ObMVCheckerParams &params,
                                            ObMVCheckerResult &result)
{
  enum CheckerType
  {
    PRINTER_CHECKER = 0,
    RELATION_CHECKER = 1,
    CHECKER_COUNT
  };

  int ret = OB_SUCCESS;

#ifdef OB_BUILD_MV_CLOSE_MODULES
  int64_t checker_cnt = 1;
  CheckerType checker_order[CHECKER_COUNT] = {PRINTER_CHECKER, RELATION_CHECKER};
  bool relation_checker_enabled = false;
  if (OB_FAIL(ObMVRelationChecker::is_enabled(params, relation_checker_enabled))) {
    LOG_WARN("failed to check whether relation checker is enabled", K(ret));
  } else if (relation_checker_enabled) {
    checker_cnt = CHECKER_COUNT;
    // set priority of checkers
    const bool prefer_relation_based = stmt.get_query_ctx()->check_opt_compat_version(
        COMPAT_VERSION_4_4_2_BP4, COMPAT_VERSION_4_5_0, COMPAT_VERSION_5_0_2);
    checker_order[0] = prefer_relation_based ? RELATION_CHECKER : PRINTER_CHECKER;
    checker_order[1] = prefer_relation_based ? PRINTER_CHECKER : RELATION_CHECKER;
  }
#else
  const int checker_cnt = 1;
  CheckerType checker_order[1] = {PRINTER_CHECKER};
#endif

  // perform check(s)
  bool can_fast_refresh = false;
  bool relation_rejected = false;
  ObMVCheckerResult checker_results[CHECKER_COUNT];
  for (int64_t i = 0; OB_SUCC(ret) && !can_fast_refresh && i < checker_cnt; ++i) {
    const CheckerType checker_type = checker_order[i];
    ObMVCheckerResult &checker_result = checker_results[i];
    if (PRINTER_CHECKER == checker_type) {
      if (OB_FAIL(run_checker(stmt, params, checker_result, ObMVPrinterChecker::check))) {
        LOG_WARN("failed to check printer refresh type", K(ret));
      }
    } else if (RELATION_CHECKER == checker_type) {
#ifdef OB_BUILD_MV_CLOSE_MODULES
      if (OB_FAIL(run_checker(stmt, params, checker_result, ObMVRelationChecker::check))) {
        LOG_WARN("failed to check relation-based refresh", K(ret));
      } else if (OB_MV_COMPLETE_REFRESH == checker_result.get_refresh_type()) {
        relation_rejected = true;
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("relation checker is unavailable", K(ret));
#endif
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown checker type", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (IS_VALID_FAST_REFRESH_TYPE(checker_result.get_refresh_type())) {
        can_fast_refresh = true;
        // the printer checker supports fast refresh but relation checker rejects
        // record a warning for further diagnostics
        if (PRINTER_CHECKER == checker_type && relation_rejected) {
          LOG_WARN("mv fast refresh supported by printer checker, but rejected by relation checker",
                   K(checker_result.get_refresh_type()),
                   K(result.get_note().error_));
        }
      }
      if ((0 == i || can_fast_refresh || PRINTER_CHECKER == checker_type)
          && OB_FAIL(result.assign(checker_result))) {
        LOG_WARN("failed to assign checker result", K(ret), K(checker_type));
      }
    }
  }

  return ret;
}

int ObMVCheckerResult::start_check()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::INITED != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in initial state", K(ret), K(static_cast<int64_t>(state_)));
  } else {
    reset_output();
    state_ = State::IN_PROGRESS;
  }
  return ret;
}

int ObMVCheckerResult::finish_check()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress", K(ret), K(static_cast<int64_t>(state_)));
  } else {
    state_ = State::FINISHED;
  }
  return ret;
}

int ObMVCheckerResult::assign(const ObMVCheckerResult &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress", K(ret), K(static_cast<int64_t>(state_)));
  } else if (OB_UNLIKELY(State::FINISHED != other.state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source mv checker result is not finished", K(ret), K(static_cast<int64_t>(other.state_)));
  } else {
    reset_output();
  }
  if (OB_FAIL(ret)) {
  } else if (!other.note_.error_.empty() && OB_FAIL(note_.error_.assign(other.note_.error_))) {
    LOG_WARN("failed to assign fast refreshable error", K(ret));
  } else if (OB_FAIL(dependent_columns_.assign(other.dependent_columns_))) {
    LOG_WARN("failed to assign dependent columns", K(ret));
  } else if (OB_FAIL(mlog_required_columns_info_.merge_from(other.mlog_required_columns_info_))) {
    LOG_WARN("failed to assign mlog required columns", K(ret));
  } else if (OB_FAIL(mlog_tables_.assign(other.mlog_tables_))) {
    LOG_WARN("failed to assign mlog tables", K(ret));
  } else if (OB_FAIL(child_refresh_types_.assign(other.child_refresh_types_))) {
    LOG_WARN("failed to assign child refresh types", K(ret));
  } else {
    refresh_type_ = other.refresh_type_;
    union_all_marker_idx_ = other.union_all_marker_idx_;
  }
  return ret;
}

int ObMVCheckerResult::set_refresh_type(const ObMVRefreshableType refresh_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress", K(ret), K(static_cast<int64_t>(state_)), K(refresh_type));
  } else {
    refresh_type_ = refresh_type;
    if (OB_MV_COMPLETE_REFRESH == refresh_type_) {
      // clear fast-refresh dependent infos
      dependent_columns_.reuse();
      mlog_required_columns_info_.reset();
      mlog_tables_.reuse();
      union_all_marker_idx_ = OB_INVALID_INDEX;
      child_refresh_types_.reuse();
    }
  }
  return ret;
}

int ObMVCheckerResult::add_dependent_column(ObRawExpr *dep_col, const int64_t stmt_idx)
{
  int ret = OB_SUCCESS;
  bool is_existed = false;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress",
             K(ret),
             K(static_cast<int64_t>(state_)),
             KPC(dep_col),
             K(stmt_idx));
  } else if (OB_ISNULL(dep_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dependent column is unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < dependent_columns_.count(); ++i) {
    const std::pair<ObRawExpr *, int64_t> &existing_dep_col = dependent_columns_.at(i);
    if (OB_ISNULL(existing_dep_col.first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("recorded dependent column is unexpected null", K(ret), K(i));
    } else if (stmt_idx == existing_dep_col.second && dep_col->same_as(*existing_dep_col.first)) {
      is_existed = true;
    }
  }
  if (OB_FAIL(ret) || is_existed) {
    // do nothing
  } else if (OB_FAIL(dependent_columns_.push_back(std::make_pair(dep_col, stmt_idx)))) {
    LOG_WARN("failed to add dependent column", K(ret), KPC(dep_col), K(stmt_idx));
  }
  return ret;
}

int ObMVCheckerResult::record_mlog_required_columns(const uint64_t base_object_id,
                                                    const ObIArray<uint64_t> &col_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress",
             K(ret), K(static_cast<int64_t>(state_)), K(base_object_id));
  } else if (OB_FAIL(mlog_required_columns_info_.record_required_columns(base_object_id, col_ids))) {
    LOG_WARN("failed to record mlog required columns", K(ret), K(base_object_id));
  }
  return ret;
}

int ObMVCheckerResult::add_mlog_table(const TableItem *table_item, const ObTableSchema *mlog_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress",
             K(ret),
             K(static_cast<int64_t>(state_)),
             KPC(table_item),
             KPC(mlog_schema));
  } else if (OB_ISNULL(table_item) || OB_ISNULL(mlog_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null mlog table", K(ret), KPC(table_item), KPC(mlog_schema));
  } else if (OB_FAIL(mlog_tables_.push_back(std::make_pair(table_item, mlog_schema)))) {
    LOG_WARN("failed to add mlog table", K(ret), KPC(table_item), KPC(mlog_schema));
  }
  return ret;
}

int ObMVCheckerResult::set_union_all_marker_idx(const int64_t marker_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress", K(ret), K(static_cast<int64_t>(state_)), K(marker_idx));
  } else {
    union_all_marker_idx_ = marker_idx;
  }
  return ret;
}

int ObMVCheckerResult::add_child_refresh_type(const ObMVRefreshableType refresh_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::IN_PROGRESS != state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv checker result is not in progress", K(ret), K(static_cast<int64_t>(state_)), K(refresh_type));
  } else if (OB_FAIL(child_refresh_types_.push_back(refresh_type))) {
    LOG_WARN("failed to add child refresh type", K(ret), K(refresh_type));
  }
  return ret;
}

void ObMVCheckerResult::reset_output()
{
  refresh_type_ = OB_MV_REFRESH_INVALID;
  note_.error_.reuse();
  dependent_columns_.reuse();
  mlog_required_columns_info_.reset();
  mlog_tables_.reuse();
  union_all_marker_idx_ = OB_INVALID_INDEX;
  child_refresh_types_.reuse();
}

uint64_t ObMVCheckerParams::get_tenant_id() const
{ return session_info_.get_effective_tenant_id(); }

int ObMVChecker::prepare_mv_stmt_and_check_refresh_type(const ObMVCheckerParams &params,
                                                        ObDMLStmt *&stmt,
                                                        ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_mv_stmt_for_check(stmt, params))) {
    LOG_WARN("failed to prepare mv stmt for check", K(ret));
  } else if (OB_FAIL(ObMVCheckerRouter::check(*static_cast<ObSelectStmt *>(stmt), params, result))) {
    LOG_WARN("failed to check mv fast refresh type", K(ret));
  }
  return ret;
}

int ObMVChecker::prepare_mv_stmt_for_check(ObDMLStmt *&stmt, const ObMVCheckerParams &params)
{
  int ret = OB_SUCCESS;
  uint64_t rule_set = 0;
  bool trans_happened = false;
  if (OB_ISNULL(stmt) || OB_UNLIKELY(!stmt->is_select_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt));
  } else {
    ObTransformerCtx trans_ctx;
    ObTransformerImpl transformer(&trans_ctx);
    trans_ctx.allocator_ = &params.allocator_;
    trans_ctx.schema_checker_ = &params.schema_checker_;
    trans_ctx.session_info_ = &params.session_info_;
    trans_ctx.exec_ctx_ = params.session_info_.get_cur_exec_ctx();
    trans_ctx.expr_factory_ = &params.expr_factory_;
    trans_ctx.stmt_factory_ = &params.stmt_factory_;
    trans_ctx.is_trans_mv_def_stmt_ = true;
    if (OB_FAIL(transform_preprocess_mv_stmt(stmt, trans_ctx, params))) {
      LOG_WARN("failed to preprocess mv stmt", K(ret), KPC(stmt));
    } else if (OB_FAIL(get_trans_rule_set(stmt, params, rule_set))) {
      LOG_WARN("failed to get trans rule set", K(ret), KPC(stmt));
    } else if (0 == rule_set) {
      // do nothing
    } else if (OB_FAIL(transformer.transform_rule_set(stmt, rule_set, 1, trans_happened))) {
      LOG_WARN("failed to transform mv stmt", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(stmt) || OB_UNLIKELY(!stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected transformed mv stmt", K(ret), KPC(stmt));
  } else if (OB_FAIL(normalize_mv_stmt_for_check(*static_cast<ObSelectStmt *>(stmt), params))) {
    LOG_WARN("failed to normalize mv stmt for check", K(ret));
  }
  return ret;
}

int ObMVChecker::transform_preprocess_mv_stmt(ObDMLStmt *stmt,
                                              ObTransformerCtx &trans_ctx,
                                              const ObMVCheckerParams &params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  bool trans_happened = false;
  if (OB_ISNULL(stmt)
      || OB_ISNULL(trans_ctx.session_info_)
      || OB_ISNULL(trans_ctx.schema_checker_)
      || OB_ISNULL(trans_ctx.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(stmt));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret), KPC(stmt));
  } else if (OB_FAIL(ObTransformUtils::right_join_to_left(stmt))) {
    LOG_WARN("failed to transform right join to left", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_all_rowkey_columns_to_stmt(trans_ctx, stmt, trans_happened))) {
    LOG_WARN("failed to add all rowkey columns to stmt", K(ret));
  } else if (OB_FAIL(transform_mv_stmt_to_full_group_by(stmt, trans_ctx, params))) {
    LOG_WARN("failed to transform mv stmt to full group by", K(ret));
  } else {
    ObExpandAggregateUtils expand_aggr_utils(*trans_ctx.expr_factory_, trans_ctx.session_info_);
    expand_aggr_utils.set_expand_for_mv();
    if (OB_FAIL(expand_aggr_utils.expand_aggr_expr(stmt, trans_happened))) {
      LOG_WARN("failed to expand aggr expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(transform_preprocess_mv_stmt(child_stmts.at(i), trans_ctx, params)))) {
      LOG_WARN("failed to preprocess child stmt", K(ret), K(i), KPC(child_stmts.at(i)));
    }
  }
  return ret;
}

int ObMVChecker::transform_mv_stmt_to_full_group_by(ObDMLStmt *stmt,
                                                    ObTransformerCtx &trans_ctx,
                                                    const ObMVCheckerParams &params)
{
  int ret = OB_SUCCESS;
  bool enable_transform = false;
  bool trans_happened = false;
  if (OB_FAIL(check_enable_transform_to_full_group_by(stmt, params, enable_transform))) {
    LOG_WARN("failed to check enable transform to full group by", K(ret));
  } else if (!enable_transform) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::transform_to_full_group_by(trans_ctx, stmt, trans_happened))) {
    LOG_WARN("failed to transform to full group by", K(ret));
  }
  return ret;
}

int ObMVChecker::check_enable_transform_to_full_group_by(ObDMLStmt *stmt,
                                                         const ObMVCheckerParams &params,
                                                         bool &enable_transform)
{
  int ret = OB_SUCCESS;
  enable_transform = !params.need_on_query_computation_;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(stmt));
  } else if (!enable_transform) {
    // do nothing
  } else if (OB_FAIL(stmt->get_query_ctx()->get_global_hint().opt_params_.has_enable_opt_param(
               ObOptParamHint::ENABLE_FAST_REFRESH_WITHOUT_ONLY_FULL_GROUP_BY,
               enable_transform))) {
    LOG_WARN("failed to get enable fast refresh without only full group by", K(ret));
  } else if (!enable_transform) {
    // do nothing
  } else if (OB_FAIL(ObMVCompatControl::check_feature_enable(
               params.compat_version_,
               ObMVCompatFeatureType::RELATION_BASED_FAST_REFRESH,
               enable_transform))) {
    LOG_WARN("failed to check relation based fast refresh feature",
             K(ret), K(params.compat_version_));
  }
  return ret;
}

int ObMVChecker::get_trans_rule_set(const ObDMLStmt *stmt,
                                    const ObMVCheckerParams &params,
                                    uint64_t &rule_set)
{
  int ret = OB_SUCCESS;
  bool enable_relation_based = false;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  ObSEArray<const JoinedTable *, 8> joined_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(ObMVCompatControl::check_feature_enable(params.compat_version_,
                                                             ObMVCompatFeatureType::RELATION_BASED_FAST_REFRESH,
                                                             enable_relation_based))) {
    LOG_WARN("failed to check relation based fast refresh feature", K(ret), K(params.compat_version_));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret), KPC(stmt));
  } else if (OB_FAIL(append(joined_tables, stmt->get_joined_tables()))) {
    LOG_WARN("failed to append joined tables", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(get_trans_rule_set(child_stmts.at(i), params, rule_set)))) {
      LOG_WARN("failed to get trans rule set", K(ret), KPC(child_stmts.at(i)));
    }
  }
  while (OB_SUCC(ret) && (0 == (rule_set & 1L << ELIMINATE_OJ)) && !joined_tables.empty()) {
    const JoinedTable *table = NULL;
    if (OB_FAIL(joined_tables.pop_back(table))) {
      LOG_WARN("failed to pop back joined table", K(ret));
    } else if (OB_ISNULL(table) || OB_ISNULL(table->left_table_) || OB_ISNULL(table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), KPC(table));
    } else if (enable_relation_based || !table->is_inner_join()) {
      rule_set |= 1L << ELIMINATE_OJ;
    } else if (table->left_table_->is_joined_table()
               && OB_FAIL(joined_tables.push_back(static_cast<const JoinedTable *>(table->left_table_)))) {
      LOG_WARN("failed to append left joined tables", K(ret));
    } else if (table->right_table_->is_joined_table()
               && OB_FAIL(joined_tables.push_back(static_cast<const JoinedTable *>(table->right_table_)))) {
      LOG_WARN("failed to append right joined tables", K(ret));
    }
  }
  return ret;
}

int ObMVChecker::normalize_mv_stmt_for_check(ObSelectStmt &stmt, const ObMVCheckerParams &params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(adjust_set_stmt_sel_item(stmt))) {
    LOG_WARN("failed to adjust set stmt select item", K(ret));
  } else if (OB_FAIL(stmt.formalize_stmt_expr_reference(&params.expr_factory_, &params.session_info_, true))) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else if (OB_FAIL(reset_stmt_id_recursively(&stmt))) {
    LOG_WARN("failed to reset stmt id recursively", K(ret));
  }
  return ret;
}

int ObMVChecker::adjust_set_stmt_sel_item(ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (stmt.is_set_stmt()) {
    ObIArray<ObSelectStmt *> &set_queries = stmt.get_set_query();
    const int64_t sel_size = stmt.get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_size; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < set_queries.count(); ++j) {
        if (OB_ISNULL(set_queries.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i), K(j), K(stmt));
        } else if (0 < j) {
          set_queries.at(j)->get_select_item(i).alias_name_ = set_queries.at(0)->get_select_item(i).alias_name_;
        }
      }
    }
  }
  return ret;
}

int ObMVChecker::reset_stmt_id_recursively(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KPC(stmt));
  } else if (OB_FALSE_IT(stmt->reset_stmt_id())) {
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(reset_stmt_id_recursively(child_stmts.at(i))))) {
        LOG_WARN("failed to reset ref query stmt id recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSqlSchemaGuard *sql_schema_guard = NULL;
  uint64_t data_version = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(params_.get_tenant_id()));
  if (OB_ISNULL(stmt.get_query_ctx()) || OB_ISNULL(sql_schema_guard = &stmt.get_query_ctx()->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(sql_schema_guard));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(params_.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (stmt.is_set_stmt()) {
    const ObIArray<ObSelectStmt *> &set_queries = stmt.get_set_query();
    if (OB_UNLIKELY(set_queries.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set query is unexpectedly empty", K(ret), K(stmt));
    } else {
      is_valid = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < set_queries.count(); ++i) {
        if (OB_ISNULL(set_queries.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set query branch is unexpected null", K(ret), K(i), K(stmt));
        } else if (OB_FAIL(SMART_CALL(check_mv_dependency_mlog_tables(*set_queries.at(i), is_valid)))) {
          LOG_WARN("failed to check set query branch mlog tables", K(ret), K(i));
        }
      }
    }
  } else if (stmt.get_table_size() == 0) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("table size is 0");
  } else {
    is_valid = true;
    const ObIArray<TableItem*> &tables = stmt.get_table_items();
    const share::schema::ObTableSchema *table_schema = NULL;
    const share::schema::ObTableSchema *mlog_schema = NULL;
    const TableItem *table = NULL;
    ObSEArray<ColumnItem, 4> table_columns;
    ObSEArray<uint64_t, 4> table_col_ids;
    bool enable_auto_build_mlog = false;
    uint64_t base_object_id = OB_INVALID_ID;
    if (((data_version >= MOCK_DATA_VERSION_4_3_5_4 && data_version < DATA_VERSION_4_4_0_0)
         || (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0)
         || (data_version >= DATA_VERSION_4_5_1_0))
        && tenant_config.is_valid()
        && tenant_config->enable_mlog_auto_maintenance) {
      enable_auto_build_mlog = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < tables.count(); ++i) {
      table_columns.reuse();
      table_col_ids.reuse();
      if (OB_ISNULL(table = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table", K(ret), KPC(table));
      } else if (table->is_mv_proctime_table_) {
        // do nothing, no need to check mlog table for proctime table
      } else if (params_.for_create_mview_ && enable_auto_build_mlog) {
        base_object_id = ObTableType::MATERIALIZED_VIEW == table->table_type_
                         ? table->mview_id_ : table->ref_id_;
        const ObIArray<std::pair<ObRawExpr *, int64_t>> &dep_cols = result_.get_dependent_columns();
        for (int64_t j = 0; OB_SUCC(ret) && j < dep_cols.count(); ++j) {
          if (OB_FAIL(ObOptimizerUtil::extract_column_ids(dep_cols.at(j).first, table->table_id_, table_col_ids))) {
            LOG_WARN("failed to extract column ids from dependent column", K(ret), KPC(dep_cols.at(j).first));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(stmt.get_column_ids(table->table_id_, table_col_ids))) {
          LOG_WARN("failed to get column items", K(ret));
        } else if (OB_FAIL(result_.record_mlog_required_columns(base_object_id, table_col_ids))) {
          LOG_WARN("failed to record mlog required columns", K(ret), K(base_object_id), KPC(table));
        }
      } else if (OB_FAIL(sql_schema_guard->get_table_schema(table->ref_id_, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(table_schema));
      } else if (OB_FAIL(sql_schema_guard->get_table_mlog_schema(table->ref_id_, mlog_schema))
                 || OB_ISNULL(mlog_schema)
                 || !mlog_schema->is_available_mlog()) {
        if (OB_TABLE_NOT_EXIST == ret || OB_ERR_TABLE_NO_MLOG == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get mlog schema", K(ret), K(table_schema->get_table_name()));
        } else {
          is_valid = false;
          result_.get_note().error_.assign_fmt("base table %.*s doesn't have mlog table",
                                               table->get_table_name().length(),
                                               table->get_table_name().ptr());
        }
      } else if (OB_FAIL(stmt.get_column_items(table->table_id_, table_columns))) {
        LOG_WARN("failed to get column items", K(ret));
      } else if (OB_FAIL(check_mlog_table_valid(table_schema,
                                                table,
                                                table_columns,
                                                *mlog_schema,
                                                sql_schema_guard->get_schema_guard(),
                                                is_valid))) {
        LOG_WARN("failed to get and check mlog table", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else if (OB_FAIL(result_.add_mlog_table(table, mlog_schema))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

// get mlog table schema, check columns exists in mlog table
int ObMVChecker::check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                                         const TableItem *table_item,
                                         const ObIArray<ColumnItem> &columns,
                                         const share::schema::ObTableSchema &mlog_schema,
                                         ObSchemaGetterGuard *schema_guard,
                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  uint64_t mlog_cid = OB_INVALID_ID;
  ObSEArray<uint64_t, 4> unique_col_ids;
  if (OB_ISNULL(table_schema) || OB_ISNULL(table_item) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(schema_guard), K(table_item));
  } else if (OB_FAIL(ObMVCheckerUtils::get_table_rowkey_ids(table_schema, schema_guard, unique_col_ids))) {
    LOG_WARN("failed to get table rowkey ids", K(ret), KPC(table_schema));
  }
  for (int i = 0; is_valid && OB_SUCC(ret) && i < unique_col_ids.count(); ++i) {
    mlog_cid = ObTableSchema::gen_mlog_col_id_from_ref_col_id(unique_col_ids.at(i));
    is_valid = NULL != mlog_schema.get_column_schema(mlog_cid);
    if (!is_valid) {
      ObString column_name;
      bool column_exist = false;
      table_schema->get_column_name_by_column_id(unique_col_ids.at(i), column_name, column_exist);
      if (OB_UNLIKELY(!column_exist)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column name", K(ret), K(unique_col_ids.at(i)), KPC(table_schema));
      } else {
        // clang-format off
        result_.get_note().error_.assign_fmt(
          "all primary keys and partition keys of table %.*s are required in the corresponding mlog table, missing column %s",
          table_item->get_table_name().length(),
          table_item->get_table_name().ptr(),
          column_name.ptr());
        // clang-format on
      }
    }
    LOG_DEBUG("check mlog_table unique column is valid", K(is_valid), K(i), K(mlog_cid), K(unique_col_ids));
  }
  for (int i = 0; is_valid && OB_SUCC(ret) && i < columns.count(); ++i) {
    mlog_cid = ObTableSchema::gen_mlog_col_id_from_ref_col_id(columns.at(i).base_cid_);
    is_valid = NULL != mlog_schema.get_column_schema(mlog_cid);
    if (!is_valid) {
      // clang-format off
      result_.get_note().error_.assign_fmt(
          "column %s of table %.*s used in mv is required in the corresponding mlog table",
          columns.at(i).column_name_.ptr(),
          table_item->get_table_name().length(),
          table_item->get_table_name().ptr());
      // clang-format on
    }
    LOG_DEBUG("check mlog_table column is valid", K(is_valid), K(i), K(mlog_cid), K(columns));
  }
  return ret;
}

int ObMVChecker::check_fast_refresh_dep_consistency(bool &is_dep_consistent)
{
  int ret = OB_SUCCESS;
  is_dep_consistent = true;
  uint64_t data_version = 0;
  ObSEArray<share::schema::ObDependencyInfo, 8> dependency_infos;
  ObSEArray<ObMVDepInfo, 8> previous_mv_dep_infos;
  const ObQueryCtx *query_ctx = stmt_.get_query_ctx();
  const uint64_t tenant_id = params_.get_tenant_id();
  const uint64_t mview_id = params_.mv_schema_.get_table_id();
  if (OB_ISNULL(query_ctx) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(query_ctx), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (OB_FAIL(share::schema::ObDependencyInfo::collect_dep_infos_for_view(
               query_ctx->reference_obj_tables_,
               data_version >= MOCK_DATA_VERSION_4_4_2_0 /* need_nested_dep */,
               dependency_infos))) {
    LOG_WARN("failed to collect dep infos", K(ret));
  } else if (OB_FAIL(ObMVDepUtils::get_mview_dep_infos(*GCTX.sql_proxy_,
                                                       tenant_id,
                                                       mview_id,
                                                       previous_mv_dep_infos,
                                                       false /* ignore_udt_udf */))) {
    LOG_WARN("fail to parse mview ref infos", KR(ret), K(tenant_id), K(mview_id));
  } else if (previous_mv_dep_infos.count() > dependency_infos.count()) {
    is_dep_consistent = false;
    LOG_INFO("dependency num not match", K(dependency_infos), K(previous_mv_dep_infos));
  }

  // Check dependency consistency against the state persisted when the MV was created.
  for (int64_t i = 0; OB_SUCC(ret) && is_dep_consistent && i < dependency_infos.count(); ++i) {
    const share::schema::ObDependencyInfo &dep = dependency_infos.at(i);
    if (i >= previous_mv_dep_infos.count()) {
      // For fast refresh mview with user-defined proctime view, tables in that view
      // were not recorded in previous_mv_dep_infos in earlier versions and
      // dep_obj_id was recorded as a valid id. For upgrades from that version, skip.
      if (OB_INVALID_ID == dep.get_dep_obj_id()) {
        is_dep_consistent = false;
        LOG_INFO("dependency num not match", K(i), K(dependency_infos));
      }
    } else if (dep.get_ref_obj_id() != previous_mv_dep_infos.at(i).p_obj_) {
      is_dep_consistent = false;
      LOG_INFO("dependency changed", K(i), K(dep), K(previous_mv_dep_infos.at(i)));
    } else if (previous_mv_dep_infos.at(i).is_complete_refresh_only_) {
      is_dep_consistent = false;
      LOG_INFO("dependency is complete refresh only", K(i), K(dep), K(previous_mv_dep_infos.at(i)));
    }
  }
  return ret;
}

int ObMVChecker::collect_dep_mview_ids_from_stmt(const ObSelectStmt &stmt,
                                                 ObIArray<uint64_t> &dep_mview_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  const TableItem *table = NULL;
  const ObSelectStmt *child_stmt = NULL;
  if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    if (OB_ISNULL(table = stmt.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table", K(ret), K(i));
    } else if (table->is_mv_proctime_table_ && NULL != table->ref_query_
               && OB_FAIL(ObOptimizerUtil::remove_item(child_stmts, table->ref_query_))) {
      LOG_WARN("failed to remove proctime view stmt", K(ret));
    } else if (ObTableType::MATERIALIZED_VIEW == table->table_type_
               && OB_FAIL(add_var_to_array_no_dup(dep_mview_ids, table->mview_id_))) {
      LOG_WARN("failed to add dependency mview id", K(ret), KPC(table));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child stmt", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(collect_dep_mview_ids_from_stmt(*child_stmt, dep_mview_ids)))) {
      LOG_WARN("failed to collect dependency mview ids from child stmt", K(ret), K(i));
    }
  }
  return ret;
}

int ObMVChecker::set_final_refresh_type(const ObMVRefreshableType refresh_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_MV_REFRESH_INVALID == refresh_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid final refresh type", K(ret), K(refresh_type));
  } else if (IS_VALID_FAST_REFRESH_TYPE(refresh_type)) {
    bool need_fallback = true;
    if (params_.for_create_mview_) {
      // fallback checks for creation time
      bool can_nested_mviews_fast_refresh = true;
      if (OB_FAIL(check_nested_mview_refresh_type(can_nested_mviews_fast_refresh))) {
        LOG_WARN("failed to check nested mview refresh type", K(ret));
      } else if (can_nested_mviews_fast_refresh) {
        need_fallback = false;
      } else if (OB_FAIL(fallback_to_complete_refresh("mview dependencies do not support fast refresh"))) {
        LOG_WARN("failed to fallback to complete refresh", K(ret));
      }
    } else {
      // fallback checks for runtime
      bool is_dep_consistent = true;
      if (!result_.get_dependent_columns().empty()) {
        if (OB_FAIL(fallback_to_complete_refresh("missing fast refresh dependent columns in the SELECT list"))) {
          LOG_WARN("failed to fallback to complete refresh", K(ret));
        }
      } else if (OB_FAIL(check_fast_refresh_dep_consistency(is_dep_consistent))) {
        LOG_WARN("failed to check fast refresh dep consistency", K(ret));
      } else if (is_dep_consistent) {
        need_fallback = false;
      } else if (OB_FAIL(fallback_to_complete_refresh("mview dependencies have changed since creation"))) {
        LOG_WARN("failed to fallback to complete refresh", K(ret));
      }
    }
    if (OB_FAIL(ret) || need_fallback) {
    } else if (OB_FAIL(result_.set_refresh_type(refresh_type))) {
      LOG_WARN("failed to set final refresh type", K(ret), K(refresh_type));
    }
  } else if (OB_FAIL(result_.set_refresh_type(refresh_type))) {
    LOG_WARN("failed to set final refresh type", K(ret), K(refresh_type));
  }
  return ret;
}

int ObMVChecker::check_nested_mview_refresh_type(bool &can_all_mviews_fast_refresh)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> dep_mview_ids;
  can_all_mviews_fast_refresh = true;
  if (OB_FAIL(collect_dep_mview_ids_from_stmt(stmt_, dep_mview_ids))) {
    LOG_WARN("failed to collect dependency mview ids from stmt", K(ret));
  } else if (dep_mview_ids.empty()) {
    // no nested mview dependency
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(share::schema::ObMViewInfo::check_all_mviews_fast_refreshable(*GCTX.sql_proxy_,
                                                                                   params_.get_tenant_id(),
                                                                                   dep_mview_ids,
                                                                                   can_all_mviews_fast_refresh))) {
    LOG_WARN("failed to check mview refresh methods", K(ret), K(params_.get_tenant_id()), K(dep_mview_ids));
  }
  return ret;
}

int ObMVChecker::fallback_to_complete_refresh(const char *error_note)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(error_note)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid error note", K(ret));
  } else {
    LOG_TRACE(error_note, K(params_.mv_schema_.get_table_name()), K(result_.get_dependent_columns()));
    if (OB_FAIL(result_.set_refresh_type(OB_MV_COMPLETE_REFRESH))) {
      LOG_WARN("failed to set complete refresh type", K(ret));
    } else {
      result_.log_error_note(error_note);
    }
  }
  return ret;
}

int ObMLogRequiredColumnsInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(required_columns_map_.create(16, lib::ObMemAttr(MTL_ID(), "MLogReqCol")))) {
    LOG_WARN("failed to create required columns map", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObMLogRequiredColumnsInfo::reset()
{
  if (is_inited_) {
    FOREACH(it, required_columns_map_) {
      common::hash::ObHashSet<uint64_t> *required_columns = it->second;
      if (OB_NOT_NULL(required_columns)) {
        required_columns->destroy();
      }
    }
  }
  required_columns_map_.destroy();
  inner_alloc_.reset();
  is_inited_ = false;
}

int ObMLogRequiredColumnsInfo::get_or_create_required_columns(const uint64_t base_object_id,
                                                              common::hash::ObHashSet<uint64_t> *&required_columns)
{
  int ret = OB_SUCCESS;
  required_columns = NULL;
  if (!is_inited_ && OB_FAIL(init())) {
    LOG_WARN("failed to init mlog required columns", K(ret));
  } else if (OB_SUCCESS != (ret = required_columns_map_.get_refactored(base_object_id,
                                                                        required_columns))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      required_columns = OB_NEWx(common::hash::ObHashSet<uint64_t>, (&inner_alloc_));
      if (OB_ISNULL(required_columns)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_FAIL(required_columns->create(16, lib::ObMemAttr(MTL_ID(), "MLogReqCol")))) {
        LOG_WARN("failed to create required columns", K(ret));
      } else if (OB_FAIL(required_columns_map_.set_refactored(base_object_id, required_columns))) {
        LOG_WARN("failed to set required columns map", K(ret));
      }
    } else {
      LOG_WARN("failed to get required columns map", K(ret));
    }
  } else if (OB_ISNULL(required_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(base_object_id));
  }
  return ret;
}

int ObMLogRequiredColumnsInfo::record_required_columns(const uint64_t base_object_id,
                                                       const ObIArray<uint64_t> &col_ids)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> *required_columns = NULL;
  if (OB_FAIL(get_or_create_required_columns(base_object_id, required_columns))) {
    LOG_WARN("failed to get required columns", K(ret), K(base_object_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
    if (OB_SUCCESS != (ret = required_columns->set_refactored(col_ids.at(i)))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to set required columns", K(ret));
      }
    }
  }
  return ret;
}

int ObMLogRequiredColumnsInfo::merge_from(const ObMLogRequiredColumnsInfo &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_inited_) {
    // do nothing
  } else {
    FOREACH_X(it, other.required_columns_map_, OB_SUCC(ret)) {
      const uint64_t base_object_id = it->first;
      const common::hash::ObHashSet<uint64_t> *src_required_columns = it->second;
      common::hash::ObHashSet<uint64_t> *dst_required_columns = NULL;
      if (OB_ISNULL(src_required_columns)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(src_required_columns));
      } else if (OB_FAIL(get_or_create_required_columns(base_object_id, dst_required_columns))) {
        LOG_WARN("failed to get destination required columns", K(ret), K(base_object_id));
      } else {
        FOREACH_X(it2, *src_required_columns, OB_SUCC(ret)) {
          if (OB_SUCCESS != (ret = dst_required_columns->set_refactored(it2->first))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to merge required column", K(ret), K(base_object_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMLogRequiredColumnsInfo::convert_to_required_columns_infos(ObIArray<obrpc::ObMVRequiredColumnsInfo>
                                                                   &required_columns_infos) const
{
  int ret = OB_SUCCESS;
  required_columns_infos.reset();
  if (!is_inited_) {
    // do nothing
  } else {
    FOREACH_X(it, required_columns_map_, OB_SUCC(ret)) {
      const uint64_t base_object_id = it->first;
      const common::hash::ObHashSet<uint64_t> *required_column_set = it->second;
      ObSEArray<uint64_t, 16> required_columns;
      if (OB_ISNULL(required_column_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), K(required_column_set));
      } else {
        FOREACH_X(it2, *required_column_set, OB_SUCC(ret)) {
          const uint64_t column_id = it2->first;
          if (OB_FAIL(required_columns.push_back(column_id))) {
            LOG_WARN("failed to push back", KR(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(required_columns_infos.push_back(
                     obrpc::ObMVRequiredColumnsInfo(base_object_id, required_columns)))) {
        LOG_WARN("failed to push back", KR(ret));
      }
    }
  }

  return ret;
}

} // end of namespace sql
}//end of namespace oceanbase
