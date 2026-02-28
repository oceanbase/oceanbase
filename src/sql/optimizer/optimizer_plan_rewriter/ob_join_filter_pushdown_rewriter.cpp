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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/optimizer_plan_rewriter/ob_join_filter_pushdown_rewriter.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_expand.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_opt_selectivity.h"

namespace oceanbase
{
namespace sql
{

struct NoSubqueryPredicate {
  int operator()(JoinFilterMetaInfo* rtf, bool &result) const {
    int ret = OB_SUCCESS;
    result = false;
    if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_)) {
      result = false;
    } else {
      // Return true if use_expr does NOT have the CNT_SUB_QUERY flag (keep it)
      result = !rtf->use_expr_->has_flag(CNT_SUB_QUERY) && !rtf->use_expr_->has_flag(CNT_ONETIME);
    }
    return ret;
  }
};

// Helper functor for filtering out expressions that contain aggregate functions
struct NoAggPredicate {
  int operator()(JoinFilterMetaInfo* rtf, bool &result) const {
    int ret = OB_SUCCESS;
    result = false;
    if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_)) {
      result = false;
    } else {
      // Return true if use_expr does NOT have the CNT_AGG flag (keep it)
      result = !rtf->use_expr_->has_flag(CNT_AGG);
    }
    return ret;
  }
};

// Helper functor for filtering out expressions that contain window functions
struct NoWindowPredicate {
  int operator()(JoinFilterMetaInfo* rtf, bool &result) const {
    int ret = OB_SUCCESS;
    result = false;
    if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_)) {
      result = false;
    } else {
      // Return true if use_expr does NOT have the CNT_WINDOW_FUNC flag (keep it)
      result = !rtf->use_expr_->has_flag(CNT_WINDOW_FUNC);
    }
    return ret;
  }
};

// Helper functor for filtering out special expressions
struct NoSpecialExprPredicate {
  int operator()(JoinFilterMetaInfo* rtf, bool &result) const {
    int ret = OB_SUCCESS;
    result = false;
    if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_) || OB_ISNULL(rtf->create_expr_)) {
      result = false;
    } else {
      result = !rtf->use_expr_->get_relation_ids().is_empty() && !rtf->use_expr_->is_nested_expr() && !rtf->create_expr_->is_nested_expr()
                && rtf->use_expr_->is_deterministic() && !rtf->use_expr_->has_flag(CNT_ROWNUM);
    }
    return ret;
  }
};

// JoinFilterPushdownChecker
    int JoinFilterPushdownChecker::visit_node(ObLogicalOperator * plannode,
      RewriterContext* context,
      RewriterResult*& result)
    {
      UNUSED(context);
      UNUSED(result);
      int ret = OB_SUCCESS;
      if (is_parallel_plan_ && has_join_) {
      } else {
        // if this node is parallel. set parallel to true
        if (OB_NOT_NULL(plannode) && ObGlobalHint::DEFAULT_PARALLEL < plannode->get_parallel()) {
          is_parallel_plan_ = true;
        }
        if (OB_SUCC(ret)) {
          RewriterResult *dummy_result = NULL;
          ret = this->visit_children(plannode, dummy_result);
        }
      }
      return ret;
    }

    int JoinFilterPushdownChecker::visit_join(ObLogJoin * joinnode,
                                              RewriterContext* context,
                                              RewriterResult*& result)
    {
      UNUSED(context);
      UNUSED(result);
      int ret = OB_SUCCESS;
      if (is_parallel_plan_ && has_join_) {
      } else {
        // if this node is parallel. set parallel to true
        if (OB_NOT_NULL(joinnode) && HASH_JOIN == joinnode->get_join_algo() &&
            !joinnode->get_equal_join_conditions().empty()) {
          has_join_ = true;
        }
        if (OB_SUCC(ret)) {
          RewriterResult *dummy_result = NULL;
          ret = visit_node(joinnode, NULL, dummy_result);
        }
      }
      return ret;
    }

// JoinFilterPruneAndUpdateRewriter
    int JoinFilterPruneAndUpdateRewriter::init(common::ObIArray<JoinFilterMetaInfo*> &valid_rtfs,
                                               common::ObIArray<JoinUseMetaInfo*> &consumer_table_infos)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(valid_rtfs_.assign(valid_rtfs))) {
        LOG_WARN("failed to assign valid rtf", K(ret));
      } else if (OB_FAIL(consumer_table_infos_.assign(consumer_table_infos))) {
        LOG_WARN("failed to assign consumer table infos", K(ret));
      } else if (OB_FAIL(valid_msg_ids_set_.create(std::max(valid_rtfs.count(), 16L)))) {
        LOG_WARN("failed to create valid msg ids set", K(ret));
      } else if (OB_FAIL(valid_table_ids_set_.create(std::max(consumer_table_infos.count(), 16L)))) {
        LOG_WARN("failed to create valid table ids set", K(ret));
      } else if (OB_FAIL(valid_producer_cache_set_.create(std::max(consumer_table_infos.count(), 16L)))) {
        LOG_WARN("failed to create valid producer cache set", K(ret));
      }
      return ret;
    }

    // do default rewrite
    int JoinFilterPruneAndUpdateRewriter::visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(context);
      UNUSED(result);
      RewriterResult *dummy_result = NULL;
      if (OB_FAIL((this->visit_children(plannode, dummy_result)))) {
        LOG_WARN("failed to check for children", K(ret));
      }
      return ret;
    }

// FilterCheckerVisitor
    int JoinFilterPruneAndUpdateRewriter::FilterCheckerVisitor::visit_temp_table_access(ObLogTempTableAccess * temp_table_access, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(context);
      UNUSED(result);
      // if has filter then true else read from cache set
      if (OB_ISNULL(temp_table_access)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("temp table access is null", K(ret));
      } else if (!is_valid_) {
        bool has_filters = !temp_table_access->get_filter_exprs().empty();
        if (has_filters) {
          OPT_TRACE("temp table has valid filters", temp_table_access->get_op_id());
          is_valid_ = true;
        } else {
          bool exists = false;
          if (OB_FAIL(valid_producer_cache_set_.exist_refactored(temp_table_access->get_temp_table_id()))) {
            if (OB_HASH_EXIST == ret) {
              exists = true;
              ret = OB_SUCCESS;
            } else if (OB_HASH_NOT_EXIST == ret) {
              exists = false;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to check table id in set", K(ret));
            }
          }
          if (OB_SUCC(ret) && exists) {
            OPT_TRACE("temp table has valid runtime filters", temp_table_access->get_op_id());
            is_valid_ = true;
          }
        }
      }
      return ret;
    }

    // if limit, make result true and stop visiting children
    int JoinFilterPruneAndUpdateRewriter::FilterCheckerVisitor::visit_limit(ObLogLimit * limitnode, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(context);
      UNUSED(result);
      if (OB_ISNULL(limitnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("limitnode is null", K(ret));
      } else if (!is_valid_) {
        OPT_TRACE("find a limit", limitnode->get_op_id());
        is_valid_ = true;
      }
      return ret;
    }

    // if is topn, make result true and stop visiting children
    int JoinFilterPruneAndUpdateRewriter::FilterCheckerVisitor::visit_sort(ObLogSort * sortnode, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(context);
      UNUSED(result);
      if (OB_ISNULL(sortnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sortnode is null", K(ret));
      } else if (!is_valid_ && (sortnode->is_topn_op() || !sortnode->get_filter_exprs().empty())) {
        OPT_TRACE("find a top or sort with filter", sortnode->get_op_id());
        is_valid_ = true;
      } else if (!is_valid_) {
        // or with children result
        RewriterResult*child_result = NULL;
        if (OB_FAIL(this->visit_children(sortnode, child_result))) {
          LOG_WARN("failed to visit children", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::FilterCheckerVisitor::visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(context);
      if (OB_ISNULL(plannode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plannode is null", K(ret));
      } else if (!is_valid_) {
        // Check if current operator has filters
        if (!plannode->get_filter_exprs().empty()) {
          OPT_TRACE("find a node with filter", plannode->get_op_id());
          is_valid_ = true;
        } else {
          // Initialize result to false, then check children
          RewriterResult *child_result = NULL;
          for (int64_t i = 0; OB_SUCC(ret) && !is_valid_ && i < plannode->get_num_of_child(); ++i) {
            ObLogicalOperator* child = plannode->get_child(i);
            if (OB_NOT_NULL(child)) {
              if (OB_FAIL(this->visit_child(child, child_result))) {
                LOG_WARN("failed to visit child", K(ret), K(i));
              }
            }
          }
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::FilterCheckerVisitor::visit_table_scan(ObLogTableScan * scannode, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(context);
      if (OB_ISNULL(scannode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scannode is null", K(ret));
      } else if (!is_valid_) {
        // Check if table_id is in valid_table_ids_set_
        // it is not enough to check this
        // todo check the rtf is from upper above the check point
        bool has_filters = !scannode->get_filter_exprs().empty();
        uint64_t table_id = scannode->get_table_id();
        bool exists = false;
        if (has_filters) {
        } else if (OB_FAIL(valid_table_ids_set_.exist_refactored(table_id))) {
          if (OB_HASH_EXIST == ret) {
            exists = true;
            ret = OB_SUCCESS;
          } else if (OB_HASH_NOT_EXIST == ret) {
            exists = false;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to check table id in set", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          is_valid_ = has_filters || exists;
          if (is_valid_) {
            OPT_TRACE("find a tablescan with filter", scannode->get_op_id(), has_filters, exists);
          }
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::check_has_filters(ObLogicalOperator *op, bool &has_filters)
    {
      int ret = OB_SUCCESS;
      has_filters = false;
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("operator is null", K(ret));
      } else {
        FilterCheckerVisitor visitor(valid_table_ids_set_, valid_producer_cache_set_);
        RewriterResult *result = NULL;
        if (OB_FAIL(visitor.visit(op, NULL, result))) {
          LOG_WARN("failed to check filters", K(ret));
        } else {
          has_filters = visitor.is_valid();
          OPT_TRACE("check has filters for op", op->get_op_id(), has_filters);
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::visit_temp_table_insert(ObLogTempTableInsert * temp_table_insert, RewriterContext* context, RewriterResult*& result)
    {
      int ret = OB_SUCCESS;
      // call visit node first
      // after that do a check and store in map
      bool has_filters = false;
      if (OB_ISNULL(temp_table_insert)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("temp table insert, context or result is null", K(ret));
      } else if (OB_FAIL(this->visit_node(temp_table_insert, context, result))) {
        LOG_WARN("failed to visit temp table insert", K(ret));
      } else if (check_has_filters(temp_table_insert, has_filters)) {
      } else if (has_filters) {
        if (OB_FAIL(valid_producer_cache_set_.set_refactored(temp_table_insert->get_temp_table_id()))) {
          LOG_WARN("failed to set refactored to valid producer cache set", K(ret));
        }
      }
      return ret;
    }

    // each join can only create one partition join filter
    int JoinFilterPruneAndUpdateRewriter::prune_partition_rtfs(ObIArray<JoinFilterInfo*> &joinfilterinfos)
    {
      int ret = OB_SUCCESS;
      int64_t partition_rtfs_count = 0;
      bool has_hint_force_part_filter = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < joinfilterinfos.count(); ++i) {
        JoinFilterInfo* info = joinfilterinfos.at(i);
        if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("join filter info is null", K(ret));
        } else if (info->need_partition_join_filter_) {
          partition_rtfs_count++;
          if (info->force_part_filter_ != NULL) {
            has_hint_force_part_filter = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (partition_rtfs_count > 1) {
        // do a for loop on all join filters
        // if has hint and found one, set found to true
        // otherwise, set found to true when
        bool found = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < joinfilterinfos.count(); ++i) {
          JoinFilterInfo *info = joinfilterinfos.at(i);
          if (OB_ISNULL(info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("join filter info is null", K(ret));
          } else if (!found && info->need_partition_join_filter_) {
            if (has_hint_force_part_filter && info->force_part_filter_ != NULL) {
              found = true;
            } else if (!has_hint_force_part_filter) {
              found = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (found && info->need_partition_join_filter_) {
            info->need_partition_join_filter_ = false;
          }
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::visit_join(ObLogJoin * joinnode, RewriterContext* context, RewriterResult*& result)
    {
      UNUSED(context);
      int ret = OB_SUCCESS;
      if (OB_ISNULL(joinnode) || OB_ISNULL(joinnode->get_child(0)) || OB_ISNULL(joinnode->get_child(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join node or children is null", K(ret));
      } else {
        // do prune and update for left child first
        RewriterResult *dummy_result = NULL;
        if (OB_FAIL(this->dispatch_visit(joinnode->get_child(0), NULL, dummy_result))) {
          LOG_WARN("failed to visit left child", K(ret));
        } else {
          bool has_valid_producer = false;
          common::ObSEArray<std::pair<JoinFilterMetaInfo*, JoinUseMetaInfo*>, 4> valid_rtfs_for_this_join;
          for (int64_t i = 0; OB_SUCC(ret) && i < valid_rtfs_.count(); ++i) {
            JoinFilterMetaInfo* rtf = valid_rtfs_.at(i);
            JoinUseMetaInfo* use_meta_info = consumer_table_infos_.at(i);
            if (OB_ISNULL(rtf) || OB_ISNULL(use_meta_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("rtf or use meta info is null", K(ret));
            } else if (rtf->origin_join_op_ == joinnode) {
                valid_rtfs_for_this_join.push_back(std::make_pair(rtf, use_meta_info));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(joinnode->get_plan())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("join node plan is null", K(ret));
          } else if (valid_rtfs_for_this_join.count() > 0) {
            // for each table id, create a joinfilterinfo
            ObSqlArray<JoinFilterInfo*> joinfilterinfos(joinnode->get_plan()->get_allocator());
            common::ObSEArray<JoinUseMetaInfo*, 4> current_use_meta_infos;
            ObSEArray<ObRawExpr*, 4> left_exprs;
            ObSEArray<ObRawExpr*, 4> right_exprs;
            if (OB_FAIL(check_has_filters(joinnode->get_child(0), has_valid_producer))) {
              LOG_WARN("failed to check has valid producer", K(ret));
            } else if (OB_FAIL(ObOptimizerUtil::extract_equal_join_conditions(joinnode->get_equal_join_conditions(),
                                                                              joinnode->get_child(0)->get_table_set(),
                                                                              left_exprs,
                                                                              right_exprs))) {
              LOG_WARN("failed format equal join conditions", K(ret));
            } else {
              int64_t i = 0;
              while (OB_SUCC(ret) && i < valid_rtfs_for_this_join.count()) {
                std::pair<JoinFilterMetaInfo*, JoinUseMetaInfo*> rtf = valid_rtfs_for_this_join.at(i);
                JoinUseMetaInfo* current_use_meta_info = rtf.second;
                common::ObSEArray<JoinFilterMetaInfo*, 4> current_valid_rtfs;
                uint64_t current_table_id = current_use_meta_info->table_id_;
                while (OB_SUCC(ret) && i < valid_rtfs_for_this_join.count()) {
                  rtf = valid_rtfs_for_this_join.at(i);
                  if (rtf.second->table_id_ == current_table_id) {
                    current_valid_rtfs.push_back(rtf.first);
                    i++;
                  } else {
                    break;
                  }
                }
                // create a joinfilterinfo for this group
                if (OB_SUCC(ret) && current_valid_rtfs.count() > 0) {
                  if (OB_FAIL(JoinFilterPruneAndUpdateRewriter::append_join_filter_info(joinnode,
                                                                                        current_valid_rtfs,
                                                                                        current_use_meta_info,
                                                                                        joinfilterinfos,
                                                                                        left_exprs))) {
                    LOG_WARN("failed to make joinfilterinfo", K(ret));
                  } else if (OB_FAIL(current_use_meta_infos.push_back(current_use_meta_info))) {
                    LOG_WARN("failed to push back use meta info", K(ret));
                  }
                }
              }
            }
            // set partition-wise info
            LOG_DEBUG("begin to check for join", K(joinnode->get_op_id()), K(has_valid_producer));
            OPT_TRACE("=========start to generate rtf for join===========", joinnode->get_op_id(), has_valid_producer);
            ObSqlArray<JoinFilterInfo*> final_joinfilterinfos(joinnode->get_plan()->get_allocator());
            if (OB_SUCC(ret)) {
              for (int i = 0; OB_SUCC(ret) && i < joinfilterinfos.count(); ++i) {
                JoinFilterInfo *info = joinfilterinfos.at(i);
                JoinUseMetaInfo* current_use_meta_info = current_use_meta_infos.at(i);
                if (OB_ISNULL(current_use_meta_info) || OB_ISNULL(info)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("current use meta info is null", K(ret));
                } else {
                  double join_filter_selectivity = 1.0;
                  // log all the info
                  OPT_TRACE("========check join filter",
                            ", joinnode_id=", joinnode->get_op_id(),
                            ", table_op_id=", current_use_meta_info->table_op_id_,
                            ", use_expr=", info->rexprs_,
                            ", create_expr=", info->lexprs_);
                  if (OB_FAIL(set_force_hint_infos(joinnode, info, current_use_meta_info))) {
                    LOG_WARN("failed to set force hint infos", K(ret));
                  } else if (OB_FAIL(check_partition_join_filter_valid(joinnode->get_join_distributed_method(),
                                                                       joinnode,
                                                                       info,
                                                                       current_use_meta_info))) {
                    LOG_WARN("failed to check partition join filter valid", K(ret));
                  } else if (OB_FAIL(check_normal_join_filter_valid(joinnode, info, current_use_meta_info, has_valid_producer))) {
                    LOG_WARN("failed to check normal join filter valid", K(ret));
                  } else if (info->can_use_join_filter_ || info->need_partition_join_filter_) {
                    // update valid_table_ids_set_ only when can use a normal join filter
                    if (info->can_use_join_filter_) {
                      if (OB_FAIL(valid_table_ids_set_.set_refactored(info->table_id_))) {
                        LOG_WARN("failed to set refactored to valid table ids", K(ret));
                      } else if (OB_FAIL(this->calc_join_filter_selectivity(current_use_meta_info, info, joinnode, join_filter_selectivity))) {
                        LOG_WARN("failed to calc join filter selectivity", K(ret));
                      }
                    }
                    if (OB_FAIL(ret)) {
                    } else {
                      info->join_filter_selectivity_ = join_filter_selectivity;
                      final_joinfilterinfos.push_back(info);
                      OPT_TRACE("rtf is added to join, joinnode_id=", joinnode->get_op_id(),
                                ", create_expr=", info->lexprs_,
                                ", use_expr=", info->rexprs_,
                                ", table_op_id=", current_use_meta_info->table_op_id_,
                                ", selectivity=", info->join_filter_selectivity_,
                                ", can_use_join_filter_=", info->can_use_join_filter_,
                                ", has_valid_producer=", has_valid_producer,
                                ", need_partition_join_filter_=", info->need_partition_join_filter_,
                                ", force_hint=", info->force_filter_ != NULL,
                                ", force_part_hint=", info->force_part_filter_ != NULL);
                      LOG_DEBUG("rtf is added to join", KPC(info));
                    }
                  }
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(joinnode->append_join_filter_infos(final_joinfilterinfos))) {
                LOG_WARN("failed to append join filter infos", K(ret));
              }
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        RewriterResult *dummy_result2 = NULL;
        if (OB_FAIL(this->dispatch_visit(joinnode->get_child(1), NULL, dummy_result2))) {
          LOG_WARN("failed to visit right child", K(ret));
        }
      }
      return ret;
    }
    // refer to ObJoinOrder::fill_join_filter_info
    // selecticity context
    int JoinFilterPruneAndUpdateRewriter::fill_join_filter_info(JoinUseMetaInfo* user_info,
                                                                 JoinFilterInfo*& join_filter_info)
    {
      int ret = OB_SUCCESS;
      const ObLogPlan *plan = NULL;
      if (OB_ISNULL(user_info) || OB_ISNULL(join_filter_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null user info", K(ret));
      } else if (OB_ISNULL(user_info->selectivity_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null selectivity ctx", K(ret));
      } else if (OB_ISNULL(plan = user_info->selectivity_ctx_->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null plan", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                               *user_info->selectivity_ctx_,
                                                               join_filter_info->rexprs_,
                                                               join_filter_info->row_count_,
                                                               join_filter_info->right_distinct_card_))) {
        LOG_WARN("failed to calc distinct", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::is_columns_contain_pkey(plan->get_basic_table_metas(),
                                                                   join_filter_info->rexprs_,
                                                                   join_filter_info->is_right_contain_pk_,
                                                                   join_filter_info->is_right_union_pk_))) {
        LOG_WARN("failed to check is columns contain pkey", K(ret));
      } else if (join_filter_info->is_right_contain_pk_ && join_filter_info->is_right_union_pk_) {
        const OptTableMeta *table_meta = user_info->update_table_meta_;
        if (OB_NOT_NULL(table_meta)) {
          join_filter_info->right_distinct_card_ = std::max(1.0, table_meta->get_rows());
        }
        if (OB_NOT_NULL(table_meta = user_info->basic_table_meta_)) {
          join_filter_info->right_origin_rows_ = std::max(1.0, table_meta->get_rows());
        }
      } else {
        const OptTableMeta *table_meta = user_info->basic_table_meta_;
        if (OB_NOT_NULL(table_meta)) {
          join_filter_info->right_origin_rows_ = std::max(1.0, table_meta->get_rows());
        }
      }
      return ret;
    }

    // refer to ObJoinOrder::calc_join_filter_sel_for_pk_join_fk
    int JoinFilterPruneAndUpdateRewriter::calc_join_filter_sel_for_pk_join_fk(ObLogJoin * joinnode,
                                                                              JoinFilterInfo* info,
                                                                              double &join_filter_selectivity,
                                                                              bool &is_pk_join_fk)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(joinnode) || OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null join node", K(ret));
      } else {
        bool is_valid = false;
        ObLogPlan *plan = joinnode->get_plan();
        bool left_contain_pk = false;
        bool is_left_union_pk = false;
        uint64_t left_table_id = OB_INVALID_ID;
        double pk_origin_rows = 1.0;
        double left_ndv = 1.0;
        if (OB_ISNULL(plan) || OB_ISNULL(joinnode->get_child(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null plan", K(ret));
        } else if (info->right_distinct_card_ <= OB_DOUBLE_EPSINON) {
          // do nothing
        } else if (OB_FAIL(ObOptSelectivity::is_columns_contain_pkey(plan->get_update_table_metas(),
                                                                     info->lexprs_,
                                                                     left_contain_pk,
                                                                     is_left_union_pk,
                                                                     &left_table_id))) {
          LOG_WARN("failed to check is columns contain pkey", K(ret));
        } else if (!info->is_right_contain_pk_ && left_contain_pk && is_left_union_pk) {
          pk_origin_rows = plan->get_basic_table_metas().get_rows(left_table_id);
          if (pk_origin_rows > OB_DOUBLE_EPSINON) {
            is_valid = true;
            if (OB_FAIL(plan->get_selectivity_ctx().get_ambient_card(left_table_id, left_ndv))) {
              LOG_WARN("failed to get ambient card", K(ret));
            } else {
              join_filter_selectivity = left_ndv / std::min(pk_origin_rows, info->right_distinct_card_);
            }
          }
        } else if (info->is_right_union_pk_ && info->is_right_contain_pk_ && !left_contain_pk && OB_INVALID_ID != left_table_id) {
          pk_origin_rows = info->right_origin_rows_;
          is_valid = true;
          double fk_origin_rows = plan->get_basic_table_metas().get_rows(left_table_id);
          if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                           plan->get_selectivity_ctx(),
                                                           info->lexprs_,
                                                           joinnode->get_child(0)->get_card(),
                                                           left_ndv))) {
            LOG_WARN("failed to calculate distinct", K(ret), K(left_table_id), KPC(info));
          } else {
            double fk_ndv = ObOptSelectivity::scale_distinct(joinnode->get_child(0)->get_card(), fk_origin_rows, pk_origin_rows);
            left_ndv = std::min(left_ndv, fk_ndv);
            join_filter_selectivity = left_ndv / info->right_distinct_card_;
          }
        }
      }
      return ret;
    }
    // refer to ObJoinOrder::calc_join_filter_selectivity
    int JoinFilterPruneAndUpdateRewriter::calc_join_filter_selectivity(JoinUseMetaInfo* user_info,
                                                                       JoinFilterInfo* join_filter_info,
                                                                       ObLogJoin * joinnode,
                                                                       double &join_filter_selectivity)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(user_info) || OB_ISNULL(joinnode) || OB_ISNULL(join_filter_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (!join_filter_info->can_use_join_filter_) {
        join_filter_selectivity = 1.0;
      } else {
        double left_distinct_card = 1.0;
        double right_distinct_card = 1.0;
        join_filter_selectivity = 1.0;
        bool is_pk_join_fk = false;
        bool est_enhance_enable = true;
        ObLogPlan *plan = joinnode->get_plan();
        if (OB_ISNULL(plan) || OB_ISNULL(joinnode->get_child(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null plan", K(ret));
        } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                                plan->get_selectivity_ctx(),
                                                                join_filter_info->lexprs_,
                                                                joinnode->get_child(0)->get_card(),
                                                                left_distinct_card,
                                                                est_enhance_enable))) {
          LOG_WARN("failed to calc distinct", K(ret));
        } else if (est_enhance_enable &&
          OB_FAIL(calc_join_filter_sel_for_pk_join_fk(joinnode, join_filter_info, join_filter_selectivity, is_pk_join_fk))) {
          LOG_WARN("failed to calc pk join fk join filter sel", K(ret));
        } else if (is_pk_join_fk) {
          // do nothing
        } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                                plan->get_selectivity_ctx(),
                                                                join_filter_info->lexprs_,
                                                                joinnode->get_child(0)->get_card(),
                                                                left_distinct_card,
                                                                est_enhance_enable))) {
          LOG_WARN("failed to calc distinct", K(ret));
        } else {
          join_filter_selectivity = left_distinct_card / join_filter_info->right_distinct_card_;
        }
        if (OB_SUCC(ret)) {
          if (join_filter_selectivity < 0) {
            join_filter_selectivity = 0;
          } else if (join_filter_selectivity > 0.9) {
            join_filter_selectivity = 0.9;
          }
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::check_normal_join_filter_valid(ObLogJoin * joinnode,
      JoinFilterInfo *info,
      JoinUseMetaInfo* &current_use_meta_info,
      bool has_valid_producer)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(current_use_meta_info) || OB_ISNULL(joinnode) || OB_ISNULL(info) ||
          OB_ISNULL(joinnode->get_child(0)) || OB_ISNULL(current_use_meta_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (info->can_use_join_filter_ && info->force_filter_ == NULL) {
        if (!has_valid_producer || (max_creater_row_count_ < 0 || joinnode->get_child(0)->get_card() > max_creater_row_count_)) {
          OPT_TRACE("cannot use join normal filter due to producer no filter or card is too large",
            ", has_valid_producer=", has_valid_producer,
            ", producer_card=", joinnode->get_child(0)->get_card(),
            ", force by hint=", info->force_filter_ != NULL);
          info->can_use_join_filter_ = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (info->force_filter_ == NULL && current_use_meta_info->use_das_) {
        OPT_TRACE("cannot use join normal filter due use table is das",
          ", force by hint=", info->force_filter_ != NULL);
        info->can_use_join_filter_ = false;
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::find_through_shuffle(ObLogicalOperator * root,
                                                               ObLogicalOperator*& right_path)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null plan", K(ret));
      } else if (log_op_def::LOG_EXCHANGE == root->get_type()) {
        if (OB_FAIL(SMART_CALL(find_through_shuffle(root->get_child(0), right_path)))) {
          LOG_WARN("failed to find the fist non shuffle node");
        }
      } else {
        right_path = root;
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::can_reuse_calc_id_expr(const DistAlgo join_dist_algo,
      ObLogJoin * joinnode,
      JoinFilterInfo *info,
      bool &can_reuse)
    {
      int ret = OB_SUCCESS;
      can_reuse = false;
      ObLogicalOperator* right_path = NULL;
      if (OB_ISNULL(joinnode) ||
          OB_ISNULL(info) ||
          OB_ISNULL(info->sharding_) ||
          OB_ISNULL(joinnode->get_child(0)) ||
          OB_ISNULL(joinnode->get_child(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null plan", K(ret));
      } else {
        bool sharding_match = (joinnode->get_child(1)->get_strong_sharding() == info->sharding_);
        bool local_scan = info->in_current_dfo_;
        bool has_shuffle = (DIST_PARTITION_NONE == join_dist_algo || DIST_PARTITION_HASH_LOCAL == join_dist_algo);
        bool right_has_shuffle = DIST_PARTITION_HASH_LOCAL == join_dist_algo;
        // when right hash shuffle. part join filter will alway do a broadcast
        // in such a case, sharding match or relid match is sufficient
        if (has_shuffle && sharding_match) {
          can_reuse = true;
        } else if (has_shuffle) {
          ObLogExchange *exch_op = NULL;
          bool found = false;
          if (OB_FAIL(joinnode->find_pkey_exchange_out(joinnode->get_child(0), exch_op, found))) {
            LOG_WARN("failed to find pkey exchange out for partition join filter", K(ret));
          } else if (!found || OB_ISNULL(exch_op) || OB_ISNULL(exch_op->get_calc_part_id_expr())
                     || OB_ISNULL(exch_op->get_child(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to find pkey exchange out for partition join filter", K(ret));
          } else if (right_has_shuffle && OB_FAIL(find_through_shuffle(joinnode->get_child(1), right_path))) {
            LOG_WARN("failed to find right patch", K(ret));
          } else if (right_path != NULL && right_path->get_strong_sharding() == info->sharding_) {
            sharding_match = true;
            can_reuse = true;
          } else if (local_scan) {
            ObRawExpr* calc_id_expr = exch_op->get_calc_part_id_expr();
            if (calc_id_expr->get_expr_type() == T_FUN_SYS_CALC_TABLET_ID &&
                calc_id_expr->get_ref_table_id() == info->index_id_) {
              can_reuse = true;
            } else {
              OPT_TRACE("can not reuse pkey expr as target table not match");
            }
          }
        } else {
          OPT_TRACE("can not reuse pkey expr as has_shuffle=", has_shuffle, ", sharding_match=", sharding_match);
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::check_partition_join_filter_valid(const DistAlgo join_dist_algo,
      ObLogJoin * joinnode,
      JoinFilterInfo *info,
      JoinUseMetaInfo* &current_use_meta_info)
    {
      int ret = OB_SUCCESS;
      ObSEArray<ObRawExpr*, 8> target_part_keys;
      ObRawExpr *left_calc_part_id_expr = NULL;
      bool match = false;
      bool skip_subpart = false;
      bool can_reuse = false;
      ObLogPlan *plan = NULL;
      if (OB_ISNULL(joinnode) || OB_ISNULL(joinnode->get_plan()) ||
          OB_ISNULL(joinnode->get_child(1)) || OB_ISNULL(info) ||
          OB_ISNULL(info->sharding_) ||
          OB_ISNULL(current_use_meta_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null plan", K(ret));
      } else if (NULL == info->force_part_filter_ && (info->sharding_->get_part_cnt() < min_user_partition_count_)
                 && OB_FAIL(can_reuse_calc_id_expr(join_dist_algo, joinnode, info, can_reuse))) {
        LOG_WARN("failed to check reuse calc id expr", K(ret));
      } else {
        plan = joinnode->get_plan();
        if (!info->need_partition_join_filter_ || DIST_PARTITION_WISE == join_dist_algo) {
          info->need_partition_join_filter_ = false;
          OPT_TRACE("part join filter is disabled as join", joinnode->get_op_id(), "is DIST_PARTITION_WISE");
        } else if (!can_reuse &&
                   NULL == info->force_part_filter_ &&
                  (info->sharding_->get_part_cnt() < min_user_partition_count_)) {
          // print out the reason
          OPT_TRACE("part join filter is disabled as can not share", joinnode->get_op_id(),
                    ", join_dist_algo: ", ob_dist_algo_str(join_dist_algo),
                    ", partition count: ", info->sharding_->get_part_cnt(),
                    ", reuse check: ", can_reuse,
                    ", hint forced: ", NULL == info->force_part_filter_ ? "no" : "yes");
          info->need_partition_join_filter_ = false;
        } else if (info->sharding_->is_single()) {
          OPT_TRACE("part join filter is disabled as sharding is single");
          info->need_partition_join_filter_ = false;
        } else if (!info->need_partition_join_filter_) {
          // do nothing
          OPT_TRACE("cannot use part join filter");
        } else if (OB_FAIL(info->sharding_->get_all_partition_keys(target_part_keys))) {
          LOG_WARN("fail to get all partion keys", K(ret));
        } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(*current_use_meta_info->equal_sets_,
                                                                              info->lexprs_,
                                                                              info->rexprs_,
                                                                              target_part_keys,
                                                                              match))) {
          LOG_WARN("fail to check if match repart", K(ret));
        } else if (!match) {
          // If the join condition includes all level 1 partition key,
          // partition bf can be generated based on the level 1 partition key
          if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(*current_use_meta_info->equal_sets_,
                                                                        info->lexprs_,
                                                                        info->rexprs_,
                                                                        info->sharding_->get_partition_keys(),
                                                                        match))) {
            LOG_WARN("fail to check if match repart", K(ret));
          } else {
            info->skip_subpart_ = true;
            OPT_TRACE("skip_subpart is true");
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!match) {
          info->need_partition_join_filter_ = false;
          OPT_TRACE("part join filter is disabled as sharding does not match, skip_subpart: ", info->skip_subpart_);
        } else if (OB_FAIL(build_join_filter_part_expr(joinnode,
                                                        info->index_id_,
                                                        info->lexprs_,
                                                        info->rexprs_,
                                                        info->sharding_,
                                                        current_use_meta_info->equal_sets_,
                                                        info->calc_part_id_expr_,
                                                        info->skip_subpart_))) {
            LOG_WARN("fail to init bf part expr", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::build_join_filter_part_expr(ObLogJoin * joinnode,
                                                                      const int64_t ref_table_id,
                                                                      const common::ObIArray<ObRawExpr *> &lexprs ,
                                                                      const common::ObIArray<ObRawExpr *> &rexprs,
                                                                      ObShardingInfo *sharding_info,
                                                                      const EqualSets* equal_sets,
                                                                      ObRawExpr *&left_calc_part_id_expr,
                                                                      bool skip_subpart)
    {
      int ret = OB_SUCCESS;
      left_calc_part_id_expr = NULL;
      if (OB_ISNULL(sharding_info) ||
          lexprs.empty() ||
          lexprs.count() != rexprs.count() ||
          sharding_info->get_partition_func().empty() ||
          (OB_ISNULL(session_info_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param", K(ret));
      } else {
        // build right calc part id expr
        ObRawExprCopier copier(expr_factory_);
        // build left calc part id expr
        ObSEArray<ObRawExpr*, 4> repart_exprs;
        ObSEArray<ObRawExpr*, 4> repart_sub_exprs;
        ObSEArray<ObRawExpr*, 4> repart_func_exprs;
        // get repart keys
        for (int i = 0; i < sharding_info->get_partition_keys().count() && OB_SUCC(ret); ++i) {
          bool is_found = false;
          for (int j = 0; j < rexprs.count() && OB_SUCC(ret) && !is_found; ++j) {
            // tablescan equivalent
            if (ObOptimizerUtil::is_expr_equivalent(sharding_info->get_partition_keys().at(i),
                                                    rexprs.at(j),
                                                    *equal_sets)) {
              if (OB_FAIL(repart_exprs.push_back(lexprs.at(j)))) {
                LOG_WARN("failed to push back expr", K(ret));
              } else {
                is_found = true;
              }
            }
          }
        }
        // get subpart keys
        for (int i = 0; i < sharding_info->get_sub_partition_keys().count() &&
             !skip_subpart && OB_SUCC(ret);
             ++i) {
          bool is_found = false;
          for (int j = 0; j < rexprs.count() && OB_SUCC(ret) && !is_found; ++j) {
            if (ObOptimizerUtil::is_expr_equivalent(sharding_info->get_sub_partition_keys().at(i),
                                                    rexprs.at(j),
                                                    *equal_sets)) {
              if (OB_FAIL(repart_sub_exprs.push_back(lexprs.at(j)))) {
                LOG_WARN("failed to push back expr", K(ret));
              } else {
                is_found = true;
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(copier.add_replaced_expr(sharding_info->get_partition_keys(),
                                                    repart_exprs))) {
          LOG_WARN("failed to add replace pair", K(ret));
        } else if (!skip_subpart &&
                   OB_FAIL(copier.add_replaced_expr(sharding_info->get_sub_partition_keys(),
                                                    repart_sub_exprs))) {
          LOG_WARN("failed to add replace pair", K(ret));
        } else {
          ObRawExpr *repart_func_expr = NULL;
          for (int64_t i = 0; OB_SUCC(ret) && i < sharding_info->get_partition_func().count(); i++) {
            repart_func_expr = NULL;
            ObRawExpr *target_func_expr = sharding_info->get_partition_func().at(i);
            if (OB_ISNULL(target_func_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (1 == i && skip_subpart) {
              ObConstRawExpr *const_expr = NULL;
              ObRawExpr *dummy_expr = NULL;
              int64_t const_value = 1;
              if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                                 ObIntType,
                                                                 const_value,
                                                                 const_expr))) {
                LOG_WARN("Failed to build const expr", K(ret));
              } else if (OB_ISNULL(dummy_expr = const_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else if (OB_FAIL(dummy_expr->formalize(session_info_))) {
                LOG_WARN("Failed to formalize a new expr", K(ret));
              } else if (OB_FAIL(repart_func_exprs.push_back(dummy_expr))) {
                LOG_WARN("failed to push back expr", K(ret));
              }
            } else if (OB_FAIL(copier.copy_on_replace(target_func_expr,
                                                        repart_func_expr))) {
              LOG_WARN("failed to copy on replace repart expr", K(ret));
            } else if (OB_FAIL(repart_func_exprs.push_back(repart_func_expr))) {
              LOG_WARN("failed to add repart func expr", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObRawExprUtils::build_calc_tablet_id_expr(expr_factory_,
                                                                    *session_info_,
                                                                    ref_table_id,
                                                                    sharding_info->get_part_level(),
                                                                    repart_func_exprs.at(0),
                                                                    repart_func_exprs.count() > 1 ?
                                                                    repart_func_exprs.at(1) : NULL,
                                                                    left_calc_part_id_expr))) {
              LOG_WARN("fail to init calc part id expr", K(ret));
            } else if (OB_ISNULL(left_calc_part_id_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected calc part id expr", K(ret));
            } else if (skip_subpart) {
              left_calc_part_id_expr->set_partition_id_calc_type(CALC_IGNORE_SUB_PART);
            }
          }
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::set_force_hint_infos(ObLogJoin *&join_op,
                                                                JoinFilterInfo *info,
                                                                JoinUseMetaInfo* &current_use_meta_info)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(join_op) || OB_ISNULL(join_op->get_plan()) ||
          OB_ISNULL(join_op->get_plan()->get_stmt()) ||
          OB_ISNULL(join_op->get_child(0)) ||
          OB_ISNULL(current_use_meta_info) ||
          OB_ISNULL(info) ||
          OB_ISNULL(query_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        bool config_disable = !join_op->get_plan()->get_optimizer_context().enable_runtime_filter();
        bool can_use_normal = false;
        bool can_use_part = false;
        TableItem* view_tableitem = NULL;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(view_tableitem = join_op->get_plan()->get_stmt()->get_table_item_by_id(info->filter_table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table id is invalid", K(ret), K(info->filter_table_id_));
        } else if (OB_FAIL(join_op->get_plan()->get_log_plan_hint().check_join_filter_pushdown_hints(join_op->get_plan()->get_allocator(),
                                                   query_ctx_->get_query_hint(),
                                                   info->filter_table_id_,
                                                   join_op->get_child(0)->get_table_set(),
                                                   view_tableitem,
                                                   current_use_meta_info->tableitem_,
                                                   false,
                                                   config_disable,
                                                   can_use_normal,
                                                   info->force_filter_))) {
          LOG_WARN("failed to check use join filter", K(ret));
        } else if (OB_FAIL(join_op->get_plan()->get_log_plan_hint().check_join_filter_pushdown_hints(join_op->get_plan()->get_allocator(),
                                                           query_ctx_->get_query_hint(),
                                                           info->filter_table_id_,
                                                           join_op->get_child(0)->get_table_set(),
                                                           view_tableitem,
                                                           current_use_meta_info->tableitem_,
                                                           true,
                                                           config_disable,
                                                           can_use_part,
                                                           info->force_part_filter_))) {
          LOG_WARN("failed to check use join filter", K(ret));
        } else {
          info->can_use_join_filter_ &= can_use_normal;
          info->need_partition_join_filter_ &= can_use_part;
          if (!can_use_normal) {
            OPT_TRACE("normal join filter is disabled by hint/config");
            LOG_DEBUG("normal join filter is disabled by hint/config", K(info->force_filter_));
          }
          if (!can_use_part) {
            OPT_TRACE("partition join filter is disabled by hint/config");
            LOG_DEBUG("partition join filter is disabled by hint/config", K(info->force_part_filter_));
          }
        }
      }
      return ret;
    }

    int JoinFilterPruneAndUpdateRewriter::append_join_filter_info(ObLogJoin *&join_op,
                                                                  ObIArray<JoinFilterMetaInfo*> &rtfs,
                                                                  JoinUseMetaInfo* use_meta_info,
                                                                  ObIArray<JoinFilterInfo*> &joinfilters,
                                                                  ObIArray<ObRawExpr*>& all_join_key_left_exprs)
    {
      int ret = OB_SUCCESS;
      // assert rtfs count > 0
      // merge all use exprs in a array
      bool is_current_dfo = true;
      bool is_partition_wise = false;
      void *ptr = NULL;
      JoinFilterInfo* info = NULL;
      JoinFilterMetaInfo* rtf = NULL;
      if (OB_ISNULL(use_meta_info) || OB_ISNULL(join_op) ||
          OB_ISNULL(join_op->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_ISNULL(ptr = join_op->get_plan()->get_allocator().alloc(sizeof(JoinFilterInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate join filter info", K(ret));
      } else {
        info = new (ptr) JoinFilterInfo(join_op->get_plan()->get_allocator());
      }
      if (OB_SUCC(ret) && OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join filter info is null", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rtfs.count(); ++i) {
        rtf = rtfs.at(i);
        if (OB_ISNULL(rtf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else {
          info->rexprs_.push_back(rtf->use_expr_);
          info->lexprs_.push_back(rtf->create_expr_);
          is_current_dfo &= (rtf->pushdown_level_count_ == 0);
          is_partition_wise |= rtf->is_partition_wise_;
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        info->table_id_ = use_meta_info->table_id_;
        info->filter_table_id_ = rtf->filter_table_id_;
        info->index_id_ = use_meta_info->index_id_;
        info->ref_table_id_ = use_meta_info->ref_table_id_;
        info->sharding_ = use_meta_info->sharding_;
        info->row_count_ = use_meta_info->row_count_;
        info->need_partition_join_filter_ = is_partition_wise;
        info->can_use_join_filter_ = true;
        info->in_current_dfo_ = is_current_dfo;
        info->pushdown_filter_table_ = use_meta_info->pushdown_filter_table_;
        info->use_column_store_ = use_meta_info->use_column_store_;
        info->right_origin_rows_ = std::max(1.0, use_meta_info->basic_table_meta_->get_rows());
        double join_filter_selectivity = 1.0;
        if (OB_FAIL(info->all_join_key_left_exprs_.assign(all_join_key_left_exprs))) {
          LOG_WARN("failed to assign expr array");
        } else if (OB_FAIL(this->fill_join_filter_info(use_meta_info, info))) {
          LOG_WARN("failed to fill join filter info", K(ret));
        } else {
          joinfilters.push_back(info);
        }
      }
      return ret;
    }
// JoinFilterPushdownRewriter
    int JoinFilterPushdownRewriter::visit_node(ObLogicalOperator * plannode,
                                                JoinFilterPushdownContext* context,
                                                JoinFilterPushdownResult*& result)
    {
      UNUSED(context);
      UNUSED(result);
      int ret = OB_SUCCESS;
      JoinFilterPushdownResult *dummy_result = NULL;
      if (OB_ISNULL(plannode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(this->visit_children(plannode, dummy_result))) {
        LOG_WARN("failed to visit children", K(ret));
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::rewrite_children(ObLogicalOperator * plannode,
      JoinFilterPushdownContext* context)
    {
      int ret = OB_SUCCESS;
      JoinFilterPushdownResult *dummy_result = NULL;
      if (OB_ISNULL(plannode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL((this->visit_children_with(plannode, context, dummy_result)))) {
        LOG_WARN("failed to visit children", K(ret));
      }
      return ret;
    }
    int JoinFilterPushdownRewriter::visit_material(ObLogMaterial * materialnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(materialnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("material node or context is null", K(ret));
      } else if (OB_FAIL(this->rewrite_children(materialnode, context))) {
        LOG_WARN("failed to rewrite children", K(ret));
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_expand(ObLogExpand * expandnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(expandnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expand node is null", K(ret));
      } else if (context == NULL) {
        // default rewrite
        if (OB_FAIL(this->rewrite_children(expandnode, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      } else {
        // filter by common expr in expand grouping set
        ObGroupingSetInfo *grouping_set_info = expandnode->get_grouping_set_info();
        if (OB_ISNULL(grouping_set_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("grouping set info is null", K(ret));
        } else if (OB_FAIL(context->filter_by_expr_range(grouping_set_info->common_group_exprs_))) {
          LOG_WARN("failed to filter context by expr range", K(ret));
        } else {
          if (OB_FAIL(this->rewrite_children(expandnode, context))) {
            LOG_WARN("failed to rewrite children", K(ret));
          }
        }
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_groupby(ObLogGroupBy * groupbynode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      // filter thoese by group by exprs
      NoAggPredicate predicate;
      if (OB_ISNULL(groupbynode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("groupby node is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        // default rewrite
        if (OB_FAIL(this->rewrite_children(groupbynode, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      } else if (OB_FAIL(context->filter_by_predicate(predicate))) {
        LOG_WARN("failed to filter context by predicate", K(ret));
      } else if (OB_FAIL(context->filter_by_expr_range(groupbynode->get_group_by_exprs()))) {
        LOG_WARN("failed to filter context by expr range", K(ret));
      } else {
        if (OB_FAIL(this->rewrite_children(groupbynode, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_distinct(ObLogDistinct * distinctnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      // filter thoese by group by exprs
      if (OB_ISNULL(distinctnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("distinct node is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        // default rewrite
        if (OB_FAIL(this->rewrite_children(distinctnode, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      } else if (OB_FAIL(context->filter_by_expr_range(distinctnode->get_distinct_exprs()))) {
        LOG_WARN("failed to filter context by expr range", K(ret));
      } else {
        ret = this->rewrite_children(distinctnode, context);
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_window_function(ObLogWindowFunction * windownode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      ObSEArray<ObRawExpr*, 4> partition_by_exprs;
      NoWindowPredicate predicate;
      // filter thoese by group by exprs
      if (OB_ISNULL(windownode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function node is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        // default rewrite
        if (OB_FAIL(this->rewrite_children(windownode, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      } else if (OB_FAIL(windownode->get_win_partition_intersect_exprs(windownode->get_window_exprs(), partition_by_exprs))) {
        LOG_WARN("failed to get window partition intersect exprs", K(ret));
      } else if (OB_FAIL(context->filter_by_predicate(predicate))) {
        LOG_WARN("failed to filter context by expr range", K(ret));
      } else if (OB_FAIL(context->filter_by_expr_range(partition_by_exprs))) {
        LOG_WARN("failed to filter context by expr range", K(ret));
      } else {
        if (OB_FAIL(this->rewrite_children(windownode, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_monitoring_dump(ObLogMonitoringDump * monitoringdumpnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(monitoringdumpnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("monitoring dump node is null", K(ret));
      } else if (OB_FAIL(this->rewrite_children(monitoringdumpnode, context))) {
        LOG_WARN("failed to rewrite children", K(ret));
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_sort(ObLogSort * sortnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      // if it is top, do not pass through
      int ret = OB_SUCCESS;
      if (OB_ISNULL(sortnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort node is null", K(ret));
      } else if (OB_FAIL(this->rewrite_children(sortnode, (sortnode->is_topn_op() ? NULL : context)))) {
        LOG_WARN("failed to rewrite children", K(ret));
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_limit(ObLogLimit * limitnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      UNUSED(context);
      UNUSED(result);
      int ret = OB_SUCCESS;
      if (OB_ISNULL(limitnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("limit node is null", K(ret));
      } else if (OB_FAIL(this->rewrite_children(limitnode, NULL))) {
        LOG_WARN("failed to rewrite children", K(ret));
      }
      return ret;
    }

    // topnnode is @deprecated
    int JoinFilterPushdownRewriter::visit_topk(ObLogTopk * topnnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      UNUSED(context);
      UNUSED(result);
      int ret = OB_SUCCESS;
      if (OB_ISNULL(topnnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("topn node is null", K(ret));
      } else if (OB_FAIL(this->rewrite_children(topnnode, NULL))) {
        LOG_WARN("failed to rewrite children", K(ret));
      }
      return ret;
    }

    // tedious part, subplan scan
    int JoinFilterPushdownRewriter::visit_subplan_scan(ObLogSubPlanScan * subplan_scan, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      // map and filter subplan_scan by subplanscan mapping
      if (OB_ISNULL(subplan_scan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subplanscan node is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        // default rewrite
        if (OB_FAIL(this->rewrite_children(subplan_scan, context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      } else {
        ObLogicalOperator* child_op = subplan_scan->get_child(0);
        const ObDMLStmt *child_stmt = NULL;
        const ObSelectStmt *child_select_stmt = NULL;
        ObSEArray<ObRawExpr*, 4> output_cols;
        ObSEArray<ObRawExpr*, 4> input_cols;
        ObSEArray<ObRawExpr*, 4> use_exprs;
        JoinFilterPushdownContext *pushdown_context = NULL;
        NoSpecialExprPredicate predicate;
        bool can_pushdown = false;
        bool child_result = false;
        if (OB_ISNULL(subplan_scan) ||
            OB_ISNULL(child_op) ||
            OB_ISNULL(child_op->get_plan()) ||
            OB_ISNULL(subplan_scan->get_plan()) ||
            OB_ISNULL(child_stmt = child_op->get_plan()->get_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(child_op), K(child_stmt));
        } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
        } else if (OB_ISNULL(child_select_stmt = static_cast<const ObSelectStmt *>(child_stmt))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to cast select stmt", K(ret));
        } else if (OB_FAIL(context->assign_to(&get_allocator(), pushdown_context))) {
          LOG_WARN("failed to assign pushdown context", K(ret));
        } else if (OB_FAIL(pushdown_context->get_use_exprs(use_exprs))) {
          LOG_WARN("failed to get use exprs", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::get_subplan_scan_output_to_input_mapping(*child_select_stmt,
                                                                                     use_exprs,
                                                                                     input_cols,
                                                                                     output_cols))) {
          LOG_WARN("failed to convert subplan scan expr", K(ret));
        } else if (OB_FAIL(pushdown_context->map_and_filter(&this->get_allocator(),
                                                            &this->get_expr_factory(),
                                                            this->get_session_info(),
                                                            output_cols,
                                                            input_cols,
                                                            child_op->get_table_set()))) {
          LOG_WARN("failed to map context", K(ret));
        } else if (OB_FAIL(context->filter_by_predicate(predicate))) {
          LOG_WARN("failed to filter special exprs", K(ret));
        } else if (OB_FAIL(pushdown_context->set_through_subplan_scan(subplan_scan->get_subquery_id()))) {
          LOG_WARN("failed to set pushdown filter id", K(ret));
        } else {
          if (OB_FAIL(this->rewrite_children(subplan_scan, pushdown_context))) {
            LOG_WARN("failed to rewrite children", K(ret));
          }
        }
      }
      return ret;
    }

    // need to update the following marks
    // update dfo level ++
    int JoinFilterPushdownRewriter::visit_exchange(ObLogExchange * exchangenode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      JoinFilterPushdownContext* pushdown_context = context;
      if (OB_ISNULL(exchangenode) || OB_ISNULL(exchangenode->get_child(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exchange node is null", K(ret));
      } else if (context == NULL) {
        // do nothing
        // we only update context for exchange in.
      } else if (exchangenode->is_px_coord()) {
        context = NULL;
      } else if (exchangenode->is_consumer()) {
        if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), pushdown_context))) {
          LOG_WARN("failed to init pushdown context", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < context->pushdown_rtfs_.count(); ++i) {
            JoinFilterMetaInfo* rtf = context->pushdown_rtfs_.at(i);
            if (OB_FAIL(pushdown_context->add_runtime_filter(&this->get_allocator(),
                                                             rtf->msg_id_,
                                                             rtf->origin_join_op_,
                                                             rtf->create_expr_,
                                                             rtf->use_expr_,
                                                             rtf->is_partition_wise_
                                                               && !exchangenode->is_px_coord(),
                                                             rtf->pushdown_level_count_ + 1,
                                                             rtf->filter_table_id_))) {
              LOG_WARN("failed to add runtime filter", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(this->rewrite_children(exchangenode, pushdown_context))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      }
      return ret;
    }

    /*
     *
     * for every equal condition of hash join
     * allocate a rtf_id
     * put them in join filter pushdown context
     * update other info for context
     *
     * using equal sets to rewrite pushdown context from parent
     * - push to left  pushdown context rewrite
     * - push to right current_context + pushdown context rewrite
     *
     * refer to ObOptimizerUtil::extract_pushdown_join_filter_quals
    */
    int JoinFilterPushdownRewriter::visit_join(ObLogJoin * joinnode, JoinFilterPushdownContext* current_context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(joinnode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("joinnode is null", K(ret));
      } else {
        JoinFilterPushdownContext *join_context = NULL;
        // extract current context from joinnode
        if (OB_FAIL(extract_rtf_context_from_join(joinnode, join_context))) {
          LOG_WARN("failed to extract runtime filters from join", K(ret));
        } else {
          JoinFilterPushdownContext* to_left_context = NULL;
          JoinFilterPushdownContext* to_right_context = NULL;
          ObLogicalOperator* left_child = joinnode->get_child(0);
          ObLogicalOperator* right_child = joinnode->get_child(1);
          switch (joinnode->get_join_type()) {
            // handle inner join
            case INNER_JOIN:
            case LEFT_SEMI_JOIN:
            case RIGHT_SEMI_JOIN: {
              // for inner join, current_context to both sides
              // join context to right side only
              if (OB_FAIL(handle_inner_join(joinnode,
                                            current_context,
                                            join_context,
                                            to_left_context,
                                            to_right_context))) {
                LOG_WARN("failed to handle context derive for inner join", K(ret));
              }
              break;
            }
            case LEFT_OUTER_JOIN:
            case RIGHT_OUTER_JOIN:
            case LEFT_ANTI_JOIN:
            case RIGHT_ANTI_JOIN: {
              if (OB_FAIL(handle_outer_join(joinnode,
                                            current_context,
                                            join_context,
                                            joinnode->is_left_join() ? to_left_context : to_right_context,
                                            joinnode->is_left_join() ? to_right_context : to_left_context,
                                            joinnode->get_join_path()->is_naaj_))) {
                LOG_WARN("failed to handle context derive for outer join", K(ret));
              }
              break;
            }
            default: {
              // CONNECT_BY_JOIN
              // FULL_JOIN
              // do nothing
            }
          }
          if (OB_FAIL(ret)) {
          } else if (NESTED_LOOP_JOIN == joinnode->get_join_algo() &&
                     !(left_child->get_is_at_most_one_row() || joinnode->is_nlj_without_param_down()) &&
                     to_right_context != NULL &&
                     OB_FALSE_IT(to_right_context->reset())) {
            // only push to right side of nested-loop join if
            // left is at most scalar
            // or is nlj withoug params down
          } else if (OB_FAIL((this->dispatch_visit(left_child, to_left_context, result)))) {
            LOG_WARN("failed to rewrite child", K(ret), K(joinnode->get_name()));
          } else if (OB_FAIL((this->dispatch_visit(right_child, to_right_context, result)))) {
            LOG_WARN("failed to rewrite child", K(ret), K(joinnode->get_name()));
          }
        }
      }
      return ret;
    }

    /*
     * for left outer join
     *     left outer
     *     /     \
     *   outer    inner
     *
     * its left side is called the outer side
     * and vice versa
     *
     * REMARK: pushdown_context is NULLABLE
     */
    int JoinFilterPushdownRewriter::handle_outer_join(ObLogJoin * joinnode,
                                                      JoinFilterPushdownContext* pushdown_context, //NULLABLE
                                                      JoinFilterPushdownContext* join_context,
                                                      JoinFilterPushdownContext*& outer_side_context,
                                                      JoinFilterPushdownContext*& inner_side_context,
                                                      const bool is_naaj)
    {
      // for left outer join.
      // current context push to left
      // for thoes that pushed to left, it can also be pushed to right (by join equalsets)
      // for those pull up from left, it can push to right (currently not supported)
      // join context, push to inner side (only support for left joins)
      // a special case: naaj
      // when naaj,
      // pushdown context push to source side
      // nothing push to filtering side
      int ret = OB_SUCCESS;
      ObLogicalOperator* outer_child = NULL;
      ObLogicalOperator* inner_child = NULL;
      if (OB_ISNULL(joinnode) || OB_ISNULL(joinnode->get_child(0))
          || OB_ISNULL(joinnode->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join op is null", K(ret), KP(joinnode));
      } else {
        outer_child = joinnode->is_left_join() ? joinnode->get_child(0) : joinnode->get_child(1);
        inner_child = joinnode->is_left_join() ? joinnode->get_child(1) : joinnode->get_child(0);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), outer_side_context))) {
        LOG_WARN("failed to init pushdown context", K(ret));
      } else if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), inner_side_context))) {
        LOG_WARN("failed to init pushdown context", K(ret));
      } else {
        // push pushdown context to outer side
        if (OB_ISNULL(pushdown_context)) {
          // do nothing
        } else if (OB_ISNULL(outer_side_context) || (OB_ISNULL(outer_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("outer side context is null", K(ret));
        } else if (OB_FAIL(outer_side_context->copy_from(pushdown_context))) {
          LOG_WARN("failed to copy context", K(ret));
        } else if (OB_FAIL(outer_side_context->map_and_filter(&joinnode->get_plan()->get_allocator(),
                                                              this->get_session_info(),
                                                              joinnode->get_output_equal_sets(),
                                                              outer_child->get_table_set()))) {
          LOG_WARN("failed to map context", K(ret));
        }
        // copy outer side rtf to inner side
        // here using equal sets is not enough
        // int ObEqualAnalysis::compute_equal_set(ObIAllocator *allocator
        PersistentEqualSets output_equal_sets(joinnode->get_plan()->get_allocator());
        ObSEArray<ObRawExpr*, 4> join_conditions;
        if (OB_FAIL(ret)) {
        } else if (is_naaj) {
          // naaj do nothing
        } else if (OB_ISNULL(inner_side_context) || (OB_ISNULL(inner_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inner side context is null", K(ret));
        } else if (OB_FAIL(inner_side_context->copy_from(outer_side_context))) {
          LOG_WARN("failed to copy context", K(ret));
        } else if (OB_FAIL(join_conditions.assign(joinnode->get_equal_join_conditions()))) {
        } else if (OB_FAIL(append(join_conditions, joinnode->get_other_join_conditions()))) {
        } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&this->get_allocator(),
                                                          join_conditions,
                                                          joinnode->get_output_equal_sets(),
                                                          output_equal_sets))) {
          LOG_WARN("failed to map context", K(ret));
        } else if (OB_FAIL(inner_side_context->map_and_filter(&this->get_allocator(),
                                                              this->get_session_info(),
                                                              output_equal_sets,
                                                              inner_child->get_table_set()))) {
          LOG_WARN("failed to map context", K(ret));
        } else if (joinnode->is_left_join()) {
          // if is left, join context can push to inner side
          // as outer side is build(left) side
          if (OB_FAIL(inner_side_context->merge_from(join_context))) {
            LOG_WARN("failed to merge context", K(ret));
          }
        }
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::handle_inner_join(ObLogJoin * joinnode,
                                                      JoinFilterPushdownContext* pushdown_context, // NULLABLE
                                                      JoinFilterPushdownContext* join_context,
                                                      JoinFilterPushdownContext*& left_context,
                                                      JoinFilterPushdownContext*& right_context)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(joinnode) || OB_ISNULL(join_context) ||
          OB_ISNULL(joinnode->get_child(0)) || OB_ISNULL(joinnode->get_child(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join prt is invalid", K(ret), KP(joinnode));
      } else {
        ObLogicalOperator* left_child = joinnode->get_child(0);
        ObLogicalOperator* right_child = joinnode->get_child(1);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), left_context))) {
          LOG_WARN("failed to init context for left child", K(ret));
        } else if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), right_context))) {
          LOG_WARN("failed to init context for right child", K(ret));
        } else if (OB_ISNULL(left_context) || (OB_ISNULL(right_context))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("left or right context is null", K(ret));
        } else if (OB_FAIL(left_context->copy_from(pushdown_context))) {
          LOG_WARN("failed to copy context", K(ret));
        } else if (OB_FAIL(left_context->map_and_filter(&this->get_allocator(),
                                                        this->get_session_info(),
                                                        joinnode->get_output_equal_sets(),
                                                        left_child->get_table_set()))) {
          LOG_WARN("failed to map context", K(ret));
        } else if (OB_FAIL(right_context->copy_from(pushdown_context))) {
          LOG_WARN("failed to assign context", K(ret));
        } else if (OB_FAIL(right_context->map_and_filter(&this->get_allocator(),
                                                         this->get_session_info(),
                                                         joinnode->get_output_equal_sets(),
                                                         right_child->get_table_set()))) {
          LOG_WARN("failed to map context", K(ret));
        } else if (OB_FAIL(append(right_context->pushdown_rtfs_, join_context->pushdown_rtfs_))) {
          LOG_WARN("failed to append rtfs", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::pushdown_rtf_into_tablescan(ObLogTableScan * scannode,
                                                                ObIArray<JoinFilterMetaInfo*> &rtfs,
                                                                ObIArray<JoinFilterMetaInfo*> &pushdown_rtfs)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(scannode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scannode is null", K(ret));
      } else if (scannode->has_limit()) {
        pushdown_rtfs.reset();
      } else if (OB_SUCC(ret)) {
        bool has_group_by = scannode->has_group_by_or_aggr();
        ObSEArray<ObRawExpr*, 4> groupby_keys;
        if (OB_FAIL(scannode->get_pushdown_groupby_columns().assign(groupby_keys))) {
          LOG_WARN("failed to assign groupby columns", K(ret));
        }
        for (int i = 0; OB_SUCC(ret) && i < rtfs.count(); ++i) {
          JoinFilterMetaInfo* rtf = rtfs.at(i);
          // do a check that rtf consumer is output of tablescan
          if (!rtf->use_expr_->get_relation_ids().is_subset(scannode->get_table_set())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pushed down rtf is invalid", K(ret), KP(rtf));
          } else if (has_group_by && !ObOptimizerUtil::find_equal_expr(groupby_keys, rtf->use_expr_)) {
          } else if (OB_FALSE_IT(pushdown_rtfs.push_back(rtf))) {
          }
        }
      }
      return ret;
    }
    /*
     * filter out those contains subquery expr and onetime expr
     * and push to source side
     */
    int JoinFilterPushdownRewriter::visit_subplan_filter(ObLogSubPlanFilter * subplanfilter, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(result);
      if (OB_ISNULL(subplanfilter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subplanfilter is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        ret = this->rewrite_children(subplanfilter, NULL);
      } else if (OB_FAIL(context->filter_by_predicate(NoSubqueryPredicate()))) {
        LOG_WARN("failed to filter by predicate", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < subplanfilter->get_num_of_child(); ++i) {
          if (OB_FAIL((this->dispatch_visit(subplanfilter->get_child(i), i == 0 ? context : NULL, result)))) {
            LOG_WARN("failed to rewrite child", K(ret));
          }
        }
      }
      return ret;
    }

    int JoinFilterPushdownRewriter::visit_set(ObLogSet * set_op, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      ObSEArray<ObRawExpr *, 8> select_exprs;
      ObSEArray<ObRawExpr*, 4> child_select_exprs;
      ObSEArray<ObRawExpr*, 4> new_aggr_list;
      const ObSelectStmt *child_stmt = NULL;
      const ObSelectStmt *stmt = NULL;
      // set op exprs may have different size with children
      if (OB_ISNULL(set_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("setnode is null", K(ret));
      } else if (context == NULL || context->is_empty() || set_op->is_recursive_union()) {
        if (OB_FAIL(this->rewrite_children(set_op, NULL))) {
          LOG_WARN("failed to rewrite children", K(ret));
        }
      } else if (OB_FAIL(set_op->get_pure_set_exprs(select_exprs))) {
        LOG_WARN("failed to get set exprs", K(ret));
      } else if (OB_ISNULL(stmt = static_cast<const ObSelectStmt *>(set_op->get_stmt()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(stmt), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < set_op->get_num_of_child(); ++i) {
          // for each child, get the select exprs
          ObLogicalOperator* child_op = set_op->get_child(i);
          JoinFilterPushdownContext *pushdown_context = NULL;
          if (OB_ISNULL(child_op) ||
              OB_ISNULL(child_op->get_plan()) ||
              OB_ISNULL(child_stmt = static_cast<const ObSelectStmt *>(child_op->get_plan()->get_stmt()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(child_op), K(child_stmt));
          } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
          } else if (OB_FALSE_IT(child_select_exprs.reset())) {
            LOG_WARN("failed to reset array", K(ret));
          } else if (OB_FAIL(child_stmt->get_select_exprs(child_select_exprs))) {
            LOG_WARN("failed to get select exprs", K(ret));
          } else if (child_select_exprs.count() != select_exprs.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child stmt select exprs size is incorrect", K(child_select_exprs),
                                                                  K(select_exprs), K(ret));
          } else {
            if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), pushdown_context))) {
              LOG_WARN("failed to init pushdown context", K(ret));
            } else if (OB_FAIL(pushdown_context->copy_from(context))) {
              LOG_WARN("failed to copy context", K(ret));
            } else if (OB_FAIL(pushdown_context->map_and_filter(&this->get_allocator(),
                                                               &this->get_expr_factory(),
                                                              this->get_session_info(),
                                                              select_exprs,
                                                              child_select_exprs,
                                                              child_op->get_table_set()))) {
              LOG_WARN("failed to map context", K(ret));
            } else if (OB_FAIL(this->dispatch_visit(child_op, pushdown_context, result))) {
              LOG_WARN("failed to rewrite child", K(ret));
            }
          }
        }
      }
      return ret;
    }

    // similar to what we do for tablescannode
    // except that we set parition-wise to false
    int JoinFilterPushdownRewriter::visit_temp_table_access(ObLogTempTableAccess * temp_table_access, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(result);
      if (OB_ISNULL(temp_table_access)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("temp_table_access is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        // do nothing
      } else {
        JoinUseMetaInfo* meta_info = NULL;
        NoSpecialExprPredicate predicate;
        if (OB_FAIL(context->filter_by_predicate(predicate))) {
          LOG_WARN("failed to filter special exprs", K(ret));
        } else if (!context->is_empty()) {
          // put to valid rtfs and corresponding join use meta
          if (OB_FAIL(JoinUseMetaInfo::init_from_temp_table_access(&this->get_allocator(),
                                          meta_info,
                                          temp_table_access))) {
            LOG_WARN("failed to get joinfilter use info from temp table access", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < context->pushdown_rtfs_.count(); ++i) {
              JoinFilterMetaInfo* rtf = context->pushdown_rtfs_.at(i);
              if (OB_ISNULL(rtf)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("rtf is null", K(ret));
              } else {
                // set rtf filter_join_id
                if (rtf->filter_table_id_ == OB_INVALID_ID || rtf->is_partition_wise_) {
                  // update filter table id
                  JoinFilterMetaInfo* rewrite_rtf = NULL;
                  if (OB_FAIL(JoinFilterMetaInfo::init(&this->allocator_,
                                                       rewrite_rtf,
                                                       rtf->msg_id_,
                                                       rtf->origin_join_op_,
                                                       rtf->create_expr_,
                                                       rtf->use_expr_,
                                                       false,
                                                       rtf->pushdown_level_count_,
                                                       rtf->filter_table_id_ == OB_INVALID_ID ?
                                                            temp_table_access->get_table_id() : rtf->filter_table_id_))) {
                    LOG_WARN("failed to add runtime filter expr", K(ret));
                  } else {
                    rtf = rewrite_rtf;
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(this->valid_rtfs_.push_back(rtf))) {
                  LOG_WARN("failed to push back valid rtf", K(ret));
                } else if (OB_FAIL(this->consumer_table_infos_.push_back(meta_info))) {
                  LOG_WARN("failed to push back valid use meta info", K(ret));
                } else {
                  LOG_DEBUG("find one valid rtf", KPC(rtf), KPC(meta_info));
                }
              }
            }
          }
        }
      }
      return ret;
    }

    // currently, we only support rtf in the following position
    // -------ts ndoe--------
    // |   partial-limit    |
    // |   partial-group-by |
    // |   filter & rtfs    |
    // |--------------------|
    //  rtf after ts node is not supported
    //  even if we allocate a join filter use node is after tablescan
    //  it always means the upper order semanticaly
    int JoinFilterPushdownRewriter::visit_table_scan(ObLogTableScan * scannode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result)
    {
      int ret = OB_SUCCESS;
      UNUSED(result);
      // it is ok, we put tablename inside rewriter global context
      if (OB_ISNULL(scannode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scannode is null", K(ret));
      } else if (context == NULL || context->is_empty()) {
        // do nothing
      } else {
        JoinUseMetaInfo* meta_info = NULL;
        ObSEArray<JoinFilterMetaInfo*, 4> valid_rtfs;
        NoSpecialExprPredicate predicate;
        if (OB_FAIL(context->filter_by_predicate(predicate))) {
          LOG_WARN("failed to filter special exprs", K(ret));
        } else if (OB_FAIL(pushdown_rtf_into_tablescan(scannode,
                                                context->pushdown_rtfs_,
                                                valid_rtfs))) {
          LOG_WARN("failed to extract join filter use info from table", K(ret));
        } else if (!valid_rtfs.empty()) {
          // put to valid rtfs and corresponding join use meta
          if (OB_FAIL(JoinUseMetaInfo::init_from_tablescan(&this->get_allocator(),
                                          meta_info,
                                          scannode))) {
            LOG_WARN("failed to get joinfilter use info from scan", K(ret));
          } else {
            // for each valid rtf, add to global context
            for (int64_t i = 0; OB_SUCC(ret) && i < valid_rtfs.count(); ++i) {
              JoinFilterMetaInfo* rtf = valid_rtfs.at(i);
              if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_) || OB_ISNULL(rtf->create_expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("rtf is null", K(ret));
              } else if (rtf->use_expr_->is_nested_expr() || rtf->create_expr_->is_nested_expr()) {
                continue;
              } else {
                // set rtf filter_join_id
                if (rtf->filter_table_id_ == OB_INVALID_ID) {
                  // update filter table id
                  JoinFilterMetaInfo* rewrite_rtf = NULL;
                  if (OB_FAIL(JoinFilterMetaInfo::init(&this->allocator_,
                                                        rewrite_rtf,
                                                        rtf->msg_id_,
                                                        rtf->origin_join_op_,
                                                        rtf->create_expr_,
                                                        rtf->use_expr_,
                                                        !scannode->use_das() && rtf->is_partition_wise_,
                                                        rtf->pushdown_level_count_,
                                                        scannode->get_table_id()))) {
                    LOG_WARN("failed to add runtime filter expr", K(ret));
                  } else {
                    rtf = rewrite_rtf;
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(this->valid_rtfs_.push_back(rtf))) {
                  LOG_WARN("failed to push back valid rtf", K(ret));
                } else if (OB_FAIL(this->consumer_table_infos_.push_back(meta_info))) {
                  LOG_WARN("failed to push back valid use meta info", K(ret));
                } else {
                  LOG_DEBUG("find one valid rtf", KPC(rtf), KPC(meta_info));
                }
              }
            }
          }
        }
      }
      return ret;
    }

    /*
     * check the following conditions
     * 1. join algo is hash join with equal join conditions
     * 2. join type is not right outer join, full outer join, right anti join, connect by join, or naaj
     * 3. build side row count < 64000000.
     */
    int JoinFilterPushdownRewriter::extract_rtf_context_from_join(ObLogJoin *&joinnode,
                                                                  JoinFilterPushdownContext *& context)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(joinnode) || OB_ISNULL(joinnode->get_join_path())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join prt is invalid", K(ret), KP(joinnode), KP(joinnode->get_join_path()));
      } else if (OB_FAIL(JoinFilterPushdownContext::init(&this->get_allocator(), context))){
        LOG_WARN("failed to init pushdown context", K(ret), KP(joinnode));
      } else if (HASH_JOIN != joinnode->get_join_algo()) {
        //do nothing
      } else if (RIGHT_OUTER_JOIN == joinnode->get_join_type() ||
                 FULL_OUTER_JOIN == joinnode->get_join_type() ||
                 RIGHT_ANTI_JOIN == joinnode->get_join_type() ||
                 CONNECT_BY_JOIN == joinnode->get_join_type() ||
                 joinnode->get_join_path()->is_naaj_) {
        //do nothing
      } else {
        ObSEArray<ObRawExpr*, 4> left_join_exprs;
        ObSEArray<ObRawExpr*, 4> left_producer_exprs;
        ObSEArray<int64_t, 4> left_producer_ids;
        ObSEArray<ObRawExpr*, 4> right_join_exprs;
        NoSpecialExprPredicate predicate;
        bool has_other_conditions = false;
        // use extract_equal_join_conditions
        if (OB_FAIL(ObOptimizerUtil::extract_equal_join_conditions(joinnode->get_equal_join_conditions(),
                                                                   joinnode->get_child(0)->get_table_set(),
                                                                   left_join_exprs,
                                                                   right_join_exprs))) {
              LOG_WARN("failed format equal join conditions", K(ret));
        } else {
          // for each left keys, generate a new id if not met before and
          // put the pair into current context
          for (int64_t i = 0; OB_SUCC(ret) && i < left_join_exprs.count(); ++i) {
            if (OB_ISNULL(left_join_exprs.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is null", K(ret));
            } else if (left_join_exprs.at(i)->is_nested_expr()) {
              continue;
            }
            bool found = false;
            int64_t msg_id = -1;
            for (int64_t j = 0; OB_SUCC(ret) && !found && j < left_producer_exprs.count(); ++j) {
              if (left_producer_exprs.at(j) == left_join_exprs.at(i)) {
                found = true;
                msg_id = left_producer_ids.at(j);
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!found) {
              if (OB_FAIL(left_producer_exprs.push_back(left_join_exprs.at(i)))) {
                LOG_WARN("failed to push back producer expr", K(ret));
              } else {
                msg_id = ++(this->next_id_);
                if (OB_FAIL(left_producer_ids.push_back(msg_id))) {
                  LOG_WARN("failed to push back producer id", K(ret));
                }
              }
            }
            // add to pushdown context
            // if right has px-coordinator partition-join-filter is disabled
            // if dist-partition-wise, partition join is disabled (even forced by hint)
            if (OB_FAIL(ret) || context == NULL) {
              // do nothing
            } else if (OB_FAIL(context->add_runtime_filter(&this->get_allocator(),
                                                           msg_id,
                                                           joinnode,
                                                           left_join_exprs.at(i),
                                                           right_join_exprs.at(i),
                                                           DIST_PARTITION_WISE != joinnode->get_join_distributed_method()))) {
              LOG_WARN("failed to add runtime filter", K(ret));
            }
          }
          if (OB_FAIL(ret) || context == NULL || context->is_empty()) {
            // do nothing
          } else if (OB_FAIL(context->filter_by_predicate(predicate))) {
            LOG_WARN("failed to filter special exprs", K(ret));
          }
        }
      }
      return ret;
    }

// JoinFilterPushdownContext
    int JoinFilterPushdownContext::init(ObIAllocator *allocator, JoinFilterPushdownContext* &new_context)
    {
      int ret = OB_SUCCESS;
      void *ptr = NULL;
      if (OB_ISNULL(ptr = static_cast<JoinFilterPushdownContext *>(allocator->alloc(sizeof(JoinFilterPushdownContext))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc pushdown context", K(ret));
      } else {
        new_context = new (ptr) JoinFilterPushdownContext();
      }
      return ret;
    }
    int JoinFilterPushdownContext::set_through_subplan_scan(uint64_t filter_table_id)
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_rtfs_.count(); ++i) {
        JoinFilterMetaInfo* rtf = pushdown_rtfs_.at(i);
        if (OB_ISNULL(rtf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rtf is null", K(ret));
        } else if (OB_INVALID_ID == rtf->filter_table_id_) {
          rtf->filter_table_id_ = filter_table_id;
        }
      }
      return ret;
    }
    int JoinFilterPushdownContext::merge_from(JoinFilterPushdownContext* &new_context)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(new_context)) {
        // do nothing
      } else {
        if (OB_FAIL(append(pushdown_rtfs_, (new_context->pushdown_rtfs_)))) {
          LOG_WARN("failed to append pushdown rtfs", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPushdownContext::get_use_exprs(ObIArray<ObRawExpr*> &use_exprs)
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; i < pushdown_rtfs_.count() && OB_SUCC(ret); ++i) {
        JoinFilterMetaInfo* rtf = pushdown_rtfs_.at(i);
        if (OB_ISNULL(rtf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rtf is null", K(ret));
        } else if (OB_ISNULL(rtf->use_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("use expr is null", K(ret));
        } else if (OB_FAIL(use_exprs.push_back(rtf->use_expr_))) {
          LOG_WARN("failed to push back use expr", K(ret));
        }
      }
      return ret;
    }
    int JoinFilterPushdownContext::copy_from(JoinFilterPushdownContext* &new_context)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(new_context)) {
        // do nothing
        pushdown_rtfs_.reset();
      } else {
        pushdown_rtfs_.reset();
        if (OB_FAIL(pushdown_rtfs_.assign(new_context->pushdown_rtfs_))) {
          LOG_WARN("failed to assign pushdown rtfs", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPushdownContext::assign_to(ObIAllocator *allocator, JoinFilterPushdownContext* &new_context)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(JoinFilterPushdownContext::init(allocator, new_context))) {
        LOG_WARN("failed to alloc pushdown context", K(ret));
      } else {
        // assign to new context
        if (OB_FAIL(new_context->pushdown_rtfs_.assign(this->pushdown_rtfs_))) {
          LOG_WARN("failed to assign pushdown rtfs", K(ret));
        }
      }
      return ret;
    }

    // Helper functor for expr_range filtering
    struct ExprRangePredicate {
      const ObIArray<ObRawExpr*> &expr_range_;
      ExprRangePredicate(const ObIArray<ObRawExpr*> &expr_range) : expr_range_(expr_range) {}
      int operator()(JoinFilterMetaInfo* rtf, bool &result) const {
        int ret = OB_SUCCESS;
        result = false;
        if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_)) {
          result = false;
        } else {
          // extract column expr from use expr
          ObSEArray<ObRawExpr*, 4> column_exprs;
          if (OB_FAIL(ObRawExprUtils::extract_column_exprs(rtf->use_expr_, column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          } else {
            // if all column exprs are in expr range, then return true
            bool all_in_range = true;
            for (int64_t i = 0; OB_SUCC(ret) && all_in_range && i < column_exprs.count(); ++i) {
              if (!ObOptimizerUtil::find_equal_expr(expr_range_, column_exprs.at(i))) {
                all_in_range = false;
              }
            }
            if (OB_SUCC(ret)) {
              result = all_in_range;
            }
          }
        }
        return ret;
      }
    };

    int JoinFilterPushdownContext::filter_by_expr_range(const ObIArray<ObRawExpr*> &expr_range)
    {
      int ret = OB_SUCCESS;
      if (expr_range.empty()) {
        pushdown_rtfs_.reset();
      } else {
        ExprRangePredicate predicate(expr_range);
        if (OB_FAIL(filter_by_predicate(predicate))) {
          LOG_WARN("failed to filter by predicate", K(ret));
        }
      }
      return ret;
    }

    int JoinFilterPushdownContext::map_and_filter(ObIAllocator *allocator,
                                                  sql::ObRawExprFactory *expr_factory,
                                                  ObSQLSessionInfo *session_info,
                                                  const ObIArray<ObRawExpr*> &from_exprs,
                                                  const ObIArray<ObRawExpr*> &to_exprs,
                                                  const ObRelIds &tableset_range)
    {
      int ret = OB_SUCCESS;
      ObRawExprCopier copier(*expr_factory);
      ObSEArray<ObRawExpr*, 4> replaced_exprs;
      ObSEArray<JoinFilterMetaInfo*, 4> rewrite_pushdown_rtfs;
      if (OB_FAIL(copier.add_replaced_expr(from_exprs, to_exprs))) {
        LOG_WARN("failed to add replace pair", K(ret));
      } else {
        for (int64_t i = 0; i < pushdown_rtfs_.count() && OB_SUCC(ret); ++i) {
          JoinFilterMetaInfo* rtf = pushdown_rtfs_.at(i);
          JoinFilterMetaInfo* rewrite_rtf = NULL;
          ObRawExpr* replaced_expr = NULL;
          if (OB_ISNULL(rtf) || OB_ISNULL(rtf->use_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rtf is null", K(ret), KP(rtf), KP(rtf->use_expr_));
          } else if (OB_FAIL(copier.copy_on_replace(rtf->use_expr_, replaced_expr))) {
            LOG_WARN("failed to copy on replace", K(ret));
          } else if (OB_ISNULL(replaced_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replaced expr is null", K(ret));
          } else if (OB_FAIL(replaced_expr->formalize(session_info))) {
            LOG_WARN("failed to formalize expr", K(ret));
          } else if (OB_FAIL(replaced_expr->pull_relation_id())) {
            LOG_WARN("failed to formalize expr", K(ret));
          } else if (!replaced_expr->get_relation_ids().is_subset(tableset_range)) {
            // do nothing
          } else if (OB_FAIL(JoinFilterMetaInfo::init(allocator,
                                                      rewrite_rtf,
                                                      rtf->msg_id_,
                                                      rtf->origin_join_op_,
                                                      rtf->create_expr_,
                                                      replaced_expr,
                                                      rtf->is_partition_wise_,
                                                      rtf->pushdown_level_count_,
                                                      rtf->filter_table_id_))) {
            LOG_WARN("failed to add runtime filter expr", K(ret));
          } else if (OB_FALSE_IT(rewrite_pushdown_rtfs.push_back(rewrite_rtf))) {
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FALSE_IT(pushdown_rtfs_.reset())) {
        } else if (OB_FAIL(pushdown_rtfs_.assign(rewrite_pushdown_rtfs))) {
          LOG_WARN("failed to assign array", K(ret));
        }
      }
      return ret;
    }
    int JoinFilterPushdownContext::map_and_filter(ObIAllocator *allocator,
      ObSQLSessionInfo *session_info,
      const EqualSets &equal_sets,
      const ObRelIds &tableset_range)
    {
      int ret = OB_SUCCESS;
      ObSEArray<JoinFilterMetaInfo*, 4> rewrite_pushdown_rtfs;
      for (int64_t i = 0; i < pushdown_rtfs_.count() && OB_SUCC(ret); ++i) {
        JoinFilterMetaInfo* rtf = pushdown_rtfs_.at(i);
        ObRawExpr* consumer_expr = rtf->use_expr_;
        ObRawExpr* mapped_expr = NULL;
        if (OB_FAIL(ObOptimizerUtil::rewrite_expr_using_equal_sets(consumer_expr,
                                                                   tableset_range,
                                                                   equal_sets,
                                                                   mapped_expr))) {
          LOG_WARN("failed to rewrite expr by equal sets", K(ret));
        } else if (OB_ISNULL(mapped_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mapped expr is null", K(ret));
        } else if (OB_FAIL(mapped_expr->formalize(session_info))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (OB_FAIL(mapped_expr->pull_relation_id())) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else {
          ObRelIds rel_ids = mapped_expr->get_relation_ids();
          if (rel_ids.is_subset(tableset_range)) {
            JoinFilterMetaInfo * meta_info = NULL;
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(JoinFilterMetaInfo::init(allocator,
                                                          meta_info,
                                                          rtf->msg_id_,
                                                          rtf->origin_join_op_,
                                                          rtf->create_expr_,
                                                          mapped_expr,
                                                          rtf->is_partition_wise_,
                                                          rtf->pushdown_level_count_,
                                                          rtf->filter_table_id_))) {
              LOG_WARN("failed to add runtime filter expr", K(ret));
            } else if (OB_FALSE_IT(rewrite_pushdown_rtfs.push_back(meta_info))) {
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(pushdown_rtfs_.reset())) {
      } else if (OB_FAIL(pushdown_rtfs_.assign(rewrite_pushdown_rtfs))) {
        LOG_WARN("failed to assign array", K(ret));
      }
      return ret;
    }
    int JoinFilterPushdownContext::add_runtime_filter(ObIAllocator* allocator,
      int64_t msg_id,
      ObLogicalOperator *origin_join_op,
      ObRawExpr* create_expr,
      ObRawExpr* use_expr,
      bool is_partition_wise)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(add_runtime_filter(allocator, msg_id, origin_join_op, create_expr, use_expr, is_partition_wise, 0, OB_INVALID_ID))) {
        LOG_WARN("failed to add runtime filter", K(ret));
      }
      return ret;
    }

    int JoinFilterPushdownContext::add_runtime_filter(ObIAllocator* allocator,
                                                      int64_t msg_id,
                                                      ObLogicalOperator *origin_join_op,
                                                      ObRawExpr* create_expr,
                                                      ObRawExpr* use_expr,
                                                      bool is_partition_wise,
                                                      uint64_t pushdown_level_count,
                                                      uint64_t filter_table_id)
    {
      // add join filter info to pushdown context list
      int ret = OB_SUCCESS;
      JoinFilterMetaInfo * meta_info = NULL;
      if (OB_FAIL(JoinFilterMetaInfo::init(allocator,
                                           meta_info,
                                           msg_id,
                                           origin_join_op,
                                           create_expr,
                                           use_expr,
                                           is_partition_wise,
                                           pushdown_level_count,
                                           filter_table_id))) {
        LOG_WARN("failed to init pushdown context", K(ret));
      } else {
        if (OB_FAIL(this->pushdown_rtfs_.push_back(meta_info))) {
          LOG_WARN("failed to push back meta info for rtf", K(ret));
        }
      }
      return ret;
    }
// JoinFilterMetaInfo
    int JoinFilterMetaInfo::init(ObIAllocator* allocator,
                                 JoinFilterMetaInfo *& meta_info,
                                 int64_t msg_id,
                                 ObLogicalOperator *origin_join_op,
                                 ObRawExpr* create_expr,
                                 ObRawExpr* use_expr,
                                 bool is_partition_wise,
                                 uint64_t pushdown_level_count,
                                 uint64_t filter_table_id)
    {
      int ret = OB_SUCCESS;
      meta_info = NULL;
      if (OB_FAIL(JoinFilterMetaInfo::init(allocator, meta_info))) {
        LOG_WARN("failed to init pushdown context", K(ret));
      } else if (OB_ISNULL(meta_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta info is null", K(ret));
      } else {
        meta_info->msg_id_ = msg_id;
        meta_info->origin_join_op_ = origin_join_op;
        meta_info->create_expr_ = create_expr;
        meta_info->use_expr_ = use_expr;
        meta_info->is_partition_wise_ = is_partition_wise;
        meta_info->pushdown_level_count_ = pushdown_level_count;
        meta_info->filter_table_id_ = filter_table_id;
      }
      return ret;
    }
    int JoinFilterMetaInfo::init(ObIAllocator *allocator, JoinFilterMetaInfo* &new_context)
    {
      int ret = OB_SUCCESS;
      void *ptr = NULL;
      if (OB_ISNULL(ptr = static_cast<JoinFilterMetaInfo *>(allocator->alloc(sizeof(JoinFilterMetaInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc join filter meta", K(ret));
      } else {
        new_context = new (ptr) JoinFilterMetaInfo();
      }
      return ret;
    }

// JoinUseMetaInfo
    int JoinUseMetaInfo::init_from_temp_table_access(ObIAllocator* allocator,
                              JoinUseMetaInfo *& join_use_meta_info,
                              ObLogTempTableAccess* temp_table_access)
    {
      int ret = OB_SUCCESS;
      join_use_meta_info = NULL;
      void *ptr = NULL;
      if (OB_ISNULL(temp_table_access) || OB_ISNULL(allocator)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(temp_table_access), KP(allocator));
      } else if (OB_ISNULL(ptr = static_cast<JoinUseMetaInfo *>(allocator->alloc(sizeof(JoinUseMetaInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc use meta info", K(ret));
      } else if (OB_ISNULL(temp_table_access->get_plan())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("plan is null", K(ret));
      } else {
        join_use_meta_info = new (ptr) JoinUseMetaInfo();
        join_use_meta_info->table_id_ = temp_table_access->get_table_id();
        join_use_meta_info->table_op_id_ = temp_table_access->get_op_id();
        join_use_meta_info->sharding_ = temp_table_access->get_sharding();
        join_use_meta_info->use_das_ = false;
        join_use_meta_info->row_count_ = temp_table_access->get_card();
        OptTableMeta *basic_table_meta = temp_table_access->get_plan()->get_basic_table_metas().get_table_meta_by_table_id(temp_table_access->get_table_id());
        OptTableMeta *update_table_meta = temp_table_access->get_plan()->get_update_table_metas().get_table_meta_by_table_id(temp_table_access->get_table_id());
        join_use_meta_info->basic_table_meta_ = basic_table_meta;
        join_use_meta_info->update_table_meta_ = update_table_meta;
        join_use_meta_info->selectivity_ctx_ = &temp_table_access->get_plan()->get_selectivity_ctx();
        // todo join_use_meta_info->use_column_store_ = temp_table_access->use_column_store();
        if (OB_ISNULL(temp_table_access->get_plan()->get_stmt()->get_table_item_by_id(temp_table_access->get_table_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table id is invalid", K(ret), K(temp_table_access->get_table_id()));
        } else {
          join_use_meta_info->tableitem_ = temp_table_access->get_plan()->get_stmt()->get_table_item_by_id(temp_table_access->get_table_id());
          join_use_meta_info->pushdown_filter_table_.set_table(*(temp_table_access->get_plan()->get_stmt()->get_table_item_by_id(temp_table_access->get_table_id())));
          join_use_meta_info->equal_sets_ = &temp_table_access->get_output_equal_sets();
        }
      }
      return ret;
    }
    int JoinUseMetaInfo::init_from_tablescan(ObIAllocator* allocator,
                                             JoinUseMetaInfo *& join_use_meta_info,
                                             ObLogTableScan* tablescan)
    {
      int ret = OB_SUCCESS;
      join_use_meta_info = NULL;
      void *ptr = NULL;
      if (OB_ISNULL(tablescan) || OB_ISNULL(allocator)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(tablescan), KP(allocator));
      } else if (OB_ISNULL(tablescan->get_plan())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("plan is null", K(ret));
      } else if (OB_ISNULL(ptr = static_cast<JoinUseMetaInfo *>(allocator->alloc(sizeof(JoinUseMetaInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc use meta info", K(ret));
      } else {
        join_use_meta_info = new (ptr) JoinUseMetaInfo();
        join_use_meta_info->table_id_ = tablescan->get_table_id();
        join_use_meta_info->table_op_id_ = tablescan->get_op_id();
        join_use_meta_info->ref_table_id_ = tablescan->get_ref_table_id();
        join_use_meta_info->index_id_ = tablescan->get_index_table_id();
        join_use_meta_info->row_count_ = tablescan->get_output_row_count();
        join_use_meta_info->sharding_ = tablescan->get_sharding();
        // todo check whether has rtf will make table use column store???
        join_use_meta_info->use_column_store_ = tablescan->use_column_store();
        join_use_meta_info->use_das_ = tablescan->use_das();
        join_use_meta_info->equal_sets_ = &tablescan->get_output_equal_sets();
        // ndv info os use exprs to be used later
        // fill in table meta here for later use
        OptTableMeta *basic_table_meta = tablescan->get_plan()->get_basic_table_metas().get_table_meta_by_table_id(tablescan->get_table_id());
        OptTableMeta *update_table_meta = tablescan->get_plan()->get_update_table_metas().get_table_meta_by_table_id(tablescan->get_table_id());
        join_use_meta_info->basic_table_meta_ = basic_table_meta;
        join_use_meta_info->update_table_meta_ = update_table_meta;
        join_use_meta_info->selectivity_ctx_ = &tablescan->get_plan()->get_selectivity_ctx();
        if (OB_ISNULL(tablescan->get_plan()->get_stmt()->get_table_item_by_id(tablescan->get_table_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table id is invalid", K(ret), K(tablescan->get_table_id()));
        } else {
          join_use_meta_info->tableitem_ = tablescan->get_plan()->get_stmt()->get_table_item_by_id(tablescan->get_table_id());
          join_use_meta_info->pushdown_filter_table_.set_table(*(tablescan->get_plan()->get_stmt()->get_table_item_by_id(tablescan->get_table_id())));
        }
      }
      return ret;
    }
} // namespace sql
} // namespace oceanbase
