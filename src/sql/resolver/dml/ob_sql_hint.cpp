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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_log_plan.h"
namespace oceanbase {
using namespace common;
namespace sql {

const char* ObStmtHint::MULTILINE_INDENT = "      ";  // 6 space, align with 'Outputs & filters'
const char* ObStmtHint::ONELINE_INDENT = " ";
const char* ObStmtHint::QB_NAME = "@\"%.*s\"";
const char* ObStmtHint::FULL_HINT = "FULL";
const char* ObStmtHint::INDEX_HINT = "INDEX";
const char* ObStmtHint::NO_INDEX_HINT = "NO_INDEX";
const char* ObStmtHint::LEADING_HINT = "LEADING";
const char* ObStmtHint::ORDERED_HINT = "ORDERED";
const char* ObStmtHint::READ_CONSISTENCY = "READ_CONSISTENCY";
const char* ObStmtHint::HOTSPOT = "HOTSPOT";
const char* ObStmtHint::TOPK = "TOPK";
const char* ObStmtHint::QUERY_TIMEOUT = "QUERY_TIMEOUT";
const char* ObStmtHint::FROZEN_VERSION = "FROZEN_VERSION";
const char* ObStmtHint::USE_PLAN_CACHE = "USE_PLAN_CACHE";
const char* ObStmtHint::USE_JIT_AUTO = "USE_JIT_AUTO";
const char* ObStmtHint::USE_JIT_FORCE = "USE_JIT_FORCE";
const char* ObStmtHint::NO_USE_JIT = "NO_USE_JIT";
const char* ObStmtHint::NO_REWRITE = "NO_REWRITE";
const char* ObStmtHint::TRACE_LOG = "TRACE_LOG";
const char* ObStmtHint::LOG_LEVEL = "LOG_LEVEL";
const char* ObStmtHint::TRANS_PARAM_HINT = "TRANS_PARAM";
const char* ObStmtHint::USE_HASH_AGGREGATE = "USE_HASH_AGGREGATION";
const char* ObStmtHint::NO_USE_HASH_AGGREGATE = "NO_USE_HASH_AGGREGATION";
const char* ObStmtHint::USE_LATE_MATERIALIZATION = "USE_LATE_MATERIALIZATION";
const char* ObStmtHint::NO_USE_LATE_MATERIALIZATION = "NO_USE_LATE_MATERIALIZATION";
const char* ObStmtHint::MAX_CONCURRENT = "MAX_CONCURRENT";
const char* ObStmtHint::PRIMARY_KEY = "primary";
const char* ObStmtHint::QB_NAME_HINT = "QB_NAME";
const char* ObStmtHint::PARALLEL = "PARALLEL";
const char* ObStmtHint::USE_PX = "USE_PX";
const char* ObStmtHint::NO_USE_PX = "NO_USE_PX";
const char* ObStmtHint::USE_NL_MATERIAL = "USE_NL_MATERIALIZATION";
const char* ObStmtHint::NO_USE_NL_MATERIAL = "NO_USE_NL_MATERIALIZATION";
const char* ObStmtHint::PQ_DISTRIBUTE = "PQ_DISTRIBUTE";
const char* ObStmtHint::PQ_MAP = "PQ_MAP";
const char* ObStmtHint::NO_EXPAND_HINT = "NO_EXPAND";
const char* ObStmtHint::USE_CONCAT_HINT = "USE_CONCAT";
const char* ObStmtHint::VIEW_MERGE_HINT = "MERGE";
const char* ObStmtHint::NO_VIEW_MERGE_HINT = "NO_MERGE";
const char* ObStmtHint::UNNEST_HINT = "UNNEST";
const char* ObStmtHint::NO_UNNEST_HINT = "NO_UNNEST";
const char* ObStmtHint::PLACE_GROUP_BY_HINT = "PLACE_GROUP_BY";
const char* ObStmtHint::NO_PLACE_GROUP_BY_HINT = "NO_PLACE_GROUP_BY";
const char* ObStmtHint::ENABLE_PARALLEL_DML_HINT = "ENABLE_PARALLEL_DML";
const char* ObStmtHint::DISABLE_PARALLEL_DML_HINT = "DISABLE_PARALLEL_DML";
const char* ObStmtHint::TRACING_HINT = "TRACING";
const char* ObStmtHint::STAT_HINT = "STAT";
const char* ObStmtHint::PX_JOIN_FILTER_HINT = "PX_JOIN_FILTER";
const char* ObStmtHint::NO_PX_JOIN_FILTER_HINT = "NO_PX_JOIN_FILTER";
const char* ObStmtHint::NO_PRED_DEDUCE_HINT = "NO_PRED_DEDUCE";
const char* ObStmtHint::ENABLE_EARLY_LOCK_RELEASE_HINT = "ENABLE_EARLY_LOCK_RELEASE";
const char* ObStmtHint::FORCE_REFRESH_LOCATION_CACHE_HINT = "FORCE_REFRESH_LOCATION_CACHE";

const char* get_plan_cache_policy_str(const ObPlanCachePolicy policy)
{
  const char* c_ret = "INVALID";
  if (OB_USE_PLAN_CACHE_NONE == policy) {
    c_ret = "NONE";
  } else if (OB_USE_PLAN_CACHE_DEFAULT == policy) {
    c_ret = "DEFAULT";
  } else {
  }
  return c_ret;
}

void ObQueryHint::reset()
{
  read_consistency_ = INVALID_CONSISTENCY;
  query_timeout_ = -1;
  frozen_version_ = -1;
  plan_cache_policy_ = OB_USE_PLAN_CACHE_INVALID;
  force_trace_log_ = false;
  log_level_.reset();
  parallel_ = -1;
  force_refresh_lc_ = false;
}

OB_SERIALIZE_MEMBER(ObQueryHint, read_consistency_, dummy_, query_timeout_, plan_cache_policy_, force_trace_log_,
    log_level_, parallel_);

int ObQueryHint::deep_copy(const ObQueryHint& other, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  read_consistency_ = other.read_consistency_;
  query_timeout_ = other.query_timeout_;
  frozen_version_ = other.frozen_version_;
  plan_cache_policy_ = other.plan_cache_policy_;
  force_trace_log_ = other.force_trace_log_;
  parallel_ = other.parallel_;
  force_refresh_lc_ = other.force_refresh_lc_;
  if (OB_FAIL(ob_write_string(allocator, other.log_level_, log_level_))) {
    LOG_WARN("Failed to deep copy log level", K(ret));
  }
  return ret;
}

int ObStmtHint::assign(const ObStmtHint& other)
{
  int ret = OB_SUCCESS;
  converted_ = other.converted_;
  read_static_ = other.read_static_;
  no_rewrite_ = other.no_rewrite_;
  frozen_version_ = other.frozen_version_;
  topk_precision_ = other.topk_precision_;
  sharding_minimum_row_count_ = other.sharding_minimum_row_count_;
  query_timeout_ = other.query_timeout_;
  hotspot_ = other.hotspot_;
  join_ordered_ = other.join_ordered_;
  plan_cache_policy_ = other.plan_cache_policy_;
  read_consistency_ = other.read_consistency_;

  use_late_mat_ = other.use_late_mat_;
  force_trace_log_ = other.force_trace_log_;
  aggregate_ = other.aggregate_;
  bnl_allowed_ = other.bnl_allowed_;
  max_concurrent_ = other.max_concurrent_;
  only_concurrent_hint_ = other.only_concurrent_hint_;
  parallel_ = other.parallel_;
  use_px_ = other.use_px_;
  pdml_option_ = other.pdml_option_;
  has_px_hint_ = other.has_px_hint_;
  use_expand_ = other.use_expand_;
  use_view_merge_ = other.use_view_merge_;
  use_unnest_ = other.use_unnest_;
  use_place_groupby_ = other.use_place_groupby_;
  use_pred_deduce_ = other.use_pred_deduce_;

  // reset array and bitset
  org_indexes_.reset();
  join_order_.reset();
  join_order_pairs_.reset();
  use_merge_.reset();
  no_use_merge_.reset();
  use_merge_order_pairs_.reset();
  no_use_merge_order_pairs_.reset();
  use_hash_.reset();
  use_hash_order_pairs_.reset();
  no_use_hash_.reset();
  no_use_hash_order_pairs_.reset();
  use_nl_.reset();
  no_use_nl_.reset();
  use_nl_order_pairs_.reset();
  no_use_nl_order_pairs_.reset();
  use_bnl_.reset();
  no_use_bnl_.reset();
  use_bnl_order_pairs_.reset();
  no_use_bnl_order_pairs_.reset();
  use_nl_materialization_.reset();
  no_use_nl_materialization_.reset();
  use_nl_materialization_order_pairs_.reset();
  no_use_nl_materialization_order_pairs_.reset();
  subquery_hints_.reset();

  indexes_.reset();
  join_order_ids_.reset();
  use_merge_ids_.reset();
  no_use_merge_ids_.reset();
  use_hash_ids_.reset();
  no_use_hash_ids_.reset();
  use_nl_ids_.reset();
  no_use_nl_ids_.reset();
  use_bnl_ids_.reset();
  no_use_bnl_ids_.reset();
  use_nl_materialization_ids_.reset();
  no_use_nl_materialization_ids_.reset();

  part_hints_.reset();
  access_paths_.reset();

  use_merge_idxs_.reset();
  no_use_merge_idxs_.reset();
  use_hash_idxs_.reset();
  no_use_hash_idxs_.reset();
  use_nl_idxs_.reset();
  no_use_nl_idxs_.reset();
  use_bnl_idxs_.reset();
  no_use_bnl_idxs_.reset();

  org_pq_distributes_.reset();
  pq_distributes_.reset();
  pq_distributes_idxs_.reset();

  org_pq_maps_.reset();
  pq_maps_.reset();
  pq_map_idxs_.reset();

  rewrite_hints_.reset();
  no_expand_.reset();
  use_concat_.reset();
  v_merge_.reset();
  no_v_merge_.reset();
  unnest_.reset();
  no_unnest_.reset();
  place_groupby_.reset();
  no_place_groupby_.reset();
  no_pred_deduce_.reset();

  px_join_filter_.reset();
  no_px_join_filter_.reset();
  px_join_filter_ids_.reset();
  no_px_join_filter_ids_.reset();
  px_join_filter_order_pairs_.reset();
  no_px_join_filter_order_pairs_.reset();
  px_join_filter_idxs_.reset();
  no_px_join_filter_idxs_.reset();
  // assign array and bitset
  if (OB_FAIL(org_indexes_.assign(other.org_indexes_))) {
    LOG_WARN("assign org indexs failed", K(ret));
  } else if (OB_FAIL(join_order_.assign(other.join_order_))) {
    LOG_WARN("assign join order failed", K(ret));
  } else if (OB_FAIL(join_order_pairs_.assign(other.join_order_pairs_))) {
    LOG_WARN("assign join order pairs failed", K(ret));
  } else if (OB_FAIL(use_merge_.assign(other.use_merge_))) {
    LOG_WARN("assign use merge failed", K(ret));
  } else if (OB_FAIL(no_use_merge_.assign(other.no_use_merge_))) {
    LOG_WARN("assign no use merge failed", K(ret));
  } else if (OB_FAIL(use_hash_.assign(other.use_hash_))) {
    LOG_WARN("assign use hash failed", K(ret));
  } else if (OB_FAIL(no_use_hash_.assign(other.no_use_hash_))) {
    LOG_WARN("assign no use hash failed", K(ret));
  } else if (OB_FAIL(use_nl_.assign(other.use_nl_))) {
    LOG_WARN("assign use nl failed", K(ret));
  } else if (OB_FAIL(no_use_nl_.assign(other.no_use_nl_))) {
    LOG_WARN("assign no use nl failed", K(ret));
  } else if (OB_FAIL(use_bnl_.assign(other.use_bnl_))) {
    LOG_WARN("assign use bnl failed", K(ret));
  } else if (OB_FAIL(no_use_bnl_.assign(other.no_use_bnl_))) {
    LOG_WARN("assign no use bnl failed", K(ret));
  } else if (OB_FAIL(use_nl_materialization_.assign(other.use_nl_materialization_))) {
    LOG_WARN("assign use material nl failed", K(ret));
  } else if (OB_FAIL(no_use_nl_materialization_.assign(other.no_use_nl_materialization_))) {
    LOG_WARN("assign no use material nl failed", K(ret));
  } else if (OB_FAIL(use_merge_order_pairs_.assign(other.use_merge_order_pairs_))) {
    LOG_WARN("assign use merge order pairs failed", K(ret));
  } else if (OB_FAIL(no_use_merge_order_pairs_.assign(other.no_use_merge_order_pairs_))) {
    LOG_WARN("assign no use merge order pairs failed", K(ret));
  } else if (OB_FAIL(use_hash_order_pairs_.assign(other.use_hash_order_pairs_))) {
    LOG_WARN("assign use hash order pairs failed", K(ret));
  } else if (OB_FAIL(no_use_hash_order_pairs_.assign(other.no_use_hash_order_pairs_))) {
    LOG_WARN("assign no use hash order pairs failed", K(ret));
  } else if (OB_FAIL(use_nl_order_pairs_.assign(other.use_nl_order_pairs_))) {
    LOG_WARN("assign use nl order pairs failed", K(ret));
  } else if (OB_FAIL(no_use_nl_order_pairs_.assign(other.no_use_nl_order_pairs_))) {
    LOG_WARN("assign no use nl order pairs failed", K(ret));
  } else if (OB_FAIL(use_bnl_order_pairs_.assign(other.use_bnl_order_pairs_))) {
    LOG_WARN("assign use bnl order pairs failed", K(ret));
  } else if (OB_FAIL(no_use_bnl_order_pairs_.assign(other.no_use_bnl_order_pairs_))) {
    LOG_WARN("assign no use bnl order pairs failed", K(ret));
  } else if (OB_FAIL(subquery_hints_.assign(other.subquery_hints_))) {
    LOG_WARN("assign subquery_hints failed", K(ret));
  } else if (OB_FAIL(indexes_.assign(other.indexes_))) {
    LOG_WARN("assign indexs failed", K(ret));
  } else if (OB_FAIL(join_order_ids_.assign(other.join_order_ids_))) {
    LOG_WARN("assign join order ids failed", K(ret));
  } else if (OB_FAIL(no_use_merge_ids_.assign(other.no_use_merge_ids_))) {
    LOG_WARN("assign no use merge ids failed", K(ret));
  } else if (OB_FAIL(use_merge_ids_.assign(other.use_merge_ids_))) {
    LOG_WARN("assign use merge ids failed", K(ret));
  } else if (OB_FAIL(no_use_hash_ids_.assign(other.no_use_hash_ids_))) {
    LOG_WARN("assign no use hash ids failed", K(ret));
  } else if (OB_FAIL(use_hash_ids_.assign(other.use_hash_ids_))) {
    LOG_WARN("assign use hash ids failed", K(ret));
  } else if (OB_FAIL(no_use_nl_ids_.assign(other.no_use_nl_ids_))) {
    LOG_WARN("assign no use nl ids failed", K(ret));
  } else if (OB_FAIL(use_nl_ids_.assign(other.use_nl_ids_))) {
    LOG_WARN("assign use nl ids failed", K(ret));
  } else if (OB_FAIL(no_use_bnl_ids_.assign(other.no_use_bnl_ids_))) {
    LOG_WARN("assign no use bnl ids failed", K(ret));
  } else if (OB_FAIL(use_bnl_ids_.assign(other.use_bnl_ids_))) {
    LOG_WARN("assign use bnl ids failed", K(ret));
  } else if (OB_FAIL(part_hints_.assign(other.part_hints_))) {
    LOG_WARN("assign part hints failed", K(ret));
  } else if (OB_FAIL(access_paths_.assign(other.access_paths_))) {
    LOG_WARN("assign access paths failed", K(ret));
  } else if (OB_FAIL(no_use_merge_idxs_.assign(other.no_use_merge_idxs_))) {
    LOG_WARN("assign no use merge idxs failed", K(ret));
  } else if (OB_FAIL(use_merge_idxs_.assign(other.use_merge_idxs_))) {
    LOG_WARN("assign use merge idxs failed", K(ret));
  } else if (OB_FAIL(use_hash_idxs_.assign(other.use_hash_idxs_))) {
    LOG_WARN("assign use hash idxs failed", K(ret));
  } else if (OB_FAIL(no_use_hash_idxs_.assign(other.no_use_hash_idxs_))) {
    LOG_WARN("assign no use hash idxs failed", K(ret));
  } else if (OB_FAIL(no_use_nl_idxs_.assign(other.no_use_nl_idxs_))) {
    LOG_WARN("assign no use nl idxs failed", K(ret));
  } else if (OB_FAIL(use_nl_idxs_.assign(other.use_nl_idxs_))) {
    LOG_WARN("assign use nl idxs failed", K(ret));
  } else if (OB_FAIL(no_use_bnl_idxs_.assign(other.no_use_bnl_idxs_))) {
    LOG_WARN("assign no use bnl idxs failed", K(ret));
  } else if (OB_FAIL(use_bnl_idxs_.assign(other.use_bnl_idxs_))) {
    LOG_WARN("assign use bnl idxs failed", K(ret));
  } else if (OB_FAIL(org_pq_distributes_.assign(other.org_pq_distributes_))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(pq_distributes_.assign(other.pq_distributes_))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(pq_distributes_idxs_.assign(other.pq_distributes_idxs_))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(org_pq_maps_.assign(other.org_pq_maps_))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(pq_maps_.assign(other.pq_maps_))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(pq_map_idxs_.add_members(other.pq_map_idxs_))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(rewrite_hints_.assign(other.rewrite_hints_))) {
    LOG_WARN("rewrite hints array assign failed.", K(ret));
  } else if (OB_FAIL(no_expand_.assign(other.no_expand_))) {
    LOG_WARN("no expand array assign failed.", K(ret));
  } else if (OB_FAIL(use_concat_.assign(other.use_concat_))) {
    LOG_WARN("use concat array assign failed.", K(ret));
  } else if (OB_FAIL(v_merge_.assign(other.v_merge_))) {
    LOG_WARN("merge array assign failed.", K(ret));
  } else if (OB_FAIL(no_v_merge_.assign(other.no_v_merge_))) {
    LOG_WARN("no v merge array assign failed.", K(ret));
  } else if (OB_FAIL(unnest_.assign(other.unnest_))) {
    LOG_WARN("unnest array assign failed.", K(ret));
  } else if (OB_FAIL(no_unnest_.assign(other.no_unnest_))) {
    LOG_WARN("no unnest array assign failed.", K(ret));
  } else if (OB_FAIL(place_groupby_.assign(other.place_groupby_))) {
    LOG_WARN("place groupby expand array assign failed.", K(ret));
  } else if (OB_FAIL(no_place_groupby_.assign(other.no_place_groupby_))) {
    LOG_WARN("no place groupby expand array assign failed.", K(ret));
  } else if (OB_FAIL(px_join_filter_.assign(other.px_join_filter_))) {
    LOG_WARN("px join filter expand array assign failed.", K(ret));
  } else if (OB_FAIL(no_px_join_filter_.assign(other.no_px_join_filter_))) {
    LOG_WARN("no px join filter array assign failed.", K(ret));
  } else if (OB_FAIL(px_join_filter_ids_.assign(other.px_join_filter_ids_))) {
    LOG_WARN("px join filter id expand array assign failed.", K(ret));
  } else if (OB_FAIL(no_px_join_filter_ids_.assign(other.no_px_join_filter_ids_))) {
    LOG_WARN("no px join filter id array assign failed.", K(ret));
  } else if (OB_FAIL(no_px_join_filter_order_pairs_.assign(other.no_px_join_filter_order_pairs_))) {
    LOG_WARN("px join filter order assign failed", K(ret));
  } else if (OB_FAIL(px_join_filter_order_pairs_.assign(other.px_join_filter_order_pairs_))) {
    LOG_WARN("no px join filter order assign failed", K(ret));
  } else if (OB_FAIL(px_join_filter_idxs_.assign(other.px_join_filter_idxs_))) {
    LOG_WARN("px join filter idx assign failed", K(ret));
  } else if (OB_FAIL(px_join_filter_idxs_.assign(other.px_join_filter_idxs_))) {
    LOG_WARN("no px join filter idx assign failed", K(ret));
  } else if (OB_FAIL(no_pred_deduce_.assign(other.no_pred_deduce_))) {
    LOG_WARN("no pred deduce array assign failed.", K(ret));
  } else {
  }  // do nothing

  return ret;
}

const ObIndexHint* ObStmtHint::get_index_hint(const uint64_t table_id) const
{
  const ObIndexHint* hint = NULL;
  // check if we have any hinted index to use
  bool found = false;
  for (int64_t i = 0; !found && i < indexes_.count(); i++) {
    if (table_id == indexes_.at(i).table_id_) {
      if (indexes_.at(i).index_list_.count() > 0) {
        hint = &indexes_.at(i);
      }
      found = true;
    }
  }
  return hint;
}

ObIndexHint* ObStmtHint::get_index_hint(const uint64_t table_id)
{
  return const_cast<ObIndexHint*>(static_cast<const ObStmtHint*>(this)->get_index_hint(table_id));
}

const ObPartHint* ObStmtHint::get_part_hint(const uint64_t table_id) const
{
  const ObPartHint* hint = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < part_hints_.count(); i++) {
    if (table_id == part_hints_.at(i).table_id_) {
      hint = &part_hints_.at(i);
      found = true;
    }
  }
  return hint;
}

int ObStmtHint::add_whole_hint(ObDMLStmt* stmt)
{
  // 1. If there is a hint in top_level, ignore the hint in view(sub_query)
  // 2. If there is no hint in top_level, the hint in view (sub_query) is used consistently.
  // 3. If the hint in view (sub_query) conflicts, ignore the hint in view (sub_query) and
  //    use the value of the corresponding variable in the session of the hint.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt should not be NULL", K(ret));
  } else {
    const uint64_t READ_CONSISTENCY_FLAG = 0x1;
    const uint64_t QUERY_TIMEOUT_FLAG = 0x1 << 1;
    // removed, const uint8_t BURIED_POINT_FLAG = 0x1 << 2;
    const uint64_t LOG_LEVEL_FLAG = 0x1 << 3;
    const uint64_t USE_PLAN_CACHE_FLAG = 0x1 << 4;
    const uint64_t FORCE_TRACE_LOG_FLAG = 0x1 << 5;
    const uint64_t MAX_CONCURRENT_FLAG = 0x1 << 6;
    const uint64_t PARALLEL_FLAG = 0x1 << 7;
    const uint64_t AGGREGATE_FLAG = 0x1 << 8;
    const uint64_t USE_PX_FLAG = 0x1 << 9;
    const uint64_t USE_PX_JOIN_FILTER_FLAG = 0x1 << 10;

    // Record whether the corresponding hint is set in parent_level
    uint64_t main_hint_flags = 0;

    ObStmtHint& main_hint = stmt->get_stmt_hint();
    ObQueryCtx* query_ctx = stmt->get_query_ctx();
    if (UNSET_CONSISTENCY != main_hint.read_consistency_) {
      main_hint_flags |= READ_CONSISTENCY_FLAG;
    }
    if (UNSET_QUERY_TIMEOUT != main_hint.query_timeout_) {
      main_hint_flags |= QUERY_TIMEOUT_FLAG;
    }
    if (!main_hint.log_level_.empty()) {
      main_hint_flags |= LOG_LEVEL_FLAG;
    }
    if (main_hint.force_trace_log_) {
      main_hint_flags |= FORCE_TRACE_LOG_FLAG;
    }
    if (main_hint.plan_cache_policy_ != OB_USE_PLAN_CACHE_INVALID) {
      main_hint_flags |= USE_PLAN_CACHE_FLAG;
    }
    if (UNSET_MAX_CONCURRENT != main_hint.max_concurrent_) {
      main_hint_flags |= MAX_CONCURRENT_FLAG;
    }
    if (UNSET_PARALLEL != main_hint.parallel_) {
      main_hint_flags |= PARALLEL_FLAG;
    }
    if (OB_UNSET_AGGREGATE_TYPE != main_hint.aggregate_) {
      main_hint_flags |= AGGREGATE_FLAG;
    }
    if (ObUsePxHint::NOT_SET != main_hint.use_px_) {
      main_hint_flags |= USE_PX_FLAG;
    }

    if (ObUseRewriteHint::NOT_SET != main_hint.use_expand_ &&
        OB_FAIL(
            main_hint.rewrite_hints_.push_back(std::pair<int64_t, int64_t>(stmt->stmt_id_, main_hint.use_expand_)))) {
      LOG_WARN("failed to push back the rewrite hint.", K(ret));
    } else if (ObUseRewriteHint::NOT_SET != main_hint.use_unnest_ &&
               OB_FAIL(main_hint.rewrite_hints_.push_back(
                   std::pair<int64_t, int64_t>(stmt->stmt_id_, main_hint.use_unnest_)))) {
      LOG_WARN("failed to push back the rewrite hint.", K(ret));
    } else if (ObUseRewriteHint::NOT_SET != main_hint.use_view_merge_ &&
               OB_FAIL(main_hint.rewrite_hints_.push_back(
                   std::pair<int64_t, int64_t>(stmt->stmt_id_, main_hint.use_view_merge_)))) {
      LOG_WARN("failed to push back the rewrite hint.", K(ret));
    } else if (ObUseRewriteHint::NOT_SET != main_hint.use_place_groupby_ &&
               OB_FAIL(main_hint.rewrite_hints_.push_back(
                   std::pair<int64_t, int64_t>(stmt->stmt_id_, main_hint.use_place_groupby_)))) {
      LOG_WARN("failed to push back the rewrite hint.", K(ret));
    } else if (ObUseRewriteHint::NOT_SET != main_hint.use_pred_deduce_ &&
               OB_FAIL(main_hint.rewrite_hints_.push_back(
                   std::pair<int64_t, int64_t>(stmt->stmt_id_, main_hint.use_pred_deduce_)))) {
      LOG_WARN("failed to push back the rewrite hint.", K(ret));
    } else {
    }  // do nothing

    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->no_expand_.count(); idx++) {
      if (OB_FAIL(main_hint.no_expand_.push_back(query_ctx->no_expand_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->use_concat_.count(); idx++) {
      if (OB_FAIL(main_hint.use_concat_.push_back(query_ctx->use_concat_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->unnest_.count(); idx++) {
      if (OB_FAIL(main_hint.unnest_.push_back(query_ctx->unnest_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->no_unnest_.count(); idx++) {
      if (OB_FAIL(main_hint.no_unnest_.push_back(query_ctx->no_unnest_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->merge_.count(); idx++) {
      if (OB_FAIL(main_hint.v_merge_.push_back(query_ctx->merge_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->no_merge_.count(); idx++) {
      if (OB_FAIL(main_hint.no_v_merge_.push_back(query_ctx->no_merge_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->place_group_by_.count(); idx++) {
      if (OB_FAIL(main_hint.place_groupby_.push_back(query_ctx->place_group_by_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->no_place_group_by_.count(); idx++) {
      if (OB_FAIL(main_hint.no_place_groupby_.push_back(query_ctx->no_place_group_by_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint.", K(ret));
      } else {
      }  // do nothing
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < query_ctx->no_pred_deduce_.count(); idx++) {
      if (OB_FAIL(main_hint.no_pred_deduce_.push_back(query_ctx->no_pred_deduce_.at(idx)))) {
        LOG_WARN("failed to push back the rewrite hint", K(ret));
      }
    }
    const ObSelectStmt* sub_stmt = NULL;
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < child_stmts.count(); ++index) {
      if (OB_ISNULL(sub_stmt = child_stmts.at(index))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Sub-stmt should not be NULL", K(ret), K(index));
      } else {
        const ObStmtHint& sub_hint = sub_stmt->get_stmt_hint();

        // read consistency
        if (!(main_hint_flags & READ_CONSISTENCY_FLAG)) {
          main_hint.query_hint_conflict_map_ |= (sub_hint.query_hint_conflict_map_ & READ_CONSISTENCY_FLAG);
          if (!(main_hint.query_hint_conflict_map_ & READ_CONSISTENCY_FLAG)) {
            if (UNSET_CONSISTENCY == main_hint.read_consistency_) {
              main_hint.read_consistency_ = sub_hint.read_consistency_;
              main_hint.frozen_version_ = sub_hint.frozen_version_;
            } else if (main_hint.read_consistency_ != sub_hint.read_consistency_ &&
                       UNSET_CONSISTENCY != sub_hint.read_consistency_) {
              main_hint.read_consistency_ = UNSET_CONSISTENCY;
              main_hint.frozen_version_ = -1;
              main_hint.query_hint_conflict_map_ |= READ_CONSISTENCY_FLAG;
            } else {
            }
          }
        }

        // query timeout
        if (!(main_hint_flags & QUERY_TIMEOUT_FLAG)) {
          main_hint.query_hint_conflict_map_ |= (sub_hint.query_hint_conflict_map_ & QUERY_TIMEOUT_FLAG);
          if (!(main_hint.query_hint_conflict_map_ & QUERY_TIMEOUT_FLAG)) {
            if (UNSET_QUERY_TIMEOUT == main_hint.query_timeout_) {
              main_hint.query_timeout_ = sub_hint.query_timeout_;
            } else if (main_hint.query_timeout_ != sub_hint.query_timeout_ &&
                       UNSET_QUERY_TIMEOUT != sub_hint.query_timeout_) {
              main_hint.query_timeout_ = UNSET_QUERY_TIMEOUT;
              main_hint.query_hint_conflict_map_ |= QUERY_TIMEOUT_FLAG;
            } else {
            }
          }
        }

        // log level
        if (!(main_hint_flags & LOG_LEVEL_FLAG) && !(main_hint.query_hint_conflict_map_ & LOG_LEVEL_FLAG)) {
          if (main_hint.log_level_.empty()) {
            main_hint.log_level_.assign_ptr(sub_hint.log_level_.ptr(), sub_hint.log_level_.length());
          } else if (main_hint.log_level_ != sub_hint.log_level_ && !sub_hint.log_level_.empty()) {
            main_hint.log_level_.reset();
            main_hint.query_hint_conflict_map_ |= LOG_LEVEL_FLAG;
          } else {
          }
        }
        // trace log
        if (!(main_hint_flags & FORCE_TRACE_LOG_FLAG) && !(main_hint.query_hint_conflict_map_ & FORCE_TRACE_LOG_FLAG)) {
          if (!main_hint.force_trace_log_) {
            main_hint.force_trace_log_ = sub_hint.force_trace_log_;
          }
        }
        // use_plan_cache
        if (!(main_hint_flags & USE_PLAN_CACHE_FLAG) && !(main_hint.query_hint_conflict_map_ & USE_PLAN_CACHE_FLAG)) {
          if (OB_USE_PLAN_CACHE_INVALID == main_hint.plan_cache_policy_) {
            main_hint.plan_cache_policy_ = sub_hint.plan_cache_policy_;
          } else if (main_hint.plan_cache_policy_ != sub_hint.plan_cache_policy_ &&
                     sub_hint.plan_cache_policy_ != OB_USE_PLAN_CACHE_INVALID) {
            main_hint.query_hint_conflict_map_ |= USE_PLAN_CACHE_FLAG;
          } else {
          }
        }

        // max_concurrent
        if (!(main_hint_flags & MAX_CONCURRENT_FLAG) && !(main_hint.query_hint_conflict_map_ & MAX_CONCURRENT_FLAG)) {
          if (UNSET_MAX_CONCURRENT == main_hint.max_concurrent_) {
            main_hint.max_concurrent_ = sub_hint.max_concurrent_;
          } else if (main_hint.max_concurrent_ != sub_hint.max_concurrent_ &&
                     UNSET_MAX_CONCURRENT != sub_hint.max_concurrent_) {
            main_hint.max_concurrent_ = UNSET_MAX_CONCURRENT;
            main_hint.query_hint_conflict_map_ |= MAX_CONCURRENT_FLAG;
          } else {
          }
        }

        // parallel
        if (!(main_hint_flags & PARALLEL_FLAG) && !(main_hint.query_hint_conflict_map_ & PARALLEL_FLAG)) {
          if (UNSET_PARALLEL == main_hint.parallel_) {
            main_hint.parallel_ = sub_hint.parallel_;
          } else if (main_hint.parallel_ != sub_hint.parallel_ && UNSET_PARALLEL != sub_hint.parallel_) {
            main_hint.parallel_ = UNSET_PARALLEL;
            main_hint.query_hint_conflict_map_ |= PARALLEL_FLAG;
          } else {
          }
        }

        // aggregate
        if (!(main_hint_flags & AGGREGATE_FLAG) && !(main_hint.query_hint_conflict_map_ & AGGREGATE_FLAG)) {
          if (OB_UNSET_AGGREGATE_TYPE == main_hint.aggregate_) {
            main_hint.aggregate_ = sub_hint.aggregate_;
          } else if (main_hint.aggregate_ != sub_hint.aggregate_ && OB_UNSET_AGGREGATE_TYPE != sub_hint.aggregate_) {
            main_hint.aggregate_ = OB_UNSET_AGGREGATE_TYPE;
            main_hint.query_hint_conflict_map_ |= AGGREGATE_FLAG;
          } else {
          }
        }

        // use_px
        // no_use_px
        if ((ObUsePxHint::DISABLE == main_hint.use_px_ && ObUsePxHint::ENABLE == sub_hint.use_px_) ||
            (ObUsePxHint::ENABLE == main_hint.use_px_ && ObUsePxHint::DISABLE == sub_hint.use_px_)) {
          // Conflict hint, we set to invalid
          main_hint.use_px_ = ObUsePxHint::INVALID;
        } else if (ObUsePxHint::INVALID == main_hint.use_px_ || ObUsePxHint::INVALID == sub_hint.use_px_) {
          main_hint.use_px_ = ObUsePxHint::INVALID;
        } else {
          main_hint.use_px_ = sub_hint.use_px_ != ObUsePxHint::NOT_SET ? sub_hint.use_px_ : main_hint.use_px_;
        }

        main_hint.has_hint_exclude_concurrent_ =
            main_hint.has_hint_exclude_concurrent_ || sub_hint.has_hint_exclude_concurrent_;

        main_hint.enable_lock_early_release_ =
            main_hint.enable_lock_early_release_ || sub_hint.enable_lock_early_release_;

        main_hint.force_refresh_lc_ = main_hint.force_refresh_lc_ || sub_hint.force_refresh_lc_;

        // rewrite related hints
        for (int64_t i = 0; OB_SUCC(ret) && i < sub_hint.rewrite_hints_.count(); i++) {
          std::pair<int64_t, int64_t> rewrite_hint = sub_hint.rewrite_hints_.at(i);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.rewrite_hints_, rewrite_hint))) {
            LOG_WARN("failed to push back the sub hint rewrite hints to main hint.");
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.no_expand_.count(); idx++) {
          ObString qb_name = sub_hint.no_expand_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.no_expand_, qb_name))) {
            LOG_WARN("failed to add the no expand hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.use_concat_.count(); idx++) {
          ObString qb_name = sub_hint.use_concat_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.use_concat_, qb_name))) {
            LOG_WARN("failed to add the use concat hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.unnest_.count(); idx++) {
          ObString qb_name = sub_hint.unnest_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.unnest_, qb_name))) {
            LOG_WARN("failed to add the unnest hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.no_unnest_.count(); idx++) {
          ObString qb_name = sub_hint.no_unnest_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.no_unnest_, qb_name))) {
            LOG_WARN("failed to add the no unnest hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.v_merge_.count(); idx++) {
          ObString qb_name = sub_hint.v_merge_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.v_merge_, qb_name))) {
            LOG_WARN("failed to add the v merge hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.no_v_merge_.count(); idx++) {
          ObString qb_name = sub_hint.no_v_merge_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.no_v_merge_, qb_name))) {
            LOG_WARN("failed to add the no v merge hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.place_groupby_.count(); idx++) {
          ObString qb_name = sub_hint.place_groupby_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.place_groupby_, qb_name))) {
            LOG_WARN("failed to add the place groupby hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.no_place_groupby_.count(); idx++) {
          ObString qb_name = sub_hint.no_place_groupby_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.no_place_groupby_, qb_name))) {
            LOG_WARN("failed to add the no place groupby hints.", K(ret));
          } else {
          }  // do nothing.
        }
        for (int64_t idx = 0; OB_SUCC(ret) && idx < sub_hint.no_pred_deduce_.count(); idx++) {
          ObString qb_name = sub_hint.no_pred_deduce_.at(idx);
          if (OB_FAIL(add_var_to_array_no_dup(main_hint.no_pred_deduce_, qb_name))) {
            LOG_WARN("failed to add the no place groupby hints.", K(ret));
          } else {
          }  // do nothing.
        }
      }
    }  // end of for

    if (OB_SUCC(ret)) {
      main_hint.only_concurrent_hint_ =
          (UNSET_MAX_CONCURRENT != main_hint.max_concurrent_) && (!main_hint.has_hint_exclude_concurrent_);
    }
  }
  return ret;
}

int ObStmtHint::replace_name_in_hint(const ObSQLSessionInfo& session_info, ObDMLStmt& stmt,
    ObIArray<ObTableInHint>& arr, uint64_t table_id, ObString& to_db_name, ObString& to_table_name)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); i++) {
    // ObString compare
    uint64_t hint_table_id = OB_INVALID_ID;
    if (OB_FAIL(stmt.get_table_id(
            session_info, ObString::make_empty_string(), arr.at(i).db_name_, arr.at(i).table_name_, hint_table_id))) {
      LOG_WARN("failed to get table id of hint");
    } else if (OB_INVALID_ID == hint_table_id) {
      /* Doing nothing */
    } else if (hint_table_id == table_id) {
      arr.at(i).db_name_ = to_db_name;
      arr.at(i).table_name_ = to_table_name;
    }
  }
  return ret;
}

int ObStmtHint::add_single_table_view_hint(const ObSQLSessionInfo& session_info, ObDMLStmt* stmt, uint64_t table_id,
    ObString& to_db_name, ObString& to_table_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL stmt", K(stmt));
  } else {
    ObStmtHint& hint = stmt->get_stmt_hint();
    if (OB_FAIL(replace_name_in_hint(session_info, *stmt, hint.join_order_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in join_order_");
    } else if (OB_FAIL(
                   replace_name_in_hint(session_info, *stmt, hint.use_merge_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in use_merge_");
    } else if (OB_FAIL(replace_name_in_hint(
                   session_info, *stmt, hint.no_use_merge_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in no use_merge_");
    } else if (OB_FAIL(
                   replace_name_in_hint(session_info, *stmt, hint.use_hash_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in use_hash_");
    } else if (OB_FAIL(
                   replace_name_in_hint(session_info, *stmt, hint.no_use_hash_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in no_use_hash_");
    } else if (OB_FAIL(replace_name_in_hint(session_info, *stmt, hint.use_nl_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in use_nl_");
    } else if (OB_FAIL(
                   replace_name_in_hint(session_info, *stmt, hint.no_use_nl_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in no use_nl_");
    } else if (OB_FAIL(replace_name_in_hint(session_info, *stmt, hint.use_bnl_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in use_bnl_");
    } else if (OB_FAIL(
                   replace_name_in_hint(session_info, *stmt, hint.no_use_bnl_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in no use_bnl_");
    } else if (OB_FAIL(replace_name_in_hint(
                   session_info, *stmt, hint.use_nl_materialization_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in use_nl_materialization_");
    } else if (OB_FAIL(replace_name_in_hint(
                   session_info, *stmt, hint.no_use_nl_materialization_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in no no_use_nl_materialization_");
    } else if (OB_FAIL(replace_name_in_hint(
                   session_info, *stmt, hint.px_join_filter_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in px join filter", K(ret));
    } else if (OB_FAIL(replace_name_in_hint(
                   session_info, *stmt, hint.no_px_join_filter_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("failed to replace in no px join filter", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObStmtHint::add_view_merge_hint(const ObStmtHint* view_hint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(view_hint)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("View hint should not be NULL", K(ret));
  } else if (OB_FAIL(subquery_hints_.push_back(view_hint))) {
    LOG_WARN("Add subquery hint error", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObStmtHint::add_index_hint(const uint64_t table_id, const common::ObString& index_name)
{
  int ret = OB_SUCCESS;
  bool dup = false;
  for (int64_t idx = 0; OB_SUCC(ret) && !dup && idx < indexes_.count(); ++idx) {
    ObIndexHint& index_hint = indexes_.at(idx);
    if (table_id == index_hint.table_id_) {
      dup = true;
      if (ObIndexHint::IGNORE == index_hint.type_) {
        // method /*+index(t i)*/ will ignore IGNORE hint
        index_hint.type_ = ObIndexHint::USE;
        index_hint.index_list_.reset();
      }
      if (OB_FAIL(index_hint.index_list_.push_back(index_name))) {
        LOG_WARN("Failed to add index name", K(ret));
      }
    }
  }
  if (!dup) {
    ObIndexHint index_hint;
    index_hint.table_id_ = table_id;
    if (OB_FAIL(index_hint.index_list_.push_back(index_name))) {
      LOG_WARN("Failed to add index hint", K(ret));
    } else if (OB_FAIL(indexes_.push_back(index_hint))) {
      LOG_WARN("Failed to add index hint", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObStmtHint::merge_view_hints()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_hints_.count(); ++i) {
    const ObStmtHint* view_hint = subquery_hints_.at(i);
    if (OB_ISNULL(view_hint)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Stmt hint should not be NULL", K(ret));
    } else {
      if (!join_ordered_) {
        join_ordered_ = view_hint->join_ordered_;
      }
      if (join_order_ids_.empty()) {
        join_order_ids_ = view_hint->join_order_ids_;
        join_order_pairs_ = view_hint->join_order_pairs_;
      } else if (!view_hint->join_order_ids_.empty()) {
        join_order_ids_.reset();
      } else {
      }  // do nothing

      if (OB_SUCC(ret)) {
        // hint on signle table
        if (OB_FAIL(append(indexes_, view_hint->indexes_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_merge_ids_, view_hint->use_merge_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_merge_ids_, view_hint->no_use_merge_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_hash_ids_, view_hint->use_hash_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_hash_ids_, view_hint->no_use_hash_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_nl_ids_, view_hint->use_nl_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_nl_ids_, view_hint->no_use_nl_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_bnl_ids_, view_hint->use_bnl_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_bnl_ids_, view_hint->no_use_bnl_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_nl_materialization_ids_, view_hint->use_nl_materialization_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_nl_materialization_ids_, view_hint->no_use_nl_materialization_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(part_hints_, view_hint->part_hints_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(pq_distributes_, view_hint->pq_distributes_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_merge_order_pairs_, view_hint->use_merge_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_merge_order_pairs_, view_hint->no_use_merge_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_hash_order_pairs_, view_hint->use_hash_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_hash_order_pairs_, view_hint->no_use_hash_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_nl_order_pairs_, view_hint->use_nl_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_nl_order_pairs_, view_hint->no_use_nl_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(use_bnl_order_pairs_, view_hint->use_bnl_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_use_bnl_order_pairs_, view_hint->no_use_bnl_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(pq_maps_, view_hint->pq_maps_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(px_join_filter_ids_, view_hint->px_join_filter_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_px_join_filter_ids_, view_hint->no_px_join_filter_ids_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(px_join_filter_order_pairs_, view_hint->px_join_filter_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else if (OB_FAIL(append(no_px_join_filter_order_pairs_, view_hint->no_px_join_filter_order_pairs_))) {
          LOG_WARN("Append array error", K(ret));
        } else { /* do nothing.*/
        }
      }
    }
  }
  return ret;
}

ObIArray<ObTableInHint>* ObStmtHint::get_join_tables(TablesHint hint)
{
  ObIArray<ObTableInHint>* tables = NULL;
  if (LEADING == hint) {
    tables = &join_order_;
  } else if (USE_MERGE == hint) {
    tables = &use_merge_;
  } else if (NO_USE_MERGE == hint) {
    tables = &no_use_merge_;
  } else if (USE_HASH == hint) {
    tables = &use_hash_;
  } else if (NO_USE_HASH == hint) {
    tables = &no_use_hash_;
  } else if (USE_NL == hint) {
    tables = &use_nl_;
  } else if (NO_USE_NL == hint) {
    tables = &no_use_nl_;
  } else if (USE_BNL == hint) {
    tables = &use_bnl_;
  } else if (NO_USE_BNL == hint) {
    tables = &no_use_bnl_;
  } else if (USE_NL_MATERIALIZATION == hint) {
    tables = &use_nl_materialization_;
  } else if (NO_USE_NL_MATERIALIZATION == hint) {
    tables = &no_use_nl_materialization_;
  } else if (PX_JOIN_FILTER == hint) {
    tables = &px_join_filter_;
  } else if (NO_PX_JOIN_FILTER == hint) {
    tables = &no_px_join_filter_;
  } else {
    // do nothing
  }
  return tables;
}

int ObStmtHint::print_global_hint_for_outline(planText& plan_text, const ObDMLStmt* stmt) const
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;

  // READ_CONSISTENCY && FROZEN_VERSION
  if (OB_SUCC(ret) && (read_consistency_ != UNSET_CONSISTENCY)) {
    if (FROZEN == read_consistency_ && frozen_version_ != -1) {
      if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(BUF_PRINTF(FROZEN_VERSION))) {
      } else if (OB_FAIL(BUF_PRINTF("(%ld)", frozen_version_))) {
      } else { /*do nothing*/
      }
    } else {
      if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(BUF_PRINTF(READ_CONSISTENCY))) {
      } else if (OB_FAIL(BUF_PRINTF("(\"%s\")", get_consistency_level_str(read_consistency_)))) {
      } else { /*do nothing*/
      }
    }
  }

  // QUERY_TIMEOUT
  if (OB_SUCC(ret) && query_timeout_ > 0) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(QUERY_TIMEOUT))) {
    } else if (OB_FAIL(BUF_PRINTF("(%ld)", query_timeout_))) {
    } else { /*do nothing*/
    }
  }

  // USE_PLAN_CACHE
  if (OB_SUCC(ret) && plan_cache_policy_ != OB_USE_PLAN_CACHE_INVALID) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(USE_PLAN_CACHE))) {
    } else if (OB_FAIL(BUF_PRINTF("(\"%s\")", get_plan_cache_policy_str(plan_cache_policy_)))) {
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && use_jit_policy_ != OB_USE_JIT_INVALID) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_NO_USE_JIT == use_jit_policy_) {
      if (OB_FAIL(BUF_PRINTF(NO_USE_JIT))) {}
    } else if (OB_USE_JIT_AUTO == use_jit_policy_) {
      if (OB_FAIL(BUF_PRINTF(USE_JIT_AUTO))) {}
    } else if (OB_USE_JIT_FORCE == use_jit_policy_) {
      if (OB_FAIL(BUF_PRINTF(USE_JIT_FORCE))) {}
    } else { /*do nothing*/
    }
  }

  // NO_REWRITE
  if (OB_SUCC(ret) && no_rewrite_) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(NO_REWRITE))) {
    } else { /*do nothing*/
    }
  }

  // TRACE_LOG
  if (OB_SUCC(ret) && force_trace_log_) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(TRACE_LOG))) {
    } else { /*do nothing*/
    }
  }

  // TRANS_PARAM
  if (OB_SUCC(ret) && enable_lock_early_release_) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(TRANS_PARAM_HINT))) {
    } else if (OB_FAIL(BUF_PRINTF("(\'"))) {
    } else if (OB_FAIL(BUF_PRINTF(ENABLE_EARLY_LOCK_RELEASE_HINT))) {
    } else if (OB_FAIL(BUF_PRINTF("\' \'true\'"))) {
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
    } else { /*do nothing*/
    }
  }

  // FORCE_REFRESH_LOCATION_CACHE
  if (OB_SUCC(ret) && force_refresh_lc_) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(FORCE_REFRESH_LOCATION_CACHE_HINT))) {
    } else { /*do nothing*/
    }
  }

  // LOG_LEVEL
  if (OB_SUCC(ret) && !log_level_.empty()) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(LOG_LEVEL))) {
    } else if (OB_FAIL(BUF_PRINTF("(\'%.*s\')", log_level_.length(), log_level_.ptr()))) {
    } else { /*do nothing*/
    }
  }

  // PARALLEL
  if (OB_SUCC(ret) && parallel_ > 0) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(PARALLEL))) {
    } else if (OB_FAIL(BUF_PRINTF("(%ld)", parallel_))) {
    } else { /*do nothing*/
    }
  }

  // PX
  if (OB_SUCC(ret)) {
    bool is_need = false;
    if (OB_FAIL(is_need_print_use_px(plan_text, is_need))) {
      LOG_WARN("failed to check the use px hint.", K(ret));
    } else if (!is_need) {  // do nothing
    } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else if (enable_use_px() && OB_FAIL(BUF_PRINTF(USE_PX))) {
    } else if (disable_use_px() && OB_FAIL(BUF_PRINTF(NO_USE_PX))) {
    } else { /*do nothing*/
    }
  }

  // PDML
  if (OB_SUCC(ret) && ObPDMLOption::NOT_SPECIFIED != pdml_option_) {
    if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
    } else {
      switch (pdml_option_) {
        case ObPDMLOption::ENABLE:
          ret = BUF_PRINTF(ENABLE_PARALLEL_DML_HINT);
          break;
        case ObPDMLOption::DISABLE:
          ret = BUF_PRINTF(DISABLE_PARALLEL_DML_HINT);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected pdml hint value", K(ret), K_(pdml_option));
          break;
      }
    }
  }

  // REWRITE(print the outline for optimizer).
  if (OB_SUCC(ret)) {
    common::ObString qb_name;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < rewrite_hints_.count(); ++idx) {
      int64_t stmt_id = rewrite_hints_.at(idx).first;
      int64_t hint_type = rewrite_hints_.at(idx).second;
      if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(stmt->get_stmt_name_by_id(stmt_id, qb_name))) {
        LOG_WARN("failed to get stmt name by id.", K(ret));
      } else if (OB_FAIL(print_rewrite_hint_for_outline(plan_text, hint_type, qb_name))) {
        LOG_WARN("failed to print rewrite hint for outline.", K(ret));
      } else { /*do nothing*/
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObQueryCtx* query_ctx = stmt->get_query_ctx();
    if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, NO_EXPAND_HINT, no_expand_, query_ctx))) {
      LOG_WARN("failed to print no expand hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, USE_CONCAT_HINT, use_concat_, query_ctx))) {
      LOG_WARN("failed to print use concat hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, UNNEST_HINT, unnest_, query_ctx))) {
      LOG_WARN("failed to print unnest hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, NO_UNNEST_HINT, no_unnest_, query_ctx))) {
      LOG_WARN("failed to print no_unnest hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, VIEW_MERGE_HINT, v_merge_, query_ctx))) {
      LOG_WARN("failed to print v merge hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, NO_VIEW_MERGE_HINT, no_v_merge_, query_ctx))) {
      LOG_WARN("failed to print no v merge hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, PLACE_GROUP_BY_HINT, place_groupby_, query_ctx))) {
      LOG_WARN("failed to print place group by hint for outline.", K(ret));
    } else if (OB_FAIL(
                   print_rewrite_hints_for_outline(plan_text, NO_PLACE_GROUP_BY_HINT, no_place_groupby_, query_ctx))) {
      LOG_WARN("failed to print no place group by hint for outline.", K(ret));
    } else if (OB_FAIL(print_rewrite_hints_for_outline(plan_text, NO_PRED_DEDUCE_HINT, no_pred_deduce_, query_ctx))) {
      LOG_WARN("failed to print no place group by hint for outline.", K(ret));
    } else {
    }  // do nothing.
  }

  return ret;
}

int ObStmtHint::print_rewrite_hint_for_outline(planText& plan_text, int64_t hint_type, const ObString& qb_name) const
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  if (hint_type == ObUseRewriteHint::NO_EXPAND && OB_FAIL(BUF_PRINTF(NO_EXPAND_HINT))) {
  } else if (hint_type == ObUseRewriteHint::USE_CONCAT && OB_FAIL(BUF_PRINTF(USE_CONCAT_HINT))) {
  } else if (hint_type == ObUseRewriteHint::UNNEST && OB_FAIL(BUF_PRINTF(UNNEST_HINT))) {
  } else if (hint_type == ObUseRewriteHint::NO_UNNEST && OB_FAIL(BUF_PRINTF(NO_UNNEST_HINT))) {
  } else if (hint_type == ObUseRewriteHint::V_MERGE && OB_FAIL(BUF_PRINTF(VIEW_MERGE_HINT))) {
  } else if (hint_type == ObUseRewriteHint::NO_V_MERGE && OB_FAIL(BUF_PRINTF(NO_VIEW_MERGE_HINT))) {
  } else if (hint_type == ObUseRewriteHint::PLACE_GROUPBY && OB_FAIL(BUF_PRINTF(PLACE_GROUP_BY_HINT))) {
  } else if (hint_type == ObUseRewriteHint::NO_PLACE_GROUPBY && OB_FAIL(BUF_PRINTF(NO_PLACE_GROUP_BY_HINT))) {
  } else if (hint_type == ObUseRewriteHint::NO_PRED_DEDUCE && OB_FAIL(BUF_PRINTF(NO_PRED_DEDUCE_HINT))) {
  } else if (OB_FAIL(BUF_PRINTF("("))) {
  } else if (BUF_PRINTF(ObStmtHint::QB_NAME, qb_name.length(), qb_name.ptr())) {
    LOG_WARN("fail to print buffer", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(")"))) {
  } else { /*do nothing*/
  }
  return ret;
}

int ObStmtHint::print_rewrite_hints_for_outline(planText& plan_text, const char* rewrite_hint,
    const ObIArray<ObString>& qb_names, const ObQueryCtx* query_ctx) const
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;
  OutlineType outline_type = plan_text.outline_type_;
  common::ObString origin_name;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no query ctx for the hints context", K(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < qb_names.count(); ++idx) {
      ObString qb_name = qb_names.at(idx);
      if (OUTLINE_DATA == outline_type && OB_FAIL(query_ctx->get_stmt_org_name(qb_name, origin_name))) {
        // for invalid qb name, reset ret
        // select /*+use_concat(@sel$5)*/ * from t1;
        ret = OB_SUCCESS;
      } else if (USED_HINT == outline_type && FALSE_IT(origin_name = qb_name)) {
      } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(BUF_PRINTF(rewrite_hint))) {
      } else if (OB_FAIL(BUF_PRINTF("("))) {
      } else if (BUF_PRINTF(ObStmtHint::QB_NAME, origin_name.length(), origin_name.ptr())) {
        LOG_WARN("fail to print buffer", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
      } else { /*do nothing*/
      }
    }
  }

  return ret;
}

int ObStmtHint::is_need_print_use_px(planText& plan_text, bool& is_need) const
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OUTLINE_DATA == outline_type) {
    is_need = has_px_hint_;
  } else if (USED_HINT == outline_type) {
    is_need = has_px_hint_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

void ObStmtHint::reset_valid_join_type()
{
  valid_use_merge_idxs_.reset();  // for record in optimizer.
  valid_use_hash_idxs_.reset();
  valid_use_nl_idxs_.reset();
  valid_use_bnl_idxs_.reset();
}

int ObStmtHint::is_used_in_leading(uint64_t table_id, bool& is_used) const
{
  int ret = OB_SUCCESS;
  is_used = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_used && i < join_order_ids_.count(); ++i) {
    if (table_id == join_order_ids_.at(i)) {
      is_used = true;
    }
  }
  return ret;
}

int ObStmtHint::is_used_join_type(JoinAlgo join_algo, int32_t table_id_idx, bool& is_used) const
{
  int ret = OB_SUCCESS;
  is_used = false;
  if (NESTED_LOOP_JOIN == join_algo) {
    if (OB_FAIL(has_table_member(valid_use_nl_idxs_, table_id_idx, is_used))) {
      LOG_WARN("check the used table idx error.", K(ret));
    } else {
    }  // do nothing.
  } else if (MERGE_JOIN == join_algo) {
    if (OB_FAIL(has_table_member(valid_use_merge_idxs_, table_id_idx, is_used))) {
      LOG_WARN("check the used table idx error.", K(ret));
    } else {
    }  // do nothing.
  } else if (HASH_JOIN == join_algo) {
    if (OB_FAIL(has_table_member(valid_use_hash_idxs_, table_id_idx, is_used))) {
      LOG_WARN("check the used table idx error.", K(ret));
    } else {
    }  // do nothing.
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeced join type", K(ret), K(join_algo), K(table_id_idx));
  }
  return ret;
}

int ObStmtHint::has_table_member(
    const common::ObIArray<ObRelIds>& valid_use_idxs, int32_t table_id_idx, bool& is_used) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < valid_use_idxs.count(); i++) {
    if (valid_use_idxs.at(i).has_member(table_id_idx)) {
      is_used = true;
      break;
    } else {
    }  // do nothing.
  }
  return ret;
}

void ObStmtHint::set_table_name_error_flag(bool flag)
{
  is_table_name_error_ = flag;
}

bool ObStmtHint::is_hints_all_worked() const
{
  bool is_hints_all_worked = true;
  // no need to check hotspot, frozen version, topk, query_timeout,
  // read consistency, log level, qb name, full, not conflict.
  // leading
  if (join_order_.count() != 0 && join_order_ids_.count() == 0) {
    is_hints_all_worked &= false;
  }
  // index error.
  if (is_table_name_error_) {
    is_hints_all_worked &= false;
  } else {
    for (int64_t i = 0; i < indexes_.count(); i++) {
      ObIndexHint index_hint = indexes_.at(i);
      if (index_hint.index_list_.count() > index_hint.valid_index_ids_.count()) {
        is_hints_all_worked &= false;
        break;
      }
    }
  }
  // merge/no use merge, hash/no use hash, nl/no use nl, bnl/no use bnl
  if (use_merge_.count() != 0 && valid_use_merge_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (use_hash_.count() != 0 && valid_use_hash_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (use_nl_.count() != 0 && valid_use_nl_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (use_bnl_.count() != 0 && valid_use_bnl_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (use_nl_materialization_.count() != 0 && valid_use_nl_materialization_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (no_use_merge_.count() != 0 && valid_no_use_merge_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (no_use_hash_.count() != 0 && valid_no_use_hash_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (no_use_nl_.count() != 0 && valid_no_use_nl_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (no_use_bnl_.count() != 0 && valid_no_use_bnl_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (no_use_nl_materialization_.count() != 0 && valid_no_use_nl_materialization_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  // no need to check px join filter, ordered, use plan cache, use jit, no use jit,
  // use px/no use px, use hash aggregation, no use hash aggregation, use late materialization,
  // no use late materialization, no rewrite, trace log, max concurrent, parallel, not conflict.
  // check pq distribute
  if (pq_distributes_idxs_.count() != 0 && valid_pq_distributes_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  // check px join filter
  if (no_px_join_filter_ids_.count() != 0 && valid_no_px_join_filter_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  if (px_join_filter_ids_.count() != 0 && valid_px_join_filter_idxs_.count() == 0) {
    is_hints_all_worked &= false;
  }
  // add the rewrite related hints to hints_all_worked.
  // check tracing
  // check stat
  return is_hints_all_worked;
}

}  // end of namespace sql
}  // end of namespace oceanbase
