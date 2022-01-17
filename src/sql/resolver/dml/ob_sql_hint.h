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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_SQL_HINT_
#define OCEANBASE_SQL_RESOLVER_DML_OB_SQL_HINT_
#include "lib/string/ob_string.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_hint.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_log_operator_factory.h"

namespace oceanbase {
namespace sql {
class ObDMLStmt;
class ObSelectStmt;
class ObSQLSessionInfo;
class planText;

struct ObTableInHint {
  ObTableInHint()
  {}
  ObTableInHint(common::ObString& qb_name, common::ObString& db_name, common::ObString& table_name)
      : qb_name_(qb_name), db_name_(db_name), table_name_(table_name)
  {}

  TO_STRING_KV(N_QB_NAME, qb_name_, N_DATABASE_NAME, db_name_, N_TABLE_NAME, table_name_);
  common::ObString qb_name_;
  common::ObString db_name_;
  common::ObString table_name_;
};

struct ObOrgIndexHint {
  ObOrgIndexHint() : distributed_(false)
  {}
  ObTableInHint table_;
  common::ObString index_name_;
  bool distributed_;
  TO_STRING_KV(N_TABLE_NAME, table_, N_INDEX_TABLE, index_name_);
};

struct ObQNameIndexHint {
  ObQNameIndexHint() : distributed_(false)
  {}
  ObQNameIndexHint(common::ObString& qb_name, ObOrgIndexHint& index_hint)
      : qb_name_(qb_name), index_hint_(index_hint), distributed_(false)
  {}
  common::ObString qb_name_;
  ObOrgIndexHint index_hint_;
  bool distributed_;
  TO_STRING_KV(N_QB_NAME, qb_name_, N_INDEX_TABLE, index_hint_);
};

struct ObIndexHint {
  enum HintType {
    UNINIT,
    USE,
    FORCE,
    IGNORE,
  };

  ObIndexHint() : table_id_(common::OB_INVALID_ID), type_(USE), valid_index_ids_()
  {}

  uint64_t table_id_;
  common::ObSEArray<common::ObString, 3> index_list_;
  HintType type_;
  common::ObSEArray<uint64_t, 3> valid_index_ids_;
  TO_STRING_KV(N_TID, table_id_, N_INDEX_TABLE, index_list_, "type", type_, "valid_index_ids", valid_index_ids_);
};

struct ObPartHint {
  ObPartHint() : table_id_(common::OB_INVALID_ID)
  {}
  uint64_t hash(uint64_t seed) const
  {
    seed = common::do_hash(table_id_, seed);
    for (int64_t i = 0; i < part_ids_.count(); i++) {
      seed = common::do_hash(part_ids_.at(i), seed);
    }

    return seed;
  }
  uint64_t table_id_;
  common::ObSEArray<int64_t, 3> part_ids_;
  common::ObSEArray<ObString, 3> part_names_;
  TO_STRING_KV(K_(table_id), K_(part_ids), K_(part_names));
};

enum ObAccessPathMethod { USE_INDEX = 0, USE_SCAN };

enum ObPlanCachePolicy {
  OB_USE_PLAN_CACHE_INVALID = 0,  // policy not set
  OB_USE_PLAN_CACHE_NONE,         // do not use plan cache
  OB_USE_PLAN_CACHE_DEFAULT,      // use plan cache
};

enum ObUseJitPolicy {
  OB_USE_JIT_INVALID = 0,
  OB_USE_JIT_AUTO,
  OB_USE_JIT_FORCE,
  OB_NO_USE_JIT,
};

enum ObLateMaterializationMode {
  OB_LATE_MATERIALIZATION_INVALID = -1,
  OB_NO_USE_LATE_MATERIALIZATION = 0,
  OB_USE_LATE_MATERIALIZATION,
};

const char* get_plan_cache_policy_str(ObPlanCachePolicy policy);

struct ObQueryHint {
  OB_UNIS_VERSION(1);

public:
  ObQueryHint()
      : read_consistency_(common::INVALID_CONSISTENCY),
        dummy_(),
        query_timeout_(-1),
        frozen_version_(-1),
        plan_cache_policy_(OB_USE_PLAN_CACHE_INVALID),
        use_jit_policy_(OB_USE_JIT_INVALID),
        force_trace_log_(false),
        log_level_(),
        parallel_(-1),
        force_refresh_lc_(false)
  {}

  ObQueryHint(const common::ObConsistencyLevel read_consistency, const int64_t query_timeout,
      const int64_t frozen_version, const ObPlanCachePolicy plan_cache_policy, const ObUseJitPolicy use_jit_policy,
      const bool force_trace_log, const common::ObString& log_level, const int64_t parallel, bool force_refresh_lc)
      : read_consistency_(read_consistency),
        dummy_(),
        query_timeout_(query_timeout),
        frozen_version_(frozen_version),
        plan_cache_policy_(plan_cache_policy),
        use_jit_policy_(use_jit_policy),
        force_trace_log_(force_trace_log),
        log_level_(log_level),
        parallel_(parallel),
        force_refresh_lc_(force_refresh_lc)
  {}

  int deep_copy(const ObQueryHint& other, common::ObIAllocator& allocator);

  void reset();

  TO_STRING_KV(K_(read_consistency), K_(query_timeout), K_(frozen_version), K_(plan_cache_policy), K_(use_jit_policy),
      K_(force_trace_log), K_(log_level), K_(parallel));

  common::ObConsistencyLevel read_consistency_;
  lib::UNFDummy<1> dummy_;
  int64_t query_timeout_;
  int64_t frozen_version_;
  ObPlanCachePolicy plan_cache_policy_;
  ObUseJitPolicy use_jit_policy_;
  bool force_trace_log_;
  common::ObString log_level_;
  int64_t parallel_;
  bool force_refresh_lc_;
};

static const int8_t OB_UNSET_AGGREGATE_TYPE = 0;
static const int8_t OB_USE_HASH_AGGREGATE = 0x1 << 1;
static const int8_t OB_NO_USE_HASH_AGGREGATE = 0x1 << 2;

static const int8_t OB_MONITOR_TRACING = 0x1 << 1;
static const int8_t OB_MONITOR_STAT = 0x1 << 2;

struct ObMonitorHint {
  ObMonitorHint() : id_(0), flags_(0){};
  ~ObMonitorHint() = default;
  uint64_t id_;
  uint64_t flags_;
  TO_STRING_KV(K_(id), K_(flags));
};

struct ObTablesInHint {
  ObTablesInHint() : distributed_(false)
  {}
  common::ObString qb_name_;
  common::ObSEArray<ObTableInHint, 3> tables_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> join_order_pairs_;  // record leading hint pairs
  bool distributed_;
  TO_STRING_KV(K_(qb_name), K_(tables));
};

struct ObTablesIndex {
  ObTablesIndex()
  {}
  common::ObSEArray<uint64_t, 3> indexes_;
  TO_STRING_KV(K_(indexes));
};

struct ObPQDistributeHintMethod {
  bool is_join_ = false;
  ObPQDistributeMethod::Type load_ = ObPQDistributeMethod::NONE;
  ObPQDistributeMethod::Type outer_ = ObPQDistributeMethod::NONE;
  ObPQDistributeMethod::Type inner_ = ObPQDistributeMethod::NONE;

  VIRTUAL_TO_STRING_KV(K_(is_join), K_(load), K_(outer), K_(inner));
};

struct ObOrgPQDistributeHint : public ObPQDistributeHintMethod {
  common::ObSEArray<ObTableInHint, 3> tables_;
  INHERIT_TO_STRING_KV("method", ObPQDistributeHintMethod, K_(tables));
};

struct ObOrgPQMapHint {
  ObOrgPQMapHint() : table_(), use_pq_map_(false)
  {}
  ObTableInHint table_;
  bool use_pq_map_;
  TO_STRING_KV(N_TABLE_NAME, table_, K_(use_pq_map));
};

struct ObQBNamePQMapHint : public ObOrgPQMapHint {
  ObQBNamePQMapHint()
  {}
  ObQBNamePQMapHint(const common::ObString qb_name, const ObOrgPQMapHint& hint)
      : ObOrgPQMapHint(hint), qb_name_(qb_name)
  {}
  bool distributed_ = false;
  common::ObString qb_name_;
  INHERIT_TO_STRING_KV("org_hint", ObOrgPQMapHint, K_(qb_name), K_(distributed));
};

struct ObQBNamePQDistributeHint : public ObOrgPQDistributeHint {
  ObQBNamePQDistributeHint(){};
  ObQBNamePQDistributeHint(const common::ObString qb_name, const ObOrgPQDistributeHint& hint)
      : ObOrgPQDistributeHint(hint), qb_name_(qb_name)
  {}
  bool distributed_ = false;
  common::ObString qb_name_;
  INHERIT_TO_STRING_KV("org_hint", ObOrgPQDistributeHint, K_(qb_name), K_(distributed));
};

struct ObPQDistributeHint : public ObPQDistributeHintMethod {
  common::ObSEArray<uint64_t, 3> table_ids_;
  INHERIT_TO_STRING_KV("method", ObPQDistributeHintMethod, K_(table_ids));
};

struct ObPQDistributeIndex : public ObPQDistributeHintMethod {
  ObRelIds rel_ids_;
  INHERIT_TO_STRING_KV("method", ObPQDistributeHintMethod, K_(rel_ids));
};

struct ObPQMapHint {
  uint64_t table_id_ = common::OB_INVALID_ID;
  TO_STRING_KV(K_(table_id));
};

struct ObPQMapIndex {
  ObRelIds rel_ids_;
  TO_STRING_KV(K_(rel_ids));
};

struct ObStmtHint {
  // TODO yangz.yz.rewrite, ObStmtHint should have a member ObQueryHint, not contain every member of
  // query hint.
  enum TablesHint {
    LEADING = 0,
    USE_MERGE,
    NO_USE_MERGE,
    USE_HASH,
    NO_USE_HASH,
    USE_NL,
    NO_USE_NL,
    USE_BNL,
    NO_USE_BNL,
    USE_NL_MATERIALIZATION,
    NO_USE_NL_MATERIALIZATION,
    PX_JOIN_FILTER,
    NO_PX_JOIN_FILTER
  };

  enum RewriteHint {
    NO_EXPAND = 0,
    USE_CONCAT,
    UNNEST,
    NO_UNNEST,
    MERGE,
    NO_MERGE,
    PLACE_GROUPBY,
    NO_PLACE_GROUPBY,
    NO_PRED_DEDUCE,
  };

  ObStmtHint()
      : converted_(false),
        read_static_(false),
        no_rewrite_(false),
        frozen_version_(-1),
        topk_precision_(-1),
        sharding_minimum_row_count_(0),
        query_timeout_(-1),
        hotspot_(false),
        join_ordered_(false),
        read_consistency_(common::INVALID_CONSISTENCY),
        plan_cache_policy_(OB_USE_PLAN_CACHE_INVALID),
        use_jit_policy_(OB_USE_JIT_INVALID),
        dummy_(),
        use_late_mat_(OB_LATE_MATERIALIZATION_INVALID),
        force_trace_log_(false),
        aggregate_(OB_UNSET_AGGREGATE_TYPE),
        bnl_allowed_(false),
        max_concurrent_(UNSET_MAX_CONCURRENT),
        only_concurrent_hint_(false),
        has_hint_exclude_concurrent_(false),
        enable_lock_early_release_(false),
        force_refresh_lc_(false),
        log_level_(),
        parallel_(UNSET_PARALLEL),
        use_px_(ObUsePxHint::NOT_SET),
        pdml_option_(ObPDMLOption::NOT_SPECIFIED),
        has_px_hint_(false),
        rewrite_hints_(),
        no_expand_(),
        use_concat_(),
        v_merge_(),
        no_v_merge_(),
        unnest_(),
        no_unnest_(),
        place_groupby_(),
        no_place_groupby_(),
        no_pred_deduce_(),
        use_expand_(ObUseRewriteHint::NOT_SET),
        use_view_merge_(ObUseRewriteHint::NOT_SET),
        use_unnest_(ObUseRewriteHint::NOT_SET),
        use_place_groupby_(ObUseRewriteHint::NOT_SET),
        use_pred_deduce_(ObUseRewriteHint::NOT_SET),
        is_table_name_error_(false),
        query_hint_conflict_map_(0),
        org_indexes_(),
        join_order_(),
        join_order_pairs_(),
        use_merge_(),
        no_use_merge_(),
        use_hash_(),
        no_use_hash_(),
        use_nl_(),
        no_use_nl_(),
        use_bnl_(),
        no_use_bnl_(),
        use_nl_materialization_(),
        no_use_nl_materialization_(),
        use_merge_order_pairs_(),
        no_use_merge_order_pairs_(),
        use_hash_order_pairs_(),
        no_use_hash_order_pairs_(),
        use_nl_order_pairs_(),
        no_use_nl_order_pairs_(),
        use_bnl_order_pairs_(),
        no_use_bnl_order_pairs_(),
        use_nl_materialization_order_pairs_(),
        no_use_nl_materialization_order_pairs_(),
        px_join_filter_order_pairs_(),
        no_px_join_filter_order_pairs_(),
        subquery_hints_(),
        px_join_filter_(),
        no_px_join_filter_(),
        org_pq_distributes_(),
        indexes_(),
        join_order_ids_(),
        use_merge_ids_(),
        no_use_merge_ids_(),
        use_hash_ids_(),
        no_use_hash_ids_(),
        use_nl_ids_(),
        no_use_nl_ids_(),
        use_bnl_ids_(),
        no_use_bnl_ids_(),
        use_nl_materialization_ids_(),
        no_use_nl_materialization_ids_(),
        part_hints_(),
        access_paths_(),
        pq_distributes_(),
        px_join_filter_ids_(),
        no_px_join_filter_ids_(),
        use_merge_idxs_(),
        no_use_merge_idxs_(),
        use_hash_idxs_(),
        no_use_hash_idxs_(),
        use_nl_idxs_(),
        no_use_nl_idxs_(),
        use_bnl_idxs_(),
        no_use_bnl_idxs_(),
        use_nl_materialization_idxs_(),
        no_use_nl_materialization_idxs_(),
        pq_distributes_idxs_(),
        pq_map_idxs_(),
        px_join_filter_idxs_(),
        no_px_join_filter_idxs_(),
        valid_use_merge_idxs_(),
        valid_no_use_merge_idxs_(),
        valid_use_hash_idxs_(),
        valid_no_use_hash_idxs_(),
        valid_use_nl_idxs_(),
        valid_no_use_nl_idxs_(),
        valid_use_bnl_idxs_(),
        valid_no_use_bnl_idxs_(),
        valid_use_nl_materialization_idxs_(),
        valid_no_use_nl_materialization_idxs_(),
        valid_pq_distributes_idxs_(),
        valid_pq_maps_idxs_()
  {}

  static const common::ObConsistencyLevel UNSET_CONSISTENCY = common::INVALID_CONSISTENCY;
  static const int64_t UNSET_QUERY_TIMEOUT = -1;
  static const int64_t UNSET_MAX_CONCURRENT = -1;
  static const int64_t UNSET_PARALLEL = -1;
  static const int64_t DEFAULT_PARALLEL = 1;

  static const char* FULL_HINT;
  static const char* INDEX_HINT;
  static const char* NO_INDEX_HINT;
  static const char* MULTILINE_INDENT;
  static const char* ONELINE_INDENT;
  static const char* QB_NAME;
  static const char* LEADING_HINT;
  static const char* ORDERED_HINT;
  static const char* READ_CONSISTENCY;
  static const char* HOTSPOT;
  static const char* TOPK;
  static const char* QUERY_TIMEOUT;
  static const char* FROZEN_VERSION;
  static const char* USE_PLAN_CACHE;
  static const char* USE_JIT_AUTO;
  static const char* USE_JIT_FORCE;
  static const char* NO_USE_JIT;
  static const char* NO_REWRITE;
  static const char* TRACE_LOG;
  static const char* LOG_LEVEL;
  static const char* TRANS_PARAM_HINT;
  static const char* USE_HASH_AGGREGATE;
  static const char* NO_USE_HASH_AGGREGATE;
  static const char* USE_LATE_MATERIALIZATION;
  static const char* NO_USE_LATE_MATERIALIZATION;
  static const char* MAX_CONCURRENT;
  static const char* PRIMARY_KEY;
  static const char* QB_NAME_HINT;
  static const char* PARALLEL;
  static const char* USE_PX;
  static const char* NO_USE_PX;
  static const char* PQ_DISTRIBUTE;
  static const char* USE_NL_MATERIAL;
  static const char* NO_USE_NL_MATERIAL;
  static const char* PQ_MAP;
  static const char* NO_EXPAND_HINT;
  static const char* USE_CONCAT_HINT;
  static const char* VIEW_MERGE_HINT;
  static const char* NO_VIEW_MERGE_HINT;
  static const char* UNNEST_HINT;
  static const char* NO_UNNEST_HINT;
  static const char* PLACE_GROUP_BY_HINT;
  static const char* NO_PLACE_GROUP_BY_HINT;
  static const char* ENABLE_PARALLEL_DML_HINT;
  static const char* DISABLE_PARALLEL_DML_HINT;
  static const char* TRACING_HINT;
  static const char* STAT_HINT;
  static const char* PX_JOIN_FILTER_HINT;
  static const char* NO_PX_JOIN_FILTER_HINT;
  static const char* NO_PRED_DEDUCE_HINT;
  static const char* ENABLE_EARLY_LOCK_RELEASE_HINT;
  static const char* FORCE_REFRESH_LOCATION_CACHE_HINT;
  int assign(const ObStmtHint& other);
  ObQueryHint get_query_hint() const
  {
    return ObQueryHint(read_consistency_,
        query_timeout_,
        frozen_version_,
        plan_cache_policy_,
        use_jit_policy_,
        force_trace_log_,
        log_level_,
        parallel_,
        force_refresh_lc_);
  }
  const ObIndexHint* get_index_hint(const uint64_t table_id) const;
  ObIndexHint* get_index_hint(const uint64_t table_id);
  const ObPartHint* get_part_hint(const uint64_t table_id) const;

  // strategy to add whole hint from subquery
  // TODO rewrite. Be a function of ObQueryHint.
  static int add_whole_hint(ObDMLStmt* stmt);

  static int replace_name_in_hint(const ObSQLSessionInfo& session_info, ObDMLStmt& stmt,
      common::ObIArray<ObTableInHint>& arr, uint64_t table_id, common::ObString& to_db_name,
      common::ObString& to_table_name);

  static int add_single_table_view_hint(const ObSQLSessionInfo& session_info, ObDMLStmt* stmt, uint64_t table_id,
      common::ObString& to_db_name, common::ObString& to_table_name);

  int add_view_merge_hint(const ObStmtHint* view_hint);
  bool is_only_concurrent_hint() const
  {
    return only_concurrent_hint_;
  }

  int merge_view_hints();

  // add index hint as USE_TYPE
  int add_index_hint(const uint64_t table_id, const common::ObString& index_name);
  int print_global_hint_for_outline(planText& plan_text, const ObDMLStmt* stmt) const;
  int print_rewrite_hints_for_outline(planText& plan_text, const char* rewrite_hint, const ObIArray<ObString>& qb_names,
      const ObQueryCtx* query_ctx) const;
  int print_rewrite_hint_for_outline(planText& plan_text, int64_t hint_type, const ObString& qb_name) const;
  int print_outline_only_concurrent(common::ObIAllocator& allocator, common::ObString& outline);
  inline static const char* get_outline_indent(bool is_oneline)
  {
    return is_oneline ? ONELINE_INDENT : MULTILINE_INDENT;
  }
  inline bool enable_use_px() const
  {
    return ObUsePxHint::ENABLE == use_px_;
  }
  inline bool enable_no_expand() const
  {
    return ObUseRewriteHint::NO_EXPAND == use_expand_;
  }
  inline bool enable_use_concat() const
  {
    return ObUseRewriteHint::USE_CONCAT == use_expand_;
  }
  inline bool enable_view_merge() const
  {
    return ObUseRewriteHint::V_MERGE == use_view_merge_;
  }
  inline bool enable_no_view_merge() const
  {
    return ObUseRewriteHint::NO_V_MERGE == use_view_merge_;
  }
  inline bool enable_unnest() const
  {
    return ObUseRewriteHint::UNNEST == use_unnest_;
  }
  inline bool enable_no_unnest() const
  {
    return ObUseRewriteHint::NO_UNNEST == use_unnest_;
  }
  inline bool enable_place_groupby() const
  {
    return ObUseRewriteHint::PLACE_GROUPBY == use_place_groupby_;
  }
  inline bool enable_no_place_groupby() const
  {
    return ObUseRewriteHint::NO_PLACE_GROUPBY == use_place_groupby_;
  }
  inline bool enable_no_pred_deduce() const
  {
    return ObUseRewriteHint::NO_PRED_DEDUCE == use_pred_deduce_ || no_rewrite_;
  }
  common::ObIArray<uint64_t>* get_join_ids(TablesHint hint);

  inline bool disable_use_px() const
  {
    return ObUsePxHint::DISABLE == use_px_;
  }
  common::ObIArray<ObTableInHint>* get_join_tables(TablesHint hint);
  void reset_valid_join_type();
  int is_used_join_type(JoinAlgo join_algo, int32_t table_id_idx, bool& is_used) const;
  int has_table_member(const common::ObIArray<ObRelIds>& use_idxs, int32_t table_id_idx, bool& is_used) const;
  bool is_used_leading() const
  {
    return join_order_ids_.count() != 0;
  }
  bool is_topk_specified() const
  {
    return topk_precision_ > 0 || sharding_minimum_row_count_ > 0;
  }
  int is_used_in_leading(uint64_t table_id, bool& is_used) const;
  int64_t get_parallel_hint() const
  {
    return parallel_;
  }
  bool has_parallel_hint() const
  {
    return UNSET_PARALLEL != parallel_;
  }
  int is_need_print_use_px(planText& plan_text, bool& is_need) const;
  void set_table_name_error_flag(bool flag);
  bool is_hints_all_worked() const;
  ObPDMLOption get_pdml_option() const
  {
    return pdml_option_;
  }
  TO_STRING_KV("read_static", read_static_, "no_rewrite", no_rewrite_, "frozen_version", frozen_version_,
      "topk_precision", topk_precision_, "sharding_minimum_row_count", sharding_minimum_row_count_, "query_timeout",
      query_timeout_, "hotspot", hotspot_, N_INDEX, indexes_, "read_consistency", read_consistency_, "join_ordered",
      join_ordered_, N_JOIN_ORDER, join_order_ids_, "merge_hint_ids", use_merge_ids_, "hash_hint_ids", use_hash_ids_,
      "no_hash_hint_ids", no_use_hash_ids_, "nl_hint_ids", use_nl_ids_, "part_hints", part_hints_,
      "use_late_materialization", use_late_mat_, "log_level", log_level_, "max_concurrent", max_concurrent_,
      "only_concurrent_hint", only_concurrent_hint_, "has_hint_exclude_concurrent", has_hint_exclude_concurrent_,
      "parallel", parallel_, "use_px", use_px_, "use_expand", use_expand_, "use_view_merge", use_view_merge_,
      "use_unnest", use_unnest_, "use_place_groupby", use_place_groupby_, "use join filter", px_join_filter_,
      K_(org_pq_distributes), K_(pq_distributes), K_(pdml_option));

  bool converted_;
  bool read_static_;
  bool no_rewrite_;
  int64_t frozen_version_;
  int64_t topk_precision_;
  int64_t sharding_minimum_row_count_;
  int64_t query_timeout_;
  bool hotspot_;
  bool join_ordered_;
  common::ObConsistencyLevel read_consistency_;
  ObPlanCachePolicy plan_cache_policy_;  // this not used now
  ObUseJitPolicy use_jit_policy_;
  lib::UNFDummy<1> dummy_;  // replace obsolete `burried_point_'
  ObLateMaterializationMode use_late_mat_;
  bool force_trace_log_;  // if trace log is forced
  int8_t aggregate_;
  bool bnl_allowed_;
  int64_t max_concurrent_;
  bool only_concurrent_hint_;
  bool has_hint_exclude_concurrent_;
  bool enable_lock_early_release_;
  bool force_refresh_lc_;
  common::ObString log_level_;
  int64_t parallel_;
  ObUsePxHint::Type use_px_;
  ObPDMLOption pdml_option_;
  bool has_px_hint_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 3> rewrite_hints_;

  common::ObSEArray<ObString, 4> no_expand_;
  common::ObSEArray<ObString, 4> use_concat_;
  common::ObSEArray<ObString, 4> v_merge_;
  common::ObSEArray<ObString, 4> no_v_merge_;
  common::ObSEArray<ObString, 4> unnest_;
  common::ObSEArray<ObString, 4> no_unnest_;
  common::ObSEArray<ObString, 4> place_groupby_;
  common::ObSEArray<ObString, 4> no_place_groupby_;
  common::ObSEArray<ObString, 4> no_pred_deduce_;

  ObUseRewriteHint::Type use_expand_;
  ObUseRewriteHint::Type use_view_merge_;
  ObUseRewriteHint::Type use_unnest_;
  ObUseRewriteHint::Type use_place_groupby_;
  ObUseRewriteHint::Type use_pred_deduce_;

  bool is_table_name_error_;

  uint64_t query_hint_conflict_map_;

  common::ObSEArray<ObOrgIndexHint, 3> org_indexes_;

  common::ObSEArray<ObTableInHint, 3> join_order_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> join_order_pairs_;  // record leading hint pairs
  common::ObSEArray<ObTableInHint, 3> use_merge_;                       // record table infos to use merge for join
  common::ObSEArray<ObTableInHint, 3> no_use_merge_;                    // record table infos to no use merge for join
  common::ObSEArray<ObTableInHint, 3> use_hash_;                        // record table infos to use hash for join
  common::ObSEArray<ObTableInHint, 3> no_use_hash_;                     // record table infos to no use hash for join
  common::ObSEArray<ObTableInHint, 3> use_nl_;                          // record table infos to use nestedloop for join
  common::ObSEArray<ObTableInHint, 3> no_use_nl_;   // record table infos to no use nestedloop for join
  common::ObSEArray<ObTableInHint, 3> use_bnl_;     // record table infos to use block-nestedloop for join
  common::ObSEArray<ObTableInHint, 3> no_use_bnl_;  // record table infos to no use block-nestedloop for join
  common::ObSEArray<ObTableInHint, 3> use_nl_materialization_;     // record table infos to use material before nl join
  common::ObSEArray<ObTableInHint, 3> no_use_nl_materialization_;  // record table infos to no use material before nl
                                                                   // join
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> use_merge_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> no_use_merge_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> use_hash_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> no_use_hash_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> use_nl_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> no_use_nl_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> use_bnl_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> no_use_bnl_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> use_nl_materialization_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> no_use_nl_materialization_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> px_join_filter_order_pairs_;
  common::ObSEArray<std::pair<uint8_t, uint8_t>, 3> no_px_join_filter_order_pairs_;
  common::ObSEArray<const ObStmtHint*, 3> subquery_hints_;
  common::ObSEArray<ObTableInHint, 3> px_join_filter_;     // record table infos to px join filter
  common::ObSEArray<ObTableInHint, 3> no_px_join_filter_;  // record table infos to px join filter
  common::ObSEArray<ObOrgPQDistributeHint, 2> org_pq_distributes_;
  common::ObSEArray<ObOrgPQMapHint, 3> org_pq_maps_;

  common::ObSEArray<ObIndexHint, 16> indexes_;
  common::ObSEArray<uint64_t, 3> join_order_ids_;
  common::ObSEArray<ObTablesIndex, 3> use_merge_ids_;     // record table ids to use merge for join
  common::ObSEArray<ObTablesIndex, 3> no_use_merge_ids_;  // record table ids to no use merge for join
  common::ObSEArray<ObTablesIndex, 3> use_hash_ids_;      // record table ids to use hash for join
  common::ObSEArray<ObTablesIndex, 3> no_use_hash_ids_;   // record table ids to no use hash for join
  common::ObSEArray<ObTablesIndex, 3> use_nl_ids_;        // record table ids to use nestedloop for join
  common::ObSEArray<ObTablesIndex, 3> no_use_nl_ids_;     // record table ids to no use nestedloop for join
  common::ObSEArray<ObTablesIndex, 3> use_bnl_ids_;       // record table ids to use block-nestedloop for join
  common::ObSEArray<ObTablesIndex, 3> no_use_bnl_ids_;    // record table ids to no use block-nestedloop for join
  common::ObSEArray<ObTablesIndex, 3> use_nl_materialization_ids_;  // record table ids to use material before nl join
  common::ObSEArray<ObTablesIndex, 3> no_use_nl_materialization_ids_;  // record table ids to no use material before nl
                                                                       // join
  common::ObSEArray<ObPartHint, 3> part_hints_;                        // record partition ids for table
  common::ObSEArray<std::pair<uint64_t, ObAccessPathMethod>, 5> access_paths_;
  common::ObSEArray<ObPQDistributeHint, 2> pq_distributes_;
  common::ObSEArray<ObPQMapHint, 2> pq_maps_;                  // record table ids to use pq map for partition wise join
  common::ObSEArray<ObTablesIndex, 3> px_join_filter_ids_;     // record table ids to use px bloom filter
  common::ObSEArray<ObTablesIndex, 3> no_px_join_filter_ids_;  // record table ids to no use px bloom filter

  common::ObSEArray<ObRelIds, 3> use_merge_idxs_;  // for record in optimizer.
  common::ObSEArray<ObRelIds, 3> no_use_merge_idxs_;
  common::ObSEArray<ObRelIds, 3> use_hash_idxs_;
  common::ObSEArray<ObRelIds, 3> no_use_hash_idxs_;
  common::ObSEArray<ObRelIds, 3> use_nl_idxs_;
  common::ObSEArray<ObRelIds, 3> no_use_nl_idxs_;
  common::ObSEArray<ObRelIds, 3> use_bnl_idxs_;
  common::ObSEArray<ObRelIds, 3> no_use_bnl_idxs_;
  common::ObSEArray<ObRelIds, 3> use_nl_materialization_idxs_;
  common::ObSEArray<ObRelIds, 3> no_use_nl_materialization_idxs_;
  common::ObSEArray<ObPQDistributeIndex, 2> pq_distributes_idxs_;
  ObRelIds pq_map_idxs_;
  common::ObSEArray<ObRelIds, 3> px_join_filter_idxs_;
  common::ObSEArray<ObRelIds, 3> no_px_join_filter_idxs_;

  common::ObSEArray<ObRelIds, 3> valid_use_merge_idxs_;  // for record valid join type
  common::ObSEArray<ObRelIds, 3> valid_no_use_merge_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_use_hash_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_no_use_hash_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_use_nl_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_no_use_nl_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_use_bnl_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_no_use_bnl_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_use_nl_materialization_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_no_use_nl_materialization_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_pq_distributes_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_px_join_filter_idxs_;
  common::ObSEArray<ObRelIds, 3> valid_no_px_join_filter_idxs_;
  ObRelIds valid_pq_maps_idxs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStmtHint);
};

struct ObLoctionSensitiveHint {
  ObLateMaterializationMode use_late_mat_;

  ObLoctionSensitiveHint()
  {
    use_late_mat_ = OB_LATE_MATERIALIZATION_INVALID;
  }

  void reset()
  {
    use_late_mat_ = OB_LATE_MATERIALIZATION_INVALID;
  }

  void from(const ObStmtHint& hint)
  {
    if (hint.use_late_mat_ == OB_USE_LATE_MATERIALIZATION) {
      use_late_mat_ = hint.use_late_mat_;
    } else {
      use_late_mat_ = OB_NO_USE_LATE_MATERIALIZATION;
    }
  }

  void update_to(ObStmtHint& hint)
  {
    if (hint.use_late_mat_ == OB_LATE_MATERIALIZATION_INVALID) {
      hint.use_late_mat_ = use_late_mat_;
    }
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif
