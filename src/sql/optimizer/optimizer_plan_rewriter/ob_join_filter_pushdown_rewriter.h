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

#ifndef OCEANBASE_SQL_OB_JOIN_FILTER_PUSHDOWN_REWRITER_H
#define OCEANBASE_SQL_OB_JOIN_FILTER_PUSHDOWN_REWRITER_H

#include "sql/optimizer/optimizer_plan_rewriter/ob_plan_visitor.h"
#include "sql/optimizer/ob_log_join.h"
#include "lib/hash/ob_hashset.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "sql/resolver/dml/ob_hint.h"
#include "sql/optimizer/ob_join_order.h"

namespace oceanbase
{
namespace sql
{
class ObLogPlan;
class OptSelectivityCtx;

// Forward declaration
struct JoinFilterMetaInfo;
struct JoinUseMetaInfo;
// Join filter pushdown specific context and result
struct JoinFilterPushdownContext : public RewriterContext {
  JoinFilterPushdownContext()
    : pushdown_rtfs_()
  { }

  virtual ~JoinFilterPushdownContext()
  { }

  bool inline is_empty() {
    return pushdown_rtfs_.empty();
  }
  static int init(ObIAllocator *allocator, JoinFilterPushdownContext* &new_context);
  int assign_to(ObIAllocator *allocator, JoinFilterPushdownContext* &new_context);
  int copy_from(JoinFilterPushdownContext* &new_context);
  int get_use_exprs(ObIArray<ObRawExpr*> &use_exprs);

  int set_through_subplan_scan(uint64_t filter_table_id);
  inline void reset() {
    pushdown_rtfs_.reset();
  }
  int merge_from(JoinFilterPushdownContext* &new_context);
  int add_runtime_filter(ObIAllocator* allocator,
                         int64_t msg_id,
                         ObLogicalOperator *origin_join_op,
                         ObRawExpr* create_expr,
                         ObRawExpr* use_expr,
                         bool is_partition_wise);
  int add_runtime_filter(ObIAllocator* allocator,
                         int64_t msg_id,
                         ObLogicalOperator *origin_join_op,
                         ObRawExpr* create_expr,
                         ObRawExpr* use_expr,
                         bool is_partition_wise,
                         uint64_t pushdown_level_count,
                         uint64_t filter_table_id);
  int map_and_filter(ObIAllocator *allocator,
                     ObSQLSessionInfo *session_info,
                     const EqualSets &equal_sets,
                     const ObRelIds &tableset_range);
  int map_and_filter(ObIAllocator *allocator,
                     sql::ObRawExprFactory *expr_factory,
                     ObSQLSessionInfo *session_info,
                     const ObIArray<ObRawExpr*> &from_exprs,
                     const ObIArray<ObRawExpr*> &to_exprs,
                     const ObRelIds &tableset_range);

  int filter_by_expr_range(const ObIArray<ObRawExpr*> &expr_range);

  template<typename Predicate>
  int filter_by_predicate(const Predicate &predicate)
  {
    int ret = OB_SUCCESS;
    ObSEArray<JoinFilterMetaInfo*, 4> filtered_pushdown_rtfs;
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_rtfs_.count(); ++i) {
      JoinFilterMetaInfo* rtf = pushdown_rtfs_.at(i);
      bool should_keep = false;
      if (OB_FAIL(predicate(rtf, should_keep))) {
        LOG_WARN("predicate function failed", K(ret));
      } else if (should_keep) {
        if (OB_FAIL(filtered_pushdown_rtfs.push_back(rtf))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      pushdown_rtfs_.reset();
      if (OB_FAIL(pushdown_rtfs_.assign(filtered_pushdown_rtfs))) {
        LOG_WARN("failed to assign array", K(ret));
      }
    }
    return ret;
  }

  common::ObSEArray<JoinFilterMetaInfo*, 4, common::ModulePageAllocator, true> pushdown_rtfs_;

  TO_STRING_KV(K_(pushdown_rtfs));
};

struct JoinFilterPushdownResult : RewriterResult {
  JoinFilterPushdownResult()
  { }
  virtual ~JoinFilterPushdownResult()
  {}
};

struct JoinUseMetaInfo {
  JoinUseMetaInfo()
    : table_id_(OB_INVALID_ID),
      table_op_id_(OB_INVALID_ID),
      sharding_(NULL),
      ref_table_id_(OB_INVALID_ID),
      index_id_(OB_INVALID_ID),
      row_count_(1.0),
      use_column_store_(false),
      use_das_(false),
      pushdown_filter_table_(),
      basic_table_meta_(NULL),
      update_table_meta_(NULL),
      selectivity_ctx_(NULL),
      equal_sets_(NULL),
      tableitem_(NULL)
  { }
  virtual ~JoinUseMetaInfo()
  {}

  static int init_from_tablescan(ObIAllocator* allocator,
                                JoinUseMetaInfo*& metainfo,
                                ObLogTableScan* tablescan);
  static int init_from_temp_table_access(ObIAllocator* allocator,
                                JoinUseMetaInfo*& metainfo,
                                ObLogTempTableAccess* tablescan);
  uint64_t table_id_;
  int64_t table_op_id_;
  ObShardingInfo *sharding_;      //join filter use基表的sharding
  uint64_t ref_table_id_;         //join filter use基表的ref table id
  uint64_t index_id_;             //index id for join filter use
  double row_count_;              //join filter use基表的output rows
  bool use_column_store_;
  bool use_das_;
  ObTableInHint pushdown_filter_table_;
  OptTableMeta * basic_table_meta_;
  OptTableMeta * update_table_meta_;
  OptSelectivityCtx * selectivity_ctx_;
  const EqualSets* equal_sets_;
  TableItem * tableitem_;
  TO_STRING_KV(
  K_(table_id),
  K_(use_das),
  K_(sharding),
  K_(ref_table_id),
  K_(index_id),
  K_(row_count),
  K_(use_column_store));
};

struct JoinFilterMetaInfo {
  JoinFilterMetaInfo()
    : msg_id_(-1),
      origin_join_op_(NULL),
      pushdown_level_count_(0),
      create_expr_(NULL),
      use_expr_(NULL),
      is_partition_wise_(false),
      filter_table_id_(OB_INVALID_ID)
  { }

  virtual ~JoinFilterMetaInfo()
  {}

  static int init(ObIAllocator *allocator, JoinFilterMetaInfo* &new_context);
  static int init(ObIAllocator* allocator,
                  JoinFilterMetaInfo *& meta_info,
                  int64_t msg_id,
                  ObLogicalOperator* origin_join_op_,
                  ObRawExpr* create_expr,
                  ObRawExpr* use_expr,
                  bool is_partition_wise,
                  uint64_t pushdown_level_count,
                  uint64_t filter_table_id);
  int64_t msg_id_;
  ObLogicalOperator *origin_join_op_;
  uint64_t pushdown_level_count_;
  ObRawExpr* create_expr_;
  ObRawExpr* use_expr_;
  bool is_partition_wise_;

  uint64_t filter_table_id_;      // for version compatible

  TO_STRING_KV(K_(msg_id),
  K_(origin_join_op),
  K_(pushdown_level_count),
  K_(create_expr),
  K_(use_expr),
  K_(is_partition_wise),
  K_(filter_table_id));
};

/*
 * checks whether the plan:
 * is parallel: any node has parallel > 2
 * has hash join
 */
class JoinFilterPushdownChecker : public SimplePlanVisitor<RewriterResult, RewriterContext> {
public:
  JoinFilterPushdownChecker()
    : has_join_(false),
      is_parallel_plan_(false)
  {}
  virtual ~JoinFilterPushdownChecker() {}
  int visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result) override;
  int visit_join(ObLogJoin * joinnode, RewriterContext* context, RewriterResult*& result) override;

  bool has_join_;
  bool is_parallel_plan_;
};

/*
 * for each join node:
 * check whehter join filter is valid (with valid producer)
 * put join filter info to corresponding join
 */
class JoinFilterPruneAndUpdateRewriter : public SimplePlanVisitor<RewriterResult, RewriterContext> {
public:
  JoinFilterPruneAndUpdateRewriter(common::ObIAllocator &allocator,
                                   sql::ObRawExprFactory &expr_factory,
                                   ObSQLSessionInfo *session_info,
                                   ObQueryCtx *query_ctx,
                                   int64_t max_creater_row_count,
                                   int64_t min_user_partiton_count)
    : allocator_(allocator),
      expr_factory_(expr_factory),
      session_info_(session_info),
      query_ctx_(query_ctx),
      max_creater_row_count_(max_creater_row_count),
      min_user_partition_count_(min_user_partiton_count),
      valid_rtfs_(),
      consumer_table_infos_(),
      valid_producer_cache_set_()
  {}
  virtual ~JoinFilterPruneAndUpdateRewriter() {}
  int visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result) override;
  int visit_join(ObLogJoin * joinnode, RewriterContext* context, RewriterResult*& result) override;
  int visit_temp_table_insert(ObLogTempTableInsert * temp_table_insert, RewriterContext* context, RewriterResult*& result) override;
  int init(common::ObIArray<JoinFilterMetaInfo*> &valid_rtfs,
           common::ObIArray<JoinUseMetaInfo*> &consumer_table_infos);
  int set_force_hint_infos(ObLogJoin *&join_op,
                           JoinFilterInfo* info,
                           JoinUseMetaInfo* &current_use_meta_info);
  int append_join_filter_info(ObLogJoin *& joinnode,
                              ObIArray<JoinFilterMetaInfo*> &rtfs,
                              JoinUseMetaInfo* use_meta_info,
                              ObIArray<JoinFilterInfo*> &joinfilter,
                              ObIArray<ObRawExpr*> &all_join_key_left_exprs);
  static int can_reuse_calc_id_expr(const DistAlgo join_dist_algo,
                                     ObLogJoin * joinnode,
                                     JoinFilterInfo* info,
                                     bool &can_reuse);
  static int find_through_shuffle(ObLogicalOperator * root,
                                  ObLogicalOperator*& right_path);
  int check_partition_join_filter_valid(const DistAlgo join_dist_algo,
                                        ObLogJoin * joinnode,
                                        JoinFilterInfo* join_filter_infos,
                                        JoinUseMetaInfo* &current_use_meta_infos);
  int check_normal_join_filter_valid(ObLogJoin * joinnode,
                                     JoinFilterInfo *join_filter_infos,
                                     JoinUseMetaInfo* &current_use_meta_infos,
                                     bool has_valid_producer);
  int build_join_filter_part_expr(ObLogJoin * joinnode,
                                  const int64_t ref_table_id,
                                  const common::ObIArray<ObRawExpr *> &lexprs ,
                                  const common::ObIArray<ObRawExpr *> &rexprs,
                                  ObShardingInfo *sharding_info,
                                  const EqualSets* equal_sets,
                                  ObRawExpr *&left_calc_part_id_expr,
                                  bool skip_subpart);
  int fill_join_filter_info(JoinUseMetaInfo* user_info,
                            JoinFilterInfo *&join_filter_info);
  int prune_partition_rtfs(ObIArray<JoinFilterInfo*> &joinfilterinfos);
  int calc_join_filter_selectivity(JoinUseMetaInfo* user_info,
                                   JoinFilterInfo* join_filter_info,
                                   ObLogJoin * joinnode,
                                   double &join_filter_selectivity);
  int calc_join_filter_sel_for_pk_join_fk(ObLogJoin * joinnode,
                                          JoinFilterInfo* info,
                                          double &join_filter_selectivity,
                                          bool &is_pk_join_fk);
  // Utility visitor to check if tree contains any filters
  // or limit/topn
  class FilterCheckerVisitor : public SimplePlanVisitor<RewriterResult, RewriterContext> {
  public:
    FilterCheckerVisitor(const common::hash::ObHashSet<uint64_t> &valid_table_ids_set,
                         common::hash::ObHashSet<uint64_t> &valid_producer_cache_set)
      : valid_table_ids_set_(valid_table_ids_set),
        valid_producer_cache_set_(valid_producer_cache_set),
        is_valid_(false) {}
    virtual ~FilterCheckerVisitor() {}
    int visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result) override;
    int visit_table_scan(ObLogTableScan * scannode, RewriterContext* context, RewriterResult*& result) override;
    int visit_temp_table_access(ObLogTempTableAccess * temp_table_access, RewriterContext* context, RewriterResult*& result) override;
    int visit_sort(ObLogSort * sortnode, RewriterContext* context, RewriterResult*& result) override;
    int visit_limit(ObLogLimit * limitnode, RewriterContext* context, RewriterResult*& result) override;

    bool inline is_valid() {
      return is_valid_;
    }

  private:
    const common::hash::ObHashSet<uint64_t> &valid_table_ids_set_;
    common::hash::ObHashSet<uint64_t> &valid_producer_cache_set_;
    bool is_valid_;
  };

  // Check if the subtree rooted at op contains any filters
  int check_has_filters(ObLogicalOperator *op, bool &has_filters);
  common::ObIAllocator &allocator_;
  sql::ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *session_info_;
  ObQueryCtx *query_ctx_;
  int64_t max_creater_row_count_;
  int64_t min_user_partition_count_;
  common::ObSEArray<JoinFilterMetaInfo*, 4, common::ModulePageAllocator, true> valid_rtfs_;
  common::ObSEArray<JoinUseMetaInfo*, 4, common::ModulePageAllocator, true> consumer_table_infos_;
  // valid msg id hash set
  common::hash::ObHashSet<int64_t> valid_msg_ids_set_;
  common::hash::ObHashSet<uint64_t> valid_table_ids_set_;
  common::hash::ObHashSet<uint64_t> valid_producer_cache_set_;
};
// Join filter pushdown rewriter implementation
class JoinFilterPushdownRewriter : public SimplePlanVisitor<JoinFilterPushdownResult, JoinFilterPushdownContext> {
public:
  JoinFilterPushdownRewriter(common::ObIAllocator &allocator,
                             sql::ObRawExprFactory &expr_factory,
                             ObSQLSessionInfo *session_info)
    : next_id_(0),
      allocator_(allocator),
      expr_factory_(expr_factory),
      session_info_(session_info),
      valid_rtfs_(),
      consumer_table_infos_()
  {}
  virtual ~JoinFilterPushdownRewriter() {}

  int visit_node(ObLogicalOperator * plannode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_join(ObLogJoin * joinnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_table_scan(ObLogTableScan * scannode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_exchange(ObLogExchange * exchangenode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_groupby(ObLogGroupBy * groupbynode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_expand(ObLogExpand * expandnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_material(ObLogMaterial * materialnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_distinct(ObLogDistinct * distinctnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_window_function(ObLogWindowFunction * windownode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_sort(ObLogSort * sortnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_limit(ObLogLimit * limitnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_topk(ObLogTopk * topnnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_monitoring_dump(ObLogMonitoringDump * monitoringdumpnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_set(ObLogSet * setnode, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_subplan_scan(ObLogSubPlanScan * subplanscan, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_subplan_filter(ObLogSubPlanFilter * subplanfilter, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int visit_temp_table_access(ObLogTempTableAccess * temp_table_access, JoinFilterPushdownContext* context, JoinFilterPushdownResult*& result) override;
  int extract_rtf_context_from_join(ObLogJoin *&joinnode,
                                    JoinFilterPushdownContext *&context);
  int handle_inner_join(ObLogJoin * joinnode,
                        JoinFilterPushdownContext* pushdown_context,
                        JoinFilterPushdownContext* join_context,
                        JoinFilterPushdownContext*& left_context,
                        JoinFilterPushdownContext*& right_context);
  int handle_outer_join(ObLogJoin * joinnode,
                        JoinFilterPushdownContext* pushdown_context,
                        JoinFilterPushdownContext* join_context,
                        JoinFilterPushdownContext*& outer_side_context,
                        JoinFilterPushdownContext*& inner_side_context,
                        const bool is_naaj);
  int rewrite_children(ObLogicalOperator * plannode,
                       JoinFilterPushdownContext* context);
  int pushdown_rtf_into_tablescan(ObLogTableScan * scannode,
                                  ObIArray<JoinFilterMetaInfo*> &rtfs,
                                  ObIArray<JoinFilterMetaInfo*> &pushdown_rtfs);
  common::ObIAllocator &get_allocator() const { return allocator_; }
  sql::ObRawExprFactory &get_expr_factory() const { return expr_factory_; }
  ObSQLSessionInfo* &get_session_info() {return session_info_;}
   // keeps all valid rtfs we added
   // rtf is identified by its id
   // however, we need a list instead of a set because if a rtf is used multiple times
   // multiple join create is needed
   // static common::hash::ObHashMap<int64_t, ConsumerInfos> valid_rtfs;
  int64_t next_id_;

  common::ObIAllocator &allocator_;
  sql::ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *session_info_;
  common::ObSEArray<JoinFilterMetaInfo*, 4, common::ModulePageAllocator, true> valid_rtfs_;
  common::ObSEArray<JoinUseMetaInfo*, 4, common::ModulePageAllocator, true> consumer_table_infos_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_JOIN_FILTER_PUSHDOWN_REWRITER_H
