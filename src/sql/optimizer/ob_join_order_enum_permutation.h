/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_PERMUTATION_H
#define OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_PERMUTATION_H
#include "sql/optimizer/ob_join_order_enum.h"

namespace oceanbase
{
namespace sql
{

class ObJoinOrderEnumPermutation: public ObJoinOrderEnum
{
public:
  ObJoinOrderEnumPermutation(ObLogPlan &plan, const common::ObIArray<ObRawExpr*> &quals);
  virtual ~ObJoinOrderEnumPermutation() = default;

  virtual int prune_path_global(const Path &path, bool &should_add) override;
protected:
  virtual int init() override;
  virtual int inner_enumerate() override;

  struct JoinHelper
  {
    JoinHelper():
      left_tree_(nullptr),
      right_tree_(nullptr),
      join_tree_(nullptr),
      force_order_(false) {}

    void reuse()
    {
      left_tree_ = nullptr;
      right_tree_ = nullptr;
      join_tree_ = nullptr;
      join_info_.reuse();
      force_order_ = false;
    }

    int assign(const JoinHelper &other)
    {
      int ret = OB_SUCCESS;
      left_tree_ = other.left_tree_;
      right_tree_ = other.right_tree_;
      join_tree_ = other.join_tree_;
      force_order_ = other.force_order_;
      if (OB_FAIL(join_info_.assign(other.join_info_))) {
        SQL_LOG(WARN, "failed to assign", K(ret));
      }
      return ret;
    }

    inline bool is_valid() const
    {
      return OB_NOT_NULL(right_tree_) &&
             OB_NOT_NULL(join_tree_);
    }

    inline int64_t get_join_type_priority() const
    {
      int64_t ret = 3;
      if (left_tree_ == nullptr || !is_valid()) {
        // invalid join
        ret = 3;
      } else if (join_info_.where_conditions_.empty() &&
                 join_info_.on_conditions_.empty()) {
        // cross join
        ret = 2;
      } else if (join_info_.equal_join_conditions_.empty()) {
        // non equal join
        ret = 1;
      } else {
        // equal join
        ret = 0;
      }
      return ret;
    }

    int calc_cardinality();
    int compute_join_property();
    int generate_join_paths();

    TO_STRING_KV(KP_(left_tree),
                 KP_(right_tree),
                 KP_(join_tree),
                 KPC_(join_tree),
                 K_(join_info),
                 K_(force_order));

    ObJoinOrder *left_tree_;
    ObJoinOrder *right_tree_;
    ObJoinOrder *join_tree_;
    JoinInfo join_info_;
    bool force_order_;

    DISABLE_COPY_ASSIGN(JoinHelper);
  };

  struct GreedyParam
  {
    GreedyParam() {}

    virtual ~GreedyParam() = default;

    virtual bool compare(const JoinHelper &left, const JoinHelper &right) const = 0;

    DISABLE_COPY_ASSIGN(GreedyParam);
  };

  struct GreedyParamMinJoinCardinality : public GreedyParam
  {
    GreedyParamMinJoinCardinality() : GreedyParam() {}

    virtual ~GreedyParamMinJoinCardinality() = default;

    virtual bool compare(const JoinHelper &left, const JoinHelper &right) const override;
    DISABLE_COPY_ASSIGN(GreedyParamMinJoinCardinality);
  };

  typedef common::ObPooledAllocator<common::hash::HashMapTypes<ObRelIds, double>::AllocType,
                                    common::ObWrapperAllocator> IdCostMapAllocer;
  typedef common::hash::ObHashMap<ObRelIds,
                                  double,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<ObRelIds>,
                                  common::hash::equal_to<ObRelIds>,
                                  IdCostMapAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator,
                                  2> IdCostMap;

  struct JoinOrderCostInfo
  {
    JoinOrderCostInfo(ObIAllocator &allocator):
      id_cost_map_allocer_(RELORDER_HASHBUCKET_SIZE,
                           ObWrapperAllocator(&allocator)),
      bucket_allocator_wrapper_(&allocator) {}

    ObSEArray<double, 10, common::ModulePageAllocator, true> best_cardinality_;
    ObSEArray<double, 10, common::ModulePageAllocator, true> best_cost_;

    IdCostMapAllocer id_cost_map_allocer_;
    common::ObWrapperAllocator bucket_allocator_wrapper_;
    IdCostMap id_cost_map_;

    int init(int64_t size);

    void reuse()
    {
      best_cardinality_.reuse();
      best_cost_.reuse();
      if (id_cost_map_.created()) {
        id_cost_map_.reuse();
      }
    }

    TO_STRING_KV(K_(best_cardinality), K_(best_cost));
    DISABLE_COPY_ASSIGN(JoinOrderCostInfo);
  };

  inline JoinOrderCostInfo &get_cost_info(const Path *path)
  {
    bool check_pipeline = OB_NOT_NULL(path) && path->is_nl_style_pipelined_path() &&
                          OB_NOT_NULL(get_stmt()) && get_stmt()->has_limit();
    if (check_pipeline && OB_NOT_NULL(get_stmt()) && get_stmt()->has_order_by()) {
      check_pipeline = check_pipeline && (path->get_interesting_order_info() & OrderingFlag::ORDERBY_MATCH);
    }
    return check_pipeline ? pipeline_cost_info_ : normal_cost_info_;
  }

private:
  inline bool use_leading_hint() const { return nullptr != leading_tree_; }

  int init_outer_join_info(TableItem *table);
  int init_join_depend_info(const TableItem &table, const TableItem &depend_table);

  int do_enumerate();
  int flatten_base_join_orders(ObIArray<ObJoinOrder *> &base_levels);

  int init_join_tree_from_hint(ObJoinOrder *&leading_join_order);
  int recursive_init_join_tree_from_hint(const LeadingInfo *leading_info, ObJoinOrder *&leading_join_order);

  int recursive_append_base_levels(TableItem *table_item,
                                   ObIArray<ObJoinOrder *> &base_levels);
  int recursive_enumerate(TableItem *table_item, ObJoinOrder *&top_level);
  int reduce_join_orders(const ObIArray<ObJoinOrder *> &base_levels,
                         ObJoinOrder *&top_level,
                         bool use_leading = false);

  int init_permutation(const ObIArray<ObJoinOrder *> &base_levels,
                       const GreedyParam &greedy_method);
  int generate_initial_join_tree(const ObIArray<ObJoinOrder *> &candidate_base_levels,
                                 const GreedyParam &greedy_method);
  int get_current_join_node(const ObIArray<ObJoinOrder *> &candi_nodes,
                            const GreedyParam &greedy_method,
                            ObJoinOrder *pre_join_tree,
                            int64_t cur_level,
                            JoinHelper &cur_join_helper);
  int greedy_best_join_node(const ObIArray<JoinHelper> &candi_helpers,
                            const GreedyParam &greedy_method,
                            int64_t &best_idx);
  int get_candidate_leading_base_levels(const ObIArray<ObJoinOrder *> &base_levels,
                                        ObIArray<ObJoinOrder *> &leading_base_levels);
  int initial_permutation_status();


  int fill_join_helper(ObJoinOrder *cur_tree,
                       ObJoinOrder *next_order,
                       const int64_t level,
                       JoinHelper &helper);
  int try_join_two_tree(ObJoinOrder *left_tree,
                        ObJoinOrder *right_tree,
                        ObJoinOrder *&new_join_tree);

  int find_base_level_join_rel(TableItem *table, ObJoinOrder *&join_order);
  int find_base_level_join_rel(const ObRelIds &table_set, ObJoinOrder *&join_order);

  int enumerate_permutations();
  int eval_cur_permutation(int64_t same_prefix, int64_t &valid_prefx);
  bool get_next_permutation(const int64_t pruning_prefix, int64_t &same_prefix);
  int update_best_cost();
  int update_best_cost(const ObJoinOrder *join_tree, int64_t level);
  int update_best_cost(const Path *path, int64_t level);
  void trace_best_info();

  int update_internal_cost(const ObJoinOrder *join_order);

  void reuse_permutation_infos()
  {
    size_ = 0;
    initial_permutation_.reuse();
    cur_permutation_.reuse();
    join_helpers_.reuse();
    cur_top_tree_ = nullptr;
    eval_level_ = -1;
    unaccepted_levels_.clear_all();
    global_contain_normal_nl_ = true;
    global_has_none_equal_join_ = true;
    use_leading_ = false;
    normal_cost_info_.reuse();
    pipeline_cost_info_.reuse();
  }

  ObJoinOrder *&get_join_tree(int64_t level) {
    return join_helpers_.at(level).join_tree_;
  }

  int check_path_sanity(const Path* path) const;

public:
  INHERIT_TO_STRING_KV("JoinOrderEnum", ObJoinOrderEnum,
                       K_(size),
                       K_(initial_permutation),
                       K_(cur_permutation),
                       K_(use_leading),
                       K_(eval_level),
                       K_(normal_cost_info),
                       K_(pipeline_cost_info),
                       K_(join_helpers),
                       KPC_(cur_top_tree));

private:
  common::ObSEArray<ObRelIds, 10, common::ModulePageAllocator, true> leading_table_depend_infos_;

  ObJoinOrder *leading_tree_;
  bool use_leading_;

  int64_t size_;
  JoinOrderArray initial_permutation_;
  ObSEArray<int64_t, 10, common::ModulePageAllocator, true> cur_permutation_;
  ObSEArray<JoinHelper, 10, common::ModulePageAllocator, true> join_helpers_;

  ObJoinOrder *cur_top_tree_;

  bool ignore_hint_;
  bool global_contain_normal_nl_;
  bool global_has_none_equal_join_;
  bool global_has_expansion_join_;

  ObRelIds unaccepted_levels_;
  int64_t eval_level_;

  JoinOrderCostInfo normal_cost_info_;
  JoinOrderCostInfo pipeline_cost_info_;

  DISABLE_COPY_ASSIGN(ObJoinOrderEnumPermutation);
};

}
}

#endif // OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_PERMUTATION_H