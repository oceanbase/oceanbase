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

#ifndef OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_H
#define OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_H
#include "sql/optimizer/ob_join_order.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}

namespace sql
{
class ObLogPlan;

class ObJoinOrderEnum
{
public:
  ObJoinOrderEnum(ObLogPlan &plan, const common::ObIArray<ObRawExpr*> &quals);
  virtual ~ObJoinOrderEnum() = default;

  int enumerate();
  inline ObJoinOrder *get_output_join_order() { return output_join_order_; }
  inline ObLogPlan &get_plan() { return plan_; }
  inline common::ObIAllocator &get_allocator() { return get_plan().get_allocator(); }
  inline const common::ObIArray<ObConflictDetector*> &get_conflict_detectors() const { return conflict_detectors_; }
  inline const common::ObIArray<TableDependInfo> &get_table_depend_infos() const { return table_depend_infos_; }
  int64_t get_next_path_number() { return ++path_cnt_; }
  virtual int prune_path_global(const Path &path, bool &should_add) {
    should_add = true;
    return OB_SUCCESS;
  }

  JoinPath* alloc_join_path();
  int free(Path *&path);

  ObJoinOrder* create_join_order(PathType type);
  int free(ObJoinOrder *&join_order);

  typedef common::ObSEArray<ObJoinOrder*, 4, common::ModulePageAllocator, true> JoinOrderArray;
  static const int64_t RELORDER_HASHBUCKET_SIZE = 256;
  static const int64_t JOINPATH_SET_HASHBUCKET_SIZE = 3000;
  struct JoinPathPairInfo
  {
    JoinPathPairInfo()
    : left_ids_(),
      right_ids_() {}
    virtual ~JoinPathPairInfo() = default;
    uint64_t hash() const
    {
      return left_ids_.hash() + right_ids_.hash();
    }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    bool operator ==(const JoinPathPairInfo &src_info) const
    {
      return (left_ids_ == src_info.left_ids_) && (right_ids_ == src_info.right_ids_);
    }
    TO_STRING_KV(K_(left_ids), K_(right_ids));
    ObRelIds left_ids_;
    ObRelIds right_ids_;
  };
  typedef common::ObPooledAllocator<common::hash::HashMapTypes<ObRelIds, ObJoinOrder *>::AllocType,
                                    common::ObWrapperAllocator> IdOrderMapAllocer;
  typedef common::ObPooledAllocator<common::hash::HashSetTypes<JoinPathPairInfo>::AllocType,
                                    common::ObWrapperAllocator> JoinPathSetAllocer;
  typedef common::hash::ObHashMap<ObRelIds,
                                  ObJoinOrder *,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<ObRelIds>,
                                  common::hash::equal_to<ObRelIds>,
                                  IdOrderMapAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator,
                                  2> IdOrderMap;
  typedef common::hash::ObHashSet<JoinPathPairInfo,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<JoinPathPairInfo>,
                                  common::hash::equal_to<JoinPathPairInfo>,
                                  JoinPathSetAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator,
                                  2> JoinPathSet;

protected:
  virtual int init();
  virtual int inner_enumerate() = 0;

protected:
  inline const ObDMLStmt *get_stmt() const { return plan_.get_stmt(); }
  inline ObRawExprCopier *get_onetime_copier() { return get_plan().get_onetime_copier(); }
  inline ObOptimizerContext &get_optimizer_context() const { return plan_.get_optimizer_context(); }
  inline const ObLogPlanHint &get_log_plan_hint() const { return plan_.get_log_plan_hint(); }
  inline bool has_join_order_hint() { return !get_log_plan_hint().join_order_.leading_tables_.is_empty(); }
  inline const ObRelIds& get_leading_tables() { return get_log_plan_hint().join_order_.leading_tables_; }
  inline const LeadingInfo *get_leading_table_tree() const
  {
    return get_log_plan_hint().join_order_.top_leading_info_;
  }

  int check_join_hint(const ObRelIds &left_set,
                      const ObRelIds &right_set,
                      bool &match_hint,
                      bool &is_legal,
                      bool &is_strict_order);
  int process_join_pred(ObJoinOrder *left_tree,
                        ObJoinOrder *right_tree,
                        JoinInfo &join_info);

private:
  int get_base_table_items(const ObDMLStmt *stmt);
  int generate_base_level_join_order(const common::ObIArray<TableItem*> &table_items,
                                     common::ObIArray<ObJoinOrder*> &base_level);
  int init_function_table_depend_info(const ObIArray<TableItem*> &table_items);
  int init_json_table_depend_info(const ObIArray<TableItem*> &table_items);
  int init_json_table_column_depend_info(ObRelIds& depend_table_set,
                                                   TableItem* json_table,
                                                   const ObDMLStmt *stmt);
  int init_default_val_json(ObRelIds& depend_table_set,
                            ObRawExpr*& default_expr);
  int init_lateral_table_depend_info(const ObIArray<TableItem*> &table_items);
  int distribute_filters_to_baserels(ObIArray<ObJoinOrder*> &base_level,
                                     ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters);
  int mock_base_rel_detectors(ObJoinOrder *&base_rel);

  int init_bushy_tree_info(const ObIArray<TableItem*> &table_items);
  int init_bushy_tree_info_from_joined_tables(TableItem *table);
  int join_side_from_one_table(ObJoinOrder &child_tree,
                               ObIArray<ObRawExpr*> &join_pred,
                               bool &is_valid,
                               ObRelIds &intersect_rel_ids);
  int re_add_necessary_predicate(ObIArray<ObRawExpr*> &join_pred,
                                 ObIArray<ObRawExpr*> &new_join_pred,
                                 ObIArray<bool> &skip,
                                 EqualSets &equal_sets);
  int inner_remove_redundancy_pred(ObIAllocator *allocator,
                                   ObIArray<ObRawExpr*> &join_pred,
                                   EqualSets &equal_sets,
                                   ObJoinOrder *left_tree,
                                   ObJoinOrder *right_tree);

public:
  VIRTUAL_TO_STRING_KV(K_(conflict_detectors),
                       K_(table_depend_infos),
                       K_(bushy_tree_infos),
                       KPC_(output_join_order));

protected:
  ObLogPlan &plan_;
  const common::ObIArray<ObRawExpr*> &quals_;
  common::ObSEArray<TableItem*, 8, common::ModulePageAllocator, true> from_table_items_; // include from tables and semi tables
  common::ObSEArray<TableItem*, 8, common::ModulePageAllocator, true> base_table_items_; // all base tables

  common::ObSEArray<TableDependInfo, 8, common::ModulePageAllocator, true> table_depend_infos_;
  common::ObSEArray<ObRelIds, 8, common::ModulePageAllocator, true> bushy_tree_infos_;
  JoinOrderArray base_level_;

  ObConflictDetectorGenerator generator_;
  common::ObSEArray<ObConflictDetector*, 8, common::ModulePageAllocator, true> conflict_detectors_;

  ObJoinOrder *output_join_order_;

  common::ObSEArray<ObJoinOrder *, 8, common::ModulePageAllocator, true> recycled_join_orders_;
  common::ObSEArray<JoinPath *,  8, common::ModulePageAllocator, true> recycled_join_paths_;

  int64_t path_cnt_;

private:
  DISABLE_COPY_ASSIGN(ObJoinOrderEnum);
};

}
}

#endif // OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_H