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
#ifndef OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_IDP_H
#define OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_IDP_H
#include "sql/optimizer/ob_join_order_enum.h"

namespace oceanbase
{
namespace sql
{

class ObJoinOrderEnumIDP: public ObJoinOrderEnum
{
public:
  ObJoinOrderEnumIDP(ObLogPlan &plan, const common::ObIArray<ObRawExpr*> &quals);
  virtual ~ObJoinOrderEnumIDP() = default;

protected:
  virtual int inner_enumerate() override;

private:
  int prepare_ordermap_pathset(const JoinOrderArray base_level);
  int init_idp(int64_t initial_idp_step,
               common::ObIArray<JoinOrderArray> &idp_join_rels,
               common::ObIArray<JoinOrderArray> &full_join_rels);
  int generate_join_levels_with_IDP(common::ObIArray<JoinOrderArray> &join_rels);
  int generate_join_levels_with_orgleading(common::ObIArray<JoinOrderArray> &join_rels);
  int inner_generate_join_levels_with_IDP(common::ObIArray<JoinOrderArray> &join_rels,
                                          bool ignore_hint);
  int do_one_round_idp(common::ObIArray<JoinOrderArray> &temp_join_rels,
                       uint32_t curr_idp_step,
                       bool ignore_hint,
                       uint32_t &real_base_level,
                       ObIDPAbortType &abort_type);
  int check_and_abort_curr_level_dp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                    uint32_t curr_level,
                                    ObIDPAbortType &abort_type);
  int check_and_abort_curr_round_idp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                     uint32_t curr_level,
                                     ObIDPAbortType &abort_type);
  int prepare_next_round_idp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                             uint32_t initial_idp_step,
                             ObJoinOrder *&best_order);
  int greedy_idp_best_order(uint32_t current_level,
                            common::ObIArray<JoinOrderArray> &idp_join_rels,
                            ObJoinOrder *&best_order);
  int process_join_level_info(const ObIArray<TableItem*> &table_items,
                              ObIArray<JoinOrderArray> &join_rels,
                              ObIArray<JoinOrderArray> &new_join_rels);
  int generate_join_order_with_table_tree(ObIArray<JoinOrderArray> &join_rels,
                                          TableItem *table,
                                          ObJoinOrder* &join_tree);
  int generate_single_join_level_with_DP(ObIArray<JoinOrderArray> &join_rels,
                                         uint32_t left_level,
                                         uint32_t right_level,
                                         uint32_t level,
                                         bool ignore_hint,
                                         const uint64_t curr_idp_step,
                                         ObIDPAbortType &abort_type);
  int inner_generate_join_order(ObIArray<JoinOrderArray> &join_rels,
                                ObJoinOrder *left_tree,
                                ObJoinOrder *right_tree,
                                uint32_t level,
                                bool hint_force_order,
                                bool delay_cross_product,
                                bool &is_valid_join,
                                ObJoinOrder *&join_tree);
  int check_detector_valid(ObJoinOrder *left_tree,
                           ObJoinOrder *right_tree,
                           const ObIArray<ObConflictDetector*> &valid_detectors,
                           ObJoinOrder *cur_tree,
                           bool &is_valid);
  int find_join_rel(ObRelIds& relids, ObJoinOrder *&join_rel);
  int check_need_gen_join_path(const ObJoinOrder *left_tree,
                               const ObJoinOrder *right_tree,
                               bool &need_gen);
  int check_need_bushy_tree(common::ObIArray<JoinOrderArray> &join_rels,
                            const int64_t level,
                            bool &need);

  inline uint64_t get_idp_reduction_threshold() const
  {
    return get_optimizer_context().get_idp_reduction_threshold();
  }

  static constexpr uint64_t IDP_MIN_STEP = 2;

private:
  IdOrderMapAllocer id_order_map_allocer_;
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  IdOrderMap relid_joinorder_map_;
  JoinPathSetAllocer join_path_set_allocer_;
  JoinPathSet join_path_set_;

private:
  DISABLE_COPY_ASSIGN(ObJoinOrderEnumIDP);
};

}
}

#endif // OCEANBASE_SQL_OB_JOIN_ORDER_ENUM_IDP_H