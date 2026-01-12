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

#define USING_LOG_PREFIX SQL_JO
#include "sql/optimizer/ob_join_order_enum_idp.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

ObJoinOrderEnumIDP::ObJoinOrderEnumIDP(ObLogPlan &plan, const common::ObIArray<ObRawExpr*> &quals):
    ObJoinOrderEnum(plan, quals),
    id_order_map_allocer_(RELORDER_HASHBUCKET_SIZE,
                          ObWrapperAllocator(&plan.get_allocator())),
    bucket_allocator_wrapper_(&plan.get_allocator()),
    relid_joinorder_map_(),
    join_path_set_allocer_(JOINPATH_SET_HASHBUCKET_SIZE,
                           ObWrapperAllocator(&plan.get_allocator())),
    join_path_set_() {}

int ObJoinOrderEnumIDP::inner_enumerate()
{
  int ret = OB_SUCCESS;
  common::ObArray<JoinOrderArray> join_rels;
  int64_t join_level = base_level_.count(); //需要连接的层次数
  //初始化动规数据结构
  if (OB_UNLIKELY(join_level < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join level", K(ret), K(join_level));
  } else if (OB_FAIL(join_rels.prepare_allocate(join_level))) {
    LOG_WARN("failed to prepare allocate join rels", K(ret));
  } else if (OB_FAIL(join_rels.at(0).assign(base_level_))) {
    LOG_WARN("failed to assign base level join order", K(ret));
  } else if (OB_FAIL(prepare_ordermap_pathset(base_level_))) {
    LOG_WARN("failed to prepare order map and path set", K(ret));
  }

  //枚举join order
  //如果有leading hint就在这里按leading hint指定的join order枚举,
  //如果根据leading hint没有枚举到有效join order，就忽略hint重新枚举。
  if (OB_SUCC(ret)) {
    OPT_TRACE("START IDP");
    if (OB_FAIL(generate_join_levels_with_IDP(join_rels))) {
      LOG_WARN("failed to generate join levels with dynamic program", K(ret));
    } else if (join_rels.at(join_level - 1).count() < 1 &&
               OB_FAIL(generate_join_levels_with_orgleading(join_rels))) {
      LOG_WARN("failed to enum with greedy", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObJoinOrder *top_join_order = nullptr;
    if (1 != join_rels.at(join_level - 1).count()) {
      ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
      LOG_WARN("No final JoinOrder generated",
                K(ret), K(join_level), K(join_rels.at(join_level -1).count()));
    } else if (OB_ISNULL(top_join_order = join_rels.at(join_level -1).at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null join order", K(ret), K(top_join_order));
    } else if (OB_UNLIKELY(top_join_order->get_interesting_paths().empty())) {
      ret = OB_ERR_NO_PATH_GENERATED;
      LOG_WARN("No final join path generated", K(ret), KPC(top_join_order));
    } else {
      output_join_order_ = top_join_order;
      LOG_TRACE("succeed to generate join order", K(ret));
      OPT_TRACE("SUCCEED TO GENERATE JOIN ORDER, try path count:",
                        top_join_order->get_total_path_num(),
                        ",interesting path count:", top_join_order->get_interesting_paths().count());
      OPT_TRACE_TIME_USED;
      OPT_TRACE_MEM_USED;
    }
  }

  return ret;
}

int ObJoinOrderEnumIDP::init_idp(int64_t initial_idp_step,
                                 common::ObIArray<JoinOrderArray> &idp_join_rels,
                                 common::ObIArray<JoinOrderArray> &full_join_rels)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(idp_join_rels.prepare_allocate(initial_idp_step))) {
    LOG_WARN("failed to prepare allocate join rels", K(ret));
  } else if (relid_joinorder_map_.created() || join_path_set_.created()) {
    relid_joinorder_map_.reuse();
    join_path_set_.reuse();
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_join_rels.at(0).count(); ++i) {
      if (OB_FAIL(idp_join_rels.at(0).push_back(full_join_rels.at(0).at(i)))) {
        LOG_WARN("failed to init level 0 rels", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::prepare_ordermap_pathset(const JoinOrderArray base_level)
{
  int ret = OB_SUCCESS;
  if (!relid_joinorder_map_.created() &&
      OB_FAIL(relid_joinorder_map_.create(RELORDER_HASHBUCKET_SIZE,
                                          &id_order_map_allocer_,
                                          &bucket_allocator_wrapper_))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (!join_path_set_.created() &&
             OB_FAIL(join_path_set_.create(JOINPATH_SET_HASHBUCKET_SIZE,
                                          &join_path_set_allocer_,
                                          &bucket_allocator_wrapper_))) {
    LOG_WARN("create hash set failed", K(ret));
  } else {
    int64_t join_level = base_level.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < join_level; ++i) {
      ObJoinOrder *join_order = base_level.at(i);
      if (OB_ISNULL(join_order)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid join order", K(ret));
      } else if (OB_FAIL(relid_joinorder_map_.set_refactored(join_order->get_tables(), join_order))) {
        LOG_WARN("failed to add basic join orders into map", K(ret));
      }
    }
  }
  return ret;
}


int ObJoinOrderEnumIDP::generate_join_levels_with_IDP(common::ObIArray<JoinOrderArray> &join_rels)
{
  int ret = OB_SUCCESS;
  int64_t join_level = join_rels.count();
  if (has_join_order_hint() &&
      OB_FAIL(inner_generate_join_levels_with_IDP(join_rels,
                                                  false))) {
    LOG_WARN("failed to generate join levels with hint", K(ret));
  } else if (1 == join_rels.at(join_level - 1).count()) {
    //根据hint，枚举到了有效join order
    OPT_TRACE("succeed to generate join order with hint");
  } else if (OB_FAIL(inner_generate_join_levels_with_IDP(join_rels,
                                                         true))) {
    LOG_WARN("failed to generate join level in generate_join_orders", K(ret), K(join_level));
  }
  return ret;
}

int ObJoinOrderEnumIDP::generate_join_levels_with_orgleading(common::ObIArray<JoinOrderArray> &join_rels)
{
  int ret = OB_SUCCESS;
  ObArray<JoinOrderArray> temp_join_rels;
  int64_t join_level = join_rels.count();
  int64_t temp_join_level = from_table_items_.count();
  LOG_TRACE("idp start enum join order with orig leading", K(temp_join_level));
  if (OB_FAIL(process_join_level_info(from_table_items_,
                                      join_rels,
                                      temp_join_rels))) {
    LOG_WARN("failed to preprocess for linear", K(ret));
  } else if (OB_FAIL(generate_join_levels_with_IDP(temp_join_rels))) {
    LOG_WARN("failed to generate join level in generate_join_orders", K(ret), K(join_level));
  } else if (OB_FALSE_IT(join_rels.at(join_level - 1).reset())) {
  } else if (OB_FAIL(append(join_rels.at(join_level - 1),
                            temp_join_rels.at(temp_join_level -1)))) {
    LOG_WARN("failed to append join orders", K(ret));
  }
  return ret;
}

int ObJoinOrderEnumIDP::inner_generate_join_levels_with_IDP(common::ObIArray<JoinOrderArray> &join_rels,
                                                            bool ignore_hint)
{
  int ret = OB_SUCCESS;
  // stop plan enumeration if
  // a. illegal ordered hint
  // b. idp path num exceeds limitation
  // c. idp enumeration failed, under bushy cases, etc
  ObIDPAbortType abort_type = ObIDPAbortType::IDP_NO_ABORT;
  uint32_t join_level = 0;
  ObArray<JoinOrderArray> temp_join_rels;
  if (ignore_hint) {
    OPT_TRACE("IDP without hint");
  } else {
    OPT_TRACE("IDP with hint");
  }
  if (join_rels.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect empty join rels", K(ret));
  } else {
    join_level = join_rels.at(0).count();
    uint32_t initial_idp_step = join_level;
    if (OB_FAIL(init_idp(initial_idp_step, temp_join_rels, join_rels))) {
      LOG_WARN("failed to init idp", K(ret));
    } else {
      uint32_t curr_idp_step = initial_idp_step;
      uint32_t curr_level = 1;
      if (get_idp_reduction_threshold() <= 0) {
        curr_idp_step = IDP_MIN_STEP;
      }
      for (uint32_t i = 1; OB_SUCC(ret) && i < join_level &&
          ObIDPAbortType::IDP_NO_ABORT == abort_type; i += curr_level) {
        if (curr_idp_step > join_level - i + 1) {
          curr_idp_step = join_level - i + 1;
        }
        LOG_TRACE("start new round of idp", K(i), K(curr_idp_step));
        OPT_TRACE("start new round of idp", KV(i), KV(curr_idp_step));
        if (OB_FAIL(do_one_round_idp(temp_join_rels,
                                    curr_idp_step,
                                    ignore_hint,
                                    curr_level,
                                    abort_type))) {
          LOG_WARN("failed to do idp internal", K(ret));
        } else if (ObIDPAbortType::IDP_INVALID_HINT_ABORT == abort_type) {
          LOG_WARN("failed to do idp internal", K(ret));
        } else if (temp_join_rels.at(curr_level).count() < 1) {
          abort_type = ObIDPAbortType::IDP_ENUM_FAILED_ABORT;
          OPT_TRACE("failed to enum join order at current level", KV(curr_level));
        } else {
          OPT_TRACE("end new round of idp", K(i), K(curr_idp_step));
          ObJoinOrder *best_order = NULL;
          bool is_last_round = (i >= join_level - curr_level);
          if (abort_type == ObIDPAbortType::IDP_STOPENUM_EXPDOWN_ABORT) {
            curr_idp_step /= 2;
            curr_idp_step = max(curr_idp_step, 2U);
          } else if (abort_type == ObIDPAbortType::IDP_STOPENUM_LINEARDOWN_ABORT) {
            curr_idp_step -= 2;
            curr_idp_step = max(curr_idp_step, 2U);
          }
          OPT_TRACE("select best join order");
          if (OB_FAIL(greedy_idp_best_order(curr_level,
                                            temp_join_rels,
                                            best_order))) {
            LOG_WARN("failed to greedy idp best order", K(ret));
          } else if (OB_ISNULL(best_order)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected invalid best order", K(ret));
          } else if (!is_last_round &&
                    OB_FAIL(prepare_next_round_idp(temp_join_rels,
                                                   initial_idp_step,
                                                   best_order))) {
            LOG_WARN("failed to prepare next round of idp", K(curr_level));
          } else {
            if (abort_type == ObIDPAbortType::IDP_STOPENUM_EXPDOWN_ABORT ||
                abort_type == ObIDPAbortType::IDP_STOPENUM_LINEARDOWN_ABORT) {
              abort_type = ObIDPAbortType::IDP_NO_ABORT;
            }
            if (is_last_round) {
              if (OB_FALSE_IT(join_rels.at(join_level - 1).reset())) {
              } else if (OB_FAIL(append(join_rels.at(join_level - 1),
                                        temp_join_rels.at(curr_level)))) {
                LOG_WARN("failed to append join orders", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::do_one_round_idp(common::ObIArray<JoinOrderArray> &temp_join_rels,
                                         uint32_t curr_idp_step,
                                         bool ignore_hint,
                                         uint32_t &real_base_level,
                                         ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  real_base_level = 1;
  for (uint32_t base_level = 1; OB_SUCC(ret) && base_level < curr_idp_step &&
       ObIDPAbortType::IDP_NO_ABORT == abort_type; ++base_level) {
    LOG_TRACE("idp start base level plan enumeration", K(base_level), K(curr_idp_step));
    OPT_TRACE("idp start base level plan enumeration", K(base_level), K(curr_idp_step));
    for (uint32_t i = 0; OB_SUCC(ret) && i <= base_level/2 &&
         ObIDPAbortType::IDP_NO_ABORT == abort_type; ++i) {
      uint32_t right_level = i;
      uint32_t left_level = base_level - 1 - right_level;
      if (right_level > left_level) {
        //do nothing
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status fail", K(ret));
      } else if (OB_FAIL(generate_single_join_level_with_DP(temp_join_rels,
                                                            left_level,
                                                            right_level,
                                                            base_level,
                                                            ignore_hint,
                                                            curr_idp_step,
                                                            abort_type))) {
        LOG_WARN("failed to generate join order with dynamic program", K(ret));
      }
      bool need_bushy = false;
      if (OB_FAIL(ret) || right_level > left_level) {
      } else if (abort_type < ObIDPAbortType::IDP_NO_ABORT) {
        LOG_TRACE("abort idp join order generation",
                  K(left_level), K(right_level), K(abort_type));
      } else if (temp_join_rels.count() <= base_level) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("Index out of range", K(ret), K(temp_join_rels.count()), K(base_level));
      } else if (OB_FAIL(check_need_bushy_tree(temp_join_rels,
                                              base_level,
                                              need_bushy))) {
        LOG_WARN("failed to check need bushy tree", K(ret));
      } else if (need_bushy) {
        OPT_TRACE("no valid ZigZag tree or leading hint required, we will enumerate bushy tree");
      } else {
        //如果当前level已经枚举到了有效计划，默认关闭bushy tree
        OPT_TRACE("there is valid ZigZag tree, we will not enumerate bushy tree");
        break;
      }
    }
    if (OB_SUCC(ret)) {
      real_base_level = base_level;
      if (abort_type == ObIDPAbortType::IDP_NO_ABORT &&
          curr_idp_step > IDP_MIN_STEP &&
          OB_FAIL(check_and_abort_curr_round_idp(temp_join_rels,
                                                 base_level,
                                                 abort_type))) {
        LOG_WARN("failed to check and abort current round idp", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::check_and_abort_curr_level_dp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                                      uint32_t curr_level,
                                                      ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  if (curr_level < 0 || curr_level >= idp_join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(idp_join_rels.count()), K(curr_level));
  } else if (abort_type < ObIDPAbortType::IDP_NO_ABORT) {
    // do nothing
  } else {
    uint64_t total_path_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(curr_level).count(); ++i) {
      total_path_num += idp_join_rels.at(curr_level).at(i)->get_total_path_num();
    }
    if (OB_SUCC(ret)) {
      uint64_t stop_down_abort_limit = 2 * get_idp_reduction_threshold();
      if (total_path_num >= stop_down_abort_limit) {
        abort_type = ObIDPAbortType::IDP_STOPENUM_EXPDOWN_ABORT;
        OPT_TRACE("there is too much path, we will stop current level dp ",
        KV(total_path_num), KV(stop_down_abort_limit));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::check_and_abort_curr_round_idp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                                       uint32_t curr_level,
                                                       ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  if (curr_level < 0 || curr_level >= idp_join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(idp_join_rels.count()), K(curr_level));
  } else if (abort_type < ObIDPAbortType::IDP_NO_ABORT) {
    // do nothing
  } else {
    uint64_t total_path_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(curr_level).count(); ++i) {
      total_path_num += idp_join_rels.at(curr_level).at(i)->get_total_path_num();
    }
    if (OB_SUCC(ret)) {
      uint64_t stop_abort_limit = get_idp_reduction_threshold();
      if (total_path_num >= stop_abort_limit) {
        abort_type = ObIDPAbortType::IDP_STOPENUM_LINEARDOWN_ABORT;
        OPT_TRACE("there is too much path, we will stop current round idp ",
        KV(total_path_num), KV(stop_abort_limit));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::prepare_next_round_idp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                               uint32_t initial_idp_step,
                                               ObJoinOrder *&best_order)
{
  int ret = OB_SUCCESS;
  JoinOrderArray remained_rels;
  if (OB_ISNULL(best_order)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid best order", K(best_order), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < initial_idp_step; ++i) {
      remained_rels.reuse();
      for(int64_t j = 0; OB_SUCC(ret) && j < idp_join_rels.at(i).count(); ++j) {
        if (!idp_join_rels.at(i).at(j)->get_tables().overlap(best_order->get_tables())) {
          if (OB_FAIL(remained_rels.push_back(idp_join_rels.at(i).at(j)))) {
            LOG_WARN("failed to add level 0 rels", K(ret));
          }
        } else if (!idp_join_rels.at(i).at(j)->get_tables().is_subset(best_order->get_tables())) {
          if (OB_FAIL(free(idp_join_rels.at(i).at(j)))) {
            LOG_WARN("failed to free join order", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(idp_join_rels.at(i).assign(remained_rels))) {
          LOG_WARN("failed to add remained rels", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(idp_join_rels.at(0).push_back(best_order))) {
        LOG_WARN("failed to add selected tree rels", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::greedy_idp_best_order(uint32_t current_level,
                                              common::ObIArray<JoinOrderArray> &idp_join_rels,
                                              ObJoinOrder *&best_order)
{
  // choose best order from join_rels at current_level
  // can be based on cost/min cards/max interesting path num
  // currently based on cost
  int ret = OB_SUCCESS;
  best_order = NULL;
  if (current_level < 0 ||
      current_level >= idp_join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(idp_join_rels.count()), K(current_level));
  } else if (idp_join_rels.at(current_level).count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal join rels at current level", K(ret));
  }
  if (OB_SUCC(ret)) {
    Path *min_cost_path = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(current_level).count(); ++i) {
      ObJoinOrder * join_order = idp_join_rels.at(current_level).at(i);
      if (OB_ISNULL(join_order)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid join order found", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < join_order->get_interesting_paths().count(); ++j) {
          Path *path = join_order->get_interesting_paths().at(j);
          if (OB_ISNULL(path)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid join order found", K(ret));
          } else if (NULL == min_cost_path || min_cost_path->cost_ > path->cost_) {
            best_order = join_order;
            min_cost_path = path;
          }
        }
      }
    }
    OPT_TRACE("succeed to find best join order:", best_order, " best cost :", min_cost_path->cost_);
  }
  return ret;
}

/**
 * 进行join order枚举前，需要为所有的joined table生成join order
 * 然后把joined table当前整体进行join reorder
 **/
int ObJoinOrderEnumIDP::process_join_level_info(const ObIArray<TableItem*> &table_items,
                                                ObIArray<JoinOrderArray> &join_rels,
                                                ObIArray<JoinOrderArray> &new_join_rels)
{
  int ret = OB_SUCCESS;
  int64_t new_join_level = table_items.count();
  if (join_rels.empty()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("join_rels is empty", K(ret), K(join_rels.count()));
  } else if (OB_FAIL(new_join_rels.prepare_allocate(new_join_level))) {
    LOG_WARN("failed to prepare allocate join rels", K(ret));
  } else if (relid_joinorder_map_.created() || join_path_set_.created()) {
    // clear relid_joinorder map to avoid existing join order misuse
    relid_joinorder_map_.reuse();
    join_path_set_.reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ObJoinOrder *join_tree = NULL;
    if (OB_FAIL(generate_join_order_with_table_tree(join_rels,
                                                    table_items.at(i),
                                                    join_tree))) {
      LOG_WARN("failed to generate join order", K(ret));
    } else if (OB_ISNULL(join_tree)) {
      ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
      LOG_WARN("no valid join order generated", K(ret));
    } else if (OB_FAIL(new_join_rels.at(0).push_back(join_tree))) {
      LOG_WARN("failed to push back base level join order", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::generate_join_order_with_table_tree(ObIArray<JoinOrderArray> &join_rels,
                                                            TableItem *table,
                                                            ObJoinOrder* &join_tree)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  ObJoinOrder *left_tree = NULL;
  ObJoinOrder *right_tree = NULL;
  join_tree = NULL;
  if (OB_ISNULL(table) || join_rels.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!table->is_joined_table()) {
    //find base join rels
    ObIArray<ObJoinOrder *> &single_join_rels = join_rels.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && NULL == join_tree && i < single_join_rels.count(); ++i) {
      ObJoinOrder *base_rel = single_join_rels.at(i);
      if (OB_ISNULL(base_rel)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null base rel", K(ret));
      } else if (table->table_id_ == base_rel->get_table_id()) {
        join_tree = base_rel;
      }
    }
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(table))) {
    // do nothing
  } else if (OB_FAIL(SMART_CALL(generate_join_order_with_table_tree(join_rels,
                                                                    joined_table->left_table_,
                                                                    left_tree)))) {
    LOG_WARN("failed to generate join order", K(ret));
  } else if (OB_FAIL(SMART_CALL(generate_join_order_with_table_tree(join_rels,
                                                                    joined_table->right_table_,
                                                                    right_tree)))) {
    LOG_WARN("failed to generate join order", K(ret));
  } else if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
    LOG_WARN("no valid join order generated", K(ret));
  } else {
    bool is_valid_join = false;
    int64_t level = left_tree->get_tables().num_members() + right_tree->get_tables().num_members() - 1;
    if (OB_FAIL(inner_generate_join_order(join_rels,
                                          left_tree,
                                          right_tree,
                                          level,
                                          false,
                                          false,
                                          is_valid_join,
                                          join_tree))) {
      LOG_WARN("failed to generated join order", K(ret));
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::generate_single_join_level_with_DP(ObIArray<JoinOrderArray> &join_rels,
                                                           uint32_t left_level,
                                                           uint32_t right_level,
                                                           uint32_t level,
                                                           bool ignore_hint,
                                                           const uint64_t curr_idp_step,
                                                           ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  abort_type = ObIDPAbortType::IDP_NO_ABORT;
  if (join_rels.empty() ||
      left_level >= join_rels.count() ||
      right_level >= join_rels.count() ||
      level >= join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()),
                          K(left_level), K(right_level), K(level));
  } else {
    ObIArray<ObJoinOrder *> &left_rels = join_rels.at(left_level);
    ObIArray<ObJoinOrder *> &right_rels = join_rels.at(right_level);
    ObJoinOrder *left_tree = NULL;
    ObJoinOrder *right_tree = NULL;
    ObJoinOrder *join_tree = NULL;
    //优先枚举有连接条件的join order
    for (int64_t i = 0; OB_SUCC(ret) && i < left_rels.count() &&
         ObIDPAbortType::IDP_NO_ABORT == abort_type; ++i) {
      left_tree = left_rels.at(i);
      if (OB_ISNULL(left_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null join tree", K(ret));
      } else {
        OPT_TRACE("Permutations for Starting Table :", left_tree);
        OPT_TRACE_BEGIN_SECTION;
        for (int64_t j = 0; OB_SUCC(ret) && j < right_rels.count() &&
             ObIDPAbortType::IDP_NO_ABORT == abort_type; ++j) {
          right_tree = right_rels.at(j);
          bool match_hint = false;
          bool is_legal = true;
          bool is_strict_order = true;
          bool is_valid_join = false;
          if (OB_ISNULL(right_tree)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join tree", K(ret));
          } else if (!ignore_hint &&
                    OB_FAIL(check_join_hint(left_tree->get_tables(),
                                            right_tree->get_tables(),
                                            match_hint,
                                            is_legal,
                                            is_strict_order))) {
            LOG_WARN("failed to check join hint", K(ret));
          } else if (!is_legal) {
            //与hint冲突
            OPT_TRACE("join order conflict with leading hint,", left_tree, right_tree);
            LOG_TRACE("join order conflict with leading hint",
                      K(left_tree->get_tables()), K(right_tree->get_tables()));
          } else if (OB_FAIL(inner_generate_join_order(join_rels,
                                                        is_strict_order ? left_tree : right_tree,
                                                        is_strict_order ? right_tree : left_tree,
                                                        level,
                                                        match_hint,
                                                        !match_hint,
                                                        is_valid_join,
                                                        join_tree))) {
            LOG_WARN("failed to generate join order", K(level), K(ret));
          } else if (match_hint &&
                     !get_leading_tables().is_subset(left_tree->get_tables()) &&
                     !is_valid_join) {
            abort_type = ObIDPAbortType::IDP_INVALID_HINT_ABORT;
            OPT_TRACE("leading hint is invalid, stop idp ", left_tree, right_tree);
          } else if (curr_idp_step > IDP_MIN_STEP &&
                     OB_FAIL(check_and_abort_curr_level_dp(join_rels,
                                                           level,
                                                           abort_type))) {
            LOG_WARN("failed to check abort current dp", K(level), K(abort_type));
          } else {
            LOG_TRACE("succeed to generate join order", K(left_tree->get_tables()),
                       K(right_tree->get_tables()), K(is_valid_join), K(abort_type));
          }
          OPT_TRACE_TIME_USED;
          OPT_TRACE_MEM_USED;
        }
        OPT_TRACE_END_SECTION;
      }
    }
  }
  return ret;
}

/**
 * 使用动态规划算法
 * 通过join_rels[left_level]组合join_rels[right_level]
 * 来枚举join_rels[level]的有效计划
 */
int ObJoinOrderEnumIDP::inner_generate_join_order(ObIArray<JoinOrderArray> &join_rels,
                                                  ObJoinOrder *left_tree,
                                                  ObJoinOrder *right_tree,
                                                  uint32_t level,
                                                  bool hint_force_order,
                                                  bool delay_cross_product,
                                                  bool &is_valid_join,
                                                  ObJoinOrder *&join_tree)
{
  int ret = OB_SUCCESS;
  is_valid_join = false;
  join_tree = NULL;
  bool need_gen = true;
  if (join_rels.empty() || level >= join_rels.count() ||
      OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()),
                          K(left_tree), K(right_tree), K(level));
  } else {
    //依次检查每一个join info，是否有合法的连接
    ObSEArray<ObConflictDetector*, 4> valid_detectors;
    ObRelIds cur_relids;
    JoinInfo join_info;
    bool is_strict_order = true;
    bool is_detector_valid = true;
    if (left_tree->get_tables().overlap(right_tree->get_tables())) {
      //非法连接，do nothing
    } else if (OB_FAIL(cur_relids.add_members(left_tree->get_tables()))) {
      LOG_WARN("fail to add left tree' table ids", K(ret));
    } else if (OB_FAIL(cur_relids.add_members(right_tree->get_tables()))) {
      LOG_WARN("fail to add right tree' table ids", K(ret));
    } else if (OB_FAIL(find_join_rel(cur_relids, join_tree))) {
      LOG_WARN("fail to find join rel", K(ret), K(level));
    } else if (OB_FAIL(check_need_gen_join_path(left_tree, right_tree, need_gen))) {
      LOG_WARN("failed to find join tree pair", K(ret));
    } else if (!need_gen) {
      // do nothing
      is_valid_join = true;
      OPT_TRACE(left_tree, right_tree,
      " is legal, and has join path cache, no need to generate join path");
    } else if (OB_FAIL(ObConflictDetector::choose_detectors(left_tree->get_tables(),
                                                            right_tree->get_tables(),
                                                            left_tree->get_conflict_detectors(),
                                                            right_tree->get_conflict_detectors(),
                                                            table_depend_infos_,
                                                            conflict_detectors_,
                                                            valid_detectors,
                                                            delay_cross_product,
                                                            is_strict_order))) {
      LOG_WARN("failed to choose join info", K(ret));
    } else if (valid_detectors.empty()) {
      OPT_TRACE("there is no valid join condition for ", left_tree, "join", right_tree);
      LOG_TRACE("there is no valid join info for ", K(left_tree->get_tables()),
                                                    K(right_tree->get_tables()));
    } else if (NULL != join_tree &&
               OB_FAIL(check_detector_valid(left_tree,
                                            right_tree,
                                            valid_detectors,
                                            join_tree,
                                            is_detector_valid))) {
      LOG_WARN("failed to check detector valid", K(ret));
    } else if (!is_detector_valid) {
      OPT_TRACE("join tree will be remove: ", left_tree, "join", right_tree);
    } else if (OB_FAIL(ObConflictDetector::merge_join_info(valid_detectors,
                                                           join_info))) {
      LOG_WARN("failed to merge join info", K(ret));
    } else if (OB_FAIL(process_join_pred(left_tree, right_tree, join_info))) {
      LOG_WARN("failed to preocess join pred", K(ret));
    } else if (NULL != join_tree && level <= 1 && !hint_force_order) {
      //level==1的时候，左右树都是单表，如果已经生成过AB的话，BA的path也已经生成了，没必要再次生成一遍BA
      is_valid_join = true;
      OPT_TRACE("path has been generated in level one");
    } else {
      if (!is_strict_order) {
        if (!hint_force_order) {
          std::swap(left_tree, right_tree);
        } else {
          //如果leading hint指定了join order，但是合法的join order与
          //leading hint指定的join order相反，那么应该把连接类型取反
          join_info.join_type_ = get_opposite_join_type(join_info.join_type_);
        }
      }
      JoinPathPairInfo pair;
      pair.left_ids_ = left_tree->get_tables();
      pair.right_ids_ = right_tree->get_tables();
      if (NULL == join_tree) {
        if(OB_ISNULL(join_tree = create_join_order(JOIN))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create join tree", K(ret));
        } else if (OB_FAIL(join_tree->init_join_order(left_tree,
                                                      right_tree,
                                                      join_info,
                                                      valid_detectors))) {
          LOG_WARN("failed to init join order", K(ret));
        } else if (OB_FAIL(join_tree->compute_join_property(left_tree,
                                                            right_tree,
                                                            join_info))) {
          LOG_WARN("failed to init join order", K(ret));
        } else if (OB_FAIL(join_rels.at(level).push_back(join_tree))) {
          LOG_WARN("failed to push back join order", K(ret));
        } else if (relid_joinorder_map_.created() &&
                   OB_FAIL(relid_joinorder_map_.set_refactored(join_tree->get_tables(), join_tree))) {
          LOG_WARN("failed to add table ids join order to hash map", K(ret));
        }
      }
      OPT_TRACE_TITLE("Now", left_tree, "join", right_tree, join_info);
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_FAIL(join_tree->calc_cardinality(left_tree,
                                                     right_tree,
                                                     join_info))) {
        LOG_WARN("failed to calc ambient card", K(ret));
      } else if (OB_FAIL(join_tree->generate_join_paths(*left_tree,
                                                        *right_tree,
                                                        join_info,
                                                        hint_force_order))) {
        LOG_WARN("failed to generate join paths for left_tree", K(level), K(ret));
      } else if (join_path_set_.created() &&
                 OB_FAIL(join_path_set_.set_refactored(pair))) {
        LOG_WARN("failed to add join path set", K(ret));
      } else {
        is_valid_join = true;
        LOG_TRACE("succeed to generate join order for ", K(left_tree->get_tables()),
                  K(right_tree->get_tables()), K(is_strict_order));
      }
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::check_detector_valid(ObJoinOrder *left_tree,
                                             ObJoinOrder *right_tree,
                                             const ObIArray<ObConflictDetector*> &valid_detectors,
                                             ObJoinOrder *cur_tree,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObConflictDetector*, 4> all_detectors;
  ObSEArray<ObConflictDetector*, 4> common_detectors;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(cur_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_detectors, left_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_detectors, right_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_detectors, valid_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::intersect(all_detectors, cur_tree->get_conflict_detectors(), common_detectors))) {
    LOG_WARN("failed to calc common detectors", K(ret));
  } else if (common_detectors.count() != all_detectors.count() ||
             common_detectors.count() != cur_tree->get_conflict_detectors().count()) {
    is_valid = false;
  }
  return ret;
}

int ObJoinOrderEnumIDP::find_join_rel(ObRelIds& relids, ObJoinOrder *&join_rel)
{
  int ret = OB_SUCCESS;
  join_rel = NULL;
  if (relid_joinorder_map_.created() &&
      OB_FAIL(relid_joinorder_map_.get_refactored(relids, join_rel))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get refactored", K(ret), K(relids));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::check_need_gen_join_path(const ObJoinOrder *left_tree,
                                                 const ObJoinOrder *right_tree,
                                                 bool &need_gen)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  need_gen = true;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input join order", K(left_tree), K(right_tree), K(ret));
  } else if (join_path_set_.created()) {
    JoinPathPairInfo pair;
    pair.left_ids_ = left_tree->get_tables();
    pair.right_ids_ = right_tree->get_tables();
    hash_ret = join_path_set_.exist_refactored(pair);
    if (OB_HASH_EXIST == hash_ret) {
      need_gen = false;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      // do nothing
    } else {
      ret = hash_ret != OB_SUCCESS ? hash_ret : OB_ERR_UNEXPECTED;
      LOG_WARN("failed to check hash set exsit", K(ret), K(hash_ret), K(pair));
    }
  }
  return ret;
}

int ObJoinOrderEnumIDP::check_need_bushy_tree(common::ObIArray<JoinOrderArray> &join_rels,
                                              const int64_t level,
                                              bool &need)
{
  int ret = OB_SUCCESS;
  need = false;
  if (level >= join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(level));
  } else if (join_rels.at(level).empty()) {
    need = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !need && i < bushy_tree_infos_.count(); ++i) {
    const ObRelIds &table_ids = bushy_tree_infos_.at(i);
    if (table_ids.num_members() != level + 1) {
      //do nothing
    } else {
      bool has_generated = false;
      int64_t N = join_rels.at(level).count();
      for (int64_t j = 0; OB_SUCC(ret) && !has_generated && j < N; ++j) {
        ObJoinOrder *join_order = join_rels.at(level).at(j);
        if (OB_ISNULL(join_order)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null join order", K(ret));
        } else if (table_ids.is_subset(join_order->get_tables())) {
          has_generated = true;
        }
      }
      if (OB_SUCC(ret)) {
        need = !has_generated;
      }
    }
  }
  return ret;
}