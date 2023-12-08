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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_range_graph_generator.h"
#include "common/ob_smart_call.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_column_index_provider.h"


namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObRangeGraphGenerator::generate_range_graph(const ObIArray<ObRawExpr*> &exprs,
                                                ObExprRangeConverter &range_node_generator)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = nullptr;
  ObSEArray<ObRangeNode*, 4> range_nodes;
  ObSEArray<ObPriciseExprItem, 4> pricise_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRangeNode *range_node = nullptr;
    bool is_precise = false;
    int64_t max_offset = 0;
    if (OB_ISNULL(expr = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr");
    } else if (OB_FAIL(generate_range_node(expr, range_node_generator, range_node, 0, is_precise, max_offset))) {
      LOG_WARN("faield to generate range node", K(ret));
    } else if (range_nodes.push_back(range_node)) {
      LOG_WARN("failed to push back range node");
    } else if (expr->is_const_expr()) {
      // isolated const expr which can be used as startup filter. Consider it imprecise.
    } else if (is_precise && OB_FAIL(pricise_exprs.push_back(ObPriciseExprItem(expr, max_offset)))) {
      LOG_WARN("failed to push back pricise expr item");
    }
  }
  if (OB_SUCC(ret)) {
    ObRangeNode *final_range_node = nullptr;
    if (range_nodes.empty()) {
      if (OB_FAIL(range_node_generator.generate_always_true_or_false_node(true, final_range_node))) {
        LOG_WARN("failed to generate whole range");
      } else {
        final_range_node->node_id_ = 0;
        pre_range_graph_->set_range_size(1);
        pre_range_graph_->set_node_count(1);
        pre_range_graph_->set_range_head(final_range_node);
        pre_range_graph_->set_is_precise_get(false);
        pre_range_graph_->set_is_standard_range(true);
        pre_range_graph_->set_is_equal_range(false);
        pre_range_graph_->set_is_get(false);
        update_max_precise_offset(0);
      }
    } else if (OB_FAIL(and_range_nodes(range_nodes, ctx_.column_cnt_, final_range_node))) {
      LOG_WARN("failed to and range nodes");
    } else if (OB_FAIL(formalize_final_range_node(final_range_node))) {
      LOG_WARN("failed to formalize final range node");
    } else if (OB_FAIL(check_graph_type(final_range_node))) {
      LOG_WARN("check graph type failed", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_range_exprs(pricise_exprs))) {
      LOG_WARN("failed to fill range exprs", K(ret));
    } else if (OB_FAIL(generate_expr_final_info())) {
      LOG_WARN("failed to generate final exprs");
    }
  }
  return ret;
}

int ObRangeGraphGenerator::generate_range_node(ObRawExpr* expr,
                                               ObExprRangeConverter &range_node_generator,
                                               ObRangeNode *&range_node,
                                               int64_t expr_depth,
                                               bool &is_precise,
                                               int64_t &max_offset)
{
  int ret = OB_SUCCESS;
  range_node = nullptr;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr");
  } else if (T_OP_AND == expr->get_expr_type()) {
    if (OB_FAIL(generate_and_range_node(expr, range_node_generator, range_node, expr_depth+1, is_precise, max_offset))) {
      LOG_WARN("failed to generate and range node");
    }
  } else if (T_OP_OR == expr->get_expr_type()) {
    if (OB_FAIL(generate_or_range_node(expr, range_node_generator, range_node, expr_depth+1, is_precise, max_offset))) {
      LOG_WARN("failed to generate or range node");
    }
  } else if (OB_FAIL(range_node_generator.convert_expr_to_range_node(expr, range_node, expr_depth, is_precise))) {
    LOG_WARN("failed to generate range node");
  } else if (OB_ISNULL(range_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range node");
  } else {
    max_offset = range_node->max_offset_;
  }
  return ret;
}

int ObRangeGraphGenerator::generate_and_range_node(ObRawExpr *and_expr,
                                                   ObExprRangeConverter &range_node_generator,
                                                   ObRangeNode *&range_node,
                                                   int64_t expr_depth,
                                                   bool &is_precise,
                                                   int64_t &max_offset)
{
  int ret = OB_SUCCESS;
  is_precise = true;
  max_offset = 0;
  if (OB_ISNULL(and_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr");
  } else {
    ObSEArray<ObRangeNode*, 4> range_nodes;
    for (int64_t i = 0; OB_SUCC(ret) && i < and_expr->get_param_count(); ++i) {
      ObRawExpr *expr = and_expr->get_param_expr(i);
      ObRangeNode *tmp_node = nullptr;
      bool cur_precise = false;
      int64_t cur_max_offset = 0;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(i), KPC(and_expr));
      } else if (OB_FAIL(generate_range_node(expr, range_node_generator, tmp_node,
                                             expr_depth, cur_precise, cur_max_offset))) {
        LOG_WARN("failed to generate range node", K(ret));
      } else if (range_nodes.push_back(tmp_node)) {
        LOG_WARN("failed to push back range node");
      } else {
        is_precise &= cur_precise;
        max_offset = std::max(max_offset, cur_max_offset);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(and_range_nodes(range_nodes, ctx_.column_cnt_, range_node))) {
        LOG_WARN("failed to do and range nodes", K(ret));
      }
    }
  }
  return ret;
}

int ObRangeGraphGenerator::generate_or_range_node(ObRawExpr *or_expr,
                                                  ObExprRangeConverter &range_node_generator,
                                                  ObRangeNode *&range_node,
                                                  int64_t expr_depth,
                                                  bool &is_precise,
                                                  int64_t &max_offset)
{
  int ret = OB_SUCCESS;
  is_precise = true;
  max_offset = 0;
  if (OB_ISNULL(or_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr");
  } else {
    ObSEArray<ObRangeNode*, 4> range_nodes;
    for (int64_t i = 0; OB_SUCC(ret) && i < or_expr->get_param_count(); ++i) {
      ObRawExpr *expr = or_expr->get_param_expr(i);
      ObRangeNode *tmp_node = nullptr;
      bool cur_precise = false;
      int64_t cur_max_offset = 0;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(i), KPC(or_expr));
      } else if (OB_FAIL(generate_range_node(expr, range_node_generator, tmp_node,
                                             expr_depth, cur_precise, cur_max_offset))) {
        LOG_WARN("failed to generate range node", K(ret));
      } else if (range_nodes.push_back(tmp_node)) {
        LOG_WARN("failed to push back range node");
      } else {
        is_precise &= cur_precise;
        max_offset = std::max(max_offset, cur_max_offset);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(or_range_nodes(range_node_generator, range_nodes, ctx_.column_cnt_, range_node))) {
        LOG_WARN("failed to do and range nodes", K(ret));
      }
    }
  }
  return ret;
}

/**
 * 尝试合并多个and节点，要求满足如下条件
 *  1. 合并的range node不含or next节点
 *  2. 两个range node的参数必须存在交集或相邻
*/
int ObRangeGraphGenerator::and_range_nodes(ObIArray<ObRangeNode*> &range_nodes,
                                           const int64_t column_cnt,
                                           ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  if (range_nodes.count() > 1) {
    // sort range node as following order:
    //  1. always false
    //  2. always true
    //  3. node with smaller min_offset
    std::sort(&range_nodes.at(0), &range_nodes.at(0) + range_nodes.count(), RangeNodeCmp());
  }
  ObRangeNode *always_true_node = nullptr;
  ObRangeNode *last_node = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_nodes.count(); ++i) {
    ObRangeNode *cur_node = range_nodes.at(i);
    if (OB_ISNULL(cur_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null range node");
    } else if (cur_node->always_false_) {
      range_node = cur_node;
      break;
    } else if (cur_node->always_true_) {
      if (always_true_node == nullptr) {
        always_true_node = cur_node;
      }
      continue;
    } else if (range_node == nullptr) {
      range_node = cur_node;
      last_node = cur_node;
    } else if (cur_node->is_phy_rowid_ != last_node->is_phy_rowid_) {
      // physical rowid node can only intersect with pyhsical rowid node
      if (last_node->is_phy_rowid_) {
        // do nothing
      } else {
        range_node = cur_node;
        last_node = cur_node;
      }
    } else if (cur_node->min_offset_ == -1 || last_node->min_offset_ == -1) {
      and_link_range_node(last_node, cur_node);
      last_node = cur_node;
    } else if (nullptr == cur_node->and_next_ && nullptr == cur_node->or_next_ &&
               nullptr == last_node->and_next_ && nullptr == last_node->or_next_ &&
               !(cur_node->contain_in_ && last_node->contain_in_)) {
      bool merged = false;
      if (OB_FAIL(and_two_range_node(last_node, cur_node, column_cnt, merged))) {
        LOG_WARN("failed to and two range node");
      } else if (merged) {
        last_node->max_offset_ = std::max(last_node->max_offset_, cur_node->max_offset_);
      } else {
        and_link_range_node(last_node, cur_node);
        last_node = cur_node;
      };
    } else {
      and_link_range_node(last_node, cur_node);
      last_node = cur_node;
    }
  }
  if (OB_SUCC(ret) && range_node == nullptr) {
    if (OB_ISNULL(always_true_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected range node", K(range_nodes));
    } else {
      range_node = always_true_node;
    }
  }
  return ret;
}

int ObRangeGraphGenerator::and_two_range_node(ObRangeNode *&l_node,
                                              ObRangeNode *&r_node,
                                              const int64_t column_cnt,
                                              bool &is_merge)
{
  int ret = OB_SUCCESS;
  is_merge = false;
  if (OB_ISNULL(l_node) || OB_ISNULL(r_node) ||
      OB_UNLIKELY(l_node->min_offset_ > r_node->min_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range node", KPC(l_node), KPC(r_node));
  } else if (l_node->max_offset_ < r_node->min_offset_ - 1) {
    // only merge consistent node
  } else if (r_node->min_offset_ - 1 >= 0 && r_node->min_offset_ - 1 >= l_node->min_offset_ &&
             l_node->start_keys_[r_node->min_offset_ - 1] != l_node->end_keys_[r_node->min_offset_ - 1]) {
    // only merge previous offset has equal range node
    // for example `c1 > 1` (1, max; max, max) and `c2 < 2` (ept, min; ept, 2)
    //    will not be merged as (1, max; max, 2) which equals to (1, max; max, max).
    //    Instead we remain both (1, max; max, max) and (ept, min; ept, 2).
    //    (ept, min; ept, 2) can used to extract skip scan range.
  } else {
    bool merge_start = false;
    bool use_r_start = false;
    int64_t start_offset = 0;
    bool merge_end = false;
    bool use_r_end = false;
    int64_t end_offset = 0;
    for (int64_t i = r_node->min_offset_; !merge_start && i < column_cnt; ++i) {
      int64_t s1 = l_node->start_keys_[i];
      int64_t s2 = r_node->start_keys_[i];
      if (OB_UNLIKELY(s1 == OB_RANGE_EMPTY_VALUE || s2 == OB_RANGE_EMPTY_VALUE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected range node", K(i), K(s1), K(s2), K(l_node->min_offset_), K(r_node->min_offset_));
      } else if (s1 == OB_RANGE_MIN_VALUE) {
        if (s2 == OB_RANGE_MIN_VALUE) {
          continue;
        } else if (s2 == OB_RANGE_MAX_VALUE) {
          use_r_start = true;
          merge_start = true;
        } else if (s2 == OB_RANGE_NULL_VALUE) {
          use_r_start = true;
          merge_start = true;
        } else if (s2 < OB_RANGE_EXTEND_VALUE) {
          use_r_start = true;
          merge_start = true;
        }
      } else if (s1 == OB_RANGE_MAX_VALUE) {
        if (s2 == OB_RANGE_MIN_VALUE) {
          merge_start = true;
        } else if (s2 == OB_RANGE_MAX_VALUE) {
          continue;
        } else if (s2 == OB_RANGE_NULL_VALUE) {
          merge_start = true;
        } else if (s2 < OB_RANGE_EXTEND_VALUE) {
          merge_start = true;
        }
      } else if (s1 == OB_RANGE_NULL_VALUE) {
        if (s2 == OB_RANGE_MIN_VALUE) {
          merge_start = true;
        } else if (s2 == OB_RANGE_MAX_VALUE) {
          use_r_start = true;
          merge_start = true;
        } else if (s2 == OB_RANGE_NULL_VALUE) {
          continue;
        } else if (s2 < OB_RANGE_EXTEND_VALUE) {
          if (lib::is_oracle_mode()) {
            merge_start = true;
          } else {
            use_r_start = true;
            merge_start = true;
          }
        }
      } else if (s1 < OB_RANGE_EXTEND_VALUE) {
        if (s2 == OB_RANGE_MIN_VALUE) {
          merge_start = true;
        } else if (s2 == OB_RANGE_MAX_VALUE) {
          use_r_start = true;
          merge_start = true;
        } else if (s2 == OB_RANGE_NULL_VALUE) {
          if (lib::is_oracle_mode()) {
            use_r_start = true;
            merge_start = true;
          } else {
            merge_start = true;
          }
        } else if (s2 < OB_RANGE_EXTEND_VALUE) {
          // can not compare
          break;
        }
      }
      if (use_r_start) {
        start_offset = i;
      }
    }
    for (int64_t i = r_node->min_offset_; !merge_end && i < column_cnt; ++i) {
      int64_t e1 = l_node->end_keys_[i];
      int64_t e2 = r_node->end_keys_[i];
      if (OB_UNLIKELY(e1 == OB_RANGE_EMPTY_VALUE || e2 == OB_RANGE_EMPTY_VALUE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected range node", K(i), K(e1), K(e1));
      } else if (e1 == OB_RANGE_MIN_VALUE) {
        if (e2 == OB_RANGE_MIN_VALUE) {
          continue;
        } else if (e2 == OB_RANGE_MAX_VALUE) {
          merge_end = true;
        } else if (e2 == OB_RANGE_NULL_VALUE) {
          merge_end = true;
        } else if (e2 < OB_RANGE_EXTEND_VALUE) {
          merge_end = true;
        }
      } else if (e1 == OB_RANGE_MAX_VALUE) {
        if (e2 == OB_RANGE_MIN_VALUE) {
          use_r_end = true;
          merge_end = true;
        } else if (e2 == OB_RANGE_MAX_VALUE) {
          continue;
        } else if (e2 == OB_RANGE_NULL_VALUE) {
          use_r_end = true;
          merge_end = true;
        } else if (e2 < OB_RANGE_EXTEND_VALUE) {
          use_r_end = true;
          merge_end = true;
        }
      } else if (e1 == OB_RANGE_NULL_VALUE) {
        if (e2 == OB_RANGE_MIN_VALUE) {
          use_r_end = true;
          merge_end = true;
        } else if (e2 == OB_RANGE_MAX_VALUE) {
          merge_end = true;
        } else if (e2 == OB_RANGE_NULL_VALUE) {
          continue;
        } else if (e2 < OB_RANGE_EXTEND_VALUE) {
          if (lib::is_oracle_mode()) {
            use_r_end = true;
            merge_end = true;
          } else {
            merge_end = true;
          }
        }
      } else if (e1 < OB_RANGE_EXTEND_VALUE) {
        if (e2 == OB_RANGE_MIN_VALUE) {
          use_r_end = true;
          merge_end = true;
        } else if (e2 == OB_RANGE_MAX_VALUE) {
          merge_end = true;
        } else if (e2 == OB_RANGE_NULL_VALUE) {
          if (lib::is_oracle_mode()) {
            merge_end = true;
          } else {
            use_r_end = true;
            merge_end = true;
          }
        } else if (e2 < OB_RANGE_EXTEND_VALUE) {
          // can not compare
          break;
        }
      }
      if (use_r_end) {
        end_offset = i;
      }
    }

    if (merge_start && merge_end) {
      is_merge = true;
      if (use_r_start) {
        set_new_start_key(*l_node, *r_node, column_cnt, start_offset);
        l_node->include_start_ = r_node->include_start_;
      }
      if (use_r_end) {
        set_new_end_key(*l_node, *r_node, column_cnt, end_offset);
        l_node->include_end_ = r_node->include_end_;
      }
      if (use_r_start || use_r_end) {
        l_node->max_offset_ = std::max(l_node->max_offset_, r_node->max_offset_);
      }
      if (l_node->contain_in_ || r_node->contain_in_) {
        bool cnt_in = false;
        for (int64_t i = 0; !cnt_in && i < column_cnt; ++i) {
          if (l_node->start_keys_[i] < 0 || l_node->end_keys_[i] < 0) {
            cnt_in = true;
          }
        }
        l_node->contain_in_ = cnt_in;
        l_node->in_param_count_ = cnt_in ? std::max(l_node->in_param_count_, r_node->in_param_count_) : 0;
      }
    }
  }
  return ret;
}

void ObRangeGraphGenerator::set_new_start_key(ObRangeNode &l_node,
                                              ObRangeNode &r_node,
                                              const int64_t column_cnt,
                                              int64_t start_offset)
{
  for (int64_t i = start_offset; i < column_cnt; ++i) {
    l_node.start_keys_[i] = r_node.start_keys_[i];
  }
}

void ObRangeGraphGenerator::set_new_end_key(ObRangeNode &l_node,
                                            ObRangeNode &r_node,
                                            const int64_t column_cnt,
                                            int64_t start_offset)
{
  for (int64_t i = start_offset; i < column_cnt; ++i) {
    l_node.end_keys_[i] = r_node.end_keys_[i];
  }
}

int ObRangeGraphGenerator::or_range_nodes(ObExprRangeConverter &range_node_generator,
                                          ObIArray<ObRangeNode*> &range_nodes,
                                          const int64_t column_cnt,
                                          ObRangeNode *&range_node)
{
  int ret = OB_SUCCESS;
  if (range_nodes.count() > 1) {
    // sort range node as following order:
    //  1. always false
    //  2. always true
    //  3. node with smaller min_offset
    std::sort(&range_nodes.at(0), &range_nodes.at(0) + range_nodes.count(), RangeNodeCmp());
  }
  ObRangeNode *last_node = nullptr;
  ObRangeNode *always_false_node = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_nodes.count(); ++i) {
    ObRangeNode *cur_node = range_nodes.at(i);
    if (OB_ISNULL(cur_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null range node");
    } else if (cur_node->always_false_) {
      if (always_false_node == nullptr) {
        always_false_node = cur_node;
      }
      continue;
    } else if (cur_node->always_true_) {
      range_node = cur_node;
      break;
    } else if (range_node == nullptr) {
      range_node = cur_node;
      last_node = cur_node;
    } else if (OB_UNLIKELY(cur_node->is_phy_rowid_ != last_node->is_phy_rowid_)) {
      // rowid = xxx or c1 = 1 get always true
      range_node = nullptr;
      if (OB_FAIL(range_node_generator.generate_always_true_or_false_node(true, range_node))) {
        LOG_WARN("failed to generate whole range");
      }
      break;
    } else {
      while (last_node->or_next_ != nullptr) {
        last_node = last_node->or_next_;
      }
      last_node->or_next_ = cur_node;
      last_node = cur_node;
    }
  }
  if (OB_SUCC(ret) && range_node == nullptr) {
    if (OB_ISNULL(always_false_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected range node", K(range_nodes));
    } else {
      range_node = always_false_node;
    }
  }
  return ret;
}

/**
 * 在 pre 阶段看不到具体的参数值, or 节点的合并只能处理一些非常简单的场景
 * 1. c1 > :1      (:1,max,max; null,min,min)
 *    c1 is null   (null,min,min; null,max,max)
 *    =>      (:1,max,max, null,max,max)
 * 2. c1 < :1             (min,min,min; :1,min,min)
 *    c1 >= :1            (:1,min,min; null,min,min)
 *    =>      (min,min,min, null,min,min)
*/
int ObRangeGraphGenerator::or_two_range_node(ObRangeNode *&l_node,
                                             ObRangeNode *&r_node,
                                             const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_node) || OB_ISNULL(r_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range node", KP(l_node), KP(r_node));
  } else {
    bool is_equal = true;
    for (int64_t i = r_node->min_offset_; is_equal && i < column_cnt; ++i) {
      if (l_node->end_keys_[i] != r_node->start_keys_[i])  {
        is_equal = false;
      } else if (i == column_cnt - 1) {
        if (l_node->end_keys_[i] != OB_RANGE_MIN_VALUE &&
            l_node->end_keys_[i] != OB_RANGE_MAX_VALUE &&
            !l_node->include_end_ && !r_node->include_start_)  {
          is_equal = false;
        }
      }
    }
    if (is_equal) {
      MEMCPY(l_node->end_keys_, r_node->end_keys_, sizeof(int64_t) * column_cnt);
    } else {
      bool is_equal = true;
      for (int64_t i = r_node->min_offset_; is_equal && i < column_cnt; ++i) {
        if (l_node->start_keys_[i] != r_node->end_keys_[i])  {
          is_equal = false;
        } else if (i == column_cnt - 1) {
          if (l_node->start_keys_[i] != OB_RANGE_MIN_VALUE &&
              l_node->start_keys_[i] != OB_RANGE_MAX_VALUE &&
              !l_node->include_start_ && !r_node->include_end_)  {
            is_equal = false;
          }
        }
      }
      if (is_equal) {
        MEMCPY(l_node->start_keys_, r_node->start_keys_, sizeof(int64_t) * column_cnt);
      }
    }
  }
  return ret;
}

int ObRangeGraphGenerator::and_link_range_node(ObRangeNode *&l_node,
                                               ObRangeNode *&r_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRangeNode*, 8> and_tails;
  if (OB_FAIL(get_and_tails(l_node, and_tails))) {
    LOG_WARN("failed to get and tails");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < and_tails.count(); ++i) {
      if (OB_ISNULL(and_tails.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else {
        and_tails.at(i)->and_next_ = r_node;
      }
    }
  }
  return ret;
}

int ObRangeGraphGenerator::get_and_tails(ObRangeNode *range_node,
                                         ObIArray<ObRangeNode*> &and_tails)
{
  int ret = OB_SUCCESS;
  for (ObRangeNode* cur_node = range_node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
    if (cur_node->and_next_ == nullptr) {
      if (OB_FAIL(and_tails.push_back(cur_node))) {
        LOG_WARN("failed to push back and tail");
      }
    } else if (OB_FAIL(SMART_CALL(get_and_tails(cur_node->and_next_, and_tails)))) {
      LOG_WARN("failed to and link range node");
    }
  }
  return ret;
}

/**
 * formalize final range graph
 * 1. estimate range size
 * 2. check if skip scan valid
 * 3. remove useless range node for standard range
 * 4. generate node id
*/
int ObRangeGraphGenerator::formalize_final_range_node(ObRangeNode *range_node)
{
  int ret = OB_SUCCESS;
  if (range_node->always_false_ || range_node->always_true_) {
    pre_range_graph_->set_range_size(1);
  } else {
    bool start_from_zero = false;
    uint64_t total_range_sizes[pre_range_graph_->get_column_count()];
    uint64_t range_sizes[pre_range_graph_->get_column_count()];
    for (int64_t i = 0; i < pre_range_graph_->get_column_count(); ++i) {
      total_range_sizes[i] = 0;
      range_sizes[i] = 1;
    }
    if (OB_FAIL(collect_graph_infos(range_node, total_range_sizes, range_sizes, start_from_zero))) {
      LOG_WARN("failed to collect graph infos");
    } else {
      // no not refine large graph for now
      // bool need_refine = false;
      // int64_t i = 0;
      // for (; !need_refine && i < pre_range_graph_->get_column_count(); ++i) {
      //   if (total_range_sizes[i] > 10000) {
      //     need_refine = true;
      //   }
      // }
      // if (OB_SUCC(ret)) {
      //   int64_t range_size = need_refine ? (i > 0 ? total_range_sizes[i - 1] : 1) : total_range_sizes[pre_range_graph_->get_column_count() - 1];
      //   pre_range_graph_->set_range_size(range_size);
      //   if (need_refine) {
      //     if (i == 0) {
      //       range_node->set_always_true();
      //     } else {
      //       // refine range graph
      //     }
      //   }
      // }
    }
    if (OB_SUCC(ret) && is_standard_range(range_node)) {
      ObRangeNode *ss_head = nullptr;
      if (OB_FAIL(check_skip_scan_valid(range_node, ss_head))) {
        LOG_WARN("failed to check skip scan valid");
      } else if (ss_head != nullptr) {
        if (OB_FAIL(remove_useless_range_node(ss_head, pre_range_graph_->get_skip_scan_offset()))) {
          LOG_WARN("failed to remove useless range", K(ret));
        }
      } else if (OB_FAIL(remove_useless_range_node(range_node))) {
        LOG_WARN("failed to remove useless range", K(ret));
      }
    }
    if (OB_SUCC(ret) && !start_from_zero && !pre_range_graph_->is_ss_range()) {
      range_node->set_always_true();
      pre_range_graph_->set_range_size(1);
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t node_count = 0;
    if (OB_FAIL(generate_node_id(range_node, node_count))) {
      LOG_WARN("failed to generate node id");
    } else {
      pre_range_graph_->set_node_count(node_count);
    }
  }
  return ret;
}

int ObRangeGraphGenerator::collect_graph_infos(ObRangeNode *range_node,
                                               uint64_t *total_range_sizes,
                                               uint64_t *range_sizes,
                                               bool &start_from_zero)
{
  int ret = OB_SUCCESS;
  uint64_t cur_or_count = 0;
  if (range_node->always_true_ || range_node->always_false_) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected pos", KPC(range_node));
  } else {
    for (const ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
      if (cur_node->contain_in_) {
        cur_or_count += cur_node->in_param_count_;
      } else {
        ++cur_or_count;
      }

      if (nullptr == cur_node->or_next_ ||
          cur_node->min_offset_ != cur_node->or_next_->min_offset_ ||
          cur_node->or_next_->and_next_ != cur_node->and_next_) {
        if (cur_node->min_offset_ <= 0) {
          start_from_zero = true;
        }
        int64_t idx = range_node->min_offset_ == -1 ? 0 : range_node->min_offset_;
        range_sizes[idx] *= cur_or_count;
        if (cur_node->and_next_ != nullptr) {
          if (OB_FAIL(SMART_CALL(collect_graph_infos(cur_node->and_next_, total_range_sizes,
                                                     range_sizes, start_from_zero)))) {
            LOG_WARN("failed to collect graph infos");
          }
        } else {
          uint64_t range_size = 1;
          for (int64_t i = 0; i < cur_node->column_cnt_; ++i) {
            range_size *= range_sizes[i];
            total_range_sizes[i] += range_size;
          }
        }
        range_sizes[idx] /= cur_or_count;
        cur_or_count = 0;
      }
    }
  }
  return ret;
}

int ObRangeGraphGenerator::check_skip_scan_valid(ObRangeNode *range_node,
                                                ObRangeNode *&ss_head)
{
  int ret = OB_SUCCESS;
  ss_head = NULL;
  int64_t max_precise_pos = 0;
  int64_t ss_max_precise_pos = 0;
  ObRangeNode *cur_node = range_node;
  if (OB_FAIL(get_max_precise_pos(range_node, max_precise_pos))) {
    LOG_WARN("failed to get max precise pos");
  } else {
    update_max_precise_offset(max_precise_pos);
    // skip prefix precise range
    while (cur_node != nullptr && cur_node->min_offset_ < max_precise_pos) {
      cur_node = cur_node->and_next_;
    }
    if (NULL != cur_node) {
      ss_head = cur_node;
      pre_range_graph_->set_skip_scan_offset(ss_head->min_offset_);
      if (OB_FAIL(get_max_precise_pos(ss_head, ss_max_precise_pos, ss_head->min_offset_))) {
        LOG_WARN("failed to get max precise pos");
      } else {
        update_ss_max_precise_offset(ss_max_precise_pos);
      }
    }
  }
  return ret;
}

int ObRangeGraphGenerator::remove_useless_range_node(ObRangeNode *range_node, int64_t start_pos) const
{
  int ret = OB_SUCCESS;
  int64_t max_offset = start_pos;
  for (ObRangeNode *cur_node = range_node; cur_node != nullptr; cur_node = cur_node->and_next_) {
    if (cur_node->min_offset_ > max_offset) {
      cur_node->and_next_ = nullptr;
    } else if (cur_node->max_offset_ > max_offset) {
      max_offset = cur_node->max_offset_;
    }
  }
  return ret;
}

int ObRangeGraphGenerator::generate_node_id(ObRangeNode *range_node, uint64_t &node_count)
{
  int ret = OB_SUCCESS;
  for (ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
    cur_node->node_id_ = node_count++;
    if (cur_node->and_next_ != nullptr && -1 == cur_node->and_next_->node_id_) {
      if (OB_FAIL(SMART_CALL(generate_node_id(cur_node->and_next_, node_count)))) {
        LOG_WARN("failed to generate node id");
      }
    }
  }
  return ret;
}

int ObRangeGraphGenerator::check_graph_type(ObRangeNode *range_node)
{
  int ret = OB_SUCCESS;
  bool is_equal_range = false;
  bool is_get = false;
  int64_t max_precise_pos = 0;
  pre_range_graph_->set_range_head(range_node);
  pre_range_graph_->set_is_precise_get(is_precise_get(range_node));
  pre_range_graph_->set_is_standard_range(is_standard_range(range_node));
  if (pre_range_graph_->is_ss_range()) {
    // do nothing
  } else if (OB_FAIL(get_max_precise_pos(range_node, max_precise_pos))) {
    LOG_WARN("failed to get max precise pos");
  } else {
    update_max_precise_offset(max_precise_pos);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(is_strict_equal_graph(range_node, is_equal_range, is_get))) {
      LOG_WARN("is strict equal graph failed", K(ret));
    } else {
      pre_range_graph_->set_is_equal_range(is_equal_range);
      pre_range_graph_->set_is_get(is_get);
    }
  }
  return ret;
}

bool ObRangeGraphGenerator::is_precise_get(ObRangeNode *range_node) const
{
  bool is_precise_get = false;
  if (range_node->and_next_ == nullptr && range_node->or_next_ == nullptr && range_node->min_offset_ == 0) {
    // range nodes can always merge to one node if is precise get
    if (range_node->is_geo_node_ || range_node->contain_in_) {
      is_precise_get = false;
    } else if (is_strict_equal_node(range_node)) {
      is_precise_get = true;
    }
  }
  return is_precise_get;
}

bool ObRangeGraphGenerator::is_standard_range(ObRangeNode *range_node) const
{
  bool is_standard_range = true;
  for (const ObRangeNode *cur_node = range_node;
       is_standard_range && cur_node != nullptr;
       cur_node = cur_node->and_next_) {
    if (cur_node->contain_in_ || cur_node->or_next_ != nullptr) {
      is_standard_range = false;
    }
  }
  return is_standard_range;
}

int ObRangeGraphGenerator::get_max_precise_pos(ObRangeNode *range_node,
                                               int64_t &max_precise_pos,
                                               int64_t start_pos) const
{
  int ret = OB_SUCCESS;
  max_precise_pos = ctx_.column_cnt_;
  if (!pre_range_graph_->is_precise_get()) {
    bool equals[ctx_.column_cnt_];
    MEMSET(equals, 0, sizeof(bool) * ctx_.column_cnt_);
    ret = inner_get_max_precise_pos(range_node, equals, max_precise_pos, start_pos);
  }
  return ret;
}

int ObRangeGraphGenerator::inner_get_max_precise_pos(const ObRangeNode *range_node,
                                                     bool* equals,
                                                     int64_t &max_offset,
                                                     int64_t start_pos) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> new_idx;
  for (const ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != NULL; cur_node = cur_node->or_next_) {
    // reset equal flag in previous or range node
    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
    new_idx.reset();

    if (cur_node->min_offset_ >= 0 &&
      OB_FAIL(get_new_equal_idx(cur_node, equals, new_idx))) {
      LOG_WARN("failed to get new idx");
    } else if (cur_node->and_next_ != nullptr) {
      if(SMART_CALL(inner_get_max_precise_pos(cur_node->and_next_, equals, max_offset, start_pos))) {
        LOG_WARN("failed to check is strict equal graph");
      }
    } else {
      int64_t cur_max_offset = ctx_.column_cnt_;
      for (int64_t i = start_pos; i < ctx_.column_cnt_; ++i) {
        if (!equals[i]) {
          cur_max_offset = i;
          break;
        }
      }
      if (cur_max_offset + 1 < max_offset) {
        max_offset = cur_max_offset + 1;
      }
    }
  }
  // reset equal flag in current range node
  if (OB_SUCC(ret)) {
    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
  }
  return ret;
}

/**
 * check if all ranges are strict euqal range.
 * 1. key consecutive
 * 2. key alignment
 * 3. key has equal condition
*/
int ObRangeGraphGenerator::is_strict_equal_graph(const ObRangeNode *range_node,
                                                 bool &is_strict_equal,
                                                 bool &is_get) const
{
  int ret = OB_SUCCESS;
  if (pre_range_graph_->is_precise_get()) {
    is_strict_equal = true;
    is_get = true;
  } else {
    int64_t max_offset = -1;
    int64_t max_node_offset = -1;
    bool equals[ctx_.column_cnt_];
    MEMSET(equals, 0, sizeof(bool) * ctx_.column_cnt_);
    if (OB_FAIL(SMART_CALL(inner_is_strict_equal_graph(range_node, equals, max_offset,
                                                       max_node_offset, is_strict_equal)))) {
      LOG_WARN("failed to check inner is strict equal graph");
    } else if (is_strict_equal) {
      is_get = max_offset == ctx_.column_cnt_;
    }
  }
  return ret;
}

int ObRangeGraphGenerator::inner_is_strict_equal_graph(const ObRangeNode *range_node,
                                                      bool* equals,
                                                      int64_t &max_offset,
                                                      int64_t &max_node_offset,
                                                      bool &is_strict_equal) const
{
  int ret = OB_SUCCESS;
  is_strict_equal = true;
  ObSEArray<int64_t, 8> new_idx;
  for (const ObRangeNode *cur_node = range_node;
       OB_SUCC(ret) && is_strict_equal && cur_node != NULL;
       cur_node = cur_node->or_next_) {
    // reset equal flag in previous or range node
    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
    new_idx.reset();
    if (cur_node->max_offset_ > max_node_offset) {
      max_node_offset = cur_node->max_offset_;
    }

    if (cur_node->min_offset_ >= 0 &&
        OB_FAIL(get_new_equal_idx(cur_node, equals, new_idx))) {
      LOG_WARN("failed to get new idx");
    } else if (cur_node->and_next_ != nullptr) {
      if(OB_FAIL(SMART_CALL(inner_is_strict_equal_graph(cur_node->and_next_, equals, max_offset,
                                                        max_node_offset, is_strict_equal)))) {
        LOG_WARN("failed to check is strict equal graph");
      }
    } else {
      if (-1 == max_offset) {
        // max_offset = max_equal_offset + 1;
        for (max_offset = 0; max_offset < ctx_.column_cnt_; ++max_offset) {
          if (!equals[max_offset]) {
            break;
          }
        }
        for (int64_t i = max_offset + 1; i < ctx_.column_cnt_; ++i) {
          if (equals[i]) {
            is_strict_equal = false;
          }
        }
        if (max_node_offset >= max_offset) {
          is_strict_equal = false;
        }
      } else {
        for (int64_t i = 0; i < max_offset; ++i) {
          if (!equals[i]) {
            is_strict_equal = false;
          }
        }
        for (int64_t i = max_offset; i < ctx_.column_cnt_; ++i) {
          if (equals[i]) {
            is_strict_equal = false;
          }
        }
        if (max_node_offset >= max_offset) {
          is_strict_equal = false;
        }
      }
    }
  }
  // reset equal flag in current range node
  if (OB_SUCC(ret) && is_strict_equal) {
    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
  }
  return ret;
}

int ObRangeGraphGenerator::get_new_equal_idx(const ObRangeNode *range_node,
                                             bool* equals,
                                             ObIArray<int64_t> &new_idx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = range_node->min_offset_; OB_SUCC(ret) && i <= range_node->max_offset_; ++i){
    if (!equals[i] && range_node->start_keys_[i] == range_node->end_keys_[i] &&
        (range_node->start_keys_[i] < OB_RANGE_EXTEND_VALUE || range_node->start_keys_[i] == OB_RANGE_NULL_VALUE)) {
      equals[i] = true;
      if (OB_FAIL(new_idx.push_back(i))) {
        LOG_WARN("failed to push back idx", K(i));
      }
    }
  }
  return ret;
}

/**
 * check if all key are equal condition. e.g. (:1,:2,:3; :1,:2,:3)
*/
bool ObRangeGraphGenerator::is_strict_equal_node(const ObRangeNode *range_node) const
{
  bool is_equal = true;
  for (int64_t i = 0; is_equal && i < ctx_.column_cnt_; ++i) {
    if (range_node->start_keys_[i] != range_node->end_keys_[i] ||
        !is_const_expr_or_null(range_node->start_keys_[i])) {
      is_equal = false;
    }
  }
  return is_equal;
}

/**
 * check if all valid key are equal condition. e.g. (ept,:2,min; ept,:2,max)
*/
bool ObRangeGraphGenerator::is_equal_node(const ObRangeNode *range_node) const
{
  bool is_equal = true;
  if (range_node->min_offset_ < 0) {
    // range node for const expr
    is_equal = false;
  } else {
    for (int64_t i = range_node->min_offset_; is_equal && i < ctx_.column_cnt_; ++i) {
      if (range_node->start_keys_[i] == OB_RANGE_MIN_VALUE &&
          range_node->end_keys_[i] == OB_RANGE_MAX_VALUE) {
        // do nothing
      } else if (range_node->start_keys_[i] != range_node->end_keys_[i]) {
        is_equal = false;
      }
    }
  }
  return is_equal;
}

int ObRangeGraphGenerator::fill_range_exprs(ObIArray<ObPriciseExprItem> &pricise_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> range_exprs;
  ObSEArray<ObRawExpr*, 4> ss_range_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < pricise_exprs.count(); ++i) {
    ObPriciseExprItem &expr_item = pricise_exprs.at(i);
    if (expr_item.max_offset_ < max_precise_offset_) {
      if (OB_FAIL(range_exprs.push_back(const_cast<ObRawExpr*>(expr_item.expr_)))) {
        LOG_WARN("push back precise range expr failed", K(ret));
      }
    } else if (expr_item.max_offset_ < ss_max_precise_offset_) {
      if (OB_FAIL(ss_range_exprs.push_back(const_cast<ObRawExpr*>(expr_item.expr_)))) {
        LOG_WARN("push back precise range expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pre_range_graph_->set_range_exprs(range_exprs))) {
      LOG_WARN("failed to assign range exprs", K(ret));
    } else if (OB_FAIL(pre_range_graph_->set_ss_range_exprs(ss_range_exprs))) {
      LOG_WARN("failed to assign range exprs", K(ret));
    } else {
      LOG_TRACE("finish fill range exprs", K(max_precise_offset_), K(ss_max_precise_offset_),
                  K(range_exprs), K(ss_range_exprs));
    }
  }
  return ret;
}

int ObRangeGraphGenerator::generate_expr_final_info()
{
  int ret = OB_SUCCESS;
  RowDesc row_desc;
  ObRangeMap &range_map = pre_range_graph_->get_range_map();
  ObIArray<const ObRawExpr*> &final_exprs = ctx_.final_exprs_;
  ObExecContext *exec_ctx = ctx_.exec_ctx_;
  range_map.expr_final_infos_.reset();
  range_map.in_params_.reset();
  int64_t N = final_exprs.count();
  bool cnt_exec_param = false;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(exec_ctx));
  } else if (OB_FAIL(range_map.expr_final_infos_.prepare_allocate(N))) {
    LOG_WARN("failed to prepare allocate expr final infos", K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObRawExpr *expr = final_exprs.at(i);
    ObRangeMap::ExprFinalInfo &expr_info = range_map.expr_final_infos_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr");
    } else if (T_QUESTIONMARK == expr->get_expr_type()) {
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(expr);
      const ObObj& val = const_expr->get_value();
      if (OB_UNLIKELY(!val.is_unknown())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected value type", K(val));
      } else {
        val.get_unknown(expr_info.param_idx_);
        expr_info.is_param_ = true;
      }
    } else if (expr->has_flag(IS_CONST)) {
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(expr);
      void *ptr = exec_ctx->get_allocator().alloc(sizeof(ObObj));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memeory for ObObj");
      } else {
        expr_info.const_val_ = new(ptr)ObObj();
        expr_info.is_const_ = true;
        if (OB_FAIL(ob_write_obj(exec_ctx->get_allocator(), const_expr->get_value(), *expr_info.const_val_))) {
          LOG_WARN("failed to deep copy obj", K(const_expr->get_value()));
        }
      }
    } else if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(
            expr, row_desc,
            exec_ctx->get_allocator(),
            exec_ctx->get_my_session(),
            exec_ctx->get_sql_ctx()->schema_guard_,
            expr_info.temp_expr_))) {
      LOG_WARN("failed to generate expr with row desc", K(ret));
    } else {
      expr_info.is_expr_ = true;
    }

    if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
      cnt_exec_param = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.null_safe_value_idxs_.count(); ++i) {
    int64_t idx = ctx_.null_safe_value_idxs_.at(i);
    if (OB_UNLIKELY(idx < 0 || idx >= range_map.expr_final_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null safe idx", K(idx), K(range_map.expr_final_infos_.count()));
    } else {
      range_map.expr_final_infos_.at(idx).null_safe_ = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.rowid_idxs_.count(); ++i) {
    int64_t idx = ctx_.rowid_idxs_.at(i).first;
    if (OB_UNLIKELY(idx < 0 || idx >= range_map.expr_final_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null safe idx", K(idx), K(range_map.expr_final_infos_.count()));
    } else if (ctx_.rowid_idxs_.at(i).second == PHYSICAL_ROWID_IDX) {
      range_map.expr_final_infos_.at(idx).rowid_idx_ = static_cast<uint32_t>(ctx_.rowid_idxs_.at(i).second);
    } else {
      range_map.expr_final_infos_.at(idx).rowid_idx_ = static_cast<uint32_t>(ctx_.rowid_idxs_.at(i).second) + 1;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(range_map.in_params_.assign(ctx_.in_params_))) {
      LOG_WARN("failed to assign in params");
    } else {
      pre_range_graph_->set_contain_exec_param(cnt_exec_param);
    }
  }
  return ret;
}

}
}