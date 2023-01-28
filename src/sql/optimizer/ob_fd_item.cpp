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

#define USING_LOG_PREFIX SQL_JO
#include "sql/optimizer/ob_fd_item.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_fd_item.h"


using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObFdItem::assign(const ObFdItem &other)
{
  parent_exprs_ = other.parent_exprs_;
  is_unique_ = other.is_unique_;
  return OB_SUCCESS;
}

int ObFdItem::check_expr_in_child(const ObRawExpr *expr,
                                  const EqualSets &equal_sets,
                                  bool &is_in_child) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(equal_sets);
  is_in_child = false;
  return ret;
}

int ObFdItem::check_exprs_in_child(const ObIArray<ObRawExpr *> &exprs,
                                   const EqualSets &equal_sets,
                                   bool &is_in_child) const
{
  int ret = OB_SUCCESS;
  is_in_child = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_in_child && i < exprs.count(); ++i) {
    if (OB_FAIL(check_expr_in_child(exprs.at(i), equal_sets, is_in_child))) {
      LOG_WARN("failed to check expr in child", K(ret));
    }
  }
  return ret;
}

int ObFdItem::check_exprs_in_child(const ObIArray<ObRawExpr *> &exprs,
                                   const EqualSets &equal_sets,
                                   const int64_t pos_start,
                                   ObBitSet<> &exprs_set)
{
  int ret = OB_SUCCESS;
  bool in_child = false;
  if (OB_UNLIKELY(pos_start < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(pos_start), K(ret));
  }
  for (int64_t i = pos_start; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (exprs_set.has_member(i)) {
      // do nothing
    } else if (OB_FAIL(check_expr_in_child(exprs.at(i), equal_sets, in_child))) {
      LOG_WARN("failed to check expr in child", K(ret));
    } else if (in_child && OB_FAIL(exprs_set.add_member(i))) {
      LOG_WARN("failed to add member to set", K(ret));
    }
  }
  return ret;
}

int ObFdItem::check_exprs_in_child(const common::ObIArray<OrderItem> &order_items,
                                   const EqualSets &equal_sets,
                                   const int64_t pos_start,
                                   ObBitSet<> &exprs_set)
{
  int ret = OB_SUCCESS;
  bool in_child = false;
  if (OB_UNLIKELY(pos_start < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(pos_start), K(ret));
  }
  for (int64_t i = pos_start; OB_SUCC(ret) && i < order_items.count(); ++i) {
    if (exprs_set.has_member(i)) {
      // do nothing
    } else if (OB_FAIL(check_expr_in_child(order_items.at(i).expr_, equal_sets, in_child))) {
      LOG_WARN("failed to check expr in child", K(ret));
    } else if (in_child && OB_FAIL(exprs_set.add_member(i))) {
      LOG_WARN("failed to add member to set", K(ret));
    }
  }
  return ret;
}

int ObTableFdItem::assign(const ObTableFdItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_FAIL(ObFdItem::assign(other))) {
      LOG_WARN("failed to assign ObFdItem", K(ret));
    } else {
      child_tables_ = other.child_tables_;
    }
  }
  return ret;
}

int ObTableFdItem::check_expr_in_child(const ObRawExpr *expr,
                                       const EqualSets &equal_sets,
                                       bool &is_in_child) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr *cur_expr = NULL;
  bool is_consistent = false;
  is_in_child = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (expr->has_flag(CNT_AGG)) {
    //do nothing
  } else if (expr->has_flag(CNT_COLUMN) || expr->has_flag(CNT_SET_OP)) {
    if (!expr->get_relation_ids().is_empty() &&
        child_tables_.is_superset(expr->get_relation_ids())) {
      is_in_child = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_in_child && i < equal_sets.count(); ++i) {
      const EqualSet *equal_set = equal_sets.at(i);
      if (OB_ISNULL(equal_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null equal set", K(ret));
      } else if (!ObOptimizerUtil::find_equal_expr(*equal_set, expr)) {
        continue;
      }  
      for (int64_t j = 0; OB_SUCC(ret) && !is_in_child && j < equal_set->count(); ++j) {
        if (OB_ISNULL(cur_expr = equal_set->at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret));
        } else if (cur_expr->get_relation_ids().is_empty() || 
                  !child_tables_.is_superset(cur_expr->get_relation_ids())) {
          // do nothing
        } else if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(cur_expr, expr, is_consistent))) {
          LOG_WARN("failed to check expr is order consistent", K(ret));
        } else if (is_consistent) {
          is_in_child = true;
        }
      }
    }
  }
  return ret;
}

int ObExprFdItem::assign(const ObExprFdItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_FAIL(ObFdItem::assign(other))) {
      LOG_WARN("failed to assign ObFdItem", K(ret));
    } else if (OB_FAIL(child_exprs_.assign(other.child_exprs_))) {
      LOG_WARN("failed to assign child tables", K(ret));
    }
  }
  return ret;
}

int ObExprFdItem::check_expr_in_child(const ObRawExpr *expr,
                                      const EqualSets &equal_sets,
                                      bool &is_in_child) const
{
  int ret = OB_SUCCESS;
  is_in_child = ObOptimizerUtil::find_equal_expr(child_exprs_, expr, equal_sets);
  return ret;
}

int ObFdItemFactory::create_fd_item_set(ObFdItemSet *&fd_item_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fd_item_set = (ObFdItemSet *)allocator_.alloc(sizeof(ObFdItemSet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fd item sets", K(ret));
  } else {
    fd_item_set = new(fd_item_set) ObFdItemSet();
    if (OB_FAIL(item_set_store_.store_obj(fd_item_set))) {
      LOG_WARN("failed to store obj", K(ret));
      fd_item_set->~ObFdItemSet();
      fd_item_set = NULL;
    }
  }
  return ret;
}

int ObFdItemFactory::get_parent_exprs_ptr(ObRawExpr *parent_expr,
                                          ObRawExprSet *&parent_exprs_ptr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 1> parent_exprs;
  if (OB_FAIL(parent_exprs.push_back(parent_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    ret = get_parent_exprs_ptr(parent_exprs, parent_exprs_ptr);
  }
  return ret;
}

int ObFdItemFactory::get_parent_exprs_ptr(const ObIArray<ObRawExpr *> &parent_exprs,
                                          ObRawExprSet *&parent_exprs_ptr)
{
  int ret = OB_SUCCESS;
  parent_exprs_ptr = NULL;
  bool find = false;
  if (OB_UNLIKELY(parent_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get empty parent exprs", K(ret));
  } else {
    // check parent exprs exists
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < parent_sets_.count(); ++i) {
      if (OB_ISNULL(parent_sets_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null fd parent set", K(ret));
      } else if (parent_exprs.count() == parent_sets_.at(i)->count() &&
                 ObOptimizerUtil::subset_exprs(*parent_sets_.at(i), parent_exprs)) {
        parent_exprs_ptr = parent_sets_.at(i);
        find = true;
      }
    }
    // if not exists, create new one
    if (find) { 
      // do nothing
    } else if (OB_FAIL(ObRawExprSetUtils::add_expr_set(&allocator_, parent_exprs, parent_sets_))) {
      LOG_WARN("failed to add expr set", K(ret));
    } else {
      parent_exprs_ptr = parent_sets_.at(parent_sets_.count() - 1);
    }
  }
  return ret;
}

int ObFdItemFactory::create_table_fd_item(ObTableFdItem *&fd_item,
                                          const bool is_unique,
                                          ObRawExprSet *parent_exprs)
{
  int ret = OB_SUCCESS;
  fd_item = NULL;
  void *ptr = allocator_.alloc(sizeof(ObTableFdItem));
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory to create ObTableFdItem", K(ret));
  } else {
    fd_item = new(ptr) ObTableFdItem(is_unique, parent_exprs);
    if (OB_FAIL(item_store_.store_obj(fd_item))) {
      LOG_WARN("failed to store obj", K(ret));
      fd_item->~ObTableFdItem();
      fd_item = NULL;
    }
  }
  return ret;
}

int ObFdItemFactory::create_table_fd_item(ObTableFdItem *&fd_item,
                                          const bool is_unique,
                                          const ObIArray<ObRawExpr *> &parent_exprs,
                                          const ObRelIds &table_set)
{
  int ret = OB_SUCCESS;
  fd_item = NULL;
  ObRawExprSet *parent_exprs_ptr = NULL;
  if (OB_FAIL(get_parent_exprs_ptr(parent_exprs, parent_exprs_ptr))) {
    LOG_WARN("failed to get parent exprs ptr", K(ret));
  } else if (OB_FAIL(create_table_fd_item(fd_item, is_unique, parent_exprs_ptr))) {
    LOG_WARN("failed to create table fd item", K(ret));
  } else if (OB_FAIL(fd_item->get_child_tables().add_members(table_set))) {
    LOG_WARN("failed to add members to child tables", K(ret));
  }
  return ret;
}

int ObFdItemFactory::create_table_fd_item(ObTableFdItem *&fd_item,
                                          const ObTableFdItem &other)
{
  int ret = OB_SUCCESS;
  fd_item = NULL;
  if (OB_FAIL(create_table_fd_item(fd_item,
                                   other.is_unique(),
                                   other.get_parent_exprs()))) {
    LOG_WARN("failed to create table fd item", K(ret));
  } else if (OB_FAIL(fd_item->get_child_tables().add_members(other.get_child_tables()))) {
    LOG_WARN("failed to add member to child tables", K(ret));
  }
  return ret;
}

int ObFdItemFactory::create_expr_fd_item(ObExprFdItem *&fd_item,
                                         const bool is_unique,
                                         ObRawExprSet *parent_exprs)
{
  int ret = OB_SUCCESS;
  fd_item = NULL;
  void *ptr = allocator_.alloc(sizeof(ObExprFdItem));
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory to create ObExprFdItem", K(ret));
  } else {
    fd_item = new(ptr) ObExprFdItem(is_unique, parent_exprs);
    if (OB_FAIL(item_store_.store_obj(fd_item))) {
      LOG_WARN("failed to store obj", K(ret));
      fd_item->~ObExprFdItem();
      fd_item = NULL;
    }
  }
  return ret;
}

int ObFdItemFactory::create_expr_fd_item(ObExprFdItem *&fd_item,
                                         const bool is_unique,
                                         const ObIArray<ObRawExpr *> &parent_exprs,
                                         const ObIArray<ObRawExpr *> &child_exprs)
{
  int ret = OB_SUCCESS;
  fd_item = NULL;
  ObRawExprSet *parent_exprs_ptr = NULL;
  if (OB_FAIL(get_parent_exprs_ptr(parent_exprs, parent_exprs_ptr))) {
    LOG_WARN("failed to get parent exprs ptr", K(ret));
  } else if (OB_FAIL(create_expr_fd_item(fd_item, is_unique, parent_exprs_ptr))) {
    LOG_WARN("failed to create table fd item", K(ret));
  } else if (OB_FAIL(ObRawExprSetUtils::to_expr_set(&allocator_,
                                                    child_exprs,
                                                    fd_item->get_child_exprs()))) {
    LOG_WARN("failed to expr set", K(ret));
  }
  return ret;
}

int ObFdItemFactory::create_expr_fd_item(ObExprFdItem *&fd_item,
                                         const ObExprFdItem &other)
{
  int ret = OB_SUCCESS;
  fd_item = NULL;
  if (OB_FAIL(create_expr_fd_item(fd_item,
                                  other.is_unique(),
                                  other.get_parent_exprs()))) {
    LOG_WARN("failed to create table fd item", K(ret));
  } else if (OB_FAIL(fd_item->get_child_exprs().assign(other.get_child_exprs()))) {
    LOG_WARN("failed to assign child exprs", K(ret));
  }
  return ret;
}

int ObFdItemFactory::copy_fd_item(ObFdItem *&fd_item, const ObFdItem &other)
{
  int ret = OB_SUCCESS;
  if (other.is_table_fd_item()) {
    ObTableFdItem *table_fd_item = NULL;
    if (OB_FAIL(create_table_fd_item(table_fd_item, static_cast<const ObTableFdItem &>(other)))) {
      LOG_WARN("failed to create table fd item", K(ret));
    } else {
      fd_item = table_fd_item;
    }
  } else if (other.is_expr_fd_item()) {
    ObExprFdItem *expr_fd_item = NULL;
    if (OB_FAIL(create_expr_fd_item(expr_fd_item, static_cast<const ObExprFdItem &>(other)))) {
      LOG_WARN("failed to create expr fd item", K(ret));
    } else {
      fd_item = expr_fd_item;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected fd item type", K(ret));
  }
  return ret;
}

int ObFdItemFactory::deduce_fd_item_set(const EqualSets &equal_sets,
                                        ObIArray<ObRawExpr *> &column_exprs,
                                        ObIArray<ObRawExpr *> &const_exprs,
                                        ObFdItemSet &fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> deduced_fd;
  int64_t const_exprs_count = -1;
  do {
    const_exprs_count = const_exprs.count();
    if (OB_FAIL(do_deduce_fd_item_set(equal_sets, column_exprs, const_exprs,
                                      fd_item_set, deduced_fd))) {
      LOG_WARN("failed to simplify fd item", K(ret));
    }
  } while (OB_SUCC(ret) && const_exprs.count() != const_exprs_count);
  return ret;
}

//If all parent exprs are const, add child exprs to const_exprs
int ObFdItemFactory::do_deduce_fd_item_set(const EqualSets &equal_sets,
                                           ObIArray<ObRawExpr *> &column_exprs,
                                           ObIArray<ObRawExpr *> &const_exprs,
                                           ObFdItemSet &fd_item_set,
                                           ObSqlBitSet<> &deduced_fd)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < fd_item_set.count(); i++) {
    ObFdItem *fd_item = NULL;
    ObRawExprSet *parent_exprs = NULL;
    if (OB_ISNULL(fd_item = fd_item_set.at(i))
        || OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (deduced_fd.has_member(i)) {// has deduced
      /*do nothing*/
    } else {
      bool all_const = true;
      for (int64_t j = 0; all_const && OB_SUCC(ret) && j < parent_exprs->count(); j++) {
        if (OB_FAIL(ObOptimizerUtil::is_const_expr(parent_exprs->at(j), equal_sets,
                                                   const_exprs, all_const))) {
          LOG_WARN("failed to check is const expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && all_const) { // all parent exprs is const, add const exprs
        bool is_in_child = false;
        int64_t cnt = 0;
        ObRawExpr *expr = NULL;
        for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); j++) {
          if (OB_ISNULL(expr = column_exprs.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(expr), K(ret));
          } else if (expr->is_const_expr() || ObOptimizerUtil::find_item(const_exprs, expr)) {
            /* do nothing */
          } else if (OB_FAIL(fd_item->check_expr_in_child(expr, equal_sets, is_in_child))) {
            LOG_WARN("failed to check expr in child", K(ret));
          } else if (!is_in_child) {
            column_exprs.at(cnt++) = expr;
          } else if (OB_FAIL(const_exprs.push_back(expr))) {  // add const exprs
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObOptimizerUtil::revert_items(column_exprs, cnt);
        }
      }
    }
  }
  return ret;
}
