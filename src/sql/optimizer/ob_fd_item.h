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

#ifndef OCEANBASE_SQL_OB_FD_ITEM_H
#define OCEANBASE_SQL_OB_FD_ITEM_H
#include "lib/allocator/page_arena.h"
#include "sql/resolver/dml/ob_raw_expr_sets.h"
#include "lib/container/ob_bit_set.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
enum FdLevel
{
  INVALID_LEVEL = -1,
  TABLE_LEVEL,
  EXPR_LEVEL
};

/**
 * Fd means function dependent, for table t1 (c1 int primary key, c2 int, c3 int),
 * we known c1 can decide c2 and c3, then we say c1 function dependent c1, c2, c3,
 * or c1 function dependent table t1
 * here [c1]          is parent exprs
 *      [c1, c2, c3]  is child exprs
 *      {t1}          is child tables
 */
class ObFdItem
{
public:
  ObFdItem(const bool is_unique = false,
           ObRawExprSet *parent_exprs = NULL) :
      parent_exprs_(parent_exprs), is_unique_(is_unique) {}
  virtual ~ObFdItem() {}

  inline ObRawExprSet *get_parent_exprs() const { return parent_exprs_; }
  inline void set_parent_exprs(ObRawExprSet *parent_exprs) { parent_exprs_ = parent_exprs; }
  inline bool is_unique() const { return is_unique_; }
  inline void set_is_unique(const bool is_unique) { is_unique_ = is_unique; }
  virtual inline FdLevel get_level() const { return INVALID_LEVEL; }
  virtual inline bool is_table_fd_item() const { return false; }
  virtual inline bool is_expr_fd_item() const { return false; }

  virtual int assign(const ObFdItem &other);
  virtual int check_expr_in_child(const ObRawExpr *expr,
                                  const EqualSets &equal_sets,
                                  bool &is_in_child) const;
  int check_exprs_in_child(const ObIArray<ObRawExpr *> &exprs,
                           const EqualSets &equal_sets,
                           bool &is_in_child) const;
  int check_exprs_in_child(const ObIArray<ObRawExpr *> &exprs,
                           const EqualSets &equal_sets,
                           const int64_t pos_start,
                           ObBitSet<> &exprs_set);
  int check_exprs_in_child(const common::ObIArray<OrderItem> &order_items,
                           const EqualSets &equal_sets,
                           const int64_t pos_start,
                           ObBitSet<> &exprs_set);

  VIRTUAL_TO_STRING_KV(K_(parent_exprs), K_(is_unique));
protected:
  ObRawExprSet *parent_exprs_;
  // 表示parent exprs在当前logical operator/join order中是否是unique的，主要用来加速一些判断
  bool is_unique_;
};

typedef common::ObSEArray<ObFdItem *, 8, common::ModulePageAllocator, true> ObFdItemSet;

class ObTableFdItem : public ObFdItem
{
public:
  ObTableFdItem() : ObFdItem(), child_tables_() {}
  ObTableFdItem(const bool is_unique, ObRawExprSet *parent_exprs) :
      ObFdItem(is_unique, parent_exprs), child_tables_() {}
  virtual ~ObTableFdItem() {}

  virtual int assign(const ObTableFdItem &other);
  virtual int check_expr_in_child(const ObRawExpr *expr,
                                  const EqualSets &equal_sets,
                                  bool &is_in_child) const override;

  inline ObRelIds& get_child_tables() { return child_tables_; }
  inline const ObRelIds& get_child_tables() const { return child_tables_; }
  virtual FdLevel get_level() const override;
  virtual bool is_table_fd_item() const override;

  VIRTUAL_TO_STRING_KV(K_(parent_exprs), K_(is_unique), K_(child_tables));
protected:
  ObRelIds child_tables_;
};
inline FdLevel ObTableFdItem::get_level() const { return TABLE_LEVEL; }
inline bool ObTableFdItem::is_table_fd_item() const { return true; }

class ObExprFdItem : public ObFdItem
{
public:
  ObExprFdItem() : ObFdItem(), inner_alloc_("ExprFdItem"), child_exprs_(&inner_alloc_) {}
  ObExprFdItem(const bool is_unique, ObRawExprSet *parent_exprs)
    : ObFdItem(is_unique, parent_exprs), inner_alloc_("ExprFdItem"),
      child_exprs_(&inner_alloc_) {}
  virtual ~ObExprFdItem() {}

  virtual int assign(const ObExprFdItem &other);
  virtual int check_expr_in_child(const ObRawExpr *expr,
                                  const EqualSets &equal_sets,
                                  bool &is_in_child) const override;

  ObRawExprSet& get_child_exprs() { return child_exprs_; }
  const ObRawExprSet& get_child_exprs() const { return child_exprs_; }
  virtual FdLevel get_level() const override;
  virtual bool is_expr_fd_item() const override;

  VIRTUAL_TO_STRING_KV(K_(parent_exprs), K_(is_unique), K_(child_exprs));
protected:
  common::ModulePageAllocator inner_alloc_;
  ObRawExprSet child_exprs_;
};
inline FdLevel ObExprFdItem::get_level() const { return EXPR_LEVEL; }
inline bool ObExprFdItem::is_expr_fd_item() const { return true; }

class ObFdItemFactory
{
public:
  explicit ObFdItemFactory(common::ObIAllocator &alloc) :
      allocator_(alloc), item_store_(alloc), item_set_store_(alloc) {}
  ~ObFdItemFactory() {}

  int create_fd_item_set(ObFdItemSet *&fd_item_set);

  int get_parent_exprs_ptr(const ObIArray<ObRawExpr *> &parent_exprs,
                           ObRawExprSet *&parent_exprs_ptr);
  int get_parent_exprs_ptr(ObRawExpr *parent_expr,
                           ObRawExprSet *&parent_exprs_ptr);

  int create_table_fd_item(ObTableFdItem *&fd_item,
                           const bool is_unique,
                           ObRawExprSet *parent_exprs);

  int create_table_fd_item(ObTableFdItem *&fd_item,
                           const bool is_unique,
                           const ObIArray<ObRawExpr *> &parent_exprs,
                           const ObRelIds &table_set);

  int create_table_fd_item(ObTableFdItem *&fd_item,
                           const ObTableFdItem &other);

  int create_expr_fd_item(ObExprFdItem *&fd_item,
                          const bool is_unique,
                          ObRawExprSet *parent_exprs);

  int create_expr_fd_item(ObExprFdItem *&fd_item,
                          const bool is_unique,
                          const ObIArray<ObRawExpr *> &parent_exprs,
                          const ObIArray<ObRawExpr *> &child_exprs);

  int create_expr_fd_item(ObExprFdItem *&fd_item,
                          const ObExprFdItem &other);

  int copy_fd_item(ObFdItem *&fd_item, const ObFdItem &other);

  //利用已有 fd_item_set/const/equal_set 推导添加新 fd/const
  int deduce_fd_item_set(const EqualSets &equal_sets,
                         ObIArray<ObRawExpr *> &column_exprs,
                         ObIArray<ObRawExpr *> &const_exprs,
                         ObFdItemSet &fd_item_set);

  int do_deduce_fd_item_set(const EqualSets &equal_sets,
                            ObIArray<ObRawExpr *> &column_exprs,
                            ObIArray<ObRawExpr *> &const_exprs,
                            ObFdItemSet &fd_item_set,
                            ObSqlBitSet<> &deduced_fd);

  inline void destory()
  {
    DLIST_FOREACH_NORET(node, item_store_.get_obj_list()) {
      if (node != NULL && node->get_obj() != NULL) {
        node->get_obj()->~ObFdItem();
      }
    }
    DLIST_FOREACH_NORET(node, item_set_store_.get_obj_list()) {
      if (node != NULL && node->get_obj() != NULL) {
        node->get_obj()->~ObFdItemSet();
      }
    }
    parent_sets_.destroy();
  }

  inline common::ObIAllocator &get_allocator() { return allocator_; }
  TO_STRING_KV("", "");
private:
  common::ObIAllocator &allocator_;
  common::ObObjStore<ObFdItem *, common::ObIAllocator&, true> item_store_;
  common::ObObjStore<ObFdItemSet *, common::ObIAllocator&, true> item_set_store_;
  ObRawExprSets parent_sets_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFdItemFactory);
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_FD_ITEM_H
