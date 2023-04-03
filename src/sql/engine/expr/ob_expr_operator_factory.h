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

#ifndef OCEANBASE_SQL_OB_EXPR_OPERATOR_FACTORY_H
#define OCEANBASE_SQL_OB_EXPR_OPERATOR_FACTORY_H
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
typedef int (*AllocFunc) (common::ObIAllocator &alloc, ObExprOperator *&phy_op);
class ObExprOperatorFactory
{
public:
  explicit  ObExprOperatorFactory(common::ObIAllocator &alloc) : alloc_(alloc), next_expr_id_(NULL) {}
  ~ObExprOperatorFactory() {}
  int alloc(ObExprOperatorType type, ObExprOperator *&phy_op);
  template <typename T>
  int create_fast_expr(ObExprOperatorType type, T *&fast_op);
  int alloc_fast_expr(ObExprOperatorType type, ObFastExprOperator *&fast_op);
  template <typename T>
  inline int alloc(ObExprOperatorType type, T *&phy_op);
  void set_next_expr_id(uint32_t *next_expr_id) { next_expr_id_ = next_expr_id; }
  //int free(ObExprOperator *&phy_op);
  static bool is_expr_op_type_valid(ObExprOperatorType type);
  static ObExprOperatorType get_type_by_name(const common::ObString &name);
  static void get_internal_info_by_name(const ObString &name, bool &exist, bool &is_internal);
  static void get_function_alias_name(const ObString &origin_name, ObString &alias_name);
  static void register_expr_operators();
  static char *str_toupper(char *buff);
  void destroy()
  {
    //nothing todo
    //分配出来的expr_op的内存空间由alloc统一释放
  }
  struct NameType
  {
    const char *name_;
    ObExprOperatorType type_;
    bool is_internal_;
  };
private:
  template <typename ClassT>
      static int alloc(common::ObIAllocator &alloc, ObExprOperator *&phy_op);
private:
  static NameType NAME_TYPES[EXPR_OP_NUM];
  static NameType NAME_TYPES_ORCL[EXPR_OP_NUM];
  common::ObIAllocator &alloc_;
  uint32_t *next_expr_id_;
};

template <typename T>
inline int ObExprOperatorFactory::alloc(ObExprOperatorType type, T *&phy_op)
{
  int ret = common::OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = alloc_.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc expr_op", K(ret), K(type));
  } else {
    phy_op = new(ptr) T();
    phy_op->set_expr_type(type);
  }
  if (OB_SUCC(ret) && NULL != next_expr_id_) {
    phy_op->set_expr_id((*next_expr_id_)++);
  }
  return ret;
}

template <typename T>
inline int ObExprOperatorFactory::create_fast_expr(ObExprOperatorType type, T *&fast_op)
{
  int ret = common::OB_SUCCESS;
  ObFastExprOperator *op = NULL;
  if (OB_SUCC(alloc_fast_expr(type, op))) {
    fast_op = static_cast<T *>(op);
  }
  return ret;
}
}
}
#endif //OCEANBASE_SQL_OB_EXPR_OPERATOR_FACTORY_H
