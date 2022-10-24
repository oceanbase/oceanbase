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

#ifndef OCEANBASE_SQL_OB_SQL_EXPRESSION_FACTORY_H
#define OCEANBASE_SQL_OB_SQL_EXPRESSION_FACTORY_H

#include "lib/container/ob_array.h"
namespace oceanbase
{
namespace sql
{
//不提供free(sql-expression)的接口，请在alloc上面统一释放
//最好expr_op和sql-expression使用同一个alloc, 否则要求要求用户
//单独释放expr_op占用的空间
class ObSqlExpressionFactory
{
public:
  explicit  ObSqlExpressionFactory(common::ObIAllocator &alloc) : alloc_(alloc)
  {}
  ~ObSqlExpressionFactory()
  {
  }

  template<typename T>
      int alloc(T *&sql_expression)
      {
        int ret = common::OB_SUCCESS;
        void *ptr = NULL;
        int64_t item_count = 0;
        if (OB_ISNULL(ptr = (alloc_.alloc(sizeof(T))))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "fail to alloc memory", K(ret), K(ptr));
        } else {
          sql_expression = new(ptr)T(alloc_, item_count);
        }
        return ret;
      }

  template<typename T>
      int alloc(T *&sql_expression, int64_t item_count)
      {
        int ret = common::OB_SUCCESS;
        void *ptr = NULL;
        if (OB_ISNULL(ptr = (alloc_.alloc(sizeof(T))))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "fail to alloc memory", K(ret), K(ptr));
        } else {
          sql_expression = new(ptr)T(alloc_, item_count);
        }
        return ret;
      }
  void destroy()
  {
    //nothing todo
    //所有内存都在alloc上面，由alloc统一释放
  }
  //template<typename T>
  //    void free(T *&sql_expression)
  //    {
  //      if (OB_ISNULL(sql_expression)) {
  //      } else {
  //        sql_expression->~ObSqlExpression();
  //        alloc_.free(sql_expression);
  //        sql_expression = NULL;
  //      }
  //    }
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlExpressionFactory);
private:
  common::ObIAllocator &alloc_;
};
} //namespace sql
} //namespace oceanbase
#endif

