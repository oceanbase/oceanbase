/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXTRA_INFO_FACTORY_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXTRA_INFO_FACTORY_

#include "sql/engine/expr/ob_expr.h"
#include "objit/common/ob_item_type.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace sql
{
struct ObIExprExtraInfo;

struct ObExprExtraInfoFactory
{
public:
  static constexpr int64_t MAX_ITEM_ID = T_DEFAULT;
  typedef int (*AllocExtraInfoFunc) (common::ObIAllocator &alloc, ObIExprExtraInfo *&extra_info,
                                     const ObExprOperatorType type);
  // allocate extra info
  static int alloc(common::ObIAllocator &alloc,
                   const ObExprOperatorType &type,
                   ObIExprExtraInfo *&extra_info);

  static void register_expr_extra_infos();

  inline static constexpr bool is_valid_item_type(const ObExprOperatorType &type) {
    return (type > T_INVALID && type < MAX_ITEM_ID);
  }

  inline static bool is_registered(const ObExprOperatorType &type)
  {
    return is_valid_item_type(type)
           && NULL != ALLOC_FUNS_[type];
  }

private:
  template <typename T>
  static int alloc(common::ObIAllocator &alloc, ObIExprExtraInfo *&extra_info,
                   const ObExprOperatorType type);

private:
  static AllocExtraInfoFunc ALLOC_FUNS_[MAX_ITEM_ID];
};

template <typename T>
int ObExprExtraInfoFactory::alloc(common::ObIAllocator &alloc,
                                  ObIExprExtraInfo *&extra_info,
                                  const ObExprOperatorType type)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc expr_operator", K(ret));
  } else {
    extra_info = new(buf) T(alloc, type);
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
#endif
