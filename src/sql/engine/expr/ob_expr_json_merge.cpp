/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_merge.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonMerge::ObExprJsonMerge(ObIAllocator &alloc)
    : ObExprJsonMergePreserve(alloc,
      T_FUN_SYS_JSON_MERGE,
      N_JSON_MERGE, 
      MORE_THAN_ONE,
      NOT_ROW_DIMENSION)
{
}

ObExprJsonMerge::~ObExprJsonMerge()
{
}

}
}