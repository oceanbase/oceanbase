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
 * This file is for implementation of func json_merge
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