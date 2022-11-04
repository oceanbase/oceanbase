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

#include "sql/resolver/ddl/ob_drop_index_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

ObDropIndexStmt::ObDropIndexStmt(ObIAllocator *name_pool)
   : ObDDLStmt(name_pool, stmt::T_DROP_INDEX),
    name_pool_(name_pool),
    drop_index_arg_(),
    table_id_(OB_INVALID_ID)
{
}

ObDropIndexStmt::ObDropIndexStmt()
    :ObDDLStmt(NULL, stmt::T_DROP_INDEX),name_pool_(NULL), table_id_(OB_INVALID_ID)
{
}

ObDropIndexStmt::~ObDropIndexStmt()
{
}

}// end of sql
}//end of oceanbase
