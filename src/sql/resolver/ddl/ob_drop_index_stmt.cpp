/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
