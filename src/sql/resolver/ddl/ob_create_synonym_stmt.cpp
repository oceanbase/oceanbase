/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_create_synonym_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{

ObCreateSynonymStmt::ObCreateSynonymStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_SYNONYM),
      create_synonym_arg_()
{
}

ObCreateSynonymStmt::ObCreateSynonymStmt()
    : ObDDLStmt(stmt::T_CREATE_SYNONYM),
      create_synonym_arg_()
{
}

ObCreateSynonymStmt::~ObCreateSynonymStmt()
{
}


} // namespace sql
} // namespace oceanbase
