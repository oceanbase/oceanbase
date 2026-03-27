/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_catalog_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObCatalogStmt::ObCatalogStmt()
    : ObDDLStmt(stmt::T_CREATE_CATALOG), catalog_arg_()
  {
  }

ObCatalogStmt::~ObCatalogStmt()
{
}

}//namespace sql
}//namespace oceanbase
