/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_VIRTUAL_TABLE_CTX_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_VIRTUAL_TABLE_CTX_

#include "share/ob_define.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}

namespace sql
{
class ObIVirtualTableIteratorFactory;
class ObSQLSessionInfo;
class ObVirtualTableCtx
{
public:
  ObVirtualTableCtx()
      : vt_iter_factory_(NULL),
        schema_guard_(NULL),
        session_(NULL)
  {}
  ~ObVirtualTableCtx() {}

  void reset()
  {
    vt_iter_factory_ = NULL;
    schema_guard_ = NULL;
    session_ = NULL;
  }

  ObIVirtualTableIteratorFactory *vt_iter_factory_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  ObSQLSessionInfo *session_;
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_TABLE_OB_VIRTUAL_TABLE_CTX_ */
