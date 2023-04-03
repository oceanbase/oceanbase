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
