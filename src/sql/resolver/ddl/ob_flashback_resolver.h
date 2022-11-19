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

#ifndef OCEANBASE_SQL_OB_FLASHBACK_RESOLVER_
#define OCEANBASE_SQL_OB_FLASHBACK_RESOLVER_

#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObFlashBackTableFromRecyclebinResolver : public ObDDLResolver
{
  static const int ORIGIN_TABLE_NODE = 0;
  static const int NEW_TABLE_NODE = 1;
public:
  explicit ObFlashBackTableFromRecyclebinResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObFlashBackTableFromRecyclebinResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackTableFromRecyclebinResolver);
};

class ObFlashBackTableToScnResolver : public ObDDLResolver
{
  static const int TABLE_NODES = 0;
  static const int TIME_NODE = 1;
public:
  explicit ObFlashBackTableToScnResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObFlashBackTableToScnResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackTableToScnResolver);
};

class ObFlashBackIndexResolver : public ObDDLResolver
{
  static const int ORIGIN_TABLE_NODE = 0;
  static const int NEW_TABLE_NODE = 1;
public:
  explicit ObFlashBackIndexResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObFlashBackIndexResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackIndexResolver);
};

class ObFlashBackDatabaseResolver : public ObDDLResolver
{
  static const int ORIGIN_DB_NODE = 0;
  static const int NEW_DB_NODE = 1;
public:
  explicit ObFlashBackDatabaseResolver(ObResolverParams &params)
   : ObDDLResolver(params){}
  virtual ~ObFlashBackDatabaseResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackDatabaseResolver);
};

class ObFlashBackTenantResolver : public ObDDLResolver
{
  static const int ORIGIN_TENANT_NODE = 0;
  static const int NEW_TENANT_NODE = 1;
public:
  explicit ObFlashBackTenantResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObFlashBackTenantResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackTenantResolver);
};


}
}
#endif //OCEANBASE_SQL_OB_FLASHBACK_RESOLVER_

