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

#ifndef OCEANBASE_SQL_OB_Purge_RESOLVER_
#define OCEANBASE_SQL_OB_Purge_RESOLVER_

#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

class ObPurgeTableResolver : public ObDDLResolver
{
  static const int TABLE_NODE = 0;
public:
  explicit ObPurgeTableResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObPurgeTableResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  static const int OLD_NAME_NODE = 0;
  static const int NEW_NAME_NODE = 1;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeTableResolver);
};

class ObPurgeIndexResolver : public ObDDLResolver
{
  static const int TABLE_NODE = 0;
public:
  explicit ObPurgeIndexResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObPurgeIndexResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  static const int OLD_NAME_NODE = 0;
  static const int NEW_NAME_NODE = 1;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeIndexResolver);
};

class ObPurgeDatabaseResolver : public ObDDLResolver
{
  static const int DATABASE_NODE = 0;
public:
  explicit ObPurgeDatabaseResolver(ObResolverParams &params)
   : ObDDLResolver(params){}
  virtual ~ObPurgeDatabaseResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPurgeDatabaseResolver);
};

class ObPurgeTenantResolver : public ObDDLResolver
{
  static const int TENANT_NODE = 0;
public:
  explicit ObPurgeTenantResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObPurgeTenantResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPurgeTenantResolver);
};

class ObPurgeRecycleBinResolver : public ObDDLResolver
{
public:
  explicit ObPurgeRecycleBinResolver(ObResolverParams &params)
    : ObDDLResolver(params){}
  virtual ~ObPurgeRecycleBinResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPurgeRecycleBinResolver);
};

} //namespace sql
} //namespace oceanbase
#endif //OCEANBASE_SQL_OB_PURGE_RESOLVER_



