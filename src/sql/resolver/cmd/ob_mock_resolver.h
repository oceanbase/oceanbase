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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_MOCK_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_MOCK_RESOLVER_H_
#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObMockResolver : public ObCMDResolver
{
public:
  explicit ObMockResolver(ObResolverParams &params) :
        ObCMDResolver(params)
  {
  }

  virtual ~ObMockResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMockResolver);

  stmt::StmtType type_convert(ObItemType type) {
    stmt::StmtType ret = stmt::T_NONE;
    switch (type) {
      case T_FLUSH_PRIVILEGES:
        ret = stmt::T_FLUSH_PRIVILEGES;
        break;
      case T_INSTALL_PLUGIN:
        ret = stmt::T_INSTALL_PLUGIN;
        break;
      case T_UNINSTALL_PLUGIN:
        ret = stmt::T_UNINSTALL_PLUGIN;
        break;
      case T_FLUSH_MOCK:
        ret = stmt::T_FLUSH_MOCK;
        break;
      case T_HANDLER_MOCK:
        ret = stmt::T_HANDLER_MOCK;
        break;
      case T_SHOW_PLUGINS:
        ret = stmt::T_SHOW_PLUGINS;
        break;
      case T_REPAIR_TABLE:
        ret = stmt::T_REPAIR_TABLE;
        break;
      case T_CHECKSUM_TABLE:
        ret = stmt::T_CHECKSUM_TABLE;
        break;
      case T_CACHE_INDEX:
        ret = stmt::T_CACHE_INDEX;
        break;
      case T_LOAD_INDEX_INTO_CACHE:
        ret = stmt::T_LOAD_INDEX_INTO_CACHE;
        break;
      case T_CREATE_SERVER:
        ret = stmt::T_CREATE_SERVER;
        break;
      case T_ALTER_SERVER:
        ret = stmt::T_ALTER_SERVER;
        break;
      case T_DROP_SERVER:
        ret = stmt::T_DROP_SERVER;
        break;
      case T_CREATE_LOGFILE_GROUP:
        ret = stmt::T_CREATE_LOGFILE_GROUP;
        break;
      case T_ALTER_LOGFILE_GROUP:
        ret = stmt::T_ALTER_LOGFILE_GROUP;
        break;
      case T_DROP_LOGFILE_GROUP:
        ret = stmt::T_DROP_LOGFILE_GROUP;
        break;
      default:
        ret = stmt::T_NONE;
    }
    return ret;
  }
};

}  // namespace sql
}  // namespace oceanbase
#endif