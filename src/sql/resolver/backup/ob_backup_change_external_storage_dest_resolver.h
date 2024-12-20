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

#ifndef OCEANBASE_SQL_RESOVLER_BACKUP_CHANGE_EXTERNAL_STORAGE_DEST_RESOLVER_
#define OCEANBASE_SQL_RESOVLER_BACKUP_CHANGE_EXTERNAL_STORAGE_DEST_RESOLVER_

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"
#include "sql/session/ob_sql_session_info.h" // ObSqlSessionInfo
#include "share/ls/ob_ls_i_life_manager.h" //OB_LS_MAX_SCN_VALUE

namespace oceanbase
{
namespace sql
{

class ObChangeExternalStorageDestResolver : public ObSystemCmdResolver
{
public:
  ObChangeExternalStorageDestResolver(ObResolverParams &params) : ObSystemCmdResolver(params) {}
  virtual ~ObChangeExternalStorageDestResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
};

} // end namespace sql
} // end namespace oceanbase

#endif
