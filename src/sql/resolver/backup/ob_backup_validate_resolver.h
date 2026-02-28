
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

#ifndef OCEANBASE_SQL_RESOLVER_BACKUP_OB_BACKUP_VALIDATE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_BACKUP_OB_BACKUP_VALIDATE_RESOLVER_H_

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"
#include "sql/session/ob_sql_session_info.h" // ObSqlSessionInfo
#include "share/ls/ob_ls_i_life_manager.h" //OB_LS_MAX_SCN_VALUE
#include "share/backup/ob_backup_store.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObBackupValidateResolver : public ObSystemCmdResolver
{
public:
  ObBackupValidateResolver(ObResolverParams &params) : ObSystemCmdResolver(params) {}
  virtual ~ObBackupValidateResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int parse_and_set_stmt_(
      const ParseNode &parse_tree,
      const uint64_t tenant_id,
      ObBackupValidateStmt *stmt);
  int get_set_or_piece_ids_(const char *set_or_piece_ids_str, ObIArray<uint64_t> &set_or_piece_ids);
  int get_dest_(const ParseNode &path_node, const uint64_t validate_type_value,
          share::ObBackupPathString &backup_dest_str);
};

} // end namespace sql
} // end namespace oceanbase
#endif