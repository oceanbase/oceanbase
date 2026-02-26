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

#ifndef OCEANBASE_RESOLVER_CMD_OB_BACKUP_CLEAN_RESOLVER_
#define OCEANBASE_RESOLVER_CMD_OB_BACKUP_CLEAN_RESOLVER_

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace sql
{

class ObAlterSystemResolverBackupCleanUtil
{
public:
  static int get_set_or_piece_ids(const ParseNode &id_node, common::ObIArray<int64_t> &backup_ids);
  static int get_clean_all_path_and_type(const ParseNode &path_node,
                                         share::ObBackupPathString &backup_dest_str,
                                         share::ObBackupDestType::TYPE &dest_type);
  static int handle_backup_clean_parameters(const ParseNode &parse_tree,
                                            share::ObNewBackupCleanType::TYPE clean_type,
                                            common::ObIArray<int64_t> &value,
                                            share::ObBackupPathString &backup_dest_str,
                                            share::ObBackupDestType::TYPE &dest_type);
};

class ObBackupCleanResolver : public ObSystemCmdResolver
{
public:
  ObBackupCleanResolver(ObResolverParams &params) : ObSystemCmdResolver(params) {}
  virtual ~ObBackupCleanResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_RESOLVER_CMD_OB_BACKUP_CLEAN_RESOLVER_
