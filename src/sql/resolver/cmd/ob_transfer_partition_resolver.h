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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_TRANSFER_PARTITION_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_TRANSFER_PARTITION_RESOLVER_

#include "sql/resolver/cmd/ob_transfer_partition_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObTransferPartitionResolver : public ObSystemCmdResolver
{
public:
  ObTransferPartitionResolver(ObResolverParams &params) : ObSystemCmdResolver(params) {}
  virtual ~ObTransferPartitionResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_transfer_partition_(const ParseNode &parse_tree);
  int resolve_cancel_transfer_partition_(const ParseNode &parse_tree);
  int resolve_balance_job_op_(const ParseNode &parse_tree);
  int resolve_part_info_(const ParseNode &parse_node, uint64_t &table_id, ObObjectID &object_id);
  int resolve_transfer_partition_to_ls_(
    const ParseNode &parse_node,
    const uint64_t target_tenant_id,
    const uint64_t exec_tenant_id,
    ObTransferPartitionStmt *stmt);
};
} // share
} // oceanbase
#endif // OCEANBASE_SQL_RESOLVER_CMD_OB_TRANSFER_PARTITION_RESOLVER_
