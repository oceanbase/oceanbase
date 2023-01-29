// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_SQL_OB_TM_SERVICE_H
#define OCEANBASE_SQL_OB_TM_SERVICE_H

#include "storage/tx/ob_dblink_client.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
class ObTMService
{
public:
  static int tm_rm_start(ObExecContext &exec_ctx,
                         const common::sqlclient::DblinkDriverProto dblink_type,
                         common::sqlclient::ObISQLConnection *dblink_conn,
                         transaction::ObTransID &tx_id);
  static int tm_commit(ObExecContext &exec_ctx,
                       transaction::ObTransID &tx_id);
  static int tm_rollback(ObExecContext &exec_ctx,
                         transaction::ObTransID &tx_id);
  // for callback link
  static int recover_tx_for_callback(const transaction::ObTransID &tx_id,
                                     ObExecContext &exec_ctx);
  static int revert_tx_for_callback(ObExecContext &exec_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTMService);
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_TM_SERVICE_H