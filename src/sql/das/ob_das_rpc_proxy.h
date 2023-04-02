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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROXY_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROXY_H_
#include "sql/das/ob_das_task.h"
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/rpc/ob_async_rpc_proxy.h"
namespace oceanbase
{
namespace obrpc
{
class ObDASRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObDASRpcProxy);
  virtual ~ObDASRpcProxy() {}
  //stream rpc interface
  RPC_S(@PR5 remote_sync_access, obrpc::OB_DAS_SYNC_ACCESS, (sql::ObDASTaskArg), sql::ObDASTaskResp);
  // sync rpc for das task result
  RPC_S(@PR5 sync_fetch_das_result, obrpc::OB_DAS_SYNC_FETCH_RESULT, (sql::ObDASDataFetchReq), sql::ObDASDataFetchRes);
  // async rpc to erase das task result
  RPC_AP(@PR5 async_erase_das_result, obrpc::OB_DAS_ASYNC_ERASE_RESULT, (sql::ObDASDataEraseReq));
  RPC_AP(@PR5 das_async_access, obrpc::OB_DAS_ASYNC_ACCESS, (sql::ObDASTaskArg), sql::ObDASTaskResp);
};

}  // namespace obrpc
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROXY_H_ */
