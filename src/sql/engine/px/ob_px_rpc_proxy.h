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

#ifndef OB_PX_RPC_PROXY_H
#define OB_PX_RPC_PROXY_H

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "sql/engine/px/ob_dfo.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {

namespace obrpc {

class ObPxRpcProxy : public ObRpcProxy {
public:
  DEFINE_TO(ObPxRpcProxy);
  // init sqc rpc synchronously
  RPC_S(PR5 init_sqc, OB_PX_INIT_SQC, (sql::ObPxRpcInitSqcArgs), sql::ObPxRpcInitSqcResponse);
  RPC_S(PR5 init_task, OB_PX_INIT_TASK, (sql::ObPxRpcInitTaskArgs), sql::ObPxRpcInitTaskResponse);
  // init sqc rpc asynchronously
  RPC_AP(PR5 async_init_sqc, OB_PX_ASYNC_INIT_SQC, (sql::ObPxRpcInitSqcArgs), sql::ObPxRpcInitSqcResponse);
  // init sqc rpc single dfo scheduling
  RPC_AP(PR5 fast_init_sqc, OB_PX_FAST_INIT_SQC, (sql::ObPxRpcInitSqcArgs), sql::ObPxRpcInitSqcResponse);
};

}  // namespace obrpc
}  // namespace oceanbase

#endif /* OB_PX_RPC_PROXY_H */
