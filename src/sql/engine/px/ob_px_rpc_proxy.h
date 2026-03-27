/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_PX_RPC_PROXY_H
#define OB_PX_RPC_PROXY_H

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "sql/engine/px/ob_dfo.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/px/ob_px_target_monitor_rpc.h"

namespace oceanbase {

namespace obrpc {


class ObPxRpcProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(ObPxRpcProxy);
  // init sqc rpc synchronously
  RPC_S(PR5 init_sqc, OB_PX_INIT_SQC, (sql::ObPxRpcInitSqcArgs), sql::ObPxRpcInitSqcResponse);
  RPC_S(PR5 init_task, OB_PX_INIT_TASK, (sql::ObPxRpcInitTaskArgs), sql::ObPxRpcInitTaskResponse);
  // init sqc rpc asynchronously
  RPC_AP(PR5 async_init_sqc, OB_PX_ASYNC_INIT_SQC, (sql::ObPxRpcInitSqcArgs), sql::ObPxRpcInitSqcResponse);
  // 单dfo调度rpc
  RPC_AP(PR5 fast_init_sqc, OB_PX_FAST_INIT_SQC, (sql::ObPxRpcInitSqcArgs), sql::ObPxRpcInitSqcResponse);
  // px资源监控
  RPC_S(PR5 fetch_statistics, OB_PX_TARGET_REQUEST, (sql::ObPxRpcFetchStatArgs), sql::ObPxRpcFetchStatResponse);
  RPC_AP(PR5 clean_dtl_interm_result, OB_CLEAN_DTL_INTERM_RESULT, (sql::ObPxCleanDtlIntermResArgs));
};

}  // obrpc
}  // oceanbase


#endif /* OB_PX_RPC_PROXY_H */
