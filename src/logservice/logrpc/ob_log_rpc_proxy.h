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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RPC_PROXY_
#define OCEANBASE_LOGSERVICE_OB_LOG_RPC_PROXY_

#include "ob_log_rpc_req.h"
#include "rpc/obrpc/ob_rpc_proxy.h"                             // ObRpcProxy

namespace oceanbase
{
namespace obrpc
{

class ObLogServiceRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObLogServiceRpcProxy);
  RPC_S(PR3 send_log_config_change_cmd, OB_LOG_CONFIG_CHANGE_CMD, 
      (logservice::LogConfigChangeCmd), logservice::LogConfigChangeCmdResp);
  RPC_S(PR3 get_palf_stat, OB_LOG_GET_PALF_STAT,
      (logservice::LogGetPalfStatReq), logservice::LogGetPalfStatResp);
};

} // end namespace obrpc
} // end namespace oceanbase

#endif


