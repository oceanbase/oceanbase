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

#ifndef OCEANBASE_CLOG_OB_LOG_RPC_PROXY_H_
#define OCEANBASE_CLOG_OB_LOG_RPC_PROXY_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "ob_clog_sync_msg.h"
#include "ob_log_define.h"
#include "ob_log_rpc.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace obrpc {
class ObLogRpcProxy;
struct ObLogRpcProxyBuffer {
  OB_UNIS_VERSION(1);

public:
  ObLogRpcProxyBuffer() : pcode_(0), data_(NULL), len_(0)
  {}
  ObLogRpcProxyBuffer(int pcode, const char* data, int64_t len) : pcode_(pcode), data_(data), len_(len)
  {}
  ~ObLogRpcProxyBuffer()
  {}
  int get_pcode()
  {
    return pcode_;
  }
  int pcode_;
  const char* data_;
  int64_t len_;
  TO_STRING_KV(N_PCODE, pcode_, N_DATA_LEN, len_, N_BUF, ((uint64_t)data_));
};

class ObLogRpcProxy : public clog::ObILogRpc, public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObLogRpcProxy);
  static const int64_t MAX_PROCESS_HANDLER_TIME = 100 * 1000;

  typedef struct ObLogRpcProxyBuffer Buffer;

  RPC_AP(PR3 log_rpc, OB_CLOG, (ObLogRpcProxyBuffer));
  RPC_S(PR5 get_mc_ts, OB_LOG_GET_MC_TS, (ObLogGetMCTsRequest), ObLogGetMCTsResponse);
  RPC_S(PR5 get_mc_ctx_array, OB_LOG_GET_MC_CTX_ARRAY, (ObLogGetMcCtxArrayRequest), ObLogGetMcCtxArrayResponse);
  RPC_S(
      PR5 get_priority_array, OB_LOG_GET_PRIORITY_ARRAY, (ObLogGetPriorityArrayRequest), ObLogGetPriorityArrayResponse);
  RPC_S(PR5 get_remote_log, OB_LOG_GET_REMOTE_LOG, (ObLogGetRemoteLogRequest), ObLogGetRemoteLogResponse);

  int post(const common::ObAddr& server, int pcode, const char* data, int64_t len);

private:
  bool cluster_version_before_1472_() const;
};

};  // end namespace obrpc
};  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_RPC_PROXY_
