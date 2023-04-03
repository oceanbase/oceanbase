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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_RPC_DEFINE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_RPC_DEFINE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"         // print
#include "lib/net/ob_addr.h"                    // ObAddr
#include "logservice/palf/lsn.h"                // LSN
#include "rpc/frame/ob_req_transport.h"
#include "rpc/obrpc/ob_rpc_proxy.h"             // ObRpcProxy
#include "rpc/obrpc/ob_rpc_proxy_macros.h"      // RPC_*
#include "share/ob_ls_id.h"                     // ObLSID
#include "observer/ob_server_struct.h"          // GCTX
#include "share/config/ob_server_config.h"      // GCONF
#include "ob_log_restore_define.h"              // MAX_FETCH_LOG_BUF_LEN
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
class ObLogService;
}
namespace obrpc
{
class ObSrvRpcProxy;
// remote fetch log request and response
struct ObRemoteFetchLogRequest
{
  uint64_t tenant_id_;
  share::ObLSID id_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;

  ObRemoteFetchLogRequest() {}
  ObRemoteFetchLogRequest(const uint64_t tenant_id,
                          const share::ObLSID &id,
                          const palf::LSN &start_lsn,
                          const palf::LSN &end_lsn) :
    tenant_id_(tenant_id),
    id_(id),
    start_lsn_(start_lsn),
    end_lsn_(end_lsn)
  {}

  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(id), K_(start_lsn), K_(end_lsn));

  OB_UNIS_VERSION(1);
};

struct ObRemoteFetchLogResponse
{
public:
  ObRemoteFetchLogResponse() { reset(); }
  ~ObRemoteFetchLogResponse() { reset(); }

public:
  int err_code_;
  share::ObLSID id_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;
  int64_t data_len_;
  char data_[logservice::MAX_FETCH_LOG_BUF_LEN];

  bool is_valid() const;
  bool is_empty() const;
  void reset();

  TO_STRING_KV(K_(err_code), K_(id), K_(start_lsn), K_(end_lsn), K_(data_len), K_(data));

  NEED_SERIALIZE_AND_DESERIALIZE;
};

class ObLogResSvrProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObLogResSvrProxy);

  RPC_S(PR1 remote_fetch_log, OB_REMOTE_FETCH_LOG,
      (ObRemoteFetchLogRequest), ObRemoteFetchLogResponse);
};

///////////////////////////////// RPC处理函数 /////////////////////////////////////
class ObRemoteFetchLogP : public ObLogResSvrProxy::Processor<OB_REMOTE_FETCH_LOG>
{
public:
  ObRemoteFetchLogP() : log_service_(NULL) {}
  virtual ~ObRemoteFetchLogP() {}
protected:
  int process();
private:
  int get_log_service_(const uint64_t tenant_id, logservice::ObLogService *&log_service);
  int fetch_log_(const share::ObLSID &id, const palf::LSN &start_lsn, const palf::LSN &end_lsn,
      char *data, int64_t &data_len);
private:
  logservice::ObLogService *log_service_;
};

} // namespace obrpc
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_RPC_DEFINE_H_ */
