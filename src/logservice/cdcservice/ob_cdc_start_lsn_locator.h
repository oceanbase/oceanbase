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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_START_LSN_LOCATOR_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_START_LSN_LOCATOR_

#include "share/ob_ls_id.h"                     // ObLSID
#include "logservice/palf_handle_guard.h"       // PalfHandleGuard
#include "logservice/palf/lsn.h"                // LSN
#include "ob_cdc_req.h"                         // RPC Request and Response
#include "ob_cdc_struct.h"
#include "logservice/archiveservice/large_buffer_pool.h" // LargeBufferPool
namespace oceanbase
{
namespace logservice
{
class ObLogExternalStorageHandler;
}
namespace cdc
{
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;
class ObCdcService;

typedef obrpc::ObCdcReqStartLSNByTsReq ObLocateLSNByTsReq;
typedef obrpc::ObCdcReqStartLSNByTsResp ObLocateLSNByTsResp;

class ObCdcStartLsnLocator
{
public:
  ObCdcStartLsnLocator();
  ~ObCdcStartLsnLocator();
  int init(const uint64_t tenant_id,
      ObCdcService *host,
      archive::LargeBufferPool *large_buffer_pool,
      logservice::ObLogExternalStorageHandler *log_ext_handler);
  void destroy();
  int req_start_lsn_by_ts_ns(const ObLocateLSNByTsReq &req_msg,
      ObLocateLSNByTsResp &result,
      volatile bool &stop_flag);

private:
  int do_req_start_lsn_(const ObLocateLSNByTsReq &req,
      ObLocateLSNByTsResp &resp,
      volatile bool &stop_flag);
  inline int64_t get_rpc_deadline_() const { return THIS_WORKER.get_timeout_ts(); }
  int handle_when_hurry_quit_(const ObLocateLSNByTsReq::LocateParam &locate_param,
      ObLocateLSNByTsResp &result);
  int do_locate_ls_(const bool fetch_archive_only,
      const ObLocateLSNByTsReq::LocateParam &locate_param,
      ObLocateLSNByTsResp &resp);

  // @retval OB_SUCCESS         Success
  // @retval OB_ENTRY_NOT_EXIST LS not exist in this server
  int init_palf_handle_guard_(const ObLSID &ls_id,
      palf::PalfHandleGuard &palf_handle_guard);

private:
  bool is_inited_;
  ObCdcService *host_;
  uint64_t tenant_id_;
  archive::LargeBufferPool *large_buffer_pool_;
  logservice::ObLogExternalStorageHandler *log_ext_handler_;
};

} // namespace cdc
} // namespace oceanbase

#endif
