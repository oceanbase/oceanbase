/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT_H_
#define OB_ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT_H_

#include "logservice/cdcservice/ob_cdc_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "logservice/cdcservice/ob_cdc_req_struct.h"

namespace oceanbase
{

namespace observer
{
class ObAllVirtualLogTransportDestStat: public common::ObVirtualTableScannerIterator
{
  static constexpr int MAX_RPC_REQUEST_INFO_LEN = 4096;
  enum ObAllVirtualLogTransportDestStatColumn : int64_t
  {
    TENANT_ID = OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    LS_ID,
    CLIENT_IP,
    CLIENT_PID,
    CLIENT_TENANT_ID,
    CLIENT_TYPE,
    START_SERVE_TIME,
    LAST_SERVE_TIME,
    LAST_READ_SOURCE,
    LAST_REQUEST_TYPE,
    LAST_REQUEST_LOG_LSN,
    LAST_REQUEST_LOG_SCN,
    LAST_FAILED_REQUEST,
    AVG_REQUEST_PROCESS_TIME,
    AVG_REQUEST_QUEUE_TIME,
    AVG_REQUEST_READ_LOG_TIME,
    AVG_REQUEST_READ_LOG_SIZE,
    AVG_LOG_TRANSPORT_BANDWIDTH,
  };

  class ClientLSCtxMapStatFunctor
  {
  public:
    ClientLSCtxMapStatFunctor(ObAllVirtualLogTransportDestStat &stat_table):
        stat_vtable_(stat_table) { }
    bool operator()(const cdc::ClientLSKey &key, const cdc::ClientLSCtx *ctx);
    ObAllVirtualLogTransportDestStat &stat_vtable_;
  };

  class TenantStatFunctor
  {
  public:
    TenantStatFunctor(ObAllVirtualLogTransportDestStat &stat_table):
      functor_(stat_table) {}
    int operator()();

    ClientLSCtxMapStatFunctor functor_;
  };

public:
  explicit ObAllVirtualLogTransportDestStat(omt::ObMultiTenant *omt);
  virtual ~ObAllVirtualLogTransportDestStat();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void destroy();
private:
  int insert_log_transport_dest_stat_(const cdc::ClientLSKey &key,
      const cdc::ClientLSCtx &ctx);
  int rpc_req_info_to_string_(const cdc::ClientLSCtx::RpcRequestInfo &info);
private:
  char self_ip_[MAX_IP_ADDR_LENGTH] = {'\0'};
  char client_ip_[MAX_IP_ADDR_LENGTH] = { '\0' };
  char last_failed_reqeuest_[MAX_RPC_REQUEST_INFO_LEN] = { '\0' };
  omt::ObMultiTenant *omt_;
};
}
}

#endif