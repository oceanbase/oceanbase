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

#include "ob_cdc_rpc_processor.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace obrpc
{
int __get_cdc_service(uint64_t tenant_id, cdc::ObCdcService *&cdc_service)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = nullptr;

  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "mtl id not match", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "get_log_service failed", KR(ret));
  } else if (OB_ISNULL(cdc_service = log_service->get_cdc_service())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "cdc_service is NULL", KR(ret), K(tenant_id));
	} else {
    // get CdcService succ
  }

	return ret;
}

int ObCdcLSReqStartLSNByTsP::process()
{
  int ret = common::OB_SUCCESS;
  const ObCdcReqStartLSNByTsReq &req = arg_;
  ObCdcReqStartLSNByTsResp &resp = result_;
  cdc::ObCdcService *cdc_service = nullptr;

  if (OB_FAIL(__get_cdc_service(rpc_pkt_->get_tenant_id(), cdc_service))) {
    EXTLOG_LOG(ERROR, "__get_cdc_service failed", KR(ret));
  } else if (OB_ISNULL(cdc_service)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "cdc_service is null", KR(ret));
  } else {
    ret = cdc_service->req_start_lsn_by_ts_ns(req, resp);
  }

  // rewrite ret for rpc framework
  return OB_SUCCESS;
}

int ObCdcLSFetchLogP::process()
{
  int ret = common::OB_SUCCESS;
  const ObCdcLSFetchLogReq &req = arg_;
  ObCdcLSFetchLogResp &resp = result_;
  cdc::ObCdcService *cdc_service = nullptr;

  if (OB_FAIL(__get_cdc_service(rpc_pkt_->get_tenant_id(), cdc_service))) {
    EXTLOG_LOG(ERROR, "__get_cdc_service failed", KR(ret));
  } else if (OB_ISNULL(cdc_service)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "cdc_service is null", KR(ret));
  } else {
    set_result_compress_type(req.get_compressor_type());
    ret = cdc_service->fetch_log(req, resp, get_send_timestamp(), get_receive_timestamp());
  }

  // rewrite ret for rpc framework
  return OB_SUCCESS;
}

int ObCdcLSFetchMissingLogP::process()
{
  int ret = common::OB_SUCCESS;
  const ObCdcLSFetchMissLogReq &req = arg_;
  ObCdcLSFetchLogResp &resp = result_;
  cdc::ObCdcService *cdc_service = nullptr;

  if (OB_FAIL(__get_cdc_service(rpc_pkt_->get_tenant_id(), cdc_service))) {
    EXTLOG_LOG(ERROR, "__get_cdc_service failed", KR(ret));
  } else if (OB_ISNULL(cdc_service)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "cdc_service is null", KR(ret));
  } else {
    set_result_compress_type(req.get_compressor_type());
    ret = cdc_service->fetch_missing_log(req, resp, get_send_timestamp(), get_receive_timestamp());
  }

  // rewrite ret for rpc framework
  return OB_SUCCESS;
}

} // namespace obrpc
} // namespace oceanbase
