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

#define USING_LOG_PREFIX EXTLOG
#include "ob_cdc_start_lsn_locator.h"
#include "ob_cdc_util.h"
#include "logservice/ob_log_service.h"          // ObLogService

namespace oceanbase
{
namespace cdc
{
ObCdcStartLsnLocator::ObCdcStartLsnLocator()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObCdcStartLsnLocator::~ObCdcStartLsnLocator()
{
  destroy();
}

int ObCdcStartLsnLocator::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
  }

  return ret;
}

void ObCdcStartLsnLocator::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
  }
}

int ObCdcStartLsnLocator::req_start_lsn_by_ts_ns(const ObLocateLSNByTsReq &req_msg,
    ObLocateLSNByTsResp &result,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcStartLsnLocator not init", KR(ret), K(req_msg));
  } else if (ATOMIC_LOAD(&stop_flag)) {
    ret = OB_IN_STOP_STATE;
    EXTLOG_LOG(INFO, "ObCdcService is stopped", K(req_msg));
  } else if (OB_UNLIKELY(! req_msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObLocateLSNByTsReq is not valid", KR(ret), K(req_msg));
  } else {
    if (OB_FAIL(do_req_start_lsn_(req_msg, result, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        EXTLOG_LOG(WARN, "do do_req_start_lsn_ failed", K(ret), K(req_msg), K(result));
      }
    } else {
      EXTLOG_LOG(INFO, "do req_start_log_id success", K(ret), K(req_msg), K(result));
    }
  }

  if (OB_SUCC(ret)) {
    result.set_err(OB_SUCCESS);
  } else {
    if (OB_IN_STOP_STATE == ret) {
      result.set_err(ret);
    } else {
      result.set_err(OB_ERR_SYS);
    }
  }

  return ret;
}

int ObCdcStartLsnLocator::do_req_start_lsn_(const ObLocateLSNByTsReq &req,
    ObLocateLSNByTsResp &resp,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool is_hurry_quit = false;
  const ObLocateLSNByTsReq::LocateParamArray &locate_params = req.get_params();
  ObExtRpcQit qit;

  if (0 == locate_params.count()) {
    EXTLOG_LOG(INFO, "no LS in request", K(req));
  } else if (OB_FAIL(qit.init(get_rpc_deadline_()))) {
    EXTLOG_LOG(WARN, "init qit error", K(ret));
  } else {
    for (int64_t idx = 0; ! stop_flag && OB_SUCC(ret) && idx < locate_params.count(); ++idx) {
      const ObLocateLSNByTsReq::LocateParam &locate_param = locate_params[idx];

      if (OB_UNLIKELY(is_hurry_quit = qit.should_hurry_quit())) {
        if (OB_FAIL(handle_when_hurry_quit_(locate_param, resp))) {
          LOG_WARN("handle_when_hurry_quit_ failed", KR(ret), K(tenant_id_), K(locate_param));
        }
      } else if (OB_FAIL(do_locate_ls_(locate_param, resp))) {
        LOG_WARN("do_locate_ls_ failed", KR(ret), K(tenant_id_), K(locate_param));
      } else {}
    } // for

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    EXTLOG_LOG(INFO, "locator req success", KR(ret), K(tenant_id_), K(req), K(resp), K(is_hurry_quit));
  }

  return ret;
}

int ObCdcStartLsnLocator::handle_when_hurry_quit_(const ObLocateLSNByTsReq::LocateParam &locate_param,
    ObLocateLSNByTsResp &result)
{
  int ret = OB_SUCCESS;
  ObLocateLSNByTsResp::LocateResult locate_res;
  locate_res.reset();

  // If exit halfway, mark every subsequent LS with unfinished error code
  // TODO err code
  locate_res.set_locate_err(OB_EXT_HANDLE_UNFINISH);

  if (OB_FAIL(result.append_result(locate_res))) {
    LOG_WARN("ObLocateLSNByTsResp append_result fail", KR(ret), K(tenant_id_), K(locate_param),
        K(locate_res));
  }

  EXTLOG_LOG(INFO, "LS locate_by_timestamp mark unfinish", KR(ret), K(locate_param), K(locate_res));

  return ret;
}

int ObCdcStartLsnLocator::do_locate_ls_(const ObLocateLSNByTsReq::LocateParam &locate_param,
    ObLocateLSNByTsResp &resp)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
  palf::PalfGroupBufferIterator group_iter;
  LogGroupEntry log_group_entry;
  ObLocateLSNByTsResp::LocateResult locate_res;
  const ObLSID &ls_id = locate_param.ls_id_;
  const int64_t start_ts_ns = locate_param.start_ts_ns_;
  LSN result_lsn;
  int64_t result_ts_ns = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(init_palf_handle_guard_(ls_id, palf_handle_guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret= OB_LS_NOT_EXIST;
    } else {
      LOG_WARN("init_palf_handle_guard_ fail", KR(ret), K(ls_id));
    }
  // - OB_SUCCESS: seek success
  // - OB_ENTRY_NOT_EXIST: there is no log's log_ts is higher than ts_ns
  // - OB_ERR_OUT_OF_LOWER_BOUND: ts_ns is too old, log files may have been recycled
  } else if (OB_FAIL(palf_handle_guard.seek(start_ts_ns, group_iter))) {
    LOG_WARN("PalfHandle seek fail", KR(ret), K(tenant_id_), K(locate_param));
  } else if (OB_FAIL(group_iter.next())) {
    LOG_WARN("PalfGroupBufferIterator next failed, unexpected", KR(ret), K_(tenant_id), K(ls_id));
  } else if (OB_FAIL(group_iter.get_entry(log_group_entry, result_lsn))) {
    LOG_WARN("group_iter get_entry fail", KR(ret), K_(tenant_id), K(ls_id));
  } else {
    result_ts_ns = log_group_entry.get_log_ts();
  }

  // Unconditional setting ret code
  locate_res.reset(ret, result_lsn, result_ts_ns);
  // Reset ret
  ret = OB_SUCCESS;

  if (OB_FAIL(resp.append_result(locate_res))) {
    LOG_WARN("ObLocateLSNByTsResp append_result fail", KR(ret), K(tenant_id_), K(locate_param), K(locate_res));
  }

  return ret;
}

int ObCdcStartLsnLocator::init_palf_handle_guard_(const ObLSID &ls_id,
    palf::PalfHandleGuard &palf_handle_guard)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);

  if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("ObLogService open_palf fail", KR(ret), K(ls_id));
    }
  }

  return ret;
}

} // namespace cdc
} // namespace oceanbase
