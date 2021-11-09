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

#include "ob_log_fetcher_rpc_interface.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace liboblog;
using namespace liboblog::fetcher;
namespace unittest
{

class MockFectherInterface : public IFetcherRpcInterface
{
public:
  MockFectherInterface(ObNetClient &net_client,
                       const uint64_t tenant_id = OB_SYS_TENANT_ID)
    : net_client_(net_client),
      tenant_id_(tenant_id)
  {
    svr_finder_ = NULL;
  }
  void set_svr(const ObAddr &svr)
  {
    svr_ = svr;
  }
  virtual const ObAddr& get_svr() const
  {
    return svr_;
  }
  void set_timeout(const int64_t timeout)
  {
    timeout_ = timeout;
  }
  virtual int req_start_log_id_by_ts(const ObLogReqStartLogIdByTsRequest &req,
                                     ObLogReqStartLogIdByTsResponse &res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_SUCCESS;
  }
  virtual int req_start_log_id_by_ts_2(
      const ObLogReqStartLogIdByTsRequestWithBreakpoint &req,
      ObLogReqStartLogIdByTsResponseWithBreakpoint &res)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).req_start_log_id_by_ts_with_breakpoint(req, res);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc req start log id by ts", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        res.reset();
        res.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else {}
      _D_("rpc: req start log id by ts", K(ret), "svr", get_svr(),
          K(req), K(res));
    }
    return ret;
  }

  virtual int req_start_pos_by_log_id(
      const ObLogReqStartPosByLogIdRequest &req,
      ObLogReqStartPosByLogIdResponse &res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_SUCCESS;
  }
  virtual int req_start_pos_by_log_id_2(
      const ObLogReqStartPosByLogIdRequestWithBreakpoint& req,
      ObLogReqStartPosByLogIdResponseWithBreakpoint& res)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).req_start_pos_by_log_id_with_breakpoint(req, res);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc req start pos by log id", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        res.reset();
        res.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else { }
      _D_("rpc: req start pos by log id", K(ret), "svr", get_svr(),
          K(req), K(res));
    }
    return ret;
  }

  virtual int fetch_log(const ObLogExternalFetchLogRequest& req,
                        ObLogExternalFetchLogResponse& res)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).fetch_log(req, res);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc fetch log", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        res.reset();
        res.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else { }
      _D_("rpc: fetch log", K(ret), "svr", get_svr(), K(req), K(res));
    }
    return ret;
  }

  virtual int req_heartbeat_info(const ObLogReqHeartbeatInfoRequest& req,
                                 ObLogReqHeartbeatInfoResponse& res)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).req_heartbeat_info(req, res);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc req heartbeat info", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        res.reset();
        res.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else { }
      _D_("rpc: req heartbeat info", K(ret), "svr", get_svr(), K(req), K(res));
    }
    return ret;
  }

  virtual int req_leader_heartbeat(
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).leader_heartbeat(req, res);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc req heartbeat info", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        res.reset();
        res.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else { }
      _D_("rpc: req heartbeat info", K(ret), "svr", get_svr(), K(req), K(res));
    }
    return ret;
  }

  virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
  {
    // This interface is deprecated.
    UNUSED(feedback);
    return common::OB_NOT_IMPLEMENT;
  }

  virtual int open_stream(const ObLogOpenStreamReq &req,
                          ObLogOpenStreamResp &resp)
  {
    int ret = OB_SUCCESS;
    UNUSED(req);
    UNUSED(resp);
    return ret;
  }

  virtual int fetch_stream_log(const ObLogStreamFetchLogReq &req,
                               ObLogStreamFetchLogResp &resp)
  {
    int ret = OB_SUCCESS;
    UNUSED(req);
    UNUSED(resp);
    return ret;
  }
private:
  ObNetClient &net_client_;
  SvrFinder *svr_finder_;
  ObAddr svr_;
  uint64_t tenant_id_;
  int64_t timeout_;
};
}
}

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::liboblog;
using namespace oceanbase::unittest;

ObAddr get_svr_addr()
{
  ObAddr svr;
  int32_t port = 59700;
  svr.set_ip_addr("100.81.152.31", port);
  return svr;
}

#define MILLI_SECOND 1000
#define SECOND (1000 * 1000)

int64_t get_timeout()
{
  return 1000 * SECOND;
}

#define N 3
const ObPartitionKey pk1(1099511677785, 0, 1);
const ObPartitionKey pk3(1099511677784, 0, 1);
const ObPartitionKey pk2(1099511677783, 0, 1);
ObPartitionKey pks[N] = { pk1, pk2, pk3 };

typedef ObLogReqStartLogIdByTsRequestWithBreakpoint TsReq;
typedef ObLogReqStartLogIdByTsRequestWithBreakpoint::Param TsReqParam;
typedef ObLogReqStartLogIdByTsRequestWithBreakpoint::ParamArray TsReqParamArray;
typedef ObLogReqStartLogIdByTsResponseWithBreakpoint TsResp;
typedef ObLogReqStartLogIdByTsResponseWithBreakpoint::Result TsRespResult;
typedef ObLogReqStartLogIdByTsResponseWithBreakpoint::ResultArray TsRespResultArray;

typedef ObLogReqStartPosByLogIdRequestWithBreakpoint IdReq;
typedef ObLogReqStartPosByLogIdRequestWithBreakpoint::Param IdReqParam;
typedef ObLogReqStartPosByLogIdRequestWithBreakpoint::ParamArray IdReqParamArray;
typedef ObLogReqStartPosByLogIdResponseWithBreakpoint IdResp;
typedef ObLogReqStartPosByLogIdResponseWithBreakpoint::Result IdRespResult;
typedef ObLogReqStartPosByLogIdResponseWithBreakpoint::ResultArray IdRespResultArray;

void test_ts_break(const int64_t start_ts, TsResp &resp)
{
  int ret = OB_SUCCESS;
  ObNetClient net_client;
  if (OB_FAIL(net_client.init())) {
    _E_("net client init error", K(ret));
  } else {
    MockFectherInterface rpc(net_client);
    rpc.set_svr(get_svr_addr());
    rpc.set_timeout(get_timeout());
    TsReq req;
    for (int i = 0; OB_SUCC(ret) && i < N; i++) {
      TsReqParam param;
      param.pkey_ = pks[i];
      param.start_tstamp_ = start_ts;
      if (OB_FAIL(req.append_param(param))) {
        _W_("push param error", K(ret));
      }
    }
    ret = rpc.req_start_log_id_by_ts_2(req, resp);
    _I_("----------------------------------------");
    _I_("start_ts:", K(start_ts));
    _I_("req_start_log_id_by_ts finish", K(ret), K(req), K(resp));
    _I_("----------------------------------------");
  }
}

void test_id_break(uint64_t start_log_ids[N], IdResp &resp)
{
  int ret = OB_SUCCESS;
  ObNetClient net_client;
  if (OB_FAIL(net_client.init())) {
    _E_("net client init error", K(ret));
  } else {
    MockFectherInterface rpc(net_client);
    rpc.set_svr(get_svr_addr());
    rpc.set_timeout(get_timeout());
    IdReq req;
    for (int i = 0; OB_SUCC(ret) && i < N; i++) {
      IdReqParam param;
      param.pkey_ = pks[i];
      param.start_log_id_ = start_log_ids[i];
      if (OB_FAIL(req.append_param(param))) {
        _W_("push param error", K(ret));
      }
    }
    ret = rpc.req_start_pos_by_log_id_2(req, resp);
    _I_("----------------------------------------");
    _I_("start_log_id", K(start_log_ids[0]), K(start_log_ids[1]), K(start_log_ids[2]));
    _I_("req_start_pos_by_log_id finish", K(ret), K(req), K(resp));
    _I_("----------------------------------------");
  }
}

void ts_case_1()
{
  // normal test
  int64_t start_ts = 1460969850000000;
  TsResp resp;
  test_ts_break(start_ts, resp);
}

void ts_case_2()
{
  // large enough, test handle_cold_pkeys, get predict value
  int64_t start_ts = 1500000000000000;
  TsResp resp;
  test_ts_break(start_ts, resp);
}

void ts_case_3()
{
  // large enough, test handle cold by last info
  int64_t start_ts = 1460970107619884 + 1;
  TsResp resp;
  test_ts_break(start_ts, resp);
}

void ts_case_4()
{
  // small enough, test after_scan
  int64_t start_ts = 1400000000080000;
  TsResp resp;
  test_ts_break(start_ts, resp);
}

void ts_case_5()
{
  // test break
  int ret = OB_SUCCESS;
  int64_t start_ts = 1400000000080000;
  ObNetClient net_client;
  if (OB_FAIL(net_client.init())) {
    _E_("net client init error", K(ret));
  } else {
    MockFectherInterface rpc(net_client);
    rpc.set_svr(get_svr_addr());
    rpc.set_timeout(get_timeout());

    _I_("++++++++++++++++++++++++++++++++++++++++");
    TsReq req;
    TsResp resp;
    bool stop = false;
    for (int i = 0; OB_SUCC(ret) && i < N; i++) {
      TsReqParam param;
      param.pkey_ = pks[i];
      param.start_tstamp_ = start_ts;
      if (OB_FAIL(req.append_param(param))) {
        _W_("push param error", K(ret));
      }
    }

    while (!stop) {
      stop = true;
      ret = rpc.req_start_log_id_by_ts_2(req, resp);
      _I_("----------------------------------------");
      _I_("start_ts:", K(start_ts));
      _I_("req_start_log_id_by_ts_with_breakpoint finish", K(ret), K(req), K(resp));
      _I_("----------------------------------------");

      const TsRespResultArray &res_arr = resp.get_results();
      TsReqParamArray param_arr = req.get_params();
      int64_t i = 0;
      int64_t res_count = res_arr.count();
      req.reset();
      for (i = 0; OB_SUCC(ret) && i < res_count; i++) {
        const TsRespResult &res = res_arr[i];
        if (OB_EXT_HANDLE_UNFINISH == res.err_) {
          TsReqParam param;
          param.pkey_ = param_arr[i].pkey_;
          param.start_tstamp_ = start_ts;
          param.break_info_.break_file_id_ = res.break_info_.break_file_id_;
          param.break_info_.min_greater_log_id_ = res.break_info_.min_greater_log_id_;
          ret = req.append_param(param);
          stop = false;
        } else {
          // finished pkey
        }
      }
      resp.reset();
      if (OB_FAIL(ret)) {
        _W_("re-send rpc error", K(ret));
      }
    }
    _I_("++++++++++++++++++++++++++++++++++++++++");
  }
}

//----
void id_case_1()
{
  // large enough, test handle_cold_pkeys_by_sw
  uint64_t start_log_ids[N] = {1000, 1000, 1000};
  IdResp resp;
  test_id_break(start_log_ids, resp);
}

void id_case_2()
{
  // min_log_id in last_info_block, test handle_cold_pkeys_by_last_info_block
  uint64_t start_log_ids[N] = {251, 251, 251};
  IdResp resp;
  test_id_break(start_log_ids, resp);
}

void id_case_3()
{
  // normal case
  uint64_t start_log_ids[N] = {230, 230, 230};
  IdResp resp;
  test_id_break(start_log_ids, resp);
}

void id_case_4()
{
  // test break
  int ret = OB_SUCCESS;
  uint64_t start_log_ids[N] = {1, 1, 1};
  ObNetClient net_client;
  if (OB_FAIL(net_client.init())) {
    _E_("net client init error", K(ret));
  } else {
    MockFectherInterface rpc(net_client);
    rpc.set_svr(get_svr_addr());
    rpc.set_timeout(get_timeout());

    _I_("++++++++++++++++++++++++++++++++++++++++");
    IdReq req;
    IdResp resp;
    bool stop = false;
    for (int i = 0; OB_SUCC(ret) && i < N; i++) {
      IdReqParam param;
      param.pkey_ = pks[i];
      param.start_log_id_ = start_log_ids[i];
      if (OB_FAIL(req.append_param(param))) {
        _W_("push param error", K(ret));
      }
    }

    while (!stop) {
      stop = true;
      ret = rpc.req_start_pos_by_log_id_2(req, resp);
      _I_("----------------------------------------");
      _I_("req_start_pos_by_log_id_with_breakpoint finish", K(ret), K(req), K(resp));
      _I_("----------------------------------------");

      const IdRespResultArray &res_arr = resp.get_results();
      IdReqParamArray param_arr = req.get_params();
      int64_t i = 0;
      int64_t res_count = res_arr.count();
      req.reset();
      for (i = 0; OB_SUCC(ret) && i < res_count; i++) {
        const IdRespResult &res = res_arr[i];
        if (OB_EXT_HANDLE_UNFINISH == res.err_) {
          IdReqParam param;
          param.pkey_ = param_arr[i].pkey_;
          param.start_log_id_ = start_log_ids[i];
          param.break_info_.break_file_id_ = res.break_info_.break_file_id_;
          param.break_info_.min_greater_log_id_ = res.break_info_.min_greater_log_id_;
          ret = req.append_param(param);
          stop = false;
        } else {
          // finished pkey
        }
      }
      resp.reset();
      if (OB_FAIL(ret)) {
        _W_("re-send rpc error", K(ret));
      }
    }
    _I_("++++++++++++++++++++++++++++++++++++++++");
  }
}

void ts_test()
{
  ts_case_1();
  ts_case_2();
  ts_case_3();
  ts_case_4();
  ts_case_5();
}

void id_test()
{
  id_case_1();
  id_case_2();
  id_case_3();
  id_case_4();
}

void test_id_cold()
{
  int ret = OB_SUCCESS;
  ObNetClient net_client;
  if (OB_FAIL(net_client.init())) {
    _E_("net client init error", K(ret));
  } else {
    MockFectherInterface rpc(net_client);
    rpc.set_svr(get_svr_addr());
    rpc.set_timeout(get_timeout());

    IdReq req;
    IdResp resp;

    ObPartitionKey pkey(1099511677782, 0, 1);
    IdReqParam param;
    param.pkey_ = pkey;
    param.start_log_id_ = 5;
    if (OB_FAIL(req.append_param(param))) {
      _W_("push param error", K(ret));
    }
    ret = rpc.req_start_pos_by_log_id_2(req, resp);
    _I_("----------------------------------------");
    _I_("req_start_pos_by_log_id finish", K(ret), K(req), K(resp));
    _I_("----------------------------------------");
  }
}

int main(int argc, char** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  ObLogger::get_logger().set_mod_log_levels("ALL.*:INFO, TLOG.*:INFO");

  test_id_cold();

  return 0;
}
