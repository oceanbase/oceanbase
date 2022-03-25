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

#include <gtest/gtest.h>
#include "common/ob_queue_thread.h"
#include "ob_log_fetcher_rpc_interface.h"
#include "clog/ob_log_entry.h"

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
                                     ObLogReqStartLogIdByTsResponse &resp)
  {
    UNUSED(req);
    UNUSED(resp);
    return OB_SUCCESS;
  }
  virtual int req_start_log_id_by_ts_2(
      const ObLogReqStartLogIdByTsRequestWithBreakpoint &req,
      ObLogReqStartLogIdByTsResponseWithBreakpoint &resp)
  {
    UNUSED(req);
    UNUSED(resp);
    return OB_SUCCESS;
  }

  virtual int req_start_pos_by_log_id(
      const ObLogReqStartPosByLogIdRequest &req,
      ObLogReqStartPosByLogIdResponse &resp)
  {
    UNUSED(req);
    UNUSED(resp);
    return OB_SUCCESS;
  }
  virtual int req_start_pos_by_log_id_2(
      const ObLogReqStartPosByLogIdRequestWithBreakpoint& req,
      ObLogReqStartPosByLogIdResponseWithBreakpoint& resp)
  {
    UNUSED(req);
    UNUSED(resp);
    return OB_SUCCESS;
  }
  virtual int fetch_log(const ObLogExternalFetchLogRequest& req,
                        ObLogExternalFetchLogResponse& resp)
  {
    UNUSED(req);
    UNUSED(resp);
    return OB_SUCCESS;
  }
  virtual int req_heartbeat_info(const ObLogReqHeartbeatInfoRequest& req,
                                 ObLogReqHeartbeatInfoResponse& resp)
  {
    UNUSED(req);
    UNUSED(resp);
    return OB_SUCCESS;
  }
  virtual int req_leader_heartbeat(
    const obrpc::ObLogLeaderHeartbeatReq &req,
    obrpc::ObLogLeaderHeartbeatResp &res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_SUCCESS;
  }
  virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
  {
    UNUSED(feedback);
    return OB_SUCCESS;
  }

  virtual int open_stream(const ObLogOpenStreamReq &req,
                          ObLogOpenStreamResp &resp)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).open_stream(req, resp);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc req heartbeat info", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        resp.reset();
        resp.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else { }
      _D_("rpc: open_stream", K(ret), "svr", get_svr(), K(req), K(resp));
    }
    return ret;
  }

  virtual int fetch_stream_log(const ObLogStreamFetchLogReq &req,
                               ObLogStreamFetchLogResp &resp)
  {
    int ret = OB_SUCCESS;
    ObLogExternalProxy proxy;
    if (OB_SUCCESS != (ret = net_client_.get_proxy(proxy))) {
      _E_("err get proxy", K(ret));
    } else {
      ret = proxy.to(svr_).by(tenant_id_).timeout(timeout_).stream_fetch_log(req, resp);
      int err = proxy.get_result_code().rcode_;
      if (_FAIL_(ret) && _FAIL_(err)) {
        _W_("err rpc req heartbeat info", K(ret), "result_code", err,
            "svr", get_svr(), K(req));
        resp.reset();
        resp.set_err(OB_ERR_SYS);
        ret = OB_SUCCESS;
      }
      else { }
      _D_("rpc: stream_fetch_log", K(ret), "svr", get_svr(), K(req), K(resp));
    }
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
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;
using namespace oceanbase::liboblog;
using namespace oceanbase::unittest;
using namespace oceanbase::clog;

ObAddr get_svr_addr()
{
  ObAddr svr;
  int32_t port = 27800;
  svr.set_ip_addr("100.81.140.76", port);
  // int32_t port = 27800;
  // svr.set_ip_addr("10.210.170.16", port);
  return svr;
}

int64_t get_timeout()
{
  return 60L * 1000 * 1000;
}

//#define PKEY_COUNT 1
#define PKEY_COUNT 2
ObPartitionKey pks[PKEY_COUNT];
ObCond table_ready;
int64_t trans_log_count_recved[PKEY_COUNT];
uint64_t start_log_id[PKEY_COUNT];

#define INSERT_COUNT 9
#define LIFE_TIME (1000 * 1000 * 60)

void init_env()
{
  const int64_t table_id = 1101710651081591;
  // const int64_t table_id = 1101710651081589;
  for (int i = 0; i < PKEY_COUNT; i++) {
    pks[i].init(table_id + i, 0, 1);
    trans_log_count_recved[i] = 0;
    start_log_id[i] = 1;
  }
}

void report_log_recved()
{
  for (int i = 0; i < PKEY_COUNT; i++) {
    fprintf(stdout, "pkey.table_id = %ld, trans_log_num = %ld, next_log_id = %ld\n", static_cast<int64_t>(pks[i].table_id_), trans_log_count_recved[i], start_log_id[i]);
  }
}

void recv_log(ObLogStreamFetchLogResp &fetch_resp)
{
  int ret = OB_SUCCESS;
  const int64_t log_num = fetch_resp.get_log_num();
  const char *buf = fetch_resp.get_log_entry_buf();
  ObLogEntry entry;
  int64_t pos = 0;
  int p = 0;
  for (int64_t idx = 0; idx < log_num; ++idx) {
    ret = entry.deserialize(buf, OB_MAX_LOG_BUFFER_SIZE, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    const ObLogEntryHeader &header = entry.get_header();
    _I_("recv clog_entry", K(ret), K(entry));
    for (p = 0; p < PKEY_COUNT && pks[p] != header.get_partition_key(); p++);
    ASSERT_TRUE(p < PKEY_COUNT);
    if (OB_LOG_SUBMIT == header.get_log_type()) {
      trans_log_count_recved[p]++;
      _I_("trans_log_count_recved", K(p), "pkey", pks[p], "trans_cnt", trans_log_count_recved[p]);
    }
    ASSERT_TRUE(header.get_log_id() == start_log_id[p]);
    start_log_id[p]++;
  }
}

bool recv_all()
{
  int i = 0;
  for (i = 0; (trans_log_count_recved[i] == INSERT_COUNT) && i < PKEY_COUNT; i++);
  // return i == PKEY_COUNT;
  return false;
}

void start_fetch()
{
  int ret = OB_SUCCESS;
  ObNetClient net_client;
  ASSERT_EQ(OB_SUCCESS, net_client.init());
  MockFectherInterface rpc(net_client);
  rpc.set_svr(get_svr_addr());
  rpc.set_timeout(get_timeout());

  int64_t c1 = 0;
  int64_t c2 = 0;
  int err = OB_SUCCESS;
  while (!recv_all()) {
    c1++;
    ObLogOpenStreamReq open_req;
    ObLogOpenStreamResp open_resp;
    for (int i = 0; OB_SUCC(ret) && i < PKEY_COUNT; i++) {
      ObLogOpenStreamReq::Param param;
      param.pkey_ = pks[i];
      param.start_log_id_ = start_log_id[i];
      ASSERT_EQ(OB_SUCCESS, open_req.append_param(param));
    }
    open_req.set_stream_lifetime(LIFE_TIME);
    ret = rpc.open_stream(open_req, open_resp);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(open_resp.get_stream_seq().is_valid());

    _I_("open_stream success", K(open_resp));

    const ObStreamSeq &seq = open_resp.get_stream_seq();
    const int64_t upper_lmt_ts = 100000000000000000L; // large enough
    const int64_t step = 100;
    c2 = 0;
    while (!recv_all()) {
      c2++;
      ObLogStreamFetchLogReq fetch_req;
      ObLogStreamFetchLogResp fetch_resp;
      ASSERT_EQ(OB_SUCCESS, fetch_req.set_stream_seq(seq));
      ASSERT_EQ(OB_SUCCESS, fetch_req.set_upper_limit_ts(upper_lmt_ts));
      ASSERT_EQ(OB_SUCCESS, fetch_req.set_log_cnt_per_part_per_round(step));

      ret = rpc.fetch_stream_log(fetch_req, fetch_resp);
      ASSERT_EQ(OB_SUCCESS, ret);
      err = fetch_resp.get_err();
      if (OB_SUCCESS == err) {
        recv_log(fetch_resp);
      } else if (OB_STREAM_NOT_EXIST == err) {
        fprintf(stdout, "stream not exist\n");
        break;
      } else {
        fprintf(stdout, "error ret=%d\n", err);
        ASSERT_TRUE(false);
      }
      _I_("fetch", K(c1), K(c2));
      if (true && REACH_TIME_INTERVAL(1000 * 1000)) {
        fprintf(stdout, "--------------------------------------------------\n");
        fprintf(stdout, "fetch, c1 = %ld, c2 = %ld\n", c1, c2);
        report_log_recved();
      }
      usleep(1000 * 1000);
    }
  }
}

void del_stale()
{
  int ret = OB_SUCCESS;
  ObNetClient net_client;
  ASSERT_EQ(OB_SUCCESS, net_client.init());
  MockFectherInterface rpc(net_client);
  rpc.set_svr(get_svr_addr());
  rpc.set_timeout(get_timeout());

  ObLogOpenStreamReq open_req;
  ObLogOpenStreamResp open_resp;
  for (int i = 0; OB_SUCC(ret) && i < PKEY_COUNT; i++) {
    ObLogOpenStreamReq::Param param;
    param.pkey_ = pks[i];
    param.start_log_id_ = start_log_id[i];
    ASSERT_EQ(OB_SUCCESS, open_req.append_param(param));
  }
  open_req.set_stream_lifetime(LIFE_TIME);
  ret = rpc.open_stream(open_req, open_resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(open_resp.get_stream_seq().is_valid());

  _I_("open_stream success", K(open_resp));

  const ObStreamSeq &first_seq = open_resp.get_stream_seq();

  ObLogOpenStreamReq open_req2;
  ObLogOpenStreamResp open_resp2;
  for (int i = 0; OB_SUCC(ret) && i < PKEY_COUNT; i++) {
    ObLogOpenStreamReq::Param param;
    param.pkey_ = pks[i];
    param.start_log_id_ = start_log_id[i];
    ASSERT_EQ(OB_SUCCESS, open_req2.append_param(param));
  }
  open_req2.set_stale_stream(first_seq);
  open_req2.set_stream_lifetime(LIFE_TIME);
  ret = rpc.open_stream(open_req2, open_resp2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(open_resp2.get_stream_seq().is_valid());

  _I_("open_stream success", K(open_resp2));
}

int main(int argc, char** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  system("rm els.log");
  OB_LOGGER.set_file_name("els.log", true);
  ObLogger::get_logger().set_mod_log_levels("ALL.*:INFO, TLOG.*:DEBUG");
  init_env();
  start_fetch();
  // del_stale();
  return 0;
}
