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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/net/ob_addr.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_gts_rpc.h"
#include "storage/tx/ob_gts_define.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace obrpc;
namespace unittest
{

class MyResponseRpc : public ObIGtsResponseRpc
{
public:
  MyResponseRpc() {}
  ~MyResponseRpc() {}
public:
  void set_valid_arg(const uint64_t tenant_id,
                     const int status,
                     const ObAddr &sender,
                     const ObAddr self)
  {
    tenant_id_ = tenant_id;
    status_ = status;
    sender_ = sender;
    self_ = self;
  }
  int post(const uint64_t tenant_id, const ObAddr &server, const ObGtsErrResponse &msg)
  {
    int ret = OB_SUCCESS;
    if (!msg.is_valid() ||
        tenant_id != tenant_id_ ||
        msg.get_status() != status_ ||
        msg.get_sender() != sender_ ||
        server != self_) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(server), K(msg), K(*this));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_id), K_(status), K_(sender), K_(self));
private:
  uint64_t tenant_id_;
  int status_;
  ObAddr sender_;
  ObAddr self_;
};

class MyTimestampService : public ObTimestampService
{
public:
  MyTimestampService() {}
  ~MyTimestampService() {}
  void init(const ObAddr &self)
  {
    self_ = self;
    service_type_ = ServiceType::TimestampService;
    pre_allocated_range_ = TIMESTAMP_PREALLOCATED_RANGE;
    last_id_ = 0;
    limited_id_ = ObTimeUtility::current_time_ns() + TIMESTAMP_PREALLOCATED_RANGE;
  }
public:
  int handle_request(const ObGtsRequest &request, ObGtsRpcResult &result)
  {
    static int64_t total_cnt = 0;
    static int64_t total_rt = 0;
    static const int64_t STATISTICS_INTERVAL_US = 10000000;
    const MonotonicTs start = MonotonicTs::current_time();
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!request.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(request));
    } else {
      TRANS_LOG(DEBUG, "handle gts request", K(request));
      int64_t gts = 0;
      const MonotonicTs srr = request.get_srr();
      const uint64_t tenant_id = request.get_tenant_id();
      const ObAddr &requester = request.get_sender();
      int64_t end_id;
      if (requester == self_) {
      // Go local call to get gts
      TRANS_LOG(DEBUG, "handle local gts request", K(requester));
      ret = handle_local_request_(request, result);
      } else if (OB_FAIL(get_number(1, ObTimeUtility::current_time_ns(), gts, end_id))) {
        TRANS_LOG(WARN, "get timestamp failed", KR(ret));
        int tmp_ret = OB_SUCCESS;
        ObGtsErrResponse response;
        if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
          TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
        } else if (OB_SUCCESS != (tmp_ret = response.init(tenant_id, srr, ret, self_))) {
          TRANS_LOG(WARN, "gts err response init failed", K(tmp_ret), K(request));
        } else if (OB_SUCCESS != (tmp_ret = rpc_.post(tenant_id, requester, response))) {
          TRANS_LOG(WARN, "post gts err response failed", K(tmp_ret), K(response));
        } else {
          TRANS_LOG(DEBUG, "post gts err response success", K(response));
        }
      } else {
        if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
          TRANS_LOG(WARN, "gts result init failed", KR(ret), K(request));
        }
      }
    }
    return ret;
  }
  int handle_local_request_(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
  {
    int ret = OB_SUCCESS;
    int64_t gts = 0;
    const uint64_t tenant_id = request.get_tenant_id();
    const MonotonicTs srr = request.get_srr();
    int64_t end_id;
    if (OB_FAIL(get_number(1, ObTimeUtility::current_time_ns(), gts, end_id))) {
      TRANS_LOG(WARN, "get timestamp failed", KR(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
        TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
      }
    } else {
      if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
        TRANS_LOG(WARN, "local gts result init failed", KR(ret), K(request));
      }
    }
    return ret;
  }
private:
  int get_number(const int64_t range, const int64_t base_id, int64_t &start_id, int64_t &end_id)
  {
    int ret = OB_SUCCESS;
    if (range != 1) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(range), K(base_id));
    } else if (base_id < ObTimeUtility::current_time()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(range), K(base_id));
    } else {
      const int64_t last_id = ATOMIC_LOAD(&last_id_);
      int64_t tmp_id = 0;
      if (base_id > last_id) {
        if (ATOMIC_BCAS(&last_id_, last_id, base_id + range)) {
          tmp_id = base_id;
        } else {
          tmp_id = ATOMIC_FAA(&last_id_, range);
        }
      } else {
        tmp_id = ATOMIC_FAA(&last_id_, range);
      }
      start_id = tmp_id;
    }
    return ret;
  }
  MyResponseRpc rpc_;
};

class TestObGtsMgr : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

//////////////////////basic function test//////////////////////////////////////////

TEST_F(TestObGtsMgr, handle_gts_request_by_leader)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ts_service.init(server);

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, client));
  EXPECT_EQ(OB_SUCCESS, ts_service.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(ts_range - 1, result.get_gts_end() - result.get_gts_start());
  //EXPECT_TRUE(result.get_gts_start() >= srr);
}

TEST_F(TestObGtsMgr, handle_local_gts_request)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ts_service.init(server);

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, server);
  ObGtsRequest request;
  ObGtsRpcResult result;
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, server));
  EXPECT_EQ(OB_SUCCESS, ts_service.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(ts_range - 1, result.get_gts_end() - result.get_gts_start());
  // EXPECT_TRUE(result.get_gts_start() >= srr);
}

TEST_F(TestObGtsMgr, invalid_argument)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const MonotonicTs stc;
  const int64_t ts_range = 1;
  ObGtsRequest request;
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(0, srr, ts_range, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, stc, ts_range, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, 0, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, ts_range, ObAddr()));
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_gts_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
