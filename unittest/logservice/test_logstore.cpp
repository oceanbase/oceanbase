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


#include "logservice/palf/log_define.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_entry.h"
#include "logservice/palf/log_meta_entry.h"
#include "logservice/palf/log_meta.h"
#include "logservice/palf/log_io_utils.h"
#define private public
#include "logservice/ob_log_device.h"
#undef private
#include "share/scn.h"
#include "grpc/newlogstorepb.grpc.pb.h"
#include "grpc/newlogstorepb_mock.grpc.pb.h"
#include "grpc/ob_grpc_context.h"
#include <gtest/gtest.h>
#include <thread>

#define TEST_LOGSTORE

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
using namespace newlogstorepb;
using namespace logservice;

int test_case = 1;
namespace obgrpc
{
int ObGrpcContext::translate_error(const Status &status, const int64_t start_ts, const char *func_name)
{
  int ret = OB_SUCCESS;
  return ret;
}
}

namespace logservice
{
// int ObLogDevice::reload_epoch_()
// {
//   int ret = OB_SUCCESS;
//   switch (test_case) {
//     case 1:
//       epoch_ = 2;
//       break;
//     case 2:
//       epoch_ = 2;
//       version_ = 2;
//       break;
//     default:
//       break;
//   }
//   return ret;
// }
int ObLogGrpcAdapter::open(const OpenReq &req, OpenResp &resp)
{
  int ret = OB_SUCCESS;
  switch (test_case) {
    case 1:
      resp.set_ret_code(OB_SUCCESS);
      resp.set_err_no(0);
      resp.set_fd(2);
      break;
    case 2:
      if (req.epoch() == 2) {
        resp.set_ret_code(OB_SUCCESS);
      } else if (req.epoch() == 1) {
        resp.set_ret_code(OB_STATE_NOT_MATCH);
      }
    default:
      break;
  }
  return ret;
}

int ObLogGrpcAdapter::close(const CloseReq &req, CloseResp &resp)
{
  int ret = OB_SUCCESS;
  switch (test_case) {
    case 1:
      resp.set_ret_code(OB_SUCCESS);
      resp.set_err_no(0);
      break;
    case 2:
      if (req.epoch() == 1) {
        resp.set_ret_code(OB_STATE_NOT_MATCH);
      } else {
        resp.set_ret_code(OB_SUCCESS);
        resp.set_err_no(0);
      }
      break;
    default:
      break;
  }
  return ret;
}

int ObLogGrpcAdapter::load_log_store(const LoadLogStoreReq &req, LoadLogStoreResp &resp)
{
  int ret = OB_SUCCESS;
  switch (test_case) {
    case 1:
      resp.set_ret_code(OB_SUCCESS);
      resp.set_version(1);
      resp.set_epoch(1);
      break;
    case 2:
      resp.set_ret_code(OB_SUCCESS);
      resp.set_version(2);
      resp.set_epoch(2);
    default:
      break;
  }
  return ret;
}

int ObLogGrpcAdapter::pread(const PreadReq &req, PreadResp &resp)
{
  int ret = OB_SUCCESS;
  switch (test_case) {
    case 1:
      resp.set_size(1024);
      break;
    case 2:
      resp.set_ret_code(OB_STATE_NOT_MATCH);
    default:
      break;
  }
  return ret;
}

int ObLogGrpcAdapter::rmdir(const RmdirReq &req, RmdirResp &resp)
{
  int ret = OB_SUCCESS;
  if (req.epoch() == 1) {
    resp.set_ret_code(OB_STATE_NOT_MATCH);
  } else if (req.epoch() == 2) {
    resp.set_ret_code(OB_SUCCESS);
  }
  return ret;
}
}

namespace unittest
{
static int64_t test_rpc_count = 1;

// class FakeClient {
// public:
//   explicit FakeClient(NewLogStore::StubInterface *stub) : stub_(stub) {}
//   void DoOpen() {
//     ClientContext context;
//     OpenRequest req;
//     OpenResponse resp;
//     // req.set
//     Status s = stub_->Open(&context, req, &resp);
//   }
//   void ResetStub(NewLogStore::StubInterface* stub) { stub_ = stub; }

// private:
//   NewLogStore::StubInterface* stub_;
// };

class TestLogstore : public ::testing::Test
{
public:
  TestLogstore() {}
  virtual ~TestLogstore() {}
public:
  virtual void SetUp();
  virtual void TearDown();
public:
  ObLogDevice log_device_;
};

const int64_t RPC_TIMEOUT = 2 * 1000 * 1000L;

void TestLogstore::SetUp()
{
  log_device_.is_inited_ = true;
  log_device_.is_running_ = true;
  log_device_.epoch_ = ObLogDevice::LOG_COMPATIBLE_VERSION;
  oceanbase::common::ObAddr logstore_server_addr;
  logstore_server_addr.set_ip_addr("127.0.0.1", 50051);
  log_device_.grpc_adapter_.is_inited_ = true;
  log_device_.grpc_adapter_.grpc_client_.init(logstore_server_addr, RPC_TIMEOUT, 1, 500);
  PALF_LOG(INFO, "SetUp success");
}

void TestLogstore::TearDown()
{
  log_device_.is_inited_ = false;
  log_device_.is_running_ = false;
  log_device_.grpc_adapter_.is_inited_ = false;
  log_device_.epoch_ = ObLogDevice::LOG_COMPATIBLE_VERSION;
  PALF_LOG(INFO, "TestLogStore TearDown");
}

TEST_F(TestLogstore, test_load_log_store)
{
  log_device_.is_running_ = false;
  common::ObIODOpts opts;
  opts.opts_ = new ObIODOpt();
  EXPECT_EQ(OB_SUCCESS, log_device_.start(opts));
  EXPECT_EQ(1, log_device_.epoch_);
  EXPECT_EQ(1, log_device_.version_);
}

TEST_F(TestLogstore, test_logstore_open)
{
  const char *pathname = "/test_path";
  int flags = O_RDWR | O_CREAT;
  mode_t mode = FILE_OPEN_MODE;
  ObIOFd fd;

  //temporaily mock resp before starting rust-logstore process
  test_case = 1;
  EXPECT_EQ(OB_SUCCESS, log_device_.open(pathname, flags, mode, fd));
  EXPECT_EQ(OB_SUCCESS, log_device_.close(fd));
  test_case = 2;
  // test epoch not match, trigger reload_epoch_
  fd.reset();
  EXPECT_EQ(OB_SUCCESS, log_device_.open(pathname, flags, mode, fd));
  EXPECT_EQ(2, log_device_.epoch_);
  EXPECT_EQ(2, log_device_.version_);
  EXPECT_EQ(OB_SUCCESS, log_device_.close(fd));
}

TEST_F(TestLogstore, test_logstore_pread)
{
  const char *pathname = "/test_path";
  int flags = O_RDWR | O_CREAT;
  mode_t mode = FILE_OPEN_MODE;
  ObIOFd fd;

  const int64_t offset = 1024;
  const int64_t size = 1024;
  char *buf = reinterpret_cast<char*>(ob_malloc(1024, "unittest"));
  int64_t read_size = 0;

  test_case = 1;
  EXPECT_EQ(OB_SUCCESS, log_device_.open(pathname, flags, mode ,fd));
  EXPECT_EQ(OB_SUCCESS, log_device_.pread(fd, offset, size, buf, read_size));
  EXPECT_EQ(size, read_size);

  // test epoch changed
  test_case = 2;
  read_size = 0;
  const int64_t expected_read_size = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_device_.pread(fd, offset, size, buf, read_size));
  EXPECT_EQ(expected_read_size, read_size);
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_device_.close(fd));
  ob_free(buf);
}

TEST_F(TestLogstore, test_concurrent_reload)
{
  test_case = 1;
  ObIOFd fd;
  {
    std::thread thread_1([this, &fd] {
      const char *pathname = "/test_path";
      int flags = O_RDWR | O_CREAT;
      mode_t mode = FILE_OPEN_MODE;
      EXPECT_EQ(OB_SUCCESS, this->log_device_.open(pathname, flags, mode, fd));
      sleep(5);
      EXPECT_EQ(OB_STATE_NOT_MATCH, this->log_device_.close(fd));
    });

    std::thread thread_2([this] {
      const char *pathname = "/test_path";
      sleep(2);
      test_case = 2;
      EXPECT_EQ(OB_SUCCESS, this->log_device_.rmdir(pathname)); // should stuck for a while and succeed after close
      EXPECT_EQ(2, this->log_device_.epoch_);
      EXPECT_EQ(2, this->log_device_.version_);
    });

    std::thread thread_3([this] {
      const char *pathname = "/test_path";
      sleep(3);
      EXPECT_EQ(OB_SUCCESS, this->log_device_.rmdir(pathname)); // should stuck for a while
    });
    thread_1.join();
    thread_2.join();
    thread_3.join();
  }

}

}   // unittest end
}   // oceanabse end

int main(int argc, char **argv)
{
  system("rm -f ./test_logstore.log*");
  OB_LOGGER.set_file_name("test_logstore.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_logstore");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
