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
#include <random>
#include <string>
#include "lib/ob_define.h"
#include "logservice/palf/i_log_location_cache.h"
#include "logservice/palf/i_log_role_change_cb.h"
#include "logservice/palf/i_ls_state.h"
#include "logservice/palf/i_log_replay_engine.h"
#include "logservice/palf/i_log_apply_service.h"
#include "logservice/palf/log_meta_info.h"
#include "logservice/palf/palf_options.h"
#include "share/ls_id.h"
#include "logservice/palf/log_writer_utils.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "lib/oblog/log_print_kv.h"
#include "lib/oblog/log_module.h"
#include "logservice/palf/log_engine.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_io_task_cb_thread_pool.h"
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_rpc.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/palf_handle_impl_guard.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_entry_header.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{
class MockLogCtx : public logservice::AppendCb
{
public:
  explicit MockLogCtx(int j, int i) : j_(j), i_(i)
  {}
  ~MockLogCtx() {}
  int on_success() override {
    PALF_LOG(INFO, "on_success", K(j_), K(i_));
    return OB_SUCCESS;
  }
  // 日志未形成多数派时会调用此函数，调用此函数后对象不再使用
  int on_failure() override {
    PALF_LOG(INFO, "on_failure", K(j_), K(i_));
    return OB_SUCCESS;
  }
  int j_;
  int i_;
};

class MockLogRoleChangeCB : public ILogRoleChangeCB
{
  virtual int on_leader_revoke(const int64_t palf_id) override
  {
    PALF_LOG(INFO, "on_leader_revoke", K(palf_id));
    UNUSED(palf_id);
    return OB_SUCCESS;
  }
  virtual int is_leader_revoke_done(const int64_t palf_id,
                                    bool &is_done) const override
  {
    PALF_LOG(INFO, "is_leader_revoke_done", K(palf_id));
    UNUSED(palf_id);
    is_done = true;
    return OB_SUCCESS;
  }
  virtual int on_leader_takeover(const int64_t palf_id) override
  {
    PALF_LOG(INFO, "on_leader_takeover", K(palf_id));
    UNUSED(palf_id);
    return OB_SUCCESS;
  }
  virtual int is_leader_takeover_done(const int64_t palf_id,
                                      bool &is_done) const override
  {
    PALF_LOG(INFO, "is_leader_takeover_done", K(palf_id));
    UNUSED(palf_id);
    is_done = true;
    return OB_SUCCESS;
  }
  virtual int on_leader_active(const int64_t palf_id) override
  {
    PALF_LOG(INFO, "on_leader_active", K(palf_id));
    UNUSED(palf_id);
    return OB_SUCCESS;
  }
};

class TestLogSubmitLog : public TestDataFilePrepare
{
public:
  TestLogSubmitLog();
  virtual ~TestLogSubmitLog();
  virtual void SetUp();
  virtual void TearDown();
  int generate_data(char *&buf, int buf_len, int wanted_size);
  int generate_data(LogWriteBuf &write_buf, char *&buf, int buf_len, int &wanted_size);
protected:
  char log_dir_[1024];
  int64_t  palf_id_;
  PalfEnvImpl palf_env_impl_;
  MockLogRoleChangeCB role_change_cb_;
  PalfHandleImplGuard palf_handle_impl_guard_;
};

TestLogSubmitLog::TestLogSubmitLog()
    : TestDataFilePrepare("TestLogSubmitLog"),
      log_dir_("unit_01"),
      palf_id_(1),
      role_change_cb_(),
      palf_handle_impl_guard_()
{
}

TestLogSubmitLog::~TestLogSubmitLog()
{
}

void TestLogSubmitLog::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_LOGSERVICE_IO_TIMEOUT, OB_TIMEOUT, 0, 0);
#endif
  const ObReplicaType replica_type = common::REPLICA_TYPE_FULL;
  ILogReplayExecutor *executor=  reinterpret_cast<ILogReplayExecutor*>(0x123);
  rpc::frame::ObReqTransport *transport = reinterpret_cast<rpc::frame::ObReqTransport*>(0x123);
  const char *fake_ip = "127.0.0.1";
  int32_t fake_port = 2882;
  ObAddr self(ObAddr::IPV4, fake_ip, fake_port);
  ObMemberList member_list;
  (void) member_list.add_server(self);
  const int64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.init());
  ObILogAllocator *tenant_allocator = NULL;
  EXPECT_EQ(OB_SUCCESS, OB_FILE_SYSTEM_ROUTER.get_instance().init("dummy", "dummy", 1, "dummy", self));
  EXPECT_EQ(OB_SUCCESS, ObTenantMutilAllocatorMgr::get_instance().get_tenant_log_allocator(tenant_id, tenant_allocator));

  ASSERT_EQ(OB_SUCCESS, palf_env_impl_.init(&role_change_cb_, executor,
        tenant_allocator, transport, log_dir_, self));
  palf_env_impl_.set_member_list(member_list);
  ASSERT_EQ(OB_SUCCESS, palf_env_impl_.start());
  ASSERT_EQ(OB_SUCCESS, palf_env_impl_.create_palf_handle_impl(palf_id_, palf_handle_impl_guard_));
  int64_t leader_epoch = 9;
  // set leader to self
  palf_handle_impl_guard_.get_palf_handle_impl()->set_leader(self, leader_epoch);
  // sleep 1s to wait leader takeover
  usleep(1 * 1000 * 1000);
}

void TestLogSubmitLog::TearDown()
{
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_LOGSERVICE_IO_TIMEOUT, OB_TIMEOUT, 0, 0);
#endif
  PALF_LOG(INFO, "TestLogSubmitLog has TearDown");
  PALF_LOG(INFO, "TearDown success");
  TestDataFilePrepare::TearDown();
}

constexpr int MAX_BUF_SIZE = 2 * 1024 * 1024;

int TestLogSubmitLog::generate_data(char *&buf, int buf_len, int wanted_data_size)
{
  int ret = OB_SUCCESS;
  if (buf_len < wanted_data_size) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<int> distribution{'a', 'z'};
    int generate_len = wanted_data_size;
    std::string rand_str(generate_len, '\0');
    for(auto& dis: rand_str) {
      dis = distribution(generator);
    }
    memcpy(buf, rand_str.c_str(), wanted_data_size);
  }
  return ret;
}

TEST_F(TestLogSubmitLog, test_submit_group_log)
{
  ReadBufGuard read_guard("TestLogSubmitLog", MAX_LOG_BUFFER_SIZE);
  ReadBuf &read_buf = read_guard.read_buf_;
  char *buf = static_cast<char *>(ob_malloc(MAX_BUF_SIZE));
  int real_log_data_size = 0;

  LogGroupEntryHeader entry_header;
  const int64_t LOG_HEADER_SER_SIZE = entry_header.get_serialize_size();
  LogEntryHeader group_header;
  const int64_t LOG_HEADER_SIZE = group_header.get_serialize_size();
  const int64_t LOG_LOG_CNT = 20;
  int64_t last_submit_log_ts = -1;

  PalfAppendOptions opts; opts.need_check_proposal_id = false; opts.need_nonblock = false;
  for (int64_t j = 0; j < 10; j++) {  // 调大循环次数可以测试写多个clog文件场景
    LSN offset_array[LOG_LOG_CNT];
    int64_t log_size_array[LOG_LOG_CNT];
    int64_t data_checksum_array[LOG_LOG_CNT];
    for (int i = 0; i < LOG_LOG_CNT; i++) {
      int64_t log_ts;
			int64_t base_ts = i + j*LOG_LOG_CNT;
      log_size_array[i] = rand() % MAX_BUF_SIZE + 1;
      generate_data(buf, MAX_BUF_SIZE, log_size_array[i]);
      data_checksum_array[i] = static_cast<int64_t>(ob_crc64(buf, log_size_array[i]));
      MockLogCtx *log_ctx = new MockLogCtx(j, i);
      log_ctx->__palf_set_id(palf_id);
      EXPECT_EQ(OB_SUCCESS, palf_handle_impl_guard_.get_palf_handle_impl()->submit_log(opts, buf, log_size_array[i], base_ts,
            log_ctx, offset_array[i], log_ts));
      // check if log_ts backwards
      if (-1 == last_submit_log_ts) {
        // skip
      } else if (log_ts <= last_submit_log_ts) {
        PALF_LOG(ERROR, "log_ts is less than last_submit_log_ts", K(j), K(i), "lsn", offset_array[i],
            K(log_ts), K(last_submit_log_ts));
        abort();
      } else {}
      // update last_submit_log_ts
      last_submit_log_ts = log_ts;
    }
    usleep(1 * 1000);
    for (int i = 0; i < LOG_LOG_CNT; i++) {
      int64_t read_total_len = log_size_array[i] + LOG_HEADER_SIZE;
      int ret = OB_SUCCESS;
      int64_t read_size = 0;
      while (OB_FAIL(palf_handle_impl_guard_.get_palf_handle_impl()->read_log(offset_array[i],
              read_total_len, read_buf, read_size))) {
        // sleep a while before retry when read log failed
        PALF_LOG(WARN, "read log failed, sleep 5ms", K(ret), K(j), "lsn", offset_array[i]);
        usleep(1 * 1000);
      }
      char *tmp_buf = read_buf.buf_ + LOG_HEADER_SIZE;
      PALF_LOG(INFO, "read log", K(j), K(i), "lsn", offset_array[i], K(read_total_len), K(read_size),
          K(LOG_HEADER_SER_SIZE), K(LOG_HEADER_SIZE));
      const int64_t read_data_checksum = static_cast<int64_t>(ob_crc64(tmp_buf, log_size_array[i]));
      EXPECT_EQ(data_checksum_array[i], read_data_checksum);
      bool is_equal = (data_checksum_array[i] == read_data_checksum);
      PALF_LOG(INFO, "read log, compare finished", K(j), K(i), K(is_equal), "lsn", offset_array[i], K(read_data_checksum));
    }
  }
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -fr ./unit_01");
  system("mkdir ./unit_01");
  system("rm -f ./test_log_submit_log.log");
  OB_LOGGER.set_file_name("test_log_submit_log.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_submit_log");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
