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

#include <cstdio>
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <unistd.h>
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/file/file_directory_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_meta_info.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/ob_file_system_router.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/oblog/ob_log_module.h"
#include "logservice/palf/log_engine.h"
#define private public
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf/log_sliding_window.h"
#include "logservice/palf/lsn_allocator.h"
#undef private
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_io_task_cb_thread_pool.h"
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_rpc.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/palf_handle_impl_guard.h"
#include "logservice/palf/log_entry_header.h"
#include "logservice/palf/log_entry.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/palf/log_group_entry.h"
#ifdef OB_BUILD_ARBITRATION
#include "close_modules/arbitration/logservice/arbserver/palf_handle_lite.h"
#endif
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "close_modules/shared_log_service/logservice/libpalf/libpalf_handle.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace palf;
static ObSimpleMemLimitGetter getter;

namespace unittest
{
class MockLogCtx : public logservice::AppendCb
{
public:
  explicit MockLogCtx()
  {}
  ~MockLogCtx() {}
  int on_success() override {
    PALF_LOG(INFO, "on_success");
    return OB_SUCCESS;
  }
  // 日志未形成多数派时会调用此函数，调用此函数后对象不再使用
  int on_failure() override {
    PALF_LOG(INFO, "on_failure");
    return OB_SUCCESS;
  }
};

class TestLogService: public TestDataFilePrepare
{
public:
  TestLogService();
  virtual ~TestLogService();
  virtual void SetUp();
  virtual void TearDown();
  int generate_data(char *&buf, int buf_len, int &wanted_size);
  int generate_data(LogWriteBuf &write_buf, char *&buf, int buf_len, int &wanted_size);
protected:
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];
  int64_t  palf_id_;
  PalfEnvImpl palf_env_impl_;
  IPalfHandleImplGuard palf_handle_impl_guard_;
};

TestLogService::TestLogService()
  : TestDataFilePrepare(&getter,
                        "TestLogService"),
      palf_id_(1),
      palf_handle_impl_guard_()
{
}

TestLogService::~TestLogService()
{
}

void TestLogService::SetUp()
{
// 因为rpc不能用，接入选举会core掉
//   int ret = OB_SUCCESS;
//   TestDataFilePrepare::SetUp();
// #ifdef ERRSIM
//   TP_SET_EVENT(EventTable::EN_LOGSERVICE_IO_TIMEOUT, OB_TIMEOUT, 0, 0);
// #endif
//   const ObReplicaType replica_type = common::REPLICA_TYPE_FULL;
//   rpc::frame::ObReqTransport *transport = reinterpret_cast<rpc::frame::ObReqTransport*>(0x123);
//   const char *fake_ip = "127.0.0.1";
//   int32_t fake_port = 2882;
//   ObAddr self(ObAddr::IPV4, fake_ip, fake_port);
//   const int64_t tenant_id = 1;
//   ASSERT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.init());
//   ObILogAllocator *tenant_allocator = NULL;
//   EXPECT_EQ(OB_SUCCESS, OB_FILE_SYSTEM_ROUTER.get_instance().init("dummy"));
//   EXPECT_EQ(OB_SUCCESS, ObTenantMutilAllocatorMgr::get_instance().get_tenant_log_allocator(tenant_id, tenant_allocator));
//   std::snprintf(log_dir_, OB_MAX_FILE_NAME_LENGTH, "%s_%ld", "unittest", ob_gettid());
//   common::FileDirectoryUtils::delete_directory_rec(log_dir_);
//   common::FileDirectoryUtils::create_directory(log_dir_);
//   ASSERT_EQ(OB_SUCCESS, palf_env_impl_.init(log_dir_, self, transport, tenant_allocator));
//   ObMemberList member_list;
//   (void) member_list.add_server(self);
//   palf_env_impl_.set_member_list(member_list);
//   ASSERT_EQ(OB_SUCCESS, palf_env_impl_.start());
//   ASSERT_EQ(OB_SUCCESS, palf_env_impl_.create_palf_handle_impl(palf_id_, palf_handle_impl_guard_));
// 	ASSERT_EQ(OB_SUCCESS, palf_env_impl_.get_palf_handle_impl(palf_id_, palf_handle_impl_guard_));
//   // sleep 1s to wait leader takeover
//   usleep(10 * 1000 * 1000);
}

void TestLogService::TearDown()
{
// #ifdef ERRSIM
//   TP_SET_EVENT(EventTable::EN_LOGSERVICE_IO_TIMEOUT, OB_TIMEOUT, 0, 0);
// #endif
//   PALF_LOG(INFO, "TestLogService has TearDown");
//   PALF_LOG(INFO, "TearDown success");
}

int TestLogService::generate_data(char *&buf, int buf_len, int &wanted_data_size)
{
  int ret = OB_SUCCESS;
  if (buf_len < wanted_data_size) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    constexpr int MAX_SIZE = 2 * 1024 * 1024;
    wanted_data_size = rand() % MAX_SIZE + 1;
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<int> distribution{'a', 'z'};
    int generate_len = wanted_data_size;
    std::string rand_str(generate_len, '\0');
    for(auto& dis: rand_str) {
      dis = distribution(generator);
    }
    memcpy(buf, rand_str.c_str(), wanted_data_size);
    PALF_LOG(INFO, "runlin trace generate_data", K(ret), K(wanted_data_size));
  }
  return ret;
}

int TestLogService::generate_data(LogWriteBuf &write_buf, char *&buf, int buf_len, int &wanted_data_size)
{
  int ret = OB_SUCCESS;
  srand((unsigned)time(NULL));
  constexpr int MAX_SIZE = 2 * 1024 * 1024;
  wanted_data_size = rand() % MAX_SIZE + 1;
  int buf_len1 = rand() % wanted_data_size;
  char *buf1 = buf;
  generate_data(buf1, buf_len, buf_len1);
  write_buf.push_back(buf1, buf_len1);
  int buf_len2 = wanted_data_size - buf_len1;
  char *buf2 = buf + buf_len1;
  generate_data(buf2, buf_len, buf_len2);
  write_buf.push_back(buf2, buf_len2);
  PALF_LOG(INFO, "runlin trace generate_data", K(ret), K(write_buf), K(buf_len1), K(buf_len2));

  return ret;
}

// TEST_F(TestLogService, submit_group_entry_to_local)
// {
//   ReadBufGuard read_guard("TestLogService");
//   ReadBuf &read_buf = read_guard.read_buf_;
//   const int buf_len = 2 * 1024 * 1024;
//   const int max_group_entry_size = buf_len + 4096;
//   char *buf = static_cast<char *>(ob_malloc(buf_len));
//   int real_log_data_size = 0;
//   LSN lsn;
//   lsn.block_id_ = 1;
//   lsn.val_ = 0;
//   int64_t read_size = 0;
//   int64_t log_ts;
//   LogGroupEntryHeader log_group_entry_header;
//   const int64_t LOG_HEADER_SER_SIZE = log_group_entry_header.get_serialize_size();
//   LogEntryHeader log_entry_header;
//   const int64_t LOG_HEADER_SIZE = log_entry_header.get_serialize_size();
//   LogWriteBuf write_buf;

//   const int MAX_COUNT = 200;
//   for (int64_t j = 0; j < MAX_COUNT; j++) {
//     generate_data(buf, buf_len, real_log_data_size);
//     int64_t data_checksum = static_cast<int64_t>(ob_crc64(buf, real_log_data_size));
//     MockLogCtx *log_ctx = new MockLogCtx();
//     log_ctx->__palf_set_id(palf_id_);
// 		int64_t ref_ts_ns = j;
//     EXPECT_EQ(OB_SUCCESS, palf_handle_impl_guard_.get_palf_handle_impl()->submit_log(buf, real_log_data_size, j, log_ctx, lsn, log_ts));
//     usleep(10*1000);
//     int64_t read_total_len = real_log_data_size + LOG_HEADER_SIZE;

//     int ret = OB_SUCCESS;
//     while (OB_SUCCESS != (ret = palf_handle_impl_guard_.get_palf_handle_impl()->read_log(lsn, read_total_len, read_buf, read_size))) {
//       // sleep a while before retry when read log failed
//       PALF_LOG(WARN, "read log failed, sleep 5ms", K(ret), K(j), K(data_checksum), K(lsn));
//       usleep(5*1000);
//     }

//     char *tmp_buf = read_buf.buf_ + LOG_HEADER_SIZE;
//     PALF_LOG(INFO, "read log", K(j), K(lsn), K(read_total_len), K(read_size), K(real_log_data_size), K(LOG_HEADER_SER_SIZE), K(LOG_HEADER_SIZE));
//     EXPECT_EQ(0, strncmp(tmp_buf, buf, real_log_data_size));
//     int cmp_res = strncmp(tmp_buf, buf, real_log_data_size);
//     PALF_LOG(INFO, "read log, compare finished", K(j), K(data_checksum), K(cmp_res), K(lsn));
//     palf::LogEntry log_entry;
//     int64_t pos = 0;
//     //EXPECT_EQ(OB_SUCCESS, log_entry.deserialize(read_buf.buf_, max_group_entry_size, pos));
//     //EXPECT_TRUE(log_entry.check_integrity());
//     if (0 != cmp_res) {
//       // compare failed, print all log content
//       int step = 256;
//       char *print_buf = static_cast<char *>(ob_malloc(step));
//       int len = 0;
//       for (int i = 0; i < real_log_data_size; i+=step) {
//         len = step;
//         if (i + step > real_log_data_size) {
//           len = real_log_data_size - i + 1;
//         }
//         char *p_tmp_buf = tmp_buf + i;
//         memset(print_buf, 0, step);
//         memcpy(print_buf, tmp_buf + i, len);
//         PALF_LOG(INFO, "tmp_buf", K(j), K(lsn), K(real_log_data_size), K(len), K(i), K(print_buf));
//         memcpy(print_buf, buf + i, len);
//         PALF_LOG(INFO, "buf", K(j), K(lsn), K(real_log_data_size), K(i), K(print_buf));
//       }
//     }
//   }
//   // wait on_success execution
//   usleep(5 * 1000 * 1000);
//   LSN start_lsn(1, 0);
//   LSN end_lsn(BLOCK_ID_MASK, BLOCK_OFFSET_MASK);
//   LogGroupEntryIterator log_iterator;
//   EXPECT_EQ(OB_SUCCESS, palf_handle_impl_guard_.get_palf_handle_impl()->alloc_log_group_entry_iterator(start_lsn,end_lsn, log_iterator));
// 	int ret = OB_SUCCESS;
//   for (int i = 0; OB_SUCC(ret); i++) {
// 		LogGroupEntry entry;
// 		LSN lsn;
// 		if (OB_FAIL(log_iterator.next())) {
//       if (OB_ITER_END == ret) {
//         PALF_LOG(INFO, "has iterate end of file", K(ret));
//       } else if (true == log_iterator.check_is_the_last_entry()) {
// 				ret = OB_ITER_END;
// 				PALF_LOG(INFO, "this entry is the last_entry", K(i));
// 			} else {
// 				PALF_LOG(ERROR, "next failed", K(ret), K(i));
// 				ret = OB_INVALID_DATA;
// 			}
// 		} else if(OB_FAIL(log_iterator.get_entry(entry, lsn)) && OB_ITER_END != ret) {
// 			if (true == log_iterator.check_is_the_last_entry()) {
// 				ret = OB_ITER_END;
// 				PALF_LOG(INFO, "this entry is the last_entry", K(i));
// 			} else {
// 				PALF_LOG(ERROR, "gen_entry failed", K(ret), K(i));
// 				ret = OB_INVALID_DATA;
// 			}
// 		} else {
// 		}
//     PALF_LOG(INFO, "runlin trace next_group_entry", K(ret), K(i), K(entry), K(lsn), K(log_iterator));
//   }
//   //EXPECT_EQ(OB_ITER_END, ret);
//   palf_handle_impl_guard_.get_palf_handle_impl()->free_log_group_entry_iterator();
// 	sleep(2);

//   palf_handle_impl_guard_.get_palf_handle_impl()->free_log_entry_iterator();
// }

TEST(TestPalfHandleImpl, get_end_log_id_no_submit)
{
  PalfHandleImpl impl;
  share::SCN scn = share::SCN::base_scn();
  const int64_t base_log_id = FIRST_VALID_LOG_ID - 1;
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.init(base_log_id, scn, LSN(0)));

  int64_t end_log_id = OB_INVALID_LOG_ID;
  int64_t max_log_id = OB_INVALID_LOG_ID;
  EXPECT_EQ(OB_SUCCESS, impl.get_end_log_id(end_log_id));
  EXPECT_EQ(OB_SUCCESS, impl.get_max_log_id(max_log_id));
  EXPECT_EQ(OB_INVALID_LOG_ID, end_log_id);
  EXPECT_EQ(base_log_id, max_log_id);
  EXPECT_LT(end_log_id, max_log_id);
}

TEST(TestPalfHandleImpl, get_end_log_id_before_slide)
{
  PalfHandleImpl impl;
  share::SCN scn = share::SCN::base_scn();
  const int64_t base_log_id = FIRST_VALID_LOG_ID - 1;
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.init(base_log_id, scn, LSN(0)));

  const int64_t max_log_id = base_log_id + 5;
  const LSN max_lsn(100);
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.inc_update_last_log_info(max_lsn, max_log_id, scn));
  ATOMIC_STORE(&impl.sw_.last_slide_log_id_, base_log_id);

  int64_t end_log_id = OB_INVALID_LOG_ID;
  int64_t queried_max_log_id = OB_INVALID_LOG_ID;
  EXPECT_EQ(OB_SUCCESS, impl.get_end_log_id(end_log_id));
  EXPECT_EQ(OB_SUCCESS, impl.get_max_log_id(queried_max_log_id));
  EXPECT_EQ(base_log_id, end_log_id);
  EXPECT_EQ(max_log_id, queried_max_log_id);
  EXPECT_LT(end_log_id, queried_max_log_id);
}

TEST(TestPalfHandleImpl, get_end_log_id_tracks_last_slide)
{
  PalfHandleImpl impl;
  share::SCN scn = share::SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.init(10, scn, LSN(0)));
  ATOMIC_STORE(&impl.sw_.last_slide_log_id_, 10);

  int64_t end_log_id = OB_INVALID_LOG_ID;
  int64_t max_log_id = OB_INVALID_LOG_ID;
  EXPECT_EQ(OB_SUCCESS, impl.get_end_log_id(end_log_id));
  EXPECT_EQ(OB_SUCCESS, impl.get_max_log_id(max_log_id));
  EXPECT_EQ(10, end_log_id);
  EXPECT_EQ(10, max_log_id);
  EXPECT_LE(end_log_id, max_log_id);
}

#ifdef OB_BUILD_ARBITRATION
TEST(TestPalfHandleLite, get_end_log_id_not_supported)
{
  palflite::PalfHandleLite handle;
  int64_t log_id = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, handle.get_end_log_id(log_id));
  EXPECT_EQ(OB_INVALID_LOG_ID, log_id);
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
TEST(TestLibPalfHandle, get_end_log_id_not_supported)
{
  libpalf::LibPalfHandle handle;
  int64_t log_id = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, handle.get_end_log_id(log_id));
  EXPECT_EQ(OB_INVALID_LOG_ID, log_id);
}
#endif
} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  unlink("./test_palf_handle_impl.log");
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE();
  OB_LOGGER.set_file_name("test_palf_handle_impl.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_palf_handle_impl");
  const uint64_t tenant_id = 1001;
  const uint64_t server_tenant_id = OB_SERVER_TENANT_ID;
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(tenant_id, 0)) {
    malloc->create_and_add_tenant_allocator(tenant_id);
  }
  if (NULL == malloc->get_tenant_ctx_allocator(server_tenant_id, 0)) {
    malloc->create_and_add_tenant_allocator(server_tenant_id);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
