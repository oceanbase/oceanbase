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
#include <cstring>
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <unistd.h>
#define USING_LOG_PREFIX PALF
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_meta_info.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/ob_file_system_router.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/oblog/ob_log_module.h"
#include "logservice/palf/log_io_adapter.h"
#include "logservice/palf/log_io_worker.h"
#include "share/ob_device_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_cluster_version.h"
#define private public
#include "logservice/palf/log_engine.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf/log_sliding_window.h"
#include "logservice/palf/lsn_allocator.h"
#undef private
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_io_task_cb_thread_pool.h"
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_rpc.h"
#define private public
#include "logservice/palf/palf_env_impl.h"
#undef private
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

class FinishInitTestPalfHandle : public PalfHandleImpl
{
public:
  FinishInitTestPalfHandle()
      : scan_finished_count_(0)
  {}
  ~FinishInitTestPalfHandle()
  {}

  int set_scan_disk_log_finished() override
  {
    ++scan_finished_count_;
    return OB_SUCCESS;
  }

  int64_t get_scan_finished_count() const
  {
    return scan_finished_count_;
  }

private:
  int64_t scan_finished_count_;
};

TEST(TestLogStorage, async_recovery_truncate_rejects_non_last_block_and_keeps_manifest)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int64_t update_manifest_count = 0;
  const offset_t restart_tail_offset = MAX_INFO_BLOCK_SIZE + LOG_DIO_ALIGN_SIZE;
  char dirty_buf[LOG_DIO_ALIGN_SIZE];
  char read_buf[LOG_DIO_ALIGN_SIZE];
  char zero_buf[LOG_DIO_ALIGN_SIZE];
  char base_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  LogIODeviceWrapper device_wrapper;
  LogIOAdapter io_adapter;
  LogStorage storage;
  auto update_manifest_cb = [&update_manifest_count](const block_id_t, const bool) {
    ++update_manifest_count;
    return OB_ERR_UNEXPECTED;
  };
  MEMSET(dirty_buf, 'x', sizeof(dirty_buf));
  MEMSET(read_buf, '\0', sizeof(read_buf));
  MEMSET(zero_buf, '\0', sizeof(zero_buf));

  share::ObTenantEnv::set_tenant(&tenant_base);
  ret = ObDeviceManager::get_instance().init_devices_env();
  PALF_LOG(INFO, "init devices env for async recovery truncate test", K(ret));
  ASSERT_TRUE(OB_SUCCESS == ret || OB_INIT_TWICE == ret);
  ret = share::ObResourceManager::get_instance().init();
  PALF_LOG(INFO, "init resource manager for async recovery truncate test", K(ret));
  ASSERT_TRUE(OB_SUCCESS == ret || OB_INIT_TWICE == ret);
  ret = ObIOManager::get_instance().init(1000000000);
  PALF_LOG(INFO, "init io manager for async recovery truncate test", K(ret));
  ASSERT_TRUE(OB_SUCCESS == ret || OB_INIT_TWICE == ret);
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ASSERT_GT(snprintf(base_dir, OB_MAX_FILE_NAME_LENGTH,
                     "async_recovery_truncate_%ld", ob_gettid()), 0);
  FileDirectoryUtils::delete_directory_rec(base_dir);
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory(base_dir));
  ASSERT_GT(snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/log", base_dir), 0);
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory(log_dir));
  ASSERT_GT(snprintf(block_path, OB_MAX_FILE_NAME_LENGTH, "%s/0", log_dir), 0);
  ASSERT_NE(-1, fd = ::open(block_path, O_RDWR | O_CREAT | O_TRUNC, FILE_OPEN_MODE));
  ASSERT_EQ(0, ::ftruncate(fd, PALF_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE));
  ASSERT_EQ(static_cast<ssize_t>(sizeof(dirty_buf)),
            ::pwrite(fd, dirty_buf, sizeof(dirty_buf), restart_tail_offset));
  ASSERT_EQ(0, ::close(fd));
  fd = -1;

  ASSERT_EQ(OB_SUCCESS, device_wrapper.init(base_dir,
                                            1 /* disk_io_thread_count */,
                                            16 /* max_io_depth */,
                                            &ObIOManager::get_instance(),
                                            &ObDeviceManager::get_instance()));
  ASSERT_EQ(OB_SUCCESS, io_adapter.init(OB_SERVER_TENANT_ID,
                                        device_wrapper.get_local_device(),
                                        &share::ObResourceManager::get_instance(),
                                        &ObIOManager::get_instance()));
  ASSERT_EQ(OB_SUCCESS, storage.init(base_dir,
                                     "log",
                                     LSN(0),
                                     1,
                                     PALF_BLOCK_SIZE,
                                     LOG_DIO_ALIGN_SIZE,
                                     LOG_DIO_ALIGNED_BUF_SIZE_REDO,
                                     update_manifest_cb,
                                     NULL,
                                     NULL,
                                     NULL,
                                     &io_adapter));

  EXPECT_EQ(OB_INVALID_ARGUMENT, storage.truncate_async_recovery_tail_(LSN(LOG_DIO_ALIGN_SIZE), 1));
  EXPECT_EQ(0, update_manifest_count);
  EXPECT_EQ(OB_SUCCESS, storage.truncate_async_recovery_tail_(LSN(LOG_DIO_ALIGN_SIZE), 0));
  EXPECT_EQ(0, update_manifest_count);
  ASSERT_NE(-1, fd = ::open(block_path, O_RDONLY, FILE_OPEN_MODE));
  ASSERT_EQ(static_cast<ssize_t>(sizeof(read_buf)),
            ::pread(fd, read_buf, sizeof(read_buf), restart_tail_offset));
  EXPECT_EQ(0, memcmp(read_buf, zero_buf, sizeof(read_buf)));
  ASSERT_EQ(0, ::close(fd));
  fd = -1;

  storage.destroy();
  io_adapter.destroy();
  device_wrapper.destroy();
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().wait();
  ObIOManager::get_instance().destroy();
  share::ObTenantEnv::set_tenant(NULL);
  FileDirectoryUtils::delete_directory_rec(base_dir);
}

TEST(TestLogStorage, async_recovery_accepts_partial_log_only_after_valid_entry_in_last_block)
{
  LogStorage storage;
  PalfIterator<LogGroupEntry> iterator;
  bool is_async_dirty_suffix_candidate = false;

  EXPECT_TRUE(storage.is_tail_locate_result_acceptable_(OB_PARTIAL_LOG,
                                                       1,
                                                       1,
                                                       true /* allow_mid_log_hole */,
                                                       true /* has_valid_entry */,
                                                       iterator,
                                                       is_async_dirty_suffix_candidate));
  EXPECT_TRUE(is_async_dirty_suffix_candidate);
  EXPECT_FALSE(storage.is_tail_locate_result_acceptable_(OB_PARTIAL_LOG,
                                                        1,
                                                        1,
                                                        false /* allow_mid_log_hole */,
                                                        true /* has_valid_entry */,
                                                        iterator,
                                                        is_async_dirty_suffix_candidate));
  EXPECT_FALSE(is_async_dirty_suffix_candidate);
  EXPECT_FALSE(storage.is_tail_locate_result_acceptable_(OB_PARTIAL_LOG,
                                                        0,
                                                        1,
                                                        true /* allow_mid_log_hole */,
                                                        true /* has_valid_entry */,
                                                        iterator,
                                                        is_async_dirty_suffix_candidate));
  EXPECT_FALSE(is_async_dirty_suffix_candidate);
  EXPECT_FALSE(storage.is_tail_locate_result_acceptable_(OB_PARTIAL_LOG,
                                                        1,
                                                        1,
                                                        true /* allow_mid_log_hole */,
                                                        false /* has_valid_entry */,
                                                        iterator,
                                                        is_async_dirty_suffix_candidate));
  EXPECT_FALSE(is_async_dirty_suffix_candidate);
}

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

TEST(TestPalfHandleImpl, get_lsn_scn_no_submit)
{
  PalfHandleImpl impl;
  share::SCN scn = share::SCN::base_scn();
  const int64_t base_log_id = FIRST_VALID_LOG_ID - 1;
  const LSN base_lsn(0);
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.init(base_log_id, scn, base_lsn));
  impl.sw_.is_inited_ = true;
  impl.sw_.committed_end_lsn_ = base_lsn;
  impl.sw_.max_flushed_end_lsn_ = base_lsn;
  impl.sw_.last_slide_scn_ = scn;

  EXPECT_EQ(base_lsn, impl.get_end_lsn());
  EXPECT_EQ(base_lsn, impl.get_max_lsn());
  EXPECT_EQ(scn, impl.get_end_scn());
  EXPECT_EQ(scn, impl.get_max_scn());
}

TEST(TestPalfHandleImpl, get_end_lsn_returns_committed_flushed_min)
{
  PalfHandleImpl impl;
  share::SCN scn = share::SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.init(10, scn, LSN(100)));
  impl.sw_.is_inited_ = true;
  impl.sw_.committed_end_lsn_ = LSN(250);
  impl.sw_.max_flushed_end_lsn_ = LSN(240);

  EXPECT_EQ(LSN(240), impl.get_end_lsn());

  impl.sw_.max_flushed_end_lsn_ = LSN(260);
  EXPECT_EQ(LSN(250), impl.get_end_lsn());
}

TEST(TestPalfHandleImpl, get_max_lsn_scn_tracks_allocator)
{
  PalfHandleImpl impl;
  share::SCN scn = share::SCN::base_scn();
  const int64_t base_log_id = FIRST_VALID_LOG_ID - 1;
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.init(base_log_id, scn, LSN(0)));
  impl.sw_.is_inited_ = true;

  const int64_t max_log_id = base_log_id + 5;
  const LSN max_lsn(100);
  ASSERT_EQ(OB_SUCCESS, impl.sw_.lsn_allocator_.inc_update_last_log_info(max_lsn, max_log_id, scn));

  EXPECT_EQ(max_lsn, impl.get_max_lsn());
  EXPECT_EQ(scn, impl.get_max_scn());
}

TEST(TestLogEngine, trusts_persisted_v2_io_mode_below_data_version_barrier)
{
  LogEngine log_engine;
  LogIOMode io_mode = LogIOMode::INVALID;
  LogReplicaPropertyMeta property_meta;
  ASSERT_EQ(OB_SUCCESS,
            property_meta.generate(
                true, LogReplicaType::NORMAL_REPLICA, LogIOMode::ASYNC));
  ASSERT_EQ(OB_SUCCESS, log_engine.log_meta_.update_log_replica_property_meta(property_meta));

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_4_2_1);
  ObClusterVersion::get_instance().update_data_version(DATA_VERSION_4_4_2_1);
  // The data-version barrier gates mode creation and transition. Recovery must
  // trust the persisted mode to interpret the existing log contents correctly.
  EXPECT_EQ(OB_SUCCESS, log_engine.get_persisted_log_io_mode_(io_mode));
  EXPECT_EQ(LogIOMode::ASYNC, io_mode);
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
}

TEST(TestPalfEnvImpl, determine_log_io_mode_uses_tenant_config_and_data_version)
{
  PalfEnvImpl env;
  LogIOMode io_mode = LogIOMode::INVALID;
  const int64_t user_tenant_id = 1002;

  EXPECT_EQ(OB_SUCCESS, env.determine_log_io_mode_(user_tenant_id, true, io_mode));
  EXPECT_EQ(LogIOMode::ASYNC, io_mode);
  EXPECT_EQ(OB_SUCCESS, env.determine_log_io_mode_(user_tenant_id, false, io_mode));
  EXPECT_EQ(LogIOMode::SYNC, io_mode);
  EXPECT_EQ(OB_SUCCESS,
            env.determine_log_io_mode_(gen_meta_tenant_id(user_tenant_id), true, io_mode));
  EXPECT_EQ(LogIOMode::SYNC, io_mode);
  EXPECT_EQ(OB_SUCCESS, env.determine_log_io_mode_(OB_SYS_TENANT_ID, true, io_mode));
  EXPECT_EQ(LogIOMode::SYNC, io_mode);

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_4_2_1);
  ObClusterVersion::get_instance().update_data_version(DATA_VERSION_4_4_2_1);
  EXPECT_EQ(OB_SUCCESS, env.determine_log_io_mode_(user_tenant_id, true, io_mode));
  EXPECT_EQ(LogIOMode::SYNC, io_mode);
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
}

TEST(TestPalfHandleImpl, finish_handle_init_sets_scan_finished)
{
  PalfEnvImpl env;
  FinishInitTestPalfHandle impl;
  LogIOWorker submitter;

  ASSERT_EQ(OB_SUCCESS, env.finish_palf_handle_init_(1, 1, &submitter, &impl));
  EXPECT_EQ(1, impl.get_scan_finished_count());
}

TEST(TestPalfHandleImpl, finish_handle_init_propagates_errsim_failure)
{
  PalfEnvImpl env;
  FinishInitTestPalfHandle impl;
  LogIOWorker submitter;
  common::EventItem event_item;
  common::EventItem reset_item;
  int ret = OB_SUCCESS;
  event_item.error_code_ = OB_ERR_UNEXPECTED;
  event_item.occur_ = 1;
  event_item.trigger_freq_ = 0;
  ASSERT_EQ(OB_SUCCESS,
      common::EventTable::set_event("ERRSIM_PALF_FINISH_HANDLE_INIT_FAIL", event_item));

  ret = env.finish_palf_handle_init_(1, 1, &submitter, &impl);
  PALF_LOG(INFO, "finish_palf_handle_init_ errsim test returned", K(ret));
  EXPECT_EQ(OB_SUCCESS,
      common::EventTable::set_event("ERRSIM_PALF_FINISH_HANDLE_INIT_FAIL", reset_item));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(1, impl.get_scan_finished_count());
}

TEST(TestPalfHandleImpl, cleanup_failed_inserted_handle_reverts_create_ref)
{
  PalfEnvImpl env;
  PalfHandleImpl *impl = PalfHandleImplFactory::alloc();
  const LSKey hash_map_key(1001);
  ASSERT_NE(static_cast<PalfHandleImpl *>(NULL), impl);
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("TEST_PALF_MAP", OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.insert_and_get(hash_map_key, impl));
  EXPECT_EQ(1, env.palf_handle_impl_map_.count());
  EXPECT_EQ(OB_ENTRY_EXIST, env.palf_handle_impl_map_.contains_key(hash_map_key));
  EXPECT_EQ(common::RefHandle::BORN_REF + 1, impl->get_uref());

  env.cleanup_failed_inserted_palf_handle_impl_(hash_map_key, /* need_revert */ true, impl);

  EXPECT_EQ(static_cast<PalfHandleImpl *>(NULL), impl);
  EXPECT_EQ(0, env.palf_handle_impl_map_.count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, env.palf_handle_impl_map_.contains_key(hash_map_key));
}

TEST(TestPalfHandleImpl, cleanup_failed_inserted_handle_deletes_reverted_reload_ref)
{
  PalfEnvImpl env;
  PalfHandleImpl *impl = PalfHandleImplFactory::alloc();
  const LSKey hash_map_key(1002);
  ASSERT_NE(static_cast<PalfHandleImpl *>(NULL), impl);
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("TEST_PALF_MAP", OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.insert_and_get(hash_map_key, impl));
  const int32_t uref_after_insert = impl->get_uref();
  EXPECT_EQ(common::RefHandle::BORN_REF + 1, uref_after_insert);
  env.palf_handle_impl_map_.revert(impl);
  EXPECT_EQ(common::RefHandle::BORN_REF, impl->get_uref());

  env.cleanup_failed_inserted_palf_handle_impl_(hash_map_key, /* need_revert */ false, impl);

  EXPECT_EQ(static_cast<PalfHandleImpl *>(NULL), impl);
  EXPECT_EQ(0, env.palf_handle_impl_map_.count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, env.palf_handle_impl_map_.contains_key(hash_map_key));
}

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
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  oceanbase::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
