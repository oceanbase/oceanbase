// owner: zjf225077
// owner group: log

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

#define private public
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#undef private
#undef protected
#ifdef OB_BUILD_SHARED_STORAGE
#include "log/ob_shared_log_utils.h"
#endif
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager

// 测试OSS需要设置如下几个环境变量
//export BUCKET=xxxx
//export ENDPOINT=xxxx
//export ACCESS_ID=xxxx
//export ACCESS_KEY=xxx
//export MODE=ALL
//export BMSQL=~/bmsql
//export SYSBENCH=~/sysbench
//export END_BLOCK=50
//export UPLOAD=FALSE

const std::string LOG_NAME = "./";
const std::string TEST_NAME = "iterator_performance";
std::map<std::string, int64_t> g_read_bw;
std::string g_map_key;

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogSharedStorage : public ObSimpleLogClusterTestEnv
  {
  public:
    TestObSimpleLogSharedStorage() : ObSimpleLogClusterTestEnv()
    {
      int ret = init();
      if (OB_SUCCESS != ret) {
        throw std::runtime_error("TestObSimpleLogDiskMgr init failed");
      }
    }
    ~TestObSimpleLogSharedStorage()
    {
      destroy();
    }
    int init()
    {
      id_ = 0;
      return OB_SUCCESS;
    }
    void destroy()
    {}
    void prepare_data(const block_id_t end_block_id,
                      const int64_t log_size,
                      char *log_dir)
    {
      ASSERT_EQ(true, end_block_id > 0);
      ASSERT_EQ(true, log_size > 100);
      LSN end_lsn(end_block_id*PALF_BLOCK_SIZE);
      const int64_t log_entry_count = end_lsn.val_ /log_size + 1;
      int64_t id = ATOMIC_AAF(&id_, 1);
      int64_t leader_idx = 0;
      PalfHandleImplGuard leader;
      share::SCN create_scn = share::SCN::base_scn();
      EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
      EXPECT_EQ(OB_SUCCESS, submit_log(leader, log_entry_count, leader_idx, log_size));
      const char *src_log_dir = leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_;
      strncpy(log_dir, src_log_dir, OB_MAX_FILE_NAME_LENGTH);
      PALF_LOG(INFO, "prepare_data success", K(log_dir), K(log_size), K(log_entry_count));
    }

    int64_t id_;
  };

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = true;

int init_log_shared_storage(const uint64_t dst_tenant_id)
{
  int ret = OB_SUCCESS;
  const uint64_t mittest_memory = 6L * 1024L * 1024L * 1024L;
  ObDeviceConfig *device_config = OB_NEW(ObDeviceConfig, "test");
  const char *base_dir_c = getenv("BUCKET");
  if (NULL == base_dir_c) {
    PALF_LOG(ERROR, "invalid bucket", KP(base_dir_c));
    return ret;
  }
  std::string base_dir = std::string(base_dir_c) + "/";
  std::string root_path = "oss://" + base_dir;
  const char *used_for = ObStorageUsedType::get_str(ObStorageUsedType::TYPE::USED_TYPE_ALL);
//  std::string root_path = "file://" + base_dir;
//  const char *used_for = ObStorageUsedType::get_str(ObStorageUsedType::TYPE::USED_TYPE_LOG);
  const char *state = "ADDED";
  const char *endpoint = getenv("ENDPOINT");
  if (NULL == endpoint) {
    PALF_LOG(ERROR, "invalid endpoint", KP(endpoint));
    return ret;
  }
  std::string endpoint_str = std::string("host=") + endpoint;
  const char *access_id = getenv("ACCESS_ID");
  const char *access_key = getenv("ACCESS_KEY");
  if (NULL == access_id || NULL == access_key) {
    PALF_LOG(ERROR, "invalid ak/sk", KP(access_id), KP(access_key));
    return ret;
  }
  std::string access_info_str = std::string("access_mode=access_by_id&access_id=") + access_id + "&access_key=" + access_key;
  MEMCPY(device_config->path_, root_path.c_str(), root_path.size());
  MEMCPY(device_config->used_for_, used_for, strlen(used_for));
  device_config->sub_op_id_ = 1;
  STRCPY(device_config->state_, state);
  device_config->op_id_ = 1;
  device_config->last_check_timestamp_ = 0;
  device_config->storage_id_ = 1;
  STRCPY(device_config->path_, root_path.c_str());
  //STRCPY(device_config->endpoint_, endpoint_str.c_str());
  //STRCPY(device_config->access_info_, access_info_str.c_str());
  device_config->max_iops_ = 0;
  device_config->max_bandwidth_ = 0;

  const uint64_t test_memory = 6L * 1024L * 1024L * 1024L;
  char tenant_uri[OB_MAX_URI_LENGTH] = {'\0'};
  ObBackupDest dest;
  ObBackupIoAdapter io_adapter;
  SMART_VAR(ObDeviceConfig, device_config_ori) {
    if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config(
      ObStorageUsedType::TYPE::USED_TYPE_LOG, device_config_ori))) {
      CLOG_LOG(WARN, "get_device_config failed", K(device_config));
    } else if (OB_FAIL(ObDeviceConfigMgr::get_instance().remove_device_config(device_config_ori))) {
      CLOG_LOG(WARN, "remove_device_config failed", K(device_config));
    }
  }
  if (OB_FAIL(ObDeviceConfigMgr::get_instance().add_device_config(*device_config))) {
    PALF_LOG(ERROR, "add_device_config failed");
  } else {
    GCTX.startup_mode_ = ObServerMode::SHARED_STORAGE_MODE;
    PALF_LOG(INFO, "init_log_shared_storage_ success", K(tenant_uri));
  }
  OB_DELETE(ObDeviceConfig, "test", device_config);
  return ret;
}

int read_log_pf(PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  LogEntry log_entry;
  LSN curr_lsn;
  int64_t start_ts = ObTimeUtility::current_time();
  while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
    if (OB_FAIL(iterator.get_entry(log_entry, curr_lsn))) {
      PALF_LOG(WARN, "get_entry failed", K(curr_lsn));
    } else {}
  }
  if (OB_ITER_END != ret) {
    PALF_LOG(ERROR, "iterator failed", KR(ret), K(iterator));
  } else {
    int64_t read_log_size = curr_lsn - LSN(PALF_INITIAL_LSN_VAL) + log_entry.get_serialize_size();
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    int64_t bw = read_log_size/(cost_ts/1000/1000+1)/1024/1024;
    g_read_bw[g_map_key] = bw;
    LOG_DBA_ERROR(OB_SUCCESS, "msg", "bandwidth,", "key", g_map_key.c_str(), K(bw), K(iterator));
  }
  return ret;
}

int upload_blocks(const uint64_t dst_tenant_id,
                  const int64_t dst_palf_id,
                  const char *src_dir,
                  const block_id_t start_block_id,
                  const block_id_t end_block_id)
{
  ObLogExternalStorageHandler handler;
  int ret = OB_SUCCESS;
  const int64_t UPLOAD_SIZE = PALF_PHY_BLOCK_SIZE;
  char *read_buf_ptr = reinterpret_cast<char*>(ob_malloc_align(LOG_DIO_ALIGN_SIZE, UPLOAD_SIZE, "test"));
  LogReader log_reader;
  LogIOAdapter io_adapter;
  if (NULL == read_buf_ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(io_adapter.init(dst_tenant_id, LOG_IO_DEVICE_WRAPPER.get_local_device(), &G_RES_MGR, &OB_IO_MANAGER))) {
    PALF_LOG(WARN, "init io_adapter failed", K(dst_tenant_id), K(ret));
  } else if (OB_FAIL(log_reader.init(src_dir, PALF_PHY_BLOCK_SIZE, &io_adapter))) {
    PALF_LOG(WARN, "init log_reader failed", KP(src_dir));
  } else if (OB_FAIL(handler.init())) {
    PALF_LOG(WARN, "init hstartandler failed", KP(src_dir));
  } else if (OB_FAIL(handler.start(0))) {
    PALF_LOG(WARN, "start handler failed", KP(src_dir));
  } else {
    int64_t start_ts = ObTimeUtility::current_time();
    block_id_t curr_block_id = start_block_id;
    ReadBuf read_buf(read_buf_ptr, UPLOAD_SIZE + LOG_DIO_ALIGN_SIZE);
    int64_t out_read_size = 0;
    LogIOContext io_ctx(LogIOUser::DEFAULT);
    while (curr_block_id < end_block_id) {
      if (OB_FAIL(log_reader.pread(curr_block_id, 0, UPLOAD_SIZE, read_buf, out_read_size, io_ctx))) {
        PALF_LOG(WARN, "pread failed", KP(src_dir));
      } else if (OB_FAIL(handler.upload(dst_tenant_id, dst_palf_id, curr_block_id, read_buf_ptr, UPLOAD_SIZE))) {
        PALF_LOG(WARN, "upload failed", KP(src_dir));
      } else {
        curr_block_id++;
        PALF_LOG(INFO, "upload success", K(curr_block_id));
      }
    }
    if (OB_FAIL(ret)) {
      PALF_LOG(ERROR, "upload failed", K(curr_block_id));
    } else {
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      int64_t upload_size = (end_block_id - start_block_id) * PALF_PHY_BLOCK_SIZE;
      int64_t bw = upload_size/(cost_ts/1000/1000 + 1)/1024/1024;
      PALF_LOG(ERROR, "runlin trace upload bandwidth", K(cost_ts), K(upload_size), K(bw));
      LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "runlin trace upload bandwidth", K(upload_size), K(cost_ts), K(bw));
    }
    if (NULL != read_buf_ptr) {
      ob_free_align(read_buf_ptr);
    }
  }
  return ret;
}

enum class MODE {
  INVALID = 0,
  PL1,
  OSS,
  ALL
};
block_id_t g_end_block_id = 10;
std::string g_base_file_name_root = "";
std::string g_base_file_name_curr = "";

void performance_impl(int64_t id, const std::vector<int> &threads, const std::vector<int> &single_read_sizes)
{
  ObLogService *log_service = MTL(ObLogService*);
  PalfBufferIterator log_biterator;
    for (auto single_read_size : single_read_sizes) {
      for (auto thread_cnt : threads) {
        std::string log_name = g_base_file_name_curr + "_iterator_performance_th" + std::to_string(thread_cnt) ;
        log_service->get_log_ext_handler()->resize(thread_cnt);
        ObLogExternalStorageHandler::SINGLE_TASK_MINIMUM_SIZE = single_read_size/thread_cnt;
        int ret = OB_SUCCESS;
        g_map_key = g_base_file_name_curr + "_th_" + std::to_string(thread_cnt) + "_single_read_size_" + std::to_string(ObLogExternalStorageHandler::SINGLE_TASK_MINIMUM_SIZE)
                    + "_read_size_" + std::to_string(single_read_size/1024/1024) + "M";
        //SET_CASE_LOG_FILE(LOG_NAME, g_map_key.c_str());
        ObLogSharedStorage::READ_SIZE = single_read_size;
        EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), LSN(PALF_INITIAL_LSN_VAL), log_biterator));
        EXPECT_EQ(OB_ITER_END, read_log_pf(log_biterator));
      }
    }
}
void performance_oss(PalfHandleImplGuard &guard, const int64_t id)
{
  const LSN end_lsn(g_end_block_id*(PALF_BLOCK_SIZE));
  guard.palf_handle_impl_->sw_.committed_end_lsn_ = end_lsn;
  guard.palf_handle_impl_->log_engine_.log_storage_.readable_log_tail_ = end_lsn;
  guard.palf_handle_impl_->log_engine_.log_storage_.log_tail_ = end_lsn;
  guard.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.min_block_id_ = 10000;
  guard.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.max_block_id_ = 10001;
  g_base_file_name_curr = g_base_file_name_root + "_oss";
  std::vector<int> threads = {10}; //, 2, 4, 8, 16, 32, 64};
  std::vector<int> single_read_sizes = {PALF_BLOCK_SIZE};//, PALF_BLOCK_SIZE/2, PALF_BLOCK_SIZE/4, PALF_BLOCK_SIZE/8, PALF_BLOCK_SIZE/16, PALF_BLOCK_SIZE/32};
  GCTX.startup_mode_ = ObServerMode::SHARED_STORAGE_MODE;
  performance_impl(id, threads, single_read_sizes);
}
void performance_pl1(PalfHandleImplGuard &guard, const int64_t id)
{
  const LSN end_lsn(g_end_block_id*PALF_BLOCK_SIZE);
  guard.palf_handle_impl_->sw_.committed_end_lsn_ = end_lsn;
  guard.palf_handle_impl_->log_engine_.log_storage_.readable_log_tail_ = end_lsn;
  guard.palf_handle_impl_->log_engine_.log_storage_.log_tail_ = end_lsn;
  guard.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.min_block_id_ = 0;
  guard.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.max_block_id_ = 10001;
  g_base_file_name_curr = g_base_file_name_root + "_pl1";
  std::vector<int> threads = {1};
  std::vector<int> single_read_sizes = {MAX_LOG_BUFFER_SIZE};
  GCTX.startup_mode_ = ObServerMode::NORMAL_MODE;
  performance_impl(id, threads, single_read_sizes);
}
void performance(PalfHandleImplGuard &guard, const char *mode, const int64_t id)
{
  MODE mode_en;
  if (0 == strcmp(mode, "PL1")) {
    mode_en = MODE::PL1;
    performance_pl1(guard, id);
  } else if (0 == strcmp(mode, "OSS")) {
    mode_en = MODE::OSS;
    performance_oss(guard, id);
  } else if (0 == strcmp(mode, "ALL")) {
    mode_en = MODE::ALL;
    performance_pl1(guard, id);
    performance_oss(guard, id);
  } else {
    int ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error", K(mode));
    return;
  }
}

TEST_F(TestObSimpleLogSharedStorage, iterator_performance)
{
  SET_CASE_LOG_FILE(LOG_NAME, "iterator_performance");
  OB_LOGGER.set_log_level("INFO");
  const char *src_dir_sysbench  = getenv("SYSBENCH");
  const char *src_dir_bmsql  = getenv("BMSQL");
  const char *mode = getenv("MODE");
  const char *END_BLOCK = getenv("END_BLOCK");
  const char *need_upload_blocks = getenv("UPLOAD");
  const char *log_size_str = getenv("LOG_SIZE");
  int ret = OB_SUCCESS;
  if (NULL == mode || NULL == END_BLOCK || NULL == need_upload_blocks || NULL == log_size_str) {
    LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "invalid argment", KP(mode), KP(END_BLOCK), KP(need_upload_blocks), KP(log_size_str));
    return;
  }
  update_server_log_disk(10*1024*1024*1024ul);
  update_disk_options(10*1024*1024*1024ul/palf::PALF_PHY_BLOCK_SIZE);
  sleep(3);
  ObLogService *log_service = MTL(ObLogService*);
  log_service->get_log_ext_handler()->init();
  log_service->get_log_ext_handler()->start(0);
  log_service->shared_log_service_.stop();
  const uint64_t dst_tenant_id = 1002;
  //init_log_shared_storage(dst_tenant_id);
  g_end_block_id = std::stoi(END_BLOCK);
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {0};
  int64_t log_size = atoll(log_size_str);
  prepare_data(g_end_block_id, log_size, log_dir);
  if (true/*NULL != src_dir_sysbench*/) {
    char src_dir_sysbench_array[OB_MAX_FILE_NAME_LENGTH] = {0};
    STRNCPY(src_dir_sysbench_array, src_dir_sysbench, strlen(src_dir_sysbench));
    g_base_file_name_root = "sysbench";
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    const int64_t dst_palf_id = id;
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    share::SCN create_scn = share::SCN::base_scn();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
    const block_id_t start_block_id = 0;
    const block_id_t end_block_id = g_end_block_id;
    // 上传g_end_block_id个文件到oss
    if (0 == strcmp(need_upload_blocks, "TRUE")) {
      EXPECT_EQ(OB_SUCCESS, upload_blocks(dst_tenant_id, dst_palf_id, log_dir, start_block_id, end_block_id));
    }
    if (0 != strcmp(mode, "OSS")) {
      std::string cp_cmd = "cp -r " + std::string(src_dir_sysbench) + "/* " + std::string(leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_);
      PALF_LOG(ERROR, "begin cp to pl1", K(dst_palf_id));
      system(cp_cmd.c_str());
      PALF_LOG(ERROR, "after cp to pl1", K(dst_palf_id));
      sync();
      PALF_LOG(ERROR, "cp to pl1 success", K(dst_palf_id));
    }
    performance(leader, mode, id);
  }
//  if (NULL != src_dir_bmsql)  {
//    char src_dir_bmsql_array[OB_MAX_FILE_NAME_LENGTH] = {0};
//    STRNCPY(src_dir_bmsql_array, src_dir_bmsql, strlen(src_dir_bmsql));
//    SET_CASE_LOG_FILE(LOG_NAME, "iterator_performance");
//    g_base_file_name_root = "bmsql";
//    int64_t id = ATOMIC_AAF(&palf_id_, 1);
//    const int64_t dst_palf_id = id;
//    int64_t leader_idx = 0;
//    PalfHandleImplGuard leader;
//    share::SCN create_scn = share::SCN::base_scn();
//    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
//    const block_id_t start_block_id = 0;
//    const block_id_t end_block_id = g_end_block_id;
//    // 上传400个文件到oss
//    if (0 == strcmp(need_upload_blocks, "TRUE")) {
//      EXPECT_EQ(OB_SUCCESS, upload_blocks(dst_tenant_id, dst_palf_id, src_dir_bmsql_array, start_block_id, end_block_id));
//    }
//    if (0 != strcmp(mode, "OSS")) {
//      std::string cp_cmd = "cp -r " + std::string(src_dir_bmsql) + "/* " + std::string(leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_);
//      system(cp_cmd.c_str());
//      sync();
//      PALF_LOG(ERROR, "cp to pl1 success", K(dst_palf_id), K(cp_cmd.c_str()));
//    }
//    performance(leader, mode, id);
//  }
  {
    SET_CASE_LOG_FILE(LOG_NAME, "result");
    for (auto &iter : g_read_bw) {
      int ret = OB_SUCCESS;
      std::string key = iter.first;
      int64_t value = iter.second;
      SET_CASE_LOG_FILE(LOG_NAME, "result");
      LOG_DBA_ERROR(OB_SUCCESS, "result ", "msg", "key", key.c_str(), "value", value);
    }
  }
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    const int64_t dst_palf_id = id;
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    share::SCN create_scn = share::SCN::base_scn();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
