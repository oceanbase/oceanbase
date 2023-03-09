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
#include "io/easy_connection.h"
#include "lib/file/file_directory_utils.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include <cstdio>
#include <signal.h>
#include "lib/utility/ob_defer.h"
#include "share/ob_errno.h"
#define private public
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_rpc_processor.h"
#include "env/ob_simple_log_cluster_env.h"

#undef private

const std::string TEST_NAME = "basic_func";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterBasicFunc : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterBasicFunc() : ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

TEST_F(TestObSimpleLogClusterBasicFunc, submit_log)
{
  SET_CASE_LOG_FILE(TEST_NAME, "submit_log");
  //OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t create_ts = 100;
  int64_t leader_idx = 0;
  int64_t log_ts = 1;
  PalfHandleGuard leader;
  ObTimeGuard guard("submit_log", 0);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_ts, leader_idx, leader));
  guard.click("create");

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1024, id));
  guard.click("submit_log");
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  guard.click("read_log");

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1024, id));
  guard.click("submit_log2");
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  guard.click("readlog2");
  guard.click("delete");
  PALF_LOG(INFO, "end test submit_log", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, restart_and_clear_tmp_files)
{
  SET_CASE_LOG_FILE(TEST_NAME, "restart_and_clear_tmp_files");
  ObTimeGuard guard("restart_and_clear_tmp_files", 0);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  std::string logserver_dir;
  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    guard.click("create");
    logserver_dir = leader.palf_env_->palf_env_impl_.log_dir_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, 1 * 1024 * 1024));
    guard.click("submit_log");
    while (leader.palf_handle_.palf_handle_impl_->get_end_lsn()
           < LSN(100 * 1024 * 1024ul)) {
      usleep(100 * 1000);
    }
  }
  const std::string base_dir = logserver_dir;
  const std::string tmp_1_dir = base_dir + "/10000.tmp/log/";
  const std::string mkdir_tmp_1 = "mkdir -p " + tmp_1_dir;
  const std::string dir_2 = base_dir + "/10000000";
  const std::string dir_normal_file = base_dir + "/10000000/log/1.tmp";
  const std::string dir_normal_file1 = base_dir + "/10000000/meta/1.tmp";
  const std::string mkdir_2 = "mkdir -p " + dir_normal_file;
  const std::string mkdir_3 = "mkdir -p " + dir_normal_file1;
  system(mkdir_tmp_1.c_str());
  system(mkdir_2.c_str());
  system(mkdir_3.c_str());
  int ret = OB_SUCCESS;
  guard.click("mkdir");
  EXPECT_EQ(OB_ERR_UNEXPECTED, restart_paxos_groups());
  CLOG_LOG(INFO, "after restart_paxos_groups after exist tmp dir");
  guard.click("restart");
  const std::string rm_dir_2 = "rm -rf " + dir_2;
  system(rm_dir_2.c_str());
  guard.click("rm_dir");
  if (OB_FAIL(restart_paxos_groups())) {
    PALF_LOG(ERROR, "restart_paxos_groups failed", K(ret));
  } else {
    {
      CLOG_LOG(INFO, "after restart_paxos_groups after remove tmp dir");
      guard.click("restart");
      bool result = false;
      EXPECT_EQ(OB_SUCCESS,
                common::FileDirectoryUtils::is_exists(tmp_1_dir.c_str(), result));
      EXPECT_EQ(result, false);
      PalfHandleGuard leader1;
      EXPECT_EQ(OB_SUCCESS, get_leader(id, leader1, leader_idx));
      guard.click("get_leader");
      LogStorage *log_storage =
          &leader1.palf_handle_.palf_handle_impl_->log_engine_.log_storage_;
      LSN lsn_origin_log_tail = log_storage->get_log_tail_guarded_by_lock_();
      EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 10, leader_idx, 1 * 1024 * 1024));
      while (log_storage->log_tail_ == lsn_origin_log_tail) {
        usleep(1 * 1000);
        PALF_LOG(INFO, "log_tail is same", KPC(log_storage), K(lsn_origin_log_tail));
      }
      guard.click("submit_log");
      EXPECT_EQ(OB_ITER_END, read_log(leader1));
      guard.click("read_log");
    }
    // 验证tenant下有临时文件的场景，该临时文件需要归还给log_pool
    {
      PalfHandleGuard leader1;
      int64_t leader_idx1 = 0;
      EXPECT_EQ(OB_SUCCESS, get_leader(id, leader1, leader_idx1));
      std::string palf_log_dir = leader1.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_;
      ObSimpleLogServer *server = get_cluster()[leader_idx1];
      std::string log_pool = server->log_block_pool_.log_pool_path_;
      const block_id_t min_block_id = server->log_block_pool_.min_block_id_;
      char src[1024] = {'\0'};
      char dest[1024] = {'\0'};
      block_id_to_tmp_string(10000, dest, 1024);
      block_id_to_string(min_block_id, src, 1024);
      std::string src_str = log_pool + "/" + src;
      std::string dest_str = palf_log_dir + "/" + dest;
      std::string mv_system = "mv " + src_str + " " + dest_str;
      system(mv_system.c_str());
      bool result1 = false;
      EXPECT_EQ(OB_SUCCESS,
                common::FileDirectoryUtils::is_exists(dest_str.c_str(), result1));
      EXPECT_EQ(true, result1);
      leader1.reset();
      EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
      EXPECT_EQ(OB_SUCCESS,
                common::FileDirectoryUtils::is_exists(dest_str.c_str(), result1));
      EXPECT_EQ(false, result1);
    }
    EXPECT_EQ(OB_SUCCESS, remove_dir());
    EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  PALF_LOG(INFO, "end test restart", K(id), K(guard));
}

bool check_locate_correct(const std::vector<LSN> &lsn_array,
                          const std::vector<int64_t> &log_ts_ns_array,
                          const int64_t input_ts_ns, const LSN result_lsn,
                          const bool later_less)
{
  bool find_match = false;
  for (int i = 0; i < lsn_array.size(); i++) {
    if (lsn_array[i].val_ - LogGroupEntryHeader::HEADER_SER_SIZE == result_lsn) {
      if (later_less) {
        EXPECT_LE(log_ts_ns_array[i], input_ts_ns);
      } else {
        EXPECT_GE(log_ts_ns_array[i], input_ts_ns);
      }
      find_match = true;
      break;
    }
    PALF_LOG(INFO, "check_locate_correct array", K(i), K(lsn_array[i]),
             K(log_ts_ns_array[i]));
  }
  PALF_LOG(INFO, "check_locate_correct", K(find_match), K(result_lsn), K(input_ts_ns));
  return find_match;
}

TEST_F(TestObSimpleLogClusterBasicFunc, test_locate_by_ts_ns_coarsely)
{
  SET_CASE_LOG_FILE(TEST_NAME, "locate_by_ts_ns");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "test test_locate_by_ts_ns_coarsely", K(id));
  int64_t leader_idx = 0;
  int ret = OB_SUCCESS;
  std::vector<LSN> lsn_array;
  std::vector<int64_t> log_ts_ns_array;
  PalfHandleGuard leader;
  LSN result_lsn;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  // no log
  EXPECT_EQ(OB_INVALID_ARGUMENT, leader.locate_by_ts_ns_coarsely(0, result_lsn));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, leader.locate_by_ts_ns_coarsely(
                                    ObTimeUtility::current_time_ns(), result_lsn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 256, leader_idx, lsn_array, log_ts_ns_array));
  sleep(1);
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, leader.locate_by_ts_ns_coarsely(10, result_lsn));
  // contains log
  int64_t input_ts_ns = ObTimeUtility::current_time_ns();
  EXPECT_EQ(OB_SUCCESS, leader.locate_by_ts_ns_coarsely(input_ts_ns, result_lsn));
  EXPECT_TRUE(
      check_locate_correct(lsn_array, log_ts_ns_array, input_ts_ns, result_lsn, true));
  input_ts_ns += 1000 * 1000 * 1000 * 1000L;
  EXPECT_EQ(OB_SUCCESS, leader.locate_by_ts_ns_coarsely(input_ts_ns, result_lsn));
  EXPECT_TRUE(
      check_locate_correct(lsn_array, log_ts_ns_array, input_ts_ns, result_lsn, true));

  ObTimeGuard guard("mittest", 0);
  // write too much log, do not submit to farm
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 6000, leader_idx, lsn_array, log_ts_ns_array));
  const int64_t min_ts = log_ts_ns_array.front();
  const int64_t max_ts = log_ts_ns_array.back();
  for (int i = 0; i < 10; i++) {
    const int64_t this_ts = ObRandom::rand(min_ts, max_ts);
    guard.click();
    EXPECT_EQ(OB_SUCCESS, leader.locate_by_ts_ns_coarsely(this_ts, result_lsn))
        << this_ts;
    guard.click();
    EXPECT_TRUE(
        check_locate_correct(lsn_array, log_ts_ns_array, input_ts_ns, result_lsn, true));
  }
  PALF_LOG(INFO, "end test_locate_by_ts_ns_coarsely", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, test_locate_by_lsn_coarsely)
{
  SET_CASE_LOG_FILE(TEST_NAME, "locate_by_lsn");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "test test_locate_by_lsn_coarsely", K(id));
  int64_t leader_idx = 0;
  int ret = OB_SUCCESS;
  std::vector<LSN> lsn_array;
  std::vector<int64_t> log_ts_ns_array;
  PalfHandleGuard leader;
  int64_t result_ts_ns;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  // no log
  EXPECT_EQ(OB_INVALID_ARGUMENT, leader.locate_by_lsn_coarsely(LSN(), result_ts_ns));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, leader_idx, lsn_array, log_ts_ns_array));
  sleep(1);

  LSN input_lsn;
  input_lsn.val_ = 0;
  EXPECT_EQ(OB_SUCCESS, leader.locate_by_lsn_coarsely(input_lsn, result_ts_ns));
  EXPECT_TRUE(
      check_locate_correct(lsn_array, log_ts_ns_array, result_ts_ns, input_lsn, false));

  EXPECT_EQ(OB_SUCCESS,
            submit_log(leader, 65, 1024 * 1024, leader_idx, lsn_array, log_ts_ns_array));

  input_lsn.val_ = 1 * PALF_BLOCK_SIZE;
  EXPECT_EQ(OB_SUCCESS, leader.locate_by_lsn_coarsely(input_lsn, result_ts_ns));
  EXPECT_TRUE(
      check_locate_correct(lsn_array, log_ts_ns_array, result_ts_ns, input_lsn, false))
      << result_ts_ns;

  PALF_LOG(INFO, "end test_locate_by_lsn_coarsely", K(id));
}

TEST_F(TestObSimpleLogClusterBasicFunc, test_switch_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test_switch_leader", K(id));
  int64_t leader_idx = 0;
  int64_t log_ts = 1;
  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1024, id));

  // switch leader
  leader_idx = 1;
  PalfHandleGuard new_leader1;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, new_leader1));
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader1, 1024, id));

  // switch leader
  PalfHandleGuard new_leader2;
  leader_idx = 2;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, new_leader2));
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader2, 1024, id));

  // switch leader
  PalfHandleGuard new_leader3;
  leader_idx = 1;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, new_leader3));
  // block the other two followers
  block_net(1, 0);
  block_net(1, 2);
  // leader submit new logs
  // PalfHandleGuard new_leader4;
  // EXPECT_EQ(OB_SUCCESS, submit_log(new_leader4, 1024, id));
  unblock_net(1, 0);
  unblock_net(1, 2);
  PALF_LOG(INFO, "end test_switch_leader", K(id));
}

TEST_F(TestObSimpleLogClusterBasicFunc, advance_base_lsn)
{
  SET_CASE_LOG_FILE(TEST_NAME, "advance_base_lsn");
  ObTimeGuard guard("advance_base_lsn", 0);
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start advance_base_lsn", K(id));
  int64_t leader_idx = 0;
  int64_t log_ts = 1;

  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_.palf_handle_impl_->log_engine_.truncate_prefix_blocks(LSN(0)));
  guard.click("create");
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  guard.click("submit");
  guard.click("get_leader");
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_.advance_base_lsn(LSN(0)));
  guard.click("advance_base_lsn");

  PALF_LOG(INFO, "end test advance_base_lsn", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, data_corrupted)
{
  SET_CASE_LOG_FILE(TEST_NAME, "data_corrupted");
  ObTimeGuard guard("data_corrupted", 0);
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start advance_base_lsn", K(id));
  int64_t leader_idx = 0;
  int64_t log_ts = 1;

  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  guard.click("create");
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(3);
  guard.click("submit");
  guard.click("get_leader");
  PalfGroupBufferIterator iterator;
  EXPECT_EQ(OB_SUCCESS, leader.seek(LSN(0), iterator));
  LogGroupEntry entry;
  LSN lsn;
  EXPECT_EQ(OB_SUCCESS, iterator.next());
  EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, lsn));
  char *buf = static_cast<char*>(ob_malloc_align(LOG_DIO_ALIGN_SIZE, MAX_LOG_BUFFER_SIZE));
  int64_t pos = 0;
  LogWriteBuf write_buf;
  write_buf.push_back(buf, MAX_LOG_BUFFER_SIZE);
  EXPECT_EQ(OB_SUCCESS, entry.serialize(buf, MAX_LOG_BUFFER_SIZE, pos));
  char *group_size_buf = buf + 4;
  *group_size_buf = 1;
  LSN origin = leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_.palf_handle_impl_->log_engine_.
      append_log(leader.palf_handle_.palf_handle_impl_->get_max_lsn(), write_buf, leader.palf_handle_.palf_handle_impl_->get_max_ts_ns() + 1));
  PalfGroupBufferIterator iterator1;
  auto get_file_end_lsn = [](){
    return LSN(LOG_MAX_LSN_VAL);
  };
  EXPECT_EQ(OB_SUCCESS, iterator1.init(origin, &leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_, get_file_end_lsn));
  EXPECT_EQ(OB_INVALID_DATA, iterator1.next());
  PALF_LOG(INFO, "end data_corrupted advance_base_lsn", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, limit_palf_instances)
{
  SET_CASE_LOG_FILE(TEST_NAME, "limit_palf_instances");
  OB_LOGGER.set_log_level("INFO");
  int64_t id1 = ATOMIC_AAF(&palf_id_, 1);
  int64_t id2 = ATOMIC_AAF(&palf_id_, 1);
  int server_idx = 0;
  PalfEnv *palf_env = NULL;
  int64_t leader_idx = 0;
  int64_t log_ts = 1;
  int64_t create_ts = 1;
	{
    PalfHandleGuard leader1;
    PalfHandleGuard leader2;
    PalfHandleGuard leader3;
    EXPECT_EQ(OB_SUCCESS, get_palf_env(server_idx, palf_env));
		int64_t id3 = palf_id_ + 1;
    palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = 2 * MIN_DISK_SIZE_PER_PALF_INSTANCE;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id1, create_ts, leader_idx, leader1));
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id2, create_ts, leader_idx, leader2));
    EXPECT_EQ(OB_LOG_OUTOF_DISK_SPACE, create_paxos_group(id3, create_ts, leader_idx, leader3));
	}
	EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id1));
}

TEST_F(TestObSimpleLogClusterBasicFunc, out_of_disk_space)
{
  SET_CASE_LOG_FILE(TEST_NAME, "out_of_disk_space");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int server_idx = 0;
  PalfEnv *palf_env = NULL;
  int64_t leader_idx = 0;
  int64_t log_ts = 1;
  int64_t create_ts = 1;
  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, get_palf_env(server_idx, palf_env));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_ts, leader_idx, leader));
  update_disk_options(leader_idx, MIN_DISK_SIZE_PER_PALF_INSTANCE/PALF_PHY_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 6*31+1, id, MAX_LOG_BODY_SIZE));
  LogStorage *log_storage = &leader.palf_handle_.palf_handle_impl_->log_engine_.log_storage_;
  while (LSN(6*PALF_BLOCK_SIZE) > log_storage->log_tail_) {
    usleep(500);
  }
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id, MAX_LOG_BODY_SIZE));
  while (LSN(6*PALF_BLOCK_SIZE + 20 * MAX_LOG_BODY_SIZE) > log_storage->log_tail_) {
    usleep(500);
  }
  usleep(1000*10);
  EXPECT_EQ(OB_LOG_OUTOF_DISK_SPACE, submit_log(leader, 1, id, MAX_LOG_BODY_SIZE));
  palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = 5 * MIN_DISK_SIZE_PER_PALF_INSTANCE;
}

TEST_F(TestObSimpleLogClusterBasicFunc, submit_group_log)
{
  SET_CASE_LOG_FILE(TEST_NAME, "submit_group_log");
  ObTimeGuard guard("submit_group_log", 0);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start submit_group_log", K(id));
  int64_t leader_idx = 0;
  int64_t log_ts = 1;

  PalfHandleGuard leader;
  PalfHandleGuard leader_raw_write;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_raw_write, leader_idx, leader_raw_write));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id, 1024 * 1024));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, LSN(512*1024*100)));
  EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(leader_raw_write));
  EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(leader, leader_raw_write));
  EXPECT_EQ(OB_ITER_END, read_log_from_memory(leader));
  PALF_LOG(INFO, "end submit_group_log advance_base_lsn", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, io_reducer_basic)
{
  SET_CASE_LOG_FILE(TEST_NAME, "io_reducer_basic");

  OB_LOGGER.set_log_level("INFO");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  leader.palf_env_->palf_env_impl_.log_io_worker_.batch_io_task_mgr_.has_batched_size_ = 0;
  leader.palf_env_->palf_env_impl_.log_io_worker_.batch_io_task_mgr_.handle_count_ = 0;
  std::vector<PalfHandleGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  int64_t lag_follower_idx = (leader_idx + 1) % node_cnt_;
  PalfHandleGuard &lag_follower = *palf_list[lag_follower_idx];
  block_net(leader_idx, lag_follower_idx);
  block_net(lag_follower_idx, leader_idx);

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10000, leader_idx, 120));
  const LSN max_lsn = leader.palf_handle_.palf_handle_impl_->get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  const int64_t has_batched_size = leader.palf_env_->palf_env_impl_.log_io_worker_.batch_io_task_mgr_.has_batched_size_;
  const int64_t handle_count = leader.palf_env_->palf_env_impl_.log_io_worker_.batch_io_task_mgr_.handle_count_;
  const int64_t log_id = leader.palf_handle_.palf_handle_impl_->sw_.get_max_log_id();
  PALF_LOG(ERROR, "batched_size", K(has_batched_size), K(log_id));

  unblock_net(leader_idx, lag_follower_idx);
  unblock_net(lag_follower_idx, leader_idx);

  int64_t start_ts = ObTimeUtility::current_time();
  LSN lag_follower_max_lsn = lag_follower.palf_handle_.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  while (lag_follower_max_lsn < max_lsn) {
    sleep(1);
    PALF_LOG(ERROR, "follower is lagged", K(max_lsn), K(lag_follower_max_lsn));
    lag_follower_max_lsn = lag_follower.palf_handle_.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  }
  const int64_t follower_has_batched_size = lag_follower.palf_env_->palf_env_impl_.log_io_worker_.batch_io_task_mgr_.has_batched_size_;
  const int64_t follower_handle_count = lag_follower.palf_env_->palf_env_impl_.log_io_worker_.batch_io_task_mgr_.handle_count_;
  EXPECT_EQ(OB_SUCCESS, revert_cluster_palf_handle_guard(palf_list));

  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  PALF_LOG(ERROR, "runlin trace performance", K(cost_ts), K(log_id), K(max_lsn), K(has_batched_size), K(handle_count),
      K(follower_has_batched_size), K(follower_handle_count));
}

TEST_F(TestObSimpleLogClusterBasicFunc, create_palf_via_middle_lsn)
{
  SET_CASE_LOG_FILE(TEST_NAME, "create_palf_via_middle_lsn");
  const int64_t prev_log_id = 1000;
  const LSN mid_lsn(palf::PALF_BLOCK_SIZE);
  const LSN prev_lsn(palf::PALF_BLOCK_SIZE - 100);
  const int64_t prev_log_ts = common::ObTimeUtility::current_time_ns();
  const int64_t prev_log_pid = 100;
  PalfBaseInfo palf_base_info;
  palf_base_info.curr_lsn_ = mid_lsn;
  palf_base_info.prev_log_info_.log_id_ = prev_log_id;
  palf_base_info.prev_log_info_.lsn_ = prev_lsn;
  palf_base_info.prev_log_info_.log_ts_ = prev_log_ts;
  palf_base_info.prev_log_info_.log_proposal_id_ = prev_log_pid;
  palf_base_info.prev_log_info_.accum_checksum_ = 1;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  {
    PalfHandleGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, palf_base_info, leader_idx, leader));
  }

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleGuard leader;
    int64_t leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, leader_idx, 1024*1024));
    const LSN max_lsn = leader.palf_handle_.palf_handle_impl_->get_max_lsn();
    const int64_t proposal_id = leader.palf_handle_.palf_handle_impl_->state_mgr_.get_proposal_id();
    const LSN begin_lsn = leader.palf_handle_.palf_handle_impl_->log_engine_.get_begin_lsn();
    int64_t begin_ts = OB_INVALID_TIMESTAMP;
    leader.palf_handle_.palf_handle_impl_->get_begin_ts_ns(begin_ts);
    // check LogInfo
    EXPECT_GT(proposal_id, prev_log_pid);
    EXPECT_GT(begin_ts, prev_log_ts);
    EXPECT_EQ(begin_lsn, mid_lsn);
    // wait_lsn_until_flushed(max_lsn, leader);
    wait_until_has_committed(leader, max_lsn);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, 10*1024));
  }

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleGuard leader;
    int64_t leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, leader_idx, 1024*1024));
    const LSN max_lsn = leader.palf_handle_.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn, leader);
    const LSN mid_lsn(palf::PALF_BLOCK_SIZE);
    EXPECT_EQ(OB_ITER_END, read_log(leader, mid_lsn));
  }
}
} // end unittest
} // end oceanbase

// Notes: How to write a new module integrate test case in logservice?
// 1. cp test_ob_simple_log_basic_func.cpp test_ob_simple_log_xxx.cpp
// 2. modify const string TEST_NAME, class name and log file name in
// test_ob_simple_log_xxx.cpp
// 3. add ob_unittest_clog() item and set label for test_ob_simple_log_xxx in
// unittest/cluster/CMakeFiles.txt
// 4. write new TEST_F

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
