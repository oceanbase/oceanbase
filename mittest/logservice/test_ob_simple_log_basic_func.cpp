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
#include "logservice/palf/lsn_allocator.h"
#include "share/scn.h"
#include "logservice/palf/log_rpc_processor.h"
#include "env/ob_simple_log_cluster_env.h"

#undef private

const std::string TEST_NAME = "basic_func";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
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
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleLogClusterBasicFunc, submit_log)
{
  SET_CASE_LOG_FILE(TEST_NAME, "submit_log");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t create_ts = 100;
  share::SCN create_scn;
  create_scn.convert_for_logservice(create_ts);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ObTimeGuard guard("submit_log", 0);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
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

// test_max_padding_size: 测试padding entry最长的场景(2M+16K+88+4K-1B).
TEST_F(TestObSimpleLogClusterBasicFunc, test_max_padding_size)
{
  SET_CASE_LOG_FILE(TEST_NAME, "submit_log");
  //OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t create_ts = 100;
  share::SCN create_scn;
  create_scn.convert_for_logservice(create_ts);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ObTimeGuard guard("submit_log", 0);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
  guard.click("create");
  int64_t follower_1_idx = (leader_idx + 1) % ObSimpleLogClusterTestBase::member_cnt_;
  int64_t follower_2_idx = (leader_idx + 2) % ObSimpleLogClusterTestBase::member_cnt_;
  block_net(leader_idx, follower_1_idx);

  const int64_t group_entry_header_total_size = LogGroupEntryHeader::HEADER_SER_SIZE + LogEntryHeader::HEADER_SER_SIZE;
  const int64_t max_valid_group_entry_size = MAX_LOG_BODY_SIZE + group_entry_header_total_size;
  // padding entry size上限如下，预期不会达到该值，故最大值应该是该值减1Byte
  const int64_t max_padding_entry_size = max_valid_group_entry_size + CLOG_FILE_TAIL_PADDING_TRIGGER;
  // 测试写一个最大padding entry的场景
  // 首先写30条2MB的group log
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 30, leader_idx, (2 * 1024 * 1024 - group_entry_header_total_size)));
  // 接着写文件尾最后一条有效的group entry, 确保文件剩余空间触发生成最大的padding entry
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, (4 * 1024 * 1024 - MAX_INFO_BLOCK_SIZE - max_valid_group_entry_size - CLOG_FILE_TAIL_PADDING_TRIGGER - group_entry_header_total_size + 1)));
  // 提交一个2MB+16KB size的log buf, 触发生成padding
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, MAX_LOG_BODY_SIZE));
  // 预期max_lsn为单个block文件size+最大valid group entry size
  const LSN expected_max_lsn(PALF_BLOCK_SIZE + max_valid_group_entry_size);
  guard.click("submit_log for max size padding entry finish");
  CLOG_LOG(INFO, "leader submit log finished", K(expected_max_lsn));
  while (leader.palf_handle_impl_->get_end_lsn() < expected_max_lsn) {
    usleep(100 * 1000);
  }
  EXPECT_EQ(leader.palf_handle_impl_->get_end_lsn(), expected_max_lsn);
  guard.click("wait leader end_lsn finish");
  CLOG_LOG(INFO, "leader wait end_lsn finished", K(expected_max_lsn));

  unblock_net(leader_idx, follower_1_idx);
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  PalfHandleImplGuard &follower_2 = *palf_list[follower_2_idx];
  while (follower_2.palf_handle_impl_->get_end_lsn() < expected_max_lsn) {
    usleep(100 * 1000);
  }
  EXPECT_EQ(follower_2.palf_handle_impl_->get_end_lsn(), expected_max_lsn);
  guard.click("wait follower_2 end_lsn finish");
  CLOG_LOG(INFO, "follower_2 wait end_lsn finished", K(expected_max_lsn));

  PalfHandleImplGuard &follower_1 = *palf_list[follower_1_idx];
  // follower_1依赖fetch log追日志，但end_lsn无法推到与leader一致
  // 因为这里committed_end_lsn依赖周期性的keepAlive日志推进
  while (follower_1.palf_handle_impl_->get_max_lsn() < expected_max_lsn) {
    usleep(100 * 1000);
  }
  guard.click("wait follower_1 max_lsn finish");
  CLOG_LOG(INFO, "follower_1 wait max_lsn finished", K(expected_max_lsn));

  EXPECT_EQ(OB_SUCCESS, revert_cluster_palf_handle_guard(palf_list));
  guard.click("delete");
  PALF_LOG(INFO, "end test submit_log", K(id), K(guard));
}

bool check_locate_correct(const std::vector<LSN> &lsn_array,
                          const std::vector<SCN> &scn_array,
                          const share::SCN input_scn, const LSN result_lsn,
                          const bool later_less)
{
  bool find_match = false;
  for (int i = 0; i < lsn_array.size(); i++) {
    if (lsn_array[i].val_ - LogGroupEntryHeader::HEADER_SER_SIZE == result_lsn) {
      if (later_less) {
        EXPECT_LE(scn_array[i], input_scn);
      } else {
        EXPECT_GE(scn_array[i], input_scn);
      }
      find_match = true;
      break;
    }
    PALF_LOG(INFO, "check_locate_correct array", K(i), K(lsn_array[i]),
             K(scn_array[i]));
  }
  PALF_LOG(INFO, "check_locate_correct", K(find_match), K(result_lsn), K(input_scn));
  return find_match;
}

TEST_F(TestObSimpleLogClusterBasicFunc, test_locate_by_scn_coarsely)
{
  SET_CASE_LOG_FILE(TEST_NAME, "locate_by_scn");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "test test_locate_by_scn_coarsely", K(id));
  int64_t leader_idx = 0;
  int ret = OB_SUCCESS;
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  PalfHandleImplGuard leader;
  LSN result_lsn;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  // no log
  share::SCN invalid_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->locate_by_scn_coarsely(invalid_scn, result_lsn));
  share::SCN scn_cur;
  scn_cur.convert_for_logservice(ObTimeUtility::current_time_ns());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, leader.palf_handle_impl_->locate_by_scn_coarsely(
                                    scn_cur, result_lsn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 256, leader_idx, lsn_array, scn_array));
  sleep(1);
  SCN ref_scn;
  ref_scn.convert_for_tx(10);
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, leader.palf_handle_impl_->locate_by_scn_coarsely(ref_scn, result_lsn));
  // contains log
  int64_t input_ts_ns = ObTimeUtility::current_time_ns();
  share::SCN input_scn;
  input_scn.convert_for_logservice(input_ts_ns);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->locate_by_scn_coarsely(input_scn, result_lsn));
  EXPECT_TRUE(
      check_locate_correct(lsn_array, scn_array, input_scn, result_lsn, true));
  input_ts_ns += 1000 * 1000 * 1000 * 1000L;
  input_scn.convert_for_logservice(input_ts_ns);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->locate_by_scn_coarsely(input_scn, result_lsn));
  EXPECT_TRUE(
      check_locate_correct(lsn_array, scn_array, input_scn, result_lsn, true));

  ObTimeGuard guard("mittest", 0);
  // write too much log, do not submit to farm
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 6000, leader_idx, lsn_array, scn_array));
  const int64_t min_ts = scn_array.front().get_val_for_logservice();
  const int64_t max_ts = scn_array.back().get_val_for_logservice();
  for (int i = 0; i < 10; i++) {
    const int64_t this_ts = ObRandom::rand(min_ts, max_ts);
    share::SCN this_scn;
    this_scn.convert_for_logservice(this_ts);
    guard.click();
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->locate_by_scn_coarsely(this_scn, result_lsn));
    guard.click();
    EXPECT_TRUE(
        check_locate_correct(lsn_array, scn_array, input_scn, result_lsn, true));
  }
  PALF_LOG(INFO, "end test_locate_by_scn_coarsely", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, test_locate_by_lsn_coarsely)
{
  SET_CASE_LOG_FILE(TEST_NAME, "locate_by_lsn");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "test test_locate_by_lsn_coarsely", K(id));
  int64_t leader_idx = 0;
  int ret = OB_SUCCESS;
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  PalfHandleImplGuard leader;
  share::SCN result_scn;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  // no log
  EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->locate_by_lsn_coarsely(LSN(), result_scn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, leader_idx, lsn_array, scn_array));
  sleep(1);

  LSN input_lsn;
  input_lsn.val_ = 0;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->locate_by_lsn_coarsely(input_lsn, result_scn));
  EXPECT_TRUE(check_locate_correct(lsn_array, scn_array, result_scn, input_lsn, false));

  EXPECT_EQ(OB_SUCCESS,
            submit_log(leader, 65, 1024 * 1024, leader_idx, lsn_array, scn_array));

  input_lsn.val_ = 1 * PALF_BLOCK_SIZE;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->locate_by_lsn_coarsely(input_lsn, result_scn));
  EXPECT_TRUE(check_locate_correct(lsn_array, scn_array, result_scn, input_lsn, false)) ;

  PALF_LOG(INFO, "end test_locate_by_lsn_coarsely", K(id));
}

TEST_F(TestObSimpleLogClusterBasicFunc, test_switch_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test_switch_leader", K(id));
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1024, id));

  // switch leader
  leader_idx = 1;
  PalfHandleImplGuard new_leader1;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, new_leader1));
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader1, 1024, id));

  // switch leader
  PalfHandleImplGuard new_leader2;
  leader_idx = 2;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, new_leader2));
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader2, 1024, id));

  // switch leader
  PalfHandleImplGuard new_leader3;
  leader_idx = 1;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, new_leader3));
  // block the other two followers
  block_net(1, 0);
  block_net(1, 2);
  // leader submit new logs
  // PalfHandleImplGuard new_leader4;
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

  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.truncate_prefix_blocks(LSN(0)));
  guard.click("create");
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  guard.click("submit");
  guard.click("get_leader");
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(0)));
  guard.click("advance_base_lsn");

  PALF_LOG(INFO, "end test advance_base_lsn", K(id), K(guard));
}

TEST_F(TestObSimpleLogClusterBasicFunc, data_corrupted)
{
  SET_CASE_LOG_FILE(TEST_NAME, "data_corrupted");
  ObTimeGuard guard("data_corrupted", 0);
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start advance_base_lsn", K(id));
  int64_t leader_idx = 0;

  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  guard.click("create");
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(3);
  guard.click("submit");
  guard.click("get_leader");
  PalfGroupBufferIterator iterator;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(LSN(0), iterator));
  LogGroupEntry entry;
  LSN lsn;
  EXPECT_EQ(OB_SUCCESS, iterator.next());
  EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, lsn));
  char *buf = static_cast<char*>(ob_malloc_align(LOG_DIO_ALIGN_SIZE, MAX_LOG_BUFFER_SIZE,
                                                 ObMemAttr(OB_SERVER_TENANT_ID, ObNewModIds::TEST)));
  int64_t pos = 0;
  LogWriteBuf write_buf;
  write_buf.push_back(buf, MAX_LOG_BUFFER_SIZE);
  EXPECT_EQ(OB_SUCCESS, entry.serialize(buf, MAX_LOG_BUFFER_SIZE, pos));
  char *group_size_buf = buf + 4;
  *group_size_buf = 1;
  LSN origin = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  share::SCN new_scn;
  new_scn.convert_for_logservice(leader.palf_handle_impl_->get_max_scn().get_val_for_logservice() + 1);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.
      append_log(leader.palf_handle_impl_->get_max_lsn(), write_buf, new_scn));
  PalfGroupBufferIterator iterator1;
  auto get_file_end_lsn = [](){
    return LSN(LOG_MAX_LSN_VAL);
  };
  EXPECT_EQ(OB_SUCCESS, iterator1.init(origin, get_file_end_lsn, &leader.palf_handle_impl_->log_engine_.log_storage_));
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
  share::SCN create_scn = share::SCN::base_scn();
	{
    PalfHandleImplGuard leader1;
    PalfHandleImplGuard leader2;
    PalfHandleImplGuard leader3;
    EXPECT_EQ(OB_SUCCESS, get_palf_env(server_idx, palf_env));
		int64_t id3 = palf_id_ + 1;
    palf_env->palf_env_impl_.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = 2 * MIN_DISK_SIZE_PER_PALF_INSTANCE;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id1, create_scn, leader_idx, leader1));
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id2, create_scn, leader_idx, leader2));
    EXPECT_EQ(OB_LOG_OUTOF_DISK_SPACE, create_paxos_group(id3, create_scn, leader_idx, leader3));
	}
	EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id1));
}

TEST_F(TestObSimpleLogClusterBasicFunc, submit_group_log)
{
  SET_CASE_LOG_FILE(TEST_NAME, "submit_group_log");
  ObTimeGuard guard("submit_group_log", 0);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start submit_group_log", K(id));
  int64_t leader_idx = 0;

  PalfHandleImplGuard leader;
  PalfHandleImplGuard leader_raw_write;
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
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  LogIOWorker *iow = leader.palf_handle_impl_->log_engine_.log_io_worker_;

  iow->batch_io_task_mgr_.handle_count_ = 0;
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  int64_t lag_follower_idx = (leader_idx + 1) % node_cnt_;
  PalfHandleImplGuard &lag_follower = *palf_list[lag_follower_idx];
  block_net(leader_idx, lag_follower_idx);
  block_net(lag_follower_idx, leader_idx);

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10000, leader_idx, 120));
  const LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
  wait_lsn_until_flushed(max_lsn, leader);
  const int64_t handle_count = iow->batch_io_task_mgr_.handle_count_;
  const int64_t log_id = leader.palf_handle_impl_->sw_.get_max_log_id();

  unblock_net(leader_idx, lag_follower_idx);
  unblock_net(lag_follower_idx, leader_idx);

  int64_t start_ts = ObTimeUtility::current_time();
  LSN lag_follower_max_lsn = lag_follower.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  while (lag_follower_max_lsn < max_lsn) {
    sleep(1);
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "follower is lagged", K(max_lsn), K(lag_follower_max_lsn));
    lag_follower_max_lsn = lag_follower.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  }
  LogIOWorker *iow_follower = lag_follower.palf_handle_impl_->log_engine_.log_io_worker_;
  const int64_t follower_handle_count = iow_follower->batch_io_task_mgr_.handle_count_;
  EXPECT_EQ(OB_SUCCESS, revert_cluster_palf_handle_guard(palf_list));

  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
}

TEST_F(TestObSimpleLogClusterBasicFunc, create_palf_via_middle_lsn)
{
  SET_CASE_LOG_FILE(TEST_NAME, "create_palf_via_middle_lsn");
  const int64_t prev_log_id = 1000;
  const LSN mid_lsn(palf::PALF_BLOCK_SIZE);
  const LSN prev_lsn(palf::PALF_BLOCK_SIZE - 100);
  const int64_t prev_log_ts = common::ObTimeUtility::current_time_ns();
  share::SCN prev_scn;
  prev_scn.convert_for_logservice(prev_log_ts);
  const int64_t prev_log_pid = 100;
  PalfBaseInfo palf_base_info;
  palf_base_info.curr_lsn_ = mid_lsn;
  palf_base_info.prev_log_info_.log_id_ = prev_log_id;
  palf_base_info.prev_log_info_.lsn_ = prev_lsn;
  palf_base_info.prev_log_info_.scn_ = prev_scn;
  palf_base_info.prev_log_info_.log_proposal_id_ = prev_log_pid;
  palf_base_info.prev_log_info_.accum_checksum_ = 1;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, palf_base_info, leader_idx, leader));
  }

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard leader;
    int64_t leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, leader_idx, 1024*1024));
    const LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
    const int64_t proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
    const LSN begin_lsn = leader.palf_handle_impl_->log_engine_.get_begin_lsn();
    share::SCN begin_scn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_scn(begin_scn));
    // check LogInfo
    EXPECT_GT(proposal_id, prev_log_pid);
    EXPECT_GT(begin_scn, prev_scn);
    EXPECT_EQ(begin_lsn, mid_lsn);
    // wait_lsn_until_flushed(max_lsn, leader);
    wait_until_has_committed(leader, max_lsn);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, 10*1024));
  }

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard leader;
    int64_t leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, leader_idx, 1024*1024));
    const LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
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
