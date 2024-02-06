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

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#include <stdexcept>
#define private public
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#undef protected
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_worker.h"
#include "logservice/palf/lsn.h"

const std::string TEST_NAME = "single_replica";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterSingleReplica : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterSingleReplica() : ObSimpleLogClusterTestEnv()
  {
    int ret = init();
    if (OB_SUCCESS != ret) {
      throw std::runtime_error("TestObSimpleLogClusterLogEngine init failed");
    }
  }
  ~TestObSimpleLogClusterSingleReplica()
  {
    destroy();
  }
  int init()
  {
    return OB_SUCCESS;
  }
  void destroy()
  {}
  int64_t id_;
  PalfHandleImplGuard leader_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
constexpr int64_t timeout_ts_us = 3 * 1000 * 1000;

void read_padding_entry(PalfHandleImplGuard &leader, SCN padding_scn, LSN padding_log_lsn)
{
  // 从padding group entry开始读取
  {
    PalfBufferIterator iterator;
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->alloc_palf_buffer_iterator(padding_log_lsn, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    LogEntry padding_log_entry;
    LSN check_lsn;
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(padding_log_entry, check_lsn));
    EXPECT_EQ(true, padding_log_entry.header_.is_padding_log_());
    EXPECT_EQ(true, padding_log_entry.check_integrity());
    EXPECT_EQ(padding_scn, padding_log_entry.get_scn());
  }
  // 从padding log entry开始读取
  {
    PalfBufferIterator iterator;
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->alloc_palf_buffer_iterator(padding_log_lsn+LogGroupEntryHeader::HEADER_SER_SIZE, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    LogEntry padding_log_entry;
    LSN check_lsn;
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(padding_log_entry, check_lsn));
    EXPECT_EQ(true, padding_log_entry.header_.is_padding_log_());
    EXPECT_EQ(true, padding_log_entry.check_integrity());
    EXPECT_EQ(padding_scn, padding_log_entry.get_scn());
  }

}

TEST_F(TestObSimpleLogClusterSingleReplica, delete_paxos_group)
{
  update_server_log_disk(10*1024*1024*1024ul);
  update_disk_options(10*1024*1024*1024ul/palf::PALF_PHY_BLOCK_SIZE);
  SET_CASE_LOG_FILE(TEST_NAME, "delete_paxos_group");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test delete_paxos_group", K(id));
  int64_t leader_idx = 0;
  {
    unittest::PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx));
  }
  sleep(1);
  // EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  // TODO by yunlong: check log sync
  PALF_LOG(INFO, "end test delete_paxos_group", K(id));
}

TEST_F(TestObSimpleLogClusterSingleReplica, single_replica_flashback)
{
  SET_CASE_LOG_FILE(TEST_NAME, "single_replica_flashback");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PALF_LOG(INFO, "start test single replica flashback", K(id));
  SCN max_scn;
  unittest::PalfHandleImplGuard leader;
  int64_t mode_version = INVALID_PROPOSAL_ID;
  SCN ref_scn;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  {
    SCN tmp_scn;
    LSN tmp_lsn;
    // 提交1条日志后进行flashback
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 100));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn()));
    tmp_scn = leader.palf_handle_impl_->get_max_scn();
    switch_append_to_flashback(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::minus(tmp_scn, 10), timeout_ts_us));
    // 预期日志起点为LSN(0)
    EXPECT_EQ(LSN(0), leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(SCN::minus(tmp_scn, 10), leader.palf_handle_impl_->get_max_scn());
    EXPECT_EQ(LSN(0), leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_);

    // flashback到PADDING日志
    switch_flashback_to_append(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, leader_idx, MAX_LOG_BODY_SIZE));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn()));
    EXPECT_EQ(OB_ITER_END, read_log(leader));
    EXPECT_GT(LSN(PALF_BLOCK_SIZE), leader.palf_handle_impl_->sw_.get_max_lsn());
    int remained_log_size = LSN(PALF_BLOCK_SIZE) - leader.palf_handle_impl_->sw_.get_max_lsn();
    EXPECT_LT(remained_log_size, MAX_LOG_BODY_SIZE);
    int need_log_size = remained_log_size - 5*1024;
    PALF_LOG(INFO, "runlin trace print sw1", K(leader.palf_handle_impl_->sw_));
    // 保证末尾只剩小于1KB的空间
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, need_log_size));
    PALF_LOG(INFO, "runlin trace print sw2", K(leader.palf_handle_impl_->sw_));
    SCN mid_scn;
    LogEntryHeader header;
    // 此时一共存在32条日志
    EXPECT_EQ(OB_SUCCESS, get_middle_scn(32, leader, mid_scn, header));
    EXPECT_EQ(OB_ITER_END, get_middle_scn(33, leader, mid_scn, header));
    EXPECT_GT(LSN(PALF_BLOCK_SIZE), leader.palf_handle_impl_->sw_.get_max_lsn());
    remained_log_size = LSN(PALF_BLOCK_SIZE) - leader.palf_handle_impl_->sw_.get_max_lsn();
    EXPECT_LT(remained_log_size, 5*1024);
    EXPECT_GT(remained_log_size, 0);
    // 写一条大小为5KB的日志
    LSN padding_log_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 5*1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
    // 验证读取padding是否成功
    {
       share::SCN padding_scn = leader.get_palf_handle_impl()->get_max_scn();
       padding_scn = padding_scn.minus(padding_scn, 1);
       read_padding_entry(leader, padding_scn, padding_log_lsn);
    }
    PALF_LOG(INFO, "runlin trace print sw3", K(leader.palf_handle_impl_->sw_));
    // Padding日志占用日志条数，因此存在34条日志
    EXPECT_EQ(OB_SUCCESS, get_middle_scn(33, leader, mid_scn, header));
    EXPECT_EQ(OB_SUCCESS, get_middle_scn(34, leader, mid_scn, header));
    EXPECT_EQ(OB_ITER_END, get_middle_scn(35, leader, mid_scn, header));
    EXPECT_LT(LSN(PALF_BLOCK_SIZE), leader.palf_handle_impl_->sw_.get_max_lsn());
    max_scn = leader.palf_handle_impl_->sw_.get_max_scn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    switch_append_to_flashback(leader, mode_version);
    // flashback到padding日志尾部
    tmp_scn = leader.palf_handle_impl_->get_max_scn();
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::minus(tmp_scn, 1), timeout_ts_us));
    PALF_LOG(INFO, "flashback to padding tail");
    EXPECT_EQ(leader.palf_handle_impl_->get_max_lsn(), LSN(PALF_BLOCK_SIZE));
    EXPECT_EQ(OB_ITER_END, read_log(leader));
    // flashback后存在33条日志(包含padding日志)
    EXPECT_EQ(OB_SUCCESS, get_middle_scn(33, leader, mid_scn, header));
    EXPECT_EQ(OB_ITER_END, get_middle_scn(34, leader, mid_scn, header));

    // 验证读取padding是否成功
    {
       share::SCN padding_scn = leader.get_palf_handle_impl()->get_max_scn();
       padding_scn.minus(padding_scn, 1);
       PALF_LOG(INFO, "begin read_padding_entry", K(padding_scn), K(padding_log_lsn));
       read_padding_entry(leader, padding_scn, padding_log_lsn);
    }

    // flashback到padding日志头部，磁盘上还有32条日志
    tmp_scn = leader.palf_handle_impl_->get_max_scn();
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::minus(tmp_scn, 1), timeout_ts_us));
    EXPECT_LT(leader.palf_handle_impl_->get_max_lsn(), LSN(PALF_BLOCK_SIZE));
    EXPECT_EQ(OB_SUCCESS, get_middle_scn(32, leader, mid_scn, header));
    EXPECT_EQ(OB_ITER_END, get_middle_scn(33, leader, mid_scn, header));
    EXPECT_EQ(padding_log_lsn, leader.palf_handle_impl_->get_max_lsn());
    switch_flashback_to_append(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, leader_idx, 1000));
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(LSN(PALF_BLOCK_SIZE), leader));
    EXPECT_EQ(OB_ITER_END, read_log(leader));

    switch_append_to_flashback(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::min_scn(), timeout_ts_us));
    EXPECT_EQ(LSN(0), leader.palf_handle_impl_->get_max_lsn());
    switch_flashback_to_append(leader, mode_version);

    ref_scn.convert_for_tx(10000);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, ref_scn, tmp_lsn, tmp_scn));
    LSN tmp_lsn1 = leader.palf_handle_impl_->get_max_lsn();
    ref_scn.convert_for_tx(50000);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, ref_scn, tmp_lsn, tmp_scn));
    sleep(1);
    wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
    switch_append_to_flashback(leader, mode_version);
    ref_scn.convert_for_tx(30000);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, ref_scn, timeout_ts_us));
    // 验证重复的flashback任务
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->inner_flashback(ref_scn));
    EXPECT_EQ(tmp_lsn1, leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
    // 验证flashback时间戳比过小
    ref_scn.convert_from_ts(1);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->inner_flashback(ref_scn));
    EXPECT_GT(tmp_lsn1, leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
    CLOG_LOG(INFO, "runlin trace 3");
  }
  switch_flashback_to_append(leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 300, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));

  // flashback到中间某条日志
	// 1. 比较log_storage和日位点和滑动窗口是否相同

  switch_append_to_flashback(leader, mode_version);
  LogEntryHeader header_origin;
	EXPECT_EQ(OB_SUCCESS, get_middle_scn(200, leader, max_scn, header_origin));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  LogEntryHeader header_new;
  SCN new_scn;
	EXPECT_EQ(OB_SUCCESS, get_middle_scn(200, leader, new_scn, header_new));
  EXPECT_EQ(new_scn, max_scn);
  EXPECT_EQ(header_origin.data_checksum_, header_origin.data_checksum_);
  switch_flashback_to_append(leader, mode_version);
  LSN new_log_tail = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail, leader.palf_handle_impl_->sw_.committed_end_lsn_);
  EXPECT_EQ(max_scn, leader.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
	// 验证flashback后能否继续提交日志
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 500, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));

  // 再次执行flashback到上一次的flashback位点
  switch_append_to_flashback(leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  switch_flashback_to_append(leader, mode_version);
  EXPECT_EQ(new_log_tail, leader.palf_handle_impl_->sw_.committed_end_lsn_);
  EXPECT_EQ(max_scn, leader.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 500, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));

  // 再次执行flashback到上一次的flashback后提交日志的某个时间点
	EXPECT_EQ(OB_SUCCESS, get_middle_scn(634, leader, max_scn, header_origin));
  switch_append_to_flashback(leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  switch_flashback_to_append(leader, mode_version);
  new_log_tail = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(max_scn, leader.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 300, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  PALF_LOG(INFO, "flashback to middle success");

  // flashback到某个更大的时间点
  max_scn = leader.palf_handle_impl_->get_end_scn();
  new_log_tail = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  switch_append_to_flashback(leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::plus(max_scn, 1000000000000), timeout_ts_us));
  switch_flashback_to_append(leader, mode_version);
  new_log_tail = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail.val_, leader.palf_handle_impl_->sw_.committed_end_lsn_.val_);
  EXPECT_EQ(max_scn, leader.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  PALF_LOG(INFO, "flashback to greater success");

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 300, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  new_log_tail = leader.palf_handle_impl_->get_max_lsn();
  max_scn = leader.palf_handle_impl_->get_max_scn();
  PALF_LOG(INFO, "runlin trace 3", K(new_log_tail), K(max_scn));
  switch_append_to_flashback(leader, mode_version);
  LSN new_log_tail_1 = leader.palf_handle_impl_->get_end_lsn();
  SCN max_scn1 = leader.palf_handle_impl_->get_end_scn();
  PALF_LOG(INFO, "runlin trace 4", K(new_log_tail), K(max_scn), K(new_log_tail_1), K(max_scn1));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  LSN log_tail_after_flashback = leader.palf_handle_impl_->get_end_lsn();
  SCN max_ts_after_flashback = leader.palf_handle_impl_->get_end_scn();
  PALF_LOG(INFO, "runlin trace 5", K(log_tail_after_flashback), K(max_ts_after_flashback));
  switch_flashback_to_append(leader, mode_version);
  EXPECT_EQ(new_log_tail, leader.palf_handle_impl_->sw_.committed_end_lsn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  PALF_LOG(INFO, "flashback to max_scn success");

  // 再次执行flashback到提交日志前的max_scn
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 300, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  LSN curr_lsn = leader.palf_handle_impl_->get_end_lsn();
  EXPECT_NE(curr_lsn, new_log_tail);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  switch_append_to_flashback(leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  switch_flashback_to_append(leader, mode_version);
  EXPECT_EQ(new_log_tail, leader.palf_handle_impl_->get_end_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));

  // flashback reconfirming leader
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  SCN flashback_scn = leader.palf_handle_impl_->get_max_scn();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx));
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  switch_append_to_flashback(leader, mode_version);

  dynamic_cast<palf::PalfEnvImpl*>(get_cluster()[0]->get_palf_env())->log_loop_thread_.stop();
  dynamic_cast<palf::PalfEnvImpl*>(get_cluster()[0]->get_palf_env())->log_loop_thread_.wait();
  leader.palf_handle_impl_->state_mgr_.role_ = LEADER;
  leader.palf_handle_impl_->state_mgr_.state_ = RECONFIRM;

  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  EXPECT_GT(leader.palf_handle_impl_->sw_.get_max_scn(), flashback_scn);

  leader.palf_handle_impl_->state_mgr_.role_ = FOLLOWER;
  leader.palf_handle_impl_->state_mgr_.state_ = ACTIVE;

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  EXPECT_LT(leader.palf_handle_impl_->sw_.get_max_scn(), flashback_scn);

  EXPECT_EQ(new_log_tail, leader.palf_handle_impl_->get_end_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  leader.palf_handle_impl_->state_mgr_.role_ = LEADER;
  leader.palf_handle_impl_->state_mgr_.state_ = ACTIVE;
  dynamic_cast<palf::PalfEnvImpl*>(get_cluster()[0]->get_palf_env())->log_loop_thread_.start();
  switch_flashback_to_append(leader, mode_version);

  // 数据全部清空
  wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
  switch_append_to_flashback(leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::min_scn(), timeout_ts_us));
  EXPECT_EQ(LSN(0), leader.palf_handle_impl_->get_max_lsn());
  EXPECT_EQ(SCN::min_scn(), leader.palf_handle_impl_->get_max_scn());
  switch_flashback_to_append(leader, mode_version);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  PALF_LOG(INFO, "flashback to 0 success");
  leader.reset();
  delete_paxos_group(id);

}

TEST_F(TestObSimpleLogClusterSingleReplica, single_replica_flashback_restart)
{
  SET_CASE_LOG_FILE(TEST_NAME, "single_replica_flashback_restart");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  SCN max_scn = SCN::min_scn();
  SCN ref_scn;
  int64_t mode_version = INVALID_PROPOSAL_ID;
  {
    unittest::PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1000, leader_idx, 1000));
    LogEntryHeader header_origin;
		EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
		EXPECT_EQ(OB_SUCCESS, get_middle_scn(323, leader, max_scn, header_origin));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, 1000));
		wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
    EXPECT_EQ(OB_ITER_END, read_log(leader));
    switch_append_to_flashback(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
    LogEntryHeader header_new;
    SCN new_scn;
		EXPECT_EQ(OB_SUCCESS, get_middle_scn(323, leader, new_scn, header_new));
    EXPECT_EQ(new_scn, max_scn);
    EXPECT_EQ(header_origin.data_checksum_, header_new.data_checksum_);
		EXPECT_EQ(OB_ITER_END, get_middle_scn(324, leader, new_scn, header_new));
    switch_flashback_to_append(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1000, leader_idx, 1000));
		wait_until_has_committed(leader, leader.palf_handle_impl_->sw_.get_max_lsn());
		EXPECT_EQ(OB_SUCCESS, get_middle_scn(1323, leader, new_scn, header_new));
		EXPECT_EQ(OB_ITER_END, get_middle_scn(1324, leader, new_scn, header_new));
    EXPECT_EQ(OB_ITER_END, read_log(leader));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
  // 验证重启场景
  PalfHandleImplGuard new_leader;
  int64_t curr_mode_version = INVALID_PROPOSAL_ID;
  AccessMode curr_access_mode = AccessMode::INVALID_ACCESS_MODE;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, leader_idx));
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_access_mode(curr_mode_version, curr_access_mode));
  EXPECT_EQ(curr_mode_version, mode_version);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 1000, leader_idx, 1000));
	wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(new_leader));
    ref_scn.convert_for_tx(1000);
  LogEntryHeader header_new;
  LogStorage *log_storage = &new_leader.palf_handle_impl_->log_engine_.log_storage_;
  block_id_t max_block_id = log_storage->block_mgr_.max_block_id_;
	EXPECT_EQ(OB_SUCCESS, get_middle_scn(1329, new_leader, max_scn, header_new));
  // flashback跨文件场景重启
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 33, leader_idx, MAX_LOG_BODY_SIZE));
	wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_LE(max_block_id, log_storage->block_mgr_.max_block_id_);
  switch_append_to_flashback(new_leader, mode_version);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->flashback(mode_version, max_scn, timeout_ts_us));
  EXPECT_GE(max_block_id, log_storage->block_mgr_.max_block_id_);
  switch_flashback_to_append(new_leader, mode_version);
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
  PalfHandleImplGuard new_leader;
  int64_t curr_mode_version = INVALID_PROPOSAL_ID;
  AccessMode curr_access_mode = AccessMode::INVALID_ACCESS_MODE;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, leader_idx));
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_access_mode(curr_mode_version, curr_access_mode));

  // flashback到某个文件的尾部
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 65, leader_idx, MAX_LOG_BODY_SIZE));
	wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->sw_.get_max_lsn());
  switch_append_to_flashback(new_leader, mode_version);
  LSN lsn(PALF_BLOCK_SIZE);
  LogStorage *log_storage = &new_leader.palf_handle_impl_->log_engine_.log_storage_;
  SCN block_end_scn;
  {
    PalfGroupBufferIterator iterator;
    auto get_file_end_lsn = [](){
      return LSN(PALF_BLOCK_SIZE);
    };
    EXPECT_EQ(OB_SUCCESS, iterator.init(LSN(0), get_file_end_lsn, log_storage));
    LogGroupEntry entry;
    LSN lsn;
    while (OB_SUCCESS == iterator.next()) {
      EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, lsn));
    }
    block_end_scn = entry.get_scn();
  }
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->flashback(mode_version, block_end_scn, timeout_ts_us));
  EXPECT_EQ(lsn, log_storage->log_tail_);
  EXPECT_EQ(OB_ITER_END, read_log(new_leader));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  // 重启后继续提交日志
  {
    PalfHandleImplGuard new_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, leader_idx));
    switch_flashback_to_append(new_leader, mode_version);
    EXPECT_EQ(true, 0 == lsn_2_offset(new_leader.get_palf_handle_impl()->get_max_lsn(), PALF_BLOCK_SIZE));
    share::SCN padding_scn = new_leader.get_palf_handle_impl()->get_max_scn();
    EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, leader_idx));
	  wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->sw_.get_max_lsn());
    EXPECT_EQ(OB_ITER_END, read_log(new_leader));
    switch_append_to_flashback(new_leader, mode_version);
    // flashback到padding日志头后重启
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->flashback(mode_version, padding_scn.minus(padding_scn, 1), timeout_ts_us));
    EXPECT_EQ(true, 0 != lsn_2_offset(new_leader.get_palf_handle_impl()->get_max_lsn(), PALF_BLOCK_SIZE));
    new_leader.reset();
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  // 重启提交日志，不产生padding日志
  {
    PalfHandleImplGuard new_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, leader_idx));
    LSN padding_start_lsn = new_leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(true, 0 != lsn_2_offset(new_leader.get_palf_handle_impl()->get_max_lsn(), PALF_BLOCK_SIZE));
    const int64_t remained_size = PALF_BLOCK_SIZE - lsn_2_offset(new_leader.get_palf_handle_impl()->get_max_lsn(), PALF_BLOCK_SIZE);
    EXPECT_GE(remained_size, 0);
    const int64_t group_entry_body_size = remained_size - LogGroupEntryHeader::HEADER_SER_SIZE - LogEntryHeader::HEADER_SER_SIZE;
    PALF_LOG(INFO, "runlin trace print remained_size", K(remained_size), K(group_entry_body_size));
    switch_flashback_to_append(new_leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 1, leader_idx, group_entry_body_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_leader, new_leader.get_palf_handle_impl()->get_max_lsn()));
    PalfBufferIterator iterator;
    EXPECT_EQ(OB_SUCCESS, new_leader.get_palf_handle_impl()->alloc_palf_buffer_iterator(padding_start_lsn, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    LogEntry log_entry;
    LSN check_lsn;
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(log_entry, check_lsn));
    EXPECT_EQ(check_lsn, padding_start_lsn + LogGroupEntryHeader::HEADER_SER_SIZE);
    EXPECT_EQ(false, log_entry.header_.is_padding_log_());
    EXPECT_EQ(true, log_entry.check_integrity());
    new_leader.reset();
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  // 重启后继续提交日志
  {
    PalfHandleImplGuard new_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, leader_idx));
    EXPECT_EQ(true, 0 == lsn_2_offset(new_leader.get_palf_handle_impl()->get_max_lsn(), PALF_BLOCK_SIZE));
    EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, leader_idx, 1000));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_leader, new_leader.get_palf_handle_impl()->get_max_lsn()));
    EXPECT_EQ(OB_ITER_END, read_log(new_leader));
  }
  delete_paxos_group(id);
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_truncate_failed)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_truncate_failed");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  int64_t file_size = 0;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1000));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    LSN max_lsn = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1000));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    int64_t fd = leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.curr_writable_handler_.io_fd_;
    block_id_t block_id = leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.curr_writable_block_id_;
    char *log_dir = leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_;
    convert_to_normal_block(log_dir, block_id, block_path, OB_MAX_FILE_NAME_LENGTH);
    EXPECT_EQ(OB_ITER_END, read_log(leader));
    PALF_LOG_RET(ERROR, OB_SUCCESS, "truncate pos", K(max_lsn));
    EXPECT_EQ(0, ftruncate(fd, max_lsn.val_+MAX_INFO_BLOCK_SIZE));
    FileDirectoryUtils::get_file_size(block_path, file_size);
    EXPECT_EQ(file_size, max_lsn.val_+MAX_INFO_BLOCK_SIZE);
  }
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());;
  FileDirectoryUtils::get_file_size(block_path, file_size);
  EXPECT_EQ(file_size, PALF_PHY_BLOCK_SIZE);
  get_leader(id, leader, leader_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 32, id, MAX_LOG_BODY_SIZE));
  wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  // 验证truncate文件尾后重启
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.truncate(LSN(PALF_BLOCK_SIZE)));
  EXPECT_EQ(LSN(PALF_BLOCK_SIZE), leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_meta)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_meta");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  LSN upper_aligned_log_tail;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    sleep(1);
    // 测试meta文件刚好写满的重启场景
    LogEngine *log_engine = &leader.palf_handle_impl_->log_engine_;
    LogStorage *log_meta_storage = &log_engine->log_meta_storage_;
    LSN log_meta_tail = log_meta_storage->log_tail_;
    upper_aligned_log_tail.val_ = (lsn_2_block(log_meta_tail, PALF_META_BLOCK_SIZE) + 1) * PALF_META_BLOCK_SIZE;
    int64_t delta = upper_aligned_log_tail - log_meta_tail;
    int64_t delta_cnt = delta / MAX_META_ENTRY_SIZE;
    while (delta_cnt-- > 0) {
      log_engine->append_log_meta_(log_engine->log_meta_);
    }
    EXPECT_EQ(upper_aligned_log_tail, log_meta_storage->log_tail_);
    PALF_LOG_RET(ERROR, OB_SUCCESS, "runlin trace before restart", K(upper_aligned_log_tail), KPC(log_meta_storage));
  }

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    LogEngine *log_engine = &leader.palf_handle_impl_->log_engine_;
    LogStorage *log_meta_storage = &log_engine->log_meta_storage_;
    LSN log_meta_tail = log_meta_storage->log_tail_;
    upper_aligned_log_tail.val_ = (lsn_2_block(log_meta_tail, PALF_META_BLOCK_SIZE) + 1) * PALF_META_BLOCK_SIZE;
    int64_t delta = upper_aligned_log_tail - log_meta_tail;
    int64_t delta_cnt = delta / MAX_META_ENTRY_SIZE;
    while (delta_cnt-- > 0) {
      log_engine->append_log_meta_(log_engine->log_meta_);
    }
    EXPECT_EQ(upper_aligned_log_tail, log_meta_storage->log_tail_);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 32, id, MAX_LOG_BODY_SIZE));
    sleep(1);
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    block_id_t min_block_id, max_block_id;
    EXPECT_EQ(OB_SUCCESS, log_meta_storage->get_block_id_range(min_block_id, max_block_id));
    EXPECT_EQ(min_block_id, max_block_id);
  }
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_iterator)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_iterator");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t mode_version_v = 1;
  int64_t *mode_version = &mode_version_v;
  LSN end_lsn_v = LSN(100000000);
  LSN *end_lsn = &end_lsn_v;
  {
    SCN max_scn_case1, max_scn_case2, max_scn_case3;
    PalfHandleImplGuard leader;
    PalfHandleImplGuard raw_write_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    PalfHandleImpl *palf_handle_impl = leader.palf_handle_impl_;
    const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_raw_write, leader_idx, raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(raw_write_leader));
    int64_t count = 5;
    // 提交1024条日志，记录max_scn，用于后续next迭代验证，case1
    for (int i = 0; i < count; i++) {
      EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 4*1024));
      EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    }
    max_scn_case1 = palf_handle_impl->get_max_scn();
    // 提交5条日志，case1成功后，执行case2
    for (int i = 0; i < count; i++) {
      EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 4*1024));
      EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    }
    max_scn_case2 = palf_handle_impl->get_max_scn();

    // 提交5条日志, case3, 验证next(replayable_point_scn, &next_log_min_scn, &bool)
    std::vector<LSN> lsns;
    std::vector<SCN> logts;
    const int64_t log_size = 500;
    auto submit_log_private =[this](PalfHandleImplGuard &leader,
                              const int64_t count,
                              const int64_t id,
                              const int64_t wanted_data_size,
                              std::vector<LSN> &lsn_array,
                              std::vector<SCN> &scn_array)-> int{
      int ret = OB_SUCCESS;
      lsn_array.resize(count);
      scn_array.resize(count);
      for (int i = 0; i < count && OB_SUCC(ret); i++) {
        SCN ref_scn;
        ref_scn.convert_from_ts(ObTimeUtility::current_time() + 10000000);
        std::vector<LSN> tmp_lsn_array;
        std::vector<SCN> tmp_log_scn_array;
        if (OB_FAIL(submit_log_impl(leader, 1, id, wanted_data_size, ref_scn, tmp_lsn_array, tmp_log_scn_array))) {
        } else {
          lsn_array[i] = tmp_lsn_array[0];
          scn_array[i] = tmp_log_scn_array[0];
          wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
          CLOG_LOG(INFO, "submit_log_private success", K(i), "scn", tmp_log_scn_array[0], K(ref_scn));
        }
      }
      return ret;
    };
    EXPECT_EQ(OB_SUCCESS, submit_log_private(leader, count, id, log_size, lsns, logts));

    max_scn_case3 = palf_handle_impl->get_max_scn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, palf_handle_impl->get_end_lsn()));
    EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(leader, raw_write_leader));
    PalfHandleImpl *raw_write_palf_handle_impl = raw_write_leader.palf_handle_impl_;
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(raw_write_leader, raw_write_palf_handle_impl->get_end_lsn()));


    PalfBufferIterator iterator;
    auto get_file_end_lsn = [&end_lsn]() -> LSN { return *end_lsn; };
    auto get_mode_version = [&mode_version, &mode_version_v]() -> int64_t {
      PALF_LOG(INFO, "runlin trace", K(*mode_version), K(mode_version_v));
      return *mode_version;
    };
    EXPECT_EQ(OB_SUCCESS,
        iterator.init(LSN(0), get_file_end_lsn, get_mode_version, &raw_write_palf_handle_impl->log_engine_.log_storage_));
    EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn_case1)); count--;
    EXPECT_EQ(OB_ITER_END, iterator.next(SCN::base_scn()));

    // case0: 验证group iterator迭代日志功能
    EXPECT_EQ(OB_ITER_END, read_group_log(raw_write_leader, LSN(0)));

    LSN curr_lsn = iterator.get_curr_read_lsn();
    // case1:
    // - 验证mode_version变化后，cache是否清空
    // - replayable_point_scn是否生效
    // 当mode version发生变化时，预期cache应该清空
    // raw模式下，当replayable_point_scn很小时，直接返回OB_ITER_END
    PALF_LOG(INFO, "runlin trace case1", K(mode_version_v), K(*mode_version), K(max_scn_case1));
    // mode_version_v 为无效值时，预期不清空
    mode_version_v = INVALID_PROPOSAL_ID;
    end_lsn_v = curr_lsn;
    EXPECT_FALSE(curr_lsn == iterator.iterator_storage_.end_lsn_);
    EXPECT_FALSE(curr_lsn == iterator.iterator_storage_.start_lsn_);
    EXPECT_EQ(OB_ITER_END, iterator.next(SCN::base_scn()));

    //  mode_version_v 比inital_mode_version小，预期不清空
    mode_version_v = -1;
    EXPECT_FALSE(curr_lsn == iterator.iterator_storage_.end_lsn_);
    EXPECT_FALSE(curr_lsn == iterator.iterator_storage_.start_lsn_);
    EXPECT_EQ(OB_ITER_END, iterator.next(SCN::base_scn()));

    // 合理的mode_version_v，清空cache
    mode_version_v = 100;
    end_lsn_v = curr_lsn;
    EXPECT_EQ(OB_ITER_END, iterator.next(SCN::base_scn()));
    // cache清空，依赖上一次next操作
    EXPECT_EQ(curr_lsn, iterator.iterator_storage_.start_lsn_);
    EXPECT_EQ(curr_lsn, iterator.iterator_storage_.end_lsn_);

    PALF_LOG(INFO, "runlin trace", K(iterator), K(max_scn_case1), K(curr_lsn));

    end_lsn_v = LSN(1000000000);
    // 当replayable_point_scn为max_log_ts，预期max_log_ts前的日志可以吐出5条日志
    EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn_case1)); count--;
    while (count > 0) {
      EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn_case1));
      count--;
    }
    EXPECT_EQ(OB_ITER_END, iterator.next(max_scn_case1));
    // case2: next 功能是否正常
    // 尝试读取后续的5条日志
    count = 5;

    PALF_LOG(INFO, "runlin trace case2", K(iterator), K(max_scn_case2));
    while (count > 0) {
      EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn_case2));
      count--;
    }

    // 此时的curr_entry已经是第三次提交日志的第一条日志日志(first_log)
    // 由于该日志对应的时间戳比max_scn_case2大，因此不会吐出
    // NB: 这里测试时，遇到过以下情况:case3的第一次 next后的EXPECT_EQ：
    // curr_entry变为first_log后，在后续的测试中，尝试把file_end_lsn设置到
    // fisrt_log之前，然后出现了一种情况，此时调用next(fist_log_ts, next_log_min_scn)后,
    // next_log_min_scn被设置为first_scn+1，对外表现为：尽管存在first_log，但外部在
    // 没有看到first_log之前就已经next_log_min_scn一定大于first_scn
    //
    // 实际上，这种情况是不会出现的，因为file_end_lsn不会回退的
    EXPECT_EQ(OB_ITER_END, iterator.next(max_scn_case2));

    //case3: next(replayable_point_scn, &next_log_min_scn)
    PALF_LOG(INFO, "runlin trace case3", K(iterator), K(max_scn_case3), K(end_lsn_v), K(max_scn_case2));
    SCN first_scn = logts[0];
    // 在使用next(replayable_point_scn, &next_log_min_scn)接口时
    // 我们禁止使用LogEntry的头作为迭代器终点
    LSN first_log_start_lsn = lsns[0] - sizeof(LogGroupEntryHeader);
    LSN first_log_end_lsn = lsns[0]+log_size+sizeof(LogEntryHeader);
    SCN next_log_min_scn;
    bool iterate_end_by_replayable_point = false;
    count = 5;
    // 模拟提前达到文件终点, 没有读过新日志，因此next_log_min_scn为prev_entry_scn_+1
    end_lsn_v = first_log_start_lsn - 1;
    CLOG_LOG(INFO, "runlin trace 1", K(iterator), K(end_lsn_v), KPC(end_lsn), K(max_scn_case2), K(first_scn));
    EXPECT_EQ(OB_ITER_END, iterator.next(SCN::plus(first_scn, 10000), next_log_min_scn, iterate_end_by_replayable_point));
    // file_end_lsn尽管回退了，但curr_entry_已经没有被读取过, 因此next_log_min_scn依旧为first_scn
    EXPECT_EQ(SCN::plus(iterator.iterator_impl_.prev_entry_scn_, 1), next_log_min_scn);
    EXPECT_EQ(iterate_end_by_replayable_point, false);
    CLOG_LOG(INFO, "runlin trace 3.1", K(iterator), K(end_lsn_v), KPC(end_lsn));
    EXPECT_EQ(first_log_start_lsn,
    iterator.iterator_impl_.log_storage_->get_lsn(iterator.iterator_impl_.curr_read_pos_));

    // 读取一条日志成功，next_log_min_scn会被重置
    // curr_entry为fisrt_log_ts对应的log
    end_lsn_v = first_log_end_lsn;
    CLOG_LOG(INFO, "runlin trace 2", K(iterator), K(end_lsn_v), KPC(end_lsn));
    EXPECT_EQ(OB_SUCCESS, iterator.next(first_scn, next_log_min_scn, iterate_end_by_replayable_point)); count--;
    // iterator 返回成功，next_log_min_scn应该为OB_INVALID_TIMESTAMP
    EXPECT_EQ(next_log_min_scn.is_valid(), false);
    // iterator中的prev_entry_scn_被设置为first_scn
    EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, first_scn);

    CLOG_LOG(INFO, "runlin trace 3", K(iterator), K(end_lsn_v), KPC(end_lsn));
    {
      // 模拟提前达到文件终点, 此时文件终点为file_log_end_lsn
      // 预期next_log_min_scn为first_scn对应的日志+1
      SCN second_scn = logts[1];
      EXPECT_EQ(OB_ITER_END, iterator.next(second_scn, next_log_min_scn, iterate_end_by_replayable_point));
      // iterator返回OB_ITER_END，next_log_min_scn为first_scn+1
      EXPECT_EQ(next_log_min_scn, SCN::plus(first_scn, 1));
      EXPECT_EQ(iterate_end_by_replayable_point, false);
      CLOG_LOG(INFO, "runlin trace 3", K(iterator), K(end_lsn_v), KPC(end_lsn), K(first_scn), K(second_scn));
      // 再次调用next，预期next_log_min_scn依旧为first_scn+1
      EXPECT_EQ(OB_ITER_END, iterator.next(second_scn, next_log_min_scn, iterate_end_by_replayable_point));
      // iterator返回OB_ITER_END，next_log_min_scn为first_scn+1
      EXPECT_EQ(next_log_min_scn, SCN::plus(first_scn, 1));
    }

    CLOG_LOG(INFO, "runlin trace 4", K(iterator), K(end_lsn_v), KPC(end_lsn));
    SCN prev_next_success_scn;
    // 模拟到达replayable_point_scn，此时文件终点为second log, 预期next_log_min_scn为replayable_point_scn+1
    // 同时replayable_point_scn < 缓存的日志时间戳
    {
      SCN second_scn = logts[1];
      SCN replayable_point_scn = SCN::minus(second_scn, 1);
      end_lsn_v = lsns[1]+log_size+sizeof(LogEntryHeader);
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(iterate_end_by_replayable_point, true);
      // iterator返回OB_ITER_END，next_log_min_scn为replayable_point_scn + 1
      PALF_LOG(INFO, "runliun trace 4.1", K(replayable_point_scn), K(next_log_min_scn),
      K(iterator));
      EXPECT_EQ(next_log_min_scn, SCN::plus(replayable_point_scn, 1));
      // 再次调用next，预期next_log_min_scn还是replayable_point_scn+1
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      // iterator返回OB_ITER_END，next_log_min_scn为replayable_point_scn+1
      EXPECT_EQ(next_log_min_scn, SCN::plus(replayable_point_scn, 1));
      EXPECT_EQ(iterate_end_by_replayable_point, true);
      EXPECT_EQ(OB_SUCCESS, iterator.next(second_scn, next_log_min_scn, iterate_end_by_replayable_point)); count--;
      EXPECT_EQ(next_log_min_scn.is_valid(), false);
      prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;
      EXPECT_EQ(prev_next_success_scn, second_scn);
    }

    // 模拟file end lsn不是group entry的终点
   {
     // 设置终点为第三条日志LogEntry对应的起点
     end_lsn_v = lsns[2]+10;
     // 设置时间戳为第三条日志
     SCN third_scn = logts[2];
     SCN replayable_point_scn = SCN::plus(third_scn, 10);
     CLOG_LOG(INFO, "runlin trace 5.1", K(iterator), K(end_lsn_v), KPC(end_lsn), K(replayable_point_scn));
     // 此时内存中缓存的日志为第三条日志, iterator读取过新日志，但该日志由于end_lsn的原因不可读(此时，由于日志非受控回放，因此curr_read_pos_会被递推56)
     // 因此next_log_min_scn会被设置为third_scn
     EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
     CLOG_LOG(INFO, "runlin trace 5.1.1", K(iterator), K(next_log_min_scn), K(replayable_point_scn));
     EXPECT_EQ(next_log_min_scn, third_scn);
     EXPECT_EQ(iterate_end_by_replayable_point, false);

     // 验证第三条日志由于受控回放无法吐出(replayable_point_scn回退是不可能出现的，为了测试故意模拟)
     replayable_point_scn = SCN::minus(third_scn, 4);
     EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
     // 由于replayable_point_scn与curr_entry_之间不可能有日志，同时replayable_point_scn<curr_entry_，
     // 由于prev_entry_scn_此时为第二条日志对应的时间戳，小于replayable_point_scn，因此
     // next_min_scn会被设置为curr_entry_ scn和replayable_point_scn+1最小值,
     // 因此prev_entry_scn_会推到到replayable_point_scn+1
     // 同时由于prev_entry_scn_小于replayable_point_scn，同时replayable_point_scn和prev_entry_scn_之间没有日志
     // 因此，推到prev_entry_scn_为replayable_point_scn_
     EXPECT_EQ(next_log_min_scn, SCN::plus(replayable_point_scn, 1));
     EXPECT_EQ(iterate_end_by_replayable_point, true);
     EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
     prev_next_success_scn = replayable_point_scn;
     EXPECT_EQ(replayable_point_scn, iterator.iterator_impl_.prev_entry_scn_);

     CLOG_LOG(INFO, "runlin trace 5.2", K(iterator), K(end_lsn_v), KPC(end_lsn), K(replayable_point_scn));

     // 将replayable_point_scn变小，此时iterator会将next_min_scn设置为prev_next_success_scn + 1
     replayable_point_scn = SCN::minus(replayable_point_scn, 2);
     EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
     EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);
   }

    end_lsn_v = LSN(1000000000);
    while (count > 0) {
      EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn_case3, next_log_min_scn, iterate_end_by_replayable_point));
      prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;
      EXPECT_EQ(false, next_log_min_scn.is_valid());
      count--;
    }

    CLOG_LOG(INFO, "runlin trace 6.1", K(iterator), K(end_lsn_v), K(max_scn_case3));
    // 磁盘上以及受控回放点之后没有可读日志，此时应该返回受控回放点+1
    EXPECT_EQ(OB_ITER_END, iterator.next(max_scn_case3, next_log_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(SCN::plus(max_scn_case3, 1), next_log_min_scn);
    EXPECT_EQ(max_scn_case3, prev_next_success_scn);
    CLOG_LOG(INFO, "runlin trace 6.2", K(iterator), K(end_lsn_v), K(max_scn_case3), "end_lsn_of_leader",
        raw_write_leader.palf_handle_impl_->get_max_lsn());

    // raw write 变为 Append后，在写入一些日志
    // 测试raw write变apend后，迭代日志是否正常
    {
      std::vector<SCN> logts_append;
      std::vector<LSN> lsns_append;
      int count_append = 5;

      EXPECT_EQ(OB_SUCCESS, change_access_mode_to_append(raw_write_leader));
      PALF_LOG(INFO, "runlin trace 6.3", "raw_write_leader_lsn", raw_write_leader.palf_handle_impl_->get_max_lsn(),
          "new_leader_lsn", leader.palf_handle_impl_->get_max_lsn());
      EXPECT_EQ(OB_SUCCESS, submit_log_private(leader, count_append, id, log_size, lsns_append, logts_append));
      EXPECT_EQ(OB_SUCCESS, submit_log_private(raw_write_leader, count_append, id, log_size, lsns_append, logts_append));
      EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
      EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(raw_write_leader.palf_handle_impl_->get_max_lsn(), raw_write_leader));
      PALF_LOG(INFO, "runlin trace 6.4", "raw_write_leader_lsn", raw_write_leader.palf_handle_impl_->get_max_lsn(),
          "new_leader_lsn", leader.palf_handle_impl_->get_max_lsn());

      // case 7 end_lsn_v 为很大的值之后，让内存中有2M数据, 预期iterator next会由于受控回放失败，prev_entry_scn_不变
      // replayable_point_scn 为第一条日志的时间戳-2, next_log_min_scn 为append第一条LogEntry的时间戳
      // NB: 如果不将数据读到内存中来，可能会出现读数据报错OB_NEED_RETRY的问题。
      end_lsn_v = LSN(1000000000);
      SCN replayable_point_scn = SCN::minus(logts_append[0], 2);
      EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point)); count_append--;
      prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;

      end_lsn_v = lsns_append[1]+2;

      // 此时curr_entry_为第二条日志, curr_entry有效但由于file end lsn不可读
      // 对于append 日志受控回放无效
      replayable_point_scn = SCN::plus(raw_write_leader.palf_handle_impl_->get_max_scn(), 1000000);
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      PALF_LOG(INFO, "runlin trace 7.1", K(iterator), K(replayable_point_scn), K(end_lsn_v), K(logts_append[1]), K(replayable_point_scn));
      EXPECT_EQ(next_log_min_scn, logts_append[1]);
      EXPECT_EQ(prev_next_success_scn, iterator.iterator_impl_.prev_entry_scn_);
      EXPECT_EQ(iterate_end_by_replayable_point, false);

      PALF_LOG(INFO, "runlin trace 7.1.1", K(iterator), K(replayable_point_scn), K(end_lsn_v), K(logts_append[1]));
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(next_log_min_scn, logts_append[1]);
      EXPECT_EQ(prev_next_success_scn, iterator.iterator_impl_.prev_entry_scn_);

      // replayable_point_scn回退是一个不可能出现的情况, 但从iterator视角不能依赖这个
      // 验证replayable_point_scn回退到一个很小的值，预期next_log_min_scn为prev_next_success_scn+1
      // 模拟replayable_point_scn小于prev_entry_
      replayable_point_scn.convert_for_tx(100);
      PALF_LOG(INFO, "runlin trace 7.2", K(iterator), K(replayable_point_scn), K(end_lsn_v), K(logts_append[0]));
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);
      EXPECT_EQ(prev_next_success_scn, iterator.iterator_impl_.prev_entry_scn_);
      EXPECT_EQ(iterate_end_by_replayable_point, false);

      // 在迭代一次，结果一样
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);
      EXPECT_EQ(prev_next_success_scn, iterator.iterator_impl_.prev_entry_scn_);

      // 验证replayable_point_scn的值为prev_next_success_scn和第二条append的日志之间，
      // 预期next_log_min_scn为replayable_point_scn+1
      // 模拟replayable_point_scn位于[prev_entry_, curr_entry_]
      replayable_point_scn = SCN::minus(logts_append[1], 4);
      PALF_LOG(INFO, "runlin trace 7.3", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[1]), K(prev_next_success_scn));
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(next_log_min_scn, SCN::plus(replayable_point_scn, 1));
      // 由于replayable_point_scn到curr_entry_之间没有日志，因此prev_entry_scn_会被推到replayable_point_scn
      EXPECT_EQ(replayable_point_scn, iterator.iterator_impl_.prev_entry_scn_);

      // 在迭代一次
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(next_log_min_scn, SCN::plus(replayable_point_scn, 1));
      // 由于replayable_point_scn到curr_entry_之间没有日志，因此prev_entry_scn_会被推到replayable_point_scn
      EXPECT_EQ(replayable_point_scn, iterator.iterator_impl_.prev_entry_scn_);

      // 验证迭代append日志成功,
      end_lsn_v = lsns_append[2]+2;
      replayable_point_scn = logts_append[0];
      EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(false, next_log_min_scn.is_valid());
      EXPECT_EQ(logts_append[1], iterator.iterator_impl_.prev_entry_scn_); count_append--;
      prev_next_success_scn = logts_append[1];

      // replayable_point_scn比较大，预期next_log_min_scn为logts_append[2]
      replayable_point_scn.convert_from_ts(ObTimeUtility::current_time() + 100000000);
      PALF_LOG(INFO, "runlin trace 7.4", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[2]), K(prev_next_success_scn));
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(next_log_min_scn, logts_append[2]);
      // 在迭代一次，结果一样
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(next_log_min_scn, logts_append[2]);
      EXPECT_EQ(iterate_end_by_replayable_point, false);

      // 回退replayable_point_scn，预期next_log_min_scn为prev_next_success_scn+1
      replayable_point_scn.convert_for_tx(100);
      EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
      EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);
      EXPECT_EQ(iterate_end_by_replayable_point, false);

      end_lsn_v = LSN(1000000000);
      replayable_point_scn.convert_from_ts(ObTimeUtility::current_time() + 100000000);
      // 留一条日志
      while (count_append > 1) {
        EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(false, next_log_min_scn.is_valid());
        prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;
        count_append--;
      }

      // 验证append切回raw后是否正常工作
      {
        int64_t id3 = ATOMIC_AAF(&palf_id_, 1);
        std::vector<SCN> logts_append;
        std::vector<LSN> lsns_append;
        int count_append = 5;
        PALF_LOG(INFO, "runlin trace 8.1.0", "raw_write_leader_lsn", raw_write_leader.palf_handle_impl_->get_max_lsn(),
            "new_leader_lsn", leader.palf_handle_impl_->get_max_lsn());
        EXPECT_EQ(OB_SUCCESS, submit_log_private(leader, count_append, id, log_size, lsns_append, logts_append));
        SCN max_scn_case4 = leader.palf_handle_impl_->get_max_scn();
        EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
        EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(raw_write_leader));
        PALF_LOG(INFO, "runlin trace 8.1", "raw_write_leader_lsn", raw_write_leader.palf_handle_impl_->get_max_lsn(),
            "new_leader_lsn", leader.palf_handle_impl_->get_max_lsn());
        EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(leader,
                                                         raw_write_leader,
                                                         raw_write_leader.palf_handle_impl_->get_max_lsn()));
        EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(raw_write_leader.palf_handle_impl_->get_max_lsn(), raw_write_leader));
        PALF_LOG(INFO, "runlin trace 8.2", "raw_write_leader_lsn", raw_write_leader.palf_handle_impl_->get_max_lsn(),
            "new_leader_lsn", leader.palf_handle_impl_->get_max_lsn());

        // replayable_point_scn偏小
        SCN replayable_point_scn;
        replayable_point_scn.convert_for_tx(100);
        PALF_LOG(INFO, "runlin trace 8.3", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[0]), K(prev_next_success_scn));
        // 迭代前一轮的日志，不需要递减count_append
        EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;
        // 由于受控回放点不可读, next_log_min_scn应该为prev_next_success_scn+1
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);

        PALF_LOG(INFO, "runlin trace 8.3.1", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[0]), K(prev_next_success_scn));
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);

        // 推大受控回放点到第一条日志，但end_lsn_v也变为第一条日志的起点，此时会由于end_lsn_v不可读
        // 预期next_min_scn为replayable_point_scn.
        // 由于这条日志在此前的next中，不需要受控回放，会推大curr_read_pos_到LogEntry头，再次next不需要读数据直接返回OB_ITER_END
        end_lsn_v = lsns_append[0]+10;
        replayable_point_scn = logts_append[0];
        PALF_LOG(INFO, "runlin trace 8.4", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[0]), K(prev_next_success_scn));
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(replayable_point_scn, next_log_min_scn);
        EXPECT_EQ(iterate_end_by_replayable_point, false);

        PALF_LOG(INFO, "runlin trace 8.4.1", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[0]), K(prev_next_success_scn));
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(replayable_point_scn, next_log_min_scn);
        EXPECT_EQ(iterate_end_by_replayable_point, false);

        PALF_LOG(INFO, "runlin trace 8.4.2", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[0]), K(prev_next_success_scn));
        // 模拟prev_entry_后没有日志，replayable_point_scn小于prev_entry_scn_, 后续日志都需要受控回放
        // replayable_point_scn回退是不会出现的事，此时next_min_scn会返回prev_entry_scn_+1
        replayable_point_scn = SCN::minus(prev_next_success_scn, 100);
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);
        EXPECT_EQ(true, iterate_end_by_replayable_point);

        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);

        // 模拟prev_entry后有日志
        // 推大end_lsn_v到第二条日志的起点
        end_lsn_v = lsns_append[1]+2;
        replayable_point_scn = logts_append[1];
        PALF_LOG(INFO, "runlin trace 8.5", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[1]), K(prev_next_success_scn));
        EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(next_log_min_scn.is_valid(), false);
        prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;
        EXPECT_EQ(prev_next_success_scn, logts_append[0]);

        PALF_LOG(INFO, "runlin trace 8.6", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[1]), K(prev_next_success_scn));
        // 模拟prev_entry_后有日志, 但不可见的情况
        // 此时会由于replayable_point_scn不吐出第二条日志
        // 模拟replayable_point_scn在prev_entry_之前的情况
        replayable_point_scn.convert_for_tx(100);
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);

        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);

        // 模拟replayable_point_scn在prev_entry_之后的情况, 由于prev_enty_后有日志，因此
        // prev_entry_到replayable_point_scn之间不可能有未读过的日志，
        // 因此next_log_min_scn为replayable_point_scn + 1.
        replayable_point_scn = SCN::plus(prev_next_success_scn , 2);
        PALF_LOG(INFO, "runlin trace 8.7", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[1]), K(prev_next_success_scn));
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(replayable_point_scn, 1), next_log_min_scn);

        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(replayable_point_scn, 1), next_log_min_scn);

        // 模拟replayable_point_scn在curr_entry之后的情况
        replayable_point_scn.convert_from_ts(ObTimeUtility::current_time() + 100000000);
        PALF_LOG(INFO, "runlin trace 8.8", K(iterator), K(replayable_point_scn), K(end_lsn_v),
          K(logts_append[1]), K(prev_next_success_scn));
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(logts_append[1], next_log_min_scn);

        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(logts_append[1], next_log_min_scn);

        end_lsn_v = LSN(1000000000);
        EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(next_log_min_scn.is_valid(), false);
        EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, logts_append[1]);
        prev_next_success_scn = iterator.iterator_impl_.prev_entry_scn_;

        // 验证受控回放
        replayable_point_scn.convert_for_tx(100);
        EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_log_min_scn, iterate_end_by_replayable_point));
        EXPECT_EQ(SCN::plus(prev_next_success_scn, 1), next_log_min_scn);
        EXPECT_EQ(true, iterate_end_by_replayable_point);
      }
    }
  }
  // 验证重启
  restart_paxos_groups();
  {
    PalfHandleImplGuard raw_write_leader;
    PalfBufferIterator iterator;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, raw_write_leader, leader_idx));
    PalfHandleImpl *raw_write_palf_handle_impl = raw_write_leader.palf_handle_impl_;
    auto get_file_end_lsn = []() -> LSN { return LSN(1000000000); };
    auto get_mode_version = [&mode_version, &mode_version_v]() -> int64_t {
      PALF_LOG(INFO, "runlin trace", K(*mode_version), K(mode_version_v));
      return *mode_version;
    };
    EXPECT_EQ(OB_SUCCESS,
        iterator.init(LSN(0), get_file_end_lsn, get_mode_version, &raw_write_palf_handle_impl->log_engine_.log_storage_));
    SCN max_scn = raw_write_leader.palf_handle_impl_->get_max_scn();
    int64_t count = 5 + 5 + 5 + 5 + 5;
    while (count > 0) {
      EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn));
      count--;
    }
    EXPECT_EQ(OB_ITER_END, iterator.next(max_scn));
    EXPECT_EQ(OB_ITER_END, read_log_from_memory(raw_write_leader));
  }
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_gc_block)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_gc_block");
  OB_LOGGER.set_log_level("TRACE");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  LSN upper_aligned_log_tail;
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  LogEngine *log_engine = &leader.palf_handle_impl_->log_engine_;
  LogStorage *log_meta_storage = &log_engine->log_meta_storage_;
  block_id_t min_block_id;
  share::SCN min_block_scn;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 31, leader_idx, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
  block_id_t expect_block_id = 1;
  share::SCN expect_scn;
  EXPECT_EQ(OB_SUCCESS, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, log_engine->get_block_min_scn(expect_block_id, expect_scn));
  EXPECT_EQ(expect_scn, min_block_scn);
  EXPECT_EQ(OB_SUCCESS, log_engine->delete_block(0));
  EXPECT_EQ(false, log_engine->min_block_max_scn_.is_valid());

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
  expect_block_id = 2;
  EXPECT_EQ(OB_SUCCESS, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, log_engine->get_block_min_scn(expect_block_id, expect_scn));
  EXPECT_EQ(expect_scn, min_block_scn);
  EXPECT_EQ(OB_SUCCESS, log_engine->delete_block(1));
  expect_block_id = 3;
  EXPECT_EQ(OB_SUCCESS, log_engine->get_min_block_info_for_gc(min_block_id, min_block_scn));
  EXPECT_EQ(OB_SUCCESS, log_engine->get_block_min_scn(expect_block_id, expect_scn));
  EXPECT_EQ(expect_scn, min_block_scn);
}

TEST_F(TestObSimpleLogClusterSingleReplica, test_iterator_with_flashback)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_iterator_with_flashback");
  OB_LOGGER.set_log_level("TRACE");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  PalfHandleImplGuard raw_write_leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  PalfHandleImpl *palf_handle_impl = leader.palf_handle_impl_;
  const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_raw_write, leader_idx, raw_write_leader));
  EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(raw_write_leader));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 200));
  SCN max_scn1 = leader.palf_handle_impl_->get_max_scn();
  LSN end_pos_of_log1 = leader.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 200));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  SCN max_scn2 = leader.palf_handle_impl_->get_max_scn();
  LSN end_pos_of_log2 = leader.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));

  // 提交几条日志到raw_write leader
  EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(leader, raw_write_leader));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(raw_write_leader, raw_write_leader.palf_handle_impl_->get_max_lsn()));

  PalfBufferIterator iterator;
  EXPECT_EQ(OB_SUCCESS, raw_write_leader.palf_handle_impl_->alloc_palf_buffer_iterator(LSN(0), iterator));
  // 迭代flashback之前的日志成功
  SCN next_min_scn;
  SCN tmp_scn; tmp_scn.val_ = 1000;
  bool iterate_end_by_replayable_point = false;
  EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn1, next_min_scn, iterate_end_by_replayable_point));
  EXPECT_EQ(false, iterate_end_by_replayable_point);
  EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, max_scn1);
  EXPECT_EQ(OB_ITER_END, iterator.next(
    max_scn1, next_min_scn, iterate_end_by_replayable_point));
  EXPECT_EQ(true, iterate_end_by_replayable_point);
  EXPECT_EQ(end_pos_of_log1, iterator.iterator_impl_.log_storage_->get_lsn(iterator.iterator_impl_.curr_read_pos_));
  EXPECT_EQ(SCN::plus(max_scn1, 1), next_min_scn);
  PALF_LOG(INFO, "runlin trace case1", K(iterator), K(end_pos_of_log1));

  EXPECT_EQ(OB_SUCCESS, raw_write_leader.palf_handle_impl_->inner_flashback(max_scn2));

  EXPECT_EQ(max_scn2, raw_write_leader.palf_handle_impl_->get_max_scn());

  int64_t mode_version;
  switch_flashback_to_append(raw_write_leader, mode_version);

  // 磁盘上存在三条日志，一条日志已经迭代，另外一条日志没有迭代(raw_write)，最后一条日志为Append
  EXPECT_EQ(OB_SUCCESS, submit_log(raw_write_leader, 1, leader_idx, 333));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(raw_write_leader, raw_write_leader.palf_handle_impl_->get_max_lsn()));

  EXPECT_EQ(OB_ITER_END, read_group_log(raw_write_leader, LSN(0)));
  SCN max_scn3 = raw_write_leader.palf_handle_impl_->get_max_scn();
  PALF_LOG(INFO, "runlin trace case2", K(iterator), K(max_scn3), "end_lsn:", raw_write_leader.palf_handle_impl_->get_end_lsn());

  LSN iterator_end_lsn = iterator.iterator_storage_.end_lsn_;
  // iterator内存中有几条日志，预期返回成功, 此时会清cache, 前一条日志的信息会被清除（raw_write日志）
  // 迭代器游标预期依旧指向第一条日志的终点, 由于受控回放，返回iterate_end
  EXPECT_EQ(OB_ITER_END, iterator.next(
    max_scn1, next_min_scn, iterate_end_by_replayable_point));
  EXPECT_EQ(end_pos_of_log1, iterator.iterator_impl_.log_storage_->get_lsn(iterator.iterator_impl_.curr_read_pos_));
  EXPECT_EQ(true, iterator.iterator_impl_.curr_entry_is_raw_write_);

  // 需要从磁盘上将后面两日志读上来，但由于受控回放不会吐出
  // EXPECT_FALSE(iterator_end_lsn == iterator.iterator_storage_.end_lsn_);
  EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn2, next_min_scn, iterate_end_by_replayable_point));
  EXPECT_EQ(false, iterate_end_by_replayable_point);
  EXPECT_EQ(true, iterator.iterator_impl_.curr_entry_is_raw_write_);
  EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, max_scn2);

  EXPECT_EQ(OB_SUCCESS, iterator.next(max_scn3, next_min_scn, iterate_end_by_replayable_point));
  EXPECT_EQ(false, iterate_end_by_replayable_point);
  EXPECT_EQ(false, iterator.iterator_impl_.curr_entry_is_raw_write_);
  EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, max_scn3);

  // raw_write_leader已经有三条日志, raw_write(1 log entry), raw_write(1), append(1),
  // 模拟一条group entry 中有多条小日志
  LSN last_lsn = raw_write_leader.palf_handle_impl_->get_max_lsn();
  SCN last_scn = raw_write_leader.palf_handle_impl_->get_max_scn();

  LogIOWorker *io_worker = raw_write_leader.palf_handle_impl_->log_engine_.log_io_worker_;
  IOTaskCond cond(id_raw_write, raw_write_leader.palf_env_impl_->last_palf_epoch_);
  io_worker->submit_io_task(&cond);
  std::vector<LSN> lsns;
  std::vector<SCN> scns;
  EXPECT_EQ(OB_SUCCESS, submit_log(raw_write_leader, 10, 100, id_raw_write, lsns, scns));
  int group_entry_num = 1;
  int first_log_entry_index = 0, last_log_entry_index = 0;
  for (int i = 1; i < 10; i++) {
    if (lsns[i-1]+100+sizeof(LogEntryHeader) == lsns[i]) {
      last_log_entry_index = i;
    } else {
      first_log_entry_index = i;
      group_entry_num++;
      PALF_LOG(INFO, "group entry", K(i-1));
    }
    if (first_log_entry_index - last_log_entry_index > 2) {
      break;
    }
  }
  leader.reset();
  if (first_log_entry_index != 1 && last_log_entry_index != 9) {
    PALF_LOG(INFO, "no group log has more than 2 log entry", K(first_log_entry_index), K(last_log_entry_index));
    return;
  }

  cond.cond_.signal();

  // 验证从一条包含多条LogEntry中日志中flashback，iterator迭代到中间的LogEntry后，flashback位点前还有几条LogEntry
  // LogGroup LogGroup LogGroup LogGroup LogGroup(9条小日志)
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(raw_write_leader, raw_write_leader.palf_handle_impl_->get_max_lsn()));
  {
    const int64_t id_new_raw_write = ATOMIC_AAF(&palf_id_, 1);
    PalfHandleImplGuard new_raw_write_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_new_raw_write, leader_idx, new_raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(new_raw_write_leader));

    EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(raw_write_leader, new_raw_write_leader));

    PalfBufferIterator buff_iterator;
    PalfGroupBufferIterator group_iterator;

    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->alloc_palf_buffer_iterator(LSN(0), buff_iterator));
    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(LSN(0), group_iterator));

    SCN replayable_point_scn(SCN::min_scn());
    // 验证replayable_point_scn为min_scn
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);


    // replayable_point_scn为第一条日志-1
    replayable_point_scn = SCN::minus(max_scn1, 1);
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);

    // replayable_point_scn为第一条日志
    replayable_point_scn = max_scn1;
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, false);
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, false);
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);

    // replayable_point_scn为第一条日志 + 1
    replayable_point_scn = SCN::plus(max_scn1, 1);
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);

    // 成功迭代第二条日志，第三条日志
    replayable_point_scn = last_scn;
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn));

    // 第四条日志一定是LogGroupEntry
    replayable_point_scn = scns[0];
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn));
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn));
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);

    // 迭代第五条LogGroupEntry的第一条LogEntry
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_NE(buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_NE(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    // 由于被受控回放，buff_iterator以及group_iterator都没有推进curr_read_pos_
    EXPECT_EQ(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_),
              buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_));

    // 成功迭代第五条LogGroupEntry的第一条LogEntry
    replayable_point_scn = scns[1];
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    // group iterator被受控回放, 但由于第五条日志的max_scn大于受控回放点，故受控回放
    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    // 由于受控回放的group entry对应的min scn和replayable_point_scn一样，因此next_min_scn会被设置为replayable_point_scn
    EXPECT_EQ(next_min_scn, replayable_point_scn);
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, scns[0]);
    EXPECT_NE(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    // 由于被第一条LogEntry受控回放，group_iterator没有推进curr_read_pos_, buff_iter推进了curr_read_pos_
    EXPECT_NE(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_),
              buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_));

    // buff_iterator的游标到了第五条group_entry的第一条小日志
    // grou_iterator的游标到了第五条group_entry开头
    // sncs[0] 第四条group entry，scns[1] - scns[9]是第二条
    // 第五条group entry的第五条小日志被flashback
    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->inner_flashback(scns[4]));
    EXPECT_EQ(new_raw_write_leader.palf_handle_impl_->get_max_scn(), scns[4]);
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_append(new_raw_write_leader));
    // 提交一条group_entry
    // 对于buff_iterator, 存在两条group_entry未读，一条raw_rwrite(包含4条小日志，游标停留在第一条小日志末尾)，一条append
    // 对于group_iterator, 存在三条group_entry未读，一条raw_rwrite(包含4条小日志，游标停留在group_entry头部)，一条append
    EXPECT_EQ(OB_SUCCESS, submit_log(new_raw_write_leader, 1, leader_idx, 100));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_raw_write_leader, new_raw_write_leader.palf_handle_impl_->get_max_lsn()));

    // 对于buff_iterator
    // lsns[2]为第二条小日志开头，即第一条小日志末尾
    // 验证游标起始位置为第一条小日志头部
    // next 返回iterate是否清空cache
    // 迭代raw_write写入的小日志
    // 迭代append写入的小日志
    PALF_LOG(INFO, "rulin trace 1", K(lsns[2]), K(lsns[1]), K(lsns[0]), K(buff_iterator));
    EXPECT_EQ(buff_iterator.iterator_impl_.log_storage_->get_lsn(buff_iterator.iterator_impl_.curr_read_pos_), lsns[1]);
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(next_min_scn, SCN::plus(buff_iterator.iterator_impl_.prev_entry_scn_, 1));
    EXPECT_EQ(0, buff_iterator.iterator_impl_.curr_read_pos_);
    // 迭代第二条日志
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(SCN::max_scn()));
    // 迭代第三条日志
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(SCN::max_scn()));
    // 迭代第四条日志
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(SCN::max_scn()));

    // 迭代第五条日志(迭代新的GroupENtry, 非受控回放)
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(SCN::min_scn()));
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(SCN::min_scn()));

    // 对于group_iterator
    // 验证游标起始位置为raw_write日志开头
    // next 返回iterate是否清空cache
    // 迭代raw_write写入的大日志
    // 迭代append写入的大日志
    PALF_LOG(INFO, "rulin trace 2", K(lsns[2]), K(lsns[1]), K(lsns[0]), K(group_iterator));
    EXPECT_EQ(group_iterator.iterator_impl_.log_storage_->get_lsn(group_iterator.iterator_impl_.curr_read_pos_), lsns[1] - sizeof(LogGroupEntryHeader));
    EXPECT_EQ(OB_ITER_END, group_iterator.next(SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(next_min_scn, SCN::plus(group_iterator.iterator_impl_.prev_entry_scn_, 1));

    // 迭代raw_write日志
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(SCN::max_scn()));
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(SCN::max_scn()));
    EXPECT_EQ(OB_ITER_END, group_iterator.next(SCN::max_scn()));
  }

  // 验证从一条包含多条LogEntry中日志中flashback，iterator迭代到中间的LogEntry后，flashback位点前没有LogEntry
  // LogGroup LogGroup LogGroup LogGroup LogGroup(9条小日志)
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(raw_write_leader, raw_write_leader.palf_handle_impl_->get_max_lsn()));
  {
    const int64_t id_new_raw_write = ATOMIC_AAF(&palf_id_, 1);
    PalfHandleImplGuard new_raw_write_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_new_raw_write, leader_idx, new_raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(new_raw_write_leader));

    EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(raw_write_leader, new_raw_write_leader));

    PalfBufferIterator buff_iterator;
    PalfGroupBufferIterator group_iterator;

    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->alloc_palf_buffer_iterator(LSN(0), buff_iterator));
    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(LSN(0), group_iterator));

    // 成功迭代第一条日志，第二条日志，第三条日志
    SCN replayable_point_scn(last_scn);
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));

    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn));

    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn));

    // 第四条日志一定是LogGroupEntry
    replayable_point_scn = scns[0];
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn));
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(replayable_point_scn));
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);

    // 迭代第五条LogGroupEntry的第一条LogEntry
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_NE(buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(replayable_point_scn, 1));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_NE(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    // 由于被受控回放，buff_iterator以及group_iterator都没有推进curr_read_pos_
    EXPECT_EQ(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_),
              buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_));

    // 成功迭代第五条LogGroupEntry的第一条LogEntry
    replayable_point_scn = scns[1];
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(buff_iterator.iterator_impl_.prev_entry_scn_, replayable_point_scn);
    EXPECT_EQ(buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    // group iterator被受控回放, 但由于第五条日志的max_scn大于受控回放点，故受控回放
    EXPECT_EQ(OB_ITER_END, group_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    // 由于受控回放的group entry对应的min scn和replayable_point_scn一样，因此next_min_scn会被设置为replayable_point_scn
    EXPECT_EQ(next_min_scn, replayable_point_scn);
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(group_iterator.iterator_impl_.prev_entry_scn_, scns[0]);
    EXPECT_NE(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_), lsns[1]);

    // 由于被第一条LogEntry受控回放，group_iterator没有推进curr_read_pos_, buff_iter推进了curr_read_pos_
    EXPECT_NE(group_iterator.iterator_impl_.log_storage_->get_lsn(
      group_iterator.iterator_impl_.curr_read_pos_),
              buff_iterator.iterator_impl_.log_storage_->get_lsn(
      buff_iterator.iterator_impl_.curr_read_pos_));

    // 迭代日志发现需要受控回放
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(scns[1], next_min_scn, iterate_end_by_replayable_point));

    // buff_iterator的游标到了第五条group_entry的第一条小日志末尾
    // grou_iterator的游标到了第五条group_entry开头
    // sncs[0] 第四条group entry，scns[1] - scns[9]是第二条
    // 第五条group entry的第二条小日志被flashback
    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->inner_flashback(scns[2]));
    EXPECT_EQ(new_raw_write_leader.palf_handle_impl_->get_max_scn(), scns[2]);
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_append(new_raw_write_leader));
    // 提交一条group_entry
    // 对于buff_iterator, 存在两条group_entry未读，一条raw_rwrite(包含4条小日志，游标停留在第一条小日志末尾)，一条append
    // 对于group_iterator, 存在三条group_entry未读，一条raw_rwrite(包含4条小日志，游标停留在group_entry头部)，一条append
    EXPECT_EQ(OB_SUCCESS, submit_log(new_raw_write_leader, 1, leader_idx, 100));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_raw_write_leader, new_raw_write_leader.palf_handle_impl_->get_max_lsn()));

    // 对于buff_iterator
    // lsns[2]为第二条小日志开头，即第一条小日志末尾
    // 验证游标起始位置为第一条小日志头部
    // next 返回iterate是否清空cache
    // 迭代raw_write写入的小日志
    // 迭代append写入的小日志
    PALF_LOG(INFO, "rulin trace 3", K(lsns[2]), K(lsns[1]), K(lsns[0]), K(buff_iterator));
    EXPECT_EQ(buff_iterator.iterator_impl_.log_storage_->get_lsn(buff_iterator.iterator_impl_.curr_read_pos_), lsns[2]);
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(next_min_scn, SCN::plus(buff_iterator.iterator_impl_.prev_entry_scn_, 1));
    EXPECT_EQ(0, buff_iterator.iterator_impl_.curr_read_pos_);
    // 迭代第二条小日志
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(SCN::max_scn()));
    // 迭代新写入的LogGroupEntry, 不需要受控回放
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(SCN::min_scn()));

    EXPECT_EQ(OB_ITER_END, buff_iterator.next(SCN::min_scn()));

    // 对于group_iterator
    // 验证游标起始位置为raw_write日志开头
    // next 返回iterate是否清空cache
    // 迭代raw_write写入的大日志
    // 迭代append写入的大日志
    PALF_LOG(INFO, "rulin trace 4", K(lsns[2]), K(lsns[1]), K(lsns[0]), K(group_iterator));
    EXPECT_EQ(group_iterator.iterator_impl_.log_storage_->get_lsn(group_iterator.iterator_impl_.curr_read_pos_), lsns[1] - sizeof(LogGroupEntryHeader));
    EXPECT_EQ(OB_ITER_END, group_iterator.next(SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(next_min_scn, SCN::plus(group_iterator.iterator_impl_.prev_entry_scn_, 1));

    // 迭代raw_write日志
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(SCN::max_scn()));
    // 迭代新的GruopEntry
    EXPECT_EQ(OB_SUCCESS, group_iterator.next(SCN::min_scn()));
    EXPECT_EQ(OB_ITER_END, group_iterator.next(SCN::min_scn()));
  }

  // 验证一条LogGroupEntry需要受控回放，buff iterator不能更新accumlate_checksum和curr_read_pos_
  // LogGroup LogGroup LogGroup LogGroup LogGroup(9条小日志)
  //                   last_scn scns[0]  scns[1]...
  {
    const int64_t id_new_raw_write = ATOMIC_AAF(&palf_id_, 1);
    PalfHandleImplGuard new_raw_write_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_new_raw_write, leader_idx, new_raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(new_raw_write_leader));
    EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(raw_write_leader, new_raw_write_leader));
    PalfBufferIterator iterator;
    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->alloc_palf_buffer_iterator(LSN(0), iterator));
    SCN replayable_point_scn(last_scn);

    EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn));
    EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn));

    EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(iterate_end_by_replayable_point, true);
    EXPECT_EQ(next_min_scn, SCN::plus(last_scn, 1));

    replayable_point_scn = scns[0];
    EXPECT_EQ(OB_SUCCESS, iterator.next(replayable_point_scn));
    // scns[1]对应的日志无法吐出
    EXPECT_EQ(OB_ITER_END, iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(next_min_scn, SCN::plus(scns[0], 1));
    EXPECT_EQ(iterator.iterator_impl_.prev_entry_scn_, scns[0]);

    // flashback到scns[0]
    EXPECT_EQ(OB_SUCCESS, new_raw_write_leader.palf_handle_impl_->inner_flashback(scns[0]));
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_append(new_raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(new_raw_write_leader, 1, leader_idx, 100));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_raw_write_leader, new_raw_write_leader.palf_handle_impl_->get_max_lsn()));

    // scns[0]对应的日志为raw write, 被flashback了, iterator停在scns[0]的末尾
    // 迭代新写入的日志成功
    EXPECT_EQ(OB_SUCCESS, iterator.next(SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(OB_ITER_END, iterator.next(SCN::min_scn()));
  }

  // 验证一条padding LogGroupEntry需要受控回放
  {
    const int64_t append_id = ATOMIC_AAF(&palf_id_, 1);
    PalfHandleImplGuard append_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(append_id, leader_idx, append_leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(append_leader, 31, leader_idx, MAX_LOG_BODY_SIZE));
    const LSN padding_start_lsn = append_leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, submit_log(append_leader, 1, leader_idx, MAX_LOG_BODY_SIZE));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(append_leader, append_leader.get_palf_handle_impl()->get_max_lsn()));
    SCN padding_scn = append_leader.get_palf_handle_impl()->get_max_scn();
    padding_scn = padding_scn.minus(padding_scn, 1);

    const int64_t raw_write_id = ATOMIC_AAF(&palf_id_, 1);
    PalfHandleImplGuard raw_write_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(raw_write_id, leader_idx, raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(raw_write_leader));
    EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(append_leader, raw_write_leader));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(raw_write_leader, raw_write_leader.get_palf_handle_impl()->get_max_lsn()));
    switch_append_to_flashback(raw_write_leader, mode_version);

    PalfBufferIterator buff_iterator;
    PalfGroupBufferIterator group_buff_iterator;
    PalfBufferIterator buff_iterator_padding_start;
    PalfGroupBufferIterator group_buff_iterator_padding_start;
    EXPECT_EQ(OB_SUCCESS, raw_write_leader.get_palf_handle_impl()->alloc_palf_buffer_iterator(LSN(0), buff_iterator));
    EXPECT_EQ(OB_SUCCESS, raw_write_leader.get_palf_handle_impl()->alloc_palf_group_buffer_iterator(LSN(0), group_buff_iterator));
    EXPECT_EQ(OB_SUCCESS, raw_write_leader.get_palf_handle_impl()->alloc_palf_buffer_iterator(LSN(0), buff_iterator_padding_start));
    EXPECT_EQ(OB_SUCCESS, raw_write_leader.get_palf_handle_impl()->alloc_palf_group_buffer_iterator(LSN(0), group_buff_iterator_padding_start));
    SCN next_min_scn;
    bool iterate_end_by_replayable_point = false;
    EXPECT_EQ(OB_ITER_END, buff_iterator.next(share::SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(OB_ITER_END, group_buff_iterator.next(share::SCN::min_scn(), next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(true, iterate_end_by_replayable_point);

    // 一共有33条日志，包括padding
    SCN replayable_point_scn = padding_scn.minus(padding_scn, 1);
    // 直到padding日志受控回放
    int ret = OB_SUCCESS;
    while (OB_SUCC(buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point))) {
    }
    ret = OB_SUCCESS;
    while (OB_SUCC(buff_iterator_padding_start.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point))) {
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(next_min_scn, padding_scn);
    ret = OB_SUCCESS;
    while (OB_SUCC(group_buff_iterator.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point))) {
    }
    ret = OB_SUCCESS;
    while (OB_SUCC(group_buff_iterator_padding_start.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point))) {
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(true, iterate_end_by_replayable_point);
    EXPECT_EQ(next_min_scn, padding_scn);

    EXPECT_EQ(false, buff_iterator.iterator_impl_.curr_entry_is_padding_);
    EXPECT_EQ(false, group_buff_iterator.iterator_impl_.curr_entry_is_padding_);
    // flashback到padding日志尾
    EXPECT_EQ(OB_SUCCESS, raw_write_leader.get_palf_handle_impl()->flashback(mode_version, padding_scn, timeout_ts_us));
    EXPECT_EQ(OB_SUCCESS, buff_iterator.next(padding_scn, next_min_scn, iterate_end_by_replayable_point));
    LogEntry padding_log_entry;
    LSN padding_log_lsn;
    EXPECT_EQ(OB_SUCCESS, buff_iterator.get_entry(padding_log_entry, padding_log_lsn));
    EXPECT_EQ(true, padding_log_entry.check_integrity());
    EXPECT_EQ(true, padding_log_entry.header_.is_padding_log_());
    EXPECT_EQ(padding_scn, padding_log_entry.header_.scn_);
    EXPECT_EQ(false, buff_iterator.iterator_impl_.padding_entry_scn_.is_valid());

    EXPECT_EQ(OB_SUCCESS, group_buff_iterator.next(padding_scn, next_min_scn, iterate_end_by_replayable_point));
    LogGroupEntry padding_group_entry;
    LSN padding_group_lsn;
    EXPECT_EQ(OB_SUCCESS, group_buff_iterator.get_entry(padding_group_entry, padding_group_lsn));
    EXPECT_EQ(true, padding_group_entry.check_integrity());
    EXPECT_EQ(true, padding_group_entry.header_.is_padding_log());
    // 对于LogGruopEntry的iterator，在construct_padding_log_entry_后，不会重置padding状态
    EXPECT_EQ(true, group_buff_iterator.iterator_impl_.padding_entry_scn_.is_valid());
    EXPECT_EQ(padding_log_entry.header_.scn_, padding_group_entry.header_.max_scn_);
    // flashback到padding日志头
    EXPECT_EQ(OB_SUCCESS, raw_write_leader.get_palf_handle_impl()->flashback(mode_version, padding_scn.minus(padding_scn, 1), timeout_ts_us));
    // 预期是由于文件长度导致的OB_ITER_END
    EXPECT_EQ(OB_ITER_END, buff_iterator_padding_start.next(padding_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(false, iterate_end_by_replayable_point);
    EXPECT_GE(next_min_scn, buff_iterator_padding_start.iterator_impl_.prev_entry_scn_);
    EXPECT_EQ(OB_ITER_END, group_buff_iterator_padding_start.next(padding_scn, next_min_scn, iterate_end_by_replayable_point));
    EXPECT_EQ(false, iterate_end_by_replayable_point);
    EXPECT_GE(next_min_scn, group_buff_iterator_padding_start.iterator_impl_.prev_entry_scn_);
    switch_flashback_to_append(raw_write_leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, submit_log(raw_write_leader, 100, leader_idx, 1000));
    EXPECT_EQ(OB_SUCCESS, buff_iterator_padding_start.next());
    EXPECT_EQ(OB_SUCCESS, group_buff_iterator_padding_start.next());
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
