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
#include <signal.h>
#include <share/scn.h>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "data_integrity";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogDataIntergrity: public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogDataIntergrity() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 2;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 2;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

int pwrite_one_log_by_log_storage(PalfHandleImplGuard &leader, const LogGroupEntry &entry, const LSN &lsn)
{
  int ret = OB_SUCCESS;
  LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
  int dir_fd = log_storage->block_mgr_.dir_fd_;
  block_id_t writable_block_id = log_storage->block_mgr_.curr_writable_block_id_;
  LSN log_tail = log_storage->log_tail_;
  offset_t write_offset = log_storage->get_phy_offset_(lsn);
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  block_id_to_string(writable_block_id, block_path, OB_MAX_FILE_NAME_LENGTH);
  int block_fd = -1;
  if (-1 == (block_fd = ::openat(dir_fd, block_path, O_WRONLY))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "openat failed", K(ret), K(block_path), KPC(log_storage));
  } else if (0 >= pwrite(block_fd,
      entry.get_data_buf() - sizeof(LogGroupEntryHeader),
      entry.get_serialize_size(), write_offset)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "pwrite failed", K(ret), K(block_path), KPC(log_storage), K(write_offset), K(log_tail), K(entry));
  } else {

  }
  if (-1 != block_fd) {
    ::close(block_fd);
    block_fd = -1;
  }
  return ret;
}

TEST_F(TestObSimpleLogDataIntergrity, accumlate_checksum)
{
  SET_CASE_LOG_FILE(TEST_NAME, "accumlate_checksum");
  OB_LOGGER.set_log_level("TRACE");
  ObTimeGuard guard("accum_checksum", 0);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test accumlate checksum", K(id));
  int64_t leader_idx = 0;

  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id, 100 * 1024));
  const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 1234));
  const LSN end_max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, end_max_lsn));

  LSN curr_lsn;
  LogGroupEntry entry;
  PalfGroupBufferIterator iterator;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(max_lsn, iterator));
  EXPECT_EQ(OB_SUCCESS, iterator.next());
  EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
  EXPECT_EQ(curr_lsn, max_lsn);
  EXPECT_EQ(OB_SUCCESS, iterator.next());
  EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
  EXPECT_EQ(OB_ITER_END, iterator.next());
  EXPECT_EQ(OB_SUCCESS, pwrite_one_log_by_log_storage(leader, entry, max_lsn));
  PALF_LOG(INFO, "start first check");
  EXPECT_EQ(OB_CHECKSUM_ERROR, read_log(leader, max_lsn));
  PALF_LOG(INFO, "end first check");
  EXPECT_EQ(OB_CHECKSUM_ERROR, read_log(leader));
  EXPECT_EQ(OB_CHECKSUM_ERROR, read_group_log(leader, max_lsn));
  EXPECT_EQ(OB_CHECKSUM_ERROR, read_group_log(leader, LSN(0)));
  PALF_LOG(INFO, "end test accumlate checksum", K(id));
}

TEST_F(TestObSimpleLogDataIntergrity, log_corrupted)
{
  SET_CASE_LOG_FILE(TEST_NAME, "log_corrupted");
  OB_LOGGER.set_log_level("TRACE");
  ObTimeGuard guard("log_corrupted", 0);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test log corrupted", K(id));
  int64_t leader_idx = 0;

  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id, 100 * 1024));
  const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 2, id, 1234));
  const LSN end_max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, end_max_lsn));

  LSN curr_lsn;
  LogGroupEntry entry;
  PalfGroupBufferIterator iterator;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(max_lsn, iterator));
  EXPECT_EQ(OB_SUCCESS, iterator.next());
  EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
  EXPECT_EQ(curr_lsn, max_lsn);
  EXPECT_EQ(OB_SUCCESS, iterator.next());
  EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
  EXPECT_EQ(OB_ITER_END, iterator.next());
  char *buf = const_cast<char*>(entry.buf_);
  buf[4] = 1;
  EXPECT_EQ(OB_SUCCESS, pwrite_one_log_by_log_storage(leader, entry, max_lsn));
  PALF_LOG(INFO, "start first check");
  EXPECT_EQ(OB_INVALID_DATA, read_log(leader, max_lsn));
  PALF_LOG(INFO, "end first check");
  EXPECT_EQ(OB_INVALID_DATA, read_log(leader));
  EXPECT_EQ(OB_INVALID_DATA, read_group_log(leader, max_lsn));
  EXPECT_EQ(OB_INVALID_DATA, read_group_log(leader, LSN(0)));
  PALF_LOG(INFO, "end test log corrupted", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
