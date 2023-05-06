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

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
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
  int64_t pos = 0;
  char *serialize_buf = reinterpret_cast<char *>(ob_malloc(entry.get_serialize_size(), "MitTest"));
  if (NULL == serialize_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (-1 == (block_fd = ::openat(dir_fd, block_path, O_WRONLY))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "openat failed", K(ret), K(block_path), KPC(log_storage));
  } else if (OB_FAIL(entry.serialize(serialize_buf, entry.get_serialize_size(), pos))) {
    PALF_LOG(ERROR, "serialize failed", K(ret), K(block_path), KPC(log_storage), K(entry));
  } else if (0 >= pwrite(block_fd,
        serialize_buf,
        entry.get_serialize_size(), write_offset)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "pwrite failed", K(ret), K(block_path), KPC(log_storage), K(write_offset), K(log_tail), K(entry));
  } else {

  }
  if (-1 != block_fd) {
    ::close(block_fd);
    block_fd = -1;
  }
  if (NULL != serialize_buf) {
    ob_free(serialize_buf);
  }
  return ret;
}
typedef ObFunction<void(char *buf)> DataFaultInject;
int make_log_group_entry_partial_error(LogGroupEntry &entry, char *&output_buf, DataFaultInject &inject)
{
  int ret = OB_SUCCESS;
  if (!entry.is_valid() || !inject.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    return ret;
  }
  int64_t pos = 0;
  char *serialize_buf = reinterpret_cast<char *>(ob_malloc(entry.get_serialize_size(), "MitTest"));
  if (NULL == serialize_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(entry.serialize(serialize_buf, entry.get_serialize_size(), pos))) {
    PALF_LOG(ERROR, "serialize failed", K(ret), K(entry));
  } else {
    inject(serialize_buf);
    pos = 0;
    entry.deserialize(serialize_buf, entry.get_serialize_size(), pos);
    entry.buf_ = serialize_buf + entry.header_.get_serialize_size();
    output_buf = serialize_buf;
  }
  if (OB_FAIL(ret)) {
    ob_free(serialize_buf);
  }
  return ret;
}

TEST_F(TestObSimpleLogDataIntergrity, accumlate_checksum)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "accumlate_checksum");
  OB_LOGGER.set_log_level("TRACE");
  ObTimeGuard guard("accum_checksum", 0);
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test accumlate checksum", K(id));
  int64_t leader_idx = 0;

  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 100 * 1024));
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
  }
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  PALF_LOG(INFO, "runlin trace delete_paxos_group");
  // 模拟最后一条的LogEntry非原子写入(LogEntry没有写入)，报错OB_INVALID_DATA, 重启成功，预期log_tail是该日志头
  LSN expected_log_tail;
  {
    id = ATOMIC_AAF(&palf_id_, 1);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 100 * 1024));
    const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 100 * 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
    PalfGroupBufferIterator iterator;
    LogGroupEntry entry;
    LSN curr_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(max_lsn, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
    EXPECT_EQ(curr_lsn, max_lsn);
    // LogEntry完全被写坏
    char *output_buf = NULL;
    int64_t pos = sizeof(LogGroupEntryHeader);
    DataFaultInject inject = [&pos, &entry](char *buf) {
      int64_t memset_len = entry.get_serialize_size()-pos;
      memset(buf+pos, 0, memset_len);
    };
    EXPECT_EQ(OB_SUCCESS, make_log_group_entry_partial_error(entry, output_buf, inject));
    EXPECT_EQ(OB_SUCCESS, pwrite_one_log_by_log_storage(leader, entry, max_lsn));
    EXPECT_EQ(OB_ITER_END, iterator.next());
    EXPECT_EQ(OB_INVALID_DATA, read_log(leader));
    if (NULL != output_buf) {
      ob_free(output_buf);
    }
    expected_log_tail = curr_lsn;
  }
  PALF_LOG(INFO, "runlin trace first restart_paxos_groups begin");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  PALF_LOG(INFO, "runlin trace first restart_paxos_groups end");
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(expected_log_tail, leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 100 * 1024));
  }
  PALF_LOG(INFO, "runlin trace second restart_paxos_groups begin");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  PALF_LOG(INFO, "runlin trace second restart_paxos_groups end");
  // 模拟最后一条的LogEntry非原子写入(LogEntry部分写入, datacheck sum以及后续的数据被写坏为0)，报错OB_CHECKSUM_ERROR, 重启成功，预期log_tail是该日志头
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 100 * 1024));
    const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 100 * 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
    PalfGroupBufferIterator iterator;
    LogGroupEntry entry;
    LSN curr_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(max_lsn, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
    EXPECT_EQ(curr_lsn, max_lsn);
    // 模拟LogEntry的datachecsum以及后续的数据被置为全0
    // LogEntryHeader 16bit(maigc) 16bit(version) 32bit(size) 64bit(scn) datachecsum
    char *output_buf = NULL;
    int64_t pos = sizeof(LogGroupEntryHeader) + 16;
    DataFaultInject inject = [&pos, &entry](char *buf) {
      int64_t memset_len = entry.get_serialize_size()-pos;
      memset(buf+pos, 0, memset_len);
    };
    EXPECT_EQ(OB_SUCCESS, make_log_group_entry_partial_error(entry, output_buf, inject));
    EXPECT_EQ(OB_SUCCESS, pwrite_one_log_by_log_storage(leader, entry, max_lsn));
    EXPECT_EQ(OB_ITER_END, iterator.next());
    int tmp_ret = read_log(leader);
    if (OB_CHECKSUM_ERROR != tmp_ret && OB_INVALID_DATA != tmp_ret) {
      int ret = OB_SUCCESS;
      PALF_LOG(ERROR, "unexpected error", K(tmp_ret));
      EXPECT_EQ(false, true);
    }
    if (NULL != output_buf) {
      ob_free(output_buf);
    }
    expected_log_tail = curr_lsn;
  }
  PALF_LOG(INFO, "runlin trace third restart_paxos_groups begin");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  PALF_LOG(INFO, "runlin trace third restart_paxos_groups end");
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(expected_log_tail, leader.palf_handle_impl_->get_max_lsn());
  }
  // 模拟最后一条的LogEntryHeadr bit位反转, 报错OB_INVALID_DATA, 重启成功，预期log_tail是该日志头
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 100 * 1024));
    const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 100 * 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
    PalfGroupBufferIterator iterator;
    LogGroupEntry entry;
    LSN curr_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(max_lsn, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
    EXPECT_EQ(curr_lsn, max_lsn);
    // 模拟LogEntry的datachecsum以及后续的数据被置为全0
    // LogEntryHeader 16bit(maigc) 16bit(version) 32bit(size) 64bit(scn) datachecsum
    char *output_buf = NULL;
    int64_t pos = sizeof(LogGroupEntryHeader) + 14;
    DataFaultInject inject = [&pos](char *buf) {
      char ch = buf[pos];
      int random_bit = rand() % 8;
      int bit_value = 1 << random_bit;
      char tmp_ch = (ch ^ bit_value);
      PALF_LOG(INFO, "runlin trace print", K(pos), K(random_bit), K(bit_value), K(ch), K(tmp_ch));
      buf[pos] = tmp_ch;
    };
    EXPECT_EQ(OB_SUCCESS, make_log_group_entry_partial_error(entry, output_buf, inject));
    EXPECT_EQ(OB_SUCCESS, pwrite_one_log_by_log_storage(leader, entry, max_lsn));
    EXPECT_EQ(OB_ITER_END, iterator.next());
    EXPECT_EQ(OB_INVALID_DATA, read_log(leader));
    if (NULL != output_buf) {
      ob_free(output_buf);
    }
    expected_log_tail = curr_lsn;
  }
  PALF_LOG(INFO, "runlin trace fourth restart_paxos_groups begin");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  PALF_LOG(INFO, "runlin trace fourth restart_paxos_groups end");
  // 模拟最后一条的LogGroupEntryHeadr bit位反转, 报错OB_INVALID_DATA, 重启成功，预期log_tail是该日志头
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 100 * 1024));
    const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 100 * 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
    PalfGroupBufferIterator iterator;
    LogGroupEntry entry;
    LSN curr_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(max_lsn, iterator));
    EXPECT_EQ(OB_SUCCESS, iterator.next());
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
    EXPECT_EQ(curr_lsn, max_lsn);
    // 模拟LogGroupEntryHeader bit位反转
    char *output_buf = NULL;
    int64_t pos = 14;
    DataFaultInject inject = [&pos](char *buf) {
      char ch = buf[pos];
      int random_bit = rand() % 8;
      int bit_value = 1 << random_bit;
      char tmp_ch = (ch ^ bit_value);
      PALF_LOG(INFO, "runlin trace print", K(pos), K(random_bit), K(bit_value), K(ch), K(tmp_ch));
      buf[pos] = tmp_ch;
    };
    EXPECT_EQ(OB_SUCCESS, make_log_group_entry_partial_error(entry, output_buf, inject));
    EXPECT_EQ(OB_SUCCESS, pwrite_one_log_by_log_storage(leader, entry, max_lsn));
    EXPECT_EQ(OB_ITER_END, iterator.next());
    EXPECT_EQ(OB_INVALID_DATA, read_log(leader));
    if (NULL != output_buf) {
      ob_free(output_buf);
    }
    expected_log_tail = curr_lsn;
  }
  PALF_LOG(INFO, "runlin trace five restart_paxos_groups begin");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(expected_log_tail, leader.palf_handle_impl_->get_max_lsn());
  }

  PALF_LOG(INFO, "end test accumlate checksum", K(id));
}

TEST_F(TestObSimpleLogDataIntergrity, log_corrupted)
{
  disable_hot_cache_ = true;
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
  entry.header_.group_size_ = 2;
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
