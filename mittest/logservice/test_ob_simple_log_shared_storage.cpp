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
#undef private
#undef protected

const std::string TEST_NAME = "log_shared_storage";

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
      return OB_SUCCESS;
    }
    void destroy()
    {}
    int64_t id_;
  };

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = true;

TEST_F(TestObSimpleLogSharedStorage, test_iterator_interface)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_iterator_interface");
  OB_LOGGER.set_log_level("TRACE");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    share::SCN create_scn = share::SCN::base_scn();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, create_scn, leader_idx, leader));
    std::vector<LSN> lsns;
    std::vector<SCN> scns;
    // submit 1024 logs, and length of each log is 100
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1024, 100, leader_idx, lsns, scns));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    // case1. test seek buffer iterator from lsn
    CLOG_LOG(INFO, "begin seek iterator from lsn");
    {
      PalfIterator<LogEntry> iterator;
      for (int i = 10; i < 10; i++) {
        LSN start_lsn = lsns[i];
        LSN curr_lsn;
        LogEntry entry;
        EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), start_lsn, iterator));
        EXPECT_EQ(OB_SUCCESS, iterator.next());
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
        EXPECT_EQ(curr_lsn, start_lsn);
        EXPECT_EQ(entry.get_scn(), scns[i]);
      }
    }
    CLOG_LOG(INFO, "end seek buffer iterator from lsn");
    CLOG_LOG(INFO, "begin seek iterator from scn");
    // case2. test seek iterator from scn
    {
      PalfIterator<LogEntry> iterator;
      for (int i = 10; i < 20; i++) {
        SCN start_scn = scns[i];
        LSN curr_lsn;
        LogEntry entry;
        EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), start_scn, iterator));
        EXPECT_EQ(OB_SUCCESS, iterator.next());
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
        EXPECT_EQ(entry.get_scn(), scns[i]);
        EXPECT_EQ(curr_lsn, lsns[i]);
      }
    }
    lsns.clear();
    scns.clear();
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 4, MAX_LOG_BODY_SIZE, leader_idx, lsns, scns));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    // case3. test seek group iterator from lsn
    {
      PalfIterator<LogGroupEntry> iterator;
      for (int i = 0; i < 3; i++) {
        SCN start_scn = scns[i];
        LSN curr_lsn;
        LogGroupEntry entry;
        EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), start_scn, iterator));
        EXPECT_EQ(OB_SUCCESS, iterator.next());
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
        EXPECT_EQ(entry.get_scn(), scns[i]);
        EXPECT_EQ(curr_lsn+sizeof(LogGroupEntryHeader), lsns[i]);
      }
    }
    // case4. test seek group iterator from scn
    {
      PalfIterator<LogGroupEntry> iterator;
      for (int i = 0; i < 3; i++) {
        SCN start_scn = scns[i];
        LSN curr_lsn;
        LogGroupEntry entry;
        EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), start_scn, iterator));
        EXPECT_EQ(OB_SUCCESS, iterator.next());
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
        EXPECT_EQ(entry.get_scn(), scns[i]);
        EXPECT_EQ(curr_lsn+sizeof(LogGroupEntryHeader), lsns[i]);
      }
    }
    // case5. test upper bound
    {
      PalfIterator<LogGroupEntry> iterator1;
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, seek_log_iterator(ObLSID(id), SCN::max_scn(), iterator1));
      EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), SCN::min_scn(), iterator1));
      PalfIterator<LogEntry> iterator2;
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, seek_log_iterator(ObLSID(id), SCN::max_scn(), iterator2));
      EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), SCN::min_scn(), iterator2));
    }
    CLOG_LOG(INFO, "end seek iterator from scn");
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
