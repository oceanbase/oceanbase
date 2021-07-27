// Copyright 2014 Alibaba Inc. All Rights Reserved.
// Author:
//     zhenzhong.jzz@alibaba-inc.com
// Owner:
//     zhenzhong.jzz@alibaba-inc.com
//
// This file is for unit test of ObLogReconfirm

#include "clog/ob_log_reconfirm.h"
#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"
#include "clog/ob_log_state_mgr.h"
#include "clog/ob_i_net_log_buffer.h"
#include "clog/ob_i_disk_log_buffer.h"
#include "clog/ob_i_net_log_buffer_mgr.h"
#include "clog/ob_i_log_engine.h"
#include "clog/ob_log_define.h"
#include "test_accessor.h"
#include "lib/utility/ob_tracepoint.h"
#include "clog_mock_container/mock_log_membership_mgr.h"
#include "clog_mock_container/mock_log_engine.h"
#include "clog_mock_container/mock_log_replay_engine.h"
#include "clog_mock_container/mock_log_allocator.h"
#include "clog_mock_container/mock_log_state_mgr.h"
#include "clog_mock_container/mock_submit_log_cb.h"
#include "clog_mock_container/mock_log_sliding_window.h"
using namespace oceanbase::common;

namespace oceanbase
{
using namespace common;
namespace clog
{
class MockLogSWForReconfirm: public ObILogSWForReconfirm
{
public:
  MockLogSWForReconfirm() : start_id_(0), epoch_id_(0), max_id_(0), next_id_(0),
                            submit_ret_(OB_SUCCESS),
                            sw_ret_(OB_SUCCESS), array_(NULL), call_start_working_cnt_(0), call_submit_cnt_(0),
                            get_log_ok_(true), get_log_task_ret_(OB_SUCCESS), use_cnt_(0) {}
  uint64_t get_start_id() const
  {
    return start_id_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }

  int try_update_max_log_id(const uint64_t log_id)
  {
    max_id_ = log_id;
    return OB_SUCCESS;
  }

  int submit_log(const ObLogEntryHeader &header, const char *buff,
                 ObISubmitLogCb *cb)
  {
    UNUSED(header);
    UNUSED(buff);
    UNUSED(cb);
    ATOMIC_INC(&call_submit_cnt_);
    return submit_ret_;
  }
  void revert_log_task(const int64_t *ref)
  {
    UNUSED(ref);
  }
  //The even number returns the log_task that exists,
  //and the odd number returns the local non-existent
  int get_log_task(const uint64_t log_id, ObLogTask *&log_task, const int64_t *&ref)
  {
    int ret = OB_SUCCESS;
    UNUSED(ref);
    if (get_log_ok_) {
      if (array_ != NULL && log_id % 2 == 0) {
        log_task = array_ + log_id;
      } else {
        ret = OB_ERR_NULL_VALUE;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    if (get_log_task_ret_ != OB_SUCCESS && use_cnt_ < 3) {
      use_cnt_++;
      return get_log_task_ret_;
    } else {
      return ret;
    }
  }
  uint64_t get_max_log_id() const
  {
    return max_id_;
  }
  void submit_replay_task(const bool is_appending) { UNUSED(is_appending); }
public:
  uint64_t start_id_;
  int64_t epoch_id_;
  uint64_t max_id_;
  uint64_t next_id_;
  int submit_ret_;
  int sw_ret_;
  ObLogTask *array_;
  int64_t call_start_working_cnt_;
  int64_t call_submit_cnt_;
  bool get_log_ok_;
  int get_log_task_ret_;
  uint32_t use_cnt_;
};

class MockLogStateMgr : public MockObLogStateMgr
{
public:
  MockLogStateMgr() {}
  ~MockLogStateMgr() {}
public:
  void set_proposal_id(common::ObProposalID proposal_id)
  {
    proposal_id_ = proposal_id;
  }
  common::ObProposalID get_proposal_id() const
  {
    return proposal_id_;
  }
public:
  common::ObProposalID proposal_id_;
};
class ReLogAllocator : public MockObLogAllocator
{
public:
  ReLogAllocator()
  {
    alloc_.init(512 * 1024 *1024, 512 * 1024 * 1024, 64 * 1024);
  }
  void *re_alloc(const int64_t sz)
  {
    return malloc(sz);
  }
  void re_free(void *ptr)
  {
    free(ptr);
  }
  common::ObIAllocator *get_re_allocator()
  {
    return &alloc_;
  }
private:
  common::ObConcurrentFIFOAllocator alloc_;

};

class MockLogMembershipMgr : public MockObLogMembershipMgr
{
public:
  MockLogMembershipMgr() {}
  virtual ~MockLogMembershipMgr() {}
public:
  int add_member(const common::ObMember &member)
  {
    return curr_member_list_.add_member(member);
  }
  const common::ObMemberList &get_curr_member_list() const
  {
    return curr_member_list_;
  }
private:
  common::ObMemberList curr_member_list_;
};
}//namespace clog
namespace unittest
{

class ReconfirmStateAccessorForTest
{
public:
  ObLogReconfirm::State get_fetch_max_lsn()
  {
    return ObLogReconfirm::FETCH_MAX_LSN;
  }
  ObLogReconfirm::State get_reconfirming()
  {
    return ObLogReconfirm::RECONFIRMING;
  }
  ObLogReconfirm::State get_start_working()
  {
    return ObLogReconfirm::START_WORKING;
  }
  int64_t get_majority_tag_bit()
  {
    return ObLogReconfirm::MAJORITY_TAG_BIT;
  }
};

class TestReconfirm : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    const uint64_t TABLE_ID = 120;
    const int32_t PARTITION_IDX = 1400;
    const int32_t PARTITION_CNT = 3;
    partition_key_.init(TABLE_ID, PARTITION_IDX, PARTITION_CNT);
    self_.parse_from_cstring("127.0.0.1:8111");
    helper_.set_assert_on(reconfirm_, false);
    common::ObMember self_member(self_, ObTimeUtility::current_time());
    mm_.add_member(self_member);
  }
  virtual void TearDown()
  {
    helper_.set_assert_on(reconfirm_, true);
  }
protected:
  ReconfirmStateAccessorForTest state_accessor_;
  ObLogReconfirmAccessor helper_;
  clog::MockLogSWForReconfirm sw_;
  clog::MockLogStateMgr state_mgr_;
  clog::MockObLogEngine log_engine_;
  ReLogAllocator alloc_;
  MockObSubmitLogCb submit_cb_;

  ObPartitionKey partition_key_;
  ObLogReconfirm reconfirm_;
  MockLogMembershipMgr mm_;
  ObAddr self_;
};

TEST_F(TestReconfirm, reconfirm_test)
{
  ASSERT_EQ(OB_SUCCESS, reconfirm_.init(&sw_, &state_mgr_, &mm_, &log_engine_, &alloc_,
                                        partition_key_, self_));
  //The state is INITED, and the maximum log_id stored by oob
  //is greater than the sliding window
  ASSERT_TRUE(mm_.get_curr_member_list().contains(self_));
  sw_.max_id_ = 15;
  EXPECT_EQ(OB_EAGAIN, reconfirm_.reconfirm());

  //Handle FLUSHING_PREPARE_LOG
  ObProposalID proposal_id_tmp;
  proposal_id_tmp.addr_ = self_;
  proposal_id_tmp.ts_ = 1500;
  state_mgr_.set_proposal_id(proposal_id_tmp);
  proposal_id_tmp.ts_ -= 100;
  helper_.get_new_proposal_id(reconfirm_) = proposal_id_tmp;
  EXPECT_EQ(OB_EAGAIN, reconfirm_.reconfirm());
  //New proposal_id has flushed
  state_mgr_.set_proposal_id(proposal_id_tmp);
  EXPECT_EQ(OB_EAGAIN, reconfirm_.reconfirm());

  helper_.set_state(reconfirm_, state_accessor_.get_reconfirming());
}

TEST_F(TestReconfirm, receive_max_log_id_test)
{
  const uint64_t LOG_ID = 1400;
  ASSERT_EQ(OB_SUCCESS, reconfirm_.init(&sw_, &state_mgr_, &mm_, &log_engine_, &alloc_,
                                        partition_key_, self_));
  int64_t max_log_ts = 0;
  //Incorrect state
  EXPECT_EQ(OB_STATE_NOT_MATCH, reconfirm_.receive_max_log_id(self_, LOG_ID, max_log_ts));
  helper_.set_state(reconfirm_, state_accessor_.get_fetch_max_lsn());
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_max_log_id(self_, LOG_ID, max_log_ts));
  helper_.get_max_log_ack_map(reconfirm_).test_and_set(state_accessor_.get_majority_tag_bit());
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_max_log_id(self_, LOG_ID, max_log_ts));
  helper_.get_max_log_ack_map(reconfirm_).reset_map(state_accessor_.get_majority_tag_bit());
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_max_log_id(self_, LOG_ID, max_log_ts));
}

TEST_F(TestReconfirm, receive_log_test)
{
  ObProposalID proposal_id_tmp;
  proposal_id_tmp.addr_ = self_;
  proposal_id_tmp.ts_ = 1500;
  uint64_t log_id = 2000;
  int64_t data_len = 0;
  int64_t generate_timestamp = 1990;
  int64_t epoch_id = 1800;
  int64_t submit_timestamp = 1990;
  ObLogEntry log_entry;
  ObLogEntryHeader  header;
  common::ObVersion freeze_version;
  ASSERT_EQ(OB_SUCCESS, reconfirm_.init(&sw_, &state_mgr_, &mm_, &log_engine_, &alloc_,
                                        partition_key_, self_));
  EXPECT_TRUE(reconfirm_.need_start_up());
  EXPECT_EQ(OB_STATE_NOT_MATCH, reconfirm_.receive_log(log_entry, self_));
  helper_.set_state(reconfirm_, state_accessor_.get_reconfirming());
  helper_.get_max_flushed_id(reconfirm_) = 1800;
  helper_.prepare_map(reconfirm_);
  sw_.start_id_ = 1000;
  header.generate_header(OB_LOG_SUBMIT, partition_key_, log_id, NULL, data_len, generate_timestamp,
                         epoch_id,
                         proposal_id_tmp, submit_timestamp,
                         freeze_version);
  log_entry.generate_entry(header, NULL);
  EXPECT_EQ(OB_INVALID_ARGUMENT, reconfirm_.receive_log(log_entry, ObAddr()));

  log_id = 100;
  header.generate_header(OB_LOG_SUBMIT, partition_key_, log_id, NULL, data_len, generate_timestamp,
                         epoch_id,
                         proposal_id_tmp, submit_timestamp,
                         freeze_version);
  log_entry.generate_entry(header, NULL);
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_log(log_entry, self_));

  log_id = 1500;
  header.generate_header(OB_LOG_NOT_EXIST, partition_key_, log_id, NULL, data_len, generate_timestamp,
                         epoch_id,
                         proposal_id_tmp, submit_timestamp,
                         freeze_version);
  log_entry.generate_entry(header, NULL);
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_log(log_entry, self_));

  header.generate_header(OB_LOG_SUBMIT, partition_key_, log_id, NULL, data_len, generate_timestamp,
                         epoch_id,
                         proposal_id_tmp, submit_timestamp,
                         freeze_version);
  log_entry.generate_entry(header, NULL);
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_log(log_entry, self_));
  EXPECT_EQ(OB_SUCCESS, reconfirm_.receive_log(log_entry, self_));
}

TEST_F(TestReconfirm, private_test)
{
  ObProposalID proposal_id_tmp;
  proposal_id_tmp.addr_ = self_;
  proposal_id_tmp.ts_ = 1500;
  ObLogEntry log_entry;
  ObLogEntryHeader  header;
  common::ObVersion freeze_version;
  ASSERT_EQ(OB_SUCCESS, reconfirm_.init(&sw_, &state_mgr_, &mm_, &log_engine_, &alloc_,
                                        partition_key_, self_));
  helper_.set_state(reconfirm_, state_accessor_.get_reconfirming());
  helper_.get_max_flushed_id(reconfirm_) = 1800;
  helper_.prepare_map(reconfirm_);

  //Test the processing of try_fetch_log_ under normal circumstances when
  //the starting ID of the sliding window is less than the maximum fetching ID
  sw_.start_id_ = 120;
  helper_.get_start_id(reconfirm_) = 120;
  helper_.get_max_flushed_id(reconfirm_) = 150;
  EXPECT_EQ(OB_SUCCESS, helper_.execute_try_fetch_log(reconfirm_));

  //Test the processing of try_filter_invalid_log_ function when the timestamp
  //and leader_ts disagree
  uint64_t log_id = 1500;
  int64_t data_len = 0;
  int64_t generate_timestamp = 1990;
  int64_t epoch_id = 1800;
  int64_t submit_timestamp = 1990;
  header.generate_header(OB_LOG_SUBMIT, partition_key_, log_id, NULL, data_len, generate_timestamp,
                         epoch_id,
                         proposal_id_tmp,
                         submit_timestamp,
                         freeze_version);
  helper_.get_leader_ts(reconfirm_) = 1600;
  helper_.get_start_id(reconfirm_) = 140;
  helper_.get_next_id(reconfirm_) = 150;
  int idx = 10;
  ObLogEntry *log_array = helper_.get_log_array(reconfirm_);
  (log_array + idx) -> generate_entry(header, NULL);
  EXPECT_EQ(OB_ERR_UNEXPECTED, helper_.execute_try_filter_invalid_log(reconfirm_));

  //Handle start-working log
  log_id = 1500;
  header.generate_header(OB_LOG_START_MEMBERSHIP, partition_key_, log_id, NULL, data_len,
                         generate_timestamp,
                         epoch_id, proposal_id_tmp,
                         submit_timestamp, freeze_version);
  (log_array + idx) -> generate_entry(header, NULL);
  EXPECT_EQ(OB_SUCCESS, helper_.execute_try_filter_invalid_log(reconfirm_));

  //Replace ghost log, may be non-empty log
  epoch_id = 1000;
  helper_.get_start_id(reconfirm_) = 140;
  header.generate_header(OB_LOG_SUBMIT, partition_key_, log_id, NULL, data_len, generate_timestamp,
                         epoch_id,
                         proposal_id_tmp,
                         submit_timestamp, freeze_version);
  (log_array + idx) -> generate_entry(header, NULL);
  EXPECT_EQ(OB_SUCCESS, helper_.execute_try_filter_invalid_log(reconfirm_));

  //  sw_.start_id_ = 100;
  //  helper_.get_start_id(reconfirm_) = 100;
  //  helper_.get_next_id(reconfirm_) = 150;
  //  TP_SET_ERROR("ob_log_reconfirm.cpp", "retry_confirm_log_", "test_c", OB_ERROR);
  //  EXPECT_EQ(OB_ERROR, helper_.execute_retry_confirm_log(reconfirm_));
  //  TP_SET("ob_log_reconfirm.cpp", "retry_confirm_log_", "test_c", NULL);
  //
  //
  //  helper_.get_last_retry_reconfirm_ts(reconfirm_) = 0;
  //  sw_.start_id_ = 100;
  //  helper_.get_last_check_start_id(reconfirm_) = 120;
  //  EXPECT_TRUE(helper_.execute_need_retry_reconfirm(reconfirm_));
  //  sw_.start_id_ = 120;
  //  EXPECT_TRUE(helper_.execute_need_retry_reconfirm(reconfirm_));

  //init_reconfirm, when the maximum logid stored in oob_log_handler_ is greater
  //than in the sliding window, that is, when there is a log that cannot enter the sliding window
  sw_.max_id_ = 1300;
  EXPECT_EQ(OB_SUCCESS, helper_.execute_init_reconfirm(reconfirm_));

  //The operation of the processing code for several cases of failure to obtain
  //the log in get_start_id_and_leader_ts_
  sw_.get_log_task_ret_ = OB_ERROR_OUT_OF_RANGE;
  EXPECT_EQ(OB_SUCCESS, helper_.execute_get_start_id_and_leader_ts(reconfirm_));
}
//majority is 1
TEST(SampleRecnofirmTest, single_member)
{
  ObLogReconfirmAccessor helper;
  MockLogSWForReconfirm sw;
  MockLogStateMgr state_mgr;
  MockObLogEngine log_engine;
  clog::MockObSlidingCallBack sliding_cb;
  ReLogAllocator alloc;
  MockObSubmitLogCb submit_cb;

  ObPartitionKey partition_key;
  ObLogReconfirm reconfirm;
  MockLogMembershipMgr mm;
  ObAddr self;
  self.parse_from_cstring("127.0.0.1:8111");
  common::ObMemberList curr_member_list;
  common::ObMember self_member(self, ObTimeUtility::current_time());

  mm.add_member(self_member);
  ASSERT_EQ(OB_INVALID_ARGUMENT, reconfirm.init(NULL, &state_mgr, &mm, &log_engine, &alloc,
                                                partition_key, self));


  const uint64_t TABLE_ID = 120;
  const int32_t PARTITION_IDX = 1400;
  const int32_t PARTITION_CNT = 3;
  partition_key.init(TABLE_ID, PARTITION_IDX, PARTITION_CNT);
  ASSERT_EQ(OB_SUCCESS, reconfirm.init(&sw, &state_mgr, &mm, &log_engine, &alloc,
                                       partition_key, self
                                      ));
  reconfirm.clear();

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Verify that there are no pending logs
  sw.start_id_ = 1;
  sw.max_id_ = 0;
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());
  //Wait for the new proposal_id to refresh successfully

  // Modify the proposal_id of StateMgr to indicate that the flash disk and
  // the callback are executed successfully
  state_mgr.set_proposal_id(helper.get_new_proposal_id(reconfirm));

  //Verification enters the START_WORKING stage
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());

  // Retransmit after timeout
  usleep(CLOG_LEADER_RECONFIRM_SYNC_TIMEOUT + 100);
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());

  sw.start_id_ = 2;
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());
  reconfirm.clear();


  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Verify that there are three logs that require Reconfirm, and their log_ids are 0, 1, and 2,
  // where log 2 is already in the local confirmation state, log 1 does not exist locally, and
  // log 0 has a generate_header() parameter comparison table locally.

  ObProposalID pid;
  pid.ts_ = 123456;
  pid.addr_ = self;

  sw.start_id_ = 0;
  sw.max_id_ = 2;

  ObLogTask task_array[3];
  ObLogEntryHeader header;
  ObLogSimpleBitMap bit_map;
  common::ObVersion freeze_version;
  ObConfirmedInfo confirmed_info;

  header.generate_header(OB_LOG_SUBMIT, partition_key, 0, NULL, 0, ::oceanbase::common::ObTimeUtility::current_time(), 0,
                         pid,  0, freeze_version);
  task_array[0].init(&alloc, &sliding_cb, &submit_cb, 1);
  task_array[0].set_log(header, NULL, true);

  header.generate_header(OB_LOG_SUBMIT, partition_key, 1, NULL, 0, ::oceanbase::common::ObTimeUtility::current_time(), 0,
                         pid, 0, freeze_version);
  task_array[1].init(&alloc, &sliding_cb, &submit_cb, 1);
  task_array[1].set_log(header, NULL, true);

  header.generate_header(OB_LOG_SUBMIT, partition_key, 2, NULL, 0, ::oceanbase::common::ObTimeUtility::current_time(), 0,
                         pid, 0, freeze_version);
  task_array[2].init(&alloc, &sliding_cb, &submit_cb, 1);
  task_array[2].set_log(header, NULL, true);

  // Set log 2 to confirm the status
  task_array[2].set_confirmed_info(confirmed_info);
  sw.array_ = task_array;

  state_mgr.proposal_id_ = pid;

  EXPECT_EQ(OB_EAGAIN, reconfirm.reconfirm());
  // Set proposal_id to brush disk successfully and call back
  state_mgr.proposal_id_ = helper.get_new_proposal_id(reconfirm);

  //Verification timeout does not form a majority, resend
  usleep(CLOG_LEADER_RECONFIRM_SYNC_TIMEOUT + 1);
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());

  // Move the left edge of the sliding window, the synchronization of pending
  // logs is completed, and enter the writing START_WORKING phase
  sw.start_id_ = sw.max_id_ + 1;
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());

  //Move the left border of the sliding window, START_WORKING is complete,
  //and the RECONFIRM process is complete
  sw.start_id_ = sw.max_id_ + 2;
  ASSERT_EQ(OB_EAGAIN, reconfirm.reconfirm());
  reconfirm.clear();
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
  return 0;
}
