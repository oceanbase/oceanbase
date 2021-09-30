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

#include "ob_log_trans_ctx.h"                       // ObLogDepParser
#include "common/ob_clock_generator.h"              // ObClockGenerator
#include "ob_log_trans_task.h"                      // PartTransTask
#include "ob_log_trans_ctx_mgr.h"                   // ObLogTransCtxMgr
#include "test_log_part_mgr_mock.h"                 // MockObLogPartMgr
#include "ob_log_common.h"                          // MAX_CACHED_TRANS_CTX_COUNT

using namespace oceanbase::common;
using namespace oceanbase::liboblog;
using namespace oceanbase::transaction;

class TransCtxTest : public ::testing::Test
{
public:
  static const int64_t PART_TRANS_TASK_ARRAY_SIZE = 10;
  typedef ObSEArray<PartTransTask *, PART_TRANS_TASK_ARRAY_SIZE> PartTransTaskArray;

public:
  TransCtxTest();
  virtual ~TransCtxTest();
  virtual void SetUp();
  virtual void TearDown();

public:
  bool is_exist(const TransCtx::ReverseDepSet &reverse_dep_set, const ObTransID  &trans_id) const
  {
    bool ret = false;
    TransCtx::ReverseDepSet::const_iterator_t itor = reverse_dep_set.begin();
    for (; itor != reverse_dep_set.end(); ++itor) {
      if (trans_id == *itor) {
        ret = true;
        break;
      }
    }

    return ret;
  }

  bool is_exist(const TransCtx::TransIDArray dep_parsed_reverse_deps,
                const ObTransID  &trans_id) const
  {
    bool ret = false;
    ObTransID trans_id_cmp;
    for (int64_t index = 0; index < dep_parsed_reverse_deps.count(); index++) {
      EXPECT_EQ(OB_SUCCESS, dep_parsed_reverse_deps.at(index, trans_id_cmp));
      if (trans_id == trans_id_cmp) {
        ret = true;
        break;
      }
    }
    return ret;
  }

  void init_trans_ctx(const ObTransID &trans_id, TransCtx *&trans_ctx, const bool enable_create)
  {
    EXPECT_TRUE(NULL != trans_ctx_mgr_);
    EXPECT_TRUE(OB_SUCCESS == trans_ctx_mgr_->get_trans_ctx(trans_id, trans_ctx, enable_create));
    EXPECT_TRUE(NULL != trans_ctx);
    EXPECT_TRUE(OB_SUCCESS == trans_ctx->set_trans_id(trans_id));
    EXPECT_TRUE(OB_SUCCESS == trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY));
  }

  IObLogPartMgr *create_part_mgr()
  {
    IObLogPartMgr *part_mgr = NULL;
    if (NULL != (part_mgr = (MockObLogPartMgr *)ob_malloc(sizeof(MockObLogPartMgr),
                                                          ObModIds::OB_LOG_PART_INFO))) {
      new(part_mgr)MockObLogPartMgr();
    }
    return part_mgr;
  }

  IObLogTransCtxMgr *create_trans_mgr()
  {
    ObLogTransCtxMgr *tx_mgr = NULL;
    if (NULL != (tx_mgr = (ObLogTransCtxMgr *)ob_malloc(sizeof(ObLogTransCtxMgr),
                                                        ObModIds::OB_LOG_TRANS_CTX))) {
      new(tx_mgr)ObLogTransCtxMgr();
      if (OB_SUCCESS != tx_mgr->init(MAX_CACHED_TRANS_CTX_COUNT)) {
        tx_mgr->~ObLogTransCtxMgr();
        ob_free(tx_mgr);
        tx_mgr = NULL;
      }
    }
    return tx_mgr;
  }

  void destroy()
  {
    trans_ctx_.reset();
    trans_id_.reset();
    part_trans_task_.reset();
    if (NULL != part_mgr_) {
      part_mgr_->~IObLogPartMgr();
      ob_free(part_mgr_);
      part_mgr_ = NULL;
    }

    if (NULL != trans_ctx_mgr_) {
      trans_ctx_mgr_->~IObLogTransCtxMgr();
      ob_free(trans_ctx_mgr_);
      trans_ctx_mgr_ = NULL;
    }
  }

  void init_part_trans_task_array(PartTransTaskArray &array, const ObTransID &trans_id)
  {
    EXPECT_TRUE(trans_id.is_valid());

    PartTransTask *part_trans_task = NULL;
    for (int i = 0; i < PART_TRANS_TASK_ARRAY_SIZE; i++) {
      init_part_trans_task(part_trans_task, trans_id);
      EXPECT_EQ(OB_SUCCESS, array.push_back(part_trans_task));
    }
  }

  void free_part_trans_task_array(PartTransTaskArray &array)
  {
    PartTransTask *part_trans_task = NULL;
    for (int i = 0; i < PART_TRANS_TASK_ARRAY_SIZE; i++) {
      EXPECT_EQ(OB_SUCCESS, array.at(i, part_trans_task));
      free_part_trans_task(part_trans_task);
    }
  }

  void init_part_trans_task(PartTransTask *&part_trans_task, const ObTransID &trans_id)
  {
    EXPECT_TRUE(trans_id.is_valid());

    if (NULL != (part_trans_task = (PartTransTask *)ob_malloc(sizeof(PartTransTask),
                                                              ObModIds::OB_LOG_PART_TRANS_TASK_SMALL))) {
      new(part_trans_task)PartTransTask();
      part_trans_task->set_trans_id(trans_id);
      part_trans_task->set_ref_cnt(0);
      part_trans_task->set_pool(NULL);
    }
  }

  void free_part_trans_task(PartTransTask *part_trans_task)
  {
    if (NULL != part_trans_task) {
      part_trans_task->~PartTransTask();
      ob_free(part_trans_task);
      part_trans_task = NULL;
    }
  }


public:
  TransCtx trans_ctx_;
  ObTransID trans_id_;
  PartTransTask part_trans_task_;
  IObLogPartMgr *part_mgr_;
  IObLogTransCtxMgr *trans_ctx_mgr_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TransCtxTest);
};

TransCtxTest::TransCtxTest(): trans_ctx_(),
                              trans_id_(),
                              part_trans_task_(),
                              part_mgr_(NULL),
                              trans_ctx_mgr_(NULL)
{
}

TransCtxTest::~TransCtxTest()
{
}

void TransCtxTest::SetUp()
{
  const ObAddr svr(ObAddr::IPV4, "127.0.0.1", 1000);
  trans_id_ = ObTransID(svr);
  part_trans_task_.set_trans_id(trans_id_);
  EXPECT_TRUE(NULL != (part_mgr_ = create_part_mgr()));
  EXPECT_TRUE(NULL != (trans_ctx_mgr_ = create_trans_mgr()));
  trans_ctx_.set_host(trans_ctx_mgr_);
}

void TransCtxTest::TearDown()
{
  destroy();
}

TEST_F(TransCtxTest, prepare_failed)
{
  bool stop_flag = false;
  bool need_discard = false;
  IObLogPartMgr *part_mgr_null = NULL;

  // 1. If part mrg is null
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_ctx_.prepare(part_trans_task_, part_mgr_null, stop_flag,
                                                    need_discard));

  // 2. If the state is TRANS_CTX_STATE_DISCARDED, prepare returns an error
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_DISCARDED);
  EXPECT_EQ(OB_INVALID_ERROR, trans_ctx_.prepare(part_trans_task_, part_mgr_, stop_flag,
                                                 need_discard));
}

TEST_F(TransCtxTest, prepare_discard)
{
  bool stop_flag = false;
  bool need_discard = false;
  const int64_t prepare_tstamp = 1452763000;

  // prepare partition key
  ObPartitionKey partition_key_0;
  partition_key_0.init(1000000000, 0, 3);

  // Make the prepare log timestamp less than the specified timestamp
  part_trans_task_.set_partition(partition_key_0);
  part_trans_task_.set_timestamp(prepare_tstamp);
  part_trans_task_.set_prepare_log_id(1);

  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_INVALID);

  // Current transaction not in service, need discard
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.prepare(part_trans_task_, part_mgr_, stop_flag, need_discard));
  EXPECT_TRUE(need_discard);
}

TEST_F(TransCtxTest, prepare_success)
{
  bool stop_flag = false;
  bool need_discard = false;
  const int64_t prepare_tstamp = 1452763900;

  // prepare partition key
  ObPartitionKey partition_key_0;
  partition_key_0.init(1000000000, 0, 3);
  ObPartitionKey partition_key_1;
  partition_key_1.init(1000000000, 1, 3);
  ObPartitionKey partition_key_2;
  partition_key_2.init(1000000000, 2, 3);

  // If the current partitioned transaction service has 2 service participants, verify that the participants are obtained correctly
  // Make the prepare log timestamp greater than the specified timestamp
  part_trans_task_.set_partition(partition_key_0);
  part_trans_task_.set_timestamp(prepare_tstamp);
  part_trans_task_.set_prepare_log_id(1);
  ObPartitionLogInfo part_info_0(partition_key_0, 1, prepare_tstamp);
  ObPartitionLogInfo part_info_1(partition_key_1, 1, 1452763999);
  ObPartitionLogInfo part_info_2(partition_key_2, 1, 1452763000);
  PartitionLogInfoArray participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_0));
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_1));
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_2));
  EXPECT_EQ(OB_SUCCESS, part_trans_task_.set_participants(participants));

  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_INVALID);

  EXPECT_EQ(OB_SUCCESS, trans_ctx_.prepare(part_trans_task_, part_mgr_, stop_flag, need_discard));
  EXPECT_FALSE(need_discard);
  const TransPartInfo *valid_participants = trans_ctx_.get_participants();
  int64_t valid_participant_count = trans_ctx_.get_participant_count();
  int64_t participants_count = valid_participant_count;
  EXPECT_EQ(2, participants_count);
  for (int64_t index = 0; index < participants_count; index++) {
    EXPECT_FALSE(partition_key_2 == valid_participants[index].pkey_);
  }
}

TEST_F(TransCtxTest, add_participant_failed)
{
  bool is_part_trans_served = true;
  bool is_all_participants_ready = false;

  // 1. The current state is not advanced to the PREPARE state
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_INVALID);
  EXPECT_EQ(OB_STATE_NOT_MATCH, trans_ctx_.add_participant(part_trans_task_, is_part_trans_served,
                                                           is_all_participants_ready));

  // 2. The current state is already ready, the current participant will not be gathered
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY);
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.add_participant(part_trans_task_, is_part_trans_served,
                                                   is_all_participants_ready));
  EXPECT_FALSE(is_part_trans_served);
}

TEST_F(TransCtxTest, add_participant_not_served)
{
  bool stop_flag = false;
  bool need_discard = false;
  bool is_part_trans_served = true;
  bool is_all_participants_ready = false;
  const int64_t prepare_tstamp = 1452763900;

  // prepare partition key
  ObPartitionKey partition_key_0;
  partition_key_0.init(1000000000, 0, 3);
  ObPartitionKey partition_key_1;
  partition_key_1.init(1000000000, 1, 3);

  // Make the prepare log timestamp greater than the specified timestamp
  part_trans_task_.set_partition(partition_key_0);
  part_trans_task_.set_timestamp(prepare_tstamp);
  part_trans_task_.set_prepare_log_id(1);
  ObPartitionLogInfo part_info_0(partition_key_0, 1, prepare_tstamp);
  ObPartitionLogInfo part_info_1(partition_key_1, 1, 1452763999);
  PartitionLogInfoArray participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_0));
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_1));
  part_trans_task_.set_participants(participants);

  // 先prepare一次
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_INVALID);

  EXPECT_EQ(OB_SUCCESS, trans_ctx_.prepare(part_trans_task_, part_mgr_, stop_flag, need_discard));
  EXPECT_FALSE(need_discard);

  // 构造一个partition key，不在参与者列表里
  PartTransTask part_trans_task_new;
  part_trans_task_new.set_trans_id(trans_id_);
  ObPartitionKey partition_key_new;
  partition_key_new.init(1000000000, 2, 3);
  part_trans_task_new.set_partition(partition_key_new);

  // 当前是prepare状态 但partition不在参与者列表中
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_PREPARED);
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.add_participant(part_trans_task_new, is_part_trans_served,
                                                   is_all_participants_ready));
  EXPECT_FALSE(is_part_trans_served);
  EXPECT_FALSE(is_all_participants_ready);
}

TEST_F(TransCtxTest, add_participant_all_push_in_ready)
{
  bool stop_flag = false;
  bool need_discard = false;
  bool is_part_trans_served = true;
  bool is_all_participants_ready = false;
  const int64_t prepare_tstamp = 1452763900;

  // prepare partition key
  ObPartitionKey partition_key_0;
  partition_key_0.init(1000000000, 0, 3);
  ObPartitionKey partition_key_1;
  partition_key_1.init(1000000000, 1, 3);

  // Make the prepare log timestamp greater than the specified timestamp
  part_trans_task_.set_partition(partition_key_0);
  part_trans_task_.set_timestamp(prepare_tstamp);
  part_trans_task_.set_prepare_log_id(1);
  ObPartitionLogInfo part_info_0(partition_key_0, 1, prepare_tstamp);
  ObPartitionLogInfo part_info_1(partition_key_1, 1, 1452763999);
  PartitionLogInfoArray participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_0));
  EXPECT_EQ(OB_SUCCESS, participants.push_back(part_info_1));
  part_trans_task_.set_participants(participants);

  // Prepare first, generating a list of participants for all services
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_INVALID);
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.prepare(part_trans_task_, part_mgr_, stop_flag, need_discard));
  EXPECT_FALSE(need_discard);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PREPARED, trans_ctx_.get_state());

  PartTransTask part_trans_task_2;
  part_trans_task_2.set_trans_id(trans_id_);
  part_trans_task_2.set_partition(partition_key_1);
  part_trans_task_2.set_timestamp(prepare_tstamp + 100);
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.prepare(part_trans_task_2, part_mgr_, stop_flag, need_discard));
  EXPECT_FALSE(need_discard);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PREPARED, trans_ctx_.get_state());

  // 1.Currently in prepare state: partition 1 is in the participants list, then it is added to the ready list, but has not yet reached the ready state
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.add_participant(part_trans_task_, is_part_trans_served,
                                                   is_all_participants_ready));
  EXPECT_TRUE(is_part_trans_served);
  EXPECT_FALSE(is_all_participants_ready);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PREPARED, trans_ctx_.get_state());

  // 2.All partitions have been added to the ready list and should be in the ready state
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.add_participant(part_trans_task_2, is_part_trans_served,
                                                   is_all_participants_ready));
  EXPECT_TRUE(is_part_trans_served);
  EXPECT_TRUE(is_all_participants_ready);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY, trans_ctx_.get_state());
}

TEST_F(TransCtxTest, parse_deps_failed)
{
  IObLogTransCtxMgr *trans_ctx_mgr = NULL;
  bool all_deps_cleared = false;
  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_PREPARED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, trans_ctx_.parse_deps(trans_ctx_mgr, all_deps_cleared));
  EXPECT_FALSE(all_deps_cleared);

  trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY);
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_ctx_.parse_deps(trans_ctx_mgr, all_deps_cleared));
  EXPECT_FALSE(all_deps_cleared);
}

TEST_F(TransCtxTest, parse_deps_and_sequence)
{
  // Create 4 transactions
  const ObAddr svr1(ObAddr::IPV4, "127.0.0.1", 1000);
  const ObAddr svr2(ObAddr::IPV4, "127.0.0.1", 2000);
  const ObAddr svr3(ObAddr::IPV4, "127.0.0.1", 3000);
  const ObAddr svr4(ObAddr::IPV4, "127.0.0.1", 4000);
  const ObTransID trans_id_1(svr1);
  const ObTransID trans_id_2(svr2);
  const ObTransID trans_id_3(svr3);
  const ObTransID trans_id_4(svr4);

  TransCtx *trans_ctx_1 = NULL;
  TransCtx *trans_ctx_2 = NULL;
  TransCtx *trans_ctx_3 = NULL;
  TransCtx *trans_ctx_4 = NULL;
  bool enable_create = true;

  // init 4 trans_ctx
  init_trans_ctx(trans_id_1, trans_ctx_1, enable_create);
  init_trans_ctx(trans_id_2, trans_ctx_2, enable_create);
  init_trans_ctx(trans_id_3, trans_ctx_3, enable_create);
  init_trans_ctx(trans_id_4, trans_ctx_4, enable_create);

  // set deps of trans
  trans_ctx_1->set_deps(trans_id_2);
  trans_ctx_1->set_deps(trans_id_3);
  trans_ctx_2->set_deps(trans_id_3);
  trans_ctx_3->set_deps(trans_id_4);

  bool all_deps_cleared = false;
  TransCtx::TransIDArray dep_parsed_reverse_deps;

  // 1. trans_ctx 4 can be ordered
  // trans_ctx_4 parses the dependencies (since there are no dependencies, the partitioned transaction parses the end of the dependencies and the state is changed from ready->parsed)
  EXPECT_EQ(OB_SUCCESS, trans_ctx_4->parse_deps(trans_ctx_mgr_, all_deps_cleared));
  EXPECT_TRUE(all_deps_cleared);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_DEP_PARSED, trans_ctx_4->get_state());

  // trans_ctx_4 sequenced
  EXPECT_EQ(OB_SUCCESS, trans_ctx_4->sequence(0, 0));

  // trans_ctx_4 parse reverse deps
  EXPECT_EQ(OB_SUCCESS, trans_ctx_4->parse_reverse_deps(trans_ctx_mgr_, dep_parsed_reverse_deps));
  EXPECT_EQ(0, dep_parsed_reverse_deps.count());

  // 2. trans_ctx 1 cannot be ordered
  // trans_ctx_1 analyses the dependencies and adds the reverse dependency list of 2 and 3, with the status ready, because 2 and 3 are not ordered
  all_deps_cleared = false;
  EXPECT_EQ(OB_SUCCESS, trans_ctx_1->parse_deps(trans_ctx_mgr_, all_deps_cleared));
  EXPECT_FALSE(all_deps_cleared);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY, trans_ctx_1->get_state());
  // Determine if the reverse dependency list for the next 2/3 includes 1
  const TransCtx::ReverseDepSet &reverse_dep_set_2 = trans_ctx_2->get_reverse_dep_set();
  EXPECT_EQ(1, reverse_dep_set_2.count());
  EXPECT_TRUE(is_exist(reverse_dep_set_2, trans_id_1));
  const TransCtx::ReverseDepSet &reverse_dep_set_3 = trans_ctx_3->get_reverse_dep_set();
  EXPECT_EQ(1, reverse_dep_set_3.count());
  EXPECT_TRUE(is_exist(reverse_dep_set_3, trans_id_1));

  // 3.trans_ctx 2 cannot be ordered and will join the set of reverse dependencies of 3
  all_deps_cleared = false;
  EXPECT_EQ(OB_SUCCESS, trans_ctx_2->parse_deps(trans_ctx_mgr_, all_deps_cleared));
  EXPECT_FALSE(all_deps_cleared);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY, trans_ctx_2->get_state());
  // Determine the reverse dependency list for 3, containing 2
  const TransCtx::ReverseDepSet &reverse_dep_set_3_new = trans_ctx_3->get_reverse_dep_set();
  EXPECT_EQ(2, reverse_dep_set_3_new.count());
  EXPECT_TRUE(is_exist(reverse_dep_set_3_new, trans_id_2));

  // 4.trans-ctx 3 can parse deps
  all_deps_cleared = false;
  EXPECT_EQ(OB_SUCCESS, trans_ctx_3->parse_deps(trans_ctx_mgr_, all_deps_cleared));
  EXPECT_TRUE(all_deps_cleared);
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_DEP_PARSED, trans_ctx_3->get_state());

  // trans_ctx_3 sequenced
  EXPECT_EQ(OB_SUCCESS, trans_ctx_3->sequence(1, 1));

  // trans_ctx_3 parse reverse deps
  dep_parsed_reverse_deps.reset();
  EXPECT_EQ(OB_SUCCESS, trans_ctx_3->parse_reverse_deps(trans_ctx_mgr_, dep_parsed_reverse_deps));
  EXPECT_EQ(1, dep_parsed_reverse_deps.count());
  EXPECT_TRUE(is_exist(dep_parsed_reverse_deps, trans_id_2));

  // The set of dependencies of trans 2 is 0 and the set of dependencies of trans 1 is 1
  EXPECT_EQ(0, trans_ctx_2->get_cur_dep_count());
  EXPECT_EQ(1, trans_ctx_1->get_cur_dep_count());

  // 2 Execution of sequencing, reverse decoupling
  dep_parsed_reverse_deps.reset();
  EXPECT_EQ(OB_SUCCESS, trans_ctx_2->sequence(2, 2));
  EXPECT_EQ(OB_SUCCESS, trans_ctx_2->parse_reverse_deps(trans_ctx_mgr_, dep_parsed_reverse_deps));
  EXPECT_EQ(1, dep_parsed_reverse_deps.count());
  EXPECT_EQ(0, trans_ctx_1->get_cur_dep_count());
  EXPECT_TRUE(is_exist(dep_parsed_reverse_deps, trans_id_1));

  // 5.trans_ctx_1 can be sequenced
  dep_parsed_reverse_deps.reset();
  EXPECT_EQ(OB_SUCCESS, trans_ctx_1->sequence(3, 3));
  EXPECT_EQ(OB_SUCCESS, trans_ctx_1->parse_reverse_deps(trans_ctx_mgr_, dep_parsed_reverse_deps));
  EXPECT_EQ(0, dep_parsed_reverse_deps.count());
}

TEST_F(TransCtxTest, format_participant_failed)
{
  TransCtx *trans_ctx = NULL;
  bool enable_create = true;
  init_trans_ctx(trans_id_, trans_ctx, enable_create);

  // 1.Transaction status not in order, error reported
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_DEP_PARSED));
  EXPECT_EQ(OB_STATE_NOT_MATCH, trans_ctx->format_participant(part_trans_task_));

  // 2.Inconsistent transaction id, error reported
  const ObAddr svr(ObAddr::IPV4, "127.0.0.1", 2000);
  const ObTransID trans_id(svr);
  PartTransTask part_trans_task;
  part_trans_task.set_trans_id(trans_id);

  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_ctx->format_participant(part_trans_task));
}

TEST_F(TransCtxTest, format_participant)
{
  TransCtx *trans_ctx = NULL;
  bool enable_create = true;
  init_trans_ctx(trans_id_, trans_ctx, enable_create);

  // Total of 10 partition transactions
  PartTransTaskArray part_trans_task_array;
  init_part_trans_task_array(part_trans_task_array, trans_id_);
  EXPECT_TRUE(PART_TRANS_TASK_ARRAY_SIZE == part_trans_task_array.count());

  // Set the number of ready participants
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_count(PART_TRANS_TASK_ARRAY_SIZE));
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_SEQUENCED));

  // After the first 9 formations, each time the transaction status is not updated, the formated participant count is increased by 1
  int64_t formated_count = 0;
  PartTransTask *part_trans_task = NULL;
  int64_t index = 0;
  for (index = 0; index < PART_TRANS_TASK_ARRAY_SIZE - 1; index++) {
    EXPECT_EQ(OB_SUCCESS, part_trans_task_array.at(index, part_trans_task));
    EXPECT_EQ(OB_SUCCESS, trans_ctx->format_participant(*part_trans_task));
    EXPECT_EQ(++formated_count, trans_ctx->get_formatted_participant_count());
    EXPECT_EQ(TransCtx::TRANS_CTX_STATE_SEQUENCED, trans_ctx->get_state());
  }

  EXPECT_EQ(OB_SUCCESS, part_trans_task_array.at(index, part_trans_task));
  EXPECT_EQ(OB_SUCCESS, trans_ctx->format_participant(*part_trans_task));
  EXPECT_EQ(trans_ctx->get_ready_participant_count(), trans_ctx->get_formatted_participant_count());
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_FORMATTED, trans_ctx->get_state());

  free_part_trans_task_array(part_trans_task_array);
  part_trans_task_array.destroy();
}

TEST_F(TransCtxTest, commit)
{
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_SEQUENCED));
  EXPECT_EQ(OB_STATE_NOT_MATCH, trans_ctx_.commit());

  EXPECT_EQ(OB_SUCCESS, trans_ctx_.set_state(TransCtx::TRANS_CTX_STATE_FORMATTED));
  EXPECT_EQ(OB_SUCCESS, trans_ctx_.commit());
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_COMMITTED, trans_ctx_.get_state());
}

TEST_F(TransCtxTest, release_participants_failed)
{
  TransCtx *trans_ctx = NULL;
  bool enable_create = true;
  init_trans_ctx(trans_id_, trans_ctx, enable_create);

  // 1. The current state is not a commit state and an error is reported
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_SEQUENCED));
  EXPECT_EQ(OB_STATE_NOT_MATCH, trans_ctx->release_participants());

  // 2. Not all parts are currently available for release, 10 partitioned transactions in total
  PartTransTaskArray part_trans_task_array;
  init_part_trans_task_array(part_trans_task_array, trans_id_);
  EXPECT_TRUE(PART_TRANS_TASK_ARRAY_SIZE == part_trans_task_array.count());

  // Set the number of ready participants, the status is commited
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_count(PART_TRANS_TASK_ARRAY_SIZE));
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_COMMITTED));

  // set next participant
  PartTransTask *part_trans_task = NULL;
  PartTransTask *first_part_trans_task = NULL;
  PartTransTask *next_part_trans_task = NULL;
  int64_t index = 0;
  EXPECT_EQ(OB_SUCCESS, part_trans_task_array.at(0, part_trans_task));
  first_part_trans_task = part_trans_task;

  for (index = 0; index < PART_TRANS_TASK_ARRAY_SIZE - 1; index++) {
    EXPECT_EQ(OB_SUCCESS, part_trans_task_array.at(index + 1, next_part_trans_task));
    part_trans_task->set_next_participant(next_part_trans_task);
    part_trans_task = next_part_trans_task;
  }

  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_objs(first_part_trans_task));
  bool all_part_releasable = false;
  for (index = 0; index < PART_TRANS_TASK_ARRAY_SIZE - 1; index++) {
    EXPECT_EQ(OB_SUCCESS, trans_ctx->inc_releasable_participant_count(all_part_releasable));
  }

  // Not all parts are releasable, error reported
  EXPECT_EQ(OB_STATE_NOT_MATCH, trans_ctx->release_participants());
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_COMMITTED, trans_ctx->get_state());

  EXPECT_EQ(OB_SUCCESS, trans_ctx->inc_releasable_participant_count(all_part_releasable));
  EXPECT_TRUE(true == all_part_releasable);

  // 3. Not all part reference counts are 0, error reported
  part_trans_task->set_ref_cnt(2);
  next_part_trans_task->set_ref_cnt(1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, trans_ctx->release_participants());
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_COMMITTED, trans_ctx->get_state());

  free_part_trans_task_array(part_trans_task_array);
  part_trans_task_array.destroy();
}

TEST_F(TransCtxTest, releasd_participants_less_than_ready)
{
  TransCtx *trans_ctx = NULL;
  bool enable_create = true;
  init_trans_ctx(trans_id_, trans_ctx, enable_create);

  PartTransTask *part_trans_task;
  init_part_trans_task(part_trans_task, trans_id_);

  // set count of ready participants, status is commited
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_count(PART_TRANS_TASK_ARRAY_SIZE));
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_COMMITTED));

  // set next participant
  part_trans_task->set_next_participant(NULL);

  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_objs(part_trans_task));
  EXPECT_TRUE(NULL != trans_ctx->get_participant_objs());
  bool all_part_releasable = false;
  for (int index = 0; index < PART_TRANS_TASK_ARRAY_SIZE; index++) {
    EXPECT_EQ(OB_SUCCESS, trans_ctx->inc_releasable_participant_count(all_part_releasable));
  }

  EXPECT_TRUE(true == all_part_releasable);
  EXPECT_EQ(OB_INVALID_ERROR, trans_ctx->release_participants());
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_COMMITTED, trans_ctx->get_state());

  free_part_trans_task(part_trans_task);
}

TEST_F(TransCtxTest, release_participants)
{
  TransCtx *trans_ctx = NULL;
  bool enable_create = true;
  init_trans_ctx(trans_id_, trans_ctx, enable_create);

  // Total of 10 partition transactions
  PartTransTaskArray part_trans_task_array;
  init_part_trans_task_array(part_trans_task_array, trans_id_);
  EXPECT_TRUE(PART_TRANS_TASK_ARRAY_SIZE == part_trans_task_array.count());

  // set count of ready participants, status is commited
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_count(PART_TRANS_TASK_ARRAY_SIZE));
  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_state(TransCtx::TRANS_CTX_STATE_COMMITTED));

  // set ready participant objs
  PartTransTask *part_trans_task = NULL;
  PartTransTask *first_part_trans_task = NULL;
  PartTransTask *next_part_trans_task = NULL;
  int64_t index = 0;

  EXPECT_EQ(OB_SUCCESS, part_trans_task_array.at(0, part_trans_task));
  first_part_trans_task = part_trans_task;
  for (index = 0; index < PART_TRANS_TASK_ARRAY_SIZE - 1; index++) {
    EXPECT_EQ(OB_SUCCESS, part_trans_task_array.at(index + 1, next_part_trans_task));
    part_trans_task->set_next_participant(next_part_trans_task);
    part_trans_task = next_part_trans_task;
  }

  EXPECT_EQ(OB_SUCCESS, trans_ctx->set_ready_participant_objs(first_part_trans_task));
  EXPECT_TRUE(NULL != trans_ctx->get_participant_objs());

  bool all_part_releasable = false;
  for (index = 0; index < PART_TRANS_TASK_ARRAY_SIZE; index++) {
    EXPECT_EQ(OB_SUCCESS, trans_ctx->inc_releasable_participant_count(all_part_releasable));
  }

  EXPECT_TRUE(true == all_part_releasable);
  EXPECT_EQ(OB_SUCCESS, trans_ctx->release_participants());
  EXPECT_EQ(TransCtx::TRANS_CTX_STATE_PARTICIPANT_RELEASED, trans_ctx->get_state());
  EXPECT_TRUE(0 == trans_ctx->get_ready_participant_count());
  EXPECT_TRUE(0 == trans_ctx->get_releasable_participant_count());
  EXPECT_TRUE(0 == trans_ctx->get_formatted_participant_count());
  EXPECT_TRUE(NULL == trans_ctx->get_participant_objs());

  free_part_trans_task_array(part_trans_task_array);
  part_trans_task_array.destroy();
}

int main(int argc, char **argv)
{
  // used for init of ObTransIDTest for incoming length errors
  ObClockGenerator::init();

  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
