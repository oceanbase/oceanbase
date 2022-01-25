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
#include <vector>
#include <algorithm>

#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"

#include "liboblog/src/ob_log_instance.h"
#include "liboblog/src/ob_log_parser.h"

#include "ob_log_utils.h"               // get_timestamp
#include "liboblog/src/ob_map_queue.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace transaction;
using namespace storage;
using namespace clog;

namespace oceanbase
{
namespace unittest
{
// prepare log
static const int64_t PREPARE_TIMESTAMP = 10 * 1000 * 1000;
// commit log
static const int64_t GLOBAL_TRANS_VERSION = 100;

static const int64_t FIXED_PART_COUNT = 6;
static const ObPartitionLogInfo FIXED_PART_INFO[FIXED_PART_COUNT] =
{
  ObPartitionLogInfo(ObPartitionKey(1000U, 0, 6), 100, PREPARE_TIMESTAMP),
  ObPartitionLogInfo(ObPartitionKey(1000U, 1, 6), 100, PREPARE_TIMESTAMP),
  ObPartitionLogInfo(ObPartitionKey(1000U, 2, 6), 100, PREPARE_TIMESTAMP),
  ObPartitionLogInfo(ObPartitionKey(1000U, 3, 6), 100, PREPARE_TIMESTAMP),
  ObPartitionLogInfo(ObPartitionKey(1000U, 4, 6), 100, PREPARE_TIMESTAMP),
  ObPartitionLogInfo(ObPartitionKey(1000U, 5, 6), 100, PREPARE_TIMESTAMP)
};

/*
 * TransLog Generator 1.
 * Generate single partition transaction logs.
 * Support get trans logs in CORRECT order.
 * Use:
 *  - Call next_trans(), specify trans params.
 *  - Get logs in correct order: redo, redo, ..., prepare, commit/abort.
 */
struct TransParam1
{
  // Params used in trans log.
  ObPartitionKey pkey_;
  ObTransID trans_id_;
  ObAddr scheduler_;
  ObPartitionKey coordinator_;
  ObPartitionArray participants_;
  ObStartTransParam trans_param_;
};

class TransLogGenerator1
{
public:
  TransLogGenerator1()
    : param_(),
    redo_(),
    prepare_(),
    commit_(),
    abort_()
  { }
  virtual ~TransLogGenerator1() { }
public:
  void next_trans(const TransParam1 &param)
  {
    param_ = param;
  }
  const ObTransRedoLog& next_redo(const uint64_t log_id)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;
    redo_.reset();
    ObVersion active_memstore_version(1);
    err = redo_.init(OB_LOG_TRANS_REDO, param_.pkey_, param_.trans_id_,
                     tenant_id, log_id, param_.scheduler_, param_.coordinator_,
                     param_.participants_, param_.trans_param_, cluster_id, active_memstore_version);
    EXPECT_EQ(OB_SUCCESS, err);
    ObTransMutator &mutator = redo_.get_mutator();
    if (NULL == mutator.get_mutator_buf()) {
      mutator.init(true);
    }
    const char *data = "fly";
    char *buf = static_cast<char*>(mutator.alloc(strlen(data)));
    strcpy(buf, data);
    return redo_;
  }
  const ObTransPrepareLog& next_prepare(const ObRedoLogIdArray &all_redos)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;
    ObString trace_id;
    prepare_.reset();
    ObVersion active_memstore_version(1);
    err = prepare_.init(OB_LOG_TRANS_PREPARE, param_.pkey_, param_.trans_id_,
                        tenant_id, param_.scheduler_, param_.coordinator_,
                        param_.participants_, param_.trans_param_,
                        OB_SUCCESS, all_redos, 0, cluster_id, active_memstore_version, trace_id);
    EXPECT_EQ(OB_SUCCESS, err);
    return prepare_;
  }
  const ObTransCommitLog& next_commit(const uint64_t prepare_log_id)
  {
    int err = OB_SUCCESS;
    const uint64_t cluster_id = 1000;
    PartitionLogInfoArray ptl_ids;

    ObPartitionLogInfo ptl_id(param_.pkey_, prepare_log_id, PREPARE_TIMESTAMP);
    err = ptl_ids.push_back(ptl_id);
    EXPECT_EQ(OB_SUCCESS, err);

    // push Fixed participant information
    for (int64_t idx = 0; idx < FIXED_PART_COUNT; ++idx) {
      err = ptl_ids.push_back(FIXED_PART_INFO[idx]);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    commit_.reset();
    err = commit_.init(OB_LOG_TRANS_COMMIT, param_.pkey_, param_.trans_id_,
                       ptl_ids, GLOBAL_TRANS_VERSION, 0, cluster_id);
    EXPECT_EQ(OB_SUCCESS, err);
    return commit_;
  }
  const ObTransAbortLog& next_abort()
  {
    int err = OB_SUCCESS;
    const uint64_t cluster_id = 1000;
    PartitionLogInfoArray array;
    abort_.reset();
    err = abort_.init(OB_LOG_TRANS_ABORT, param_.pkey_, param_.trans_id_, array, cluster_id);
    EXPECT_EQ(OB_SUCCESS, err);
    return abort_;
  }
private:
  TransParam1 param_;
  ObTransRedoLog redo_;
  ObTransPrepareLog prepare_;
  ObTransCommitLog commit_;
  ObTransAbortLog abort_;
};

/*
 * Transaction Log Entry Generator base
 * Generate log entries of transactions.
 */
class TransLogEntryGeneratorBase
{
  static const ObAddr SCHEDULER;
public:
  // Pass in the ObTransID, which can be used to specify different transactions for the same partition
  TransLogEntryGeneratorBase(const ObPartitionKey &pkey, const ObTransID &trans_id)
    : pkey_(pkey),
    log_id_(0),
    remain_log_cnt_(0),
    is_commit_(false),
    param_(),
    trans_log_gen_(),
    prepare_id_(0),
    redos_(),
    redo_cnt_(0),
    data_len_(0)
  {
    param_.pkey_ = pkey_;
    param_.trans_id_ = trans_id;
    param_.scheduler_ = SCHEDULER;
    param_.coordinator_ = pkey_;
    int err = param_.participants_.push_back(pkey_);
    EXPECT_EQ(OB_SUCCESS, err);
    param_.trans_param_.set_access_mode(ObTransAccessMode::READ_WRITE);
    param_.trans_param_.set_isolation(ObTransIsolation::READ_COMMITED);
    param_.trans_param_.set_type(ObTransType::TRANS_NORMAL);

    buf_ = new char[buf_len_];
    EXPECT_TRUE(NULL != buf_);
  }

  virtual ~TransLogEntryGeneratorBase()
  {
    delete[] buf_;
  }

  // Generate normal trans. redo, redo...prepare, commit/abort
  // Start a new trans.
  // Specify the number of redo entries and call next_log_entry to get them in order
  void next_trans(const int64_t redo_cnt, bool is_commit)
  {
    // 正常事务日志总条数＝redo log count + 2(prepare log + commit/abort log)
    remain_log_cnt_ = redo_cnt + 2;
    is_commit_ = is_commit;
    redos_.reset();
    redo_cnt_ = redo_cnt;
    trans_log_gen_.next_trans(param_);
  }

  // Generate: redo, redo... redo-prepare, commit/abort
  // Start a new trans.
  // redo and prepare logs in the same log entry
  void next_trans_with_redo_prepare(const int64_t redo_cnt, bool is_commit)
  {
    next_trans(redo_cnt, is_commit);
    // redo-prepare在同一个log entrey, remain_log_cnt_重新赋值
    remain_log_cnt_ = redo_cnt + 1;
  }

  // Get next log entry.
  // normal trans: 依次获取redo, redo...prepare, commit/abort
  int next_log_entry(clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    if (2 < remain_log_cnt_) {
      next_redo_(log_id_, log_entry);
      // Store redo id.
      int err = redos_.push_back(log_id_);
      EXPECT_EQ(OB_SUCCESS, err);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (2 == remain_log_cnt_) {
      next_prepare_(log_id_, log_entry);
      prepare_id_ = log_id_;
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (1 == remain_log_cnt_ && is_commit_) {
      next_commit_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (1 == remain_log_cnt_ && !is_commit_) {
      next_abort_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else {
      ret = OB_ITER_END;
    }

    return ret;
  }

  // Get next log entry.
  // trans log with redo-prepare: get by order as follows: redo, redo...redo-prepare, commit/abort
  int next_log_entry_with_redo_prepare(clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    if (2 < remain_log_cnt_) {
      next_redo_(log_id_, log_entry);
      // Store redo id.
      int err = redos_.push_back(log_id_);
      EXPECT_EQ(OB_SUCCESS, err);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (2 == remain_log_cnt_) {
      // redo-prepare
      next_redo_with_prepare_(log_id_, log_entry);
      prepare_id_ = log_id_;
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (1 == remain_log_cnt_ && is_commit_) {
      next_commit_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (1 == remain_log_cnt_ && !is_commit_) {
      next_abort_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else {
      ret = OB_ITER_END;
    }

    return ret;
  }
public:
  uint64_t get_log_id()
  {
    return log_id_;
  }
protected:
  // return specified log_id and redo log
  void next_redo_(const uint64_t redo_log_id, clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransRedoLog &redo = trans_log_gen_.next_redo(redo_log_id);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_REDO);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = redo.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, redo_log_id, buf_,
                           data_len_, get_timestamp(), get_timestamp(),
                           ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  // Returns the prepare log with the specified log_id
  void next_prepare_(const uint64_t prepare_log_id, clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransPrepareLog &prepare= trans_log_gen_.next_prepare(redos_);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_PREPARE);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = prepare.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, prepare_log_id, buf_,
                           data_len_, get_timestamp(), get_timestamp(),
                           ObProposalID(), PREPARE_TIMESTAMP, ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_commit_(clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransCommitLog &commit = trans_log_gen_.next_commit(prepare_id_);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_COMMIT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = commit.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_abort_(clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransAbortLog &abort = trans_log_gen_.next_abort();
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_ABORT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = abort.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_redo_with_prepare_(const uint64_t prepare_log_id, clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransRedoLog &redo = trans_log_gen_.next_redo(prepare_log_id);
    const ObTransPrepareLog &prepare= trans_log_gen_.next_prepare(redos_);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_,
                                    pos, OB_LOG_TRANS_REDO_WITH_PREPARE);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);

    err = redo.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    err = prepare.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);

    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, prepare_log_id, buf_,
                           data_len_, get_timestamp(), get_timestamp(),
                           ObProposalID(), PREPARE_TIMESTAMP, ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
protected:
  // Params.
  ObPartitionKey pkey_;
  uint64_t log_id_;
  int64_t remain_log_cnt_;
  // mark current trans is commit or not
  bool is_commit_;
  // Gen.
  TransParam1 param_;
  TransLogGenerator1 trans_log_gen_;
  uint64_t prepare_id_;
  ObRedoLogIdArray redos_;
  int64_t redo_cnt_;
  // Buf.
  int64_t data_len_;
  static const int64_t buf_len_ = 2 * _M_;
  char *buf_;
};

const ObAddr TransLogEntryGeneratorBase::SCHEDULER = ObAddr(ObAddr::IPV4, "127.0.0.1", 5566);

/*
 * test missing redo log, When the prepare log is read, the missing redo can be detected
 *
 * two case：
 * 1. redo, redo, redo...prepare, commit/abort
 * 2. redo, redo, redo...redo-prepare, commit/abort
 */
enum CaseType
{
  NORMAL_TRAN,
  REDO_WITH_PREPARE_TRAN
};
class TransLogEntryGenerator1 : public TransLogEntryGeneratorBase
{
public:
  TransLogEntryGenerator1(const ObPartitionKey &pkey, const ObTransID &trans_id)
  : TransLogEntryGeneratorBase(pkey, trans_id),
    is_first(false),
    miss_redo_cnt_(0)
    {}
  ~TransLogEntryGenerator1() {}
public:
  // Specify the number of redo logs in redo_cnt, and the number of missing redo logs
  void next_trans_with_miss_redo(const int64_t redo_cnt, const int64_t miss_redo_cnt,
                                 bool is_commit, CaseType type)
  {
    if (NORMAL_TRAN == type) {
      next_trans(redo_cnt, is_commit);
    } else if(REDO_WITH_PREPARE_TRAN == type) {
      next_trans_with_redo_prepare(redo_cnt, is_commit);
    } else {
    }
    miss_redo_cnt_ = miss_redo_cnt;
    is_first = true;
  }

  int next_log_entry_missing_redo(CaseType type, clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    // miss_redo_cnt_bars before miss, no return but add redos
    if (is_first) {
      for (int64_t idx = 0; idx < miss_redo_cnt_; idx++) {
        next_redo_(log_id_, log_entry);
        // Store redo id.
        int err = redos_.push_back(log_id_);
        EXPECT_EQ(OB_SUCCESS, err);
        log_id_ += 1;
        remain_log_cnt_ -= 1;
      }
      is_first = false;
    }

    if (NORMAL_TRAN == type) {
      ret = next_log_entry(log_entry);
    } else if(REDO_WITH_PREPARE_TRAN == type) {
      ret = next_log_entry_with_redo_prepare(log_entry);
    } else {
    }

    return ret;
  }

  int next_miss_log_entry(const uint64_t miss_log_id, clog::ObLogEntry &miss_log_entry)
  {
    int ret = OB_SUCCESS;

    next_redo_(miss_log_id, miss_log_entry);

    return ret;
  }

  int get_prepare_log_entry(CaseType type, clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    if (NORMAL_TRAN == type) {
      next_prepare_(prepare_id_, log_entry);
    } else if(REDO_WITH_PREPARE_TRAN == type) {
      next_redo_with_prepare_(prepare_id_, log_entry);
    } else {
    }

    return ret;
  }
private:
  bool is_first;
  int64_t miss_redo_cnt_;
};

struct TransLogInfo
{
  // redo info
  int64_t redo_log_cnt_;
  ObLogIdArray redo_log_ids_;

  // prepare info
  int64_t seq_;
  common::ObPartitionKey partition_;
  int64_t prepare_timestamp_;
  ObTransID trans_id_;
  uint64_t prepare_log_id_;
  uint64_t cluster_id_;

  // commit info
  int64_t global_trans_version_;
  PartitionLogInfoArray participants_;

  void reset()
  {
    redo_log_cnt_ = -1;
    redo_log_ids_.reset();
    seq_ = -1;
    partition_.reset();
    prepare_timestamp_ = -1;
    trans_id_.reset();
    prepare_log_id_ = -1;
    cluster_id_ = -1;
    global_trans_version_ = -1;
    participants_.reset();
  }

  void reset(int64_t redo_cnt, ObLogIdArray &redo_log_ids,
     int64_t seq, const ObPartitionKey partition, int64_t prepare_timestamp,
     ObTransID &trans_id, uint64_t prepare_log_id, uint64_t cluster_id,
     uint64_t global_trans_version, PartitionLogInfoArray &participants)
  {
    reset();

    // redo
    redo_log_cnt_ = redo_cnt;
    redo_log_ids_ = redo_log_ids;
   // prepare
    seq_ = seq;
    partition_ = partition;
    prepare_timestamp_ = prepare_timestamp;
    trans_id_ = trans_id;
    prepare_log_id_ = prepare_log_id;
    cluster_id_ = cluster_id;

    // commmit
    global_trans_version_ = global_trans_version;
    participants_ = participants;
  }
};

/*
 * Mock Parser 1.
 * Read Task, revert it immediately, and count Task number.
 */
class MockParser1 : public IObLogParser
{
public:
  MockParser1() : commit_trans_cnt_(0), abort_trans_cnt_(0), info_queue_(), res_queue_() {}
  virtual ~MockParser1()
  {
    info_queue_.destroy();
    res_queue_.destroy();
  }

  int init()
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(info_queue_.init(MOD_ID))) {
    } else if (OB_FAIL(res_queue_.init(MOD_ID))) {
    } else {
    }

    return ret;
  }

  virtual int start() { return OB_SUCCESS; }
  virtual void stop() { }
  virtual void mark_stop_flag() { }
  virtual int push(PartTransTask *task, const int64_t timeout)
  {
    int ret = OB_SUCCESS;
    UNUSED(timeout);

    if (OB_ISNULL(task)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      TransLogInfo *trans_log_info = NULL;
      int tmp_ret = OB_SUCCESS;

      if (OB_SUCCESS != (tmp_ret = info_queue_.pop(trans_log_info))) {
        // pop error
      } else if(NULL == trans_log_info){
        tmp_ret = OB_ERR_UNEXPECTED;
      } else {
        // do nothing
      }

      bool check_result;
      if (task->is_normal_trans()) {
        // Verify correct data for partitioning tasks
        if (OB_SUCCESS == tmp_ret) {
          check_result = check_nomal_tran(*task, *trans_log_info);
        } else {
          check_result = false;
        }
        task->revert();
        commit_trans_cnt_ += 1;
      } else if (task->is_heartbeat()) {
        // Verify correct data for partitioning tasks
        if (OB_SUCCESS == tmp_ret) {
          check_result = check_abort_tran(*task, *trans_log_info);
        } else {
          check_result = false;
        }
        task->revert();
        abort_trans_cnt_ += 1;
      }

      // Save the validation result, no need to handle a failed push, the pop result will be validated
      if (OB_SUCCESS != (tmp_ret = res_queue_.push(check_result))) {
      }
    }

    return ret;
  }
  virtual int get_pending_task_count(int64_t &task_count)
  {
    UNUSED(task_count);

    return OB_SUCCESS;
  }
  int64_t get_commit_trans_cnt() const { return commit_trans_cnt_; }
  int64_t get_abort_trans_cnt() const { return abort_trans_cnt_; }
  int push_into_queue(TransLogInfo *trans_log_info)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(info_queue_.push(trans_log_info))) {
    } else {
    }

    return ret;
  }
  int get_check_result(bool &result)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(res_queue_.pop(result))) {
    } else {
    }

    return ret;
  }
private:
  // for PartTransTask correctness validation
  // for commit transactions, validate redo/prepare/commit info
  // For abort transactions, since they are converted to heartbeat information, only seq_, partition_, prepare_timestamp_ need to be validated
  bool check_nomal_tran(PartTransTask &task, TransLogInfo &trans_log_info)
  {
    bool bool_ret = true;

    // redo info
    const SortedRedoLogList &redo_list = task.get_sorted_redo_list();
    if (redo_list.log_num_ != trans_log_info.redo_log_cnt_) {
      bool_ret = false;
    } else {
      RedoLogNode *redo_node = redo_list.head_;
      for (int64_t idx = 0; true == bool_ret && idx < trans_log_info.redo_log_cnt_; ++idx) {
        if (redo_node->start_log_id_ == trans_log_info.redo_log_ids_[idx]) {
          // do nothing
        } else {
          bool_ret = false;
        }

        redo_node = redo_node->next_;
      }
    }

    // prepare info
    if (bool_ret) {
      if (trans_log_info.seq_ == task.get_seq()
          && trans_log_info.partition_ == task.get_partition()
          && trans_log_info.prepare_timestamp_ == task.get_timestamp()
          && trans_log_info.trans_id_ == task.get_trans_id()
          && trans_log_info.prepare_log_id_ == task.get_prepare_log_id()
          && trans_log_info.cluster_id_ == task.get_cluster_id()) {
      } else {
        bool_ret = false;
        OBLOG_LOG(INFO, "compare", K(trans_log_info.seq_), K(task.get_seq()));
        OBLOG_LOG(INFO, "compare", K(trans_log_info.partition_), K(task.get_partition()));
        OBLOG_LOG(INFO, "compare", K(trans_log_info.prepare_timestamp_), K(task.get_timestamp()));
        OBLOG_LOG(INFO, "compare", K(trans_log_info.trans_id_), K(task.get_trans_id()));
        OBLOG_LOG(INFO, "compare", K(trans_log_info.prepare_log_id_), K(task.get_prepare_log_id()));
        OBLOG_LOG(INFO, "compare", K(trans_log_info.cluster_id_), K(task.get_cluster_id()));
      }
    }

    //// commit info
    if (bool_ret) {
      if (trans_log_info.global_trans_version_ != task.get_global_trans_version()) {
        bool_ret = false;
      } else {
        const ObPartitionLogInfo *part = task.get_participants();
        const int64_t part_cnt = task.get_participant_count();

        if (trans_log_info.participants_.count() != part_cnt) {
          bool_ret = false;
        } else {
          const ObPartitionLogInfo *pinfo1 = NULL;
          const ObPartitionLogInfo *pinfo2 = NULL;

          for (int64_t idx = 0; true == bool_ret && idx < part_cnt; ++idx) {
            pinfo1 = &trans_log_info.participants_.at(idx);
            pinfo2 = part + idx;

            if (pinfo1->get_partition() == pinfo2->get_partition()
                && pinfo1->get_log_id() == pinfo2->get_log_id()
                && pinfo1->get_log_timestamp() == pinfo2->get_log_timestamp()) {
              // do nothing
            } else {
              bool_ret = false;
            }
          }
        }
      }
    }

    return bool_ret;
  }

  bool check_abort_tran(PartTransTask &task, TransLogInfo &trans_log_info)
  {
    bool bool_ret = true;

    if (trans_log_info.seq_ == task.get_seq()
        && trans_log_info.partition_ == task.get_partition()
        && trans_log_info.prepare_timestamp_ == task.get_timestamp()) {
    } else {
      bool_ret = false;
    }

    return bool_ret;
  }
private:
  static const int64_t MOD_ID = 1;
private:
  int64_t commit_trans_cnt_;
  int64_t abort_trans_cnt_;
  // save TransLogInfo
  ObMapQueue<TransLogInfo*> info_queue_;
  // save verify result
  ObMapQueue<bool> res_queue_;
};

class MockParser2 : public IObLogParser
{
public:
  MockParser2() : commit_trans_cnt_(0) {}
  virtual ~MockParser2() { }

  virtual int start() { return OB_SUCCESS; }
  virtual void stop() { }
  virtual void mark_stop_flag() { }
  virtual int push(PartTransTask *task, const int64_t timeout)
  {
    UNUSED(timeout);
    if (OB_ISNULL(task)) {
    } else if (task->is_normal_trans()) {
      task->revert();
      commit_trans_cnt_ += 1;
    }

    return OB_SUCCESS;
  }
  virtual int get_pending_task_count(int64_t &task_count)
  {
    UNUSED(task_count);

    return OB_SUCCESS;
  }
  int64_t get_commit_trans_cnt() const { return commit_trans_cnt_; }
private:
  int64_t commit_trans_cnt_;
};


}
}
