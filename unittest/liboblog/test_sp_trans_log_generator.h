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
#include "liboblog/src/ob_log_utils.h"            // get_timestamp

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
static const int64_t SP_PREPARE_TIMESTAMP = 10 * 1000 * 1000;
// commit log
static const int64_t SP_GLOBAL_TRANS_VERSION = 100;

// SP Transaction log parameters
struct TransParam2
{
  ObPartitionKey pkey_;
  ObTransID trans_id_;
  ObStartTransParam trans_param_;
};

// Sp Transaction Log Generator
class TransLogGenerator2
{
public:
  TransLogGenerator2()
    : param_(),
    redo_(),
    commit_(),
    abort_()
  { }
  virtual ~TransLogGenerator2() { }
public:
  void next_trans(const TransParam2 &param)
  {
    param_ = param;
  }
  const ObSpTransRedoLog& next_redo(const uint64_t log_id)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;

    redo_.reset();
    ObVersion active_memstore_version(1);
    err = redo_.init(OB_LOG_SP_TRANS_REDO, param_.pkey_, param_.trans_id_,
                     tenant_id, log_id, param_.trans_param_, cluster_id, active_memstore_version);
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
  const ObSpTransCommitLog& next_commit(
      const ObRedoLogIdArray &all_redos,
      const uint64_t redo_log_id)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;
    int64_t checksum = 0;
    ObVersion active_memstore_version(1);
    ObString trace_id;

    commit_.reset();
    err = commit_.init(OB_LOG_SP_TRANS_COMMIT, param_.pkey_, tenant_id, param_.trans_id_,
                       SP_GLOBAL_TRANS_VERSION, checksum, cluster_id, all_redos, param_.trans_param_,
                       active_memstore_version, redo_log_id, trace_id);
    EXPECT_EQ(OB_SUCCESS, err);
    return commit_;
  }
  const ObSpTransAbortLog& next_abort()
  {
    int err = OB_SUCCESS;
    const uint64_t cluster_id = 1000;

    abort_.reset();
    err = abort_.init(OB_LOG_SP_TRANS_ABORT, param_.pkey_, param_.trans_id_, cluster_id);
    EXPECT_EQ(OB_SUCCESS, err);

    return abort_;
  }
  const ObSpTransCommitLog& next_redo_with_commit(
      const ObRedoLogIdArray &all_redos,
      const uint64_t redo_log_id)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;
    int64_t checksum = 0;
    ObVersion active_memstore_version(1);
    ObString trace_id;

    commit_.reset();
    err = commit_.init(OB_LOG_SP_TRANS_COMMIT, param_.pkey_, tenant_id, param_.trans_id_,
                       SP_GLOBAL_TRANS_VERSION, checksum, cluster_id, all_redos, param_.trans_param_,
                       active_memstore_version, redo_log_id, trace_id);
    EXPECT_EQ(OB_SUCCESS, err);

    // write redo log
    ObTransMutator &mutator = commit_.get_mutator();
    if (NULL == mutator.get_mutator_buf()) {
      mutator.init(true);
    }
    const char *data = "fly";
    char *buf = static_cast<char*>(mutator.alloc(strlen(data)));
    strcpy(buf, data);

    return commit_;
  }
private:
  TransParam2 param_;
  ObSpTransRedoLog redo_;
  ObSpTransCommitLog commit_;
  ObSpTransAbortLog abort_;
};

/*
 * Responsible for generating Sp transaction logs
 */
class SpTransLogEntryGeneratorBase
{
  static const ObAddr SCHEDULER;
public:
  // Pass in the ObTransID, which can be used to specify different transactions for the same partition
  SpTransLogEntryGeneratorBase(const ObPartitionKey &pkey, const ObTransID &trans_id)
    : pkey_(pkey),
    log_id_(0),
    remain_log_cnt_(0),
    is_commit_(false),
    param_(),
    trans_log_gen_(),
    redos_(),
    redo_cnt_(0),
    commit_log_id_(-1),
    data_len_(-1)
  {
    param_.pkey_ = pkey_;
    param_.trans_id_ = trans_id;
    param_.trans_param_.set_access_mode(ObTransAccessMode::READ_WRITE);
    param_.trans_param_.set_isolation(ObTransIsolation::READ_COMMITED);
    param_.trans_param_.set_type(ObTransType::TRANS_NORMAL);

    buf_ = new char[buf_len_];
    EXPECT_TRUE(NULL != buf_);
  }

  virtual ~SpTransLogEntryGeneratorBase()
  {
    delete[] buf_;
  }

  // Generate normal Sp logs. redo, redo.... .redo, commit/abort
  // Call next_trans to start a new transaction
  // Call next_log_entry to get the number of redo entries in order by specifying the number of redo entries
  void next_trans(const int64_t redo_cnt, bool is_commit)
  {
    // 正常事务日志总条数＝redo log count + 1(commit/abort log)
    remain_log_cnt_ = redo_cnt + 1;
    is_commit_ = is_commit;
    redos_.reset();
    redo_cnt_ = redo_cnt;
    commit_log_id_ = -1;
    trans_log_gen_.next_trans(param_);
  }

  // Generate special Sp logs: redo, redo.... .redo, redo-commit (redo and commit logs in the same log entry)
  // call next_trans to start a new transaction
  // Call next_log_entry_with_redo_commit to get the number of redo entries in order by specifying
  void next_trans_with_redo_commit(const int64_t redo_cnt)
  {
    next_trans(redo_cnt, true);
    // redo-commit in the same log entrey, remain_log_cnt_reassigned
    remain_log_cnt_ = redo_cnt;
  }

  // Get next log entry.
  // get redo, redo..., commit/abort by order
  int next_log_entry(clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    if (1 < remain_log_cnt_) {
      next_redo_(log_id_, log_entry);
      // Store redo id.
      int err = redos_.push_back(log_id_);
      EXPECT_EQ(OB_SUCCESS, err);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (1 == remain_log_cnt_ && is_commit_) {
      commit_log_id_ = log_id_;
      next_commit_(commit_log_id_, log_entry);
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
  // get redo, redo...redo-commit by order
  int next_log_entry_with_redo_commit(clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    if (1 < remain_log_cnt_) {
      next_redo_(log_id_, log_entry);
      // Store redo id.
      int err = redos_.push_back(log_id_);
      EXPECT_EQ(OB_SUCCESS, err);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    } else if (1 == remain_log_cnt_) {
      // redo-commit
      commit_log_id_ = log_id_;
      next_redo_with_commit_(commit_log_id_, log_entry);
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
  // Returns the redo log with the specified log_id
  void next_redo_(const uint64_t redo_log_id, clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObSpTransRedoLog &redo = trans_log_gen_.next_redo(redo_log_id);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_SP_TRANS_REDO);
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
  void next_commit_(uint64_t commit_log_id, clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObSpTransCommitLog &commit = trans_log_gen_.next_commit(redos_, 1);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_SP_TRANS_COMMIT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = commit.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    // log submit timestamp using SP_PREPARE_TIMESTAMP, because for sp transactions, the partition task stores the prepare timestamp
    // commit log timestamp, for correctness verification
    header.generate_header(OB_LOG_SUBMIT, pkey_, commit_log_id, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), SP_PREPARE_TIMESTAMP, ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_abort_(clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObSpTransAbortLog &abort = trans_log_gen_.next_abort();
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_SP_TRANS_ABORT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = abort.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    // log submit timestamp using SP_PREPARE_TIMESTAMP, because for sp transactions, the partition task stores the prepare timestamp
    // commit log timestamp, for correctness verification
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), SP_PREPARE_TIMESTAMP, ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_redo_with_commit_(uint64_t commit_log_id, clog::ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObSpTransCommitLog &commit = trans_log_gen_.next_redo_with_commit(redos_, 1);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_SP_TRANS_COMMIT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = commit.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    // 日志submit timestamp采用SP_PREPARE_TIMESTAMP, 因为对于sp事务，分区任务存储的prepare时间戳
    // commit日志的时间戳，用于正确性验证
    header.generate_header(OB_LOG_SUBMIT, pkey_, commit_log_id, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), SP_PREPARE_TIMESTAMP, ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
protected:
  // Params.
  ObPartitionKey pkey_;
  uint64_t log_id_;
  int64_t remain_log_cnt_;
  // Indicates whether the current transaction has been committed or not
  bool is_commit_;
  // Gen.
  TransParam2 param_;
  TransLogGenerator2 trans_log_gen_;
  ObRedoLogIdArray redos_;
  int64_t redo_cnt_;
  // prepare log id and commit log id are same for sp trans
  uint64_t commit_log_id_;

  // Buf.
  int64_t data_len_;
  static const int64_t buf_len_ = 2 * _M_;
  char *buf_;
};

/*
 * test missing redo log, When the commit log is read, the missing redo can be detected
 *
 * two case：
 * 1. redo, redo, redo...redo, commit
 * 2. redo, redo, redo...redo, redo-commit
 */
enum SpCaseType
{
  SP_NORMAL_TRAN,
  SP_REDO_WITH_COMMIT_TRAN
};
class SpTransLogEntryGenerator1 : public SpTransLogEntryGeneratorBase
{
public:
  SpTransLogEntryGenerator1(const ObPartitionKey &pkey, const ObTransID &trans_id)
  : SpTransLogEntryGeneratorBase(pkey, trans_id),
    is_first(false),
    miss_redo_cnt_(0)
    {}
  ~SpTransLogEntryGenerator1() {}
public:
  // Specify the number of redo logs in redo_cnt, and the number of missing redo logs
  void next_trans_with_miss_redo(const int64_t redo_cnt,
      const int64_t miss_redo_cnt,
      SpCaseType type)
  {
    if (SP_NORMAL_TRAN == type) {
      next_trans(redo_cnt, true);
    } else if(SP_REDO_WITH_COMMIT_TRAN == type) {
      next_trans_with_redo_commit(redo_cnt);
    } else {
    }
    miss_redo_cnt_ = miss_redo_cnt;
    is_first = true;
  }

  int next_log_entry_missing_redo(SpCaseType type, clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    // add redo log to redos list for miss_redo_cnt_ logs before miss
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

    if (SP_NORMAL_TRAN == type) {
      ret = next_log_entry(log_entry);
    } else if(SP_REDO_WITH_COMMIT_TRAN == type) {
      ret = next_log_entry_with_redo_commit(log_entry);
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

  int get_commit_log_entry(SpCaseType type, clog::ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;

    if (SP_NORMAL_TRAN == type) {
      next_commit_(commit_log_id_, log_entry);
    } else if(SP_REDO_WITH_COMMIT_TRAN == type) {
      next_redo_with_commit_(commit_log_id_, log_entry);
    } else {
    }

    return ret;
  }

private:
  bool is_first;
  int64_t miss_redo_cnt_;
};


}
}
