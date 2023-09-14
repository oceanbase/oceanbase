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

#ifndef OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_
#define OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_
#include "mvcc/ob_mvcc_trans_ctx.h"
#include "ob_memtable_mutator.h"
#include "ob_memtable_interface.h"

namespace oceanbase
{
namespace memtable
{

// Represents the callbacks in [start_, end_]
struct ObCallbackScope
{
  ObCallbackScope() : start_(nullptr), end_(nullptr) {}
  ~ObCallbackScope() {}
  void reset()
  {
    start_.reset();
    end_.reset();
  }
  bool is_empty() const { return (nullptr == *start_) || (nullptr == *end_); }
  ObITransCallbackIterator start_;
  ObITransCallbackIterator end_;
};

struct ObRedoLogSubmitHelper
{
  ObRedoLogSubmitHelper() : callbacks_(), max_seq_no_(), data_size_(0) {}
  ~ObRedoLogSubmitHelper() {}
  void reset()
  {
    callbacks_.reset();
    max_seq_no_.reset();
    data_size_ = 0;
  }
  ObCallbackScope callbacks_; // callbacks in the redo log
  transaction::ObTxSEQ max_seq_no_;
  int64_t data_size_;  // records the data amount of all serialized trans node of this fill process
};

class ObRedoLogGenerator
{
public:
  ObRedoLogGenerator()
      : is_inited_(false),
        redo_filled_cnt_(0),
        redo_sync_succ_cnt_(0),
        redo_sync_fail_cnt_(0),
        generate_cursor_(),
        callback_mgr_(nullptr),
        mem_ctx_(NULL),
        clog_encrypt_meta_(NULL)
  {}
  ~ObRedoLogGenerator()
  {
    if (clog_encrypt_meta_ != NULL) {
      op_free(clog_encrypt_meta_);
      clog_encrypt_meta_ = NULL;
    }
  }
  void reset();
  void reuse();
  int set(ObTransCallbackMgr *mgr, ObIMemtableCtx *mem_ctx);
  int fill_redo_log(char *buf,
                    const int64_t buf_len,
                    int64_t &buf_pos,
                    ObRedoLogSubmitHelper &helper,
                    const bool log_for_lock_node);
  int search_unsubmitted_dup_tablet_redo();
  int log_submitted(const ObCallbackScope &callbacks);
  int sync_log_succ(const share::SCN scn, const ObCallbackScope &callbacks);
  void sync_log_fail(const ObCallbackScope &callbacks);
  ObITransCallback *get_generate_cursor() { return (ObITransCallback *)*generate_cursor_; }

  int64_t get_redo_filled_count() const { return redo_filled_cnt_; }
  int64_t get_redo_sync_succ_count() const { return redo_sync_succ_cnt_; }
  int64_t get_redo_sync_fail_count() const { return redo_sync_fail_cnt_; }
  void print_first_mvcc_callback();
private:
  int fill_row_redo(ObITransCallbackIterator &cursor,
                    ObMutatorWriter &mmw,
                    RedoDataNode &redo,
                    const bool log_for_lock_node,
                    bool &fake_fill,
                    transaction::ObCLogEncryptInfo &encrypt_info);
  int fill_table_lock_redo(ObITransCallbackIterator &cursor,
                           ObMutatorWriter &mmw,
                           TableLockRedoDataNode &redo,
                           const bool log_for_lock_node,
                           bool &fake_fill);
  bool check_dup_tablet_(const ObITransCallback * callback_ptr) const;
  void bug_detect_for_logging_blocked_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObRedoLogGenerator);
  bool is_inited_;
  int64_t redo_filled_cnt_;
  int64_t redo_sync_succ_cnt_;
  int64_t redo_sync_fail_cnt_;
  ObITransCallbackIterator generate_cursor_; // the pos of callback which already generated log
  ObTransCallbackMgr *callback_mgr_;
  ObIMemtableCtx *mem_ctx_;
  transaction::ObTxEncryptMeta *clog_encrypt_meta_;

  // logging block bug detector
  int64_t last_logging_blocked_time_;
};
}; // end namespace memtable
}; // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_ */

