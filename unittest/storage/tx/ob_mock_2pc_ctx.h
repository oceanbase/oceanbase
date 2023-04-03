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

#ifndef OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_2PC_CTX
#define OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_2PC_CTX
#include <vector>
#include <deque>
#include <map>

#include "ob_mailbox.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_one_phase_committer.h"
#include "storage/tx/ob_two_phase_committer.h"

namespace oceanbase
{

namespace common
{
template <>
int64_t to_string<std::vector<int64_t>>(const std::vector<int64_t> &obj, char *buffer, const int64_t buffer_size);
}

namespace transaction
{

class MockObParticipants : public std::vector<int64_t>
{
public:
  int64_t to_string(char *buffer, const int64_t size) const;
};

class MockObLogQueue : public std::deque<ObTwoPhaseCommitLogType>
{
public:
  int64_t to_string(char *buffer, const int64_t size) const;
};

// ObTxCtx inherit ObTxCycleTwoPhaseCommitter, ObTxOnePhaseCommitter and ObITxCommitter.
// It organize user interface based on ObITxCommitter. and implements it using both two
// transaction committer, ObTxCycleTwoPhaseCommitter and ObTxOnePhaseCommitter based on
// participants number
class MockOb2pcCtx : public ObTxCycleTwoPhaseCommitter,
                     public ObMailHandler<ObTwoPhaseCommitMsgType>
{
public:
  int init(ObMailBoxMgr<ObTwoPhaseCommitMsgType> *mgr);
  virtual int handle(const ObMail<ObTwoPhaseCommitMsgType>& mail) override;
  int64_t get_addr() { return addr_; }
  int handle(const bool must_have = true);
  int handle_all();
  int apply();
  // participant abnormally abort itself
  int abort();
  bool check_status_valid(const bool should_commit);

  // transaction commit implementation. The transaction can be committed in one phase
  // (with one consensus round and zero transport round latency) if it involves only
  // one participant. Otherwise the transaction will invoke the circular two phase
  // commit(with one consensus round and 2*H transport round latency)
  int commit(const MockObParticipants& participants);

  INHERIT_TO_STRING_KV("ObTxCycleTwoPhaseCommitter",
                       ObTxCycleTwoPhaseCommitter,
                       K_(addr),
                       K_(mailbox),
                       K_(upstream_state),
                       K_(downstream_state),
                       K_(tx_state),
                       K_(log_queue),
                       K_(participants),
                       K_(coordinator),
                       K_(sender));
protected:
  // Oceanbase's optimized transaction commit hook for its user.
  // We promise the failure atomicity of the method and we mainly use these method
  // to solve the concurrency control problem.
  //
  // For example, we use do/on_prepare to build the prepare version and mvcc_write
  // to maintain read write conflict. We also use on_commit/abort to release lock and
  // decide final transaction state. In Oceanbase's optimized, do_pre_commit is used to
  // optimize single machine read latency and do/on_clear is used to maintain the state
  // to recovery
  virtual int do_prepare(bool &no_need_submit_log) override;
  virtual int on_prepare() override;
  virtual int do_pre_commit(bool& need_wait) override;
  virtual int do_commit() override;
  virtual int on_commit() override;
  virtual int do_abort() override;
  virtual int on_abort() override;
  virtual int do_clear() override;
  virtual int on_clear() override;
  // for xa
  virtual int reply_to_scheduler_for_sub2pc(int64_t msg_type) override;


  // We implements the above concurrency control between transaction commit and transfer
  // It's based on that if 2pc log is ahead transfer log, the transfer procedure need bring
  // the 2pc state to the dst. Otherwise, the 2pc will invoke a cycle style commit
  // which the parent node waits for the node's 2pc state response before responsing
  // its 2pc state to his parent node.
  virtual int64_t get_downstream_size() const override;
  virtual int64_t get_self_id() override;
  virtual Ob2PCRole get_2pc_role() const override;
  virtual ObTxState get_downstream_state() const override;
  virtual int set_downstream_state(const ObTxState state) override;
  virtual ObTxState get_upstream_state() const override;
  virtual int set_upstream_state(const ObTxState state) override;
  virtual bool is_2pc_logging() const override;
  // for xa
  virtual bool is_sub2pc() const override;


  // Oceanbase's optimized log handler, if it returns success, the log is definitely proposed
  // to the consensus layer and we can rely on its sequential commitment to submit the log
  // without waiting consensus commit. While the log may be lost under consensus, so we need
  // handle it with exception handler.
  virtual int submit_log(const ObTwoPhaseCommitLogType& log_type) override;
  // The msg poster is best effort, so we need rely on the timeout handler to retry the last
  // message should be sent
  // virtual int post_msg(const ObTwoPhaseCommitMsgType& msg_type) override;
  virtual int post_msg(const ObTwoPhaseCommitMsgType& msg_type,
                       const int64_t participant_id) override;
private:
  int64_t find_participant_id(int64_t participant_key);
  virtual int apply_2pc_msg_(const ObTwoPhaseCommitMsgType msg_type) override;

private:
  common::ObSpinLock latch_;
  int64_t addr_;
  ObMailBox<ObTwoPhaseCommitMsgType> mailbox_;
  ObTxState upstream_state_;
  ObTxState downstream_state_;
  // final state for tx commit
  ObTxState tx_state_;
  MockObLogQueue log_queue_;
  int64_t coordinator_;
  int64_t sender_;
  MockObParticipants participants_;

  ObMailBoxMgr<ObTwoPhaseCommitMsgType>* mailbox_mgr_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_2PC_CTX
