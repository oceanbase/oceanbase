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

#ifndef OCEANBASE_STORAGE_TX_OB_ONE_PHASE_COMMITTER
#define OCEANBASE_STORAGE_TX_OB_ONE_PHASE_COMMITTER
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/tx/ob_committer_define.h"

namespace oceanbase
{
namespace transaction
{
// ObTxOnePhaseCommitter represents an instance of single participant transaction committer
// being driven by the corresponding transaction.
class ObTxOnePhaseCommitter
{
public:
  // One phase commit interface for transaction controller.
  // In the function, we need wait for log synchronization and drive the state machine
  int one_phase_commit(ObICommitCallback &cb);

  // One phase commit's log state machine handler
  // Oceanbase's transaction commit protocol use asynchronous log synchronization.
  // So we need provide callback to invoke the corresponding effects. The method
  // itself is failure free, so it will never report the error and instead catching
  // and handing the exception within the protocol.
  //
  // The real effect is provided in apply_xxx_log according to its state.
  int apply_log();

  // One phase commit exception handler.
  // For transaction commit, we need handle protocol timeout and machine breakdown.
  int handle_timeout();
  int handle_reboot();

  // One phase commit leader transfer handler.
  // For transaction commit with consensus protocol, we need handle leader transfer
  // and recover or abort the transaction according to the state.
  int leader_takeover();
  int leader_revoke();

protected:
  // Oceanbase's optimized one phase commit protocol state machine handler.
  // For one phase transaction commit, there are two evolved protocol state, commit
  // and abort, we need use the protocol state to produce the transaction result.
  //
  // As mentioned above, the methods need be failure free.
  int apply_commit_log();
  int apply_abort_log();

  // User operation handler
  // One phase commit user can implement its own state handler on its own.
  // NB: The implementation need guarantee the method is failure atomic[1],
  // So the method can be retried later.
  //
  // [1] failure atomic: if a method threw an error, the object should still be
  // usable afterwards. Generally, the object should be in the same state as it
  // was before invoking the method.
  virtual int do_commit() = 0;
  virtual int on_commit() = 0;
  virtual int do_abort() = 0;
  virtual int on_abort() = 0;

  // One phase commit log subbmitter.
  // Implementer need implement its own log according to all ObTwoPhaseCommitLogType.
  // And it should guarantee invoking the callback using apply_xxx_log.
  //
  // Under exception, the log may not be persisted eventually, and the above method
  // leader_revoke/leader_takeover and handle_timeout/handle_reboot wil continue to
  // drive the state machine.
  virtual int submit_log(const ObTwoPhaseCommitLogType& log_type) = 0;
};
} // transaction
} // oceanbase

#endif // OCEANBASE_STORAGE_TX_OB_ONE_PHASE_COMMITTER
