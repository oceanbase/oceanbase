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

#ifndef OCEANBASE_STORAGE_TX_OB_TWO_PHASE_COMMITTER
#define OCEANBASE_STORAGE_TX_OB_TWO_PHASE_COMMITTER
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bit_set.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_one_phase_committer.h"

namespace oceanbase
{
namespace transaction
{
// ObTxCycleTwoPhaseCommitter is the implementation of the Optimized Oceanbase
// two phase commit that introduces hierarchical cycle structure to commit the
// txn successfully during the transfer process.
//
// Also ObTxCycleTwoPhaseCommitter is an abstraction of commitable instance that
// you can implement. In Oceanbase, we implement the concurrent control manager
// based on the ObTxCycleTwoPhaseCommitter to make the consistency state
// persistent.
//
// You need remember that ObTxCycleTwoPhaseCommitter is an txn commit state
// machine, and all action that satisfy the enable conditions need be performed
// atomically. So the implementation need guarantee the atomicity of the state
// transition, lets say use the latch, and so algorithm can ensure the
// correctness.
//
// We design the following rules for atomicity:
//
//  1. All public interface of ObTxCycleTwoPhaseCommitter need be protected by
//     outer exclusive access method.
//
//  2. All inherited methods should not be exclusively protected again for self
//     invocation.
//
class ObTxCycleTwoPhaseCommitter
{
public:
  ObTxCycleTwoPhaseCommitter() : collected_(), self_id_(-1) {}
  ~ObTxCycleTwoPhaseCommitter() {}
  void reset()
  {
    collected_.reset();
    self_id_ = -1;
  }
  // two_phase_commit triggers the underlying two phase commit progress.
  //
  // Implementation need guarantee that it will never invoke the method on
  // multiple different participants
  int two_phase_commit();

  // two_phase_abort aborts the current txn if it has not entered the two phase commit
  //
  // It will return OB_STATE_NOT_MATCH if it has already entered the two phase
  // commit and cannot rollback the process arbitrarily.
  int two_phase_abort();

  // Two phase commit's msg handler
  //
  // Oceanbase's txn commit protocol use asynchronous and best effort msg. So we
  // need provide retry machinism to implement the best effort. In addititon,
  // The method itself should be failure atomicity[1], so it will never recover
  // to its origin state under the error.
  //
  // The real effect is provided in handle_xxx_request/response according to its
  // msg.
  //
  // [1] failure atomic: if a method threw an error, the object should still be
  // usable afterwards. Generally, the object should be in the same state as it
  // was before invoking the method.
  int handle_2pc_req(const ObTwoPhaseCommitMsgType msg_type);
  int handle_2pc_resp(const ObTwoPhaseCommitMsgType msg_type,
                      const int64_t participant_id);

  static int handle_orphan_2pc_req(const ObTwoPhaseCommitMsgType recv_msg_type,
                                   ObTwoPhaseCommitMsgType& send_msg_type,
                                   bool& need_ack);
  static int handle_orphan_2pc_resp(const ObTwoPhaseCommitMsgType recv_msg_type,
                                    ObTwoPhaseCommitMsgType& send_msg_type,
                                    bool& need_ack);

  // Two phase commit's log state machine handler
  //
  // Oceanbase's txn commit protocol use asynchronous log synchronization. So we
  // need provide callback to invoke the corresponding effects. In addition, The
  // method itself should be failure free, so it will never report the error and
  // instead catching and handing the exception within the method.
  //
  // The real effect is provided in apply_xxx_log or replay_xxx_log according to its state.
  int apply_log(const ObTwoPhaseCommitLogType log_type);
  int replay_log(const ObTwoPhaseCommitLogType log_type);

  // Two phase commit exception handler.
  //
  // For transaction commit, we need handle protocol timeout and machine
  // shutdown.
  int handle_timeout();
  int handle_reboot();

  // Two phase commit leader transfer handler.
  //
  // For transaction commit with consensus protocol, we need handle leader transfer
  // and recover or abort the transaction according to the state.
  int leader_takeover();
  int leader_revoke();

  VIRTUAL_TO_STRING_KV(K_(collected));
  // The overall msg also need be failure atomicity.
  //
  // two phase commit normal message handler.
  int handle_2pc_prepare_request();
  int handle_2pc_prepare_response(const int64_t participant_id);
  int handle_2pc_commit_request();
  int handle_2pc_abort_request();
  int handle_2pc_commit_response(const int64_t participant_id);
  int handle_2pc_abort_response(const int64_t participant_id);

  // Oceanbase's optimized two phase commit protocol
  //
  // In our optimized protocol, we use precommit msg to reduce single machine
  // read latency and clear msg to reduce SQL commit latency.
  int handle_2pc_pre_commit_request();
  int handle_2pc_pre_commit_response(const int64_t participant_id);
  int handle_2pc_clear_request();
  int handle_2pc_clear_response(const int64_t participant_id);

  // message handler for special usage
  // only persist redo and commit info
  int handle_2pc_prepare_redo_request();
  int handle_2pc_prepare_redo_response(const int64_t participant_id);

  // two phase commit orphan message handler message handler
  //
  // Normal msg handler under orphan context(context that not exsits). The
  // caller need create a temporary committer or use a singleton committer to
  // invoke the methods.
  static int handle_orphan_2pc_prepare_redo_request(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_prepare_redo_response(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_prepare_request(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_prepare_response(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_commit_request(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_abort_request(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_commit_response(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_abort_response(ObTwoPhaseCommitMsgType& send_msg_type);

  // Oceanbase's optimized two phase commit protocol orphan message handler
  //
  // optimized mnsg handler under orphan context(context that not exsits). The
  // caller need create a temporary committer or use a singleton committer to
  // invoke the methods.
  static int handle_orphan_2pc_pre_commit_request(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_pre_commit_response(ObTwoPhaseCommitMsgType& send_msg_type);
  static int handle_orphan_2pc_clear_request();
  static int handle_orphan_2pc_clear_response();

  // Oceanbase's optimized two phase commit protocol state machine handler.
  //
  // For two phase transaction commit, there are four evolved protocol state,
  // prepare, commit, abort and clear, we need use the protocol state to produce
  // the transaction result.
  //
  // As mentioned above, the methods need be failure free.
  int apply_commit_info_log();
  int apply_prepare_log();
  int apply_commit_log();
  int apply_abort_log();
  int apply_clear_log();

  int replay_commit_info_log();
  int replay_prepare_log();
  int replay_commit_log();
  int replay_abort_log();
  int replay_clear_log();
  int recover_from_tx_table();

  int try_enter_pre_commit_state();
  int on_pre_commit();

  // Two phase committer user should implement its own state handler on its own.
  // For ObTxCtx, we use the handler to implements the concurrent control
  // manager.
  //
  // NB: We guarantee that all methods below will only be invoked once during
  // normal execution.
  //
  // NB: The implementation need guarantee the method is failure atomic, So the
  // method should never report an error.
  virtual int do_prepare(bool &no_need_submit_log) = 0;
  virtual int do_pre_commit(bool& need_wait) = 0;
  virtual int do_commit() = 0;
  virtual int do_abort() = 0;
  virtual int do_clear() = 0;

  virtual int on_prepare() = 0;
  virtual int on_commit() = 0;
  virtual int on_abort() = 0;
  virtual int on_clear() = 0;

  // 1. when recive a new request msg, the participant will enter into the next phase.
  // 2. invoke do_xxx to execute all in-memory operation with the next phase
  // 3. set upstream_state at last
  // 4. retry submit log if upstream_state is larger than downstream_state
  int drive_self_2pc_phase(ObTxState next_phase);

  // for xa
  virtual int reply_to_scheduler_for_sub2pc(int64_t msg_type) = 0;

  // Concurrent control between transfer and 2pc.
  //
  // Different from most of the 2pc, Oceanbase's 2pc cannot know all
  // participants until definitely touch the participants. The reason is shown
  // below:
  //
  //                +--------------------+
  //                |       Txn1         |
  //                |  Participant：[L1] |
  //                +--------+-----------+
  //                         |
  //          +----write-----+
  //          |
  //          |
  // +--------v---------+           +------------------+
  // |  +--+ +--+ +--+  |           |  +--+ +--+       |
  // |  |P1| |P2| |P3|  |           |  |P4| |P5|       |
  // |  +--+ +--+ +--+  |           |  +--+ +--+       |
  // |                  |           |                  |
  // |  Log Stream:L1   |           |  Log Stream:L2   |
  // +------------------+           +------------------+
  //                Figure 1: Normal Action
  //
  //                +--------------------+
  //                |       Txn1         |
  //                |  Participant：[L1] |
  //                +--------------------+
  //
  // +------------------+            +------------------+
  // |  +--+ +--+       |            |  +--+ +--+ +--+  |
  // |  |P1| |P2|       |            |  |P4| |P5| |P3|  |
  // |  +--+ +--+       |-transfer-> |  +--+ +--+ +--+  |
  // |                  |            |                  |
  // |  Log Stream:L1   |            |  Log Stream:L2   |
  // +------------------+            +------------------+
  //                 Figure 2: Transfer Action
  //
  // The transfer in figure2 will not be noticed by Txn1 and finally lost L2 as
  // participant. So we use get_participant to handle the concurrency control
  // between transfer and 2pc.
  //
  // To be brief:
  //
  //  1. If 2pc log is ahead transfer log, the transfer procedure need bring
  //     the 2pc state to the dst.
  //
  //  2. Otherwise, the 2pc will invoke a cycle style commit which the parent
  //     node waits for the node's 2pc state response before responsing its 2pc
  //     state to his parent node.
  //
  // Detailed design is in
  virtual int64_t get_downstream_size() const = 0;
  virtual int64_t get_self_id() = 0;
  // is_root returns whether it is the root participant in the cycle two phase
  // commit
  bool is_root() const { return Ob2PCRole::ROOT == get_2pc_role(); }
  // is_leaf returns whether it is the leaf participant in the cycle two phase
  // commit
  bool is_leaf() const { return Ob2PCRole::LEAF == get_2pc_role(); }
  bool is_internal() const { return Ob2PCRole::INTERNAL == get_2pc_role(); }
  virtual Ob2PCRole get_2pc_role() const = 0;

  // is_2pc_logging returns whether it is waiting for the success of two phase
  // commit asynchronous logging. Because of the asynchronization of the
  // logging, We almost view the unfinished logging as the state transition all
  // the time(except the next few cases) and ignore other state transition action
  // when it returns true.
  //
  // As said before, in most case, the two phase commit asynchronous logging is
  // the state transition. While, we want to handle prepare responses before its
  // own prepare log suucessfully synchronized, so we introduce the exception
  // for these similiar cases.
  //
  // If you are interested in the application, see handle_2pc_prepare_request
  // and apply_prepare_log.
  virtual bool is_2pc_logging() const = 0;

  //durable state, set by applying log
  virtual ObTxState get_downstream_state() const = 0;
  virtual int set_downstream_state(const ObTxState state) = 0;
  //in-memory state, set by msg
  virtual ObTxState get_upstream_state() const = 0;
  virtual int set_upstream_state(const ObTxState state) = 0;

  // for xa
  bool is_prepared_sub2pc()
  {
    return is_sub2pc()
           && ObTxState::REDO_COMPLETE == get_downstream_state()
           && all_downstream_collected_();
  }

  // Two phase commit log subbmitter.
  // Implementer need implement its own log according to all ObTwoPhaseCommitLogType.
  // And it should guarantee invoking the callback using apply_xxx_log.
  //
  // Under exception, the log may not be persisted eventually, and the above method
  // leader_revoke/leader_takeover and handle_timeout/handle_reboot wil continue to
  // drive the state machine.
  virtual int submit_log(const ObTwoPhaseCommitLogType& log_type) = 0;

  // Two phase commit msg poster.
  // Implementer need implement its own msg according to all ObTwoPhaseCommitMsgType.
  // While it should not guarantee anything except best effort property and interface
  // adaption for hande_msg
  int post_downstream_msg(const ObTwoPhaseCommitMsgType msg_type);
  virtual int post_msg(const ObTwoPhaseCommitMsgType& msg_type,
                       const int64_t participant_id) = 0;
  // whether the processing of current two phase commit is sub part of a global transaction
  // TODO, refine in 4.1
  virtual bool is_sub2pc() const = 0;
  // only persist redo and commit info
  //
  int prepare_redo();
  // continue execution of two phase commit
  int continue_execution(const bool is_rollback);


private:
  // Inner method for handle_2pc_xxx_request/response for clearity
  int handle_2pc_prepare_redo_request_impl_();
  int handle_2pc_prepare_redo_response_impl_(const int64_t participant_id);
  int handle_2pc_prepare_request_impl_();
  int handle_2pc_pre_commit_request_impl_();
  int handle_2pc_commit_request_impl_();
  int handle_2pc_abort_request_impl_();
  int handle_2pc_clear_request_impl_();
  int handle_2pc_prepare_response_impl_(const int64_t participant_id);
  int handle_2pc_pre_commit_response_impl_(const int64_t participant);
  int handle_2pc_ack_response_impl_(const int64_t participant_id);
  int handle_2pc_abort_response_impl_(const int64_t participant_id);

  virtual int apply_2pc_msg_(const ObTwoPhaseCommitMsgType msg_type) = 0;

  // The next state of current state, it is based on the order of ObTxState.
  //
  // It will return ObTxState::UNKNOWN if you pass the state which is smaller
  // than ObTxState::INIT or larger than ObTxState::CLEAR.
  ObTxState decide_next_state_(const ObTxState cur_state);

  // Because the post_msg is best effect, we need retry to post the msg under
  // exception.
  //
  //  NB: We should take both upstream and downstream into consideration.
  int decide_downstream_msg_type_(bool &need_submit, ObTwoPhaseCommitMsgType &msg_type);
  int retransmit_downstream_msg_();
  int retransmit_upstream_msg_(const ObTxState state);
  int retransmit_downstream_msg_(const int64_t participant);

  // Because the submit_log may fail, we need retry to submit the log under
  // exception.
  int decide_2pc_log_type_(bool &need_submit, ObTwoPhaseCommitLogType &log_type);
  int submit_2pc_log_();

  int collect_downstream_(const int64_t participant);
protected:
  // Means we collect all downstream responses
  bool all_downstream_collected_();
protected:
  // colloected_ is the bit set for storing responses from participants
  //
  //  NB: We introduce the rule that the bit set is cleaned up each time state
  //      is transferred.
  common::ObBitSet<> collected_;

  int64_t self_id_;
  //set by xa or dup table
  // bool no_need_submit_prepare_log_;
};

bool is_2pc_request_msg(const ObTwoPhaseCommitMsgType msg_type);

bool is_2pc_response_msg(const ObTwoPhaseCommitMsgType msg_type);
} // transaction
} // oceanbase

#endif // OCEANBASE_STORAGE_TX_OB_TWO_PHASE_COMMITTER
