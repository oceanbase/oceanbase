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

#ifndef OCEANBASE_TRANSACTION_OB_TX_LS_STATE_MGR
#define OCEANBASE_TRANSACTION_OB_TX_LS_STATE_MGR

#include "lib/hash/ob_hashmap.h"
#include "ob_trans_define.h"

/***
 *  The Binary Representation of the LS State
 *
 *  | ... Reserved Bits (44) |
 *  | Block Start Tx Flag (1) | Block Start Normal Tx Flag (1) | Block Start ReadOnly Flag (1) |
 *  | Offline Flag (1) |
 *  | TxLSState (16) |
 */

/***
 *  State Machine
 *
 *  @Init:
 *  +--------------------+------------+----------+---------------+--------------+
 *  | BaseState          | BaseFlag   | Action   | TargetState   | TargetFlag   |
 *  +====================+============+==========+===============+==============+
 *  | INIT               | NONE       | START    | F_WORKING     | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | INIT               | NONE       | STOP     | INIT          | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | STOPPED            | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | F_WORKING          | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | L_WORKING          | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | T_SYNC_PENDING     | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | R_SYNC_PENDING     | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | T_APPLYING_PENDING | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *  | R_APPLYING_PENDING | UNLIMITED  | STOP     | STOPPED       | NONE         |
 *  +--------------------+------------+----------+---------------+--------------+
 *
 *  @Leader Takeover:
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | BaseState       | BaseFlag   | Action          | TargetState     | TargetFlag   |
 *  +=================+============+=================+=================+==============+
 *  | F_WORKING       | UNLIMITED  | LEADER_TAKEOVER | T_SYNC_PENDING  | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | F_WORKING       | UNLIMITED  | RESUME_LEADER   | R_SYNC_PENDING  | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | T_SYNC_PENDING  | UNLIMITED  | SWL_CB_SUCC     | T_APPLY_PENDING | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | R_SYNC_PENDING  | UNLIMITED  | SWL_CB_SUCC     | R_APPLY_PENDING | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | T_SYNC_PENDING  | UNLIMITED  | SWL_CB_FAIL     | T_SYNC_FAILED   | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | R_SYNC_PENDING  | UNLIMITED  | SWL_CB_FAIL     | R_SYNC_FAILED   | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | T_APPLY_PENDING | UNLIMITED  | APPLY_SWL_SUCC  | L_WORKING       | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | R_APPLY_PENDING | UNLIMITED  | APPLY_SWL_SUCC  | L_WORKING       | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | T_APPLY_PENDING | UNLIMITED  | APPLY_SWL_FAIL  | T_APPLY_PENDING | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *  | R_APPLY_PENDING | UNLIMITED  | APPLY_SWL_FAIL  | R_APPLY_PENDING | UNLIMITED    |
 *  +-----------------+------------+-----------------+-----------------+--------------+
 *
 *  @Leader Revoke:
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | BaseState       | BaseFlag   | Action                   | TargetState   | TargetFlag                    |
 *  +=================+============+==========================+===============+===============================+
 *  | L_WORKING       | UNLIMITED  | LEADER_REVOKE_GRACEFULLY | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | L_WORKING       | UNLIMITED  | LEADER_REVOKE_FORCEDLLY  | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | T_SYNC_FAILED   | UNLIMITED  | LEADER_REVOKE_GRACEFULLY | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | R_SYNC_FAILED   | UNLIMITED  | LEADER_REVOKE_GRACEFULLY | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | T_SYNC_FAILED   | UNLIMITED  | LEADER_REVOKE_FORCEDLLY  | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | R_SYNC_FAILED   | UNLIMITED  | LEADER_REVOKE_FORCEDLLY  | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | T_APPLY_PENDING | UNLIMITED  | LEADER_REVOKE_FORCEDLLY  | F_SWL_PENDING | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | R_APPLY_PENDING | UNLIMITED  | LEADER_REVOKE_FORCEDLLY  | F_SWL_PENDING | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | F_SWL_PENDING   | UNLIMITED  | APPLY_SWL_FAIL           | F_SWL_PENDING | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *  | F_SWL_PENDING   | UNLIMITED  | APPLY_SWL_SUCC           | F_WORKING     | BLOCK_STATR_NORMAL_TX = False |
 *  +-----------------+------------+--------------------------+---------------+-------------------------------+
 *
 *  @Offline:
 *  +-------------+------------+----------+---------------+-----------------------------------------------------------------------+
 *  | BaseState   | BaseFlag   | Action   | TargetState   | TargetFlag                                                            |
 *  +=============+============+==========+===============+=======================================================================+
 *  | UNLIMITED   | UNLIMITED  | OFFLINE  | UNLIMITED     | OFFLINE = True                                                        |
 *  +-------------+------------+----------+---------------+-----------------------------------------------------------------------+
 *  | UNLIMITED   | UNLIMITED  | ONLINE   | UNLIMITED     | OFFLINE = False, BLOCK_START_TX = False, BLOCK_START_READONLY = False |
 *  +-------------+------------+----------+---------------+-----------------------------------------------------------------------+
 *  Notion: Before 4.3, the ls may be started to working without online operation.
 *
 *  @Block Tx
 *  +-------------+------------+-----------------------+---------------+-------------------------------+
 *  | BaseState   | BaseFlag   | Action                | TargetState   | TargetFlag                    |
 *  +=============+============+=======================+===============+===============================+
 *  | UNLIMITED   | UNLIMITED  | BLOCK_START_TX        | UNLIMITED     | BLOCK_START_TX = True         |
 *  +-------------+------------+-----------------------+---------------+-------------------------------+
 *  | UNLIMITED   | UNLIMITED  | BLOCK_START_READONLY  | UNLIMITED     | BLOCK_START_READONLY = True   |
 *  +-------------+------------+-----------------------+---------------+-------------------------------+
 *  | L_WORKING   | UNLIMITED  | BLOCK_START_NORMAL_TX | UNLIMITED     | BLOCK_START_NORMAL_TX = True  |
 *  +-------------+------------+-----------------------+---------------+-------------------------------+
 *  | L_WORKING   | UNLIMITED  | UNBLOCK_NORMAL_TX     | UNLIMITED     | BLOCK_START_NORMAL_TX = False |
 *  +-------------+------------+-----------------------+---------------+-------------------------------+
 */

namespace oceanbase
{

namespace transaction
{

class ObTxLSStateMgr
{

public:
  enum TxLSAction : uint64_t
  {
    UNKOWN = 0,
    START = 1,
    STOP = 2,
    ONLINE = 3,
    OFFLINE = 4,
    LEADER_REVOKE_GRACEFULLY = 5,
    LEADER_REVOKE_FORCEDLLY = 6,
    SWL_CB_SUCC = 7,
    SWL_CB_FAIL = 8,
    APPLY_SWL_SUCC = 9,
    APPLY_SWL_FAIL = 10,
    LEADER_TAKEOVER = 12,
    RESUME_LEADER = 13,
    BLOCK_START_TX = 14,
    BLOCK_START_READONLY = 15,
    BLOCK_START_NORMAL_TX = 16,
    UNBLOCK_NORMAL_TX = 17,
    BLOCK_START_WR = 18
  };

  enum TxLSState : uint16_t
  {
    INIT = 0,
    F_WORKING = 1,
    L_WORKING = 4,
    STOPPED = 14,
    T_SYNC_PENDING = 15,
    R_SYNC_PENDING = 16,
    T_SYNC_FAILED = 17,
    R_SYNC_FAILED = 18,
    T_APPLY_PENDING = 19,
    R_APPLY_PENDING = 20,
    F_SWL_PENDING  = 21
  };

#define TCM_STATE_CASE_TO_STR(state) \
  case state:                        \
    str = #state;                    \
    break;

  static const char *state_str(uint16_t state)
  {
    const char *str = "INVALID";
    switch (state) {
      TCM_STATE_CASE_TO_STR(INIT);
      TCM_STATE_CASE_TO_STR(F_WORKING);
      TCM_STATE_CASE_TO_STR(L_WORKING);
      TCM_STATE_CASE_TO_STR(STOPPED);
      TCM_STATE_CASE_TO_STR(T_SYNC_PENDING);
      TCM_STATE_CASE_TO_STR(R_SYNC_PENDING);
      TCM_STATE_CASE_TO_STR(T_SYNC_FAILED);
      TCM_STATE_CASE_TO_STR(R_SYNC_FAILED);
      TCM_STATE_CASE_TO_STR(T_APPLY_PENDING);
      TCM_STATE_CASE_TO_STR(R_APPLY_PENDING);
      TCM_STATE_CASE_TO_STR(F_SWL_PENDING);

    default:
      break;
    }
    return str;
  }

  static const char *action_str(TxLSAction action)
  {
    const char *str = "INVALID";
    switch (action) {
      TCM_STATE_CASE_TO_STR(UNKOWN);
      TCM_STATE_CASE_TO_STR(START);
      TCM_STATE_CASE_TO_STR(STOP);
      TCM_STATE_CASE_TO_STR(ONLINE);
      TCM_STATE_CASE_TO_STR(OFFLINE);
      TCM_STATE_CASE_TO_STR(LEADER_REVOKE_GRACEFULLY);
      TCM_STATE_CASE_TO_STR(LEADER_REVOKE_FORCEDLLY);
      TCM_STATE_CASE_TO_STR(SWL_CB_SUCC);
      TCM_STATE_CASE_TO_STR(SWL_CB_FAIL);
      TCM_STATE_CASE_TO_STR(APPLY_SWL_SUCC);
      TCM_STATE_CASE_TO_STR(APPLY_SWL_FAIL);
      TCM_STATE_CASE_TO_STR(LEADER_TAKEOVER);
      TCM_STATE_CASE_TO_STR(RESUME_LEADER);
      TCM_STATE_CASE_TO_STR(BLOCK_START_TX);
      TCM_STATE_CASE_TO_STR(BLOCK_START_READONLY);
      TCM_STATE_CASE_TO_STR(BLOCK_START_NORMAL_TX);
      TCM_STATE_CASE_TO_STR(UNBLOCK_NORMAL_TX);
      TCM_STATE_CASE_TO_STR(BLOCK_START_WR);
    default:
      break;
    }
    return str;
  }
#undef TCM_STATE_CASE_TO_STR

  union TxLSStateContainer
  {
    struct StateVal
    {
      uint16_t state_ : 16;
      uint8_t offline_flag_ : 1;
      uint8_t block_start_tx_flag_ : 1;
      uint8_t block_start_normal_tx_flag_ : 1;
      uint8_t block_start_readonly_flag_ : 1;
      uint64_t reserved_bit_ : 44;

      TO_STRING_KV("state_",
                   state_str(state_),
                   K(offline_flag_),
                   K(block_start_tx_flag_),
                   K(block_start_normal_tx_flag_),
                   K(block_start_readonly_flag_));
    } state_val_;
    uint64_t state_container_;
  };

  struct TxStateActionPair
  {
    TxLSStateContainer base_state_;
    TxLSAction exec_action_;

    TxStateActionPair() { reset(); }
    void reset()
    {
      base_state_.state_container_ = 0;
      exec_action_ = TxLSAction::UNKOWN;
    }

    TO_STRING_KV(K(base_state_.state_val_), "exec_action_", action_str(exec_action_));
  };
  struct TxLSStateHashFunc
  {
    int operator()(const TxStateActionPair &key, TxLSStateContainer &res) const;
  };

  // typedef common::hash::
  //     ObHashMap<TxStateActionPair, TxLSState, common::hash::NoPthreadDefendMode,
  //     TxLSStateHashFunc>
  //         StateActionHashMap;

public:
  ObTxLSStateMgr() { reset(); }

  int init(const share::ObLSID ls_id)
  {
    ls_id_ = ls_id;
    return OB_SUCCESS;
  }

  void reset()
  {
    ls_id_.reset();
    cur_state_.state_container_ = 0;
    prev_state_.state_container_ = 0;
  }
  int switch_tx_ls_state(const TxLSAction action,
                         const share::SCN start_working_scn = share::SCN::invalid_scn());
  void restore_tx_ls_state();

  bool need_retry_apply_SWL(share::SCN &applying_swl_scn);
  bool waiting_SWL_cb();
  void replay_SWL_succ(const share::SCN & swl_scn);

  const char *
  iter_ctx_mgr_stat_info(uint64_t &state_container, bool &is_master, bool &is_stopped) const;
  // TxLSState get_state() const;
  bool is_master() const;
  bool is_follower() const;
  bool is_block_start_readonly() const;
  bool is_block_start_tx() const;
  bool is_block_start_normal_tx() const;
  bool is_block_WR() const;
  bool is_start_working_apply_pending() const;
  bool is_leader_takeover_pending() const;
  bool is_switch_leader_pending() const;
  bool is_resume_leader_pending() const;
  bool is_stopped() const;

  TO_STRING_KV(K(ls_id_),
               K(cur_state_.state_val_),
               K(prev_state_.state_val_),
               K(max_applying_start_working_ts_),
               K(max_applied_start_working_ts_));

private:
  int execute_tx_ls_action_(const TxLSAction action);
  static void clear_all_flag_(TxLSStateContainer &cur_state);

  static bool is_switch_leader_pending_(const TxLSStateContainer &ls_state_container);
  static bool is_resume_leader_pending_(const TxLSStateContainer &ls_state_container);

private:
  share::ObLSID ls_id_;
  TxLSStateContainer cur_state_;
  TxLSStateContainer prev_state_;

  share::SCN max_applied_start_working_ts_;
  share::SCN max_applying_start_working_ts_;
};

} // namespace transaction

} // namespace oceanbase
#endif
