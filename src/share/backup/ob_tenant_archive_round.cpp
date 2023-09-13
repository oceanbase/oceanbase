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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_tenant_archive_round.h"
#include "storage/tx/ob_ts_mgr.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_smart_var.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/ob_tenant_info_proxy.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/ls/ob_ls_i_life_manager.h"
#include "share/scn.h"

using namespace oceanbase;
using namespace share;
using namespace common;

/**
 * ------------------------------ObArchiveRoundHandler---------------------
 */
ObArchiveRoundHandler::ObArchiveRoundHandler()
  :is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), incarnation_(OB_START_INCARNATION),
   sql_proxy_(nullptr), archive_table_op_()
{

}

int ObArchiveRoundHandler::init(
    const uint64_t tenant_id,
    const int64_t incarnation,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("archive round init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret));
  } else if (!is_sys_tenant(tenant_id) && is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(archive_table_op_.init(tenant_id))) {
    LOG_WARN("failed to init archive_table_op", K(ret));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    incarnation_ = incarnation;
    sql_proxy_ = &sql_proxy;
  }
  
  return ret;
}

uint64_t ObArchiveRoundHandler::get_exec_tenant_id_() const
{
  return gen_meta_tenant_id(tenant_id_);
}

int ObArchiveRoundHandler::start_trans_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = get_exec_tenant_id_();
  if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
    LOG_WARN("failed to start transaction", K(ret), K(exec_tenant_id));
  }
  return ret;
}

int ObArchiveRoundHandler::start_archive(const ObTenantArchiveRoundAttr &round, ObTenantArchiveRoundAttr &new_round)
{
  int ret = OB_SUCCESS;
  ObTenantArchivePieceAttr first_piece;
  ObArray<ObTenantArchivePieceAttr> pieces;
  if (!round.state_.is_prepare()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round", K(ret), K(round));
  } else if (OB_FAIL(prepare_beginning_dest_round_(round, new_round))) {
    LOG_WARN("failed to prepare beginning dest round", K(ret), K(round));
  } else if (OB_FAIL(new_round.generate_first_piece(first_piece))) {
    LOG_WARN("failed to generate first piece", K(ret));
  } else if (OB_FAIL(pieces.push_back(first_piece))) {
    LOG_WARN("failed to push back first piece", K(ret));
  } else if (OB_FAIL(checkpoint_to(round, new_round, pieces))) {
    LOG_WARN("failed to checkpoint", K(ret), K(round), K(new_round));
  }
  return ret;
}

int ObArchiveRoundHandler::prepare_beginning_dest_round_(const ObTenantArchiveRoundAttr &round, ObTenantArchiveRoundAttr &new_round)
{
  int ret = OB_SUCCESS;

  SCN start_scn = SCN::min_scn();
  ObArchiveRoundState next_state = ObArchiveRoundState::beginning();
  if (!round.state_.is_prepare()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round", K(ret), K(round));
  } else if (OB_FAIL(new_round.deep_copy_from(round))) {
    LOG_WARN("failed to do copy", K(ret), K(round));
  } else if (OB_FAIL(decide_start_scn_(start_scn))) {
    LOG_WARN("failed to get start scn", K(ret), K(round));
  } else {
    new_round.start_scn_ = start_scn;
    new_round.checkpoint_scn_ = start_scn;
    new_round.max_scn_ = start_scn;
    new_round.state_ = next_state;
  }
  return ret;
}


bool ObArchiveRoundHandler::can_start_archive(const ObTenantArchiveRoundAttr &round) const
{
  // only tenant with 'STOP' state can start archive.
  return round.state_.is_stop();
}

bool ObArchiveRoundHandler::can_stop_archive(const ObTenantArchiveRoundAttr &round) const
{
  // only tenant with archive state of BEGINNING or DOING or INTERRUPTED or SUSPENDING or SUSPEND can stop archive.
  return round.state_.is_beginning() || round.state_.is_doing() || round.state_.is_interrupted() || round.state_.is_suspending() || round.state_.is_suspend();
}

bool ObArchiveRoundHandler::can_suspend_archive(const ObTenantArchiveRoundAttr &round) const
{
  // only tenant with archive state of DOING or INTERRUPTED can defer archive.
  return round.state_.is_doing() || round.state_.is_interrupted();
}

int ObArchiveRoundHandler::decide_start_scn_(SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupUtils::get_backup_scn(tenant_id_, start_scn))) {
    LOG_WARN("failed to decide archive start scn.", K(ret), K_(tenant_id));
  }
  return ret;
}


int ObArchiveRoundHandler::can_enable_archive(const int64_t dest_no, bool &can)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr round;
  bool need_lock = false;
  can = false;
  if (OB_FAIL(archive_table_op_.get_round(*sql_proxy_, dest_no, need_lock, round))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      can = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get round", K(ret), K(dest_no));
    }
  } else if (!round.state_.is_stop() && !round.state_.is_suspend()) {
    can = false;
    LOG_WARN("round exist on that dest", K(ret), K(dest_no), K(round));
  } else {
    can = true;
  }
  return ret;
}

int ObArchiveRoundHandler::enable_archive(const int64_t dest_no, ObTenantArchiveRoundAttr &round)
{
  int ret = OB_SUCCESS;
  bool can = false;
  bool is_exist = false;
  bool old_round_exist = true;
  ObMySQLTransaction trans;
  ObLogArchiveDestState dest_state;
  ObArchiveMode log_mode;

  if (OB_FAIL(can_enable_archive(dest_no, can))) {
    LOG_WARN("failed to check can enable archive", K(ret), K(dest_no));
  } else if (!can) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot enable log archive", K(ret), K(dest_no));
  } else if (OB_FAIL(start_trans_(trans))) {
    LOG_WARN("failed to start transaction", K(ret), K(dest_no));
  } else if (OB_FAIL(archive_table_op_.get_archive_mode(trans, log_mode))) {
    LOG_WARN("failed to get archive mode", K(ret));
  } else if (!log_mode.is_archivelog()) {
    ret = OB_CANNOT_START_LOG_ARCHIVE_BACKUP;
    LOG_WARN("log mode is not ARCHIVELOG", K(ret), K(dest_no), K(log_mode));
  } else if (OB_FAIL(archive_table_op_.lock_archive_dest(trans, dest_no, is_exist))) {
    LOG_WARN("failed to lock archive dest", K(ret), K(dest_no));
  } else if (!is_exist) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("empty archive dest", K(ret), K(dest_no));
  } else if (OB_FAIL(archive_table_op_.get_dest_state(trans, false, dest_no, dest_state))) {
    LOG_WARN("failed to get archive dest state", K(ret), K(dest_no));
  } else if (!dest_state.is_enable()) {
    ret = OB_CANNOT_START_LOG_ARCHIVE_BACKUP;
    LOG_WARN("dest state is not enable", K(ret), K(dest_no), K(dest_state));
  } else if (OB_FAIL(archive_table_op_.get_round(trans, dest_no, true, round))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      old_round_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get last round", K(ret), K(dest_no));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (old_round_exist && round.state_.is_suspend()) {
    ObArchiveRoundState next_state = ObArchiveRoundState::beginning();
    if (OB_FAIL(archive_table_op_.switch_round_state_to(trans, round, next_state))) {
      LOG_WARN("failed to switch state", K(ret), K(round), K(next_state));
    } else if (OB_FAIL(trans.end(true))) {
      LOG_WARN("failed to commit trans", K(ret), K(dest_no), K(round));
    } else {
      round.switch_state_to(next_state);
      LOG_INFO("continue archive", K(round));
    }
  } else if (OB_FAIL(prepare_new_dest_round_(dest_no, trans, round))) {
    LOG_WARN("failed to prepare new archive round", K(ret), K(dest_no));
  } else {
    if (OB_FAIL(archive_table_op_.start_new_round(trans, round))) {
      LOG_WARN("failed to start new archive round", K(ret), K(dest_no), K(round));
    } else if (OB_FAIL(trans.end(true))) {
      LOG_WARN("failed to commit trans", K(ret), K(dest_no), K(round));
    } 
  }
  
  if (OB_FAIL(ret) && trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("failed to end trans", K(tmp_ret));
    }
  }
  
  return ret;
}

int ObArchiveRoundHandler::disable_archive(const int64_t dest_no, ObTenantArchiveRoundAttr &round)
{
  int ret = OB_SUCCESS;
  bool can = false;
  ObMySQLTransaction trans;
  ObArchiveRoundState next_state = ObArchiveRoundState::stopping();
  if (OB_FAIL(start_trans_(trans))) {
    LOG_WARN("failed to start transaction", K(ret), K(dest_no));
  } else if (OB_FAIL(archive_table_op_.get_round(trans, dest_no, true, round))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot disable log archive", K(ret), K(dest_no));
    } else {
      LOG_WARN("failed to get round", K(ret), K(dest_no));
    }
  } else if (!can_stop_archive(round)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot stop log archive", K(ret), K(round));
  } else if (OB_FAIL(archive_table_op_.switch_round_state_to(trans, round, next_state))) {
    LOG_WARN("failed to switch state", K(ret), K(round), K(next_state));
  } else if (OB_FAIL(archive_table_op_.clean_round_comment(trans, dest_no))) {
    LOG_WARN("failed to clean comment", K(ret), K(round));
  } else if (OB_FAIL(trans.end(true))) {
    LOG_WARN("failed to commit trans", K(ret), K(round));
  } else {
    round.switch_state_to(next_state);
    LOG_INFO("stop archive", K(round));
  }

  if (OB_FAIL(ret) && trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("failed to end trans", K(tmp_ret));
    }
  }
  return ret;
}

int ObArchiveRoundHandler::defer_archive(const int64_t dest_no, ObTenantArchiveRoundAttr &round)
{
  int ret = OB_SUCCESS;
  bool can = false;
  ObMySQLTransaction trans;
  ObArchiveRoundState next_state = ObArchiveRoundState::suspending();
  if (OB_FAIL(start_trans_(trans))) {
    LOG_WARN("failed to start transaction", K(ret), K(dest_no));
  } else if (OB_FAIL(archive_table_op_.get_round(trans, dest_no, true, round))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot defer log archive", K(ret), K(dest_no));
    } else {
      LOG_WARN("failed to get round", K(ret), K(dest_no));
    }
  } else if (!can_suspend_archive(round)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot defer log archive", K(ret), K(round));
  } else if (OB_FAIL(archive_table_op_.switch_round_state_to(trans, round, next_state))) {
    LOG_WARN("failed to switch state", K(ret), K(round), K(next_state));
  } else if (OB_FAIL(archive_table_op_.clean_round_comment(trans, dest_no))) {
    LOG_WARN("failed to clean comment", K(ret), K(round));
  } else if (OB_FAIL(trans.end(true))) {
    LOG_WARN("failed to commit trans", K(ret), K(round));
  } else {
    round.switch_state_to(next_state);
    LOG_INFO("defer archive", K(round));
  }

  if (OB_FAIL(ret) && trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("failed to end trans", K(tmp_ret));
    }
  }
  return ret;
}

int ObArchiveRoundHandler::prepare_new_dest_round_(const int64_t dest_no, 
    ObMySQLTransaction &trans, ObTenantArchiveRoundAttr &round)
{
  int ret = OB_SUCCESS;
  bool need_lock = true;
  int64_t dest_id = 0;
  int64_t piece_switch_interval = 0;
  ObBackupPathString dest_str;
  ObBackupDest archive_dest;
  ObTenantArchiveRoundAttr last_round;

  if (OB_FAIL(archive_table_op_.get_dest_id(trans, need_lock, dest_no, dest_id))) {
    LOG_WARN("failed to get dest id", K(ret));
  } else if (OB_FAIL(archive_table_op_.get_piece_switch_interval(trans, need_lock, dest_no, piece_switch_interval))) {
    LOG_WARN("failed to get piece switch interval", K(ret));
  } else if (OB_FAIL(archive_table_op_.get_archive_dest(trans, need_lock, dest_no, dest_str))) {
    LOG_WARN("failed to get archive path", K(ret));
  } else if (OB_FAIL(archive_dest.set(dest_str))) {
    LOG_WARN("failed to set archive dest", K(ret), K(dest_str));
  } else if (OB_FAIL(archive_dest.get_backup_path_str(dest_str.ptr(), dest_str.capacity()))) {
    LOG_WARN("failed to get backup path", K(ret), K(archive_dest));
  } else {
    HEAP_VAR(ObTenantArchiveRoundAttr, last_round) {
      if (OB_FAIL(archive_table_op_.get_round(trans, dest_no, need_lock, last_round))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // This channel has not archived before.
          ObTenantArchiveRoundAttr::Key key = { tenant_id_, dest_no };
          if (OB_FAIL(ObTenantArchiveRoundAttr::generate_initial_round(key, OB_START_INCARNATION, dest_id, piece_switch_interval, dest_str, round))) {
            LOG_WARN("failed to generate initial round", K(ret), K(key));
          }
        } else {
          LOG_WARN("failed to get last round", K(ret), K(dest_no));
        }
      } else if (OB_FAIL(last_round.generate_next_round(OB_START_INCARNATION, dest_id, piece_switch_interval, dest_str, round))) {
        LOG_WARN("failed to generate next round", K(ret), K(last_round));
      }
    }
  }

  return ret;
}

int ObArchiveRoundHandler::checkpoint_to(
    const ObTenantArchiveRoundAttr &old_round, 
    const ObTenantArchiveRoundAttr &new_round,
    const common::ObIArray<ObTenantArchivePieceAttr> &pieces)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;

  HEAP_VAR(ObTenantArchiveHisRoundAttr, his_round) {
    if (new_round.state_.is_stop()) {
      his_round = new_round.generate_his_round();
    }

    if (OB_FAIL(start_trans_(trans))) {
      LOG_WARN("failed to start transaction", K(ret), K(old_round), K(new_round), K(pieces));
    } else {
      if (new_round.state_.is_stop() && OB_FAIL(archive_table_op_.stop_round(trans, new_round))) {
        LOG_WARN("failed to del round", K(ret), K(old_round), K(new_round));
      } else if (new_round.state_.is_stop() && OB_FAIL(archive_table_op_.insert_his_round(trans, his_round))) {
        LOG_WARN("failed to insert his round", K(ret), K(old_round), K(new_round), K(his_round));
      } else if (!new_round.state_.is_stop() && OB_FAIL(archive_table_op_.switch_round_state_to(
                  trans, old_round, new_round))) {
        LOG_WARN("failed to advance round", K(ret), K(old_round), K(new_round));
      } else if (OB_FAIL(archive_table_op_.batch_update_pieces(trans, pieces))) {
        LOG_WARN("failed to advance pieces", K(ret), K(old_round), K(new_round), K(pieces));
      } else if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret), K(old_round), K(new_round), K(pieces));
      }

      if (OB_FAIL(ret) && trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          LOG_WARN("failed to end trans", K(tmp_ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (new_round.state_.status_ != old_round.state_.status_) {
      // state switched
      FLOG_INFO("[LOG_ARCHIVE] status changed.", K(old_round), K(new_round), K(pieces));
      ROOTSERVICE_EVENT_ADD("log_archive", "change_status", "tenant_id", tenant_id_,
        "dest_no", new_round.key_.dest_no_, "old_status", old_round.state_.to_status_str(), 
        "new_status", new_round.state_.to_status_str());
    }
  }

  return ret;
}


int ObArchiveRoundHandler::checkpoint_to(const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantArchivePieceAttr> empty_pieces;
  if (OB_FAIL(checkpoint_to(old_round, new_round, empty_pieces))) {
    LOG_WARN("failed to checkpoint", K(ret), K(old_round), K(new_round));
  }
  return ret;
}
