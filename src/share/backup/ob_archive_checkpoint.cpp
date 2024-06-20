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
#include "share/backup/ob_archive_checkpoint.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/backup/ob_tenant_archive_round.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase;
using namespace common;
using namespace share;

/**
 * ------------------------------ObDestRoundCheckpointer---------------------
 */
ObDestRoundCheckpointer::Counter::Counter()
{
  reset();
}

void ObDestRoundCheckpointer::Counter::reset()
{
  ls_count_ = 0;
  deleted_ls_count_ = 0;
  not_start_cnt_ = 0;
  interrupted_cnt_ = 0;
  doing_cnt_ = 0;
  stopped_cnt_ = 0;
  suspended_cnt_ = 0;
  max_scn_.set_min();
  checkpoint_scn_.set_max();
  max_active_piece_id_ = INT64_MAX;
}


int ObDestRoundCheckpointer::init(ObArchiveRoundHandler *round_handler, const PieceGeneratedCb &piece_generated_cb, 
    const RoundCheckpointCb &round_checkpoint_cb, const SCN &max_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDestRoundCheckpointer init twice.", K(ret));
  } else if (OB_ISNULL(round_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round_handler is null.", K(ret));
  } else if (SCN::min_scn() >= max_checkpoint_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max_checkpoint_scn.", K(ret), K(max_checkpoint_scn));
  } else {
    round_handler_ = round_handler;
    piece_generated_cb_ = piece_generated_cb;
    round_checkpoint_cb_ = round_checkpoint_cb;
    max_checkpoint_scn_ = max_checkpoint_scn;
    is_inited_ = true;
  }

  return ret;
}

void ObDestRoundCheckpointer::set_allow_force_stop()
{
  allow_force_stop_ = true;
  LOG_INFO("set allow force stop");
}

int ObDestRoundCheckpointer::checkpoint(const ObTenantArchiveRoundAttr &round_info, const ObDestRoundSummary &summary)
{
  int ret = OB_SUCCESS;
  int64_t limit_ts = 0;
  Counter counter;
  Result result;
  bool need_checkpoint = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDestRoundCheckpointer not init.", K(ret));
  } else if (!round_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round", K(ret), K(round_info), K(summary));
  } else if (!summary.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid summary", K(ret), K(round_info), K(summary));
  } else if (!can_do_checkpoint_(round_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("this round can not do checkpoint", K(ret), K(round_info), K(summary));
  } else if (round_info.checkpoint_scn_ > max_checkpoint_scn_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backwards, checkpoint scn is bigger than limit scn", K(ret), K(round_info), K_(max_checkpoint_scn));
  } else if (OB_FAIL(count_(round_info, summary, counter))) {
    LOG_WARN("failed to to count", K(ret), K(round_info), K(summary));
  } else if (OB_FAIL(gen_new_round_info_(round_info, summary, counter, result.new_round_info_, need_checkpoint))) {
    LOG_WARN("failed to decide next state", K(ret), K(round_info), K(counter), K(summary));
  } else if (!need_checkpoint) {
  } else if (OB_FAIL(checkpoint_(round_info, summary, result))) {
    LOG_WARN("failed to do checkpoint", K(ret), K(round_info), K(summary), K(result));
  }

  return ret;
}

int ObDestRoundCheckpointer::count_(
    const ObTenantArchiveRoundAttr &old_round_info,
    const ObDestRoundSummary &summary,
    ObDestRoundCheckpointer::Counter &counter) const
{
  int ret = OB_SUCCESS;

  counter.reset();
  counter.ls_count_ = summary.ls_count();
  const ObArray<ObLSDestRoundSummary> &ls_round_list = summary.ls_round_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_round_list.count(); i++) {
    const ObLSDestRoundSummary &ls_round = ls_round_list.at(i);
    // Only not started log stream has no piece. Otherwise, piece must be exist.
    if (!ls_round.has_piece()) {
      // has not started archive.
      counter.not_start_cnt_++;
      LOG_INFO("encounter a log stream which has not started archive.", K(ls_round));
    } else if (ls_round.is_deleted_) {
      // All log had been archived before delete the log stream.
      counter.deleted_ls_count_++;
      counter.max_scn_ = MAX(counter.max_scn_, ls_round.checkpoint_scn_);
    } else if (ls_round.state_.is_doing()) {
      counter.doing_cnt_++;
      counter.max_scn_ = MAX(counter.max_scn_, ls_round.checkpoint_scn_);
      counter.checkpoint_scn_ = MIN(counter.checkpoint_scn_, ls_round.checkpoint_scn_);
      counter.max_active_piece_id_ = MIN(counter.max_active_piece_id_, ls_round.max_piece_id());
    } else if (ls_round.state_.is_interrupted()) {
      counter.interrupted_cnt_++;
      counter.max_scn_ = MAX(counter.max_scn_, ls_round.checkpoint_scn_);
      counter.checkpoint_scn_ = MIN(counter.checkpoint_scn_, ls_round.checkpoint_scn_);
      counter.max_active_piece_id_ = MIN(counter.max_active_piece_id_, ls_round.max_piece_id());

      // Set first interrupt ls id.
      if (1 == counter.interrupted_cnt_) {
        counter.interrupted_ls_id_ = ls_round.ls_id_;
      }
    } else if (ls_round.state_.is_stop()) {
      counter.stopped_cnt_++;
      counter.max_scn_ = MAX(counter.max_scn_, ls_round.checkpoint_scn_);
      counter.checkpoint_scn_ = MIN(counter.checkpoint_scn_, ls_round.checkpoint_scn_);
      counter.max_active_piece_id_ = MIN(counter.max_active_piece_id_, ls_round.max_piece_id());
    } else if (ls_round.state_.is_suspend()) {
      counter.suspended_cnt_++;
      counter.max_scn_ = MAX(counter.max_scn_, ls_round.checkpoint_scn_);
      counter.checkpoint_scn_ = MIN(counter.checkpoint_scn_, ls_round.checkpoint_scn_);
      counter.max_active_piece_id_ = MIN(counter.max_active_piece_id_, ls_round.max_piece_id());
    }
    else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected archive state", K(ret), K(ls_round));
    }
  }

  if (OB_SUCC(ret)) {
    // adjust scn
    counter.checkpoint_scn_ = MAX(old_round_info.start_scn_, counter.checkpoint_scn_);
    counter.max_scn_ = MAX(old_round_info.start_scn_, counter.max_scn_);
    if (counter.not_start_cnt_ > 0) {
      counter.max_active_piece_id_ = old_round_info.used_piece_id_;
    }
  }

  LOG_INFO("print count result", K(ret), K(counter));

  return ret;
}

int ObDestRoundCheckpointer::calc_next_checkpoint_scn_(
    const ObTenantArchiveRoundAttr &old_round_info,
    const ObDestRoundSummary &summary,
    const Counter &counter,
    SCN &next_checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  SCN max_avail_piece_start_scn;
  SCN max_avail_piece_checkpoint_scn;
  int64_t max_avail_piece_id = 0;
  // The next checkpoint scn can not exceed the max_checkpoint_scn_ which takes the GTS.
  next_checkpoint_scn = MIN(max_checkpoint_scn_, counter.checkpoint_scn_);
  if (OB_FAIL(ObTenantArchiveMgr::decide_piece_id(
              old_round_info.start_scn_,
              old_round_info.base_piece_id_,
              old_round_info.piece_switch_interval_,
              next_checkpoint_scn,
              max_avail_piece_id))) {
    LOG_WARN("failed to calc max available piece id", K(ret), K(old_round_info), K(next_checkpoint_scn));
  } else if (OB_FAIL(ObTenantArchiveMgr::decide_piece_start_scn(
                     old_round_info.start_scn_,
                     old_round_info.base_piece_id_,
                     old_round_info.piece_switch_interval_,
                     max_avail_piece_id,
                     max_avail_piece_start_scn))) {
    LOG_WARN("failed to calc max available piece start scn", K(ret), K(old_round_info), K(max_avail_piece_id));
  }

  // Consider 2 log streams, the log groups info are as following :
  // 1001: [500, 600], [700, 1200]
  // 1002: [500, 900]
  // Then the reasonable next round checkpoint scn is 900. However, suppose the piece switch end scn is 1000,
  // if we specify to restore until 800, the result is that it will return and cannot be recovered. As the
  // log with range [700, 800] is in next piece, but the file status is BACKUP_FILE_INCOMPLETE which we will
  // ignore during restore. In this case, the next round checkpoint scn will be adjust to 600, instead of 900.
  max_avail_piece_checkpoint_scn = SCN::max_scn();
  const ObArray<ObLSDestRoundSummary> &ls_round_list = summary.ls_round_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_round_list.count(); i++) {
    const ObLSDestRoundSummary &ls_round = ls_round_list.at(i);
    // search the piece
    int64_t idx = ls_round.get_piece_idx(max_avail_piece_id);
    if (-1 == idx) {
      LOG_INFO("ls piece not found", K(ret), K(max_avail_piece_id), K(ls_round));
    } else {
      bool last_piece = false;
      const ObLSDestRoundSummary::OnePiece &ls_piece = ls_round.piece_list_.at(idx);
      if (OB_FAIL(ls_round.check_is_last_piece_for_deleted_ls(max_avail_piece_id, last_piece))) {
        LOG_WARN("failed to check is last piece for deleted ls", K(ret));
      } else if (last_piece) {
        // If the ls is deleted, and this is the last piece. It should not
        // affect the checkpoint_scn.
        // Mark the last piece deleted for deleted ls. For example, piece 10 and 11 is found of
        // a deleted ls for current checkpoint, piece 10 is not marked with deleted, but piece 11
        // is marked with deleted.
        // do nothing.
      } else {
        // checkpoint scn may be smaller than start scn for empty piece.
        max_avail_piece_checkpoint_scn = MAX(max_avail_piece_start_scn, MIN(max_avail_piece_checkpoint_scn, ls_piece.checkpoint_scn_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    next_checkpoint_scn = MIN(next_checkpoint_scn, max_avail_piece_checkpoint_scn);
  }

  return ret;
}

int ObDestRoundCheckpointer::gen_new_round_info_(
    const ObTenantArchiveRoundAttr &old_round_info,
    const ObDestRoundSummary &summary,
    const ObDestRoundCheckpointer::Counter &counter,
    ObTenantArchiveRoundAttr &new_round_info,
    bool &need_checkpoint) const
{
  int ret = OB_SUCCESS;
  // Current existing log stream count.
  int64_t actual_count = counter.ls_count_ - counter.deleted_ls_count_;
  SCN next_checkpoint_scn = SCN::min_scn();
  need_checkpoint = true;
  if (OB_FAIL(new_round_info.deep_copy_from(old_round_info))) {
    LOG_WARN("failed to deep copy round info", K(ret), K(old_round_info), K(counter));
  } else if (counter.ls_count_ == counter.not_start_cnt_) {
    // no log stream is archiving.
  } else if (OB_FAIL(ObTenantArchiveMgr::decide_piece_id(old_round_info.start_scn_, old_round_info.base_piece_id_, 
      old_round_info.piece_switch_interval_, counter.max_scn_, new_round_info.used_piece_id_))) {
    LOG_WARN("failed to calc MAX piece id", K(ret), K(old_round_info), K(counter));
  } else if (OB_FALSE_IT(new_round_info.max_scn_ = counter.max_scn_)) {
  } else if (OB_FAIL(calc_next_checkpoint_scn_(old_round_info, summary, counter, next_checkpoint_scn))) {
    LOG_WARN("failed to calc next checkpoint scn", K(ret), K(old_round_info), K(summary), K(counter));
  }


  if (OB_FAIL(ret)) {
  } else if (old_round_info.state_.is_beginning()) {
    if (counter.not_start_cnt_ > 0) {
      need_checkpoint = false;
    } else if (counter.interrupted_cnt_ > 0) {
      ObSqlString comment;
      new_round_info.state_.set_interrupted();
      if (OB_FAIL(comment.append_fmt("log stream %ld interrupted.", counter.interrupted_ls_id_.id()))) {
        LOG_WARN("failed to append interrupted log stream comment", K(ret), K(new_round_info), K(counter));
      } else if (OB_FAIL(new_round_info.comment_.assign(comment.ptr()))) {
        LOG_WARN("failed to assign comment", K(ret), K(new_round_info), K(counter), K(comment));
      }
      LOG_INFO("switch to INTERRUPTED state", K(ret), K(old_round_info), K(counter), K(new_round_info));
    } else if (next_checkpoint_scn <= old_round_info.start_scn_) {
      need_checkpoint = false;
    } else if (OB_FALSE_IT(new_round_info.checkpoint_scn_ = next_checkpoint_scn)) {
    } else if (counter.doing_cnt_ == actual_count) {
      new_round_info.state_.set_doing();
      LOG_INFO("switch to DOING state", K(old_round_info), K(counter), K(new_round_info));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error occur", K(ret), K(old_round_info), K(counter), K(new_round_info));
    }
  } else if (old_round_info.state_.is_doing()) {
    if (counter.not_start_cnt_ > 0) {
      need_checkpoint = false;
    } else if (OB_FALSE_IT(new_round_info.checkpoint_scn_ = next_checkpoint_scn)) {
    } else if (counter.interrupted_cnt_ > 0) {
      ObSqlString comment;
      new_round_info.state_.set_interrupted();
      if (OB_FAIL(comment.append_fmt("log stream %ld interrupted.", counter.interrupted_ls_id_.id()))) {
        LOG_WARN("failed to append interrupted log stream comment", K(ret), K(new_round_info), K(counter));
      } else if (OB_FAIL(new_round_info.comment_.assign(comment.ptr()))) {
        LOG_WARN("failed to assign comment", K(ret), K(new_round_info), K(counter), K(comment));
      }
      LOG_INFO("switch to INTERRUPTED state", K(ret), K(old_round_info), K(counter), K(new_round_info));
    } else if (counter.doing_cnt_ == actual_count) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error occur", K(ret), K(old_round_info), K(counter), K(new_round_info));
    }
  } else if (old_round_info.state_.is_suspending()) {
    if (counter.suspended_cnt_ + counter.not_start_cnt_ == actual_count) {
      // previous state is SUSPENDING, expected next state is SUSPEND.
      new_round_info.state_.set_suspend();
      LOG_INFO("switch to SUSPEND state", K(old_round_info), K(counter), K(new_round_info));
    } else if (counter.suspended_cnt_ + counter.not_start_cnt_ < actual_count) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error occur", K(ret), K(old_round_info), K(counter), K(new_round_info));
    }
  } else if (old_round_info.state_.is_stopping()) {
    if (counter.stopped_cnt_ + counter.not_start_cnt_ == actual_count) {
      // previous state is STOPPING, expected next state is STOP.
      new_round_info.state_.set_stop();
      LOG_INFO("switch to STOP state", K(old_round_info), K(counter), K(new_round_info));
    } else if (counter.stopped_cnt_ + counter.not_start_cnt_ < actual_count) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error occur", K(ret), K(old_round_info), K(counter), K(new_round_info));
    }
  }

  return ret;
}


int ObDestRoundCheckpointer::checkpoint_(const ObTenantArchiveRoundAttr &old_round_info, const ObDestRoundSummary &summary,
    ObDestRoundCheckpointer::Result &result) const
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantArchivePieceAttr> pieces;
  if (OB_FAIL(generate_pieces_(old_round_info, summary, result))) {
    LOG_WARN("failed to generate pieces", K(ret), K(old_round_info), K(summary));
  } else if (OB_FAIL(round_checkpoint_cb_(round_handler_->get_sql_proxy(), old_round_info, result.new_round_info_))) {
    if (result.new_round_info_.state_.is_stop() && allow_force_stop_) {
      ret = OB_SUCCESS;
      LOG_INFO("allow_force_stop is set, ignore round_checkpoint_cb error", K(old_round_info), K(summary), K(result));
    } else {
      LOG_WARN("failed to call round_checkpoint_cb", K(ret), K(old_round_info), K(summary), K(result));
    }
  }

  if (FAILEDx(fill_generated_pieces_(result, pieces))){
    LOG_WARN("failed to fill generated pieces", K(ret), K(old_round_info), K(summary), K(result));
  } else if (OB_FAIL(round_handler_->checkpoint_to(old_round_info, result.new_round_info_, pieces))) {
    LOG_WARN("failed to checkpoint", K(ret), K(old_round_info), K(summary), K(result));
  }

  return ret;
}

int ObDestRoundCheckpointer::generate_pieces_(const ObTenantArchiveRoundAttr &old_round_info, const ObDestRoundSummary &summary, 
    Result &result) const
{
  int ret = OB_SUCCESS;

  if (old_round_info.state_ == result.new_round_info_.state_ && result.new_round_info_.max_scn_ == old_round_info.start_scn_) {
    // No log stream started archive before disable archive, then no piece generated in the round.
    LOG_INFO("no piece generated.", K(old_round_info), K(result));
  } else {
    int64_t active_input_bytes = 0;
    int64_t active_output_bytes = 0;
    int64_t frozen_input_bytes = 0;
    int64_t frozen_output_bytes = 0;

    int64_t since_piece_id = 0;
    if (OB_FAIL(ObTenantArchiveMgr::decide_piece_id(old_round_info.start_scn_, old_round_info.base_piece_id_, old_round_info.piece_switch_interval_, old_round_info.checkpoint_scn_, since_piece_id))) {
      LOG_WARN("failed to calc since piece id", K(ret), K(old_round_info));
    }

    // generate pieces from last active and valid piece id to 'to_piece_id'
    for (int64_t piece_id = since_piece_id; OB_SUCC(ret) && piece_id <= result.new_round_info_.used_piece_id_; piece_id++) {
      GeneratedPiece piece;
      if (OB_FAIL(generate_one_piece_(old_round_info, result.new_round_info_, summary, piece_id, piece))) {
        LOG_WARN("failed to generate one piece", K(ret), K(old_round_info), K(result), K(summary), K(piece_id));
      } else if (OB_FAIL(piece_generated_cb_(round_handler_->get_sql_proxy(), old_round_info, result, piece))) {
        if (result.new_round_info_.state_.is_stop() && allow_force_stop_) {
          ret = OB_SUCCESS;
          LOG_INFO("allow_force_stop is set, ignore piece_generated_cb_ error", K(old_round_info), K(piece));
        } else {
          LOG_WARN("call piece_generated_cb_ failed", K(ret), K(old_round_info), K(piece));
        }
      }

      if (FAILEDx(result.piece_list_.push_back(piece))) {
        LOG_WARN("failed to push back piece", K(ret), K(result), K(piece));
      } else if (piece.piece_info_.status_.is_frozen()) {
        frozen_input_bytes += piece.piece_info_.input_bytes_;
        frozen_output_bytes += piece.piece_info_.output_bytes_;
      } else {
        // active piece
        active_input_bytes += piece.piece_info_.input_bytes_;
        active_output_bytes += piece.piece_info_.output_bytes_;
      }
    }

    if (OB_SUCC(ret)) {
      result.new_round_info_.frozen_input_bytes_ += frozen_input_bytes;
      result.new_round_info_.frozen_output_bytes_ += frozen_output_bytes;
      result.new_round_info_.active_input_bytes_ = active_input_bytes;
      result.new_round_info_.active_output_bytes_ = active_output_bytes;
    }
  }

  return ret;
}


int ObDestRoundCheckpointer::generate_one_piece_(const ObTenantArchiveRoundAttr &old_round_info, const ObTenantArchiveRoundAttr &new_round_info, const ObDestRoundSummary &summary, 
    const int64_t piece_id, GeneratedPiece &piece) const
{
  int ret = OB_SUCCESS;
  int64_t max_active_piece_id = 0;
   
  piece.piece_info_.key_.tenant_id_ = new_round_info.key_.tenant_id_;
  piece.piece_info_.key_.dest_id_ = new_round_info.dest_id_;
  piece.piece_info_.key_.round_id_ = new_round_info.round_id_;
  piece.piece_info_.key_.piece_id_ = piece_id;
  piece.piece_info_.incarnation_ = new_round_info.incarnation_;
  piece.piece_info_.dest_no_ = new_round_info.key_.dest_no_;
  piece.piece_info_.file_count_ = 0;
  piece.piece_info_.compatible_ = new_round_info.compatible_;

  if (OB_FAIL(ObTenantArchiveMgr::decide_piece_id(new_round_info.start_scn_, new_round_info.base_piece_id_, new_round_info.piece_switch_interval_, new_round_info.checkpoint_scn_, max_active_piece_id))) {
    LOG_WARN("failed to calc MAX active piece id", K(ret), K(new_round_info), K(piece_id));
  } else if (OB_FAIL(ObTenantArchiveMgr::decide_piece_start_scn(new_round_info.start_scn_, new_round_info.base_piece_id_, new_round_info.piece_switch_interval_, piece_id, piece.piece_info_.start_scn_))) {
    LOG_WARN("failed to calc piece base ts", K(ret), K(new_round_info), K(piece_id));
  } else if (OB_FAIL(ObTenantArchiveMgr::decide_piece_end_scn(new_round_info.start_scn_, new_round_info.base_piece_id_, new_round_info.piece_switch_interval_, piece_id, piece.piece_info_.end_scn_))) {
    LOG_WARN("failed to calc piece end ts", K(ret), K(new_round_info), K(piece_id));
  } else if (OB_FAIL(piece.piece_info_.set_path(new_round_info.path_))) {
    LOG_WARN("failed to set path", K(ret), K(piece), K(new_round_info));
  }

  // stat data amount and checkpoint ts for current piece.
  const ObArray<ObLSDestRoundSummary> &ls_round_list = summary.ls_round_list_;
  piece.piece_info_.checkpoint_scn_ = SCN::max_scn();
  piece.piece_info_.max_scn_ = piece.piece_info_.start_scn_;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_round_list.count(); i++) {
    const ObLSDestRoundSummary &ls_round = ls_round_list.at(i);
    // search the piece
    int64_t idx = ls_round.get_piece_idx(piece_id);
    if (-1 == idx) {
      LOG_INFO("ls piece not found", K(ret), K(piece_id), K(ls_round));
    } else {
      // piece is found.
      // fill ls piece
      const ObLSDestRoundSummary::OnePiece &ls_piece = ls_round.piece_list_.at(idx);
      GeneratedLSPiece gen_ls_piece;
      gen_ls_piece.ls_id_ = ls_round.ls_id_;
      gen_ls_piece.start_scn_ = ls_piece.start_scn_;
      gen_ls_piece.checkpoint_scn_ = ls_piece.checkpoint_scn_;
      gen_ls_piece.min_lsn_ = ls_piece.min_lsn_;
      gen_ls_piece.max_lsn_ = ls_piece.max_lsn_;
      gen_ls_piece.input_bytes_ = ls_piece.input_bytes_;
      gen_ls_piece.output_bytes_ = ls_piece.output_bytes_;
      gen_ls_piece.is_ls_deleted_ = false;


      // fill piece
      bool last_piece = false;
      if (OB_FAIL(ls_round.check_is_last_piece_for_deleted_ls(piece_id, last_piece))) {
        LOG_WARN("failed to check is last piece for deleted ls", K(ret));
      } else if (last_piece) {
        // If the ls is deleted, and this is the last piece. It should not
        // affect the checkpoint_scn.
        // Mark the last piece deleted for deleted ls. For example, piece 10 and 11 is found of
        // a deleted ls for current checkpoint, piece 10 is not marked with deleted, but piece 11
        // is marked with deleted.
        gen_ls_piece.is_ls_deleted_ = true;
      } else {
        // checkpoint scn may be smaller than start scn for empty piece.
        piece.piece_info_.checkpoint_scn_ = MAX(piece.piece_info_.start_scn_, MIN(piece.piece_info_.checkpoint_scn_, ls_piece.checkpoint_scn_));
      }

      piece.piece_info_.max_scn_ = MAX(piece.piece_info_.max_scn_, ls_piece.checkpoint_scn_);
      piece.piece_info_.input_bytes_ += ls_piece.input_bytes_;
      piece.piece_info_.output_bytes_ += ls_piece.output_bytes_;

      if (FAILEDx(piece.ls_piece_list_.push_back(gen_ls_piece))) {
        LOG_WARN("failed to push back ls piece", K(ret), K(piece), K(gen_ls_piece));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (piece_id < max_active_piece_id) {
    piece.piece_info_.status_.set_frozen();
    piece.piece_info_.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE;
  } else if (piece_id == max_active_piece_id) {
    piece.piece_info_.checkpoint_scn_ = MIN(new_round_info.checkpoint_scn_, piece.piece_info_.checkpoint_scn_);
    piece.piece_info_.status_.set_active();
    if (piece.piece_info_.checkpoint_scn_ > new_round_info.start_scn_
        && piece.piece_info_.checkpoint_scn_ >= piece.piece_info_.start_scn_) {
      // As the scn of one log group is the max log scn among the log entries. If checkpoint_scn_
      // is equal to start_scn_, the piece is not empty, and may be used for restore. For example,
      // Piece#1 : <2022-06-01 06:00:00, 2022-06-02 05:00:00, 2022-06-02 06:00:00>
      // Piece#2 : <2022-06-02 06:00:00, 2022-06-02 06:00:00, 2022-06-03 06:00:00>
      // And the first log group in Piece#2 with scn range [2022-06-02 05:30:00, 2022-06-02 06:00:00], this piece
      // is required while restore to 2022-06-02 05:40:00.
      piece.piece_info_.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE;
    } else {
      piece.piece_info_.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE;
    }
  } else {
    // piece_id > max_active_piece_id
    piece.piece_info_.status_.set_active();
    piece.piece_info_.checkpoint_scn_ = piece.piece_info_.start_scn_;
    piece.piece_info_.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE;
  }

  if (OB_FAIL(ret)) {
  } else if (new_round_info.state_.is_stop()) {
    // next state is stop, force set piece frozon.
    piece.piece_info_.status_.set_frozen();
  }

  return ret;
}

bool ObDestRoundCheckpointer::can_do_checkpoint_(const ObTenantArchiveRoundAttr &round_info) const
{
  return round_info.state_.is_beginning() || round_info.state_.is_doing() || round_info.state_.is_stopping() || round_info.state_.is_suspending();
}

int ObDestRoundCheckpointer::fill_generated_pieces_(const Result &result, common::ObIArray<ObTenantArchivePieceAttr> &pieces) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < result.piece_list_.count(); i++) {
    const GeneratedPiece &gen_piece = result.piece_list_.at(i);
    if (OB_FAIL(pieces.push_back(gen_piece.piece_info_))) {
      LOG_WARN("failed to push backup piece", K(ret), K(i), K(gen_piece));
    }
  }

  return ret;
}
