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

#include "fake_archive_piece_mgr.h"
#include "lib/ob_errno.h"
#include "logservice/palf/log_define.h"
#include "share/scn.h"
#include <cstdint>

namespace oceanbase
{
using namespace palf;
namespace unittest
{
FakePieceComponent::FakePieceComponent()
  : state_(FakePieceState::INVALID),
  piece_id_(0),
  min_file_id_(0),
  max_file_id_(0),
  min_lsn_(palf::LOG_INVALID_LSN_VAL),
  max_lsn_(palf::LOG_INVALID_LSN_VAL)
{}

FakeArchiveComponent::FakeArchiveComponent()
  : state_(FakeRoundState::INVALID),
  round_id_(0),
  start_scn_(),
  end_scn_(),
  base_piece_id_(0),
  base_piece_scn_(),
  piece_switch_interval_(0),
  min_piece_id_(0),
  max_piece_id_(0),
  array_()
{}

int FakePieceComponent::assgin(const FakePieceComponent &other)
{
  int ret = OB_SUCCESS;
  state_ = other.state_;
  piece_id_ = other.piece_id_;
  min_file_id_ = other.min_file_id_;
  max_file_id_ = other.max_file_id_;
  min_lsn_ = other.min_lsn_;
  max_lsn_ = other.max_lsn_;
  return ret;
}

int FakeArchiveComponent::get_piece(const int64_t piece_id, FakePieceComponent *&piece)
{
  int ret = OB_SUCCESS;
  bool done = false;
  for (int64_t i = 0; ! done && i < array_.count(); i++) {
    if (array_.at(i).piece_id_ == piece_id) {
      done = true;
      piece = &array_.at(i);
    }
  }
  if (! done) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int FakeArchiveComponent::assgin(const FakeArchiveComponent &other)
{
  int ret = OB_SUCCESS;
  state_ = other.state_;
  round_id_ = other.round_id_;
  start_scn_ = other.start_scn_;
  end_scn_ = other.end_scn_;
  base_piece_id_ = other.base_piece_id_;
  base_piece_scn_ = other.base_piece_scn_;
  piece_switch_interval_ = other.piece_switch_interval_;
  min_piece_id_ = other.min_piece_id_;
  max_piece_id_ = other.max_piece_id_;
  ret = array_.assign(other.array_);
  return ret;
}

int FakeRounds::get_round(const int64_t round_id, FakeArchiveComponent *&component)
{
  int ret = OB_SUCCESS;
  bool done = false;
  for (int64_t i = 0; i < array_.count() && ! done; i++) {
    if (array_.at(i).round_id_ == round_id) {
      done = true;
      component = &array_.at(i);
    }
  }
  if (! done) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int FakeArchivePieceContext::init(const share::ObLSID &id, FakeRounds *rounds)
{
  int ret = OB_SUCCESS;
  if (!id.is_valid() || nullptr == rounds) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    id_ = id;
    rounds_ = rounds;
    archive_dest_.set("file:///data/1/");
    is_inited_ = true;
  }
  return ret;
}

int FakeArchivePieceContext::load_archive_meta_()
{
  dest_id_ = 1;
  return OB_SUCCESS;
}

int FakeArchivePieceContext::get_round_(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  RoundArray *array = &(rounds_->array_);
  bool done = false;
  for (int64_t i = 0; ! done && i < array->count(); i ++) {
    FakeArchiveComponent *component = &array->at(i);
    if (component->end_scn_ > start_scn) {
      done = true;
      round_context_.reset();
      round_context_.round_id_ = component->round_id_;
      locate_round_ = true;
    }
  }

  if (! done) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int FakeArchivePieceContext::get_round_range_()
{
  int ret = OB_SUCCESS;
  dest_id_ = 1;
  min_round_id_ = rounds_->array_.at(0).round_id_;
  max_round_id_ = rounds_->array_.at(rounds_->array_.count() - 1).round_id_;
  return ret;
}

int FakeArchivePieceContext::load_round_(const int64_t round_id, RoundContext &round_context, bool &exist)
{
  int ret = OB_SUCCESS;
  FakeArchiveComponent *component = NULL;
  exist = false;
  if (OB_FAIL(rounds_->get_round(round_id, component))) {
  } else {
    exist = true;
    round_context.round_id_ = round_id;
    round_context.start_scn_ = component->start_scn_;
    round_context.base_piece_id_ = component->base_piece_id_;
    round_context.base_piece_scn_ = component->base_piece_scn_;
    round_context.piece_switch_interval_ = component->piece_switch_interval_;
    bool end_exist = false;
    if (component->state_ == FakeRoundState::STOP) {
      end_exist = true;
    }
    if (end_exist) {
      round_context.state_ = RoundContext::State::STOP;
      round_context.end_scn_ = component->end_scn_;;
      round_context.max_piece_id_ = cal_piece_id_(round_context.end_scn_);
    } else {
      round_context.state_ = RoundContext::State::ACTIVE;
      round_context.end_scn_ = share::SCN::max_scn();
    }

  }
  return ret;
}

int FakeArchivePieceContext::get_round_piece_range_(const int64_t round_id, int64_t &min_piece_id, int64_t &max_piece_id)
{
  int ret = OB_SUCCESS;
  FakeArchiveComponent *component = NULL;
  if (OB_FAIL(rounds_->get_round(round_id, component))) {
  } else {
    min_piece_id = component->min_piece_id_;
    max_piece_id = component->max_piece_id_;
  }
  return ret;
}

int FakeArchivePieceContext::check_round_exist_(const int64_t round_id, bool &exist)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; !exist && i < rounds_->array_.count(); i++) {
    FakeArchiveComponent *component = &rounds_->array_.at(i);
    if (component->round_id_ == round_id) {
      exist = true;
    } else if (component->round_id_ > round_id) {
      break;
    }
  }
  return ret;
}

int FakeArchivePieceContext::get_piece_meta_info_(const int64_t piece_id)
{
  int ret = OB_SUCCESS;
  const int64_t round_id = round_context_.round_id_;
  FakeArchiveComponent *component = NULL;
  if (OB_FAIL(rounds_->get_round(round_id, component))) {
  } else {
    inner_piece_context_.round_id_ = round_id;
    inner_piece_context_.piece_id_ = piece_id;
    FakePieceComponent *piece = nullptr;
    if (OB_FAIL(component->get_piece(piece_id, piece))) {
    } else if (FakePieceState::ACTIVE == piece->state_) {
      inner_piece_context_.state_ = InnerPieceContext::State::ACTIVE;
      if (piece->min_file_id_ == 0) {
        ret = common::OB_ITER_END;
      }
    } else if (FakePieceState::EMPTY == piece->state_) {
      inner_piece_context_.state_ = InnerPieceContext::State::EMPTY;
      inner_piece_context_.max_lsn_in_piece_ = piece->max_lsn_;
    } else {
      inner_piece_context_.state_ = InnerPieceContext::State::FROZEN;
      inner_piece_context_.max_lsn_in_piece_ = piece->max_lsn_;
      inner_piece_context_.min_file_id_ = piece->min_file_id_;
      inner_piece_context_.max_file_id_ = piece->max_file_id_;
    }
  }
  return ret;
}

int FakeArchivePieceContext::get_piece_file_range_()
{
  int ret = OB_SUCCESS;
  const int64_t round_id = round_context_.round_id_;
  const int64_t piece_id = inner_piece_context_.piece_id_;
  FakeArchiveComponent *component = NULL;
  FakePieceComponent *piece = nullptr;
  if (OB_FAIL(rounds_->get_round(round_id, component))) {
  } else if (OB_FAIL(component->get_piece(piece_id, piece))) {
  } else {
    if (FakePieceState::EMPTY == piece->state_) {
    } else if (FakePieceState::FRONZEN == piece->state_) {
      inner_piece_context_.min_file_id_ = piece->min_file_id_;
      inner_piece_context_.max_file_id_ = piece->max_file_id_;
    } else {
      inner_piece_context_.min_file_id_ = piece->min_file_id_;
      inner_piece_context_.max_file_id_ = piece->max_file_id_;
    }
  }
  return ret;
}

int FakeArchivePieceContext::get_min_lsn_in_piece_()
{
  int ret = OB_SUCCESS;
  const int64_t round_id = round_context_.round_id_;
  const int64_t piece_id = inner_piece_context_.piece_id_;
  FakeArchiveComponent *component = NULL;
  FakePieceComponent *piece = nullptr;
  if (OB_FAIL(rounds_->get_round(round_id, component))) {
  } else if (OB_FAIL(component->get_piece(piece_id, piece))) {
  } else {
    inner_piece_context_.min_lsn_in_piece_ = piece->min_lsn_;
  }
  return ret;
}

}
}
