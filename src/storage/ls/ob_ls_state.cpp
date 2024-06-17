/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ls/ob_ls_state.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{


// the state machine of ObLSRunningState
//STATE \ ACTION    CREATE_FINISH   ONLINE    PRE_OFFLINE    POST_OFFLINE    STOP
//--------------------------------------------------------------------------------
//INIT              OFFLINED        N         N              N               STOPPED
//RUNNING           N               RUNNING   OFFLINING      N               N
//OFFLINING         N               N         OFFLINING      OFFLINED        N
//OFFLINED          N               RUNNING   OFFLINED       OFFLINED        STOPPED
//STOPPED           N               N         STOPPED        STOPPED         STOPPED
int ObLSRunningState::StateHelper::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = State::INVALID;
  static const int64_t LS_INIT = State::LS_INIT;
  static const int64_t LS_RUNNING = State::LS_RUNNING;
  static const int64_t LS_OFFLINING = State::LS_OFFLINING;
  static const int64_t LS_OFFLINED = State::LS_OFFLINED;
  static const int64_t LS_STOPPED = State::LS_STOPPED;

  static const int64_t STATE_MAP[State::MAX][Ops::MAX] = {
          //      CREATE_FINISH   ONLINE       PRE_OFFLINE    POST_OFFLINE       STOP
/* INIT */        {LS_OFFLINED,   N,           N,             N,                 LS_STOPPED},
/* RUNNING */     {N,             LS_RUNNING,  LS_OFFLINING,  N,                 N},
/* OFFLINING */   {N,             N,           LS_OFFLINING,  LS_OFFLINED,       N},
/* OFFLINED */    {N,             LS_RUNNING,  LS_OFFLINED,   LS_OFFLINED,       LS_STOPPED},
/* STOPPED */     {N,             N,           LS_STOPPED,    LS_STOPPED,        LS_STOPPED},
  };

  if (OB_UNLIKELY(!Ops::is_valid(op))) {
    LOG_WARN("invalid argument", K(op));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!State::is_valid(state_))) {
    LOG_WARN("ObLSRunningState current state is invalid", K_(state), K(op));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t new_state = STATE_MAP[state_][op];
    if (OB_UNLIKELY(!State::is_valid(new_state))) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      last_state_ = state_;
      state_ = new_state;
    }
  }
  if (OB_SUCC(ret)) {
    _LOG_INFO("ObLSRunningState switch state success(ls_id=%jd, %s ~> %s, op=%s)",
              ls_id_.id(), State::state_str(last_state_), State::state_str(state_), Ops::op_str(op));
  } else {
    _LOG_ERROR("ObLSRunningState switch state error(ret=%d, ls_id=%jd, state=%s, op=%s)",
               ret, ls_id_.id(), State::state_str(state_), Ops::op_str(op));
  }
  return ret;
}

int ObLSRunningState::create_finish(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::CREATE_FINISH))) {
    LOG_WARN("create finish failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSRunningState::online(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::ONLINE))) {
    LOG_WARN("online failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSRunningState::pre_offline(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::PRE_OFFLINE))) {
    LOG_WARN("pre offline failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSRunningState::post_offline(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::POST_OFFLINE))) {
    LOG_WARN("post offline failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSRunningState::stop(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::STOP))) {
    LOG_WARN("stop failed", K(ret), K(ls_id));
  }
  return ret;
}

ObLSPersistentState &ObLSPersistentState::operator=(const ObLSPersistentState &other)
{
  if (this != &other) {
    state_ = other.state_;
  }
  return *this;
}

ObLSPersistentState &ObLSPersistentState::operator=(const int64_t state)
{
  state_ = state;
  return *this;
}

// the state machine of ObLSPersistentState
//STATE \ ACTION    START_WORK   START_HA    FINISH_HA      REMOVE
//-------------------------------------------------------------------
//INIT              NORMAL       HA          N              ZOMBIE
//NORMAL            NORMAL       HA          N              ZOMBIE
//CREATE_ABORTED    N            N           N              N
//ZOMBIE            N            N           N              ZOMBIE
//HA                N            HA          NORMAL         ZOMBIE
int ObLSPersistentState::StateHelper::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = State::INVALID;
  static const int64_t LS_INIT = State::LS_INIT;
  static const int64_t LS_NORMAL = State::LS_NORMAL;
  static const int64_t LS_CREATE_ABORTED = State::LS_CREATE_ABORTED;
  static const int64_t LS_ZOMBIE = State::LS_ZOMBIE;
  static const int64_t LS_HA = State::LS_HA;

  static const int64_t STATE_MAP[State::MAX][Ops::MAX] = {
          //         START_WORK      START_HA     FINISH_HA      REMOVE
/* INIT */           {LS_NORMAL,     LS_HA,       N,             LS_ZOMBIE},
/* NORMAL */         {LS_NORMAL,     LS_HA,       N,             LS_ZOMBIE},
/* CREATE_ABORTED */ {N,             N,           N,             N},
/* ZOMBIE */         {N,             N,           N,             LS_ZOMBIE},
/* HA */             {N,             LS_HA,       LS_NORMAL,     LS_ZOMBIE},
  };

  if (OB_UNLIKELY(!Ops::is_valid(op))) {
    LOG_WARN("invalid argument", K(op));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!State::is_valid(state_))) {
    LOG_WARN("ObLSPersistentState current state is invalid", K_(state), K(op));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t new_state = STATE_MAP[state_][op];
    if (OB_UNLIKELY(!State::is_valid(new_state))) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      last_state_ = state_;
      state_ = new_state;
    }
  }
  if (OB_SUCC(ret)) {
    _LOG_INFO("ObLSPersistentState switch state success(ls_id=%jd, %s ~> %s, op=%s)",
              ls_id_.id(), State::state_str(last_state_), State::state_str(state_), Ops::op_str(op));
  } else {
    _LOG_ERROR("ObLSPersistentState switch state error(ret=%d, ls_id=%jd, state=%s, op=%s)",
               ret, ls_id_.id(), State::state_str(state_), Ops::op_str(op));
  }
  return ret;
}

int ObLSPersistentState::start_work(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::START_WORK))) {
    LOG_WARN("start work failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSPersistentState::start_ha(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::START_HA))) {
    LOG_WARN("start ha failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSPersistentState::finish_ha(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::FINISH_HA))) {
    LOG_WARN("finish ha failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSPersistentState::remove(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(ls_id, state_);
  if (OB_FAIL(state_helper.switch_state(Ops::REMOVE))) {
    LOG_WARN("remove failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObLSPersistentState::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, state_))) {
    LOG_WARN("serialize state failed", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObLSPersistentState::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &state_))) {
    LOG_WARN("deserialize state failed", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObLSPersistentState::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(state_);
  return size;
}

}
}
