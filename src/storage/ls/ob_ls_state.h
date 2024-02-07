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

#ifndef OCEABASE_STORAGE_LS_STATE_
#define OCEABASE_STORAGE_LS_STATE_
namespace oceanbase
{
namespace share
{
class ObLSID;
}

namespace storage
{
class ObLSRunningState
{
public:
  ObLSRunningState() : state_(State::LS_INIT) {}
  ~ObLSRunningState() {}
  bool is_offline() const
  {
    return (State::LS_INIT == state_ ||
            State::LS_OFFLINING == state_ ||
            State::LS_OFFLINED == state_ ||
            State::LS_STOPPED == state_);
  }
  bool is_running() const
  {
    return State::LS_RUNNING == state_;
  }
  bool is_stopped() const
  {
    return (State::LS_INIT == state_ ||
            State::LS_STOPPED == state_);
  }
  int create_finish(const share::ObLSID &ls_id);
  int online(const share::ObLSID &ls_id);
  int pre_offline(const share::ObLSID &ls_id);
  int post_offline(const share::ObLSID &ls_id);
  int stop(const share::ObLSID &ls_id);
private:
  class State
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t LS_INIT = 0;
    static const int64_t LS_RUNNING = 1;
    static const int64_t LS_OFFLINING = 2;
    static const int64_t LS_OFFLINED = 3;
    static const int64_t LS_STOPPED = 4;
    static const int64_t MAX = 5;
  public:
    static bool is_valid(const int64_t state)
    { return state > INVALID && state < MAX; }

    #define TCM_STATE_CASE_TO_STR(state)            \
    case state:                                 \
      str = #state;                             \
      break;
    static const char* state_str(uint64_t state)
    {
      const char* str = "INVALID";
      switch (state) {
        TCM_STATE_CASE_TO_STR(LS_INIT);
        TCM_STATE_CASE_TO_STR(LS_RUNNING);
        TCM_STATE_CASE_TO_STR(LS_OFFLINING);
        TCM_STATE_CASE_TO_STR(LS_OFFLINED);
        TCM_STATE_CASE_TO_STR(LS_STOPPED);
      default:
        break;
      }
      return str;
    }
    #undef TCM_STATE_CASE_TO_STR
  };

  class Ops
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t CREATE_FINISH = 0;
    static const int64_t ONLINE = 1;
    static const int64_t PRE_OFFLINE = 2;
    static const int64_t POST_OFFLINE = 3;
    static const int64_t STOP = 4;
    static const int64_t MAX = 5;
  public:
    static bool is_valid(const int64_t op)
    { return op > INVALID && op < MAX; }

    #define TCM_OP_CASE_TO_STR(op)                  \
    case op:                                    \
      str = #op;                                \
      break;

    static const char* op_str(uint64_t op)
    {
      const char* str = "INVALID";
      switch (op) {
        TCM_OP_CASE_TO_STR(CREATE_FINISH);
        TCM_OP_CASE_TO_STR(ONLINE);
        TCM_OP_CASE_TO_STR(PRE_OFFLINE);
        TCM_OP_CASE_TO_STR(POST_OFFLINE);
        TCM_OP_CASE_TO_STR(STOP);
      default:
        break;
      }
      return str;
    }
    #undef TCM_OP_CASE_TO_STR
  };

  class StateHelper
  {
  public:
    explicit StateHelper(const share::ObLSID &ls_id, int64_t &state)
      : ls_id_(ls_id), state_(state), last_state_(state) {}
    ~StateHelper() {}
    int switch_state(const int64_t op);
  private:
    const share::ObLSID &ls_id_;
    int64_t &state_;
    int64_t last_state_;
  };
public:
  TO_STRING_KV(K_(state));
private:
  int64_t state_;
};

class ObLSPersistentState
{
public:
  ObLSPersistentState() : state_(State::LS_INIT) {}
  ObLSPersistentState(const int64_t state) : state_(state) {}
  ~ObLSPersistentState() {}
  ObLSPersistentState &operator=(const ObLSPersistentState &other);
  ObLSPersistentState &operator=(const int64_t state);
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;

  int start_work(const share::ObLSID &ls_id);
  int start_ha(const share::ObLSID &ls_id);
  int finish_ha(const share::ObLSID &ls_id);
  int remove(const share::ObLSID &ls_id);
  inline bool can_update_ls_meta() const
  {
    return (State::LS_NORMAL == state_ ||
            State::LS_HA == state_);
  }
  bool is_need_gc() const
  {
    return (State::LS_INIT == state_ ||
            State::LS_ZOMBIE == state_ ||
            State::LS_CREATE_ABORTED == state_);
  }
  bool is_init_state() const
  {
    return (State::LS_INIT == state_);
  }
  bool is_normal_state() const
  {
    return (State::LS_NORMAL == state_);
  }
  bool is_zombie_state() const
  {
    return (State::LS_ZOMBIE == state_ ||
            State::LS_CREATE_ABORTED == state_);
  }
  bool is_ha_state() const
  {
    return (State::LS_HA == state_);
  }
public:
  class State
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t LS_INIT = 0;
    static const int64_t LS_NORMAL = 1;
    static const int64_t LS_CREATE_ABORTED = 2;
    static const int64_t LS_ZOMBIE = 3;
    static const int64_t LS_HA = 4;
    static const int64_t MAX = 5;
  public:
    static bool is_valid(const int64_t state)
    { return state > INVALID && state < MAX; }

    #define TCM_STATE_CASE_TO_STR(state)            \
    case state:                                 \
      str = #state;                             \
      break;
    static const char* state_str(uint64_t state)
    {
      const char* str = "INVALID";
      switch (state) {
        TCM_STATE_CASE_TO_STR(LS_INIT);
        TCM_STATE_CASE_TO_STR(LS_NORMAL);
        TCM_STATE_CASE_TO_STR(LS_CREATE_ABORTED);
        TCM_STATE_CASE_TO_STR(LS_ZOMBIE);
        TCM_STATE_CASE_TO_STR(LS_HA);
      default:
        break;
      }
      return str;
    }
    #undef TCM_STATE_CASE_TO_STR
  };

  class Ops
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t START_WORK = 0;
    static const int64_t START_HA = 1;
    static const int64_t FINISH_HA = 2;
    static const int64_t REMOVE = 3;
    static const int64_t MAX = 4;
  public:
    static bool is_valid(const int64_t op)
    { return op > INVALID && op < MAX; }

    #define TCM_OP_CASE_TO_STR(op)                  \
    case op:                                    \
      str = #op;                                \
      break;

    static const char* op_str(uint64_t op)
    {
      const char* str = "INVALID";
      switch (op) {
        TCM_OP_CASE_TO_STR(START_WORK);
        TCM_OP_CASE_TO_STR(START_HA);
        TCM_OP_CASE_TO_STR(FINISH_HA);
        TCM_OP_CASE_TO_STR(REMOVE);
      default:
        break;
      }
      return str;
    }
    #undef TCM_OP_CASE_TO_STR
  };

  class StateHelper
  {
  public:
    explicit StateHelper(const share::ObLSID &ls_id, int64_t &state)
      : ls_id_(ls_id), state_(state), last_state_(state) {}
    ~StateHelper() {}
    int switch_state(const int64_t op);
  private:
    const share::ObLSID &ls_id_;
    int64_t &state_;
    int64_t last_state_;
  };
public:
  TO_STRING_KV(K_(state));
private:
  int64_t state_;
};


}
}
#endif
