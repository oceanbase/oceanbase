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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_SPLIT
#define OCEANBASE_STORAGE_OB_PARTITION_SPLIT

#include "lib/container/ob_se_array.h"
#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "clog/ob_i_submit_log_cb.h"
#include "storage/ob_non_trans_log.h"
#include "storage/ob_storage_log_type.h"
#include "share/ob_partition_modify.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

enum ObPartitionSplitStateEnum {
  UNKNOWN_SPLIT_STATE = -1,
  LEADER_INIT = 0,
  FOLLOWER_INIT = 1,
  /* ---source split --- */
  SPLIT_START = 2,
  SPLIT_TRANS_CLEAR = 3,
  SPLIT_SOURCE_LOGGING = 4,
  LEADER_SPLIT_SOURCE_LOG = 5,
  FOLLOWER_SPLIT_SOURCE_LOG = 6,
  SHUTDOWN_SUCCESS = 7,
  TABLE_REFERENCE_SUCCESS = 8,
  /* ---dest split --- */
  LEADER_WAIT_SPLIT = 9,
  FOLLOWER_WAIT_SPLIT = 10,
  SPLIT_DEST_LOGGING = 11,
  LEADER_LOGICAL_SPLIT_SUCCESS = 12,
  FOLLOWER_LOGICAL_SPLIT_SUCCESS = 13,

  MAX_SPLIT_STATE
};

enum ObPartitionSplitAction {
  UNKNOWN_SPLIT_ACTION = -1,
  LEADER_REVOKE = 0,
  LEADER_TAKEOVER = 1,
  /* ---source split --- */
  GET_RS_SPLIT_REQUEST = 2,
  ALL_TRANS_CLEAR = 3,
  SUBMIT_SOURCE_SPLIT_LOG_SUCCESS = 4,
  SOURCE_SPLIT_LOG_REACH_MAJORITY = 5,
  SOURCE_SHUTDOWN_SUCCESS = 6,
  SET_REFERENCE_TABLE_SUCCESS = 7,
  REPLAY_SOURCE_SPLIT_LOG = 8,
  /* ---dest split --- */
  WAIT_SPLIT = 9,
  GET_SOURCE_SPLIT_REQUEST = 10,
  DEST_SPLIT_LOG_REACH_MAJORITY = 11,
  REPLAY_DEST_SPLIT_LOG = 12,
  PHYSICAL_SPLIT_SUCCESS = 13,

  MAX_SPLIT_ACTION
};

inline const char* to_state_str(const ObPartitionSplitStateEnum state)
{
  const char* str = NULL;
  switch (state) {
    case UNKNOWN_SPLIT_STATE: {
      str = "UNKNOWN_SPLIT_STATE";
      break;
    }
    case LEADER_INIT: {
      str = "LEADER_INIT";
      break;
    }
    case FOLLOWER_INIT: {
      str = "FOLLOWER_INIT";
      break;
    }
    case SPLIT_START: {
      str = "SPLIT_START";
      break;
    }
    case SHUTDOWN_SUCCESS: {
      str = "SHUTDOWN_SUCCESS";
      break;
    }
    case TABLE_REFERENCE_SUCCESS: {
      str = "TABLE_REFERENCE_SUCCESS";
      break;
    }
    case SPLIT_TRANS_CLEAR: {
      str = "SPLIT_TRANS_CLEAR";
      break;
    }
    case SPLIT_SOURCE_LOGGING: {
      str = "SPLIT_SOURCE_LOGGING";
      break;
    }
    case LEADER_SPLIT_SOURCE_LOG: {
      str = "LEADER_SPLIT_SOURCE_LOG";
      break;
    }
    case FOLLOWER_SPLIT_SOURCE_LOG: {
      str = "FOLLOWER_SPLIT_SOURCE_LOG";
      break;
    }
    case LEADER_WAIT_SPLIT: {
      str = "LEADER_WAIT_SPLIT";
      break;
    }
    case FOLLOWER_WAIT_SPLIT: {
      str = "FOLLOWER_WAIT_SPLIT";
      break;
    }
    case SPLIT_DEST_LOGGING: {
      str = "SPLIT_DEST_LOGGING";
      break;
    }
    case LEADER_LOGICAL_SPLIT_SUCCESS: {
      str = "LEADER_LOGICAL_SPLIT_SUCCESS";
      break;
    }
    case FOLLOWER_LOGICAL_SPLIT_SUCCESS: {
      str = "FOLLOWER_LOGICAL_SPLIT_SUCCESS";
      break;
    }
    default: {
      str = "UNKNOWN";
      break;
    }
  }
  return str;
}

inline const char* to_action_str(const ObPartitionSplitAction action)
{
  const char* str = NULL;
  switch (action) {
    case UNKNOWN_SPLIT_ACTION: {
      str = "";
      break;
    }
    case LEADER_REVOKE: {
      str = "LEADER_REVOKE";
      break;
    }
    case LEADER_TAKEOVER: {
      str = "LEADER_TAKEOVER";
      break;
    }
    case GET_RS_SPLIT_REQUEST: {
      str = "GET_RS_SPLIT_REQUEST";
      break;
    }
    case SOURCE_SHUTDOWN_SUCCESS: {
      str = "SOURCE_SHUTDOWN_SUCCESS";
      break;
    }
    case SET_REFERENCE_TABLE_SUCCESS: {
      str = "SET_REFERENCE_TABLE_SUCCESS";
      break;
    }
    case ALL_TRANS_CLEAR: {
      str = "ALL_TRANS_CLEAR";
      break;
    }
    case SUBMIT_SOURCE_SPLIT_LOG_SUCCESS: {
      str = "SUBMIT_SOURCE_SPLIT_LOG_SUCCESS";
      break;
    }
    case SOURCE_SPLIT_LOG_REACH_MAJORITY: {
      str = "SOURCE_SPLIT_LOG_REACH_MAJORITY";
      break;
    }
    case REPLAY_SOURCE_SPLIT_LOG: {
      str = "REPLAY_SOURCE_SPLIT_LOG";
      break;
    }
    case WAIT_SPLIT: {
      str = "WAIT_SPLIT";
      break;
    }
    case GET_SOURCE_SPLIT_REQUEST: {
      str = "GET_SOURCE_SPLIT_REQUEST";
      break;
    }
    case DEST_SPLIT_LOG_REACH_MAJORITY: {
      str = "DEST_SPLIT_LOG_REACH_MAJORITY";
      break;
    }
    case REPLAY_DEST_SPLIT_LOG: {
      str = "REPLAY_DEST_SPLIT_LOG";
      break;
    }
    case PHYSICAL_SPLIT_SUCCESS: {
      str = "PHYSICAL_SPLIT_SUCCESS";
      break;
    }
    default: {
      str = "UNKNOWN";
      break;
    }
  }
  return str;
}

inline ObPartitionSplitStateEnum to_persistent_state(const ObPartitionSplitStateEnum state)
{
  ObPartitionSplitStateEnum ret_state = UNKNOWN_SPLIT_STATE;
  switch (state) {
    case LEADER_INIT:
      // go through
    case FOLLOWER_INIT:
      // go through
    case SPLIT_START:
      // go through
    case SPLIT_TRANS_CLEAR:
      // go through
    case SPLIT_SOURCE_LOGGING: {
      ret_state = FOLLOWER_INIT;
      break;
    }
    case LEADER_SPLIT_SOURCE_LOG:
      // go through
    case FOLLOWER_SPLIT_SOURCE_LOG:
      // go through
    case SHUTDOWN_SUCCESS:
      // go through
    case TABLE_REFERENCE_SUCCESS: {
      ret_state = FOLLOWER_SPLIT_SOURCE_LOG;
      break;
    }
    case LEADER_WAIT_SPLIT:
      // go through
    case FOLLOWER_WAIT_SPLIT:
      // go through
    case SPLIT_DEST_LOGGING: {
      ret_state = FOLLOWER_WAIT_SPLIT;
      break;
    }
    case LEADER_LOGICAL_SPLIT_SUCCESS:
      // go through
    case FOLLOWER_LOGICAL_SPLIT_SUCCESS: {
      ret_state = FOLLOWER_LOGICAL_SPLIT_SUCCESS;
      break;
    }
    default: {
      // do nothing
      break;
    }
  }
  return ret_state;
}

inline bool is_valid_split_state(const ObPartitionSplitStateEnum state)
{
  return state > UNKNOWN_SPLIT_STATE && state < MAX_SPLIT_STATE;
}

inline bool is_valid_split_action(const ObPartitionSplitAction action)
{
  return action > UNKNOWN_SPLIT_ACTION && action < MAX_SPLIT_ACTION;
}

inline bool in_source_splitting(const ObPartitionSplitStateEnum state)
{
  return (SPLIT_START <= state && TABLE_REFERENCE_SUCCESS >= state);
}

inline bool is_source_split(const ObPartitionSplitStateEnum state)
{
  return (SPLIT_START <= state && TABLE_REFERENCE_SUCCESS >= state);
}

inline bool is_dest_split(const ObPartitionSplitStateEnum state)
{
  return (LEADER_WAIT_SPLIT <= state && FOLLOWER_LOGICAL_SPLIT_SUCCESS >= state);
}

inline bool in_dest_splitting(const ObPartitionSplitStateEnum state)
{
  return (LEADER_WAIT_SPLIT <= state && SPLIT_DEST_LOGGING >= state);
}

inline bool in_splitting(const ObPartitionSplitStateEnum state)
{
  return in_source_splitting(state) || in_dest_splitting(state);
}

inline bool is_split_source_log_success(const ObPartitionSplitStateEnum state)
{
  return state >= LEADER_SPLIT_SOURCE_LOG;
}

inline bool is_logical_split_dest_finish(const ObPartitionSplitStateEnum state)
{
  return state == FOLLOWER_LOGICAL_SPLIT_SUCCESS;
}

inline bool is_split_dest_log_success(const ObPartitionSplitStateEnum state)
{
  return LEADER_LOGICAL_SPLIT_SUCCESS == state || FOLLOWER_LOGICAL_SPLIT_SUCCESS == state || LEADER_INIT == state ||
         FOLLOWER_INIT == state;
}

inline bool is_physical_split_finished(const ObPartitionSplitStateEnum state)
{
  return LEADER_INIT == state || FOLLOWER_INIT == state;
}

class ObPartitionSplitSourceLog : public ObNonTransLog {
  OB_UNIS_VERSION(1);

public:
  ObPartitionSplitSourceLog() : schema_version_(0), spp_(), slave_read_ts_(0)
  {}
  ~ObPartitionSplitSourceLog()
  {}
  int init(const int64_t schema_version, const share::ObSplitPartitionPair& spp, const int64_t slave_read_ts);
  bool is_valid() const;
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  int64_t get_slave_read_ts() const
  {
    return slave_read_ts_;
  }
  const share::ObSplitPartitionPair& get_spp() const
  {
    return spp_;
  }
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(schema_version), K_(slave_read_ts), K_(spp));

private:
  int64_t schema_version_;
  share::ObSplitPartitionPair spp_;
  // record the current slave read timestamp on the split leader,
  // so as to checkpoint 1pc transactions during the replay process
  int64_t slave_read_ts_;
};

class ObPartitionSplitDestLog : public ObNonTransLog {
  OB_UNIS_VERSION(1);

public:
  ObPartitionSplitDestLog() : split_version_(0), schema_version_(0), source_log_id_(0), source_log_ts_(0), spp_()
  {}
  ~ObPartitionSplitDestLog()
  {}
  int init(const int64_t split_version, const int64_t schema_version, const int64_t source_log_id,
      const int64_t source_log_ts, const share::ObSplitPartitionPair& spp);
  bool is_valid() const;
  int64_t get_split_version() const
  {
    return split_version_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  int64_t get_source_log_id() const
  {
    return source_log_id_;
  }
  int64_t get_source_log_ts() const
  {
    return source_log_ts_;
  }
  const share::ObSplitPartitionPair& get_spp() const
  {
    return spp_;
  }
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  TO_STRING_KV(K_(split_version), K_(schema_version), K_(source_log_id), K_(source_log_ts), K_(spp));

private:
  int64_t split_version_;
  int64_t schema_version_;
  int64_t source_log_id_;
  int64_t source_log_ts_;
  share::ObSplitPartitionPair spp_;
};

class ObPartitionSplitState {
  OB_UNIS_VERSION(1);

public:
  ObPartitionSplitState() : state_(UNKNOWN_SPLIT_STATE), save_state_(UNKNOWN_SPLIT_STATE), pkey_()
  {}
  ~ObPartitionSplitState()
  {}
  int init(const common::ObPartitionKey& pkey);

public:
  int set_partition_key(const common::ObPartitionKey& pkey);
  int switch_state(const ObPartitionSplitAction& action);
  int restore_state();
  int set_state(const ObPartitionSplitStateEnum state);
  ObPartitionSplitStateEnum get_state() const
  {
    return ATOMIC_LOAD(&state_);
  }
  ObPartitionSplitStateEnum get_persistent_state() const
  {
    return to_persistent_state(state_);
  }
  TO_STRING_KV(K_(pkey), "state", to_state_str(state_), "save_state", to_state_str(save_state_));

private:
  ObPartitionSplitStateEnum state_;
  // do not need serialize
  ObPartitionSplitStateEnum save_state_;
  common::ObPartitionKey pkey_;
};

class ObSplitLogCb : public clog::ObISubmitLogCb {
public:
  ObSplitLogCb() : partition_service_(NULL), log_type_(OB_LOG_UNKNOWN)
  {}
  virtual ~ObSplitLogCb()
  {}
  int init(ObPartitionService* ps, const ObStorageLogType log_type);
  int on_success(const common::ObPartitionKey& pkey, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed);
  int on_finished(const common::ObPartitionKey& pkey, const uint64_t log_id);
  TO_STRING_KV(KP(this), KP_(partition_service), K_(log_type));

private:
  ObPartitionService* partition_service_;
  ObStorageLogType log_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSplitLogCb);
};

class ObSplitLogCbFactory {
public:
  static ObSplitLogCb* alloc();
  static void release(ObSplitLogCb* cb);
};

class ObPartitionSplitInfo {
  OB_UNIS_VERSION(1);

public:
  ObPartitionSplitInfo()
  {
    reset();
  }
  ~ObPartitionSplitInfo()
  {}
  int init(const int64_t schema_version, const share::ObSplitPartitionPair& spp, const int64_t split_type);
  void reset();
  int set(const int64_t schema_version, const share::ObSplitPartitionPair& spp, const int64_t split_type);
  int assign(const ObPartitionSplitInfo& spi);
  int64_t get_split_type() const
  {
    return split_type_;
  }
  bool is_valid() const;

public:
  void set_split_version(const int64_t split_version)
  {
    split_version_ = split_version;
  }
  void set_source_log_id(const int64_t source_log_id)
  {
    source_log_id_ = source_log_id;
  }
  void set_source_log_ts(const int64_t source_log_ts)
  {
    source_log_ts_ = source_log_ts;
  }
  int64_t get_split_version() const
  {
    return split_version_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  int64_t get_source_log_id() const
  {
    return source_log_id_;
  }
  int64_t get_source_log_ts() const
  {
    return source_log_ts_;
  }
  int64_t get_receive_split_ts() const
  {
    return receive_split_ts_;
  }
  const common::ObPartitionKey& get_src_partition() const
  {
    return partition_pair_.get_source_pkey();
  }
  const common::ObIArray<common::ObPartitionKey>& get_dest_partitions() const
  {
    return partition_pair_.get_dest_array();
  }
  const share::ObSplitPartitionPair& get_spp() const
  {
    return partition_pair_;
  }
  TO_STRING_KV(K_(split_version), K_(schema_version), K_(source_log_id), K_(source_log_ts), K_(partition_pair),
      K_(split_type), K_(receive_split_ts));

public:
  static const int64_t UNKNWON_SPLIT_TYPE = -1;
  static const int64_t SPLIT_SOURCE_PARTITION = 0;
  static const int64_t SPLIT_DEST_PARTITION = 1;
  static bool is_valid_split_type(const int64_t split_type)
  {
    return SPLIT_SOURCE_PARTITION == split_type || SPLIT_DEST_PARTITION == split_type;
  }

private:
  int64_t schema_version_;
  share::ObSplitPartitionPair partition_pair_;
  int64_t split_type_;
  int64_t split_version_;
  int64_t source_log_id_;
  int64_t source_log_ts_;
  int64_t receive_split_ts_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionSplitInfo);
};

template <typename Type>
class ObSplitProgressTemplate {
public:
  ObSplitProgressTemplate() : obj_(), progress_(share::UNKNOWN_SPLIT_PROGRESS)
  {}
  ~ObSplitProgressTemplate()
  {}

public:
  Type obj_;
  int progress_;
  TO_STRING_KV(K_(obj), K_(progress));
};

typedef ObSplitProgressTemplate<common::ObAddr> ObReplicaSplitProgress;

template <typename Type>
class ObSplitProgressArray {
public:
  ObSplitProgressArray() : is_inited_(false)
  {}
  ~ObSplitProgressArray()
  {}

public:
  int init(const common::ObIArray<Type>& obj_array)
  {
    int ret = common::OB_SUCCESS;
    if (is_inited_) {
      ret = common::OB_INIT_TWICE;
      STORAGE_LOG(WARN, "init twice", K(ret), K(obj_array));
    } else if (0 >= obj_array.count()) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(obj_array));
    } else {
      for (int64_t i = 0; common::OB_SUCCESS == ret && i < obj_array.count(); i++) {
        ObSplitProgressTemplate<Type> progress;
        progress.obj_ = obj_array.at(i);
        progress.progress_ = share::IN_SPLITTING;
        if (OB_FAIL(progress_array_.push_back(progress))) {
          STORAGE_LOG(WARN, "push back split progress failed", K(ret), K(obj_array));
        }
      }
      if (common::OB_SUCCESS == ret) {
        is_inited_ = true;
      }
    }
    return ret;
  }
  bool is_inited() const
  {
    return is_inited_;
  }
  void reset()
  {
    progress_array_.reset();
    is_inited_ = false;
  }
  int set_progress(const Type& obj, const int progress)
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited_) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "not init", K(ret), K(obj), K(progress));
    } else {
      int64_t i = 0;
      for (i = 0; i < progress_array_.count(); i++) {
        ObSplitProgressTemplate<Type>& split_progress = progress_array_.at(i);
        if (split_progress.obj_ == obj) {
          split_progress.progress_ = progress;
          break;
        }
      }
      if (i == progress_array_.count()) {
        ret = common::OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }

  int get_progress(const Type& obj, int& progress) const
  {
    int ret = common::OB_SUCCESS;
    int tmp_progress = share::UNKNOWN_SPLIT_PROGRESS;
    if (!is_inited_) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "not init", K(ret));
    } else if (0 >= progress_array_.count()) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected progress count", K(ret), K_(progress_array));
    } else {
      for (int i = 0; i < progress_array_.count(); i++) {
        if (progress_array_.at(i).obj_ == obj) {
          tmp_progress = progress_array_.at(i).progress_;
          break;
        }
      }
    }
    if (common::OB_SUCCESS == ret) {
      progress = tmp_progress;
    }
    return ret;
  }

  int get_min_progress(int& progress) const
  {
    int ret = common::OB_SUCCESS;
    int tmp_progress = share::UNKNOWN_SPLIT_PROGRESS;
    if (!is_inited_) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "not init", K(ret));
    } else if (0 >= progress_array_.count()) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected progress count", K(ret), K_(progress_array));
    } else {
      tmp_progress = progress_array_.at(0).progress_;
      for (int i = 1; i < progress_array_.count(); i++) {
        if (share::NEED_NOT_SPLIT == progress_array_.at(i).progress_) {
          // ignore NEED_NOT_SPLIT
        } else if (progress_array_.at(i).progress_ < tmp_progress) {
          tmp_progress = progress_array_.at(i).progress_;
        } else {
        }
      }
    }
    if (common::OB_SUCCESS == ret) {
      progress = tmp_progress;
    }
    return ret;
  }

  int get_not_finished(common::ObIArray<Type>& obj_array) const
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited_) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "not init", K(ret));
    } else {
      obj_array.reset();
      for (int i = 0; common::OB_SUCCESS == ret && i < progress_array_.count(); i++) {
        if (share::PHYSICAL_SPLIT_FINISH > progress_array_.at(i).progress_) {
          if (OB_FAIL(obj_array.push_back(progress_array_.at(i).obj_))) {
            STORAGE_LOG(WARN, "push back obj failed", K(ret));
          }
        }
      }
    }
    return ret;
  }

  const common::ObIArray<ObSplitProgressTemplate<Type> >& get_progress_array() const
  {
    return progress_array_;
  }

public:
  TO_STRING_KV(K_(is_inited), K_(progress_array));

private:
  bool is_inited_;
  common::ObSArray<ObSplitProgressTemplate<Type> > progress_array_;
};

typedef ObSplitProgressArray<common::ObAddr> ObReplicaSplitProgressArray;
typedef ObSplitProgressArray<common::ObPartitionKey> ObPartitionSplitProgressArray;

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_SPLIT
