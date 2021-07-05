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

#ifndef OCEANBASE_CLOG_OB_LOG_TYPE_
#define OCEANBASE_CLOG_OB_LOG_TYPE_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_member_list.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObConfirmedInfo {
  OB_UNIS_VERSION(1);

public:
  ObConfirmedInfo() : data_checksum_(0), epoch_id_(common::OB_INVALID_TIMESTAMP), accum_checksum_(0)
  {}
  ~ObConfirmedInfo()
  {}

public:
  int init(const int64_t data_checksum, const int64_t epoch_id, const int64_t accum_checksum);
  int64_t get_data_checksum() const
  {
    return data_checksum_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  int64_t get_accum_checksum() const
  {
    return accum_checksum_;
  }
  void reset()
  {
    data_checksum_ = 0;
    epoch_id_ = common::OB_INVALID_TIMESTAMP;
    accum_checksum_ = 0;
  }
  void deep_copy(const ObConfirmedInfo& confirmed_info)
  {
    data_checksum_ = confirmed_info.data_checksum_;
    epoch_id_ = confirmed_info.epoch_id_;
    accum_checksum_ = confirmed_info.accum_checksum_;
  }
  friend bool operator==(const ObConfirmedInfo& lhs, const ObConfirmedInfo& rhs);
  TO_STRING_KV(K_(data_checksum), K_(epoch_id), K_(accum_checksum));

private:
  int64_t data_checksum_;
  int64_t epoch_id_;
  int64_t accum_checksum_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfirmedInfo);
};

inline bool operator==(const ObConfirmedInfo& lhs, const ObConfirmedInfo& rhs)
{
  return (lhs.data_checksum_ == rhs.data_checksum_) && (lhs.epoch_id_ == rhs.epoch_id_) &&
         (lhs.accum_checksum_ == rhs.accum_checksum_);
}

class ObMembershipLog {
  OB_UNIS_VERSION(1);

public:
  ObMembershipLog();
  ~ObMembershipLog();

public:
  int64_t get_replica_num() const;
  int64_t get_timestamp() const;
  const common::ObMemberList& get_member_list() const;
  const common::ObMemberList& get_prev_member_list() const;
  int64_t get_type() const;
  int64_t get_cluster_id() const;
  void set_replica_num(const int64_t replica_num);
  void set_timestamp(const int64_t ms_timestamp);
  void set_member_list(const common::ObMemberList& member_list);
  void set_prev_member_list(const common::ObMemberList& prev_member_list);
  void set_type(const int64_t type);
  void set_cluster_id(const int64_t cluster_id);

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  static const int16_t MS_LOG_VERSION = 1;
  int16_t version_;
  int64_t replica_num_;
  int64_t timestamp_;
  common::ObMemberList member_list_;
  common::ObMemberList prev_member_list_;
  int64_t type_;
  int64_t cluster_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMembershipLog);
};

class ObRenewMembershipLog {
  OB_UNIS_VERSION(1);

public:
  ObRenewMembershipLog();
  ~ObRenewMembershipLog();

public:
  int64_t get_replica_num() const;
  int64_t get_timestamp() const;
  const common::ObMemberList& get_member_list() const;
  const common::ObMemberList& get_prev_member_list() const;
  int64_t get_type() const;
  int64_t get_cluster_id() const;
  common::ObProposalID get_ms_proposal_id() const;
  void set_replica_num(const int64_t replica_num);
  void set_timestamp(const int64_t ms_timestamp);
  void set_member_list(const common::ObMemberList& member_list);
  void set_prev_member_list(const common::ObMemberList& prev_member_list);
  void set_type(const int64_t type);
  void set_cluster_id(const int64_t cluster_id);
  void set_ms_proposal_id(const common::ObProposalID& ms_proposal_id);
  void set_barrier_log_id(const uint64_t barrier_log_id);
  uint64_t get_barrier_log_id() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int64_t replica_num_;
  int64_t timestamp_;
  common::ObMemberList member_list_;
  common::ObMemberList prev_member_list_;
  int64_t type_;
  int64_t cluster_id_;
  common::ObProposalID ms_proposal_id_;
  uint64_t barrier_log_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRenewMembershipLog);
};

class ObNopLog {
  OB_UNIS_VERSION(1);

public:
  ObNopLog() : version_(NOP_LOG_VERSION)
  {}
  ~ObNopLog()
  {}

private:
  static const int16_t NOP_LOG_VERSION = 1;
  int16_t version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObNopLog);
};

class ObPreparedLog {
  OB_UNIS_VERSION(1);

public:
  ObPreparedLog() : version_(PREPARED_LOG_VERSION)
  {}
  ~ObPreparedLog()
  {}

private:
  static const int16_t PREPARED_LOG_VERSION = 1;
  int16_t version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPreparedLog);
};

class ObLogArchiveInnerLog {
  OB_UNIS_VERSION(1);

public:
  ObLogArchiveInnerLog()
      : version_(ARCHIVE_CHECKPOINT_LOG_VERSION),
        checkpoint_ts_(common::OB_INVALID_TIMESTAMP),
        round_start_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObLogArchiveInnerLog()
  {}
  void set_checkpoint_ts(const int64_t ts)
  {
    checkpoint_ts_ = ts;
  }
  void set_round_start_ts(const int64_t start_ts)
  {
    round_start_ts_ = start_ts;
  }
  void set_round_snapshot_version(const int64_t snapshot_version)
  {
    round_snapshot_version_ = snapshot_version;
  }
  int64_t get_checkpoint_ts() const
  {
    return checkpoint_ts_;
  }
  int64_t get_round_start_ts() const
  {
    return round_start_ts_;
  }
  int64_t get_round_snapshot_version() const
  {
    return round_snapshot_version_;
  }
  TO_STRING_KV(K_(version), K_(checkpoint_ts), K_(round_start_ts), K_(round_snapshot_version));

private:
  static const int16_t ARCHIVE_CHECKPOINT_LOG_VERSION = 1;
  int16_t version_;
  int64_t checkpoint_ts_;
  int64_t round_start_ts_;          // only kickoff log use this
  int64_t round_snapshot_version_;  // only kickoff log use this.use for recovery point by qianchen
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveInnerLog);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_TYPE_H_
