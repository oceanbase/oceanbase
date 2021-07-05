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

#ifndef OCEANBASE_COMMON_OB_BASE_STORAGE_INFO_H_
#define OCEANBASE_COMMON_OB_BASE_STORAGE_INFO_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_member_list.h"
#include "share/ob_proposal_id.h"

namespace oceanbase {
namespace common {
class ObBaseStorageInfo {
public:
  ObBaseStorageInfo();
  virtual ~ObBaseStorageInfo();
  int init(const int64_t epoch_id, const ObProposalID& proposal_id, const uint64_t last_replay_log_id,
      const int64_t last_submit_timestamp, const int64_t accumulate_checksum, const int64_t replica_num,
      const int64_t membership_timestamp, const uint64_t membership_log_id,
      const common::ObMemberList& curr_member_list, const ObProposalID& ms_proposal_id);
  int init(const int64_t replica_num, const common::ObMemberList& member_list, const int64_t last_submit_timestamp,
      const uint64_t last_replay_log_id, const bool skip_mlist_check);
  bool is_valid() const;
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int deep_copy(const ObBaseStorageInfo& base_storage_info);

  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  void set_epoch_id(const int64_t epoch_id)
  {
    epoch_id_ = epoch_id;
  }
  ObProposalID get_proposal_id() const
  {
    return proposal_id_;
  }
  ObProposalID get_ms_proposal_id() const
  {
    return ms_proposal_id_;
  }
  uint64_t get_last_replay_log_id() const
  {
    return last_replay_log_id_;
  }
  void set_last_replay_log_id(uint64_t last_replay_log_id)
  {
    last_replay_log_id_ = last_replay_log_id;
  }
  int64_t get_submit_timestamp() const
  {
    return last_submit_timestamp_;
  }
  int64_t get_accumulate_checksum() const
  {
    return accumulate_checksum_;
  }
  void set_accumulate_checksum(int64_t accumulate_checksum)
  {
    accumulate_checksum_ = accumulate_checksum;
  }
  void set_submit_timestamp(int64_t submit_timestamp)
  {
    last_submit_timestamp_ = submit_timestamp;
  }
  void force_set_backup_replcia_num()
  {
    replica_num_ = 1;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  int64_t get_membership_timestamp() const
  {
    return membership_timestamp_;
  }
  uint64_t get_membership_log_id() const
  {
    return membership_log_id_;
  }
  const common::ObMemberList& get_curr_member_list() const
  {
    return curr_member_list_;
  }
  int try_update_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& mlist, const common::ObProposalID& ms_proposal_id);
  int standby_force_update_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& mlist, const common::ObProposalID& ms_proposal_id);
  OB_UNIS_VERSION_V(1);

protected:
  static const int16_t STORAGE_INFO_VERSION = 1;

  int16_t version_;
  int64_t epoch_id_;
  ObProposalID proposal_id_;
  uint64_t last_replay_log_id_;
  int64_t last_submit_timestamp_;
  int64_t accumulate_checksum_;
  int64_t replica_num_;
  int64_t membership_timestamp_;
  uint64_t membership_log_id_;
  common::ObMemberList curr_member_list_;
  ObProposalID ms_proposal_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseStorageInfo);
};
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_BASE_STORAGE_INFO_H_
