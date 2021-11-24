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

#ifndef OCEANBASE_CLOG_OB_LOG_ENTRY_HEADER_
#define OCEANBASE_CLOG_OB_LOG_ENTRY_HEADER_

#include "common/ob_partition_key.h"
#include "common/ob_range.h"
#include "ob_log_define.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
namespace clog {
class ObLogEntryHeader {
public:
  ObLogEntryHeader();
  ~ObLogEntryHeader();

  int generate_header(const ObLogType log_type, const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const char* buf, const int64_t data_len, const int64_t generation_timestamp, const int64_t epoch_id,
      const common::ObProposalID proposal_id, const int64_t submit_timestamp, const common::ObVersion freeze_version,
      const bool is_trans_log);
  // copy to this
  int shallow_copy(const ObLogEntryHeader& header);
  void reset();
  bool operator==(const ObLogEntryHeader& header) const;
  bool check_magic_num() const
  {
    return ENTRY_MAGIC == magic_;
  }
  bool check_data_checksum(const char* buf, const int64_t data_len) const;
  bool check_header_checksum() const;
  bool check_integrity(const char* buf, const int64_t data_len) const;
  // get member variable
  int16_t get_magic_num() const
  {
    return magic_;
  }
  int16_t get_version() const
  {
    return version_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  int64_t get_data_len() const
  {
    return data_len_;
  }
  int64_t get_total_len() const
  {
    return (data_len_ + get_serialize_size());
  }
  int64_t get_generation_timestamp() const
  {
    return generation_timestamp_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  common::ObProposalID get_proposal_id() const
  {
    return proposal_id_;
  }
  int64_t get_data_checksum() const
  {
    return data_checksum_;
  }
  int64_t get_header_checksum() const
  {
    return header_checksum_;
  }
  common::ObVersion get_freeze_version() const
  {
    return freeze_version_;
  }
  // set member variable
  void set_epoch_id(const int64_t ts)
  {
    epoch_id_ = ts;
  }
  void set_proposal_id(const common::ObProposalID ts)
  {
    proposal_id_ = ts;
  }
  void set_partition_key(const common::ObPartitionKey& pkey)
  {
    partition_key_ = pkey;
  }
  void set_data_len(const int64_t new_len)
  {
    data_len_ = new_len;
  }
  void set_log_type(const ObLogType new_log_type)
  {
    log_type_ = new_log_type;
  }
  // update checksum
  void update_header_checksum()
  {
    header_checksum_ = calc_header_checksum();
  }
  void update_data_checksum(const char* buf, const int64_t data_len)
  {
    data_checksum_ = calc_data_checksum(buf, data_len);
  }
  void update_freeze_version(const common::ObVersion& freeze_version)
  {
    if (freeze_version > freeze_version_) {
      freeze_version_ = freeze_version;
    }
  }
  int update_nop_or_truncate_submit_timestamp(const int64_t submit_timestamp);
  int update_proposal_id(const common::ObProposalID& new_proposal_id);
  TO_STRING_KV(N_MAGIC, magic_, N_VERSION, version_, N_TYPE, get_log_type(), N_PARTITION_KEY, partition_key_, N_LOG_ID,
      log_id_, N_DATA_LEN, data_len_, N_GENERATION_TIMESTAMP, generation_timestamp_, N_EPOCH_ID, epoch_id_,
      N_PROPOSAL_ID, proposal_id_, N_SUBMIT_TIMESTAMP, get_submit_timestamp(), "is_batch_committed",
      is_batch_committed(), "is_trans_log", is_trans_log(), N_DATA_CHECKSUM, data_checksum_, N_ACTIVE_FREEZE_VERSION,
      freeze_version_, N_HEADER_CHECKSUM, header_checksum_);
  NEED_SERIALIZE_AND_DESERIALIZE;
  int64_t get_submit_ts_serialize_pos() const;

  // set trans batch commit flag
  // trans batch commit is the highest bit of submit_timestamp
  void set_trans_batch_commit_flag()
  {
    submit_timestamp_ = (submit_timestamp_ | TRANS_BATCH_COMMIT_FLAG);
  }
  void clear_trans_batch_commit_flag()
  {
    if (common::OB_INVALID_TIMESTAMP != submit_timestamp_) {
      submit_timestamp_ = (submit_timestamp_ & TRANS_BATCH_COMMIT_MASK);
    }
  }
  int64_t get_submit_timestamp() const
  {
    int64_t log_ts = submit_timestamp_;
    if (common::OB_INVALID_TIMESTAMP != log_ts) {
      log_ts = log_ts & TRANS_BATCH_COMMIT_MASK;
    }
    return log_ts;
  }
  void set_submit_timestamp(const int64_t ts)
  {
    submit_timestamp_ = ts;
  }
  bool is_batch_committed() const
  {
    bool bool_ret = false;
    if (common::OB_INVALID_TIMESTAMP != submit_timestamp_) {
      bool_ret = submit_timestamp_ & TRANS_BATCH_COMMIT_FLAG;
    }
    return bool_ret;
  }

  void set_is_trans_log_flag()
  {
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
      log_type_ = (log_type_ | IS_TRANS_LOG_FLAG);
    }
  }
  bool is_trans_log() const
  {
    return log_type_ & IS_TRANS_LOG_FLAG;
  }
  ObLogType get_log_type() const
  {
    const int32_t log_type = (log_type_ & IS_TRANS_LOG_MASK);
    return static_cast<ObLogType>(log_type);
  }

  // Serialize submit_timestamp at specified offset
  int serialize_submit_timestamp(char* buf, const int64_t buf_len, int64_t& pos);

  static bool check_magic_number(const int16_t magic_number)
  {
    return ENTRY_MAGIC == magic_number;
  }

private:
  int64_t calc_header_checksum() const;
  int64_t calc_data_checksum(const char* buf, const int64_t data_len) const;

private:
  static const int16_t ENTRY_MAGIC = 0x4552;  // RE means record
  static const int16_t OB_LOG_VERSION = 1;
  static const uint64_t TRANS_BATCH_COMMIT_FLAG = (1ULL << 63);
  static const uint64_t TRANS_BATCH_COMMIT_MASK = TRANS_BATCH_COMMIT_FLAG - 1;
  static const uint32_t IS_TRANS_LOG_FLAG = (1 << 31);
  static const uint32_t IS_TRANS_LOG_MASK = IS_TRANS_LOG_FLAG - 1;

private:
  int16_t magic_;
  int16_t version_;
  // The highest position of log_type is used to indicates
  // whether it is the log of transaction commit, which is
  // used to optimize the cost of reading disk when the D/L
  // replica executes submit_replay_task
  //
  // The log recored on the disk also contains this mark
  int32_t log_type_;
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;
  int64_t data_len_;
  // Log generation timestamp, mainly used for debugging
  int64_t generation_timestamp_;
  // The timestamp when the leader was elected, mainly used for filter
  // ghost log
  int64_t epoch_id_;
  // The proposal id used for synchronizing to follower
  common::ObProposalID proposal_id_;

  // The highest bit of submit_timestamp_ is used for indicating
  // whether is batch commit
  //
  // Attention: the batch commit flag is not recorded in clog, because of
  // when a clog is being written, it doesn't know whether the log is batch
  // committed. the checksum of header also not consider batch commit
  //
  // Only used for liboblog and the rpc for fetching log
  int64_t submit_timestamp_;
  int64_t data_checksum_;
  common::ObVersion freeze_version_;

  // The checksum of log header, acquirements:
  // 1. Must locate in the last line of class declaration
  // 2. Must not exist virtual funtion
  // 3. Must keep the variable before this is 64-bit aligned
  int64_t header_checksum_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogEntryHeader);
};

}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_ENTRY_HEADER_H_
