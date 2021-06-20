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

#ifndef OCEANBASE_CLOG_OB_ILOG_MEMSTORE_H_
#define OCEANBASE_CLOG_OB_ILOG_MEMSTORE_H_
#include "lib/hash/ob_linear_hash_map.h"
#include "common/ob_member_list.h"
#include "ob_ilog_file_builder.h"
#include "ob_info_block_handler.h"       // for IndexInfoBlockEntry
#include "ob_log_define.h"               // for ObLogCursorExt
#include "ob_remote_log_query_engine.h"  // for ObPartitionLogInfo

namespace oceanbase {
namespace clog {
// Typeof event that triggers ilog_memstore freeze
enum ObIlogFreezeTriggerType {
  OB_CLOG_SIZE_TRIGGER_TYPE = 0,               // 1. triggered by clog_size
  OB_ILOG_SIZE_TRIGGER_TYPE = 1,               // 2. triggered by ilog_size
  OB_ILOG_NOT_CONTINOUS_TRIGGER_TYPE = 2,      // 3. triggered by ilog not continous
  OB_TIMER_TRIGGER_TYPE = 3,                   // 4. triggered by timer
  OB_MERGE_NEED_SWITCH_FILE_TRIGGER_TYPE = 4,  // 5. mark that whether the freeze needs to switch file
  OB_NO_NEED_FREEZE_TRIGGER_TYPE = 5,          // 6. mark that no need to freeze ilog_memstore
  OB_INVALID_TRIGGER_TYPE = 6,
};

class ObIlogMemstore {
  friend ObIlogFileBuilder;

public:
  ObIlogMemstore();
  ~ObIlogMemstore();

public:
  typedef common::ObLinearHashMap<common::ObPartitionKey, IndexInfoBlockEntry> PartitionMetaInfo;
  typedef common::ObLinearHashMap<common::ObPartitionKey, MemberListInfo> PartitionMemberListInfo;
  typedef common::ObLinearHashMap<ObPartitionLogInfo, ObLogCursorExt> PartitionLogCursorExtInfo;
  int init();
  void destroy();
  // When the function executed fail, treat it as an error
  // Freezing condition:
  //  1) total size of cursor is greater than CURSOR_SIZE_TRIGGER (default as 32MB)
  //  2) total size of clog is greater than CLOG_SIZE_TRIGGER (default as 1GB)
  //  3) reached freeze cycle, and there are some writing in memstore
  //  4) ilog are not continous
  int check_need_freeze(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObIlogFreezeTriggerType& trigger) const;
  int timer_check_need_freeze(bool& need_freeze) const;

  // Internal retry, when the function executed fail, treat it
  // as an error
  int submit_cursor(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext);
  int submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
      const int64_t memberlist_version);
  // Return value:
  //  1) OB_SUCCESS, query success
  //  2) OB_ENTRY_NOT_EXIST, the log to be queried doesn't exist
  //  3) OB_ERR_OUT_OF_UPPER_BOUND, the log to be queried is greater than the max log
  //     of this partition
  int get_cursor(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const;
  // Return value:
  //  1) OB_SUCCESS, query success
  //  2) OB_PARTITION_NOT_EXIST, partition not exist
  int get_max_log_id_and_ts(
      const common::ObPartitionKey& partition_key, uint64_t& ret_max_log_id, int64_t& ret_max_log_ts) const;
  // Return value:
  //  1) OB_SUCCESS, query success
  //  2) OB_PARTITION_NOT_EXIST, partition not exist
  int get_min_log_id_and_ts(
      const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const;
  // Return value:
  //  1) OB_SUCCESS, query success
  //  2) OB_PARTITION_NOT_EXIST, partition not exist
  int get_log_id_range(
      const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, uint64_t& ret_max_log_id) const;

  // The ilog_memstore that calls following function must be frozen
  int check_need_switch_file(bool& need_switch_file) const;

  int get_cursor_size(int64_t& cursor_size) const;
  int insert_partition_meta_info(const common::ObPartitionKey& pkey, const IndexInfoBlockEntry& entry);
  int insert_partition_memberlist_info(const common::ObPartitionKey& pkey, const MemberListInfo& member_list);
  int insert_partition_log_cursor_ext_info(const ObPartitionLogInfo& log_info, const ObLogCursorExt& log_cursor);
  template <class Function>
  int operate_partition_meta_info(Function& fn)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(partition_meta_info_.for_each(fn))) {
      CLOG_LOG(WARN, "operate_partition_meta_info failed", K(ret));
    }
    return ret;
  }
  template <class Function>
  int operate_partition_memberlist_info(Function& fn)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(partition_memberlist_info_.for_each(fn))) {
      CLOG_LOG(WARN, "operate_partition_memberlist_info failed", K(ret));
    }
    return ret;
  }
  template <class Function>
  int operate_partition_log_cursor_ext_info(Function& fn)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(log_cursor_ext_info_.for_each(fn))) {
      CLOG_LOG(WARN, "log_cursor_ext_info_ failed", K(ret));
    }
    return ret;
  }

  int get_index_entry(const common::ObPartitionKey& partition_key, IndexInfoBlockEntry& index_info_block_entry) const;
  TO_STRING_KV(K(create_ts_), K(cursor_size_), K(clog_size_), K(partition_meta_info_.count()));

public:
  const static int64_t CURSOR_SIZE_TRIGGER = 32 * 1024 * 1024;  // 32M
  const static int64_t CLOG_SIZE_TRIGGER = 1024 * 1024 * 1024;  // 1G
  const static int64_t TIME_TRIGGER_US = 10 * 1000L * 1000L;    // 10s
private:
  int check_log_continuous_(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, bool& log_continuous) const;
  int submit_cursor_(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext);
  int submit_memberlist_info_with_retry_(const common::ObPartitionKey& partition_key,
      const common::ObMemberList& memberlist, const int64_t replica_num, const uint64_t membership_log_id,
      const int64_t memberlist_version);
  int submit_memberlist_info_(const common::ObPartitionKey& partition_key, const common::ObMemberList& memberlist,
      const int64_t replica_num, const uint64_t membership_log_id, const int64_t memberlist_version);

  class PartitionMetaInfoFunctor {
  public:
    PartitionMetaInfoFunctor(const uint64_t log_id, const ObLogCursorExt& log_cursor_ext)
        : log_id_(log_id), log_cursor_ext_(log_cursor_ext)
    {}
    ~PartitionMetaInfoFunctor()
    {}

  public:
    bool operator()(const common::ObPartitionKey& partition_key, IndexInfoBlockEntry& index_info_block_entry)
    {
      UNUSED(partition_key);
      bool bool_ret = false;
      if (log_id_ != index_info_block_entry.max_log_id_ + 1) {
        //    || log_cursor_ext_.get_submit_timestamp() < index_info_block_entry.max_log_timestamp_) {
        bool_ret = false;
        CSR_LOG(ERROR, "PartitionMetaInfoFunctor failed", K(log_id_), K(log_cursor_ext_), K(index_info_block_entry));
      } else {
        index_info_block_entry.max_log_id_ = log_id_;
        index_info_block_entry.max_log_timestamp_ = log_cursor_ext_.get_submit_timestamp();
        bool_ret = true;
      }
      return bool_ret;
    }

  private:
    uint64_t log_id_;
    ObLogCursorExt log_cursor_ext_;
  };
  class MemberListInfoFunctor {
  public:
    MemberListInfoFunctor(const common::ObMemberList& memberlist, const int64_t replica_num,
        const uint64_t membership_log_id, const int64_t memberlist_version)
        : memberlist_(memberlist),
          replica_num_(replica_num),
          membership_log_id_(membership_log_id),
          memberlist_version_(memberlist_version)
    {}
    ~MemberListInfoFunctor()
    {}

  public:
    bool operator()(const common::ObPartitionKey& partition_key, MemberListInfo& memberlist_info)
    {
      UNUSED(partition_key);
      if (memberlist_info.get_memberlist_version() <= memberlist_version_) {
        memberlist_info.set_memberlist_info(memberlist_, replica_num_, membership_log_id_, memberlist_version_);
      } else {
        CSR_LOG(ERROR,
            "memberlist_version go back, unexpected",
            K(memberlist_),
            K(replica_num_),
            K(membership_log_id_),
            K(memberlist_version_),
            K(memberlist_info));
      }
      return true;
    }

  private:
    common::ObMemberList memberlist_;
    int64_t replica_num_;
    uint64_t membership_log_id_;
    int64_t memberlist_version_;
  };

private:
  bool is_inited_;
  PartitionMetaInfo partition_meta_info_;
  PartitionMemberListInfo partition_memberlist_info_;
  PartitionLogCursorExtInfo log_cursor_ext_info_;
  // The create timestamp of ilog_memstore, used to trigger
  // freeze on time
  int64_t create_ts_;
  // The total size of log cursor in ilog_memstore
  int64_t cursor_size_;
  // The total size of clog in ilog_memstore
  int64_t clog_size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIlogMemstore);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_ILOG_MEMSTORE_H_
