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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_INFO_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_INFO_H_

#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/ob_replica_define.h"
#include "common/ob_role.h"
#include "common/row/ob_row_checksum.h"
#include "common/ob_zone.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace share {
class ObIReplicaFilter;

enum ObReplicaStatus {
  // replicas can serve normally
  REPLICA_STATUS_NORMAL = 0,
  // replicas cannot serve
  REPLICA_STATUS_OFFLINE,
  // flag replica, insert into partition table before create associated storage,
  // to indicate that we will create replica on the server later.
  REPLICA_STATUS_FLAG,
  REPLICA_STATUS_UNMERGED,
  // invalid value
  REPLICA_STATUS_MAX,
};

// 0 represents normal replicas,
// 1 represents logical restore replicas,
// 2-7 represents physical restore replicas
enum ObReplicaRestoreStatus {
  REPLICA_NOT_RESTORE = 0,              // restore finished
  REPLICA_LOGICAL_RESTORE_DATA = 1,     // waiting for logical restore of baseline data
  REPLICA_RESTORE_DATA = 2,             // waiting for or in physical restore of baseline data
  REPLICA_RESTORE_CUT_DATA = 3,         // cut sstable
  REPLICA_RESTORE_ARCHIVE_DATA = 4,     // waiting for restore archive data
  REPLICA_RESTORE_LOG = 5,              // in physical incremental restore
  REPLICA_RESTORE_DUMP_MEMTABLE = 6,    // waiting for local server memtable minor merge
  REPLICA_RESTORE_WAIT_ALL_DUMPED = 7,  // waiting for all replicas in cluster to finish dumping
  REPLICA_RESTORE_MEMBER_LIST = 8,      // waiting for member list persistence to majority
  REPLICA_RESTORE_MAX,
  REPLICA_RESTORE_STANDBY = 100,  // standby replica restore
  REPLICA_RESTORE_STANDBY_MAX,
};

const char* ob_replica_status_str(const ObReplicaStatus status);
int get_replica_status(const char* str, ObReplicaStatus& status);
struct ObPartitionReplica {
  OB_UNIS_VERSION(1);

public:
  struct Member {
    OB_UNIS_VERSION(1);

  public:
    Member() : timestamp_(0)
    {}
    Member(const common::ObAddr& server, const int64_t timestamp) : server_(server), timestamp_(timestamp)
    {}
    TO_STRING_KV(K_(server), K_(timestamp));

    operator const common::ObAddr&() const
    {
      return server_;
    }
    bool operator==(const Member& o) const
    {
      return server_ == o.server_ && timestamp_ == o.timestamp_;
    }
    bool operator!=(const Member& o) const
    {
      return !(*this == o);
    }

    common::ObAddr server_;
    int64_t timestamp_;
  };

  struct FailMsg {
    OB_UNIS_VERSION(1);

  public:
    FailMsg() : task_type_(-1), start_pos_(0), count_(0)
    {}
    virtual ~FailMsg()
    {
      reset();
    }
    TO_STRING_KV(K_(dest_server), K_(task_type), "last_fail_time", get_last_fail_time(), K_(count), K_(start_pos));
    static const int64_t MAX_FAILED_TIMES_COUNT = 16;
    void reset();
    void assign(const FailMsg& other);
    void operator=(const FailMsg& other)
    {
      assign(other);
    };
    int64_t get_last_fail_time() const;
    int add_failed_timestamp(int64_t timestamp);
    int remove_useless_timestamp(int64_t current_time, int64_t timeout);
    int64_t get_failed_timestamp(int64_t index) const;
    int64_t get_failed_count() const
    {
      return count_;
    }
    common::ObAddr dest_server_;
    int64_t task_type_;
    // time information for failure, no more than 16
    int64_t last_fail_timestamp_[MAX_FAILED_TIMES_COUNT];
    // array offset for the first failed information
    int64_t start_pos_;
    // failed count
    int64_t count_;
  };

public:
  static const int64_t DEFAULT_REPLICA_COUNT = 7;
  typedef common::ObSEArray<Member, DEFAULT_REPLICA_COUNT> MemberList;
  static const int64_t DEFAULT_FAIL_LIST_COUNT = 100;
  typedef common::ObSEArray<FailMsg, DEFAULT_FAIL_LIST_COUNT> FailList;
  static const int64_t DEFAULT_ALLOCATOR_SIZE = 1200;

  ObPartitionReplica();
  ~ObPartitionReplica();
  void reset();
  void init_basic_variables();
  void reuse();
  inline bool is_valid() const;
  inline bool is_flag_replica() const;
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline int64_t get_partition_id() const
  {
    return partition_id_;
  }
  inline int64_t get_memstore_percent() const
  {
    return property_.get_memstore_percent();
  }
  inline int set_memstore_percent(const int64_t mp)
  {
    return property_.set_memstore_percent(mp);
  }
  bool is_in_service() const
  {
    return replica_status_ == share::REPLICA_STATUS_NORMAL;
  }
  bool is_paxos_candidate() const
  {
    return (common::REPLICA_TYPE_FULL == replica_type_ || common::REPLICA_TYPE_LOGONLY == replica_type_);
  }
  bool is_leader_like() const
  {
    return common::is_leader_like(role_);
  }
  bool is_leader_by_election() const
  {
    return common::is_leader_by_election(role_);
  }
  bool is_strong_leader() const
  {
    return common::is_strong_leader(role_);
  }
  bool is_restore_leader() const
  {
    return common::is_restore_leader(role_);
  }
  bool is_standby_leader() const
  {
    return common::is_standby_leader(role_);
  }
  bool is_follower() const
  {
    return common::is_follower(role_);
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }
  bool is_in_blacklist(int64_t task_type, common::ObAddr dest_server) const;
  common::ObPartitionKey partition_key() const;
  void set_remove_flag()
  {
    is_remove_ = true;
  }
  static int member_list2text(const MemberList& member_list, char* text, const int64_t length);
  static int text2member_list(const char* text, MemberList& member_list);

  static int fail_list2text(const FailList& fail_list, char* text, const int64_t length);
  static int text2fail_list(const char* text, FailList& fail_list);
  int process_faillist(FailList& fail_list);
  int set_faillist(const FailList& fail_list);
  void reset_fail_list()
  {
    allocator_.reset();
    fail_list_.reset();
  }
  int deep_copy_faillist(const common::ObString& fail_list);

  int64_t to_string(char* buf, const int64_t buf_len) const;

  int assign(const ObPartitionReplica& other);
  explicit ObPartitionReplica(const ObPartitionReplica& other)
  {
    assign(other);
  }
  void operator=(const ObPartitionReplica& other)
  {
    assign(other);
  }
  bool simple_equal_with(const ObPartitionReplica& other);
  bool need_schedule_restore_task() const
  {
    return REPLICA_LOGICAL_RESTORE_DATA <= is_restore_ && REPLICA_RESTORE_WAIT_ALL_DUMPED > is_restore_;
  }
  bool in_physical_restore_status() const
  {
    return REPLICA_RESTORE_DATA <= is_restore_ && REPLICA_RESTORE_MEMBER_LIST >= is_restore_;
  }
  bool in_standby_restore() const
  {
    return REPLICA_RESTORE_STANDBY == is_restore_;
  }
  bool need_force_full_report() const
  {
    return common::is_sys_table(table_id_) || in_physical_restore_status() || in_standby_restore();
  }

public:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;

  common::ObZone zone_;
  common::ObAddr server_;
  int64_t sql_port_;
  uint64_t unit_id_;

  common::ObRole role_;
  MemberList member_list_;
  int64_t row_count_;
  int64_t data_size_;
  int64_t data_version_;
  int64_t data_checksum_;
  common::ObRowChecksumValue row_checksum_;
  int64_t modify_time_us_;  // store utc time
  int64_t create_time_us_;  // store utc time
  int64_t member_time_us_;  // timestamp in ObMember, convenient for coding

  // is original leader before daily merge
  bool is_original_leader_;

  // discard filed (replace by to_leader_time_), left it here for serialize compatible
  bool __discard__is_previous_leader_;

  bool in_member_list_;
  bool rebuild_;
  bool is_remove_;  // memory status,no need to write into non-volatile storage
  ObReplicaStatus replica_status_;

  // latest change to leader time, to indicate the previous leader position.
  // stored in partition table is_previous_leader column.
  int64_t to_leader_time_;
  common::ObReplicaType replica_type_;
  int64_t required_size_;
  // status recorded in pt, it is different from replica_status_,
  // replica_status_ is calculated based on the memory info
  ObReplicaStatus status_;
  int64_t is_restore_;
  int64_t partition_checksum_;
  int64_t quorum_;  // quorum value in clog,designated by RS,report to meta table from observer
  ObReplicaStatus additional_replica_status_;
  // a memory status added for split
  // migration is not allowd when progressive merge is not finished,
  // this additional_replica_status_ is not recorded in replica_status_,
  // it is only used for migration, when additional_replica_status is partiton_status_unmerged,
  // replica_status_ is partition_status_normal
  common::ObArenaAllocator allocator_;  // used by faillist, no greater than 1k for faillist
  common::ObString fail_list_;          // need a \0 end
  int64_t recovery_timestamp_;
  common::ObReplicaProperty property_;  // memstore_percent
  int64_t data_file_id_;                // file id which is use to hold the sstable macro block
};

class ObPartitionInfo {
  OB_UNIS_VERSION(1);

public:
  typedef common::ObIArray<ObPartitionReplica> ReplicaArray;

  ObPartitionInfo();
  ObPartitionInfo(common::ObIAllocator& allocator);
  virtual ~ObPartitionInfo();

  uint64_t get_tenant_id() const
  {
    return common::extract_tenant_id(table_id_);
  }

  uint64_t get_table_id() const
  {
    return table_id_;
  }
  void set_table_id(const uint64_t table_id)
  {
    table_id_ = table_id;
  }

  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  void set_partition_id(const int64_t partition_id)
  {
    partition_id_ = partition_id;
  }

  common::ObIAllocator* get_allocator() const
  {
    return allocator_;
  }
  void set_allocator(common::ObIAllocator* allocator)
  {
    allocator_ = allocator;
  }

  void reset();
  void reuse()
  {
    replicas_.reuse();
  }
  inline bool is_valid() const;

  const ReplicaArray& get_replicas_v2() const
  {
    return replicas_;
  }
  ReplicaArray& get_replicas_v2()
  {
    return replicas_;
  }
  int64_t replica_count() const
  {
    return replicas_.count();
  }
  bool is_leader_like(int64_t index) const;
  // return OB_ENTRY_NOT_EXIST for not found (set %replica to NULL too).
  int find(const common::ObAddr& server, const ObPartitionReplica*& replica) const;
  // int find_leader(const ObPartitionReplica *&replica) const;
  int find_leader_v2(const ObPartitionReplica*& replica) const;
  int find_latest_leader(const ObPartitionReplica*& replica) const;
  // used for load balance, need to return paxos leader or standby leader
  int find_leader_by_election(const ObPartitionReplica*& replica) const;

  // insert or replace replica
  virtual int add_replica(const ObPartitionReplica& replica);
  virtual int add_replica_ignore_checksum_error(const ObPartitionReplica& replica, bool& is_checksum_error);
  virtual int remove(const common::ObAddr& server);

  virtual int set_unit_id(const common::ObAddr& server, const uint64_t unit_id);

  virtual int filter(const ObIReplicaFilter& filter);

  int64_t to_string(char* buf, const int64_t buf_len) const;

  int update_replica_status();
  void reset_row_checksum();
  int assign(const ObPartitionInfo& other);
  int get_partition_cnt(int64_t& partition_cnt);
  bool simple_equal_with(const ObPartitionInfo& other);
  bool in_physical_restore() const;
  bool in_standby_restore() const;

  static int alloc_new_partition_info(common::ObIAllocator& alloctor, ObPartitionInfo*& partition);

private:
  // found:
  //   return OB_SUCCESS, set %idx to array index of %replicas_
  //
  // not found:
  //   return OB_ENTRY_NOT_EXIST, set %idx to OB_INVALID_INDEX
  int find_idx(const common::ObAddr& server, int64_t& idx) const;
  int find_idx(const ObPartitionReplica& replica, int64_t& idx) const;

  int verify_checksum(const ObPartitionReplica& replica) const;

private:
  uint64_t table_id_;
  int64_t partition_id_;
  common::ObIAllocator* allocator_;
  common::ObSEArray<ObPartitionReplica, ObPartitionReplica::DEFAULT_REPLICA_COUNT> replicas_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionInfo);
};

inline bool ObPartitionReplica::is_valid() const
{
  return (common::OB_INVALID_ID != common::extract_tenant_id(table_id_) && common::OB_INVALID_ID != table_id_ &&
          partition_id_ >= 0 && partition_cnt_ >= 0 && !zone_.is_empty() && server_.is_valid());
}

inline bool ObPartitionReplica::is_flag_replica() const
{
  return -1 == data_version_ && !rebuild_;
}

inline bool ObPartitionInfo::is_valid() const
{
  return common::OB_INVALID_ID != table_id_ && partition_id_ >= 0;
}

}  // end namespace share
}  // end namespace oceanbase
#endif  // OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_INFO_H_
