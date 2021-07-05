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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_SERVICE_RPC_
#define OCEANBASE_STORAGE_OB_PARTITION_SERVICE_RPC_

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "common/ob_partition_key.h"
#include "common/ob_member.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_partition_migrate_old_rpc.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace observer {
class ObGlobalContext;
}

namespace storage {
class ObPartitionService;
class ObMSRowIterator;
class ObPartitionStorage;
}  // namespace storage

namespace obrpc {
class ObCommonRpcProxy;

struct ObMCLogRpcInfo {
  common::ObPartitionKey key_;
  uint64_t log_id_;
  int64_t timestamp_;

  ObMCLogRpcInfo()
  {
    reset();
  }
  ~ObMCLogRpcInfo(){};
  void reset();
  bool is_valid() const
  {
    return key_.is_valid() && timestamp_ > 0;
  }
  TO_STRING_KV(K_(key), K_(log_id), K_(timestamp));
  OB_UNIS_VERSION(1);
};

struct ObMemberChangeArg {
public:
  common::ObPartitionKey key_;
  common::ObReplicaMember member_;
  bool no_used_;
  int64_t quorum_;
  int64_t orig_quorum_;

  common::ObModifyQuorumType reserved_modify_quorum_type_;
  share::ObTaskId task_id_;

  int init(const common::ObPartitionKey& key, const common::ObReplicaMember& member, const bool is_permanent_offline,
      int64_t quorum, int64_t orig_quorum,
      const common::ObModifyQuorumType reserved_modify_quorum_type = WITH_MODIFY_QUORUM);
  bool is_valid() const
  {
    return key_.is_valid() && member_.is_valid() && quorum_ > 0;
  }
  TO_STRING_KV(
      K_(key), K_(member), K_(no_used), K_(quorum), K_(reserved_modify_quorum_type), K_(task_id), K_(orig_quorum));

  OB_UNIS_VERSION(3);
};

struct ObMemberChangeBatchResult {
public:
  ObMemberChangeBatchResult() : return_array_()
  {}

public:
  TO_STRING_KV(K_(return_array));

public:
  common::ObSArray<int> return_array_;

  OB_UNIS_VERSION(3);
};

struct ObMemberChangeBatchArg {
public:
  ObMemberChangeBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObMemberChangeArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;

  OB_UNIS_VERSION(3);
};

struct ObModifyQuorumBatchResult {
public:
  ObModifyQuorumBatchResult() : return_array_()
  {}

public:
  TO_STRING_KV(K_(return_array));

public:
  common::ObSArray<int> return_array_;

  OB_UNIS_VERSION(3);
};

struct ObModifyQuorumArg {
public:
  common::ObPartitionKey key_;
  int64_t quorum_;
  int64_t orig_quorum_;
  common::ObMemberList member_list_;
  share::ObTaskId task_id_;

  int init(const common::ObPartitionKey& key, int64_t quorum);
  bool is_valid() const
  {
    return key_.is_valid() && quorum_ > 0 && orig_quorum_ > 0 && member_list_.get_member_number() > 0;
  }
  TO_STRING_KV(K_(key), K_(quorum), K_(orig_quorum), K_(member_list), K_(task_id));

  OB_UNIS_VERSION(3);
};

struct ObModifyQuorumBatchArg {
public:
  ObModifyQuorumBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObModifyQuorumArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;

  OB_UNIS_VERSION(3);
};

struct ObRemoveReplicaArg {
  common::ObPartitionKey pkey_;
  common::ObReplicaMember replica_member_;

  bool is_valid() const
  {
    return pkey_.is_valid() && replica_member_.is_valid();
  }
  TO_STRING_KV(K_(pkey), K_(replica_member));
  OB_UNIS_VERSION(1);
};

struct ObRemoveReplicaArgs {
  ObRemoveReplicaArgs() : arg_array_()
  {}

  bool is_valid() const
  {
    bool b_ret = true;
    for (int64_t i = 0; i < arg_array_.count(); ++i) {
      if (!arg_array_.at(i).is_valid()) {
        b_ret = false;
        break;
      }
    }
    return b_ret;
  }

  TO_STRING_KV(K_(arg_array));

  common::ObSArray<ObRemoveReplicaArg> arg_array_;
  OB_UNIS_VERSION(1);
};

struct ObFetchMacroBlockArg {
  OB_UNIS_VERSION(2);

public:
  ObFetchMacroBlockArg() : macro_block_index_(0), data_version_(0), data_seq_(0)
  {}
  void reset();
  TO_STRING_KV(K_(macro_block_index), K_(data_version), K_(data_seq));

  int64_t macro_block_index_;  // the index of the macro block in the sstable macro block array
  common::ObVersion data_version_;
  int64_t data_seq_;
};

struct ObFetchMacroBlockListArg {
  OB_UNIS_VERSION(2);

public:
  ObFetchMacroBlockListArg();
  TO_STRING_KV(K_(table_key), "arg_count", arg_list_.count());
  storage::ObITable::TableKey table_key_;
  common::ObSArray<ObFetchMacroBlockArg> arg_list_;
};

struct ObFetchPartitionInfoArg {
  ObFetchPartitionInfoArg() : pkey_(), replica_type_(REPLICA_TYPE_FULL)
  {}
  explicit ObFetchPartitionInfoArg(const common::ObPartitionKey& pkey, const common::ObReplicaType& replica_type)
      : pkey_(pkey), replica_type_(replica_type)
  {}
  ~ObFetchPartitionInfoArg()
  {}
  TO_STRING_KV(K_(pkey), K_(replica_type));

  common::ObPartitionKey pkey_;
  common::ObReplicaType replica_type_;
  OB_UNIS_VERSION(1);
};

struct ObFetchPartitionInfoResult {
  static const int64_t OB_FETCH_PARTITION_INFO_RESULT_VERSION_V1 = 1;  // v1 meta type is ObPartitionStoreMeta
  static const int64_t OB_FETCH_PARTITION_INFO_RESULT_VERSION_V2 = 2;  // v2 meta type is ObPGPartitionStoreMeta
  ObFetchPartitionInfoResult() : meta_(), table_id_list_(), major_version_(0), is_log_sync_(false)
  {}
  ~ObFetchPartitionInfoResult()
  {}
  TO_STRING_KV(K_(meta), K_(table_id_list), K_(major_version), K_(is_log_sync));
  void reset();
  int assign(const ObFetchPartitionInfoResult& result);

  storage::ObPGPartitionStoreMeta meta_;
  ObSArray<uint64_t> table_id_list_;  // major table and index table
  common::ObVersion major_version_;   // max{major table major freeze version}
  bool is_log_sync_;
  OB_UNIS_VERSION(OB_FETCH_PARTITION_INFO_RESULT_VERSION_V2);
};

struct ObFetchTableInfoArg {
  ObFetchTableInfoArg() : pkey_(), table_id_(OB_INVALID_ID), snapshot_version_(0), is_only_major_sstable_(false)
  {}
  ~ObFetchTableInfoArg()
  {}
  TO_STRING_KV(K_(pkey), K_(table_id), K_(snapshot_version), K_(is_only_major_sstable));

  common::ObPartitionKey pkey_;
  uint64_t table_id_;
  int64_t snapshot_version_;
  bool is_only_major_sstable_;
  OB_UNIS_VERSION(1);
};

struct ObLogicMigrateRpcHeader {
  enum ConnectStatus { INVALID_STATUS = -1, RECONNECT = 0, KEEPCONNECT = 1, ENDCONNECT = 2, MAX };
  int32_t header_size_;
  int32_t occupy_size_;
  int32_t data_offset_;
  int32_t data_size_;
  int32_t object_count_;
  ConnectStatus connect_status_;

  ObLogicMigrateRpcHeader();
  ~ObLogicMigrateRpcHeader()
  {}
  bool is_valid() const;
  void reset();
  bool has_next_rpc()
  {
    return need_reconnect() || KEEPCONNECT == connect_status_;
  }
  bool need_reconnect()
  {
    return RECONNECT == connect_status_;
  }
  bool end_connect()
  {
    return ENDCONNECT == connect_status_;
  }
  TO_STRING_KV(K_(header_size), K_(occupy_size), K_(data_offset), K_(object_count), K_(data_size), K_(connect_status));

  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObLogicDataChecksumProtocol {
  ObLogicDataChecksumProtocol();
  void reset();
  bool is_valid() const;

  int64_t data_checksum_;
  bool is_rowkey_valid_;
  ObStoreRowkey rowkey_;
  TO_STRING_KV(K_(data_checksum), K_(is_rowkey_valid), K_(rowkey));
  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObFetchTableInfoResult {
  ObFetchTableInfoResult();
  ~ObFetchTableInfoResult()
  {}
  TO_STRING_KV(K_(multi_version_start), K_(is_ready_for_read), K_(table_keys), K_(gc_table_keys));

  void reset();
  ObSArray<storage::ObITable::TableKey> table_keys_;
  int64_t multi_version_start_;
  bool is_ready_for_read_;
  ObSArray<storage::ObITable::TableKey> gc_table_keys_;
  OB_UNIS_VERSION(2);
};

struct ObSplitDestPartitionResult {
  ObSplitDestPartitionResult()
      : status_(common::OB_SUCCESS), progress_(share::IN_SPLITTING), schema_version_(0), src_pkey_(), dest_pkey_()
  {}
  ~ObSplitDestPartitionResult()
  {}
  void reset()
  {
    status_ = common::OB_SUCCESS;
    progress_ = share::IN_SPLITTING;
    schema_version_ = 0;
    src_pkey_.reset();
    dest_pkey_.reset();
  }
  bool is_valid() const
  {
    return (common::OB_SUCCESS != status_) || (src_pkey_.is_valid() && dest_pkey_.is_valid());
  }
  TO_STRING_KV(K_(status), K_(progress), K_(schema_version), K_(src_pkey), K_(dest_pkey));
  int status_;
  int progress_;
  int64_t schema_version_;
  common::ObPartitionKey src_pkey_;
  common::ObPartitionKey dest_pkey_;
  OB_UNIS_VERSION(1);
};

struct ObFetchLogicBaseMetaArg {
  ObFetchLogicBaseMetaArg() : table_key_(), task_count_(1)
  {}
  ~ObFetchLogicBaseMetaArg()
  {}
  TO_STRING_KV(K_(table_key), K_(task_count));

  storage::ObITable::TableKey table_key_;
  int64_t task_count_;
  OB_UNIS_VERSION(1);
};

struct ObFetchPhysicalBaseMetaArg {
  ObFetchPhysicalBaseMetaArg() : table_key_()
  {}
  ~ObFetchPhysicalBaseMetaArg()
  {}
  TO_STRING_KV(K_(table_key));

  storage::ObITable::TableKey table_key_;
  OB_UNIS_VERSION(1);
};

struct ObFetchLogicRowArg {
  ObFetchLogicRowArg() : table_key_(), key_range_(), schema_version_(0), data_checksum_(0)
  {}
  ~ObFetchLogicRowArg()
  {}
  bool is_valid() const
  {
    return table_key_.is_valid() && schema_version_ >= OB_INVALID_VERSION;
  }

  int deep_copy(common::ObIAllocator& allocator, ObFetchLogicRowArg& dst_arg)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fetch logic row arg is invalid", K(ret));
    } else {
      dst_arg.table_key_ = table_key_;
      dst_arg.schema_version_ = schema_version_;
      dst_arg.data_checksum_ = data_checksum_;
      if (OB_FAIL(key_range_.deep_copy(allocator, dst_arg.key_range_))) {
        STORAGE_LOG(WARN, "fail to deep copy fetch logic row arg", K(ret));
      }
    }
    return ret;
  }
  TO_STRING_KV(K_(table_key), K_(key_range), K_(schema_version), K_(data_checksum));

  storage::ObITable::TableKey table_key_;
  common::ObStoreRange key_range_;
  int64_t schema_version_;
  int64_t data_checksum_;
  OB_UNIS_VERSION(1);
};

struct ObSplitDestPartitionRequestArg {
  ObSplitDestPartitionRequestArg() : dest_pkey_(), split_info_()
  {}
  ~ObSplitDestPartitionRequestArg()
  {}
  int init(const common::ObPartitionKey& dest_pkey, const storage::ObPartitionSplitInfo& split_info);
  bool is_valid() const
  {
    return dest_pkey_.is_valid();
  }
  TO_STRING_KV(K_(dest_pkey), K_(split_info));
  common::ObPartitionKey dest_pkey_;
  storage::ObPartitionSplitInfo split_info_;
  OB_UNIS_VERSION(1);
};

struct ObReplicaSplitProgressRequest {
  ObReplicaSplitProgressRequest() : schema_version_(0)
  {}
  ~ObReplicaSplitProgressRequest()
  {}
  int init(const int64_t schema_version, const common::ObPartitionKey& pkey, const common::ObAddr& addr);
  bool is_valid() const
  {
    return 0 < schema_version_ && pkey_.is_valid() && addr_.is_valid();
  }
  int64_t schema_version_;
  common::ObPartitionKey pkey_;
  common::ObAddr addr_;
  TO_STRING_KV(K_(schema_version), K_(pkey), K_(addr));
  OB_UNIS_VERSION(1);
};

struct ObReplicaSplitProgressResult {
  ObReplicaSplitProgressResult() : progress_(share::UNKNOWN_SPLIT_PROGRESS)
  {}
  ~ObReplicaSplitProgressResult()
  {}
  int init(const common::ObPartitionKey& pkey, const common::ObAddr& addr, const int progress);
  bool is_valid() const
  {
    return pkey_.is_valid() && addr_.is_valid();
  }
  common::ObPartitionKey pkey_;
  common::ObAddr addr_;
  int progress_;
  TO_STRING_KV(K_(pkey), K_(addr), K_(progress));
  OB_UNIS_VERSION(1);
};

// pg relate rpc struct
struct ObFetchPGInfoArg {
  static const int64_t FETCH_PG_INFO_ARG_COMPAT_VERSION_V1 = 1;  // below 31x
  static const int64_t FETCH_PG_INFO_ARG_COMPAT_VERSION_V2 = 2;  // 31x
  ObFetchPGInfoArg()
      : pg_key_(),
        replica_type_(REPLICA_TYPE_FULL),
        use_slave_safe_read_ts_(true),
        compat_version_(FETCH_PG_INFO_ARG_COMPAT_VERSION_V1)
  {}
  explicit ObFetchPGInfoArg(
      const common::ObPGKey& pkey, const common::ObReplicaType& replica_type, const bool use_slave_safe_read_ts)
      : pg_key_(pkey),
        replica_type_(replica_type),
        use_slave_safe_read_ts_(use_slave_safe_read_ts),
        compat_version_(FETCH_PG_INFO_ARG_COMPAT_VERSION_V2)
  {}
  ~ObFetchPGInfoArg()
  {}
  TO_STRING_KV(K_(pg_key), K_(replica_type), K_(use_slave_safe_read_ts), K_(compat_version));

  common::ObPGKey pg_key_;
  common::ObReplicaType replica_type_;
  bool use_slave_safe_read_ts_;
  int64_t compat_version_;
  OB_UNIS_VERSION(1);
};

struct ObFetchPGPartitionInfoArg {
  ObFetchPGPartitionInfoArg() : pg_key_(), snapshot_version_(0), is_only_major_sstable_(false), log_ts_(0)
  {}
  ~ObFetchPGPartitionInfoArg()
  {}
  bool is_valid() const
  {
    return pg_key_.is_valid() && snapshot_version_ >= 0;
  }

  TO_STRING_KV(K_(pg_key), K_(snapshot_version), K_(is_only_major_sstable), K_(log_ts));
  common::ObPGKey pg_key_;
  int64_t snapshot_version_;
  bool is_only_major_sstable_;
  int64_t log_ts_;
  OB_UNIS_VERSION(1);
};

struct ObPGPartitionMetaInfo {
  static const int64_t OB_PG_PARTITION_META_INFO_RESULT_VERSION_V1 = 1;  // v1 meta type is ObPartitionStoreMeta
  static const int64_t OB_PG_PARTITION_META_INFO_RESULT_VERSION_V2 = 2;  // v2 meta type is ObPGPartitionStoreMeta
  ObPGPartitionMetaInfo() : meta_(), table_id_list_(), table_info_()
  {}
  ~ObPGPartitionMetaInfo()
  {}
  TO_STRING_KV(K_(meta), K_(table_id_list), K_(table_info));
  void reset();
  int assign(const ObPGPartitionMetaInfo& result);
  bool is_valid() const;

  storage::ObPGPartitionStoreMeta meta_;
  ObSArray<uint64_t> table_id_list_;  // major table and index table
  ObSArray<ObFetchTableInfoResult> table_info_;
  OB_UNIS_VERSION(OB_PG_PARTITION_META_INFO_RESULT_VERSION_V2);
};

struct ObFetchPGInfoResult {
  static const int64_t FETCH_PG_INFO_RES_COMPAT_VERSION_V1 = 1;
  static const int64_t FETCH_PG_INFO_RES_COMPAT_VERSION_V2 = 2;
  ObFetchPGInfoResult()
      : pg_meta_(),
        major_version_(0),
        is_log_sync_(false),
        pg_file_id_(OB_INVALID_DATA_FILE_ID),
        compat_version_(FETCH_PG_INFO_RES_COMPAT_VERSION_V1)
  {}
  ~ObFetchPGInfoResult()
  {}
  TO_STRING_KV(K_(pg_meta), K_(major_version), K_(is_log_sync), K_(pg_file_id), K_(compat_version));
  void reset();
  int assign(const ObFetchPGInfoResult& result);
  bool is_from_31x() const
  {
    return compat_version_ == FETCH_PG_INFO_RES_COMPAT_VERSION_V2;
  }

  storage::ObPartitionGroupMeta pg_meta_;
  common::ObVersion major_version_;  // max{major table major freeze version}
  bool is_log_sync_;
  int64_t pg_file_id_;
  int64_t compat_version_;
  OB_UNIS_VERSION(1);
};

struct ObFetchReplicaInfoArg {
  ObFetchReplicaInfoArg() : pg_key_(), local_publish_version_()
  {}
  ~ObFetchReplicaInfoArg()
  {}
  TO_STRING_KV(K_(pg_key), K_(local_publish_version));
  void reset();
  int assign(const ObFetchReplicaInfoArg& arg);
  bool is_valid() const;
  ObPGKey pg_key_;
  int64_t local_publish_version_;
  OB_UNIS_VERSION(1);
};

struct ObFetchReplicaInfoRes {
  ObFetchReplicaInfoRes()
      : pg_key_(),
        remote_minor_snapshot_version_(),
        remote_replica_type_(REPLICA_TYPE_MAX),
        remote_major_snapshot_version_(0),
        remote_last_replay_log_id_(0)
  {}
  ~ObFetchReplicaInfoRes()
  {}
  TO_STRING_KV(K_(pg_key), K_(remote_minor_snapshot_version), K_(remote_replica_type),
      K_(remote_major_snapshot_version), K_(remote_last_replay_log_id));
  void reset();
  int assign(const ObFetchReplicaInfoRes);
  bool is_valid() const;
  ObPGKey pg_key_;
  int64_t remote_minor_snapshot_version_;
  ObReplicaType remote_replica_type_;
  int64_t remote_major_snapshot_version_;
  int64_t remote_last_replay_log_id_;
  OB_UNIS_VERSION(1);
};

struct ObBatchFetchReplicaInfoArg {
  ObBatchFetchReplicaInfoArg() : replica_info_arg_()
  {}
  ~ObBatchFetchReplicaInfoArg()
  {}
  TO_STRING_KV(K_(replica_info_arg));

  void reset();
  int assign(const ObBatchFetchReplicaInfoArg& arg);

  ObSArray<ObFetchReplicaInfoArg> replica_info_arg_;
  OB_UNIS_VERSION(1);
};

struct ObBatchFetchReplicaInfoRes {
  ObBatchFetchReplicaInfoRes() : replica_info_res_()
  {}
  ~ObBatchFetchReplicaInfoRes()
  {}
  TO_STRING_KV(K_(replica_info_res));
  void reset();
  int assign(const ObBatchFetchReplicaInfoRes& res);

  ObSArray<ObFetchReplicaInfoRes> replica_info_res_;
  OB_UNIS_VERSION(1);
};

struct ObSuspendPartitionArg {
  OB_UNIS_VERSION(1);

public:
  ObSuspendPartitionArg()
  {
    reset();
  }
  ~ObSuspendPartitionArg()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(pg_key_), K(mig_dest_server_), K(need_force_change_owner_), K(is_batch_));

public:
  ObPGKey pg_key_;
  ObAddr mig_dest_server_;
  bool need_force_change_owner_;
  bool is_batch_;
};

struct ObSuspendPartitionRes {
  OB_UNIS_VERSION(1);

public:
  ObSuspendPartitionRes()
  {
    reset();
  }
  ~ObSuspendPartitionRes()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(pg_key_), K(max_clog_id_));

public:
  ObPGKey pg_key_;
  uint64_t max_clog_id_;
};

enum ObHandoverPartitionType : int8_t {
  PARTITION_HANDOVER_TYPE_INVALID = 0,
  PARTITION_HANDOVER_TYPE_MIGRATE_OUT,
  PARTITION_HANDOVER_TYPE_MIGRATE_CLEANUP,
  PARTITION_HANDOVER_TYPE_RECOVER,
  PARTITION_HANDOVER_TYPE_MAX
};

struct ObHandoverPartitionArg {
  OB_UNIS_VERSION(1);

public:
  ObHandoverPartitionArg()
  {
    reset();
  }
  ~ObHandoverPartitionArg()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(type_), K(pg_key_), K(src_file_id_), K(candidate_server_));

public:
  ObHandoverPartitionType type_;
  ObPGKey pg_key_;
  int64_t src_file_id_;
  ObAddr candidate_server_;
};

class ObPartitionServiceRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObPartitionServiceRpcProxy);
  // 1.4x old rpc to fetch store info
  RPC_S(PR5 fetch_migrate_info, OB_PTS_FETCH_INFO, (ObMigrateInfoFetchArg), ObMigrateInfoFetchResult);

  RPC_S(PR5 add_replica_mc, OB_PTS_ADD_REPLICA, (ObMemberChangeArg), ObMCLogRpcInfo);
  RPC_S(PR5 remove_replica_mc, OB_PTS_REMOVE_REPLICA, (ObMemberChangeArg), ObMCLogRpcInfo);
  RPC_S(PR5 remove_replica, OB_PTS_REMOVE_PARTITION, (ObRemoveReplicaArg));
  RPC_S(PR5 is_member_change_done, OB_IN_MEMBER_CHANGE_DONE, (ObMCLogRpcInfo));
  RPC_S(PR5 batch_remove_member, OB_BATCH_REMOVE_MEMBER, (ObChangeMemberArgs), ObChangeMemberCtxsWrapper);
  RPC_S(PR5 batch_add_member, OB_BATCH_ADD_MEMBER, (ObChangeMemberArgs), ObChangeMemberCtxsWrapper);
  RPC_S(PR5 is_batch_member_change_done, OB_BATCH_MEMBER_CHANGE_DONE, (ObChangeMemberCtxs), ObChangeMemberCtxsWrapper);
  RPC_S(PR5 batch_remove_replica, OB_BATCH_REMOVE_PARTITION, (ObRemoveReplicaArgs));

  RPC_S(PR5 get_member_list, OB_GET_LEADER_MEMBER_LIST, (common::ObPartitionKey), common::ObMemberList);
  RPC_S(PR5 check_member_major_sstable_enough, OB_CHECK_MEMBER_MAJOR_SSTABLE_ENOUGH, (ObMemberMajorSSTableCheckArg));

  RPC_S(PR5 fetch_partition_info, OB_FETCH_PARTITION_INFO, (ObFetchPartitionInfoArg), ObFetchPartitionInfoResult);
  RPC_S(PR5 fetch_table_info, OB_FETCH_TABLE_INFO, (ObFetchTableInfoArg), ObFetchTableInfoResult);
  RPC_S(PR5 fetch_partition_group_info, OB_FETCH_PARTITION_GROUP_INFO, (ObFetchPGInfoArg), ObFetchPGInfoResult);
  RPC_S(PR5 check_member_pg_major_sstable_enough, OB_CHECK_MEMBER_PG_MAJOR_SSTABLE_ENOUGH,
      (ObMemberMajorSSTableCheckArg));
  RPC_S(PR5 fetch_replica_info, OB_FETCH_PUBLISH_VERSION, (ObBatchFetchReplicaInfoArg), ObBatchFetchReplicaInfoRes);

  RPC_SS(PR5 fetch_macro_block, OB_FETCH_MACRO_BLOCK, (ObFetchMacroBlockListArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_logic_base_meta, OB_FETCH_LOGIC_BASE_META, (ObFetchLogicBaseMetaArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_physical_base_meta, OB_FETCH_PHYSICAL_BASE_META, (ObFetchPhysicalBaseMetaArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_logic_row, OB_FETCH_LOGIC_ROW, (ObFetchLogicRowArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_logic_data_checksum, OB_FETCH_LOGIC_DATA_CHECKSUM, (ObFetchLogicRowArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_logic_data_checksum_slice, OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE, (ObFetchLogicRowArg),
      common::ObDataBuffer);
  RPC_SS(PR5 fetch_logic_row_slice, OB_FETCH_LOGIC_ROW_SLICE, (ObFetchLogicRowArg), common::ObDataBuffer);
  // 1.4x old rpc for fetch base meta data
  RPC_SS(PR5 fetch_base_data_meta, OB_FETCH_BASE_DATA_META, (ObFetchBaseDataMetaArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_macro_block_old, OB_FETCH_MACRO_BLOCK_OLD, (ObFetchMacroBlockListOldArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_pg_partition_info, OB_FETCH_PG_PARTITION_INFO, (ObFetchPGPartitionInfoArg), common::ObDataBuffer);

  RPC_AP(PR11 post_warm_up_request, OB_WARM_UP_REQUEST, (ObWarmUpRequestArg));
  RPC_AP(PR5 post_split_dest_partition_request, OB_SPLIT_DEST_PARTITION_REQUEST, (ObSplitDestPartitionRequestArg),
      ObSplitDestPartitionResult);
  RPC_AP(PR5 post_replica_split_progress_request, OB_REPLICA_SPLIT_PROGRESS_REQUEST, (ObReplicaSplitProgressRequest),
      ObReplicaSplitProgressResult);
};

// 1.4x old rpc to fetch store info
class ObPTSFetchInfoP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_PTS_FETCH_INFO> > {
public:
  explicit ObPTSFetchInfoP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObPTSFetchInfoP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObPTSAddMemberP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_PTS_ADD_REPLICA> > {
public:
  explicit ObPTSAddMemberP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObPTSAddMemberP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObPTSRemoveMemberP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_PTS_REMOVE_REPLICA> > {
public:
  explicit ObPTSRemoveMemberP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObPTSRemoveMemberP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObPTSRemoveReplicaP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_PTS_REMOVE_PARTITION> > {
public:
  explicit ObPTSRemoveReplicaP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObPTSRemoveReplicaP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObBatchRemoveReplicaP
    : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_BATCH_REMOVE_PARTITION> > {
public:
  explicit ObBatchRemoveReplicaP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObBatchRemoveReplicaP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObIsMemberChangeDoneP
    : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_IN_MEMBER_CHANGE_DONE> > {
public:
  explicit ObIsMemberChangeDoneP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObIsMemberChangeDoneP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObWarmUpRequestP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_WARM_UP_REQUEST> > {
public:
  explicit ObWarmUpRequestP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObWarmUpRequestP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObSplitDestPartitionRequestP
    : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_SPLIT_DEST_PARTITION_REQUEST> > {
public:
  explicit ObSplitDestPartitionRequestP(storage::ObPartitionService* ps) : ps_(ps)
  {}
  virtual ~ObSplitDestPartitionRequestP()
  {}

protected:
  int process();

private:
  storage::ObPartitionService* ps_;
};

class ObReplicaSplitProgressRequestP
    : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_REPLICA_SPLIT_PROGRESS_REQUEST> > {
public:
  explicit ObReplicaSplitProgressRequestP(storage::ObPartitionService* ps) : ps_(ps)
  {}
  virtual ~ObReplicaSplitProgressRequestP()
  {}

protected:
  int process();

private:
  storage::ObPartitionService* ps_;
};

class ObGetMemberListP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_GET_LEADER_MEMBER_LIST> > {
public:
  explicit ObGetMemberListP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObGetMemberListP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObCheckMemberMajorSSTableEnoughP
    : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_CHECK_MEMBER_MAJOR_SSTABLE_ENOUGH> > {
public:
  explicit ObCheckMemberMajorSSTableEnoughP(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  virtual ~ObCheckMemberMajorSSTableEnoughP()
  {
    partition_service_ = NULL;
  }

protected:
  virtual int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObCheckMemberPGMajorSSTableEnoughP
    : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_CHECK_MEMBER_PG_MAJOR_SSTABLE_ENOUGH> > {
public:
  explicit ObCheckMemberPGMajorSSTableEnoughP(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  virtual ~ObCheckMemberPGMajorSSTableEnoughP()
  {
    partition_service_ = NULL;
  }

protected:
  virtual int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObFetchReplicaInfoP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_FETCH_PUBLISH_VERSION> > {
public:
  explicit ObFetchReplicaInfoP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObFetchReplicaInfoP()
  {
    partition_service_ = NULL;
  }

protected:
  virtual int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObWarmUpRPCCB : public ObPartitionServiceRpcProxy::AsyncCB<obrpc::OB_WARM_UP_REQUEST> {
public:
  ObWarmUpRPCCB()
  {}
  virtual ~ObWarmUpRPCCB()
  {}
  virtual void set_args(const Request& args)
  {
    UNUSED(args);
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB* clone(const oceanbase::rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObWarmUpRPCCB* newcb = NULL;

    if (NULL != buf) {
      newcb = new (buf) ObWarmUpRPCCB();
    }
    return newcb;
  }

public:
  int process()
  { /* do nothing */
    return common::OB_SUCCESS;
  }
  void on_timeout()
  { /* do nothing */
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpRPCCB);
};

class ObSplitDestPartitionRPCCB : public ObPartitionServiceRpcProxy::AsyncCB<obrpc::OB_SPLIT_DEST_PARTITION_REQUEST> {
public:
  ObSplitDestPartitionRPCCB() : ps_(NULL)
  {}
  virtual ~ObSplitDestPartitionRPCCB()
  {}
  int init(storage::ObPartitionService* ps)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == ps) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ps_ = ps;
    }
    return ret;
  }
  virtual void set_args(const Request& args)
  {
    UNUSED(args);
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB* clone(const oceanbase::rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObSplitDestPartitionRPCCB* newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObSplitDestPartitionRPCCB();
      if (NULL != newcb) {
        newcb->ps_ = ps_;
      }
    }
    return newcb;
  }

public:
  int process();
  void on_timeout();

private:
  storage::ObPartitionService* ps_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSplitDestPartitionRPCCB);
};

class ObReplicaSplitProgressRPCCB
    : public ObPartitionServiceRpcProxy::AsyncCB<obrpc::OB_REPLICA_SPLIT_PROGRESS_REQUEST> {
public:
  ObReplicaSplitProgressRPCCB() : ps_(NULL)
  {}
  virtual ~ObReplicaSplitProgressRPCCB()
  {}
  int init(storage::ObPartitionService* ps)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == ps) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ps_ = ps;
    }
    return ret;
  }
  virtual void set_args(const Request& args)
  {
    UNUSED(args);
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB* clone(const oceanbase::rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObReplicaSplitProgressRPCCB* newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObReplicaSplitProgressRPCCB();
      if (NULL != newcb) {
        newcb->ps_ = ps_;
      }
    }
    return newcb;
  }

public:
  int process();
  void on_timeout();

private:
  storage::ObPartitionService* ps_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObReplicaSplitProgressRPCCB);
};

template <ObRpcPacketCode RPC_CODE>
class ObCommonPartitionServiceRpcP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<RPC_CODE> > {
public:
  explicit ObCommonPartitionServiceRpcP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObCommonPartitionServiceRpcP()
  {}

protected:
  template <typename Data>
  int fill_data(const Data& data);
  template <typename Data>
  int fill_data_list(ObIArray<Data>& data_list);
  template <typename Data>
  int fill_data_immediate(const Data& data);
  int fill_buffer(blocksstable::ObBufferReader& data);
  int flush_and_wait();
  int alloc_buffer();

protected:
  storage::ObPartitionService* partition_service_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  int64_t last_send_time_;
  common::ObArenaAllocator allocator_;
};

template <ObRpcPacketCode RPC_CODE>
class ObLogicPartitionServiceRpcP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<RPC_CODE> > {
public:
  explicit ObLogicPartitionServiceRpcP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObLogicPartitionServiceRpcP();

protected:
  template <typename Data>
  int fill_data(const Data& data);
  int reserve_header();
  int fill_header();
  int encode_header();
  int update_header(const int64_t data_size);
  int do_flush();
  int flush_and_wait();
  int set_connect_status(const ObLogicMigrateRpcHeader::ConnectStatus connect_status);
  int extend_buffer(const int64_t buf_size);

protected:
  storage::ObPartitionService* partition_service_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  int64_t last_send_time_;
  common::ObArenaAllocator allocator_;
  ObLogicMigrateRpcHeader rpc_header_;
  int64_t header_encode_length_;
  int64_t header_encode_offset_;
};

// 1.4x old rpc
class ObFetchBaseDataMetaP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_FETCH_BASE_DATA_META> > {
public:
  explicit ObFetchBaseDataMetaP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchBaseDataMetaP();

protected:
  int process();
};

// 1.4x old rpc
class ObFetchMacroBlockOldP : public ObCommonPartitionServiceRpcP<OB_FETCH_MACRO_BLOCK_OLD> {
public:
  explicit ObFetchMacroBlockOldP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchMacroBlockOldP()
  {}

protected:
  int process();
};

class ObFetchMacroBlockP : public ObCommonPartitionServiceRpcP<OB_FETCH_MACRO_BLOCK> {
public:
  explicit ObFetchMacroBlockP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchMacroBlockP()
  {}

protected:
  int process();

private:
  int64_t total_macro_block_count_;
};

class ObBatchRemoveMemberP : public ObPartitionServiceRpcProxy::Processor<OB_BATCH_REMOVE_MEMBER> {
public:
  explicit ObBatchRemoveMemberP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObBatchRemoveMemberP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObBatchAddMemberP : public ObPartitionServiceRpcProxy::Processor<OB_BATCH_ADD_MEMBER> {
public:
  explicit ObBatchAddMemberP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObBatchAddMemberP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObBatchMemberChangeDoneP : public ObPartitionServiceRpcProxy::Processor<OB_BATCH_MEMBER_CHANGE_DONE> {
public:
  explicit ObBatchMemberChangeDoneP(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  virtual ~ObBatchMemberChangeDoneP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObFetchPartitionInfoP : public ObPartitionServiceRpcProxy::Processor<OB_FETCH_PARTITION_INFO> {
public:
  explicit ObFetchPartitionInfoP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObFetchPartitionInfoP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObFetchTableInfoP : public ObRpcProcessor<obrpc::ObPartitionServiceRpcProxy::ObRpc<OB_FETCH_TABLE_INFO> > {
public:
  explicit ObFetchTableInfoP(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  virtual ~ObFetchTableInfoP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObFetchLogicBaseMetaP : public ObCommonPartitionServiceRpcP<OB_FETCH_LOGIC_BASE_META> {
public:
  ObFetchLogicBaseMetaP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  ~ObFetchLogicBaseMetaP()
  {}

protected:
  int process();
};

class ObFetchPhysicalBaseMetaP : public ObCommonPartitionServiceRpcP<OB_FETCH_PHYSICAL_BASE_META> {
public:
  ObFetchPhysicalBaseMetaP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  ~ObFetchPhysicalBaseMetaP()
  {}

protected:
  int process();
};

class ObFetchLogicRowInfo final {
public:
  ObFetchLogicRowInfo();
  ~ObFetchLogicRowInfo();
  int add_row(const storage::ObStoreRow& row);
  TO_STRING_KV(K_(total_row_count), K_(not_exist_row_count), K_(exist_row_count), K_(del_row_count),
      K_(sparse_row_count), "dml_count", common::ObArrayWrap<int64_t>(dml_count_, storage::T_DML_MAX),
      "first_dml_count", common::ObArrayWrap<int64_t>(first_dml_count_, storage::T_DML_MAX));

private:
  int64_t total_row_count_;
  int64_t not_exist_row_count_;
  int64_t exist_row_count_;
  int64_t del_row_count_;
  int64_t sparse_row_count_;
  int64_t dml_count_[storage::T_DML_MAX];
  int64_t first_dml_count_[storage::T_DML_MAX];
  DISALLOW_COPY_AND_ASSIGN(ObFetchLogicRowInfo);
};

class ObFetchLogicRowP : public ObCommonPartitionServiceRpcP<OB_FETCH_LOGIC_ROW> {
public:
  ObFetchLogicRowP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  ~ObFetchLogicRowP()
  {}

protected:
  int process();
};

class ObFetchLogicDataChecksumP : public ObCommonPartitionServiceRpcP<OB_FETCH_LOGIC_DATA_CHECKSUM> {
public:
  ObFetchLogicDataChecksumP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchLogicDataChecksumP()
  {}

protected:
  int process();
};

class ObFetchLogicDataChecksumSliceP : public ObLogicPartitionServiceRpcP<OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE> {
public:
  ObFetchLogicDataChecksumSliceP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchLogicDataChecksumSliceP()
  {}

protected:
  int process();
  int fill_rpc_buffer(const ObLogicDataChecksumProtocol& checksum_protocol);
  int set_data_checksum_protocol(const bool is_rowkey_valid, const int64_t schema_rowkey_cnt,
      const int64_t data_checksum, const storage::ObStoreRow* store_row = NULL);
};

class ObFetchLogicRowSliceP : public ObLogicPartitionServiceRpcP<OB_FETCH_LOGIC_ROW_SLICE> {
public:
  ObFetchLogicRowSliceP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchLogicRowSliceP()
  {}

protected:
  int process();
};

class ObFetchPartitionGroupInfoP : public ObPartitionServiceRpcProxy::Processor<OB_FETCH_PARTITION_GROUP_INFO> {
public:
  explicit ObFetchPartitionGroupInfoP(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  virtual ~ObFetchPartitionGroupInfoP()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObFetchPGPartitioninfoP : public ObCommonPartitionServiceRpcP<OB_FETCH_PG_PARTITION_INFO> {
public:
  explicit ObFetchPGPartitioninfoP(
      storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle);
  virtual ~ObFetchPGPartitioninfoP()
  {}

protected:
  int process();
};
}  // namespace obrpc

namespace storage {
class ObIPartitionServiceRpc {
public:
  ObIPartitionServiceRpc()
  {}
  virtual ~ObIPartitionServiceRpc()
  {}
  virtual int init(obrpc::ObPartitionServiceRpcProxy* rpc_proxy, ObPartitionService* partition_service,
      const common::ObAddr& self, obrpc::ObCommonRpcProxy* rs_rpc_proxy) = 0;
  virtual void destroy() = 0;

public:
  virtual int post_add_replica_mc_msg(
      const common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info) = 0;
  virtual int post_add_replica_res(const common::ObAddr& server, const obrpc::ObAddReplicaRes& res) = 0;
  virtual int post_remove_replica_mc_msg(
      const common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info) = 0;
  virtual int post_remove_replica(const common::ObAddr& server, const obrpc::ObRemoveReplicaArg& arg) = 0;
  virtual int batch_post_remove_replica(const common::ObAddr& server, const obrpc::ObRemoveReplicaArgs& args) = 0;

  virtual int post_migrate_replica_res(const common::ObAddr& server, const obrpc::ObMigrateReplicaRes& res) = 0;
  virtual int post_change_replica_res(const common::ObAddr& server, const obrpc::ObChangeReplicaRes& res) = 0;

  virtual int post_rebuild_replica_res(const common::ObAddr& server, const obrpc::ObRebuildReplicaRes& res) = 0;
  virtual int post_batch_backup_replica_res(const common::ObAddr& server, const obrpc::ObBackupBatchRes& res) = 0;
  virtual int post_validate_backup_res(const common::ObAddr& server, const obrpc::ObValidateRes& res) = 0;
  virtual int post_batch_validate_backup_res(const common::ObAddr& server, const obrpc::ObValidateBatchRes& res) = 0;

  virtual int post_get_member_list_msg(
      const common::ObAddr& server, const common::ObPartitionKey& key, common::ObMemberList& member_list) = 0;
  virtual int post_warm_up_request(
      const common::ObAddr& server, const uint64_t tenant_id, const obrpc::ObWarmUpRequestArg& arg) = 0;
  virtual int post_split_dest_partition_request(
      const common::ObAddr& server, const uint64_t tenant_id, const obrpc::ObSplitDestPartitionRequestArg& arg) = 0;
  virtual int post_replica_split_progress_request(
      const common::ObAddr& server, const uint64_t tenant_id, const obrpc::ObReplicaSplitProgressRequest& arg) = 0;
  virtual int batch_post_remove_replica_mc_msg(
      const common::ObAddr& server, obrpc::ObChangeMemberArgs& arg, obrpc::ObChangeMemberCtxs& mc_log_info) = 0;
  virtual int batch_post_add_replica_mc_msg(
      const common::ObAddr& server, obrpc::ObChangeMemberArgs& arg, obrpc::ObChangeMemberCtxs& mc_log_info) = 0;
  virtual int is_batch_member_change_done(const common::ObAddr& server, obrpc::ObChangeMemberCtxs& mc_log_info) = 0;
  virtual int check_member_major_sstable_enough(
      const common::ObAddr& server, obrpc::ObMemberMajorSSTableCheckArg& arg) = 0;
  virtual int check_member_pg_major_sstable_enough(
      const common::ObAddr& server, obrpc::ObMemberMajorSSTableCheckArg& arg) = 0;

  virtual int post_fetch_partition_info_request(const common::ObAddr& server, const common::ObPartitionKey& pkey,
      const common::ObReplicaType& replica_type, const int64_t cluster_id, obrpc::ObFetchPartitionInfoResult& res) = 0;
  virtual int post_fetch_table_info_request(const common::ObAddr& server, const common::ObPartitionKey& pkey,
      const uint64_t& table_id, const int64_t& snapshot_version, const bool is_only_major_sstable,
      const int64_t cluster_id, obrpc::ObFetchTableInfoResult& res) = 0;
  virtual int post_fetch_partition_group_info_request(const common::ObAddr& server, const common::ObPGKey& pkey,
      const common::ObReplicaType& replica_type, const int64_t cluster_id, const bool use_slave_safe_read_ts,
      obrpc::ObFetchPGInfoResult& res) = 0;
};

class ObPartitionServiceRpc : public ObIPartitionServiceRpc {
public:
  ObPartitionServiceRpc()
      : is_inited_(false),
        rpc_proxy_(NULL),
        partition_service_(NULL),
        split_dest_partition_cb_(),
        replica_split_progress_cb_(),
        rs_rpc_proxy_(NULL)
  {}
  ~ObPartitionServiceRpc()
  {
    destroy();
  }
  int init(obrpc::ObPartitionServiceRpcProxy* rpc_proxy, ObPartitionService* partition_service,
      const common::ObAddr& self, obrpc::ObCommonRpcProxy* rs_rpc_proxy);
  void destroy();

public:
  int post_add_replica_mc_msg(
      const common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  int post_remove_replica_mc_msg(
      const common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info);
  int post_remove_replica(const common::ObAddr& server, const obrpc::ObRemoveReplicaArg& arg);

  int batch_post_remove_replica(const common::ObAddr& server, const obrpc::ObRemoveReplicaArgs& args);
  int is_member_change_done(const common::ObAddr& server, obrpc::ObMCLogRpcInfo& mc_log_info);
  int post_add_replica_res(const common::ObAddr& server, const obrpc::ObAddReplicaRes& res);
  int post_batch_add_replica_res(const common::ObAddr& server, const obrpc::ObAddReplicaBatchRes& res);
  int post_migrate_replica_res(const common::ObAddr& server, const obrpc::ObMigrateReplicaRes& res);
  int post_batch_migrate_replica_res(const common::ObAddr& server, const obrpc::ObMigrateReplicaBatchRes& res);
  int post_batch_change_replica_res(const common::ObAddr& server, const obrpc::ObChangeReplicaBatchRes& res);
  int post_change_replica_res(const common::ObAddr& server, const obrpc::ObChangeReplicaRes& res);
  int post_restore_replica_res(const common::ObAddr& server, const obrpc::ObRestoreReplicaRes& res);
  int post_phy_restore_replica_res(const common::ObAddr& server, const obrpc::ObPhyRestoreReplicaRes& res);
  int post_validate_backup_res(const common::ObAddr& server, const obrpc::ObValidateRes& res);
  int post_batch_copy_sstable_res(const common::ObAddr& sever, const obrpc::ObCopySSTableBatchRes& res);
  int post_rebuild_replica_res(const common::ObAddr& server, const obrpc::ObRebuildReplicaRes& res);
  int post_batch_backup_replica_res(const common::ObAddr& server, const obrpc::ObBackupBatchRes& res);
  int post_batch_validate_backup_res(const common::ObAddr& server, const obrpc::ObValidateBatchRes& res);

  virtual int post_get_member_list_msg(
      const common::ObAddr& server, const common::ObPartitionKey& key, common::ObMemberList& member_list);
  int post_warm_up_request(
      const common::ObAddr& server, const uint64_t tenant_id, const obrpc::ObWarmUpRequestArg& arg);

  int post_split_dest_partition_request(
      const common::ObAddr& server, const uint64_t tenant_id, const obrpc::ObSplitDestPartitionRequestArg& arg);
  int post_replica_split_progress_request(
      const common::ObAddr& server, const uint64_t tenant_id, const obrpc::ObReplicaSplitProgressRequest& arg);

  const common::ObAddr& get_self() const
  {
    return self_;
  }

  // batch change member list
  int batch_post_remove_replica_mc_msg(
      const common::ObAddr& server, obrpc::ObChangeMemberArgs& arg, obrpc::ObChangeMemberCtxs& mc_log_info);
  int batch_post_add_replica_mc_msg(
      const common::ObAddr& server, obrpc::ObChangeMemberArgs& arg, obrpc::ObChangeMemberCtxs& mc_log_info);
  int is_batch_member_change_done(const common::ObAddr& server, obrpc::ObChangeMemberCtxs& mc_log_info);
  int check_member_major_sstable_enough(const common::ObAddr& server, obrpc::ObMemberMajorSSTableCheckArg& arg);

  int check_member_pg_major_sstable_enough(const common::ObAddr& server, obrpc::ObMemberMajorSSTableCheckArg& arg);

  // 2.0 migrate
  int post_fetch_partition_info_request(const common::ObAddr& server, const common::ObPartitionKey& pkey,
      const common::ObReplicaType& replica_type, const int64_t cluster_id, obrpc::ObFetchPartitionInfoResult& res);
  int post_fetch_table_info_request(const common::ObAddr& server, const common::ObPartitionKey& pkey,
      const uint64_t& table_id, const int64_t& snapshot_version, const bool is_only_major_sstable,
      const int64_t cluster_id, obrpc::ObFetchTableInfoResult& res);
  int post_fetch_partition_group_info_request(const common::ObAddr& server, const common::ObPGKey& pkey,
      const common::ObReplicaType& replica_type, const int64_t cluster_id, const bool use_slave_safe_read_ts,
      obrpc::ObFetchPGInfoResult& res);

private:
  bool is_inited_;
  obrpc::ObPartitionServiceRpcProxy* rpc_proxy_;
  ObPartitionService* partition_service_;
  common::ObAddr self_;
  obrpc::ObWarmUpRPCCB warm_up_cb_;
  obrpc::ObSplitDestPartitionRPCCB split_dest_partition_cb_;
  obrpc::ObReplicaSplitProgressRPCCB replica_split_progress_cb_;
  obrpc::ObCommonRpcProxy* rs_rpc_proxy_;
};
}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_SERVICE_RPC_
