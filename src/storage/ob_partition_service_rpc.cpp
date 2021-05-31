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

#define USING_LOG_PREFIX STORAGE
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_task_define.h"
#include "storage/ob_partition_service_rpc.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "blocksstable/ob_macro_block_writer.h"
#include "storage/ob_ms_row_iterator.h"
#include "storage/ob_partition_split.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_migrator_table_key_mgr.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;

namespace obrpc {

void ObMCLogRpcInfo::reset()
{
  key_.reset();
  log_id_ = 0;
  timestamp_ = 0;
}

OB_SERIALIZE_MEMBER(ObMCLogRpcInfo, key_, log_id_, timestamp_);

int ObMemberChangeArg::init(const common::ObPartitionKey& key, const common::ObReplicaMember& member,
    const bool is_permanent_offline, const int64_t quorum, const int64_t orig_quorum,
    const ObModifyQuorumType reserved_modify_quorum_type)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid() || !member.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(member), K(quorum));
  } else {
    key_ = key;
    member_ = member;
    no_used_ = is_permanent_offline;
    quorum_ = quorum;
    orig_quorum_ = orig_quorum;
    reserved_modify_quorum_type_ = reserved_modify_quorum_type;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObMemberChangeArg, key_, member_, no_used_, quorum_, reserved_modify_quorum_type_, task_id_, orig_quorum_);

OB_SERIALIZE_MEMBER(ObRemoveReplicaArg, pkey_, replica_member_);

OB_SERIALIZE_MEMBER(ObMemberChangeBatchResult, return_array_);

OB_SERIALIZE_MEMBER(ObMemberChangeBatchArg, arg_array_, timeout_ts_);
OB_SERIALIZE_MEMBER(ObRemoveReplicaArgs, arg_array_);

bool ObMemberChangeBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObModifyQuorumBatchResult, return_array_);
OB_SERIALIZE_MEMBER(ObModifyQuorumArg, key_, quorum_, orig_quorum_, member_list_);
OB_SERIALIZE_MEMBER(ObModifyQuorumBatchArg, arg_array_, timeout_ts_);

bool ObModifyQuorumBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObFetchBaseDataMetaArg, pkey_, version_);

OB_SERIALIZE_MEMBER(ObFetchMacroBlockArg, macro_block_index_, data_version_, data_seq_);

void ObFetchMacroBlockArg::reset()
{
  macro_block_index_ = 0;
  data_version_ = 0;
  data_seq_ = 0;
}

ObFetchMacroBlockListArg::ObFetchMacroBlockListArg() : table_key_(), arg_list_()
{}

OB_SERIALIZE_MEMBER(ObFetchMacroBlockListArg, table_key_, arg_list_);

ObLogicMigrateRpcHeader::ObLogicMigrateRpcHeader()
    : header_size_(0),
      occupy_size_(0),
      data_offset_(0),
      data_size_(0),
      object_count_(0),
      connect_status_(INVALID_STATUS)
{}

bool ObLogicMigrateRpcHeader::is_valid() const
{
  return header_size_ > 0 && occupy_size_ > 0 && data_offset_ > 0 && data_size_ >= 0 && object_count_ >= 0 &&
         connect_status_ > INVALID_STATUS && connect_status_ < MAX;
}

void ObLogicMigrateRpcHeader::reset()
{
  header_size_ = 0;
  occupy_size_ = 0;
  data_offset_ = 0;
  data_size_ = 0;
  object_count_ = 0;
  connect_status_ = INVALID_STATUS;
}

DEFINE_SERIALIZE(ObLogicMigrateRpcHeader)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < header_size_) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "serialize superblock failed.", KP(buf), K(buf_len), K(pos), K(header_size_), K(ret));
  } else {
    MEMCPY(buf + pos, this, sizeof(ObLogicMigrateRpcHeader));
    pos += header_size_;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogicMigrateRpcHeader)
{
  int ret = OB_SUCCESS;
  // read size first;
  if (NULL == buf || data_len - pos < static_cast<int64_t>(sizeof(int32_t))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(pos), K(header_size_), K(ret));
  } else {
    int32_t header_size = *(reinterpret_cast<const int32_t*>(buf));
    if (data_len - pos < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(ERROR, "data_len not enough for header size.", K(data_len), K(pos), K(header_size), K(ret));
    } else {
      MEMCPY(this, buf + pos, sizeof(ObLogicMigrateRpcHeader));
      pos += header_size;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogicMigrateRpcHeader)
{
  return sizeof(ObLogicMigrateRpcHeader);
}

ObLogicDataChecksumProtocol::ObLogicDataChecksumProtocol() : data_checksum_(0), is_rowkey_valid_(false), rowkey_()
{}

bool ObLogicDataChecksumProtocol::is_valid() const
{
  bool ret = true;
  if (is_rowkey_valid_) {
    ret = rowkey_.is_valid();
  }
  return ret;
}

void ObLogicDataChecksumProtocol::reset()
{
  data_checksum_ = 0;
  is_rowkey_valid_ = false;
  rowkey_.reset();
}

int ObLogicDataChecksumProtocol::serialize(char* buf, int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  // int64_t start_pos = pos;
  ObBufferWriter buffer_writer(buf, data_len, pos);

  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len));
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "logic data checksum protol is invalid ", K(ret));
  } else if (OB_FAIL(buffer_writer.write(data_checksum_))) {
    STORAGE_LOG(WARN,
        "serialization data_checksum error.",
        K(ret),
        K(data_checksum_),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(is_rowkey_valid_))) {
    STORAGE_LOG(WARN,
        "serialization is_rowkey_valid error.",
        K(ret),
        K(is_rowkey_valid_),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (is_rowkey_valid_ && OB_FAIL(buffer_writer.write(rowkey_))) {
    STORAGE_LOG(
        WARN, "serialization rowkey error.", K(ret), K(rowkey_), K(buffer_writer.capacity()), K(buffer_writer.pos()));
  }

  if (OB_SUCC(ret)) {
    pos = buffer_writer.pos();
  }
  return ret;
}

int ObLogicDataChecksumProtocol::deserialize(const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  // int64_t start_pos = pos;
  ObBufferReader buffer_reader(buf, data_len, pos);
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_reader.read(data_checksum_))) {
    STORAGE_LOG(WARN,
        "deserialization data_checksum error.",
        K(ret),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(data_checksum_));
  } else if (OB_FAIL(buffer_reader.read(is_rowkey_valid_))) {
    STORAGE_LOG(WARN,
        "deserialization is_rowkey_valid error.",
        K(ret),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(is_rowkey_valid_));
  } else if (is_rowkey_valid_ && OB_FAIL(buffer_reader.read(rowkey_))) {
    STORAGE_LOG(
        WARN, "deserialization rowkey error.", K(ret), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(rowkey_));
  }

  if (OB_SUCC(ret)) {
    pos = buffer_reader.pos();
  }
  return ret;
}

int64_t ObLogicDataChecksumProtocol::get_serialize_size() const
{
  int64_t size = sizeof(int64_t) + sizeof(bool);
  if (is_rowkey_valid_) {
    size += rowkey_.get_deep_copy_size();
  }
  return size;
}

int ObSplitDestPartitionRequestArg::init(const ObPartitionKey& dest_pkey, const ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  if (!dest_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(dest_pkey), K(split_info));
  } else if (OB_FAIL(split_info_.init(
                 split_info.get_schema_version(), split_info.get_spp(), ObPartitionSplitInfo::SPLIT_DEST_PARTITION))) {
    STORAGE_LOG(WARN, "init split info failed", K(ret), K(split_info));
  } else {
    split_info_.set_split_version(split_info.get_split_version());
    split_info_.set_source_log_id(split_info.get_source_log_id());
    split_info_.set_source_log_ts(split_info.get_source_log_ts());
    dest_pkey_ = dest_pkey;
  }
  return ret;
}

int ObReplicaSplitProgressRequest::init(const int64_t schema_version, const ObPartitionKey& pkey, const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  if (0 >= schema_version || !pkey.is_valid() || !addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version), K(pkey), K(addr));
  } else {
    schema_version_ = schema_version;
    pkey_ = pkey;
    addr_ = addr;
  }
  return ret;
}

int ObReplicaSplitProgressResult::init(const ObPartitionKey& pkey, const ObAddr& addr, const int progress)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || !addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(addr), K(progress));
  } else {
    pkey_ = pkey;
    addr_ = addr;
    progress_ = progress;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFetchPGInfoArg, pg_key_, replica_type_, use_slave_safe_read_ts_, compat_version_);
OB_SERIALIZE_MEMBER(ObFetchPGPartitionInfoArg, pg_key_, snapshot_version_, is_only_major_sstable_, log_ts_);

OB_UNIS_DEF_SERIALIZE(ObPGPartitionMetaInfo, meta_, table_id_list_, table_info_);
OB_UNIS_DEF_SERIALIZE_SIZE(ObPGPartitionMetaInfo, meta_, table_id_list_, table_info_);
int ObPGPartitionMetaInfo::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, version))) {
    LOG_WARN("fail to decode unis_version", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, len))) {
    LOG_WARN("fail to decode len", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(len < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decode length is negative", K(ret), K(len), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(data_len < len + pos)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("buf length not enough", K(ret), K(len), KP(buf), K(data_len), K(pos));
  } else if (OB_PG_PARTITION_META_INFO_RESULT_VERSION_V2 == version) {  // normal decode
    LST_DO_CODE(OB_UNIS_DECODE, meta_, table_id_list_, table_info_);
  } else if (OB_PG_PARTITION_META_INFO_RESULT_VERSION_V1 == version) {
    ObPartitionStoreMeta old_meta;
    LST_DO_CODE(OB_UNIS_DECODE, old_meta, table_id_list_, table_info_);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(meta_.copy_from_old_meta(old_meta))) {
        LOG_WARN("fail to convert old meta", K(ret), K(old_meta));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported unis_version", K(ret), K(version), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

void ObPGPartitionMetaInfo::reset()
{
  meta_.reset();
  table_id_list_.reset();
  table_info_.reset();
}

int ObPGPartitionMetaInfo::assign(const ObPGPartitionMetaInfo& result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_.deep_copy(result.meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(result));
  } else if (OB_FAIL(table_id_list_.assign(result.table_id_list_))) {
    STORAGE_LOG(WARN, "fail to assign table id list", K(ret), K(result));
  } else if (OB_FAIL(table_info_.assign(result.table_info_))) {
    STORAGE_LOG(WARN, "fail to assign table info", K(ret), K(result));
  }
  return ret;
}

bool ObPGPartitionMetaInfo::is_valid() const
{
  // consider LOG replica table_info can be empty
  return meta_.is_valid() && table_id_list_.count() >= 0 && table_info_.count() >= 0 &&
         table_info_.count() == table_id_list_.count();
}

OB_SERIALIZE_MEMBER(ObFetchPGInfoResult, pg_meta_, major_version_, is_log_sync_, pg_file_id_, compat_version_);

void ObFetchPGInfoResult::reset()
{
  pg_meta_.reset();
  major_version_ = 0;
  is_log_sync_ = false;
  pg_file_id_ = OB_INVALID_DATA_FILE_ID;
  compat_version_ = 0;
}

int ObFetchPGInfoResult::assign(const ObFetchPGInfoResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_meta_.deep_copy(result.pg_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition group meta", K(ret), K(result));
  } else {
    major_version_ = result.major_version_;
    is_log_sync_ = result.is_log_sync_;
    pg_file_id_ = result.pg_file_id_;
    compat_version_ = result.compat_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFetchReplicaInfoArg, pg_key_, local_publish_version_);
void ObFetchReplicaInfoArg::reset()
{
  pg_key_.reset();
  local_publish_version_ = 0;
}

bool ObFetchReplicaInfoArg::is_valid() const
{
  return pg_key_.is_valid() && local_publish_version_ >= 0;
}

int ObFetchReplicaInfoArg::assign(const ObFetchReplicaInfoArg& arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "assign get invalid argument", K(ret), K(arg));
  } else {
    pg_key_ = arg.pg_key_;
    local_publish_version_ = arg.local_publish_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFetchReplicaInfoRes, pg_key_, remote_minor_snapshot_version_, remote_replica_type_,
    remote_major_snapshot_version_, remote_last_replay_log_id_);
void ObFetchReplicaInfoRes::reset()
{
  pg_key_.reset();
  remote_minor_snapshot_version_ = 0;
  remote_replica_type_ = ObReplicaType::REPLICA_TYPE_MAX;
  remote_major_snapshot_version_ = 0;
  remote_last_replay_log_id_ = 0;
}

bool ObFetchReplicaInfoRes::is_valid() const
{
  return pg_key_.is_valid() && remote_minor_snapshot_version_ >= 0 &&
         ObReplicaTypeCheck::is_replica_type_valid(remote_replica_type_) && remote_major_snapshot_version_ >= 0 &&
         remote_last_replay_log_id_ >= 0;
}

OB_SERIALIZE_MEMBER(ObBatchFetchReplicaInfoArg, replica_info_arg_);
void ObBatchFetchReplicaInfoArg::reset()
{
  replica_info_arg_.reset();
}

int ObBatchFetchReplicaInfoArg::assign(const ObBatchFetchReplicaInfoArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replica_info_arg_.assign(arg.replica_info_arg_))) {
    STORAGE_LOG(WARN, "failed to assign replica info arg", K(ret), K(arg));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchFetchReplicaInfoRes, replica_info_res_);
void ObBatchFetchReplicaInfoRes::reset()
{
  replica_info_res_.reset();
}

int ObBatchFetchReplicaInfoRes::assign(const ObBatchFetchReplicaInfoRes& res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replica_info_res_.assign(res.replica_info_res_))) {
    STORAGE_LOG(WARN, "failed to assign replica info res", K(ret), K(res));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSuspendPartitionArg, pg_key_, mig_dest_server_, need_force_change_owner_, is_batch_);

void ObSuspendPartitionArg::reset()
{
  pg_key_.reset();
  mig_dest_server_.reset();
  need_force_change_owner_ = false;
  is_batch_ = false;
}

bool ObSuspendPartitionArg::is_valid() const
{
  return pg_key_.is_valid() && mig_dest_server_.is_valid();
}

OB_SERIALIZE_MEMBER(ObSuspendPartitionRes, pg_key_, max_clog_id_);

void ObSuspendPartitionRes::reset()
{
  pg_key_.reset();
  max_clog_id_ = OB_INVALID_TIMESTAMP;
}

bool ObSuspendPartitionRes::is_valid() const
{
  return pg_key_.is_valid() && OB_INVALID_TIMESTAMP != max_clog_id_;
}

OB_SERIALIZE_MEMBER(ObHandoverPartitionArg, type_, pg_key_, src_file_id_, candidate_server_);

void ObHandoverPartitionArg::reset()
{
  type_ = PARTITION_HANDOVER_TYPE_INVALID;
  pg_key_.reset();
  src_file_id_ = 0;
  candidate_server_.reset();
}

bool ObHandoverPartitionArg::is_valid() const
{
  return type_ > PARTITION_HANDOVER_TYPE_INVALID && type_ < PARTITION_HANDOVER_TYPE_MAX && pg_key_.is_valid() &&
         (PARTITION_HANDOVER_TYPE_MIGRATE_OUT == type_ ? src_file_id_ > 0 : true) && candidate_server_.is_valid();
}

// 1.4x old rpc to fetch store info
int ObPTSFetchInfoP::process()
{
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(WARN, "ob server 2.x do not support 1.4x rpc", K(ret));
  return ret;
}

int ObPTSAddMemberP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(partition_service_->add_replica_mc(arg_, result_))) {
    STORAGE_LOG(WARN, "add replica mc fail", K_(arg), K(ret));
  } else {
    STORAGE_LOG(TRACE, "add replica mc successfully", K_(arg), K_(result));
  }
  return ret;
}

int ObPTSRemoveMemberP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_service_->remove_replica_mc(arg_, result_))) {
    STORAGE_LOG(WARN, "remove replica mc fail", K_(arg), K(ret));
  } else {
    STORAGE_LOG(TRACE, "remove replica mc successfully", K_(arg), K_(result));
  }
  return ret;
}

int ObPTSRemoveReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_service_->remove_replica(arg_.pkey_, arg_.replica_member_))) {
    STORAGE_LOG(WARN, "remove replica fail", K_(arg), K(ret));
  } else {
    STORAGE_LOG(TRACE, "remove replica successfully", K_(arg));
  }
  return ret;
}

int ObBatchRemoveReplicaP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(partition_service_->batch_remove_replica(arg_))) {
    STORAGE_LOG(WARN, "batch remove replica fail", K(ret), K_(arg));
  } else {
    STORAGE_LOG(TRACE, "remove replica successfully", K_(arg));
  }

  return ret;
}

int ObIsMemberChangeDoneP::process()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = partition_service_->is_member_change_done(arg_.key_, arg_.log_id_, arg_.timestamp_))) {
    if (OB_EAGAIN != ret && OB_MEMBER_CHANGE_FAILED != ret) {
      STORAGE_LOG(WARN, "failed to check is member change done", K(ret), K_(arg));
    }
  } else {
    STORAGE_LOG(TRACE, "get is member change done", K_(arg));
  }
  return ret;
}

int ObWarmUpRequestP::process()
{
  int ret = OB_SUCCESS;
  if (NULL == partition_service_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service must not null", K(ret));
  } else if (OB_SUCCESS != (ret = partition_service_->do_warm_up_request(arg_, this->get_receive_timestamp()))) {
    STORAGE_LOG(WARN, "failed to do warm up request", K(ret));
  }
  return ret;
}

int ObSplitDestPartitionRequestP::process()
{
  int ret = OB_SUCCESS;
  if (NULL == ps_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service must not null", K(ret));
  } else if (OB_FAIL(ps_->handle_split_dest_partition_request(arg_, result_))) {
    STORAGE_LOG(WARN, "handle split dest partition request failed", K(ret));
  } else {
    STORAGE_LOG(INFO, "handle split dest partition request success", K(arg_));
  }
  return ret;
}

int ObReplicaSplitProgressRequestP::process()
{
  int ret = OB_SUCCESS;
  if (NULL == ps_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service must not null", K(ret));
  } else if (OB_FAIL(ps_->handle_replica_split_progress_request(arg_, result_))) {
    STORAGE_LOG(WARN, "handle replica split progress request failed", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "handle replica split progress request success", K(arg_));
  }
  return ret;
}

int ObSplitDestPartitionRPCCB::process()
{
  int ret = OB_SUCCESS;
  const ObSplitDestPartitionResult& result = result_;
  const ObAddr& dst = dst_;
  ObRpcResultCode& rcode = rcode_;

  if (OB_SUCCESS != rcode.rcode_) {
    STORAGE_LOG(WARN, "split dest partition rpc callback failed", K(rcode), K(dst));
  } else {
    if (OB_FAIL(ps_->handle_split_dest_partition_result(result))) {
      STORAGE_LOG(WARN, "handle split dest partition result failed", K(ret), K(result));
    } else {
      STORAGE_LOG(INFO, "handle split dest partition result success", K(result));
    }
  }
  return ret;
}

void ObSplitDestPartitionRPCCB::on_timeout()
{
  STORAGE_LOG(WARN, "split dest partition request callback timeout", K(dst_));
}

int ObReplicaSplitProgressRPCCB::process()
{
  int ret = OB_SUCCESS;
  const ObReplicaSplitProgressResult& result = result_;
  const ObAddr& dst = dst_;
  ObRpcResultCode& rcode = rcode_;

  if (OB_SUCCESS != rcode.rcode_) {
    STORAGE_LOG(WARN, "replica split progress rpc callback failed", K(rcode), K(dst));
  } else if (OB_FAIL(ps_->handle_replica_split_progress_result(result))) {
    STORAGE_LOG(WARN, "handle replica split progress result failed", K(ret), K(result));
  } else {
    STORAGE_LOG(DEBUG, "handle replica split progress result success", K(result));
  }
  return ret;
}

void ObReplicaSplitProgressRPCCB::on_timeout()
{
  STORAGE_LOG(WARN, "replica split state request callback timeout", K(dst_));
}

int ObGetMemberListP::process()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = partition_service_->get_leader_curr_member_list(arg_, result_))) {
    STORAGE_LOG(WARN, "handle get leader member list error", K(ret), "msg", arg_);
  } else {
    // do nothing
  }
  return ret;
}

int ObCheckMemberMajorSSTableEnoughP::process()
{
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(WARN, "do not support old rpc, need retry", K(ret));
  return ret;
}

int ObCheckMemberPGMajorSSTableEnoughP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service has not been inited", K(ret));
  } else if (OB_FAIL(partition_service_->check_member_pg_major_sstable_enough(arg_.pkey_, arg_.table_ids_))) {
    STORAGE_LOG(WARN, "fail to check member major sstable enough", K(ret));
  }
  return ret;
}

int ObFetchReplicaInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service has not been inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.replica_info_arg_.count(); ++i) {
      const ObFetchReplicaInfoArg& arg = arg_.replica_info_arg_.at(i);
      ObIPartitionGroupGuard guard;
      ObIPartitionGroup* partition = NULL;
      ObPartitionGroupMeta meta;

      if (!arg.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "replica info arg is invalid", K(ret), K(arg));
      } else if (OB_FAIL(partition_service_->get_partition(arg.pg_key_, guard))) {
        STORAGE_LOG(WARN, "failed to get partition", K(ret), K(arg));
      } else if (OB_ISNULL(partition = guard.get_partition_group())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "partition group should not be NULL", K(ret), KP(partition));
      } else if (OB_FAIL(partition->get_pg_storage().get_pg_meta(meta))) {
        STORAGE_LOG(WARN, "failed to get pg meta", K(ret), K(arg));
      } else {
        ObFetchReplicaInfoRes res;
        res.pg_key_ = meta.pg_key_;
        res.remote_minor_snapshot_version_ = meta.storage_info_.get_data_info().get_publish_version();
        res.remote_replica_type_ = meta.replica_type_;
        res.remote_major_snapshot_version_ = meta.report_status_.snapshot_version_;
        res.remote_last_replay_log_id_ = meta.storage_info_.get_data_info().get_last_replay_log_id();
        if (OB_FAIL(result_.replica_info_res_.push_back(res))) {
          STORAGE_LOG(WARN, "failed to push res into array", K(ret), K(arg), K(ret));
        }
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
ObCommonPartitionServiceRpcP<RPC_CODE>::ObCommonPartitionServiceRpcP(
    storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : partition_service_(partition_service),
      bandwidth_throttle_(bandwidth_throttle),
      last_send_time_(0),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE)
{}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObCommonPartitionServiceRpcP<RPC_CODE>::fill_data(const Data& data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migrate data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (serialization::encoded_length(data) > this->result_.get_remain()) {
    LOG_INFO("flush", K(this->result_));
    if (OB_FAIL(flush_and_wait())) {
      STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode(
            this->result_.get_data(), this->result_.get_capacity(), this->result_.get_position(), data))) {
      STORAGE_LOG(WARN, "failed to encode", K(ret));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObCommonPartitionServiceRpcP<RPC_CODE>::fill_buffer(blocksstable::ObBufferReader& data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migrate data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    while (OB_SUCC(ret) && data.remain() > 0) {
      if (0 == this->result_.get_remain()) {
        if (OB_FAIL(flush_and_wait())) {
          STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
        }
      } else {
        int64_t fill_length = std::min(this->result_.get_remain(), data.remain());
        if (fill_length <= 0) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "fill_length must larger than 0", K(ret), K(fill_length), K(this->result_), K(data));
        } else {
          MEMCPY(this->result_.get_cur_pos(), data.current(), fill_length);
          this->result_.get_position() += fill_length;
          if (OB_FAIL(data.advance(fill_length))) {
            STORAGE_LOG(WARN, "failed to advance fill length", K(ret), K(fill_length), K(data));
          }
        }
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObCommonPartitionServiceRpcP<RPC_CODE>::fill_data_list(ObIArray<Data>& data_list)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migrate data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_list.count(); ++i) {
      Data& data = data_list.at(i);
      if (data.get_serialize_size() > this->result_.get_remain()) {
        if (OB_FAIL(flush_and_wait())) {
          STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(
                data.serialize(this->result_.get_data(), this->result_.get_capacity(), this->result_.get_position()))) {
          STORAGE_LOG(WARN, "failed to encode data", K(ret));
        } else {
          STORAGE_LOG(DEBUG, "fill data", K(data), K(this->result_));
        }
      }
    }
  }
  return ret;
}
template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObCommonPartitionServiceRpcP<RPC_CODE>::fill_data_immediate(const Data& data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migrate data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (serialization::encoded_length(data) > this->result_.get_remain()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "data length is larger than result get_remain size, can not send",
        K(ret),
        K(serialization::encoded_length(data)),
        K(serialization::encoded_length(data)));
  } else if (OB_FAIL(serialization::encode(
                 this->result_.get_data(), this->result_.get_capacity(), this->result_.get_position(), data))) {
    STORAGE_LOG(WARN, "failed to encode", K(ret));

  } else if (OB_FAIL(flush_and_wait())) {
    STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
  } else {
    LOG_INFO("flush", K(this->result_));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObCommonPartitionServiceRpcP<RPC_CODE>::flush_and_wait()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (NULL == bandwidth_throttle_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "bandwidth_throttle_ must not null", K(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = bandwidth_throttle_->limit_out_and_sleep(
                           this->result_.get_position(), last_send_time_, max_idle_time))) {
      STORAGE_LOG(WARN, "failed limit out band", K(tmp_ret));
    }

    if (OB_FAIL(this->flush(OB_DEFAULT_STREAM_WAIT_TIMEOUT))) {
      STORAGE_LOG(WARN, "failed to flush", K(ret));
    } else {
      this->result_.get_position() = 0;
      last_send_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObCommonPartitionServiceRpcP<RPC_CODE>::alloc_buffer()
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!this->result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
ObLogicPartitionServiceRpcP<RPC_CODE>::ObLogicPartitionServiceRpcP(
    storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : partition_service_(partition_service),
      bandwidth_throttle_(bandwidth_throttle),
      last_send_time_(0),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      rpc_header_(),
      header_encode_offset_(0)
{
  header_encode_length_ = serialization::encoded_length(rpc_header_);
}

template <ObRpcPacketCode RPC_CODE>
ObLogicPartitionServiceRpcP<RPC_CODE>::~ObLogicPartitionServiceRpcP()
{}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObLogicPartitionServiceRpcP<RPC_CODE>::fill_data(const Data& data)
{
  int ret = OB_SUCCESS;
  int64_t encode_length = serialization::encoded_length(data);

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migrate data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (encode_length > this->result_.get_remain()) {
    LOG_DEBUG("flush", K(this->result_));
    if (OB_FAIL(do_flush())) {
      STORAGE_LOG(WARN, "fail to do flush", K(ret), K(rpc_header_), K(this->result_));
    } else if (this->result_.get_capacity() < encode_length + header_encode_length_ &&
               OB_FAIL(extend_buffer(encode_length + header_encode_length_))) {
      STORAGE_LOG(WARN, "Failed to extend buffer", K(encode_length), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode(
            this->result_.get_data(), this->result_.get_capacity(), this->result_.get_position(), data))) {
      STORAGE_LOG(WARN, "failed to encode", K(ret));
    } else if (OB_FAIL(update_header(encode_length))) {
      STORAGE_LOG(WARN, "fail to update rpc header", K(ret), K(rpc_header_));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::extend_buffer(const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  char* new_buf = NULL;

  if (buf_size >= OB_MAX_PACKET_BUFFER_LENGTH) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected large row to migrate", K(this->result_), K(ret));
  } else if (this->result_.get_capacity() >= buf_size) {
  } else {
    int64_t new_buf_size = min(OB_MAX_PACKET_BUFFER_LENGTH, upper_align(buf_size, OB_MALLOC_BIG_BLOCK_SIZE) * 2);
    if (OB_ISNULL(new_buf = reinterpret_cast<char*>(allocator_.alloc(new_buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
    } else if (OB_ISNULL(this->result_.get_data())) {
      this->result_.set_data(new_buf, new_buf_size);
    } else {
      int64_t pos = this->result_.get_position();
      MEMCPY(new_buf, this->result_.get_data(), pos);
      allocator_.free(this->result_.get_data());
      this->result_.set_data(new_buf, new_buf_size);
      this->result_.get_position() = pos;
      rpc_header_.occupy_size_ = static_cast<int32_t>(this->result_.get_capacity());
    }
  }

  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::reserve_header()
{
  int ret = OB_SUCCESS;
  if (NULL == (this->result_.get_data())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc migrate data buffer", K(ret));
  } else {
    rpc_header_.reset();
    header_encode_offset_ = this->result_.get_position();
    this->result_.get_position() = header_encode_length_;
    rpc_header_.occupy_size_ = static_cast<int32_t>(this->result_.get_capacity());
    rpc_header_.data_offset_ = static_cast<int32_t>(header_encode_length_);
    rpc_header_.header_size_ = sizeof(ObLogicMigrateRpcHeader);
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::encode_header()
{
  int ret = OB_SUCCESS;
  if (!rpc_header_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rpc header is invalid", K(ret), K(rpc_header_));
  } else if (OB_FAIL(serialization::encode(
                 this->result_.get_data(), this->result_.get_capacity(), header_encode_offset_, rpc_header_))) {
    STORAGE_LOG(WARN, "failed to encode", K(ret));
  } else if (header_encode_offset_ != header_encode_length_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to encode header", K(header_encode_offset_), K(header_encode_length_), K(rpc_header_));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::update_header(const int64_t data_size)
{
  int ret = OB_SUCCESS;
  if (data_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update header get invalid arugment", K(ret), K(data_size));
  } else {
    rpc_header_.data_size_ += static_cast<int32_t>(data_size);
    ++rpc_header_.object_count_;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::do_flush()
{
  int ret = OB_SUCCESS;
  const ObLogicMigrateRpcHeader::ConnectStatus connect_status = ObLogicMigrateRpcHeader::ConnectStatus::KEEPCONNECT;
  if (OB_FAIL(set_connect_status(connect_status))) {
    STORAGE_LOG(WARN, "fail to set connect status", K(ret), K(rpc_header_));
  } else if (OB_FAIL(encode_header())) {
    STORAGE_LOG(WARN, "fail to encode header", K(ret));
  } else if (OB_FAIL(flush_and_wait())) {
    STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
  } else if (OB_FAIL(reserve_header())) {
    STORAGE_LOG(WARN, "fail to reserve header", K(ret), K(rpc_header_), K(this->result_));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::flush_and_wait()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (NULL == bandwidth_throttle_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "bandwidth_throttle_ must not null", K(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = bandwidth_throttle_->limit_out_and_sleep(
                           this->result_.get_position(), last_send_time_, max_idle_time))) {
      STORAGE_LOG(WARN, "failed limit out band", K(tmp_ret));
    }

    if (OB_FAIL(this->flush(OB_DEFAULT_STREAM_WAIT_TIMEOUT))) {
      STORAGE_LOG(WARN, "failed to flush", K(ret));
    } else {
      this->result_.get_position() = 0;
      last_send_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObLogicPartitionServiceRpcP<RPC_CODE>::set_connect_status(
    const ObLogicMigrateRpcHeader::ConnectStatus connect_status)
{
  int ret = OB_SUCCESS;
  if (connect_status <= ObLogicMigrateRpcHeader::ConnectStatus::INVALID_STATUS ||
      connect_status >= ObLogicMigrateRpcHeader::ConnectStatus::MAX) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "connect status is invalid", K(ret), K(connect_status));
  } else {
    rpc_header_.connect_status_ = connect_status;
  }
  return ret;
}

ObFetchBaseDataMetaP::ObFetchBaseDataMetaP(
    storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
{
  UNUSED(partition_service);
  UNUSED(bandwidth_throttle);
}

ObFetchBaseDataMetaP::~ObFetchBaseDataMetaP()
{}

int ObFetchBaseDataMetaP::process()
{
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(WARN, "observer 2.x do not support 1.4x rpc", K(ret));
  return ret;
}

ObFetchMacroBlockOldP::ObFetchMacroBlockOldP(
    storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchMacroBlockOldP::process()
{
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(WARN, "observer 2.x do not support 1.4x rpc", K(ret));
  return ret;
}

ObFetchMacroBlockP::ObFetchMacroBlockP(
    storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle), total_macro_block_count_(0)
{}

int ObFetchMacroBlockP::process()
{
  int ret = OB_SUCCESS;
  storage::ObPartitionMacroBlockObProducer producer;
  ObFullMacroBlockMeta meta;
  blocksstable::ObBufferReader data;
  char* buf = NULL;
  last_send_time_ = ObTimeUtility::current_time();

  DEBUG_SYNC(FETCH_MACRO_BLOCK);
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  } else if (OB_ISNULL(partition_service_) || OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
        "bandwidth_throttle_ and partition_service_ must not null",
        K(ret),
        KP_(partition_service),
        KP_(bandwidth_throttle));
  } else if (OB_FAIL(producer.init(arg_.table_key_, arg_.arg_list_))) {
    STORAGE_LOG(WARN, "failed to init producer", K(ret), K(arg_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(producer.get_next_macro_block(meta, data))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next macro block", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (!meta.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "meta must not null", K(ret));
      } else if (OB_FAIL(fill_data(ObFullMacroBlockMetaEntry(const_cast<ObMacroBlockMetaV2&>(*meta.meta_),
                     const_cast<ObMacroBlockSchemaInfo&>(*meta.schema_))))) {
        STORAGE_LOG(WARN, "fail to fill macro block data", K(ret));
      } else if (OB_FAIL(fill_buffer(data))) {
        STORAGE_LOG(WARN, "failed to fill data", K(ret));
      } else {
        STORAGE_LOG(INFO,
            "succeed to fill macro block",
            "idx",
            total_macro_block_count_,
            "version",
            meta.meta_->data_version_,
            "seq",
            meta.meta_->data_seq_,
            K(result_),
            K(meta));
        ++total_macro_block_count_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (total_macro_block_count_ != arg_.arg_list_.count()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "macro block count not match", K(ret), K(total_macro_block_count_), K(arg_.arg_list_.count()));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFetchPartitionInfoArg, pkey_, replica_type_);
OB_SERIALIZE_MEMBER(ObFetchTableInfoArg, pkey_, table_id_, snapshot_version_, is_only_major_sstable_);

ObFetchTableInfoResult::ObFetchTableInfoResult()
    : table_keys_(), multi_version_start_(0), is_ready_for_read_(false), gc_table_keys_()
{}

void ObFetchTableInfoResult::reset()
{
  table_keys_.reset();
  multi_version_start_ = 0;
  is_ready_for_read_ = false;
  gc_table_keys_.reset();
}

OB_SERIALIZE_MEMBER(ObFetchTableInfoResult, table_keys_, multi_version_start_, is_ready_for_read_, gc_table_keys_);
OB_SERIALIZE_MEMBER(ObSplitDestPartitionResult, status_, progress_, schema_version_, src_pkey_, dest_pkey_);
OB_SERIALIZE_MEMBER(ObFetchLogicBaseMetaArg, table_key_, task_count_);
OB_SERIALIZE_MEMBER(ObFetchPhysicalBaseMetaArg, table_key_);
OB_SERIALIZE_MEMBER(ObFetchLogicRowArg, table_key_, key_range_, schema_version_, data_checksum_);
OB_SERIALIZE_MEMBER(ObSplitDestPartitionRequestArg, dest_pkey_, split_info_);
OB_SERIALIZE_MEMBER(ObReplicaSplitProgressRequest, schema_version_, pkey_, addr_);
OB_SERIALIZE_MEMBER(ObReplicaSplitProgressResult, pkey_, addr_, progress_);

OB_UNIS_DEF_SERIALIZE(ObFetchPartitionInfoResult, meta_, table_id_list_, major_version_, is_log_sync_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObFetchPartitionInfoResult, meta_, table_id_list_, major_version_, is_log_sync_);

int ObFetchPartitionInfoResult::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, version))) {
    LOG_WARN("fail to decode unis_version", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, len))) {
    LOG_WARN("fail to decode len", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(len < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decode length is negative", K(ret), K(len), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(data_len < len + pos)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("buf length not enough", K(ret), K(len), KP(buf), K(data_len), K(pos));
  } else if (OB_FETCH_PARTITION_INFO_RESULT_VERSION_V2 == version) {  // normal decode
    LST_DO_CODE(OB_UNIS_DECODE, meta_, table_id_list_, major_version_, is_log_sync_);
  } else if (OB_FETCH_PARTITION_INFO_RESULT_VERSION_V1 == version) {
    ObPartitionStoreMeta old_meta;
    LST_DO_CODE(OB_UNIS_DECODE, old_meta, table_id_list_, major_version_, is_log_sync_);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(meta_.copy_from_old_meta(old_meta))) {
        LOG_WARN("fail to convert old meta", K(ret), K(old_meta));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported unis_version", K(ret), K(version), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

void ObFetchPartitionInfoResult::reset()
{
  meta_.reset();
  table_id_list_.reset();
  major_version_ = 0;
  is_log_sync_ = false;
}

int ObFetchPartitionInfoResult::assign(const ObFetchPartitionInfoResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_.deep_copy(result.meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(result));
  } else if (OB_FAIL(table_id_list_.assign(result.table_id_list_))) {
    STORAGE_LOG(WARN, "fail to assgin table id list", K(ret), K(result));
  } else {
    major_version_ = result.major_version_;
    is_log_sync_ = result.is_log_sync_;
  }
  return ret;
}

// 2.0 and 2.1 rpc, 2.2 not use it
int ObFetchPartitionInfoP::process()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("this rpc server not use it again", K(ret));
  return ret;
}

int ObFetchTableInfoP::process()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("FetchTableInfo rpc is not supported by this server", K(ret));
  return ret;
}

ObFetchLogicBaseMetaP::ObFetchLogicBaseMetaP(
    ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchLogicBaseMetaP::process()
{
  int ret = OB_SUCCESS;
  ObLogicBaseMetaProducer producer;
  ObArray<ObStoreRowkey> endkey_list;
  char* buf = NULL;
  int64_t start_ts = ObTimeUtility::current_time();

  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  } else if (OB_FAIL(producer.init(partition_service_, &arg_))) {
    STORAGE_LOG(WARN, "init failed", K(ret));
  } else if (OB_FAIL(producer.get_logic_endkey_list(endkey_list))) {
    STORAGE_LOG(WARN, "failed to get logic table meta", K(ret));
  } else if (OB_FAIL(fill_data(endkey_list.count()))) {
    STORAGE_LOG(WARN, "failed to encode endkey_list count", K(ret));
  } else if (OB_FAIL(fill_data_list(endkey_list))) {
    STORAGE_LOG(WARN, "failed to encode endkey_list", K(ret));
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("complete fetch logic base meta", K(ret), K(cost_ts), K(arg_));
  return ret;
}

ObFetchPhysicalBaseMetaP::ObFetchPhysicalBaseMetaP(
    ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchPhysicalBaseMetaP::process()
{
  int ret = OB_SUCCESS;
  ObPhysicalBaseMetaProducer producer;
  ObSSTableBaseMeta sstable_meta(allocator_);
  common::ObArray<blocksstable::ObSSTablePair> macro_block_list;
  char* buf = NULL;

  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  } else if (OB_FAIL(producer.init(partition_service_, &arg_))) {
    STORAGE_LOG(WARN, "init failed", K(ret));
  } else if (OB_FAIL(producer.get_sstable_meta(sstable_meta, macro_block_list))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret));
  } else if (OB_FAIL(fill_data(sstable_meta))) {
    STORAGE_LOG(WARN, "failed to encode sstable meta", K(ret));
  } else if (OB_FAIL(fill_data(macro_block_list.count()))) {
    STORAGE_LOG(WARN, "failed to encode macro_block_list count", K(ret));
  } else if (OB_FAIL(fill_data_list(macro_block_list))) {
    STORAGE_LOG(WARN, "failed to encode macro_block_list", K(ret));
  }
  return ret;
}

int ObBatchRemoveMemberP::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObChangeMemberArgs& arg = arg_;
  obrpc::ObChangeMemberCtxsWrapper& result = result_;

  if (OB_ISNULL(partition_service_) || OB_ISNULL(partition_service_->get_clog_mgr())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "partition_service_ or clog_mgr must not null", K(ret), KP(partition_service_));
  } else if (OB_SUCCESS !=
             (result.result_code_ = partition_service_->get_clog_mgr()->batch_remove_member(arg, result.ctxs_))) {
    STORAGE_LOG(WARN, "failed to batch_remove_member", K(result.result_code_), K(arg));
  } else {
    STORAGE_LOG(INFO, "succeed to batch_remove_member", K(result));
  }
  return ret;
}

int ObBatchAddMemberP::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObChangeMemberArgs& arg = arg_;
  obrpc::ObChangeMemberCtxsWrapper& result = result_;
  if (OB_ISNULL(partition_service_) || OB_ISNULL(partition_service_->get_clog_mgr())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "partition_service_ or clog_mgr must not null", K(ret), KP(partition_service_));
  } else if (OB_SUCCESS !=
             (result.result_code_ = partition_service_->get_clog_mgr()->batch_add_member(arg, result.ctxs_))) {
    STORAGE_LOG(WARN, "failed to batch_add_member", K(result.result_code_), K(arg));
  } else {
    STORAGE_LOG(INFO, "succeed to batch_add_member", K(result));
  }
  return ret;
}

int ObBatchMemberChangeDoneP::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObChangeMemberCtxs& arg = arg_;
  obrpc::ObChangeMemberCtxsWrapper& result = result_;

  if (OB_ISNULL(partition_service_) || OB_ISNULL(partition_service_->get_clog_mgr())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "partition_service_ or clog_mgr must not null", K(ret), KP(partition_service_));
  } else if (OB_FAIL(result.ctxs_.assign(arg))) {
    STORAGE_LOG(WARN, "failed to copy ObChangeMemberCtxs", K(ret), K(arg));
  } else if (OB_SUCCESS !=
             (result.result_code_ = partition_service_->get_clog_mgr()->batch_is_member_change_done(result.ctxs_))) {
    STORAGE_LOG(WARN, "failed to batch_is_member_change_done", K(result));
  } else {
    STORAGE_LOG(INFO, "succeed to batch_is_member_change_done", K(result));
  }
  return ret;
}

/**
 * ---------------------------------------------ObLogicRow----------------------------------------------------------------------
 */

ObFetchLogicRowP::ObFetchLogicRowP(
    ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchLogicRowP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // get tables and init ms row iter
  char* buf = NULL;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObFetchLogicRowArg deep_copy_arg;
  ObFetchLogicRowInfo row_info;

  ObTaskController::get().allow_next_syslog();
  STORAGE_LOG(INFO, "start fetch logic row", K_(arg));
  DEBUG_SYNC(DEFORE_FETCH_LOGIC_ROW_SRC);

  SMART_VAR(ObLogicRowProducer, logic_row_producer)
  {
    CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, arg_.table_key_.table_id_)
    {
      if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
      } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed set data to result", K(ret));
      } else if (OB_ISNULL(partition_service_) || OB_ISNULL(bandwidth_throttle_)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "init get invalid argument", K(ret), KP(partition_service_), K(bandwidth_throttle_));
      } else if (OB_FAIL(arg_.deep_copy(allocator_, deep_copy_arg))) {
        STORAGE_LOG(WARN, "fail to deep copy fetch logic row arg", K(ret), K_(arg));
      } else if (OB_FAIL(logic_row_producer.init(partition_service_, &deep_copy_arg))) {
        STORAGE_LOG(WARN, "fail to init logic row producer", K(ret));
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        const ObStoreRow* store_row = NULL;

        while (OB_SUCC(ret)) {
          if (OB_FAIL(logic_row_producer.get_next_row(store_row))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "failed to get next row", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
            break;
          } else if (NULL == store_row) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "store row must not be NULL", K(ret), KP(store_row));
          } else {
            if (OB_SUCCESS != (tmp_ret = row_info.add_row(*store_row))) {
              LOG_WARN("failed to add row info", K(ret), K(*store_row));
            }

            if (OB_FAIL(fill_data(*store_row))) {
              STORAGE_LOG(WARN, "failed to fill data", K(ret));
            } else if (store_row->row_type_flag_.is_last_multi_version_row() &&
                       logic_row_producer.need_reset_logic_row_iter()) {
              if (OB_FAIL(logic_row_producer.reset_logical_row_iter())) {
                STORAGE_LOG(WARN, "fail to reset logical row iter", K(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            store_row = NULL;
          }
        }

        if (OB_SUCC(ret)) {
          int64_t data_checksum = logic_row_producer.get_data_checksum();
          if (data_checksum != deep_copy_arg.data_checksum_) {
            STORAGE_LOG(WARN, "data checksum is not equal", K(data_checksum), K(deep_copy_arg.data_checksum_));
          }
        }
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  ObTaskController::get().allow_next_syslog();
  STORAGE_LOG(INFO, "finish fetch logic row", K(ret), K(cost_ts), K(deep_copy_arg), K(row_info));
  return ret;
}

ObFetchLogicDataChecksumP::ObFetchLogicDataChecksumP(
    ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchLogicDataChecksumP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t start_time = start_ts;
  int64_t end_time = 0;
  char* buf = NULL;
  int64_t data_checksum = 0;
  bool is_finish = false;
  const int64_t LIMIT_OF_SEND = 15 * 1000 * 1000;
  ObFetchLogicRowArg deep_copy_arg;
  ObFetchLogicRowInfo row_info;

  ObTaskController::get().allow_next_syslog();
  STORAGE_LOG(INFO, "start to calc logic data checksum", K_(arg));

  SMART_VAR(ObLogicRowProducer, logic_row_producer)
  {
    CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, arg_.table_key_.table_id_)
    {
      if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
      } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed set data to result", K(ret));
      } else if (OB_ISNULL(partition_service_) || OB_ISNULL(bandwidth_throttle_)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "init get invalid argument", K(ret), KP(partition_service_), K(bandwidth_throttle_));
      } else if (OB_FAIL(arg_.deep_copy(allocator_, deep_copy_arg))) {
        STORAGE_LOG(WARN, "fail to deep copy fetch logic row arg", K(ret), K_(arg));
      } else if (OB_FAIL(logic_row_producer.init(partition_service_, &deep_copy_arg))) {
        STORAGE_LOG(WARN, "fail to init logic row producer", K(ret));
      } else {
        LOG_INFO("succeed to init logic row producer");
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        const ObStoreRow* store_row = NULL;

        while (OB_SUCC(ret)) {
          if (OB_FAIL(logic_row_producer.get_next_row(store_row))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "failed to get next row", K(ret));
            }
            break;
          } else if (NULL == store_row) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "store row must not be NULL", K(ret), KP(store_row));
          } else {
            end_time = ObTimeUtility::current_time();

            if (OB_SUCCESS != (tmp_ret = row_info.add_row(*store_row))) {
              LOG_WARN("failed to add row info", K(ret), K(*store_row));
            }

            if ((end_time - start_time) >= LIMIT_OF_SEND || logic_row_producer.need_reset_logic_row_iter()) {
              if (store_row->row_type_flag_.is_last_multi_version_row()) {
                if (OB_FAIL(logic_row_producer.reset_logical_row_iter())) {
                  STORAGE_LOG(WARN, "fail to reset logica row iter", K(ret));
                }
              }
              if (OB_FAIL(ret)) {
              } else {
                data_checksum = 0;
                is_finish = false;
                start_time = end_time;
                if (OB_FAIL(fill_data(is_finish))) {
                  STORAGE_LOG(WARN, "failed to fill data", K(ret), K(is_finish));
                } else if (OB_FAIL(fill_data_immediate(data_checksum))) {
                  STORAGE_LOG(WARN, "failed to fill data", K(ret), K(data_checksum));
                }
              }
            }
          }

          if (OB_SUCC(ret)) {
            store_row = NULL;
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_finish = true;
        data_checksum = logic_row_producer.get_data_checksum();

        if (OB_FAIL(fill_data(is_finish))) {
          STORAGE_LOG(WARN, "failed to fill data", K(ret), K(is_finish));
        } else if (OB_FAIL(fill_data(data_checksum))) {
          STORAGE_LOG(WARN, "failed to fill data", K(ret), K(data_checksum));
        }
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("complete fetch logic row checksum", K(ret), K(cost_ts), K(deep_copy_arg), K(row_info));
  return ret;
}

ObFetchLogicDataChecksumSliceP::ObFetchLogicDataChecksumSliceP(
    ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObLogicPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchLogicDataChecksumSliceP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t HALF_OF_TIMEOUT = MAX_LOGIC_MIGRATE_TIME_OUT / 2;  // 15s
  char* buf = NULL;
  int64_t schema_rowkey_cnt = 0;
  int64_t start_time = start_ts;
  int64_t end_time = 0;
  ObLogicMigrateRpcHeader::ConnectStatus connect_status = ObLogicMigrateRpcHeader::ConnectStatus::INVALID_STATUS;
  ObFetchLogicRowArg deep_copy_arg;
  ObFetchLogicRowInfo row_info;

  ObTaskController::get().allow_next_syslog();
  STORAGE_LOG(INFO, "start to calc logic data checksum", K_(arg));
  SMART_VAR(ObLogicRowProducer, logic_row_producer)
  {
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(partition_service_) || OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "init get invalid argument", K(ret), KP(partition_service_), K(bandwidth_throttle_));
    } else if (OB_FAIL(arg_.deep_copy(allocator_, deep_copy_arg))) {
      STORAGE_LOG(WARN, "fail to deep copy fetch logic row arg", K(ret), K_(arg));
    } else if (OB_FAIL(logic_row_producer.init(partition_service_, &deep_copy_arg))) {
      STORAGE_LOG(WARN, "fail to init logic row producer", K(ret), K(deep_copy_arg));
    } else if (OB_FAIL(reserve_header())) {
      STORAGE_LOG(WARN, "fail to reserve header", K(ret), K(deep_copy_arg));
    } else if (OB_FAIL(logic_row_producer.get_schema_rowkey_count(schema_rowkey_cnt))) {
      STORAGE_LOG(WARN, "fail to get schema rowkey count", K(ret), K(schema_rowkey_cnt));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to init logic row producer", "arg", deep_copy_arg);
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      const ObStoreRow* store_row = NULL;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(logic_row_producer.get_next_row(store_row))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next row", K(ret));
          }
          break;
        } else if (NULL == store_row) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "store row must not be NULL", K(ret), KP(store_row));
        } else {
          end_time = ObTimeUtility::current_time();
          if (OB_SUCCESS != (tmp_ret = row_info.add_row(*store_row))) {
            LOG_WARN("failed to add row info", K(ret), K(*store_row));
          }
          if ((end_time - start_time) >= MAX_RECONNECTION_INTERVAL) {
            if (store_row->row_type_flag_.is_last_multi_version_row()) {
              connect_status = ObLogicMigrateRpcHeader::ConnectStatus::RECONNECT;
              if (OB_FAIL(set_data_checksum_protocol(true, /*is_rowkey_valid*/
                      schema_rowkey_cnt,
                      logic_row_producer.get_data_checksum(),
                      store_row))) {
                STORAGE_LOG(WARN, "fail to set data checksum protol", K(ret));
              } else {

                break;
              }
            }
          }

          if (OB_SUCC(ret) && logic_row_producer.need_reset_logic_row_iter()) {
            if (store_row->row_type_flag_.is_last_multi_version_row()) {
              if (OB_FAIL(logic_row_producer.reset_logical_row_iter())) {
                STORAGE_LOG(WARN, "fail to reset logic row iter", K(ret));
              }
            }
          }

          if (OB_SUCC(ret) && THIS_WORKER.get_timeout_remain() <= HALF_OF_TIMEOUT) {

            if (OB_FAIL(do_flush())) {
              STORAGE_LOG(WARN, "fail to do flush", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          store_row = NULL;
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      connect_status = ObLogicMigrateRpcHeader::ConnectStatus::ENDCONNECT;
      if (OB_FAIL(set_data_checksum_protocol(false, /*is_rowkey_valid*/
              schema_rowkey_cnt,
              logic_row_producer.get_data_checksum(),
              NULL))) {
        STORAGE_LOG(WARN, "fail to set data checksum protol", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_connect_status(connect_status))) {
        STORAGE_LOG(WARN, "fail to set connect status", K(ret), K(connect_status));
      } else if (OB_FAIL(encode_header())) {
        STORAGE_LOG(WARN, "fail to encode header", K(ret));
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("complete fetch logic row checksum", K(ret), K(cost_ts), K(deep_copy_arg), K(row_info));
  return ret;
}

int ObFetchLogicDataChecksumSliceP::fill_rpc_buffer(const ObLogicDataChecksumProtocol& checksum_protocol)
{
  int ret = OB_SUCCESS;
  if (!checksum_protocol.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "checksum protol is invalid", K(ret), K(checksum_protocol));
  } else if (OB_FAIL(fill_data(checksum_protocol))) {
    STORAGE_LOG(WARN, "fail to fill data checksum protol", K(ret), K(checksum_protocol));
  }
  return ret;
}

int ObFetchLogicDataChecksumSliceP::set_data_checksum_protocol(const bool is_rowkey_valid,
    const int64_t schema_rowkey_cnt, const int64_t data_checksum, const ObStoreRow* store_row)
{
  int ret = OB_SUCCESS;
  ObLogicDataChecksumProtocol checksum_protocol;

  if ((is_rowkey_valid && OB_ISNULL(store_row)) || schema_rowkey_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "set data checksum protol get invalid argument",
        K(ret),
        K(is_rowkey_valid),
        K(schema_rowkey_cnt),
        KP(store_row));
  } else {
    checksum_protocol.data_checksum_ = data_checksum;
    checksum_protocol.is_rowkey_valid_ = is_rowkey_valid;
    if (is_rowkey_valid) {
      checksum_protocol.rowkey_.assign(store_row->row_val_.cells_, schema_rowkey_cnt);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_rpc_buffer(checksum_protocol))) {
        STORAGE_LOG(WARN, "fill to fill rpc buffer", K(ret));
      }
    }
  }
  return ret;
}

ObFetchLogicRowSliceP::ObFetchLogicRowSliceP(
    ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObLogicPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchLogicRowSliceP::process()
{
  int ret = OB_SUCCESS;
  // get tables and init ms row iter
  char* buf = NULL;
  const int64_t HALF_OF_TIMEOUT = MAX_LOGIC_MIGRATE_TIME_OUT / 2;  // 15s
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t start_time = start_ts;
  int64_t end_time = 0;
  ObLogicMigrateRpcHeader::ConnectStatus connect_status = ObLogicMigrateRpcHeader::ConnectStatus::INVALID_STATUS;
  ObFetchLogicRowArg deep_copy_arg;

  STORAGE_LOG(INFO, "start fetch logic row", K_(arg));
  DEBUG_SYNC(DEFORE_FETCH_LOGIC_ROW_SRC);
  SMART_VAR(ObLogicRowProducer, logic_row_producer)
  {
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(partition_service_) || OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "init get invalid argument", K(ret), KP(partition_service_), K(bandwidth_throttle_));
    } else if (OB_FAIL(arg_.deep_copy(allocator_, deep_copy_arg))) {
      STORAGE_LOG(WARN, "fail to deep copy fetch logic row arg", K(ret), K_(arg));
    } else if (OB_FAIL(logic_row_producer.init(partition_service_, &deep_copy_arg))) {
      STORAGE_LOG(WARN, "fail to init logic row producer", K(ret));
    } else if (OB_FAIL(reserve_header())) {
      STORAGE_LOG(WARN, "fail to reserve header", K(ret), K(deep_copy_arg));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      const ObStoreRow* store_row = NULL;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(logic_row_producer.get_next_row(store_row))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next row", K(ret));
          }
        } else if (NULL == store_row) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "store row must not be NULL", K(ret), KP(store_row));
        } else {
          end_time = ObTimeUtility::current_time();
          if (store_row->row_type_flag_.is_last_multi_version_row()) {
            if ((end_time - start_time) >= MAX_RECONNECTION_INTERVAL) {
              STORAGE_LOG(INFO, "fetch logic row process need re-connection");
              connect_status = ObLogicMigrateRpcHeader::ConnectStatus::RECONNECT;
              if (OB_FAIL(fill_data(*store_row))) {
                STORAGE_LOG(WARN, "fail to fill store row", K(ret), K(*store_row));
              } else {

                break;
              }
            }
          } else if (logic_row_producer.need_reset_logic_row_iter()) {
            if (OB_FAIL(fill_data(*store_row))) {
              STORAGE_LOG(WARN, "fail to fill store row", K(ret), K(*store_row));
            } else if (OB_FAIL(logic_row_producer.reset_logical_row_iter())) {
              STORAGE_LOG(WARN, "fail to reset logical row iter", K(ret));
            } else {
              continue;
            }
          } else if (end_time - start_time >= HALF_OF_TIMEOUT) {

            if (OB_FAIL(do_flush())) {
              STORAGE_LOG(WARN, "fail to do flush", K(ret));
            } else {
              start_time = ObTimeUtility::current_time();
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(fill_data(*store_row))) {
              STORAGE_LOG(WARN, "fail to fill store row", K(ret), K(*store_row));
            }
          }
        }

        if (OB_SUCC(ret)) {
          store_row = NULL;
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        connect_status = ObLogicMigrateRpcHeader::ConnectStatus::ENDCONNECT;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_connect_status(connect_status))) {
          STORAGE_LOG(WARN, "fail to set connect status", K(ret), K(connect_status));
        } else if (OB_FAIL(encode_header())) {
          STORAGE_LOG(WARN, "fail to encode header", K(ret));
        }
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  STORAGE_LOG(INFO, "finish fetch logic row", K(ret), K(cost_ts), K(deep_copy_arg));
  return ret;
}

ObFetchLogicRowInfo::ObFetchLogicRowInfo()
    : total_row_count_(0), not_exist_row_count_(0), exist_row_count_(0), del_row_count_(0), sparse_row_count_(0)
{
  MEMSET(dml_count_, 0, sizeof(dml_count_));
  MEMSET(first_dml_count_, 0, sizeof(first_dml_count_));
}

ObFetchLogicRowInfo::~ObFetchLogicRowInfo()
{}

int ObFetchLogicRowInfo::add_row(const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;

  ++total_row_count_;

  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row", K(ret), K(row));
  } else {
    if (common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == row.flag_) {
      ++not_exist_row_count_;
    } else if (common::ObActionFlag::OP_ROW_EXIST == row.flag_) {
      ++exist_row_count_;
    } else if (common::ObActionFlag::OP_DEL_ROW == row.flag_) {
      ++del_row_count_;
    }

    ++dml_count_[row.dml_];
    ++first_dml_count_[row.first_dml_];

    if (row.is_sparse_row_) {
      ++sparse_row_count_;
    }
  }

  return ret;
}

int ObFetchPartitionGroupInfoP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPGStorage* pg_storage = NULL;
  ObTablesHandle tmp_handle;
  ObVersion version;
  uint64_t last_slide_log_id = 0;
  ObRole role;
  LOG_INFO("start to fetch partition group info", K(arg_.pg_key_));
  const int64_t src_cluster_id = get_src_cluster_id();
  bool is_disk_error = false;

#ifdef ERRSIM
  if (OB_SUCC(ret) && !is_disk_error) {
    is_disk_error = GCONF.fake_disk_error;
  }
#endif

  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition_service_ should not be null here", K(ret));
  } else if (ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2 > arg_.compat_version_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "fetch partition group info from new server to old server is not supported", K(ret), K_(arg));
  } else if (!is_disk_error && OB_FAIL(ObIOManager::get_instance().is_disk_error_definite(is_disk_error))) {
    STORAGE_LOG(WARN, "failed to check is disk error", K(ret));
  } else if (is_disk_error) {
    ret = OB_DISK_ERROR;
    STORAGE_LOG(ERROR, "observer has disk error, cannot be migrate src", K(ret));
  } else if (OB_FAIL(partition_service_->get_partition(arg_.pg_key_, guard)) ||
             OB_ISNULL(guard.get_partition_group())) {
    STORAGE_LOG(WARN, "Fail to get partition, ", K(ret), K(arg_.pg_key_));
  } else if (NULL == (pg_storage = &(guard.get_partition_group()->get_pg_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failt to get pg partition storage", K(ret), KP(pg_storage));
  } else if (OB_FAIL(pg_storage->get_pg_meta(result_.pg_meta_))) {
    STORAGE_LOG(WARN, "fail to copy meta", K(ret));
  } else if (!ObMigrateStatusHelper::check_can_migrate_out(result_.pg_meta_.migrate_status_)) {
    ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
    STORAGE_LOG(WARN,
        "src migrate status do not allow to migrate out",
        K(ret),
        "src migrate status",
        result_.pg_meta_.migrate_status_);

  } else if (REPLICA_NOT_RESTORE == result_.pg_meta_.is_restore_ &&
             OB_FAIL(guard.get_partition_group()->get_curr_storage_info_for_migrate(
                 arg_.use_slave_safe_read_ts_, arg_.replica_type_, src_cluster_id, result_.pg_meta_.storage_info_))) {
    STORAGE_LOG(WARN, "fail to get memstore info", K(ret), K(arg_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pg_storage->get_major_version(result_.major_version_))) {
    STORAGE_LOG(WARN, "fail to get major version", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_role(role))) {
    STORAGE_LOG(WARN, "failed to get role", K(ret));
  } else if (is_strong_leader(role)) {
    result_.is_log_sync_ = true;
  } else if (OB_FAIL(partition_service_->is_log_sync(arg_.pg_key_, result_.is_log_sync_, last_slide_log_id))) {
    if (OB_LOG_NOT_SYNC != ret) {
      STORAGE_LOG(WARN, "fail to check log sync", K(ret), "is_log_sync", result_.is_log_sync_);
    } else {
      result_.is_log_sync_ = false;
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    ObStorageFile* pg_file = nullptr;
    if (OB_ISNULL(pg_storage)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg_storage is null", K(ret));
    } else if (OB_ISNULL(pg_file = pg_storage->get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg_file is null", K(ret));
    } else {
      result_.pg_file_id_ = pg_file->get_file_id();
      result_.compat_version_ = ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2;
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "succ to get partition group info", K_(result), K(ret));
  }
  return ret;
}

ObFetchPGPartitioninfoP::ObFetchPGPartitioninfoP(
    storage::ObPartitionService* partition_service, common::ObInOutBandwidthThrottle* bandwidth_throttle)
    : ObCommonPartitionServiceRpcP(partition_service, bandwidth_throttle)
{}

int ObFetchPGPartitioninfoP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPGStorage* pg_storage = NULL;
  ObPGPartitionBaseDataMetaObProducer producer;
  const ObPGPartitionMetaInfo* pg_partition_meta_info = NULL;
  char* buf = NULL;
  ObMigrateStatus migrate_status = ObMigrateStatus::OB_MIGRATE_STATUS_MAX;
  LOG_INFO("start to fetch pg partition info");

  last_send_time_ = ObTimeUtility::current_time();
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  } else if (OB_ISNULL(partition_service_) || OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
        "bandwidth_throttle_ and partition_service_ must not null",
        K(ret),
        KP_(partition_service),
        KP_(bandwidth_throttle));
  } else if (OB_FAIL(partition_service_->get_partition(arg_.pg_key_, guard)) ||
             OB_ISNULL(guard.get_partition_group())) {
    STORAGE_LOG(WARN, "Fail to get partition, ", K(ret), K(arg_.pg_key_));
  } else if (NULL == (pg_storage = &(guard.get_partition_group()->get_pg_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failt to get pg partition storage", K(ret), KP(pg_storage));
  } else if (OB_FAIL(pg_storage->get_pg_migrate_status(migrate_status))) {
    STORAGE_LOG(WARN, "fail to get partition migrate status", K(ret));
  } else if (!ObMigrateStatusHelper::check_can_migrate_out(migrate_status)) {
    ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
    STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migrate_status));
  } else if (OB_FAIL(producer.init(arg_.pg_key_,
                 arg_.snapshot_version_,
                 arg_.is_only_major_sstable_,
                 arg_.log_ts_,
                 partition_service_))) {
    STORAGE_LOG(WARN, "fail to init producer", K(ret), K(arg_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(producer.get_next_partition_meta_info(pg_partition_meta_info))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next partition meta info", K(ret));
        }
      } else if (OB_ISNULL(pg_partition_meta_info)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid pg partition meta info", K(ret), KP(pg_partition_meta_info));
      } else if (OB_FAIL(fill_data(*pg_partition_meta_info))) {
        STORAGE_LOG(WARN, "fill to fill pg partition meta info", K(ret), K(*pg_partition_meta_info));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
}  // namespace obrpc

namespace storage {
int ObPartitionServiceRpc::init(obrpc::ObPartitionServiceRpcProxy* rpc_proxy, ObPartitionService* partition_service,
    const common::ObAddr& self, obrpc::ObCommonRpcProxy* rs_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN,
        "ObPartitionServiceRpc has inited",
        K(rpc_proxy),
        K(partition_service),
        K(self),
        K_(rpc_proxy),
        K_(partition_service),
        K_(self));
  } else if (NULL == rpc_proxy || NULL == partition_service || !self.is_valid() || OB_ISNULL(rs_rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc init with invalid argument", K(rpc_proxy), K(partition_service), K(self));
  } else if (OB_FAIL(split_dest_partition_cb_.init(partition_service))) {
    STORAGE_LOG(WARN, "split dest partition callback init failed", K(ret));
  } else if (OB_FAIL(replica_split_progress_cb_.init(partition_service))) {
    STORAGE_LOG(WARN, "replica split progress callback init failed", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    partition_service_ = partition_service;
    self_ = self;
    rs_rpc_proxy_ = rs_rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObPartitionServiceRpc::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    rpc_proxy_ = NULL;
    partition_service_ = NULL;
    self_ = ObAddr();
    rs_rpc_proxy_ = NULL;
  }
}

int ObPartitionServiceRpc::post_add_replica_mc_msg(
    const common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, ObMCLogRpcInfo& mc_log_info)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_POST_ADD_REPLICA_MC_MSG) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
    STORAGE_LOG(ERROR, "fake post_add_replica_mc_msg fail", K(server), K(arg), K(mc_log_info), K(ret));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(arg), K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(server).add_replica_mc(arg, mc_log_info))) {
    STORAGE_LOG(WARN, "add replica member change fail", K(server), K(arg), K(ret));
  } else {
    STORAGE_LOG(TRACE, "add replica member change successfully", K(server), K(arg));
  }
  DEBUG_SYNC(AFTER_POST_ADD_REPLICA_MC_MSG);
  return ret;
}

int ObPartitionServiceRpc::post_remove_replica_mc_msg(
    const common::ObAddr& server, const obrpc::ObMemberChangeArg& arg, ObMCLogRpcInfo& mc_log_info)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_POST_REMOVE_REPLICA_MC_MSG) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
    STORAGE_LOG(ERROR, "fake post_remove_replica_mc_msg fail", K(ret), K(server), K(arg), K(mc_log_info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(arg), K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(server).remove_replica_mc(arg, mc_log_info))) {
    STORAGE_LOG(WARN, "remove replica member change fail", K(server), K(arg), K(ret));
  } else {
    STORAGE_LOG(TRACE, "remove replica member change successfully", K(server), K(arg));
  }
  DEBUG_SYNC(AFTER_POST_REMOVE_REPLICA_MC_MSG);
  return ret;
}

int ObPartitionServiceRpc::post_remove_replica(const common::ObAddr& server, const obrpc::ObRemoveReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(arg), K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(server).remove_replica(arg))) {
    STORAGE_LOG(WARN, "remove replica fail", K(server), K(arg), K(ret));
  } else {
    STORAGE_LOG(TRACE, "remove replica successfully", K(server), K(arg));
  }
  return ret;
}

int ObPartitionServiceRpc::batch_post_remove_replica(
    const common::ObAddr& server, const obrpc::ObRemoveReplicaArgs& args)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(args), K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(server).batch_remove_replica(args))) {
    STORAGE_LOG(WARN, "remove replica fail", K(server), K(args), K(ret));
  } else {
    STORAGE_LOG(TRACE, "remove replica successfully", K(server), K(args));
  }
  return ret;
}

int ObPartitionServiceRpc::is_member_change_done(const common::ObAddr& server, obrpc::ObMCLogRpcInfo& mc_log_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!mc_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(mc_log_info));
  } else if (OB_SUCCESS != (ret = rpc_proxy_->to(server).is_member_change_done(mc_log_info))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to is_member_change_done", K(ret), K(server), K(mc_log_info));
    }
  } else {
    STORAGE_LOG(TRACE, "finish is_member_change_done", K(server), K(mc_log_info));
  }
  return ret;
}

int ObPartitionServiceRpc::post_add_replica_res(const common::ObAddr& server, const obrpc::ObAddReplicaRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).add_replica_res(res))) {
      STORAGE_LOG(WARN, "post add replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post add replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_batch_add_replica_res(
    const common::ObAddr& server, const obrpc::ObAddReplicaBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).add_replica_batch_res(res))) {
      STORAGE_LOG(WARN, "post batch add replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post batch add replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_rebuild_replica_res(const common::ObAddr& server, const obrpc::ObRebuildReplicaRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).rebuild_replica_res(res))) {
      STORAGE_LOG(WARN, "post rebuild replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post rebuild replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_change_replica_res(const common::ObAddr& server, const obrpc::ObChangeReplicaRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).change_replica_res(res))) {
      STORAGE_LOG(WARN, "post change replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post change replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_restore_replica_res(const common::ObAddr& server, const obrpc::ObRestoreReplicaRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).restore_replica_res(res))) {
      STORAGE_LOG(WARN, "post restore replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post restore replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_phy_restore_replica_res(
    const common::ObAddr& server, const obrpc::ObPhyRestoreReplicaRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).physical_restore_replica_res(res))) {
      STORAGE_LOG(WARN, "post physical restore replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post physical restore replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_validate_backup_res(const common::ObAddr& server, const obrpc::ObValidateRes& res)
{
  int ret = OB_NOT_SUPPORTED;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).validate_backup_res(res))) {
      STORAGE_LOG(WARN, "post physical restore replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post physical restore replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_batch_copy_sstable_res(
    const common::ObAddr& server, const obrpc::ObCopySSTableBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (res.res_array_.count() != 1) {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "copy sstable res count > 1", K(ret), K(res.res_array_.count()));
    } else if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).copy_sstable_batch_res(res))) {
      STORAGE_LOG(WARN, "post copy sstable res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(INFO, "post copy sstable res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_migrate_replica_res(const common::ObAddr& server, const obrpc::ObMigrateReplicaRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).migrate_replica_res(res))) {
      STORAGE_LOG(WARN, "post migrate replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post migrate replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_batch_migrate_replica_res(
    const common::ObAddr& server, const obrpc::ObMigrateReplicaBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).migrate_replica_batch_res(res))) {
      STORAGE_LOG(WARN, "post batch migrate replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post batch migrate replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_batch_change_replica_res(
    const common::ObAddr& server, const obrpc::ObChangeReplicaBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).change_replica_batch_res(res))) {
      STORAGE_LOG(WARN, "post batch change replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post batch change replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_batch_backup_replica_res(
    const common::ObAddr& server, const obrpc::ObBackupBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(res));
  } else {
    if (OB_SUCCESS != (ret = rs_rpc_proxy_->to(server).backup_replica_batch_res(res))) {
      STORAGE_LOG(WARN, "post batch migrate replica res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post batch backup replica res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_batch_validate_backup_res(
    const common::ObAddr& server, const obrpc::ObValidateBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server.is_valid()), K(server), K(res.is_valid()), K(res));
  } else {
    if (OB_FAIL(rs_rpc_proxy_->to(server).validate_backup_batch_res(res))) {
      STORAGE_LOG(WARN, "post batch validate backup res fail", K(ret), K(res));
    } else {
      STORAGE_LOG(TRACE, "post batch validate backup res successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_get_member_list_msg(
    const common::ObAddr& server, const common::ObPartitionKey& key, common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "post get member list with invalid argument", K(server), K(key));
  } else {
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).get_member_list(key, member_list))) {
      STORAGE_LOG(WARN, "post get member list fail", K(ret), K(key), K(member_list));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_POST_GET_MEMBER_LIST_MSG) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_POST_GET_MEMBER_LIST_MSG", K(ret));
    }
  }
#endif

  return ret;
}

int ObPartitionServiceRpc::post_warm_up_request(
    const common::ObAddr& server, const uint64_t tenant_id, const ObWarmUpRequestArg& arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(server));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                         .by(tenant_id)
                         .timeout(GCONF.rpc_timeout)
                         .post_warm_up_request(arg, &warm_up_cb_))) {
    STORAGE_LOG(WARN, "failed to post warm up request", K(ret));
  }
  return ret;
}

int ObPartitionServiceRpc::post_split_dest_partition_request(
    const ObAddr& server, const uint64_t tenant_id, const ObSplitDestPartitionRequestArg& arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(tenant_id), K(arg));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                         .by(tenant_id)
                         .timeout(GCONF.rpc_timeout)
                         .post_split_dest_partition_request(arg, &split_dest_partition_cb_))) {
    STORAGE_LOG(WARN, "post split dest partition request failed", K(ret));
  } else {
    STORAGE_LOG(INFO, "post split dest partition request success", K(server), K(tenant_id), K(arg));
  }
  return ret;
}

int ObPartitionServiceRpc::post_replica_split_progress_request(
    const ObAddr& server, const uint64_t tenant_id, const ObReplicaSplitProgressRequest& arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(tenant_id), K(arg));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                         .by(tenant_id)
                         .timeout(GCONF.rpc_timeout)
                         .post_replica_split_progress_request(arg, &replica_split_progress_cb_))) {
    STORAGE_LOG(WARN, "post replica split progress request failed", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "post replica split progress request success", K(server), K(tenant_id), K(arg));
  }
  return ret;
}

int ObPartitionServiceRpc::batch_post_remove_replica_mc_msg(
    const common::ObAddr& server, obrpc::ObChangeMemberArgs& arg, obrpc::ObChangeMemberCtxs& mc_info)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int64_t fake_remove_member_error = GCONF.fake_remove_member_error;
#endif

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(arg));
  } else {
#ifdef ERRSIM
    if (fake_remove_member_error > 0) {
      if (fake_remove_member_error > 1) {
        ret = OB_TIMEOUT;
      } else {
        ret = OB_PARTIAL_FAILED;
      }
      STORAGE_LOG(ERROR, "fake batch_post_remove_replica_mc_msg timeout", K(ret));
    }
#endif
  }

  if (OB_SUCC(ret)) {
    ObChangeMemberCtxsWrapper wrapper;
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).batch_remove_member(arg, wrapper))) {
      STORAGE_LOG(WARN, "failed to batch_remove_member", K(ret), K(server), K(arg), K(mc_info));
    } else if (OB_FAIL(mc_info.assign(wrapper.ctxs_))) {
      STORAGE_LOG(WARN, "failed to copy mc_info", K(ret));
    } else {
      ret = wrapper.result_code_;
      STORAGE_LOG(INFO, "batch_remove_member", K(ret), K(arg), K(mc_info));
    }
  }

  return ret;
}

int ObPartitionServiceRpc::batch_post_add_replica_mc_msg(
    const common::ObAddr& server, obrpc::ObChangeMemberArgs& arg, obrpc::ObChangeMemberCtxs& mc_info)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int64_t fake_add_member_error = GCONF.fake_add_member_error;
#endif

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(arg));
  } else {
#ifdef ERRSIM
    if (fake_add_member_error > 0) {
      if (fake_add_member_error > 1) {
        ret = OB_TIMEOUT;
      } else {
        ret = OB_PARTIAL_FAILED;
      }
      STORAGE_LOG(ERROR, "fake batch_post_add_replica_mc_msg timeout", K(ret));
    }
#endif
  }

  if (OB_SUCC(ret)) {
    ObChangeMemberCtxsWrapper wrapper;
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).batch_add_member(arg, wrapper))) {
      STORAGE_LOG(WARN, "failed to batch_add_member", K(ret), K(server), K(arg), K(mc_info));
    } else if (OB_FAIL(mc_info.assign(wrapper.ctxs_))) {
      STORAGE_LOG(WARN, "failed to copy mc_info", K(ret));
    } else {
      ret = wrapper.result_code_;
      STORAGE_LOG(INFO, "batch_add_member", K(ret), K(server), K(arg), K(mc_info));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::check_member_major_sstable_enough(
    const common::ObAddr& server, obrpc::ObMemberMajorSSTableCheckArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc has not been inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(server));
  } else if (OB_FAIL(rpc_proxy_->to(server).check_member_major_sstable_enough(arg))) {
    STORAGE_LOG(WARN, "fail to check member major sstable enough", K(ret));
  }
  return ret;
}

int ObPartitionServiceRpc::check_member_pg_major_sstable_enough(
    const common::ObAddr& server, obrpc::ObMemberMajorSSTableCheckArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc has not been inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(server));
  } else if (OB_FAIL(rpc_proxy_->to(server).check_member_pg_major_sstable_enough(arg))) {
    STORAGE_LOG(WARN, "fail to check member major sstable enough", K(ret));
  }
  return ret;
}

int ObPartitionServiceRpc::is_batch_member_change_done(const common::ObAddr& server, obrpc::ObChangeMemberCtxs& mc_info)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int64_t fake_wait_batch_member_chagne_done = GCONF.fake_wait_batch_member_chagne_done;
#endif

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(mc_info));
  } else {
#ifdef ERRSIM
    if (fake_wait_batch_member_chagne_done > 2) {
      ret = OB_TIMEOUT;
      STORAGE_LOG(ERROR, "fake is_batch_member_change_done timeout", K(ret));
    }
#endif
  }

  if (OB_SUCC(ret)) {
    ObChangeMemberCtxsWrapper wrapper;
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).is_batch_member_change_done(mc_info, wrapper))) {
      STORAGE_LOG(WARN, "failed to is_batch_member_change_done", K(ret), K(mc_info));
    } else if (OB_FAIL(mc_info.assign(wrapper.ctxs_))) {
      STORAGE_LOG(WARN, "failed to copy mc_info", K(ret));
    } else {
      ret = wrapper.result_code_;
      STORAGE_LOG(TRACE, "is_batch_member_change_done", K(ret), K(mc_info));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && fake_wait_batch_member_chagne_done > 0) {
    if (mc_info.count() == 0) {
      STORAGE_LOG(ERROR, "mc_info must not empty", K(ret), K(mc_info));
    } else if (1 == fake_wait_batch_member_chagne_done) {
      ret = OB_PARTIAL_FAILED;
      mc_info.at(0).ret_value_ = OB_EAGAIN;
      STORAGE_LOG(ERROR, "fake is_batch_member_change_done eagain", K(ret), K(mc_info));
    } else {
      ret = OB_PARTIAL_FAILED;
      mc_info.at(0).ret_value_ = OB_TIMEOUT;
      STORAGE_LOG(ERROR, "fake is_batch_member_change_done timeout", K(ret), K(mc_info));
    }
  }
#endif

  return ret;
}

int ObPartitionServiceRpc::post_fetch_partition_info_request(const common::ObAddr& server,
    const common::ObPartitionKey& pkey, const common::ObReplicaType& replica_type, const int64_t cluster_id,
    ObFetchPartitionInfoResult& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(res), K(replica_type));
  } else {
    ObFetchPartitionInfoArg arg(pkey, replica_type);
    res.reset();
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).dst_cluster_id(cluster_id).fetch_partition_info(arg, res))) {
      STORAGE_LOG(WARN, "post fetch partition info fail", K(ret), K(res), K(arg), K(cluster_id));
    } else {
      FLOG_INFO("fetch partition info successfully", K(res));
    }
  }
  return ret;
}

int ObPartitionServiceRpc::post_fetch_table_info_request(const common::ObAddr& server,
    const common::ObPartitionKey& pkey, const uint64_t& table_id, const int64_t& snapshot_version,
    const bool is_only_major_sstable, const int64_t cluster_id, ObFetchTableInfoResult& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(snapshot_version), K(res));
  } else {
    ObFetchTableInfoArg arg;
    arg.pkey_ = pkey;
    arg.table_id_ = table_id;
    arg.snapshot_version_ = snapshot_version;
    arg.is_only_major_sstable_ = is_only_major_sstable;
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).dst_cluster_id(cluster_id).fetch_table_info(arg, res))) {
      if (OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "post fetch table info request failed", K(ret), K(res));
      } else {
        STORAGE_LOG(INFO, "post fetch table info request need retry", K(ret), K(res));
      }
    } else {
      STORAGE_LOG(INFO, "fetch table info successfully", K(res));
    }  // TODO check continuity
  }
  return ret;
}

int ObPartitionServiceRpc::post_fetch_partition_group_info_request(const common::ObAddr& server,
    const common::ObPGKey& pg_key, const common::ObReplicaType& replica_type, const int64_t cluster_id,
    const bool use_slave_safe_read_ts, ObFetchPGInfoResult& res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionServiceRpc is not inited");
  } else if (!server.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type) || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(res), K(replica_type), K(pg_key));
  } else {
    ObFetchPGInfoArg arg(pg_key, replica_type, use_slave_safe_read_ts);
    res.reset();
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server).dst_cluster_id(cluster_id).fetch_partition_group_info(arg, res))) {
      STORAGE_LOG(WARN, "post fetch partition info fail", K(ret), K(res), K(arg), K(cluster_id));
    } else {
      FLOG_INFO("fetch partition info successfully", K(res));
    }
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
