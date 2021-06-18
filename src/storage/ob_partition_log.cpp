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

#include "storage/ob_partition_log.h"
#include "storage/ob_i_store.h"
#include "storage/ob_saved_storage_info.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
namespace storage {
int64_t ObCreateSSTableLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(sstable));
  J_OBJ_END();
  return pos;
}
int64_t ObCompleteSSTableLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(sstable));
  J_OBJ_END();
  return pos;
}
int64_t ObDeleteSSTableLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_key));
  J_OBJ_END();
  return pos;
}
int64_t ObAddSSTableLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pg_key), K_(sstable));
  J_OBJ_END();
  return pos;
}
int64_t ObRemoveSSTableLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pg_key), K_(table_key));
  J_OBJ_END();
  return pos;
}
int64_t ObCreatePartitionStoreLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(meta));
  J_OBJ_END();
  return pos;
}
int64_t ObModifyTableStoreLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(kept_multi_version_start), K_(table_store), K_(pg_key));
  J_OBJ_END();
  return pos;
}
int64_t ObDropIndexSSTableLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pkey), K_(index_id), K_(pg_key));
  J_OBJ_END();
  return pos;
}
int64_t ObUpdatePartitionMetaLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(meta));
  J_OBJ_END();
  return pos;
}
int64_t ObSplitPartitionStateLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pkey), "state", to_state_str(static_cast<ObPartitionSplitStateEnum>(state_)));
  J_OBJ_END();
  return pos;
}
int64_t ObSplitPartitionInfoLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pkey), K_(split_info));
  J_OBJ_END();
  return pos;
}
int64_t ObUpdateTenantConfigLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(units));
  J_OBJ_END();
  return pos;
}
int64_t ObCreatePartitionGroupLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(meta));
  J_OBJ_END();
  return pos;
}
int64_t ObUpdatePartitionGroupMetaLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(meta));
  J_OBJ_END();
  return pos;
}
int64_t ObCreatePGPartitionStoreLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(meta), K_(pg_key));
  J_OBJ_END();
  return pos;
}
int64_t ObUpdatePGPartitionMetaLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(meta), K_(pg_key));
  J_OBJ_END();
  return pos;
}
int64_t ObPGMacroBlockMetaLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pg_key), K_(table_key), K_(data_file_id), K_(disk_no), K_(macro_block_id), K_(meta));
  J_OBJ_END();
  return pos;
}
int64_t ObSetStartLogTsAfterMajorLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pg_key), K_(table_key), K_(start_log_ts_after_major));
  J_OBJ_END();
  return pos;
}
int64_t ObAddRecoveryPointDataLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(point_type), K_(point_data));
  J_OBJ_END();
  return pos;
}
int64_t ObRemoveRecoveryPointDataLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(point_type), K_(pg_key), K_(snapshot_version));
  J_OBJ_END();
  return pos;
}

ObBeginTransLogEntry::ObBeginTransLogEntry()
{}

ObBeginTransLogEntry::~ObBeginTransLogEntry()
{}

bool ObBeginTransLogEntry::is_valid() const
{
  return true;
}

int ObBeginTransLogEntry::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return ret;
}

int ObBeginTransLogEntry::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return ret;
}

int64_t ObBeginTransLogEntry::get_serialize_size() const
{
  return 0;
}

int64_t ObBeginTransLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  return 0;
}

ObChangePartitionLogEntry::ObChangePartitionLogEntry()
    : partition_key_(), replica_type_(REPLICA_TYPE_FULL), pg_key_(), log_id_(0)
{}

ObChangePartitionLogEntry::~ObChangePartitionLogEntry()
{}

bool ObChangePartitionLogEntry::is_valid() const
{
  return (partition_key_.is_valid() || pg_key_.is_valid()) && ObReplicaTypeCheck::is_replica_type_valid(replica_type_);
}

OB_SERIALIZE_MEMBER(ObChangePartitionLogEntry, partition_key_, replica_type_, pg_key_, log_id_);

ObChangePartitionStorageLogEntry::ObChangePartitionStorageLogEntry() : partition_key_()
{}

ObChangePartitionStorageLogEntry::~ObChangePartitionStorageLogEntry()
{}

bool ObChangePartitionStorageLogEntry::is_valid() const
{
  return partition_key_.is_valid();
}

OB_SERIALIZE_MEMBER(ObChangePartitionStorageLogEntry, partition_key_);

ObCreateSSTableLogEntry::ObCreateSSTableLogEntry(ObSSTable& sstable) : sstable_(sstable)
{}

bool ObCreateSSTableLogEntry::is_valid() const
{
  return true;
}

OB_SERIALIZE_MEMBER(ObCreateSSTableLogEntry, sstable_);

ObCompleteSSTableLogEntry::ObCompleteSSTableLogEntry(ObOldSSTable& sstable) : sstable_(sstable)
{}

bool ObCompleteSSTableLogEntry::is_valid() const
{
  return sstable_.is_valid();
}
OB_SERIALIZE_MEMBER(ObCompleteSSTableLogEntry, sstable_);

ObDeleteSSTableLogEntry::ObDeleteSSTableLogEntry() : table_key_()
{}

bool ObDeleteSSTableLogEntry::is_valid() const
{
  return table_key_.is_valid();
}
OB_SERIALIZE_MEMBER(ObDeleteSSTableLogEntry, table_key_);

ObAddSSTableLogEntry::ObAddSSTableLogEntry(const ObPGKey& pg_key, ObSSTable& sstable)
    : pg_key_(pg_key), sstable_(sstable)
{}

ObAddSSTableLogEntry::ObAddSSTableLogEntry(ObSSTable& sstable) : pg_key_(), sstable_(sstable)
{}

bool ObAddSSTableLogEntry::is_valid() const
{
  return pg_key_.is_valid() && sstable_.is_valid();
}

OB_SERIALIZE_MEMBER(ObAddSSTableLogEntry, pg_key_, sstable_);

ObRemoveSSTableLogEntry::ObRemoveSSTableLogEntry() : pg_key_(), table_key_()
{}

ObRemoveSSTableLogEntry::ObRemoveSSTableLogEntry(const ObPGKey& pg_key, const ObITable::TableKey& table_key)
    : pg_key_(pg_key), table_key_(table_key)
{}

bool ObRemoveSSTableLogEntry::is_valid() const
{
  return pg_key_.is_valid() && table_key_.is_valid();
}

OB_SERIALIZE_MEMBER(ObRemoveSSTableLogEntry, pg_key_, table_key_);

ObCreatePartitionStoreLogEntry::ObCreatePartitionStoreLogEntry() : meta_()
{}

bool ObCreatePartitionStoreLogEntry::is_valid() const
{
  return meta_.is_valid();
}
OB_SERIALIZE_MEMBER(ObCreatePartitionStoreLogEntry, meta_);

ObModifyTableStoreLogEntry::ObModifyTableStoreLogEntry(ObTableStore& table_store)
    : kept_multi_version_start_(0), table_store_(table_store)
{}

bool ObModifyTableStoreLogEntry::is_valid() const
{
  return kept_multi_version_start_ >= 0;
}
OB_SERIALIZE_MEMBER(ObModifyTableStoreLogEntry, kept_multi_version_start_, table_store_, pg_key_);

ObDropIndexSSTableLogEntry::ObDropIndexSSTableLogEntry() : pkey_(), index_id_()
{}

bool ObDropIndexSSTableLogEntry::is_valid() const
{
  return pkey_.is_valid() && index_id_ > 0;
}

OB_SERIALIZE_MEMBER(ObDropIndexSSTableLogEntry, pkey_, index_id_, pg_key_);

ObUpdatePartitionMetaLogEntry::ObUpdatePartitionMetaLogEntry() : meta_()
{}

bool ObUpdatePartitionMetaLogEntry::is_valid() const
{
  return meta_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUpdatePartitionMetaLogEntry, meta_);

OB_SERIALIZE_MEMBER(ObSplitPartitionStateLogEntry, pkey_, state_);

int ObSplitPartitionStateLogEntry::init(const ObPartitionKey& pkey, const int64_t state)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || !is_valid_split_state(static_cast<ObPartitionSplitStateEnum>(state))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(state));
  } else {
    pkey_ = pkey;
    state_ = state;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSplitPartitionInfoLogEntry, pkey_, split_info_);

int ObSplitPartitionInfoLogEntry::init(const ObPartitionKey& pkey, const ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || !split_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(split_info));
  } else if (OB_FAIL(split_info_.assign(split_info))) {
    STORAGE_LOG(WARN, "failed to assign split info", K(ret), K(split_info));
  } else {
    pkey_ = pkey;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObUpdateTenantConfigLogEntry, units_);

ObUpdateTenantConfigLogEntry::ObUpdateTenantConfigLogEntry(share::TenantUnits& units) : units_(units)
{}

ObCreatePartitionGroupLogEntry::ObCreatePartitionGroupLogEntry() : meta_()
{}

bool ObCreatePartitionGroupLogEntry::is_valid() const
{
  return meta_.is_valid();
}
OB_SERIALIZE_MEMBER(ObCreatePartitionGroupLogEntry, meta_);

ObUpdatePartitionGroupMetaLogEntry::ObUpdatePartitionGroupMetaLogEntry() : meta_()
{}

bool ObUpdatePartitionGroupMetaLogEntry::is_valid() const
{
  return meta_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUpdatePartitionGroupMetaLogEntry, meta_);

ObCreatePGPartitionStoreLogEntry::ObCreatePGPartitionStoreLogEntry() : meta_()
{}

bool ObCreatePGPartitionStoreLogEntry::is_valid() const
{
  return meta_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCreatePGPartitionStoreLogEntry, meta_, pg_key_);

ObUpdatePGPartitionMetaLogEntry::ObUpdatePGPartitionMetaLogEntry() : meta_()
{}

bool ObUpdatePGPartitionMetaLogEntry::is_valid() const
{
  return meta_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUpdatePGPartitionMetaLogEntry, meta_, pg_key_);

OB_SERIALIZE_MEMBER(ObPGMacroBlockMetaLogEntry, pg_key_, table_key_, data_file_id_, disk_no_, macro_block_id_, meta_);

ObPGMacroBlockMetaLogEntry::ObPGMacroBlockMetaLogEntry(const ObPGKey& pg_key, const ObITable::TableKey& table_key,
    const int64_t data_file_id, const int64_t disk_no, const MacroBlockId& macro_block_id, ObMacroBlockMetaV2& meta)
    : pg_key_(pg_key),
      table_key_(table_key),
      data_file_id_(data_file_id),
      disk_no_(disk_no),
      macro_block_id_(macro_block_id),
      meta_(meta)
{}

ObPGMacroBlockMetaLogEntry::ObPGMacroBlockMetaLogEntry(ObMacroBlockMetaV2& meta)
    : pg_key_(), table_key_(), data_file_id_(0), disk_no_(0), macro_block_id_(), meta_(meta)
{}

OB_SERIALIZE_MEMBER(ObSetStartLogTsAfterMajorLogEntry, pg_key_, table_key_, start_log_ts_after_major_);

ObSetStartLogTsAfterMajorLogEntry::ObSetStartLogTsAfterMajorLogEntry(
    const ObPGKey& pg_key, const ObITable::TableKey& table_key, const int64_t start_log_ts_after_major)
    : pg_key_(pg_key), table_key_(table_key), start_log_ts_after_major_(start_log_ts_after_major)
{}

bool ObSetStartLogTsAfterMajorLogEntry::is_valid() const
{
  return pg_key_.is_valid() && table_key_.is_valid();
}

OB_SERIALIZE_MEMBER(ObAddRecoveryPointDataLogEntry, point_type_, point_data_);

ObAddRecoveryPointDataLogEntry::ObAddRecoveryPointDataLogEntry(
    const ObRecoveryPointType point_type, ObRecoveryPointData& point_data)
    : point_type_(point_type), point_data_(point_data)
{}

bool ObAddRecoveryPointDataLogEntry::is_valid() const
{
  return point_data_.is_valid();
}

OB_SERIALIZE_MEMBER(ObRemoveRecoveryPointDataLogEntry, point_type_, pg_key_, snapshot_version_);

ObRemoveRecoveryPointDataLogEntry::ObRemoveRecoveryPointDataLogEntry()
    : point_type_(ObRecoveryPointType::UNKNOWN_TYPE), pg_key_(), snapshot_version_(-1)
{}

ObRemoveRecoveryPointDataLogEntry::ObRemoveRecoveryPointDataLogEntry(
    const ObRecoveryPointType point_type, const ObRecoveryPointData& point_data)
    : point_type_(point_type), pg_key_(point_data.get_pg_key())
{
  snapshot_version_ = point_data.get_snapshot_version();
}

bool ObRemoveRecoveryPointDataLogEntry::is_valid() const
{
  return pg_key_.is_valid() && snapshot_version_ >= 0;
}
}  // namespace storage
}  // namespace oceanbase
