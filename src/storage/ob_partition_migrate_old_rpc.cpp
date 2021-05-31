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
#include "ob_partition_migrate_old_rpc.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace blocksstable;
using namespace oceanbase::share::schema;
using namespace storage;

namespace storage {

ObStoreInfo::ObStoreInfo() : saved_storage_info_(), sstable_count_(-1)
{}

ObStoreInfo::~ObStoreInfo()
{}

OB_SERIALIZE_MEMBER(ObStoreInfo, saved_storage_info_, sstable_count_);

int ObStoreInfo::assign(const ObStoreInfo& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(info.saved_storage_info_, info.sstable_count_))) {
    STORAGE_LOG(WARN, "failed to assign store info", K(ret));
  }
  return ret;
}

int ObStoreInfo::assign(const ObSavedStorageInfo& info, const int64_t sstable_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(saved_storage_info_.is_valid())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot assign twice", K(ret), K(*this));
  } else if (OB_FAIL(saved_storage_info_.deep_copy(info))) {
    STORAGE_LOG(WARN, "failed to deep copy saved storage info", K(ret), K(info));
  } else {
    sstable_count_ = sstable_count;
  }
  return ret;
}

}  // namespace storage

namespace obrpc {
OB_SERIALIZE_MEMBER(ObMigrateInfoFetchArg, pkey_);

int ObMigrateStoreInfo::assign(const ObMigrateStoreInfo& info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(store_info_.assign(info.store_info_))) {
    STORAGE_LOG(WARN, "failed to deep copy saved_storage_info", K(ret));
  } else {
    server_ = info.server_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMigrateStoreInfo, store_info_, server_);

const ObMigrateStoreInfo* ObMigrateInfoFetchResult::left_migrate_info() const
{
  const ObMigrateStoreInfo* left_info = NULL;

  for (int64_t i = 0; NULL == left_info && i < info_list_.count(); ++i) {
    const ObSavedStorageInfo& info = info_list_[i].store_info_.get_saved_info();
    if (info.get_frozen_version().major_ < info.get_memstore_version().major_) {
      left_info = &info_list_[i];
    }
  }
  return left_info;
}

const ObMigrateStoreInfo* ObMigrateInfoFetchResult::right_migrate_info() const
{
  const ObMigrateStoreInfo* right_info = NULL;
  for (int64_t i = info_list_.count() - 1; NULL == right_info && i >= 0; --i) {
    const ObSavedStorageInfo& info = info_list_[i].store_info_.get_saved_info();
    if (info.get_frozen_version().major_ < info.get_memstore_version().major_) {
      right_info = &info_list_[i];
    }
  }
  return right_info;
}

uint64_t ObMigrateInfoFetchResult::get_last_replay_log_id() const
{
  uint64_t last_replay_log_id = OB_INVALID_ID;
  if (info_list_.count() > 0) {
    const ObSavedStorageInfo& info = info_list_[info_list_.count() - 1].store_info_.get_saved_info();
    last_replay_log_id = info.get_last_replay_log_id();
  }

  return last_replay_log_id;
}

int ObMigrateInfoFetchResult::add_store_info(const ObMigrateStoreInfo& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(info_list_.push_back(info))) {
    STORAGE_LOG(WARN, "failed to add info", K(ret));
  }
  return ret;
}

int ObMigrateInfoFetchResult::add_store_info(const storage::ObStoreInfo& info, const common::ObAddr server)
{
  int ret = OB_SUCCESS;
  ObMigrateStoreInfo tmp_info;
  if (OB_FAIL(tmp_info.store_info_.assign(info))) {
    STORAGE_LOG(WARN, "failed to assign store info", K(ret));
  } else {
    tmp_info.server_ = server;
    if (OB_FAIL(info_list_.push_back(tmp_info))) {
      STORAGE_LOG(WARN, "failed to add info", K(ret));
    }
  }

  return ret;
}

bool ObMigrateInfoFetchResult::is_continues() const
{
  bool is_continues = true;
  for (int64_t i = 1; is_continues && i < info_list_.count(); ++i) {
    if (info_list_[i].get_version().major_ != info_list_[i - 1].get_version().major_ + 1) {
      is_continues = false;
      STORAGE_LOG(ERROR,
          "major version not continues",
          K(i),
          "i",
          info_list_[i].get_version().major_,
          "i-1",
          info_list_[i - 1].get_version().major_,
          K(*this));
    }
  }
  return is_continues;
}

ObMigrateInfoFetchResult::ObMigrateInfoFetchResult()
{
  reset();
}

void ObMigrateInfoFetchResult::reset()
{
  info_list_.reset();
  replica_type_ = REPLICA_TYPE_FULL;
}

OB_SERIALIZE_MEMBER(ObMigrateInfoFetchResult, info_list_, replica_type_);

OB_SERIALIZE_MEMBER(ObFetchMacroBlockOldArg, index_id_, macro_block_index_, data_version_, data_seq_);

void ObFetchMacroBlockOldArg::reset()
{
  index_id_ = 0;
  macro_block_index_ = 0;
  data_version_ = 0;
  data_seq_ = 0;
}

bool ObFetchMacroBlockOldArg::operator==(const ObFetchMacroBlockOldArg& arg) const
{
  return index_id_ == arg.index_id_ && macro_block_index_ == arg.macro_block_index_ &&
         data_version_ == arg.data_version_ && data_seq_ == arg.data_seq_;
}

int64_t ObFetchMacroBlockOldArg::hash() const
{
  int64_t hash_value = 0;
  hash_value = murmurhash(&index_id_, sizeof(index_id_), hash_value);
  hash_value = murmurhash(&macro_block_index_, sizeof(macro_block_index_), hash_value);
  hash_value = murmurhash(&data_version_, sizeof(data_version_), hash_value);
  hash_value = murmurhash(&data_seq_, sizeof(data_seq_), hash_value);
  return hash_value;
}

ObFetchMacroBlockListOldArg::ObFetchMacroBlockListOldArg()
    : pkey_(), data_version_(0), arg_list_(), store_type_(storage::MAJOR_SSSTORE)
{}

OB_SERIALIZE_MEMBER(ObFetchMacroBlockListOldArg, pkey_, data_version_, arg_list_, store_type_);

}  // namespace obrpc
}  // namespace oceanbase
