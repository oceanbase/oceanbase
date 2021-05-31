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

#define USING_LOG_PREFIX SHARE
#include "ob_partition_modify.h"

namespace oceanbase {
using namespace common;

namespace share {
int ObSplitInfo::assign(const ObSplitInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  split_type_ = other.split_type_;
  part_type_ = other.part_type_;
  return ret;
}

void ObSplitInfo::reset()
{
  split_type_ = SPLIT_INVALID;
  part_type_ = share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_ID;
}

void ObSplitInfo::set_split_info(PartitionSplitType split_type, share::schema::ObPartitionFuncType part_type)
{
  split_type_ = split_type;
  part_type_ = part_type;
}

bool ObSplitInfo::is_split_table() const
{
  return split_type_ == TABLE_SPLIT && part_type_ == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
}

bool ObSplitInfo::is_split_range_partition() const
{
  return split_type_ == PARTITION_SPLIT && share::schema::is_range_part(part_type_);
}

bool ObSplitInfo::is_split_hash_partition() const
{
  return split_type_ == PARTITION_SPLIT && share::schema::is_hash_part(part_type_);
}

bool ObSplitInfo::is_split_list_partition() const
{
  return split_type_ == PARTITION_SPLIT && share::schema::is_list_part(part_type_);
}

int ObSplitPartitionPair::init(const ObPartitionKey& src_pkey, const ObIArray<ObPartitionKey>& dest_pkey_array)
{
  int ret = OB_SUCCESS;
  if (!src_pkey.is_valid() || 0 >= dest_pkey_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(src_pkey), K(dest_pkey_array));
  } else if (OB_FAIL(dest_pkey_array_.assign(dest_pkey_array))) {
    STORAGE_LOG(WARN, "assign dest pkey array failed", K(ret), K(dest_pkey_array));
  } else {
    src_pkey_ = src_pkey;
  }
  return ret;
}

bool ObSplitPartitionPair::is_valid() const
{
  return src_pkey_.is_valid() && dest_pkey_array_.count() > 0;
}

int ObSplitPartitionPair::assign(const ObSplitPartitionPair& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest_pkey_array_.assign(other.dest_pkey_array_))) {
  } else {
    src_pkey_ = other.src_pkey_;
  }
  return ret;
}

bool ObSplitPartitionPair::is_source_partition(const ObPartitionKey& pkey) const
{
  return pkey == src_pkey_;
}

bool ObSplitPartitionPair::is_dest_partition(const ObPartitionKey& pkey) const
{
  bool bool_ret = false;
  const int64_t count = dest_pkey_array_.count();
  for (int64_t i = 0; i < count; i++) {
    if (pkey == dest_pkey_array_.at(i)) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

int ObSplitPartitionPair::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t dest_count = dest_pkey_array_.count();
  ObPartitionKey new_pkey;
  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id));
  } else if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(src_pkey_, new_tenant_id, new_pkey))) {
    STORAGE_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id));
  } else {
    src_pkey_ = new_pkey;
    for (int64_t i = 0; i < dest_count && OB_SUCC(ret); ++i) {
      const ObPartitionKey& old_pkey = dest_pkey_array_.at(i);
      if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(old_pkey, new_tenant_id, dest_pkey_array_[i]))) {
        STORAGE_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id));
      }
    }
  }
  return ret;
}

bool ObSplitPartition::is_valid() const
{
  return split_info_.count() > 0 && schema_version_ > 0;
}

void ObSplitPartition::reset()
{
  split_info_.reset();
  schema_version_ = 0;
}

int ObSplitPartition::assign(const ObSplitPartition& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(split_info_.assign(other.split_info_))) {
  } else {
    schema_version_ = other.schema_version_;
  }
  return ret;
}

int ObSplitPartition::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id));
  } else {
    const int64_t count = split_info_.count();
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(split_info_[i].replace_tenant_id(new_tenant_id))) {
        STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(new_tenant_id));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSplitPartition, split_info_, schema_version_);
OB_SERIALIZE_MEMBER(ObSplitPartitionPair, src_pkey_, dest_pkey_array_);
OB_SERIALIZE_MEMBER(ObPartitionSplitProgress, pkey_, progress_);
}  // namespace share
}  // namespace oceanbase
