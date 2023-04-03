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

namespace oceanbase
{
using namespace common;

namespace share
{
int ObSplitInfo::assign(const ObSplitInfo &other)
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
  return split_type_==PARTITION_SPLIT && share::schema::is_hash_part(part_type_);
}

bool ObSplitInfo::is_split_list_partition() const
{
  return split_type_ == PARTITION_SPLIT && share::schema::is_list_part(part_type_);
}

int ObSplitPartitionPair::init()
{
  return OB_NOT_SUPPORTED;
}

bool ObSplitPartitionPair::is_valid() const
{
  return true;
}

int ObSplitPartitionPair::assign(const ObSplitPartitionPair &other)
{
  UNUSED(other);
  return OB_NOT_SUPPORTED;
}

int ObSplitPartitionPair::replace_tenant_id(const uint64_t new_tenant_id)
{
  return OB_NOT_SUPPORTED;
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

int ObSplitPartition::assign(const ObSplitPartition &other)
{
  UNUSED(other);
  return OB_NOT_SUPPORTED;
}

int ObSplitPartition::replace_tenant_id(const uint64_t new_tenant_id)
{
  UNUSED(new_tenant_id);
  return OB_NOT_SUPPORTED;
}

OB_SERIALIZE_MEMBER(ObSplitPartition, split_info_, schema_version_);
OB_SERIALIZE_MEMBER(ObSplitPartitionPair, unused_);
OB_SERIALIZE_MEMBER(ObPartitionSplitProgress, progress_);
} // namespace rootserver
} // namespace oceanbase
