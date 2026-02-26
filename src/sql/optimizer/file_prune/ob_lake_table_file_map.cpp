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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_lake_table_file_map.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObLakeTableFileMapKey, table_loc_id_, tablet_id_);

ObLakeTableFileMapKey::ObLakeTableFileMapKey()
: table_loc_id_(OB_INVALID_ID),
  tablet_id_(OB_INVALID_ID)
{}

ObLakeTableFileMapKey::ObLakeTableFileMapKey(uint64_t table_loc_id, common::ObTabletID tablet_id)
: table_loc_id_(table_loc_id),
  tablet_id_(tablet_id)
{}

int ObLakeTableFileMapKey::assign(const ObLakeTableFileMapKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    table_loc_id_ = other.table_loc_id_;
    tablet_id_ = other.tablet_id_;
  }
  return ret;
}

void ObLakeTableFileMapKey::reset()
{
  table_loc_id_ = OB_INVALID_ID;
  tablet_id_ = OB_INVALID_ID;
}

int ObLakeTableFileMapKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = do_hash(table_loc_id_, hash_val);
  hash_val = tablet_id_.hash(hash_val);
  return ret;
}

bool ObLakeTableFileMapKey::operator== (const ObLakeTableFileMapKey &other) const
{
  return table_loc_id_ == other.table_loc_id_ && tablet_id_ == other.tablet_id_;
}

OB_DEF_SERIALIZE(ObLakeTableFileDesc)
{
  int ret = OB_SUCCESS;
  int64_t key_count = keys_.count();
  int64_t value_count = values_.count();
  LST_DO_CODE(OB_UNIS_ENCODE, key_count, value_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < key_count; ++i) {
    OB_UNIS_ENCODE(keys_.at(i));
    OB_UNIS_ENCODE(offsets_.at(i));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value_count; ++i) {
    if (OB_ISNULL(values_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null field bound");
    } else {
      OB_UNIS_ENCODE(values_.at(i)->get_file_type());
      OB_UNIS_ENCODE(*values_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLakeTableFileDesc)
{
  int64_t len = 0;
  int64_t key_count = keys_.count();
  int64_t value_count = values_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, key_count, value_count);
  for (int64_t i = 0; i < key_count; ++i) {
    OB_UNIS_ADD_LEN(keys_.at(i));
    OB_UNIS_ADD_LEN(offsets_.at(i));
  }
  for (int64_t i = 0; i < value_count; ++i) {
    if (OB_NOT_NULL(values_.at(i))) {
      OB_UNIS_ADD_LEN(values_.at(i)->get_file_type());
      OB_UNIS_ADD_LEN(*values_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObLakeTableFileDesc)
{
  int ret = OB_SUCCESS;
  int64_t key_count = 0;
  int64_t value_count = 0;
  LST_DO_CODE(OB_UNIS_DECODE, key_count, value_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < key_count; ++i) {
    ObLakeTableFileMapKey *key = keys_.alloc_place_holder();
    int64_t *offset = offsets_.alloc_place_holder();
    if (OB_ISNULL(key) || OB_ISNULL(offset)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObLakeTableFileMapKey or int64_t");
    } else {
      OB_UNIS_DECODE(*key);
      OB_UNIS_DECODE(*offset);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value_count; ++i) {
    LakeFileType type = LakeFileType::INVALID;
    ObFileScanTask *table_file = nullptr;
    OB_UNIS_DECODE(type);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObFileScanTask::create_lake_table_file_by_type(allocator_, type, table_file))) {
      LOG_WARN("failed to create lake table file by type", K(type));
    } else if (OB_ISNULL(table_file)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObFileScanTask");
    } else {
      OB_UNIS_DECODE(*table_file);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(values_.push_back(table_file))) {
        LOG_WARN("failed to push back table_file");
      }
    }
  }
  return ret;
}

ObLakeTableFileDesc::ObLakeTableFileDesc(ObIAllocator &allocator)
: allocator_(allocator)
{}

void ObLakeTableFileDesc::reset()
{
  keys_.reset();
  offsets_.reset();
  values_.reset();
}

int ObLakeTableFileDesc::assign(const ObLakeTableFileDesc &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(keys_.assign(other.keys_))) {
      LOG_WARN("failed to assign keys");
    } else if (OB_FAIL(offsets_.assign(other.offsets_))) {
      LOG_WARN("failed to assign offsets");
    } else if (OB_FAIL(values_.assign(other.values_))) {
      LOG_WARN("failed to assign values");
    }
  }
  return ret;
}

int ObLakeTableFileDesc::add_lake_table_file_desc(const ObLakeTableFileMapKey &key,
                                                  const ObIArray<sql::ObFileScanTask *> *files)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(keys_.push_back(key))) {
    LOG_WARN("failed to push back key");
  } else if (OB_FAIL(offsets_.push_back(values_.count()))) {
    LOG_WARN("failed to push back offset");
  } else if (files != nullptr && OB_FAIL(append(values_, *files))) {
    LOG_WARN("failed to append value");
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObOdpsPartitionKey, table_ref_id_, partition_str_);

ObOdpsPartitionKey::ObOdpsPartitionKey()
: table_ref_id_(OB_INVALID_ID)
{}


ObOdpsPartitionKey::ObOdpsPartitionKey(uint64_t table_ref_id, const ObString &partition_str)
: table_ref_id_(table_ref_id),
  partition_str_(partition_str)
{}

int ObOdpsPartitionKey::assign(const ObOdpsPartitionKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    table_ref_id_ = other.table_ref_id_;
    partition_str_ = other.partition_str_;
  }
  return ret;
}

void ObOdpsPartitionKey::reset()
{
  table_ref_id_ = OB_INVALID_ID;
  partition_str_.reset();
}


int ObOdpsPartitionKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = do_hash(table_ref_id_, hash_val);
  hash_val = do_hash(partition_str_, hash_val);
  return ret;
}

bool ObOdpsPartitionKey::operator== (const ObOdpsPartitionKey &other) const
{
  return table_ref_id_ == other.table_ref_id_ && partition_str_ == other.partition_str_;
}

}
}