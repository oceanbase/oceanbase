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

#include "common/ob_partition_key.h"
#include "lib/json/ob_yson.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase {
namespace common {
// TODO delete partition_cnt
int ObPartitionKey::init(const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt)
{
  int ret = OB_SUCCESS;

  if (!is_valid_id(table_id) || partition_id < 0 || partition_cnt < 0) {
    // In future, partition id may be less than 0 or be uint64_t
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "init fail", K(ret), K(table_id), K(partition_id), K(partition_cnt));
  } else if (!!is_twopart(partition_id)) {
    table_id_ = table_id;
    part_id_ = static_cast<int32_t>(extract_part_id(partition_id));
    subpart_id_ = static_cast<int32_t>(extract_subpart_id(partition_id));
  } else {
    table_id_ = table_id;
    part_id_ = static_cast<int32_t>(partition_id);
    part_cnt_ = static_cast<int32_t>(partition_cnt);
  }
  hash_value_ = inner_hash();
  return ret;
}

int ObPartitionKey::parse(const char* pkey)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  int64_t partition_idx = 0;
  int32_t partition_cnt = 0;

  if (OB_ISNULL(pkey)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (3 != sscanf(pkey, "%ld:%ld:%d", &table_id, &partition_idx, &partition_cnt)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = init(table_id, partition_idx, partition_cnt);
  }
  return ret;
}

int ObPartitionKey::parse_from_cstring(const char* pkey)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  int64_t partition_idx = 0;
  int32_t partition_cnt = 0;

  if (OB_ISNULL(pkey)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (3 != sscanf(pkey,
                      "{tid:%ld, partition_id:%ld, %*[part_idx|part_cnt]:%d",
                      &table_id,
                      &partition_idx,
                      &partition_cnt)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = init(table_id, partition_idx, partition_cnt);
  }
  return ret;
}

void ObPartitionKey::reset()
{
  table_id_ = OB_INVALID_ID;
  part_id_ = OB_INVALID_INDEX;
  assit_id_ = OB_INVALID_INDEX;
  hash_value_ = 0;
}

bool ObPartitionKey::is_valid() const
{
  return is_valid_id(table_id_) && part_id_ >= 0 && assit_id_ >= 0;
}

int ObPartitionKey::compare(const ObPartitionKey& other) const
{
  int cmp_ret = 0;

  if (table_id_ > other.table_id_) {
    cmp_ret = 1;
  } else if (table_id_ < other.table_id_) {
    cmp_ret = -1;
  } else if (part_id_ > other.part_id_) {
    cmp_ret = 1;
  } else if (part_id_ < other.part_id_) {
    cmp_ret = -1;
  } else {
    cmp_ret = (assit_id_ > other.assit_id_ ? 1 : (assit_id_ < other.assit_id_ ? -1 : 0));
  }
  return cmp_ret;
}

int64_t ObPartitionKey::get_partition_id() const
{
  int64_t partition_id = 0;
  if (!!is_twopart(part_id_, assit_id_)) {
    partition_id = combine_part_id(part_id_, assit_id_);
  } else {
    partition_id = part_id_;
  }
  return partition_id;
}

int32_t ObPartitionKey::get_partition_cnt() const
{
  return is_twopart(part_id_, assit_id_) ? 0 : assit_id_;
}

int64_t ObPartitionKey::get_part_idx() const
{
  int64_t part_idx = -1;
  if (!!is_twopart(part_id_, assit_id_)) {
    part_idx = extract_idx_from_partid(part_id_);
  } else {
    part_idx = part_id_;
  }
  return part_idx;
}

int64_t ObPartitionKey::get_subpart_idx() const
{
  int64_t subpart_idx = -1;
  if (is_twopart(part_id_, assit_id_) != 0) {
    subpart_idx = extract_idx_from_partid(subpart_id_);
  }
  return subpart_idx;
}

uint64_t ObPartitionKey::inner_hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&part_id_, sizeof(part_id_), hash_val);
  hash_val = murmurhash(&assit_id_, sizeof(assit_id_), hash_val);
  return hash_val;
}

uint64_t ObPartitionKey::hash() const
{
  return hash_value_;
}

uint64_t ObPartitionKey::get_tenant_id() const
{
  return extract_tenant_id(table_id_);
}

DEFINE_SERIALIZE(ObPartitionKey)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, table_id_))) {
    COMMON_LOG(WARN, "serialize table_id_ failed, ", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, part_id_))) {
    COMMON_LOG(WARN, "serialize part_id failed, ", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, assit_id_))) {
    COMMON_LOG(WARN, "serialize sub_part_id failed, ", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObPartitionKey)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&table_id_)))) {
    COMMON_LOG(WARN, "deserialize table_id failed, ", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &part_id_))) {
    COMMON_LOG(WARN, "deserialize part_id failed, ", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &assit_id_))) {
    COMMON_LOG(WARN, "deserialize sub_part_id failed, ", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    hash_value_ = inner_hash();
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPartitionKey)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(table_id_);
  size += serialization::encoded_length_i32(part_id_);
  size += serialization::encoded_length_i32(assit_id_);
  return size;
}

OB_SERIALIZE_MEMBER(ObPartitionLeaderArray, partitions_, leaders_, type_array_);

void ObPartitionLeaderArray::reset()
{
  partitions_.reset();
  leaders_.reset();
  type_array_.reset();
}

int ObPartitionLeaderArray::push(
    const ObPartitionKey& partition, const ObAddr& leader, const common::ObPartitionType& type)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !leader.is_valid()) {
    COMMON_LOG(WARN, "invalid argument", K(partition), K(leader));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partitions_.push_back(partition))) {
    COMMON_LOG(WARN, "push partition error", K(ret), K(partition));
  } else if (OB_FAIL(leaders_.push_back(leader))) {
    COMMON_LOG(WARN, "push leader error", K(ret), K(leader));
    // pop partition when push leader error
    partitions_.pop_back();
  } else if (OB_FAIL(type_array_.push_back(type))) {
    COMMON_LOG(WARN, "push partition type error", K(ret), K(type));
    // pop partition when push type error
    partitions_.pop_back();
    leaders_.pop_back();
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionLeaderArray::assign(const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;

  if (pla.count() < 0) {
    COMMON_LOG(WARN, "invalid argument", K(pla));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partitions_.assign(pla.get_partitions()))) {
    COMMON_LOG(WARN, "partitions assign error", K(ret), K(pla));
  } else if (OB_FAIL(leaders_.assign(pla.get_leaders()))) {
    COMMON_LOG(WARN, "leader assign error", K(ret), K(pla));
    partitions_.reset();
  } else if (OB_FAIL(type_array_.assign(pla.get_types()))) {
    COMMON_LOG(WARN, "type assign error", K(ret), K(pla));
    partitions_.reset();
    leaders_.reset();
  } else {
    // do nothing
  }

  return ret;
}

int64_t ObPartitionLeaderArray::count() const
{
  int64_t count = 0;
  if (partitions_.count() != leaders_.count() || partitions_.count() != type_array_.count()) {
    COMMON_LOG(ERROR,
        "unexpected error",
        "pkey_count",
        partitions_.count(),
        "leader_count",
        leaders_.count(),
        "types_count",
        type_array_.count());
  } else {
    count = partitions_.count();
  }

  return count;
}

bool ObPartitionLeaderArray::is_single_leader() const
{
  bool bool_ret = true;
  const int64_t count = leaders_.count();

  if (0 == count) {
    bool_ret = false;
  } else {
    const ObAddr& addr = leaders_[0];
    for (int64_t i = 1; bool_ret && (i < count); i++) {
      bool_ret = (addr == leaders_[i]);
    }
  }

  return bool_ret;
}

bool ObPartitionLeaderArray::is_single_partition() const
{
  bool bool_ret = true;
  const int64_t count = partitions_.count();

  if (0 == count) {
    bool_ret = false;
  } else {
    const ObPartitionKey& partition = partitions_[0];
    for (int64_t i = 1; bool_ret && (i < count); i++) {
      bool_ret = (partition == partitions_[i]);
    }
  }

  return bool_ret;
}

bool ObPartitionLeaderArray::is_single_tenant() const
{
  bool bool_ret = true;
  const int64_t count = partitions_.count();

  if (0 == count) {
    bool_ret = false;
  } else {
    const uint64_t tenant_id = partitions_[0].get_tenant_id();
    for (int64_t i = 1; bool_ret && (i < count); i++) {
      bool_ret = (tenant_id == partitions_[i].get_tenant_id());
    }
  }

  return bool_ret;
}

bool ObPartitionLeaderArray::has_duplicate_partition() const
{
  bool bool_ret = false;

  for (int64_t i = 0; i < type_array_.count(); i++) {
    if (ObPartitionType::DUPLICATE_LEADER_PARTITION == type_array_.at(i) ||
        ObPartitionType::DUPLICATE_FOLLOWER_PARTITION == type_array_.at(i)) {
      bool_ret = true;
      break;
    }
  }

  return bool_ret;
}

bool ObPartitionLeaderArray::has_duplicate_leader_partition() const
{
  bool bool_ret = false;

  for (int64_t i = 0; i < type_array_.count(); i++) {
    if (ObPartitionType::DUPLICATE_LEADER_PARTITION == type_array_.at(i)) {
      bool_ret = true;
      break;
    }
  }

  return bool_ret;
}

int ObPartitionLeaderArray::find_leader(const ObPartitionKey& part_key, ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && i < partitions_.count(); ++i) {
    if (part_key == partitions_.at(i)) {
      idx = i;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "can not find partition key", K(ret), K(part_key), K(partitions_));
    } else if (OB_UNLIKELY(idx < 0 || idx >= leaders_.count() || leaders_.count() != partitions_.count())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "invalid parameters", K(ret), K(idx), K(partitions_.count()), K(partitions_.count()));
    } else {
      leader = leaders_.at(idx);
    }
  }
  return ret;
}

int ObPartitionLeaderArray::is_duplicate_partition(const ObPartitionKey& key, bool& is_duplicate_partition) const
{
  int ret = OB_SUCCESS;

  int i = 0;
  for (; i < partitions_.count(); i++) {
    if (partitions_.at(i) == key) {
      break;
    }
  }

  if (partitions_.count() == i) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (i < 0 || i >= type_array_.count() || type_array_.count() != partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_duplicate_partition = (ObPartitionType::DUPLICATE_LEADER_PARTITION == type_array_.at(i) ||
                              ObPartitionType::DUPLICATE_FOLLOWER_PARTITION == type_array_.at(i));
  }

  return ret;
}

int ObPartitionKey::to_yson(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (!!is_twopart(part_id_, assit_id_)) {
    ret = yson::databuff_encode_elements(buf,
        buf_len,
        pos,
        OB_ID(tid),
        table_id_,
        OB_ID(partition_id),
        combine_part_id(part_id_, subpart_id_),
        OB_ID(part_idx),
        part_id_,
        OB_ID(subpart_idx),
        subpart_id_);
  } else {
    ret = yson::databuff_encode_elements(
        buf, buf_len, pos, OB_ID(tid), table_id_, OB_ID(partition_id), part_id_, OB_ID(part_cnt), part_cnt_);
  }
  return ret;
}

int64_t ObPartitionKey::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (!!is_twopart(part_id_, assit_id_)) {
    databuff_print_id_value(buf,
        buf_len,
        pos,
        OB_ID(tid),
        table_id_,
        OB_ID(partition_id),
        combine_part_id(part_id_, subpart_id_),
        OB_ID(part_idx),
        extract_idx_from_partid(part_id_),
        OB_ID(subpart_idx),
        extract_idx_from_partid(subpart_id_));
  } else {
    databuff_print_id_value(
        buf, buf_len, pos, OB_ID(tid), table_id_, OB_ID(partition_id), part_id_, OB_ID(part_cnt), part_cnt_);
  }
  J_OBJ_END();
  return pos;
}

int64_t ObPartitionKey::to_path_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = snprintf(buf, buf_len, "%lu-%d-%d", table_id_, part_id_, part_cnt_);
  return pos;
}

int ObPartitionKey::init(uint64_t tablegroup_id, int64_t partition_group_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_id(tablegroup_id) || !is_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid tablegroup id", K(ret), K(tablegroup_id), K(partition_group_id));
  } else if (partition_group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid partition group id", K(ret), K(tablegroup_id), K(partition_group_id));
  } else if (!!is_twopart(partition_group_id)) {
    tablegroup_id_ = tablegroup_id;
    part_id_ = static_cast<int32_t>(extract_part_id(partition_group_id));
    subpart_id_ = static_cast<int32_t>(extract_subpart_id(partition_group_id));
  } else {
    tablegroup_id_ = tablegroup_id;
    part_id_ = static_cast<int32_t>(partition_group_id);
    subpart_id_ = 0;
  }
  hash_value_ = inner_hash();
  return ret;
}

int ObPartitionKey::replace_pkey_tenant_id(
    const ObPartitionKey& pkey, const uint64_t new_tenant_id, ObPartitionKey& new_pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || !is_valid_tenant_id(new_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(pkey), K(new_tenant_id));
  } else {
    uint64_t new_table_id = combine_id(new_tenant_id, pkey.get_table_id());
    if (OB_FAIL(new_pkey.init(new_table_id, pkey.get_partition_id(), pkey.get_partition_cnt()))) {
      COMMON_LOG(WARN, "failed to init new_pkey", K(ret), K(pkey), K(new_tenant_id));
    }
  }
  return ret;
}

int ObPartitionKey::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  ObPartitionKey new_pkey;
  if (OB_FAIL(replace_pkey_tenant_id(*this, new_tenant_id, new_pkey))) {
    COMMON_LOG(WARN, "failed to replace_pkey_tenant_id", K(ret), K(new_tenant_id));
  } else {
    *this = new_pkey;
  }
  return ret;
}

}  // end namespace common
}  // end namespace oceanbase
