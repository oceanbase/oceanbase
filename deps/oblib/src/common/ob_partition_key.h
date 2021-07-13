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

#ifndef OCEANBASE_COMMON_OB_PARTITION_KEY_
#define OCEANBASE_COMMON_OB_PARTITION_KEY_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_name_id_def.h"

namespace oceanbase {
namespace common {
struct ObPartitionKey final {
public:
  ObPartitionKey() : table_id_(OB_INVALID_ID), part_id_(OB_INVALID_INDEX), assit_id_(OB_INVALID_INDEX), hash_value_(0)
  {}

  ObPartitionKey(const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt)
      : table_id_(OB_INVALID_ID), part_id_(OB_INVALID_INDEX), part_cnt_(OB_INVALID_INDEX), hash_value_(0)
  {
    init(table_id, partition_id, partition_cnt);
  }
  int init(const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt);
  // int init_with_part_idx(const uint64_t table_id, const int32_t part_idx,
  //                       const int32_t subpart_idx, const int32_t partition_cnt);
  int parse(const char* str);
  int parse_from_cstring(const char* str);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  uint64_t inner_hash() const;
  int compare(const ObPartitionKey& other) const;
  bool operator==(const ObPartitionKey& other) const
  {
    return (table_id_ == other.table_id_ && part_id_ == other.part_id_ && assit_id_ == other.assit_id_);
  }
  bool operator!=(const ObPartitionKey& other) const
  {
    return !operator==(other);
  }
  bool operator<(const ObPartitionKey& other) const
  {
    return -1 == compare(other);
  }
  bool veq(const ObPartitionKey& key) const
  {
    return 0 == compare(key);
  }
  bool ideq(const ObPartitionKey& key) const
  {
    return 0 == compare(key);
  }
  uint64_t get_tenant_id() const;
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  int64_t get_partition_id() const;
  int32_t get_partition_cnt() const;
  int64_t get_part_idx() const;
  int64_t get_subpart_idx() const;
  int to_yson(char* buf, const int64_t buf_len, int64_t& pos) const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int64_t to_path_string(char* buf, const int64_t buf_len) const;

  // partition group key
  int init(uint64_t tablegroup_id, int64_t partition_group_id);
  bool is_pg() const
  {
    return is_tablegroup_id(tablegroup_id_);
  }
  uint64_t get_tablegroup_id() const
  {
    return tablegroup_id_;
  }
  uint64_t get_partition_group_id() const
  {
    return get_partition_id();
  }
  bool is_trans_table() const
  {
    return is_trans_table_id(table_id_);
  }
  int generate_trans_table_pkey(const ObPartitionKey& pkey)
  {
    int ret = OB_SUCCESS;
    table_id_ = transform_trans_table_id(pkey.table_id_);
    part_id_ = pkey.part_id_;
    assit_id_ = pkey.assit_id_;
    hash_value_ = inner_hash();
    return ret;
  }
  static int replace_pkey_tenant_id(const ObPartitionKey& pkey, const uint64_t new_tenant_id, ObPartitionKey& new_pkey);
  int replace_tenant_id(const uint64_t new_tenant_id);

  NEED_SERIALIZE_AND_DESERIALIZE;

  // After version 2.0, reuse partition key to represent partition group key
  // Use table_id to represent table_group_id, the encoding method of table_id and table_group_id has changed,
  // The highest bit of the 40 bits is 1, and this bit is used to identify PG and Partition
  // The encoding method of partition_group_id is consistent with partition_id
  union {
    uint64_t table_id_;  // Indicates the ID of the table
    uint64_t rg_id_;
    uint64_t tablegroup_id_;
  };

private:
  int32_t part_id_;  // First level part_id
  union {
    int32_t subpart_id_;  // Secondary part_id
    int32_t part_cnt_;    // Part_cnt is recorded in the first-level partition
    int32_t assit_id_;    // Assistance information, when the highest bit is 1, it means the subpart_id of the secondary
                          // partition, The highest bit is 0, indicating that the key is the first-level partition
                          // part_cnt
  };
  uint64_t hash_value_;
};

enum class ObPartitionType : int { NORMAL_PARTITION, DUPLICATE_LEADER_PARTITION, DUPLICATE_FOLLOWER_PARTITION };

////////////////////////////////////////////////////////////////
// Replication Group
typedef ObPartitionKey ObPGKey;
static const int64_t OB_DEFAULT_PARTITION_KEY_COUNT = 3;
typedef ObSEArray<ObPartitionKey, OB_DEFAULT_PARTITION_KEY_COUNT> ObPartitionArray;
typedef ObIArray<ObPartitionKey> ObPartitionIArray;
typedef ObSEArray<ObPartitionType, OB_DEFAULT_PARTITION_KEY_COUNT> ObPartitionTypeArray;

class ObPartitionLeaderArray {
  OB_UNIS_VERSION(1);

public:
  ObPartitionLeaderArray()
  {}
  ~ObPartitionLeaderArray()
  {}
  void reset();

public:
  int push(const common::ObPartitionKey& partition, const common::ObAddr& leader,
      const common::ObPartitionType& type = ObPartitionType::NORMAL_PARTITION);
  int assign(const ObPartitionLeaderArray& pla);
  const common::ObPartitionArray& get_partitions() const
  {
    return partitions_;
  }
  const common::ObAddrArray& get_leaders() const
  {
    return leaders_;
  }
  const common::ObPartitionTypeArray& get_types() const
  {
    return type_array_;
  }
  int64_t count() const;
  bool is_single_leader() const;
  bool is_single_partition() const;
  bool is_single_tenant() const;
  bool has_duplicate_partition() const;
  bool has_duplicate_leader_partition() const;
  int find_leader(const ObPartitionKey& part_key, common::ObAddr& leader) const;
  int is_duplicate_partition(const ObPartitionKey& key, bool& is_duplicate_partition) const;

  TO_STRING_KV(K_(partitions), K_(leaders), K_(type_array));

private:
  common::ObPartitionArray partitions_;
  common::ObAddrArray leaders_;
  common::ObPartitionTypeArray type_array_;
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_PARTITION_KEY_
