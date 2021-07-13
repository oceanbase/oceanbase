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

#ifndef OB_PARTITION_MODIFY_H
#define OB_PARTITION_MODIFY_H

#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_serialization.h"
#include "common/ob_partition_key.h"
#include "schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {
enum PartitionSplitType { SPLIT_INVALID = 0, TABLE_SPLIT, PARTITION_SPLIT };

enum ObSplitProgress {
  UNKNOWN_SPLIT_PROGRESS = -1,
  IN_SPLITTING = 0,
  LOGICAL_SPLIT_FINISH = 1,
  PHYSICAL_SPLIT_FINISH = 2,
  NEED_NOT_SPLIT = 10,
};

struct ObSplitInfo {
public:
  PartitionSplitType split_type_;
  schema::ObPartitionFuncType part_type_;
  uint64_t table_id_;
  uint64_t partition_id_;
  common::ObSEArray<uint64_t, 1> source_part_ids_;
  ObSplitInfo()
      : split_type_(SPLIT_INVALID),
        part_type_(schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
        table_id_(common::OB_INVALID_ID),
        partition_id_(common::OB_INVALID_ID)
  {}
  ~ObSplitInfo()
  {}
  int assign(const ObSplitInfo& other);
  TO_STRING_KV(K_(split_type), K_(part_type), K_(table_id), K_(partition_id), K_(source_part_ids));
  void reset();
  void set_split_info(PartitionSplitType split_type, share::schema::ObPartitionFuncType part_type);
  bool is_split_table() const;
  bool is_split_range_partition() const;
  bool is_split_hash_partition() const;
  bool is_split_list_partition() const;
};

class ObSplitPartitionPair {
  OB_UNIS_VERSION(1);

public:
  ObSplitPartitionPair() : src_pkey_(), dest_pkey_array_()
  {}
  ~ObSplitPartitionPair()
  {}
  int init(const common::ObPartitionKey& src_pkey, const common::ObIArray<common::ObPartitionKey>& dest_pkey_array);
  bool is_valid() const;
  void reset()
  {
    src_pkey_.reset();
    dest_pkey_array_.reset();
  }
  int assign(const ObSplitPartitionPair& other);
  bool is_source_partition(const common::ObPartitionKey& pkey) const;
  bool is_dest_partition(const common::ObPartitionKey& pkey) const;
  const common::ObPartitionKey& get_source_pkey() const
  {
    return src_pkey_;
  }
  common::ObPartitionKey& get_source_pkey()
  {
    return src_pkey_;
  }
  const common::ObIArray<common::ObPartitionKey>& get_dest_array() const
  {
    return dest_pkey_array_;
  }
  common::ObIArray<common::ObPartitionKey>& get_dest_array()
  {
    return dest_pkey_array_;
  }
  int replace_tenant_id(const uint64_t new_tenant_id);
  TO_STRING_KV(K_(src_pkey), K_(dest_pkey_array));

private:
  // Split source
  common::ObPartitionKey src_pkey_;
  // Split destination
  common::ObSArray<common::ObPartitionKey> dest_pkey_array_;
};

class ObSplitPartition {
  OB_UNIS_VERSION(1);

public:
  ObSplitPartition() : split_info_(), schema_version_(0)
  {}
  ~ObSplitPartition()
  {}
  bool is_valid() const;
  void reset();
  int assign(const ObSplitPartition& other);
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  const common::ObIArray<ObSplitPartitionPair>& get_spp_array() const
  {
    return split_info_;
  }
  common::ObIArray<ObSplitPartitionPair>& get_spp_array()
  {
    return split_info_;
  }
  int replace_tenant_id(const uint64_t new_tenant_id);
  TO_STRING_KV(K_(split_info), K_(schema_version));

private:
  // All copies of information that need to be split;
  common::ObSArray<ObSplitPartitionPair> split_info_;
  // The bottom layer needs to check that the schema_version is consistent with the schema_verion of the partition;
  int64_t schema_version_;
};

class ObPartitionSplitProgress {
  OB_UNIS_VERSION(1);

public:
  ObPartitionSplitProgress() : pkey_(), progress_(static_cast<int>(UNKNOWN_SPLIT_PROGRESS))
  {}
  ObPartitionSplitProgress(const common::ObPartitionKey& pkey, const int progress) : pkey_(pkey), progress_(progress)
  {}
  ~ObPartitionSplitProgress()
  {}
  const common::ObPartitionKey& get_pkey() const
  {
    return pkey_;
  }
  int get_progress() const
  {
    return progress_;
  }
  TO_STRING_KV(K_(pkey), K_(progress));

private:
  common::ObPartitionKey pkey_;
  int progress_;
};

}  // namespace share

}  // end namespace oceanbase

#endif /* OB_PARTITION_MODIFY_H */
