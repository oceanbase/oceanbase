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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_GROUP_INDEX_
#define OCEANBASE_STORAGE_OB_PARTITION_GROUP_INDEX_

#include "common/ob_partition_key.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace storage {
typedef common::LinkHashNode<common::ObPartitionKey> ObPGHashNode;
typedef common::LinkHashValue<common::ObPartitionKey> ObPGHashValue;

class ObPGKeyWrap : public ObPGHashValue {
public:
  ObPGKeyWrap()
  {}
  ~ObPGKeyWrap()
  {
    reset();
  }
  int init(const common::ObPGKey& pg_key);
  void reset()
  {
    pg_key_.reset();
  }
  uint64_t hash() const
  {
    return pg_key_.hash();
  }
  int compare(const common::ObPGKey& other) const
  {
    return pg_key_.compare(other);
  }
  const common::ObPGKey& get_pg_key() const
  {
    return pg_key_;
  }
  TO_STRING_KV(K_(pg_key));

private:
  common::ObPGKey pg_key_;
};

class PGKeyInfoAlloc {
public:
  static ObPGKeyWrap* alloc_value()
  {
    return op_alloc(ObPGKeyWrap);
  }
  static void free_value(ObPGKeyWrap* p)
  {
    if (NULL != p) {
      op_free(p);
      p = NULL;
    }
  }
  static ObPGHashNode* alloc_node(ObPGKeyWrap* p)
  {
    UNUSED(p);
    return op_alloc(ObPGHashNode);
  }
  static void free_node(ObPGHashNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

// static const int64_t SHRINK_THRESHOLD = 128;
typedef common::ObLinkHashMap<common::ObPartitionKey, ObPGKeyWrap, PGKeyInfoAlloc> ObPGIndexMap;

class ObPartitionGroupIndex {
public:
  ObPartitionGroupIndex();
  virtual ~ObPartitionGroupIndex()
  {
    destroy();
  }
  int init();
  void destroy();

  int add_partition(const common::ObPartitionKey& pkey, const common::ObPGKey& pg_key);
  int remove_partition(const common::ObPartitionKey& pkey);
  int get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupIndex);

private:
  bool is_inited_;
  ObPGIndexMap pg_index_map_;
  mutable lib::ObMutex change_mutex_;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PARTITION_GROUP_INDEX_
