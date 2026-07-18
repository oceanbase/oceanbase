/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 #ifndef OCEANBASE_SHARE_VECTOR_INDEX_COMMON_DEFINE_H_
 #define OCEANBASE_SHARE_VECTOR_INDEX_COMMON_DEFINE_H_

#include "common/ob_tablet_id.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

constexpr static int64_t  OB_VSAG_MAX_EF_SEARCH = 160000;
constexpr static int64_t  OB_VECTOR_INDEX_MAX_SEGMENT_CNT = 10;
constexpr static int64_t  OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH = 256;
constexpr static int64_t  OB_VECTOR_INDEX_MERGE_BASE_PERCENTAGE = 20;

enum ObVectorIndexRecordType
{
  VIRT_INC, // increment index
  VIRT_BITMAP,
  VIRT_SNAP, // snapshot index
  VIRT_DATA, // data tablet/table
  VIRT_EMBEDDED, // embedded table
  VIRT_MAX
};

enum ObCanSkip3rdAnd4thVecIndex
{
  NOT_INITED,
  SKIP,
  NOT_SKIP,
  SKIPMAX
};

// id to unique identify a vector index adapter
struct ObPluginVectorIndexIdentity
{
  ObPluginVectorIndexIdentity() : data_tablet_id_(), index_identity_() {};
  ObPluginVectorIndexIdentity(common::ObTabletID data_tablet_id, common::ObString index_identity)
    : data_tablet_id_(data_tablet_id), index_identity_(index_identity)
  {}
  ~ObPluginVectorIndexIdentity()
  {
    data_tablet_id_.reset();
    index_identity_.reset();
  }
  bool is_valid() { return data_tablet_id_.is_valid() && !index_identity_.empty(); }
  uint64_t hash() const
  {
    int64_t hash_value = data_tablet_id_.hash();
    hash_value += index_identity_.hash();
    return hash_value;
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObPluginVectorIndexIdentity &other) const
  {
    return data_tablet_id_ == other.data_tablet_id_ && index_identity_ == other.index_identity_;
  }
  TO_STRING_KV(K_(data_tablet_id), K_(index_identity));

  common::ObTabletID data_tablet_id_;
  common::ObString index_identity_; // index_name_prefix
};

}  // namespace share
}  // namespace oceanbase

#endif