/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_macro_block_meta_mgr.h"

namespace oceanbase
{
namespace blocksstable
{
/**
 * ------------------------ObMajorMacroBlockKey-----------------------------
 */
uint64_t ObMajorMacroBlockKey::hash() const
{
  uint64_t hash_value = 0;

  hash_value = common::murmurhash(&table_id_, sizeof(table_id_), hash_value);
  hash_value = common::murmurhash(&partition_id_, sizeof(partition_id_), hash_value);
  hash_value = common::murmurhash(&data_version_, sizeof(data_version_), hash_value);
  hash_value = common::murmurhash(&data_seq_, sizeof(data_seq_), hash_value);
  return hash_value;
}

void ObMajorMacroBlockKey::reset()
{
  table_id_ = 0;
  partition_id_ = -1;
  data_version_ = 0;
  data_seq_ = -1;
}

bool ObMajorMacroBlockKey::operator ==(const ObMajorMacroBlockKey &key) const
{
  return table_id_ == key.table_id_
      && partition_id_ == key.partition_id_
      && data_version_ == key.data_version_
      && data_seq_ == key.data_seq_;
}

}
}
