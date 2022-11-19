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

#ifndef OB_MACRO_BLOCK_META_MGR_H_
#define OB_MACRO_BLOCK_META_MGR_H_
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace blocksstable
{
// Only support major sstable key whose data_version_ is not 0.
struct ObMajorMacroBlockKey
{
  ObMajorMacroBlockKey() { reset(); }
  bool is_valid() const { return table_id_ > 0 && partition_id_ >= 0 && data_version_ > 0 && data_seq_ >= 0; }
  uint64_t hash() const;
  void reset();
  bool operator ==(const ObMajorMacroBlockKey &key) const;
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(data_version), K_(data_seq));

  uint64_t table_id_;
  int64_t partition_id_;
  int64_t data_version_;
  int64_t data_seq_;
};
}
}

#endif /* OB_MACRO_BLOCK_META_MGR_H_ */
