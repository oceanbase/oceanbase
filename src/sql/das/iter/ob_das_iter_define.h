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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_DEFINE_H
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_DEFINE_H

#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace sql
{

enum ObDASIterType : uint32_t
{
  DAS_ITER_INVALID = 0,
  DAS_ITER_SCAN,
  DAS_ITER_MERGE,
  DAS_ITER_GROUP_FOLD,
  DAS_ITER_LOCAL_LOOKUP,
  DAS_ITER_GLOBAL_LOOKUP,
  DAS_ITER_TEXT_RETRIEVAL,
  DAS_ITER_SORT,
  DAS_ITER_TEXT_RETRIEVAL_MERGE,
  // append DASIterType before me
  DAS_ITER_MAX
};

#define IS_LOOKUP_ITER(_iter_type)                    \
({                                                    \
    DAS_ITER_LOCAL_LOOKUP == (_iter_type)         ||  \
    DAS_ITER_GLOBAL_LOOKUP == (_iter_type);           \
})

enum MergeType : uint32_t {
  SEQUENTIAL_MERGE = 0,
  SORT_MERGE
};

// group fold iter is used on demand and not considered as a part of iter tree,
// it is placed above iter tree when in use.
enum ObDASIterTreeType : uint32_t
{
  ITER_TREE_INVALID = 0,
  ITER_TREE_TABLE_SCAN,
  ITER_TREE_GLOBAL_LOOKUP,
  ITER_TREE_PARTITION_SCAN,
  ITER_TREE_LOCAL_LOOKUP,
  ITER_TREE_GIS_LOOKUP,
  ITER_TREE_DOMAIN_LOOKUP,
  ITER_TREE_TEXT_RETRIEVAL,
  // append iter tree type before me
  ITER_TREE_MAX
};

struct ObDASRelatedTabletID
{
public:
  common::ObTabletID lookup_tablet_id_;
  common::ObTabletID aux_lookup_tablet_id_;

  /* used by fulltext index */
  common::ObTabletID inv_idx_tablet_id_;
  common::ObTabletID fwd_idx_tablet_id_;
  common::ObTabletID doc_id_idx_tablet_id_;
  /* used by fulltext index */
  void reset()
  {
    lookup_tablet_id_.reset();
    aux_lookup_tablet_id_.reset();
    inv_idx_tablet_id_.reset();
    fwd_idx_tablet_id_.reset();
    doc_id_idx_tablet_id_.reset();
  }
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_DEFINE_H */
