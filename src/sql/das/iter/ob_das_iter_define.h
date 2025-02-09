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
  DAS_ITER_VEC_VID_MERGE, /* abandoned */
  DAS_ITER_INDEX_MERGE,
  DAS_ITER_DOC_ID_MERGE, /* abandoned */
  DAS_ITER_FUNC_LOOKUP,
  DAS_ITER_FUNC_DATA,
  DAS_ITER_MVI_LOOKUP,
  DAS_ITER_HNSW_SCAN,
  DAS_ITER_DOMAIN_ID_MERGE,
  DAS_ITER_IVF_SCAN,
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
  ITER_TREE_DOMAIN_LOOKUP,  // discarded
  ITER_TREE_TEXT_RETRIEVAL,
  ITER_TREE_INDEX_MERGE,
  ITER_TREE_FUNC_LOOKUP,
  ITER_TREE_MVI_LOOKUP,
  ITER_TREE_VEC_LOOKUP,
  // append iter tree type before me
  ITER_TREE_MAX
};

struct ObDASFTSTabletID
{
public:
  ObDASFTSTabletID()
    : inv_idx_tablet_id_(),
      fwd_idx_tablet_id_(),
      doc_id_idx_tablet_id_()
  {}
  common::ObTabletID inv_idx_tablet_id_;
  common::ObTabletID fwd_idx_tablet_id_;
  common::ObTabletID doc_id_idx_tablet_id_;

  void reset()
  {
    inv_idx_tablet_id_.reset();
    fwd_idx_tablet_id_.reset();
    doc_id_idx_tablet_id_.reset();
  }
  TO_STRING_KV(K_(inv_idx_tablet_id), K_(fwd_idx_tablet_id), K_(doc_id_idx_tablet_id));
};

#define SUPPORTED_DAS_ITER_TREE(_type)                    \
({                                                       \
    ITER_TREE_PARTITION_SCAN == (_type) ||               \
    ITER_TREE_LOCAL_LOOKUP == (_type)   ||               \
    ITER_TREE_TEXT_RETRIEVAL == (_type) ||               \
    ITER_TREE_FUNC_LOOKUP == (_type)    ||               \
    ITER_TREE_INDEX_MERGE == (_type)    ||               \
    ITER_TREE_MVI_LOOKUP == (_type)     ||               \
    ITER_TREE_VEC_LOOKUP == (_type)     ||               \
    ITER_TREE_GIS_LOOKUP == (_type);                     \
})

struct ObDASRelatedTabletID
{
public:
  ObDASRelatedTabletID(common::ObIAllocator &alloc)
    : index_merge_tablet_ids_(alloc),
      domain_tablet_ids_(alloc)
  { reset(); }

  common::ObTabletID lookup_tablet_id_;
  common::ObTabletID doc_rowkey_tablet_id_;
  common::ObTabletID rowkey_doc_tablet_id_;
  common::ObTabletID rowkey_vid_tablet_id_;

  /* used by basic fulltext index */
  common::ObTabletID inv_idx_tablet_id_;
  common::ObTabletID fwd_idx_tablet_id_;
  common::ObTabletID doc_id_idx_tablet_id_;
  /* used by basic fulltext index */

  /* used by index merge */
  common::ObFixedArray<common::ObTabletID, ObIAllocator> index_merge_tablet_ids_;
  /* used by index merge */

  /* record tablet ids for fulltext index, use ir_rtdef->fts_idx_ to locate */
  common::ObSEArray<ObDASFTSTabletID, 2> fts_tablet_ids_;
  /* record tablet ids for fulltext index */

  /* used by domain id merge */
  common::ObFixedArray<common::ObTabletID, ObIAllocator> domain_tablet_ids_;
  /* used by domain id merge */

  /* used by vector index */
  common::ObTabletID delta_buf_tablet_id_;
  common::ObTabletID index_id_tablet_id_;
  common::ObTabletID snapshot_tablet_id_;
    // for ivf
  common::ObTabletID centroid_tablet_id_;
  common::ObTabletID cid_vec_tablet_id_;
  common::ObTabletID rowkey_cid_tablet_id_;
  common::ObTabletID special_aux_tablet_id_;
  /* used by vector index */

  void reset()
  {
    lookup_tablet_id_.reset();
    doc_rowkey_tablet_id_.reset();
    rowkey_doc_tablet_id_.reset();
    rowkey_vid_tablet_id_.reset();
    inv_idx_tablet_id_.reset();
    fwd_idx_tablet_id_.reset();
    doc_id_idx_tablet_id_.reset();
    index_merge_tablet_ids_.reset();
    fts_tablet_ids_.reset();
    domain_tablet_ids_.reset();
    delta_buf_tablet_id_.reset();
    index_id_tablet_id_.reset();
    snapshot_tablet_id_.reset();
  }
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_DEFINE_H */
