/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "src/share/vector_index/ob_plugin_vector_index_service.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
using namespace common;
namespace share
{
class ObVectorQueryVidIterator;
}
namespace sql
{

struct ObVecIndexBitmap
{
  static const uint64_t NORMAL_BITMAP_MAX_SIZE = 10000000;
  enum FilterType {
    BYTE_ARRAY = 0,
    ROARING_BITMAP = 1,
    VIDS = 2,
  };

  union {
    uint8_t *bitmap_;
    roaring::api::roaring64_bitmap_t *roaring_bitmap_;
    int64_t *vids_;
  };

  FilterType type_;
  uint64_t capacity_;
  uint64_t valid_cnt_;
  ObIAllocator *allocator_;
  int64_t min_vid_;
  int64_t max_vid_;
  int64_t min_vid_bound_;
  int64_t max_vid_bound_;
  uint64_t tenant_id_;

  ObVecIndexBitmap(ObIAllocator *allocator)
    : type_(VIDS), capacity_(0), valid_cnt_(0), allocator_(allocator),
      min_vid_(INT64_MAX), max_vid_(INT64_MIN), min_vid_bound_(INT64_MAX),
      max_vid_bound_(INT64_MIN), tenant_id_(MTL_ID())
  {
    vids_ = nullptr;
  }

  void set_allocator(ObIAllocator *allocator) { allocator_ = allocator; }
  int init(int64_t min_vid, int64_t max_vid, uint64_t capacity);
  int add_vid(int64_t vid);
  bool test(int64_t vid);
  int get_valid_cnt() { return valid_cnt_; }
  int64_t *get_vids() { return (type_ == VIDS) ? vids_ : nullptr; }
  void reset();


  int upgrade_to_byte_array();
  int upgrade_to_roaring_bitmap();
};

struct ObVecIndexBitmapIter
{
  ObVecIndexBitmapIter()
    : bitmap_(nullptr),
      iter_(nullptr),
      inited_(false),
      curr_vid_(OB_INVALID_INDEX)
  {}

  int init(ObVecIndexBitmap *bitmap);
  int advance_to(int64_t target_vid, int64_t &return_vid);
  int next_vid(int64_t &next_vid);
  int get_curr_vid(int64_t &curr_vid);
  void reset();
  bool is_inited() const { return inited_; }

private:
  ObVecIndexBitmap *bitmap_;
  roaring::api::roaring64_iterator_t *iter_;
  bool inited_;
  int64_t curr_vid_;
};


// Base class for vector index scan ctdef
struct ObDASVecIndexScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexScanCtDef(common::ObIAllocator &alloc, ObDASOpType op_type = DAS_OP_VEC_INDEX_SCAN)
    : ObDASAttachCtDef(alloc, op_type),
      vec_index_param_(),
      vector_index_param_(),
      algorithm_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
      vec_type_(ObVecIndexType::VEC_INDEX_INVALID),
      dim_(0),
      sort_expr_(nullptr) {}

  virtual ~ObDASVecIndexScanCtDef() = default;

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(vec_index_param), K_(vector_index_param), K_(algorithm_type),
                       K_(vec_type), K_(dim), K_(sort_expr));

  ObString vec_index_param_;
  ObVectorIndexQueryParam query_param_;
  ObVectorIndexParam vector_index_param_;

  ObVectorIndexAlgorithmType algorithm_type_;
  ObVecIndexType vec_type_;
  int64_t dim_;
  ObExpr *sort_expr_;
};

// HNSW specific ctdef, inherits from base class
struct ObDASVecIndexHNSWScanCtDef : ObDASVecIndexScanCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexHNSWScanCtDef(common::ObIAllocator &alloc)
    : ObDASVecIndexScanCtDef(alloc, DAS_OP_VEC_INDEX_HNSW_SCAN) {}

  virtual ~ObDASVecIndexHNSWScanCtDef() = default;

  // HNSW specific child table indices
  static const int64_t DELTA_BUF_TABLE_IDX = 0;
  static const int64_t INDEX_ID_TABLE_IDX = 1;
  static const int64_t SNAPSHOT_TABLE_IDX = 2;
  static const int64_t COM_AUX_VEC_TABLE_IDX = 3;

  const ObDASScanCtDef *get_child_ctdef(int64_t index, ObTSCIRScanType scan_type) const
  {
    const ObDASScanCtDef *child_ctdef = nullptr;
    if (children_cnt_ > index && index >= 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[index]);
      if (child->ir_scan_type_ == scan_type) {
        child_ctdef = child;
      }
    }
    return child_ctdef;
  }

  const ObDASScanCtDef *get_delta_buf_table_ctdef() const
  {
    return get_child_ctdef(DELTA_BUF_TABLE_IDX, ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN);
  }

  const ObDASScanCtDef *get_index_id_table_ctdef() const
  {
    return get_child_ctdef(INDEX_ID_TABLE_IDX, ObTSCIRScanType::OB_VEC_IDX_ID_SCAN);
  }

  const ObDASScanCtDef *get_snapshot_table_ctdef() const
  {
    return get_child_ctdef(SNAPSHOT_TABLE_IDX, ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN);
  }

  const ObDASScanCtDef *get_com_aux_vec_table_ctdef() const
  {
    return get_child_ctdef(COM_AUX_VEC_TABLE_IDX, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  }
};

// Base class for vector index scan rtdef
struct ObDASVecIndexScanRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexScanRtDef(ObDASOpType op_type = DAS_OP_VEC_SCAN)
    : ObDASAttachRtDef(op_type) {}

  virtual ~ObDASVecIndexScanRtDef() = default;
};

// HNSW specific rtdef, inherits from base class
struct ObDASVecIndexHNSWScanRtDef : ObDASVecIndexScanRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexHNSWScanRtDef()
    : ObDASVecIndexScanRtDef(DAS_OP_VEC_INDEX_HNSW_SCAN) {}

  virtual ~ObDASVecIndexHNSWScanRtDef() = default;

  ObDASScanRtDef *get_child_rtdef(int64_t index) const
  {
    ObDASScanRtDef *child_rtdef = nullptr;
    if (children_cnt_ > index && index >= 0 && children_ != nullptr) {
      ObDASScanRtDef *child = static_cast<ObDASScanRtDef *>(children_[index]);
      child_rtdef = child;
    }
    return child_rtdef;
  }

  ObDASScanRtDef *get_delta_buf_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::DELTA_BUF_TABLE_IDX);
  }

  ObDASScanRtDef *get_index_id_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::INDEX_ID_TABLE_IDX);
  }

  ObDASScanRtDef *get_snapshot_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::SNAPSHOT_TABLE_IDX);
  }

  ObDASScanRtDef *get_com_aux_vec_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::COM_AUX_VEC_TABLE_IDX);
  }
};

class ObDASVecIndexScanIter : public ObDASIter
{
public:
  ObDASVecIndexScanIter(ObDASIterType type)
    : ObDASIter(type),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      go_brute_force_(false),
      bitmap_(nullptr),
      limit_(0),
      search_ctx_(nullptr) {}
  virtual ~ObDASVecIndexScanIter() {}

  void set_bitmap(ObVecIndexBitmap *bitmap) { bitmap_ = bitmap; }

  ObVecIndexType get_index_type() const { return vec_index_type_; }
  void set_vec_index_type(ObVecIndexType vec_index_type, ObVecIdxAdaTryPath vec_idx_try_path, bool go_brute_force) {
    vec_index_type_ = vec_index_type;
    vec_idx_try_path_ = vec_idx_try_path;
    go_brute_force_ = go_brute_force;
  }

  void set_limit(uint64_t limit) { limit_ = limit; }
  virtual void set_adaptor(share::ObPluginVectorIndexAdaptor *adaptor) = 0;
  virtual void set_vector_query_condition(ObVectorQueryConditions *query_cond) = 0;
  virtual void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids) = 0;
  virtual share::ObVectorQueryVidIterator* get_adaptor_vid_iter() { return nullptr; }
  virtual int do_table_scan() override { return OB_SUCCESS; }
  virtual int rescan() override { return OB_SUCCESS; }
  virtual void clear_evaluated_flag() override {}

protected:
  virtual int inner_get_next_row() override = 0;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override = 0;

protected:
  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;
  bool go_brute_force_;
  ObVecIndexBitmap *bitmap_;
  uint64_t limit_;
  ObDASSearchCtx *search_ctx_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_SCAN_ITER_H_ */
