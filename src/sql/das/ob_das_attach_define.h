/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * ob_das_attach_define.h
 *
 *      Author: yuming<>
 */
#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_ATTACH_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_ATTACH_DEFINE_H_
#include "sql/das/ob_das_define.h"
#include "share/ob_define.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{
struct ObDASScanCtDef;
struct ObDASScanRtDef;

struct ObDASAttachCtDef : ObDASBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ExprFixedArray result_output_;
protected:
  ObDASAttachCtDef(common::ObIAllocator &allocator, ObDASOpType op_type)
    : ObDASBaseCtDef(op_type),
      result_output_(allocator)
  {
  }
};

struct ObDASAttachRtDef : ObDASBaseRtDef
{
  OB_UNIS_VERSION(1);
protected:
  ObDASAttachRtDef(ObDASOpType op_type)
    : ObDASBaseRtDef(op_type)
  {
  }
};

struct ObDASTableLookupCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASTableLookupCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_TABLE_LOOKUP),
      is_global_index_(false)
  {
  }
  const ObDASBaseCtDef *get_rowkey_scan_ctdef() const
  {
    OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
    return children_[0];
  }
  const ObDASScanCtDef *get_lookup_scan_ctdef() const;

public:
  bool is_global_index_;
};

struct ObDASTableLookupRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASTableLookupRtDef()
    : ObDASAttachRtDef(DAS_OP_TABLE_LOOKUP)
  {}

  virtual ~ObDASTableLookupRtDef() {}

  ObDASBaseRtDef *get_rowkey_scan_rtdef()
  {
    OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
    return children_[0];
  }
  ObDASScanRtDef *get_lookup_scan_rtdef();
};

struct ObDASSortCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASSortCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_SORT),
      sort_exprs_(alloc),
      sort_collations_(alloc),
      sort_cmp_funcs_(alloc),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      fetch_with_ties_(false) {}

  virtual ~ObDASSortCtDef() {}
public:
  ExprFixedArray sort_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  ObExpr *limit_expr_;
  ObExpr *offset_expr_;
  bool fetch_with_ties_;
};

struct ObDASSortRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASSortRtDef()
    : ObDASAttachRtDef(DAS_OP_SORT) {}

  virtual ~ObDASSortRtDef() {}
};

struct ObDASAttachSpec
{
  OB_UNIS_VERSION(1);
public:
  ObDASAttachSpec(common::ObIAllocator &alloc, ObDASBaseCtDef *scan_ctdef)
    : attach_loc_metas_(alloc),
      scan_ctdef_(nullptr),
      allocator_(alloc),
      attach_ctdef_(nullptr)
  {
  }
  common::ObList<ObDASTableLocMeta*, common::ObIAllocator> attach_loc_metas_;
  ObDASBaseCtDef *scan_ctdef_; //This ctdef represents the main task information executed by the DAS Task.
  common::ObIAllocator &allocator_;
  ObDASBaseCtDef *attach_ctdef_; //The attach_ctdef represents the task information that is bound to and executed on the DAS Task.

  const ObDASTableLocMeta *get_attach_loc_meta(int64_t table_location_id, int64_t ref_table_id) const;
  int set_calc_exprs(const ExprFixedArray &calc_exprs, const int64_t max_batch_size);
  TO_STRING_KV(K_(attach_loc_metas),
               K_(attach_ctdef));
private:
  int serialize_ctdef_tree(char *buf,
                           const int64_t buf_len,
                           int64_t &pos,
                           const ObDASBaseCtDef *root) const;
  int64_t get_ctdef_tree_serialize_size(const ObDASBaseCtDef *root) const;
  int deserialize_ctdef_tree(const char *buf,
                             const int64_t data_len,
                             int64_t &pos,
                             ObDASBaseCtDef *&root);
  int set_calc_exprs_tree(ObDASAttachCtDef *root,
                          const ExprFixedArray &calc_exprs,
                          const int64_t max_batch_size);
};

struct ObDASAttachRtInfo
{
  ObDASAttachRtInfo()
    : pushdown_tasks_(),
      attach_rtdef_(nullptr),
      related_scan_cnt_(0)
  { }
  common::ObSEArray<ObDASBaseRtDef*, 2> pushdown_tasks_;
  ObDASBaseRtDef *attach_rtdef_;
  int64_t related_scan_cnt_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_ATTACH_DEFINE_H_ */
