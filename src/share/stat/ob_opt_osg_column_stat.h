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

#ifndef _OB_OPT_OSG_COLUMN_STAT_H_
#define _OB_OPT_OSG_COLUMN_STAT_H_

#include <stdint.h>
#include "lib/allocator/ob_malloc.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/rc/ob_tenant_base.h"


namespace oceanbase {
namespace common {

enum ValEvalType {
  T_INVALID_VAL = -1,
  T_MIN_VAL,
  T_MAX_VAL
};

struct ObMinMaxValEval {
  ObMinMaxValEval(ValEvalType eval_type):
    val_type_(eval_type),
    datum_(NULL),
    meta_(),
    cmp_func_(NULL)
  { }
  void reset()
  {
    val_type_ = T_INVALID_VAL;
    if (datum_ != NULL) {
      datum_->reset();
    }
    meta_.reset();
  }
  inline bool is_valid() const { return datum_ != NULL && cmp_func_ != NULL; }
  int get_obj(ObObj &obj) const;
  int deep_copy(const ObMinMaxValEval &other, ObIAllocator &alloc);

  ValEvalType val_type_;
  ObDatum *datum_;
  ObObjMeta meta_;
  ObDatumCmpFuncType cmp_func_;
  TO_STRING_KV(K_(val_type),
               K_(datum),
               K_(meta));
};

class ObOptOSGColumnStat
{
  OB_UNIS_VERSION_V(1);
public:
  ObOptOSGColumnStat():
    col_stat_(NULL),
    min_val_(T_MIN_VAL),
    max_val_(T_MAX_VAL),
    inner_min_allocator_("OptOSGMin"),
    inner_max_allocator_("OptOSGMax"),
    inner_allocator_("OptOSG"),
    allocator_(inner_allocator_)
  {
    inner_min_allocator_.set_tenant_id(MTL_ID());
    inner_max_allocator_.set_tenant_id(MTL_ID());
    inner_allocator_.set_tenant_id(MTL_ID());
  }
  ObOptOSGColumnStat(ObIAllocator &alloc):
    col_stat_(NULL),
    min_val_(T_MIN_VAL),
    max_val_(T_MAX_VAL),
    inner_min_allocator_("OptOSGMin"),
    inner_max_allocator_("OptOSGMax"),
    inner_allocator_("OptOSG"),
    allocator_(alloc)
  {
    inner_min_allocator_.set_tenant_id(MTL_ID());
    inner_max_allocator_.set_tenant_id(MTL_ID());
    inner_allocator_.set_tenant_id(MTL_ID());
  }
  virtual ~ObOptOSGColumnStat() { reset(); }
  void reset();
  int deep_copy(const ObOptOSGColumnStat &other);
  static ObOptOSGColumnStat* create_new_osg_col_stat(common::ObIAllocator &allocator);
  int get_min_obj(ObObj &obj);
  int get_max_obj(ObObj &obj);
  int set_min_max_datum_to_obj();
  int merge_column_stat(const ObOptOSGColumnStat &other);
  int update_column_stat_info(const ObDatum *datum, const ObObjMeta &meta, const ObDatumCmpFuncType cmp_func);

  ObOptColumnStat *col_stat_;
  // members below is no need to serialize
  ObMinMaxValEval min_val_;
  ObMinMaxValEval max_val_;
  ObArenaAllocator inner_min_allocator_;
  ObArenaAllocator inner_max_allocator_;
  ObArenaAllocator inner_allocator_;
  ObIAllocator &allocator_;
  TO_STRING_KV(K_(col_stat),
               K_(min_val),
               K_(max_val));
private:
  int inner_merge_min(const ObDatum &datum, const ObObjMeta &meta, const ObDatumCmpFuncType cmp_func);
  int inner_merge_max(const ObDatum &datum, const ObObjMeta &meta, const ObDatumCmpFuncType cmp_func);
  int calc_col_len(const ObDatum &datum, const ObObjMeta &meta, int64_t &col_len);
  DISALLOW_COPY_AND_ASSIGN(ObOptOSGColumnStat);
};

}
}

#endif /* _OB_OPT_OSG_COLUMN_STAT_H_ */
