/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef __SQL_OB_ODPS_TABLE_UTILS_H__
#define __SQL_OB_ODPS_TABLE_UTILS_H__
#include "common/object/ob_object.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {

/*
  ObArrayHelper contain element information used for decode odps array record.
*/
struct ObODPSArrayHelper {
  ObODPSArrayHelper(ObIAllocator &allocator)
  : allocator_(allocator),
    array_(nullptr),
    child_helper_(nullptr)
  {}
  ~ObODPSArrayHelper()
  {
    if (array_ != nullptr) {
      array_->clear();
      allocator_.free(array_);
      array_ = nullptr;
    }
    if (child_helper_ != nullptr) {
      child_helper_->~ObODPSArrayHelper();
      allocator_.free(child_helper_);
      child_helper_ = nullptr;
    }
  }
  TO_STRING_KV(K(element_type_), K(element_precision_), K(element_scale_),
               K(element_collation_), K(element_length_));
  
  ObIAllocator &allocator_;
  // used to hold child element
  ObIArrayType *array_;
  // child array helper if array element is array
  ObODPSArrayHelper* child_helper_;
  // child element type
  ObObjType element_type_;
  // child element precision
  ObPrecision element_precision_;
  // child element scale
  ObScale element_scale_;
  // child element collation
  ObCollationType element_collation_;
  // child element length
  int32_t element_length_;
};

class ObODPSTableUtils {
public:
  static int create_array_helper(ObExecContext &exec_ctx,
                                 ObIAllocator &allocator,
                                 const ObExpr &cur_expr,
                                 ObODPSArrayHelper *&array_helper);
  static int recursive_create_array_helper(ObIAllocator &allocator,
                                           const ObCollectionTypeBase *coll_meta,
                                           ObODPSArrayHelper *&array_helper);
private:
  //disallow construct
  ObODPSTableUtils();
  ~ObODPSTableUtils();
};


} // sql
} // oceanbase

#endif // __SQL_OB_ODPS_TABLE_UTILS_H__