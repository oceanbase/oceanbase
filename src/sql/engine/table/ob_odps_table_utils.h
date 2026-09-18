/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  // Build a shadow cast expr for file-column exprs which have no args_: it is a full copy of
  // the column expr (frame fields included, so result-memory helpers keep working) with
  // args_[0] pointing at a shared static input carrying the source type/collation.
  // Only for the direct cast helper calls, never for eval(). Do not modify the scan expr.
  static void prepare_cast_expr(const ObExpr &column_expr, const ObObjType in_type,
                                const ObCollationType in_cs_type, ObExpr &cast_expr)
  {
    struct CastInput {
      CastInput(ObObjType t, ObCollationType cs)
      {
        expr_.datum_meta_.type_ = t;
        expr_.datum_meta_.cs_type_ = cs;
        expr_.obj_meta_.set_type(t);
        expr_.obj_meta_.set_collation_type(cs);
        args_[0] = &expr_;
      }
      ObExpr expr_;
      ObExpr *args_[1];
    };
    // Shared, initialized once and read-only afterwards. ObExpr::args_ is not const-qualified.
    static CastInput varchar_utf8(ObVarcharType, CS_TYPE_UTF8MB4_BIN);
    static CastInput varchar_binary(ObVarcharType, CS_TYPE_BINARY);
    static CastInput char_utf8(ObCharType, CS_TYPE_UTF8MB4_BIN);
    cast_expr = column_expr;
    if (ObVarcharType == in_type && CS_TYPE_BINARY == in_cs_type) {
      cast_expr.args_ = varchar_binary.args_;
    } else if (ObCharType == in_type && CS_TYPE_UTF8MB4_BIN == in_cs_type) {
      cast_expr.args_ = char_utf8.args_;
    } else {
      OB_ASSERT(ObVarcharType == in_type && CS_TYPE_UTF8MB4_BIN == in_cs_type);
      cast_expr.args_ = varchar_utf8.args_;
    }
    cast_expr.arg_cnt_ = 1;
  }

  static void prepare_numeric_cast_expr(const ObExpr &column_expr, ObExpr &cast_expr)
  {
    prepare_cast_expr(column_expr, ObVarcharType, CS_TYPE_UTF8MB4_BIN, cast_expr);
  }

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