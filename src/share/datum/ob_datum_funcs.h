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

#ifndef OCEANBASE_OB_DATUM_FUNCS_H_
#define OCEANBASE_OB_DATUM_FUNCS_H_

#include "common/object/ob_obj_compare.h"
#include "common/object/ob_obj_type.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace common {
class ObExprCtx;
struct ObDatum;

typedef int (*ObDatumCmpFuncType)(const ObDatum &datum1, const ObDatum &datum2, int &cmp_ret);
typedef int (*ObDatumHashFuncType)(const ObDatum &datum, const uint64_t seed, uint64_t &res);

class ObDatumFuncs {
public:
  static ObDatumCmpFuncType get_nullsafe_cmp_func(const ObObjType type1,
                                                  const ObObjType type2,
                                                  const ObCmpNullPos null_pos,
                                                  const ObCollationType cs_type,
                                                  const ObScale max_scale,
                                                  const bool is_oracle_mode,
                                                  const bool has_lob_header,
                                                  const ObPrecision prec1 = PRECISION_UNKNOWN_YET,
                                                  const ObPrecision prec2 = PRECISION_UNKNOWN_YET);

  static bool is_string_type(const ObObjType type);
  static bool is_json(const ObObjType type);
  static bool is_geometry(const ObObjType type);
  static bool is_varying_len_char_type(const ObObjType type, const ObCollationType cs_type) {
    return (type == ObNVarchar2Type || (type == ObVarcharType && cs_type != CS_TYPE_BINARY));
  }
  static bool is_null_aware_hash_type(const ObObjType type);
  static ObScale max_scale(const ObScale s1, const ObScale s2)
  {
    ObScale max_scale = SCALE_UNKNOWN_YET;
    if (s1 != SCALE_UNKNOWN_YET && s2 != SCALE_UNKNOWN_YET) {
      max_scale = MAX(s1, s2);
    }
    return max_scale;
  }
  static sql::ObExprBasicFuncs* get_basic_func(const ObObjType type,
                                               const ObCollationType cs_type,
                                               const ObScale scale = SCALE_UNKNOWN_YET,
                                               const bool is_oracle_mode = lib::is_oracle_mode(),
                                               const bool is_lob_locator = true,
                                               const ObPrecision prec = PRECISION_UNKNOWN_YET);
};

struct ObCmpFunc
{
  OB_UNIS_VERSION(1);
public:
  ObCmpFunc() : cmp_func_(NULL) {}
  union {
    common::ObDatumCmpFuncType cmp_func_;
    sql::NullSafeRowCmpFunc row_cmp_func_;
    sql::serializable_function ser_cmp_func_;
  };
  TO_STRING_KV(KP_(cmp_func));
};

struct ObHashFunc
{
  OB_UNIS_VERSION(1);
public:
  ObHashFunc() : hash_func_(NULL), batch_hash_func_(NULL) {}
  union {
    common::ObDatumHashFuncType hash_func_;
    sql::serializable_function ser_hash_func_;
  };
  union {
    sql::ObBatchDatumHashFunc batch_hash_func_;
    sql::serializable_function ser_batch_hash_func_;
  };
  TO_STRING_KV(K_(hash_func), K_(batch_hash_func));
};

typedef common::ObFixedArray<ObCmpFunc, common::ObIAllocator> ObCmpFuncs;
typedef common::ObFixedArray<ObHashFunc, common::ObIAllocator> ObHashFuncs;


} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_OB_DATUM_FUNCS_H_
