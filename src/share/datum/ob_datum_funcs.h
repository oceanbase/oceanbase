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
class ObDatum;

typedef int (*ObDatumCmpFuncType)(const ObDatum& datum1, const ObDatum& datum2);
typedef uint64_t (*ObDatumHashFuncType)(const ObDatum& datum, const uint64_t seed);

class ObDatumFuncs {
public:
  static ObDatumCmpFuncType get_nullsafe_cmp_func(const ObObjType type1, const ObObjType type2,
      const ObCmpNullPos null_pos, const ObCollationType cs_type, const bool is_oracle_mode);
  static bool is_string_type(const ObObjType type);
  static bool is_varying_len_char_type(const ObObjType type, const ObCollationType cs_type)
  {
    return (type == ObNVarchar2Type || (type == ObVarcharType && cs_type != CS_TYPE_BINARY));
  }
  static sql::ObExprBasicFuncs* get_basic_func(const ObObjType type, const ObCollationType cs_type);

  // for function serialization register
  static void** get_nullsafe_cmp_funcs()
  {
    return reinterpret_cast<void**>(NULLSAFE_CMP_FUNCS);
  }
  constexpr static int64_t get_nullsafe_cmp_funcs_size()
  {
    return sizeof(NULLSAFE_CMP_FUNCS) / sizeof(ObDatumCmpFuncType);
  }

  static void** get_nullsafe_str_cmp_funcs()
  {
    return reinterpret_cast<void**>(NULLSAFE_STR_CMP_FUNCS);
  }
  constexpr static int64_t get_nullsafe_str_cmp_funcs_size()
  {
    return sizeof(NULLSAFE_STR_CMP_FUNCS) / sizeof(ObDatumCmpFuncType);
  }

  static void** get_basic_funcs()
  {
    return reinterpret_cast<void**>(EXPR_BASIC_FUNCS);
  }
  constexpr static int64_t get_basic_funcs_size()
  {
    return sizeof(EXPR_BASIC_FUNCS) / static_cast<int>(sizeof(void*));
  }

  static void** get_basic_str_funcs()
  {
    return reinterpret_cast<void**>(EXPR_BASIC_STR_FUNCS);
  }
  constexpr static int64_t get_basic_str_funcs_size()
  {
    return sizeof(EXPR_BASIC_STR_FUNCS) / static_cast<int>(sizeof(void*));
  }

private:
  template <int, int>
  friend class InitTypeCmpArray;
  template <int>
  friend class InitStrCmpArray;
  template <int>
  friend class InitBasicFuncArray;
  template <int, int>
  friend class InitBasicStrFuncArray;

  static ObDatumCmpFuncType NULLSAFE_CMP_FUNCS[ObMaxType][ObMaxType][2];
  // cs_type, tenant_mode, calc_with_end_space
  // now only RawTC, StringTC, TextTC defined str cmp funcs
  static ObDatumCmpFuncType NULLSAFE_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];

  static sql::ObExprBasicFuncs EXPR_BASIC_FUNCS[ObMaxType];
  // 2:whether calc_end_space; whether type is lob locator
  static sql::ObExprBasicFuncs EXPR_BASIC_STR_FUNCS[CS_TYPE_MAX][2][2];
};

struct ObCmpFunc {
  OB_UNIS_VERSION(1);

public:
  ObCmpFunc() : cmp_func_(NULL)
  {}
  union {
    common::ObDatumCmpFuncType cmp_func_;
    sql::serializable_function ser_cmp_func_;
  };
  TO_STRING_KV(KP_(cmp_func));
};

struct ObHashFunc {
  OB_UNIS_VERSION(1);

public:
  ObHashFunc() : hash_func_(NULL)
  {}
  union {
    common::ObDatumHashFuncType hash_func_;
    sql::serializable_function ser_hash_func_;
  };
  TO_STRING_KV(K_(hash_func));
};

typedef common::ObFixedArray<ObCmpFunc, common::ObIAllocator> ObCmpFuncs;
typedef common::ObFixedArray<ObHashFunc, common::ObIAllocator> ObHashFuncs;

}  // end namespace common
}  // end namespace oceanbase
#endif  // OCEANBASE_OB_DATUM_FUNCS_H_
