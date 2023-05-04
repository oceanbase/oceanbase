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

#ifndef OCEANBASE_ENGINE_OB_SERIALIZABLE_FUNCTION_H_
#define OCEANBASE_ENGINE_OB_SERIALIZABLE_FUNCTION_H_

#include "lib/utility/serialization.h"
#include "lib/hash_func/murmur_hash.h"

namespace oceanbase
{
namespace sql
{

struct ObSerializeFuncTag {};
typedef void (*serializable_function)(ObSerializeFuncTag &);

struct ObExpr;
struct ObEvalCtx;
struct ObBitVector;
// Implemented in ob_expr.cpp
extern int expr_default_eval_batch_func(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const int64_t batch_size);
struct ObBatchEvalFuncTag {};
typedef void (*ser_eval_batch_function)(ObBatchEvalFuncTag &);

// serialize help macro, can be used in OB_SERIALIZE_MEMBER like this:
// OB_SERIALIZE_MEMBER(Foo, SER_FUNC(func_));
#define SER_FUNC(f) *(oceanbase::sql::serializable_function *)(&f)


// Serialize function array (SFA) id define, append only (before OB_SFA_MAX)
// can not delete or reorder.
#define SER_FUNC_ARRAY_ID_ENUM                   \
  OB_SFA_MIN,                                    \
  OB_SFA_ALL_MISC,                               \
  OB_SFA_DATUM_NULLSAFE_CMP,                     \
  OB_SFA_DATUM_NULLSAFE_STR_CMP,                 \
  OB_SFA_EXPR_BASIC_PART1,                       \
  OB_SFA_EXPR_STR_BASIC_PART1,                   \
  OB_SFA_RELATION_EXPR_EVAL,                     \
  OB_SFA_RELATION_EXPR_EVAL_STR,                 \
  OB_SFA_DATUM_CMP,                              \
  OB_SFA_DATUM_CMP_STR,                          \
  OB_SFA_DATUM_CAST_ORACLE_IMPLICIT,             \
  OB_SFA_DATUM_CAST_ORACLE_EXPLICIT,             \
  OB_SFA_DATUM_CAST_MYSQL_IMPLICIT,              \
  OB_SFA_DATUM_CAST_MYSQL_ENUMSET_IMPLICIT,      \
  OB_SFA_SQL_EXPR_EVAL,                          \
  OB_SFA_SQL_EXPR_ABS_EVAL,                      \
  OB_SFA_SQL_EXPR_NEG_EVAL,                      \
  OB_SFA_RELATION_EXPR_EVAL_BATCH,               \
  OB_SFA_RELATION_EXPR_STR_EVAL_BATCH,           \
  OB_SFA_SQL_EXPR_EVAL_BATCH,                    \
  OB_SFA_EXPR_BASIC_PART2,                       \
  OB_SFA_EXPR_STR_BASIC_PART2,                   \
  OB_SFA_RELATION_EXPR_EVAL_TEXT,                \
  OB_SFA_RELATION_EXPR_TEXT_EVAL_BATCH,          \
  OB_SFA_DATUM_CMP_TEXT,                         \
  OB_SFA_RELATION_EXPR_EVAL_TEXT_STR,            \
  OB_SFA_RELATION_EXPR_TEXT_STR_EVAL_BATCH,      \
  OB_SFA_DATUM_CMP_TEXT_STR,                     \
  OB_SFA_RELATION_EXPR_EVAL_STR_TEXT,            \
  OB_SFA_RELATION_EXPR_STR_TEXT_EVAL_BATCH,      \
  OB_SFA_DATUM_CMP_STR_TEXT,                     \
  OB_SFA_EXPR_JSON_BASIC_PART1,                  \
  OB_SFA_EXPR_JSON_BASIC_PART2,                  \
  OB_SFA_RELATION_EXPR_EVAL_JSON,                \
  OB_SFA_RELATION_EXPR_JSON_EVAL_BATCH,          \
  OB_SFA_DATUM_CMP_JSON,                         \
  OB_SFA_FIXED_DOUBLE_NULLSAFE_CMP,              \
  OB_SFA_FIXED_DOUBLE_BASIC_PART1,               \
  OB_SFA_FIXED_DOUBLE_BASIC_PART2,               \
  OB_SFA_FIXED_DOUBLE_CMP_EVAL,                  \
  OB_SFA_FIXED_DOUBLE_CMP_EVAL_BATCH,            \
  OB_SFA_DATUM_FIXED_DOUBLE_CMP,                 \
  OB_SFA_EXPR_GEO_BASIC_PART1,                   \
  OB_SFA_EXPR_GEO_BASIC_PART2,                   \
  OB_SFA_RELATION_EXPR_EVAL_GEO,                 \
  OB_SFA_RELATION_EXPR_GEO_EVAL_BATCH,           \
  OB_SFA_DATUM_CMP_GEO,                          \
  OB_SFA_DATUM_NULLSAFE_TEXT_CMP,                \
  OB_SFA_DATUM_NULLSAFE_TEXT_STR_CMP,            \
  OB_SFA_DATUM_NULLSAFE_STR_TEXT_CMP,            \
  OB_SFA_DATUM_NULLSAFE_JSON_CMP,                \
  OB_SFA_DATUM_NULLSAFE_GEO_CMP,                 \
  OB_SFA_EXPR_UDT_BASIC_PART1,                   \
  OB_SFA_EXPR_UDT_BASIC_PART2,                   \
  OB_SFA_MAX

enum ObSerFuncArrayID {
  SER_FUNC_ARRAY_ID_ENUM
};

// add unused ObSerFuncArrayID here
#define UNUSED_SER_FUNC_ARRAY_ID_ENUM            \
  OB_SFA_MIN,                                    \
  OB_SFA_MAX

class ObFuncSerialization
{
public:
  // called before worker threads started.
  static void init() { get_hash_table(); }

  // used in REG_SER_FUNC_ARRAY macro, can not used directly
  static bool reg_func_array(const ObSerFuncArrayID id, void **array, const int64_t size);

  // get serialize index by function pointer
  // return zero if fun is NULL
  // return non zero fun is serializable
  // return OB_INVALID_INDEX if function is not serializable
  static uint64_t get_serialize_index(void *func);

  // get function by serialize index
  // return NULL if %idx out of bound.
  OB_INLINE static void *get_serialize_func(const uint64_t idx);


  //
  // Convert N x N two dimension array to single dimension array which index is stable
  // while N extending. e.g:
  //
  //  00 01 02
  //  10 11 12   ==>  00 01 10 11 02 20 12 21 22
  //  20 21 22
  //
  //
  // Usage:
  //
  // fuc_array[N][N][2] can convert to ser_func_array[N * N][2] with:
  // convert_NxN_function_array(ser_func_array, func_array, N, 2, 0, 2).
  //
  // func_array[N][N][2] extend to func_array[N][N][3], the serialize function array should
  // split into to array:
  //   ser_func_array0[N * N][2] with: convert_NxN_array(ser_func_array0, func_array, N, 3, 0, 2).
  //   ser_func_array1[N * N][2] with: convert_NxN_array(ser_func_array0, func_array, N, 3, 2, 1).
  //
  //
  static bool convert_NxN_array(void **dst, void **src, const int64_t n,
                                const int64_t row_size = 1,
                                const int64_t copy_row_idx = 0,
                                const int64_t copy_row_cnt = 1);

  struct FuncIdx
  {
    void *func_;
    uint64_t idx_;
  };

  struct FuncIdxTable
  {
    FuncIdx *buckets_;
    uint64_t bucket_size_;
    uint64_t bucket_size_mask_;
  };

private:
  const static int64_t ARRAY_IDX_SHIFT_BIT = 32;

  struct FuncArray
  {
    void **funcs_;
    int64_t size_;
  };

  static uint64_t get_array_idx(const uint64_t idx) { return idx >> ARRAY_IDX_SHIFT_BIT; }
  static uint64_t get_func_idx(const uint64_t idx) { return idx & ((1ULL << ARRAY_IDX_SHIFT_BIT) - 1); }
  static uint64_t make_combine_idx(const uint64_t array_idx, const uint64_t func_idx)
  {
    return (array_idx << ARRAY_IDX_SHIFT_BIT) | (((1ULL << ARRAY_IDX_SHIFT_BIT) - 1) & func_idx);
  }

  static inline uint64_t hash(const void *func)
  {
    return common::murmurhash(&func, sizeof(func), 0);
  }

  static const FuncIdxTable &get_hash_table()
  {
    static FuncIdxTable *g_table = NULL;
    if (OB_UNLIKELY(NULL == g_table)) {
      g_table = &create_hash_table();
      check_hash_table_valid();
    }
    return *g_table;
  }

  static void check_hash_table_valid();

  // create hash table never fail, return a default empty hash table if OOM.
  static FuncIdxTable &create_hash_table();

  static FuncArray g_all_func_arrays[OB_SFA_MAX];
};

inline uint64_t ObFuncSerialization::get_serialize_index(void *func)
{
  uint64_t idx = 0;
  if (OB_LIKELY(0 != func)) {
    const FuncIdxTable &ht = get_hash_table();
    const uint64_t hash_val = hash(func);
    for (uint64_t i = 0; i < ht.bucket_size_; i++) {
      const FuncIdx &fi = ht.buckets_[(hash_val + i) & ht.bucket_size_mask_];
      if (fi.func_ == func) {
        idx = fi.idx_;
        break;
      } else if (NULL == fi.func_) {
        idx = common::OB_INVALID_INDEX;
        break;
      }
    }
  }
  return idx;
}

// return NULL for not found
OB_INLINE void *ObFuncSerialization::get_serialize_func(const uint64_t idx)
{
  void *func = NULL;
  if (OB_LIKELY(idx > 0)) {
    const uint64_t array_idx = get_array_idx(idx);
    const uint64_t func_idx = get_func_idx(idx);
    if (OB_UNLIKELY(array_idx >= OB_SFA_MAX)
        || OB_UNLIKELY(func_idx >= g_all_func_arrays[array_idx].size_)) {
      // return NULL
    } else {
      func = g_all_func_arrays[array_idx].funcs_[func_idx];
    }
  }
  return func;
}

#define REG_SER_FUNC_ARRAY(id, array, size) \
  static_assert(id >= 0 && id < OB_SFA_MAX, "too big id" #id); \
  bool g_reg_ser_func_##id = ObFuncSerialization::reg_func_array( \
      id, reinterpret_cast<void **>(array), size);

} // end namespace sql

namespace common
{
namespace serialization
{

inline int64_t encoded_length(sql::serializable_function)
{
  return sizeof(uint64_t);
}

inline int64_t encoded_length(sql::ser_eval_batch_function)
{
  return sizeof(uint64_t);
}

inline int encode(char *buf, const int64_t buf_len, int64_t &pos,
                  sql::serializable_function func)
{
  int ret = OB_SUCCESS;
  const uint64_t idx = sql::ObFuncSerialization::get_serialize_index(
      reinterpret_cast<void *>(func));
  if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
    ret = OB_INVALID_INDEX;
    SQL_LOG(WARN, "function not serializable", K(ret), KP(func));
  } else {
    ret = encode_i64(buf, buf_len, pos, idx);
  }
  return ret;
}

inline int encode(char *buf, const int64_t buf_len, int64_t &pos,
                  sql::ser_eval_batch_function func)
{
  return encode(buf, buf_len, pos, reinterpret_cast<sql::serializable_function>(func));
}

inline int decode(const char *buf, const int64_t data_len, int64_t &pos,
                  sql::serializable_function &func)
{
  int ret = OB_SUCCESS;
  uint64_t idx = 0;
  ret = decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&idx));
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == idx)) {
      func = NULL;
    } else {
      func = reinterpret_cast<sql::serializable_function>(
          sql::ObFuncSerialization::get_serialize_func(idx));
      if (NULL == func) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "function not found by idx", K(ret), K(idx));
      }
    }
  }
  return ret;
}

inline int decode(const char *buf, const int64_t data_len, int64_t &pos,
                  sql::ser_eval_batch_function &func)
{
  int ret = OB_SUCCESS;
  uint64_t idx = 0;
  ret = decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&idx));
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == idx)) {
      func = NULL;
    } else {
      func = reinterpret_cast<sql::ser_eval_batch_function>(
          sql::ObFuncSerialization::get_serialize_func(idx));
      if (NULL == func) {
        // set to default eval batch func
        SQL_LOG(DEBUG, "batch eval function not found", K(idx));
        func = reinterpret_cast<sql::ser_eval_batch_function>(
            sql::expr_default_eval_batch_func);
      }
    }
  }
  return ret;
}


} // end namespace serialization
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_SERIALIZABLE_FUNCTION_H_
