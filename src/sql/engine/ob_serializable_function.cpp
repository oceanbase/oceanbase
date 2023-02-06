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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_serializable_function.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/hash_func/murmur_hash.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

#define BOOL_EXTERN_DECLARE(id) CONCAT(extern bool g_reg_ser_func_, id)
#define REG_UNUSED_SER_FUNC_ARRAY(id) CONCAT(bool g_reg_ser_func_, id)
#define LIST_REGISTERED_FUNC_ARRAY(id) CONCAT(g_reg_ser_func_, id)

LST_DO_CODE(BOOL_EXTERN_DECLARE, SER_FUNC_ARRAY_ID_ENUM);
LST_DO_CODE(REG_UNUSED_SER_FUNC_ARRAY, UNUSED_SER_FUNC_ARRAY_ID_ENUM);

bool check_all_ser_func_registered()
{
  bool all_registered = true;
  bool all_reg_flags[] = { LST_DO(LIST_REGISTERED_FUNC_ARRAY, (,), SER_FUNC_ARRAY_ID_ENUM) };
  ObSerFuncArrayID unused_ids[] = { UNUSED_SER_FUNC_ARRAY_ID_ENUM };
  for (int64_t i = 0; i < ARRAYSIZEOF(all_reg_flags); i++) {
    if (!all_reg_flags[i]) {
      bool found = false;
      for (int64_t j =0; !found && j < ARRAYSIZEOF(unused_ids); j++) {
        if (i == unused_ids[j]) {
          found = true;
        }
      }
      if (!found) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "serialize function array not registered", "ObSerFuncArrayID", i);
        all_registered = false;
      }
    }
  }
  return all_registered;
}
#undef LIST_REGISTERED_FUNC_ARRAY
#undef REG_UNUSED_SER_FUNC_ARRAY
#undef BOOL_EXTERN_DECLARE

ObFuncSerialization::FuncArray ObFuncSerialization::g_all_func_arrays[OB_SFA_MAX];

bool ObFuncSerialization::reg_func_array(
    const ObSerFuncArrayID id, void **array, const int64_t size)
{
  bool succ = true;
  if (id < 0 || id >= OB_SFA_MAX || OB_ISNULL(array) || size < 0) {
    succ = false;
  } else {
    g_all_func_arrays[id].funcs_ = array;
    g_all_func_arrays[id].size_ = size;
  }
  return succ;
}

// All serializable functions should register here.
void *g_all_misc_serializable_functions[] = {
  NULL
  // append only, only mark delete allowed.
};

REG_SER_FUNC_ARRAY(OB_SFA_ALL_MISC, g_all_misc_serializable_functions,
                   ARRAYSIZEOF(g_all_misc_serializable_functions));


static ObFuncSerialization::FuncIdx g_def_func_table_bucket;
static ObFuncSerialization::FuncIdxTable g_def_func_table = {
  &g_def_func_table_bucket, 1, 0 };

ObFuncSerialization::FuncIdxTable &ObFuncSerialization::create_hash_table()
{
  if (!check_all_ser_func_registered()) {
    ob_abort();
  }
  int64_t func_cnt = 1;
  for (int64_t i = 0; i < ARRAYSIZEOF(g_all_func_arrays); i++) {
    func_cnt += g_all_func_arrays[i].size_;
  }
  const int64_t bucket_size = next_pow2(func_cnt * 2);
  ObMemAttr attr(OB_SERVER_TENANT_ID, "SerFuncRegHT");
  FuncIdxTable *ht = static_cast<FuncIdxTable *>(ob_malloc(sizeof(FuncIdxTable), attr));
  FuncIdx *buckets = static_cast<FuncIdx *>(ob_malloc(sizeof(FuncIdx) * bucket_size, attr));
  if (NULL == ht || NULL == buckets) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate memory failed");
    if (NULL != ht) {
      ob_free(ht);
    }
    if (NULL != buckets) {
      ob_free(buckets);
    }
    ht = &g_def_func_table;
  } else {
    int64_t conflicts = 0;
    int64_t size = 0;
    MEMSET(buckets, 0, sizeof(buckets[0]) * bucket_size);
    ht->buckets_ = buckets;
    ht->bucket_size_ = bucket_size;
    ht->bucket_size_mask_ = bucket_size - 1;
    for (uint64_t array_idx = 0; array_idx < ARRAYSIZEOF(g_all_func_arrays); array_idx++) {
      const FuncArray &array = g_all_func_arrays[array_idx];
      for (uint64_t func_idx = 0; func_idx < array.size_; func_idx++) {
        void *func = array.funcs_[func_idx];
        if (NULL == func) {
          continue;
        }
        // insert into hash table
        const uint64_t hash_val = hash(func);
        for (uint64_t i = 0; i < ht->bucket_size_; i++) {
          const uint64_t pos = (hash_val + i) & ht->bucket_size_mask_;
          if (NULL == ht->buckets_[pos].func_) {
            size += 1;
            ht->buckets_[pos].func_ = func;
            ht->buckets_[pos].idx_ = make_combine_idx(array_idx, func_idx);
            break;
          } else if (func == ht->buckets_[pos].func_) {
            // no overwrite
            break;
          } else {
            conflicts += 1;
          }
          if (i + 1 == ht->bucket_size_) {
            LOG_ERROR_RET(OB_ERROR, "hash table is full, impossible");
            ob_abort();
          }
        }
      } // end func loop
    } // end func array loop
    LOG_INFO("function serialization hash table created",
             K(func_cnt), K(bucket_size), K(size), K(conflicts));
  }
  return *ht;
}

void ObFuncSerialization::check_hash_table_valid()
{
  for (int64_t i = 0; i < ARRAYSIZEOF(g_all_func_arrays); i++) {
    for (int64_t j = 0; j < g_all_func_arrays[i].size_; j++) {
      void *func = g_all_func_arrays[i].funcs_[j];
      if (NULL != func) {
        const uint64_t idx = get_serialize_index(func);
        OB_ASSERT(idx > 0 && OB_INVALID_INDEX != idx);
        OB_ASSERT(func == get_serialize_func(idx));
      }
    }
  }
}

// define item[X][Y] offset in source array
#define SRC_ITEM_OFF(X, Y) ((X) * n * row_size + (Y) * row_size)
#define COPY_FUNCS
bool ObFuncSerialization::convert_NxN_array(
    void **dst, void **src, const int64_t n,
    const int64_t row_size, // = 1
    const int64_t copy_row_idx, // = 0
    const int64_t copy_row_cnt) // = 1
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dst) || OB_ISNULL(src) || n < 0 || row_size < 0
      || copy_row_idx < 0 || copy_row_idx >= row_size
      || copy_row_cnt < 1 || copy_row_cnt > row_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(dst), KP(src), K(n),
              K(row_size), K(copy_row_idx), K(copy_row_cnt));
  } else {
    const int64_t mem_copy_size = copy_row_cnt * sizeof(void *);
    int64_t idx = 0;
    for (int64_t i = 0; i < n; i++) {
      for (int64_t j = 0; j < i; j++) {
        memcpy(&dst[idx], &src[SRC_ITEM_OFF(i, j) + copy_row_idx], mem_copy_size);
        idx += copy_row_cnt;
        memcpy(&dst[idx], &src[SRC_ITEM_OFF(j, i) + copy_row_idx], mem_copy_size);
        idx += copy_row_cnt;
      }
      memcpy(&dst[idx], &src[SRC_ITEM_OFF(i, i) + copy_row_idx], mem_copy_size);
      idx += copy_row_cnt;
    }
  }
  return OB_SUCCESS == ret;
}

} // end namespace sql
} // end namespace oceanbase
