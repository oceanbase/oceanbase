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

#ifdef LIB_CACHE_MISS_EVENT
LIB_CACHE_MISS_EVENT(KEY_NOT_MATCH, CACHE_NODE_LEVEL, "Cache key not exists, cache_node_id: %ld. Detail info: %s")
LIB_CACHE_MISS_EVENT(TABLE_SCHEMA_NOT_EXISTS, PCV_LEVEL, "Different from pcv_id: %ld, reason: table schema not exsists. Detail info: %s")
LIB_CACHE_MISS_EVENT(BATCH_STMT_DIFF_FROM_GEN_STMT, PCV_LEVEL, "Different from pcv_id: %ld, reason: batch stmt can not share plan with general stmt. Detail info: %s")
LIB_CACHE_MISS_EVENT(NON_PARAMETERIZED_SQL_NOT_MATCH, PCV_LEVEL, "Different from pcv_id: %ld, reason: non-parameterized sql is not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(NON_PARAMETERIZED_PARAM_CANNOT_COMP, PCV_LEVEL, "Different from pcv_id: %ld, reason: non-parameterized param can not compare. Detail info: %s")
LIB_CACHE_MISS_EVENT(NON_PARAMETERIZED_PARAM_COLLATION_NOT_MATCH, PCV_LEVEL, "Different from pcv_id: %ld, reason: collation of non-parameterized params are different. Detail info: %s")
LIB_CACHE_MISS_EVENT(NON_PARAMETERIZED_CONST_NOT_MATCH, PCV_LEVEL, "Different from pcv_id: %ld, reason: non-parameterized const not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(DIFF_TMP_TABLE, PCV_LEVEL, "Different from pcv_id: %ld, reason: different tmp table. Detail info: %s")
LIB_CACHE_MISS_EVENT(DIFF_TABLE_SCHEMA, PCV_LEVEL, "Different from pcv_id: %ld, reason: schema not exists. Detail info: %s")
LIB_CACHE_MISS_EVENT(DIFF_SYNONYM_SCHEMA, PCV_LEVEL, "Different from pcv_id: %ld, reason: different synonym. Detail info: %s")
LIB_CACHE_MISS_EVENT(SYN_SAME_NAME_WITH_OBJ, PCV_LEVEL, "Different from pcv_id: %ld, reason: exist a common object same name with synonym. Detail info: %s")
LIB_CACHE_MISS_EVENT(OLD_SCHEMA_VERSION, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: schema verson is old. Detail info: %s")
LIB_CACHE_MISS_EVENT(PARAMS_INFO_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: parameter's info not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(USER_VARIABLE_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: user variables not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(CAP_FLAGS_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: capability flags not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(PRIVILEGE_CONSTR_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: privilege not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(PRE_CALC_CONSTR_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: pre-calc constraint not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(PARAM_CONSTR_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: param constraint not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(BOOL_PARAM_VALUE_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: boolean parameter value not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(CONST_PRARM_CONSTR_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: constant not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(MULTI_STMT_INFO_NOT_MATCH, PLAN_SET_LEVEL, "Different from plan_set_id: %ld, reason: multi-statement info not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(LOCATION_CONSTR_NOT_MATCH, CACHE_OBJ_LEVEL, "Different from plan_id: %ld, reason: location constraint not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(LOCAL_PLAN_NOT_EXISTS, CACHE_OBJ_LEVEL, "Not exist local plan in plan_set_id: %ld. Detail info: %s")
LIB_CACHE_MISS_EVENT(LOCAL_PLAN_DOP_NOT_MATCH, CACHE_OBJ_LEVEL, "Different from plan_id: %ld, reason: dop of local plan not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(REMOTE_PLAN_NOT_EXISTS, CACHE_OBJ_LEVEL, "Not exist remote plan in plan_set_id: %ld. Detail info: %s")
LIB_CACHE_MISS_EVENT(FORCE_HARD_PARSE, CACHE_OBJ_LEVEL, "Based on historical execution records, performing a hard parse to regenerate the plan is better than using the current plan_id: %ld. Detail info: %s")
LIB_CACHE_MISS_EVENT(EXPIRED_PHY_PLAN, CACHE_OBJ_LEVEL, "Expired physical plan, plan_id: %ld. Detail info: %s")
LIB_CACHE_MISS_EVENT(DIST_PLAN_DOP_NOT_MATCH, CACHE_OBJ_LEVEL, "Different from plan_id: %ld, reason: dop of distributed plan not match. Detail info: %s")
LIB_CACHE_MISS_EVENT(PWJ_CONSTR_NOT_MATCH, CACHE_OBJ_LEVEL, "Different from plan_id: %ld, reason: partition-wise Join constraint not match. Detail info: %s")
#endif /*LIB_CACHE_MISS_EVENT*/

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_MISS_DIAG_
#define OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_MISS_DIAG_

#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{
#define MAX_CACHE_MISS_INFO_STR_LEN 1024

enum PlanCacheLevel
{
  INVALID_CACHE_LEVEL = 0,
  CACHE_NODE_LEVEL,
  PCV_LEVEL,
  PLAN_SET_LEVEL,
  CACHE_OBJ_LEVEL,
  MAX_CACHE_LEVEL
};

enum LibCacheMissCode
{
  INVALID_CODE = 0,
  #define LIB_CACHE_MISS_EVENT(code, level, info) code,
  #include "sql/plan_cache/ob_lib_cache_miss_diag.h"
  #undef LIB_CACHE_MISS_EVENT
  MAX_CODE
};

struct LibCacheMissEvent {
  LibCacheMissEvent(enum LibCacheMissCode code, uint32_t level, const char *info):
   code_(code), level_(level), info_(info) {}
  enum LibCacheMissCode code_;
  uint32_t level_;
  const char *info_;
};

class ObLibCacheMissEventRegister
{
public:
  static LibCacheMissEvent EVENTS[MAX_CODE + 1];
};

struct ObLibCacheMissEventRecorder
{
ObLibCacheMissEventRecorder(common::ObIAllocator *allocator)
  : id_(-1), cur_process_id_(-1), event_code_(KEY_NOT_MATCH), allocator_(allocator), detail_info_(NULL)
{
}

void record(enum LibCacheMissCode code)
{
  id_ = cur_process_id_;
  event_code_ = code;
  if (OB_ISNULL(detail_info_) && OB_NOT_NULL(allocator_)) {
    detail_info_ = static_cast<char *>(allocator_->alloc(MAX_CACHE_MISS_INFO_STR_LEN));
  }
}
uint64_t id_;
uint64_t cur_process_id_;
enum LibCacheMissCode event_code_;
common::ObIAllocator *allocator_;
char *detail_info_;

TO_STRING_KV(K(id_), K(event_code_), K(cur_process_id_), KP(allocator_), K(detail_info_));
};

struct ObLibCacheMissReason
{
ObLibCacheMissReason(): id_(-1), event_code_(KEY_NOT_MATCH)
{
  detail_info_[0] = '\0';
}

void from_recorder(const ObLibCacheMissEventRecorder &other)
{
  id_ = other.id_;
  event_code_ = other.event_code_;
  if (OB_NOT_NULL(other.detail_info_)) {
    int len = MAX_CACHE_MISS_INFO_STR_LEN < strlen(other.detail_info_) ? MAX_CACHE_MISS_INFO_STR_LEN - 1 : strlen(other.detail_info_);
    MEMCPY(detail_info_, other.detail_info_, len);
    detail_info_[len] = '\0';
  }
}

int dump(ObIAllocator *allocator, char *&buf, int64_t &buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_buf_len = 1024;
  bool print_succ = false;
  if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(tmp_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_PC_LOG(WARN, "failed to allocate memory", K(ret), K(tmp_buf_len));
  }
  while (OB_SUCC(ret) && !print_succ) {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(buf, tmp_buf_len, pos, ObLibCacheMissEventRegister::EVENTS[event_code_].info_, id_, detail_info_))) {
      if (ret == OB_SIZE_OVERFLOW) {
        ret = OB_SUCCESS;
        tmp_buf_len *= 2;
        if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(tmp_buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_PC_LOG(WARN, "failed to allocate memory", K(ret), K(tmp_buf_len));
        }
      } else {
        SQL_PC_LOG(WARN, "failed to dump cache miss event", K(ret), K(tmp_buf_len), K(event_code_), K(id_), K(detail_info_));
      }
    } else {
      print_succ = true;
      buf_len = pos;
    }
  }
  return ret;
}

uint64_t id_;
enum LibCacheMissCode event_code_;
char detail_info_[MAX_CACHE_MISS_INFO_STR_LEN];

TO_STRING_KV(K(id_), K(event_code_), K(detail_info_));
};

struct ObLibCacheMatchScopeGuard
{
  ObLibCacheMatchScopeGuard(uint64_t id, ObLibCacheMissEventRecorder &recorder): last_id_(-1), recorder_(recorder)
  {
    last_id_ = recorder.cur_process_id_;
    recorder.cur_process_id_ = id;
  }
  ~ObLibCacheMatchScopeGuard()
  {
    recorder_.cur_process_id_ = last_id_;
  }
  uint64_t last_id_;
  ObLibCacheMissEventRecorder &recorder_;
};

/**
 * @brief Helper macros to print key-value pairs with comma separator
 *
 * These macros are used internally by RECORD_CACHE_MISS_PRINT_KV_IMPL to handle
 * variable number of key-value pairs. Each K(...) macro expands to two arguments
 * (key, value), so these macros handle 0, 2, 4, 6, ..., 20 arguments (0 to 10 pairs).
 *
 * The first pair doesn't have a leading comma, subsequent pairs do.
 *
 * Note: Currently supports up to 10 key-value pairs (20 arguments). If more pairs
 * are needed, additional RECORD_CACHE_MISS_KV_COMMA_* macros must be defined.
 */
#define RECORD_CACHE_MISS_KV_COMMA_0()
#define RECORD_CACHE_MISS_KV_COMMA_2(key, obj) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj)
#define RECORD_CACHE_MISS_KV_COMMA_4(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_2(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_6(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_4(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_8(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_6(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_10(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_8(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_12(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_10(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_14(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_12(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_16(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_14(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_18(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_16(__VA_ARGS__)
#define RECORD_CACHE_MISS_KV_COMMA_20(key, obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, key, true, obj); \
  RECORD_CACHE_MISS_KV_COMMA_18(__VA_ARGS__)

/**
 * @brief Helper macro to print key-value pairs with proper comma separation
 *
 * This macro prints the first key-value pair without a leading comma, and then
 * prints subsequent pairs with commas. It uses ARGS_NUM to count the number of
 * remaining arguments and dispatches to the appropriate RECORD_CACHE_MISS_KV_COMMA_*
 * macro based on the argument count.
 *
 * @param first_key The key string for the first pair
 * @param first_obj The value object for the first pair
 * @param ... Variable number of key-value pairs (each K(...) expands to key, value)
 *
 * @note Currently supports up to 10 key-value pairs (20 arguments) in addition
 *       to the first pair. If more pairs are needed, additional macros must be defined.
 *
 * @note This macro expects 'buf' and 'pos' variables to be defined in the calling scope.
 */
#define RECORD_CACHE_MISS_PRINT_KV_IMPL(first_key, first_obj, ...) \
  databuff_print_key_obj(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, first_key, false, first_obj); \
  if (ARGS_NUM(__VA_ARGS__) > 0) { \
    CONCAT(RECORD_CACHE_MISS_KV_COMMA_, ARGS_NUM(__VA_ARGS__))(__VA_ARGS__); \
  }

/**
 * @brief Main macro to record a library cache miss event with detailed information
 *
 * This macro records a cache miss event when the event level is greater than or equal
 * to the current recorded event level. It formats the detail information string with
 * file, line, and function information, followed by optional key-value pairs.
 *
 * @param CODE The LibCacheMissCode enum value identifying the cache miss event type
 * @param lib_cache_ctx The library cache context containing the recorder_ pointer
 * @param detail_info A const char* string describing the cache miss detail
 * @param ... Variable number of K(...) macro calls, each expands to (key, value) pair
 *            for additional diagnostic information. Currently supports up to 10 K(...) calls.
 *
 * @note Each K(...) macro expands to two arguments: the key string and the value object.
 *       For example, K(ret) expands to "ret", ret.
 *
 * @note The macro only records the event if:
 *       1. lib_cache_ctx.recorder_ is not NULL
 *       2. The new event's level >= the current recorded event's level
 *
 * @note The formatted output includes:
 *       - File name, line number, and function name
 *       - The detail_info string
 *       - All key-value pairs from the K(...) arguments
 *
 * @note Currently supports up to 10 K(...) arguments. If more are needed, additional
 *       RECORD_CACHE_MISS_KV_COMMA_* macros must be defined.
 *
 * Example usage:
 * @code
 * RECORD_CACHE_MISS(KEY_NOT_MATCH, lib_cache_ctx, "Cache key not found", K(param1), K(flag));
 * @endcode
 */
#define RECORD_CACHE_MISS(CODE, lib_cache_ctx, detail_info, ...) \
  do { \
    enum LibCacheMissCode _code = (CODE); \
    if (ObLibCacheMissEventRegister::EVENTS[_code].level_ >= \
        ObLibCacheMissEventRegister::EVENTS[lib_cache_ctx.recorder_.event_code_].level_) { \
      lib_cache_ctx.recorder_.record(_code); \
      if (OB_NOT_NULL(lib_cache_ctx.recorder_.detail_info_)) { \
        char *buf = lib_cache_ctx.recorder_.detail_info_; \
        int64_t pos = 0; \
        databuff_printf(buf, MAX_CACHE_MISS_INFO_STR_LEN, pos, "[%s:%d] %s. ", __FILE__, __LINE__, (detail_info)); \
        if (ARGS_NUM(__VA_ARGS__) > 0) { \
          RECORD_CACHE_MISS_PRINT_KV_IMPL(__VA_ARGS__); \
        } \
      } \
    } \
  } while(0)

/**
 * @brief Begin a match scope for library cache matching
 *
 * This macro creates a scope guard that sets the current process ID in the recorder
 * for the duration of the match operation. When the guard goes out of scope, it
 * automatically restores the previous process ID.
 *
 * @param id The process ID to set for the current match scope
 * @param lib_cache_ctx The library cache context containing the recorder_ pointer
 *
 * @note The ObLibCacheMatchScopeGuard uses RAII to automatically restore the
 *       previous process ID when the guard is destroyed.
 *
 * Example usage:
 * @code
 * MATCH_GUARD(plan_id, lib_cache_ctx);
 * // ... matching operations ...
 * // Guard automatically restores previous ID when scope ends
 * @endcode
 */
#define MATCH_GUARD(id, lib_cache_ctx) \
  ObLibCacheMatchScopeGuard match_scope_guard(id, lib_cache_ctx.recorder_);

} // namespace common
} // namespace oceanbase
#endif //OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_MISS_DIAG_