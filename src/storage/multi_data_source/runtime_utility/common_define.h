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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_UTILITY_COMMON_DEFINE_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_UTILITY_COMMON_DEFINE_H
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "ob_clock_generator.h"
#include "src/share/ob_errno.h"
#include "src/share/scn.h"
#include "src/share/ob_occam_time_guard.h"

#ifdef OB_BUILD_RPM
  #define MDS_ASSERT(x) (void)(x)
#else
  #define MDS_ASSERT(x) \
    do{                                                   \
    bool v=(x);                                         \
    if(OB_UNLIKELY(!(v))) {                             \
      _OB_LOG_RET(ERROR, oceanbase::common::OB_ERROR, "assert fail, exp=%s", #x);        \
      BACKTRACE_RET(ERROR, oceanbase::common::OB_ERROR, 1, "assert fail");               \
      ob_abort();                                       \
    }                                                   \
  } while(false)
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{

enum class NodePosition {
  MDS_TABLE,
  DISK,
  KV_CACHE,
  TABLET,
  POSITION_END,
};

enum class FowEachRowAction {
  CALCUALTE_FLUSH_SCN,
  COUNT_NODES_BEFLOW_FLUSH_SCN,
  CALCULATE_REC_SCN,
  RECYCLE,
  RESET,
};

inline const char *obj_to_string(NodePosition pos) {
  const char *ret = "UNKNOWN";
  switch (pos) {
  case NodePosition::MDS_TABLE:
    ret = "MDS_TABLE";
    break;
  case NodePosition::DISK:
    ret = "DISK";
    break;
  case NodePosition::KV_CACHE:
    ret = "KV_CACHE";
    break;
  case NodePosition::TABLET:
    ret = "TABLET";
    break;
  default:
    break;
  }
  return ret;
}

enum class TwoPhaseCommitState : uint8_t
{
  STATE_INIT = 0,
  BEFORE_PREPARE,
  ON_PREPARE,
  ON_COMMIT,
  ON_ABORT,
  STATE_END
};

static inline const char *obj_to_string(TwoPhaseCommitState state) {
  const char *ret = "UNKNOWN";
  switch (state) {
    case TwoPhaseCommitState::STATE_INIT:
    ret = "INIT";
    break;
    case TwoPhaseCommitState::BEFORE_PREPARE:
    ret = "BEFORE_PREPARE";
    break;
    case TwoPhaseCommitState::ON_PREPARE:
    ret = "ON_PREPARE";
    break;
    case TwoPhaseCommitState::ON_COMMIT:
    ret = "ON_COMMIT";
    break;
    case TwoPhaseCommitState::ON_ABORT:
    ret = "ON_ABORT";
    break;
    default:
    break;
  }
  return ret;
}

static constexpr bool STATE_CHECK_ALLOWED_MAP[static_cast<int>(TwoPhaseCommitState::STATE_END)]
                                             [static_cast<int>(TwoPhaseCommitState::STATE_END)] = {
  {0, 1, 1, 1, 1},// from INIT, can change to any state(in replay phase)
  {0, 1, 1, 1, 1},// from BEFORE_PREPARE, just allow to switch to ON_PREPARE/ON_ABORT, may switch to COMMIT in one-phase commit
  {0, 0, 1, 1, 1},// from ON_PREPARE, allow to switch to ON_COMMIT/ON_ABORT
  {0, 0, 0, 1, 0},// maybe repeat switch to ON_COMMIT
  {0, 0, 0, 0, 1},// maybe repeat switch to ON_ABORT
};

static inline void check_and_advance_two_phase_commit(TwoPhaseCommitState &state, TwoPhaseCommitState new_state)
{
  MDS_ASSERT(STATE_CHECK_ALLOWED_MAP[(int)state][(int)new_state] == true);
  state = new_state;
}

enum class WriterType : uint8_t
{
  UNKNOWN_WRITER = 0,
  TRANSACTION,
  AUTO_INC_SEQ,
  MEDIUM_INFO,
  END,
};

static inline const char *obj_to_string(WriterType type) {
  const char *ret = "UNKNOWN";
  switch (type) {
    case WriterType::TRANSACTION:
      ret = "TRANS";
      break;
    case WriterType::AUTO_INC_SEQ:
      ret = "AUTO_INC_SEQ";
      break;
    case WriterType::MEDIUM_INFO:
      ret = "MEDIUM_INFO";
      break;
    default:
      break;
  }
  return ret;
}

static inline const char *obj_to_string(share::SCN scn) {
  const char *ret = nullptr;
  if (scn == share::SCN::max_scn()) {
    ret = "MAX";
  } else if (scn == share::SCN::min_scn()) {
    ret = "MIN";
  } else {
    ret = to_cstring(scn);
  }
  return ret;
}

enum class MdsNodeType : uint8_t
{
  UNKNOWN_NODE = 0,
  SET,
  DELETE,
  TYPE_END
};

static inline const char *obj_to_string(MdsNodeType type) {
  const char *ret = "UNKNOWN";
  switch (type) {
    case MdsNodeType::SET:
    ret = "SET";
    break;
    case MdsNodeType::DELETE:
    ret = "DELETE";
    break;
    default:
    break;
  }
  return ret;
}

enum LogPhase
{
  INIT,
  DESTROY,
  SET,
  GET,
  DUMP,
  LOAD,
  SCAN,
  FLUSH,
  GC,
  FREEZE,
  NOTICE,
  NONE
};

constexpr int64_t INVALID_VALUE = -1;
#define MDS_TG(ms) TIMEGUARD_INIT(MDS, ms)
#define _MDS_LOG_PHASE(level, phase, info, args...) \
do {\
  if (phase == mds::LogPhase::NONE) {\
    MDS_LOG(level, info, ##args, PRINT_WRAPPER);\
  } else {\
    constexpr int64_t joined_length = 512;\
    char joined_info[joined_length] = {0};\
    int64_t pos = 0;\
    switch (phase) {\
    case mds::LogPhase::INIT:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[INIT]%s", info);\
      break;\
    case mds::LogPhase::DESTROY:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[DESTROY]%s", info);\
      break;\
    case mds::LogPhase::SET:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[SET]%s", info);\
      break;\
    case mds::LogPhase::GET:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[GET]%s", info);\
      break;\
    case mds::LogPhase::DUMP:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[DUMP]%s", info);\
      break;\
    case mds::LogPhase::LOAD:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[LOAD]%s", info);\
      break;\
    case mds::LogPhase::SCAN:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[SCAN]%s", info);\
      break;\
    case mds::LogPhase::FLUSH:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[FLUSH]%s", info);\
      break;\
    case mds::LogPhase::GC:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[GC]%s", info);\
      break;\
    case mds::LogPhase::FREEZE:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[FREEZE]%s", info);\
      break;\
    case mds::LogPhase::NOTICE:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[NOTICE]%s", info);\
      break;\
    case mds::LogPhase::NONE:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[NONE]%s", info);\
      break;\
    default:\
      oceanbase::common::databuff_printf(joined_info, joined_length, pos, "[UNKNOWN]%s", info);\
      break;\
    }\
    MDS_LOG(level, joined_info, ##args, PRINT_WRAPPER);\
  }\
} while(0)

#define MDS_LOG_INIT(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::INIT, info, ##args)
#define MDS_LOG_DESTROY(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::DESTROY, info, ##args)
#define MDS_LOG_SET(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::SET, info, ##args)
#define MDS_LOG_GET(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::GET, info, ##args)
#define MDS_LOG_DUMP(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::DUMP, info, ##args)
#define MDS_LOG_LOAD(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::LOAD, info, ##args)
#define MDS_LOG_SCAN(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::SCAN, info, ##args)
#define MDS_LOG_FLUSH(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::FLUSH, info, ##args)
#define MDS_LOG_GC(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::GC, info, ##args)
#define MDS_LOG_FREEZE(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::FREEZE, info, ##args)
#define MDS_LOG_NOTICE(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::NOTICE, info, ##args)
#define MDS_LOG_NONE(level, info, args...) _MDS_LOG_PHASE(level, mds::LogPhase::NONE, info, ##args)

// flag is needed to rollback logic
#define MDS_FAIL_FLAG(stmt, flag) (CLICK_FAIL(stmt) || FALSE_IT(flag = true))
#define MDS_FAIL(stmt) (CLICK_FAIL(stmt))
#define MDS_TMP_FAIL(stmt) (CLICK_TMP_FAIL(stmt))

}
}
}
#endif