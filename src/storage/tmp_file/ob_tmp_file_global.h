/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
#include  "deps/oblib/src/lib/ob_define.h"

namespace oceanbase
{
namespace tmp_file
{
struct ObTmpFileGlobal final
{
  // SN_TMP_FILE
  static const int64_t INVALID_TMP_FILE_FD;
  static const int64_t INVALID_TMP_FILE_DIR_ID;

  static constexpr int64_t PAGE_SIZE = 8 * 1024;  // 8KB
  static constexpr int64_t SN_BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE; // 2MB
  static constexpr int64_t BLOCK_PAGE_NUMS =
                           SN_BLOCK_SIZE / PAGE_SIZE;   // 256 pages per macro block

  static const int64_t TMP_FILE_READ_BATCH_SIZE;
  static const int64_t TMP_FILE_WRITE_BATCH_PAGE_NUM;
  static const int64_t TMP_FILE_WRITE_BATCH_SIZE;

  static const int64_t TMP_FILE_MAX_LABEL_SIZE = 15;

  // SN_TMP_FILE_BLOCK
  static const int64_t INVALID_TMP_FILE_BLOCK_INDEX;
  static const int64_t TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM = 64; // 512KB
  static const int64_t TMP_FILE_MIN_SHARED_PRE_ALLOC_PAGE_NUM = 2; // 16KB
  static const int64_t TMP_FILE_MAX_SHARED_PRE_ALLOC_BLOCK_NUM = 4;

  // TMP_FILE_WRITE_BUFFER
  static const uint32_t INVALID_PAGE_ID;
  static const int64_t INVALID_VIRTUAL_PAGE_ID;

};

enum OB_TMP_FILE_TYPE
{
  NORMAL = 0,
  COMPRESS_BUFFER = 1,
  COMPRESS_STORE = 2,
  COMPRESS_INDEX = 3,
};

enum class BlockFlushLevel : int8_t {
  INVALID = -1,
  L1 = 0, // for exclusive block, flushing page num is in (64, 256)
  L2,     // for exclusive block, free page num is in (0, 64]
  L3,     // for shared block, free page num is in (64, 256)
  L4,     // for shared block, free page num is in (0, 64]
  L5,     // all flushing pages are incomplete
  MAX
};

constexpr int64_t to_flush_level_idx(const BlockFlushLevel level)
{
  return static_cast<int64_t>(level);
}

#define REACH_TIME_INTERVAL_WITH_TS(last_ts_ptr, interval) \
  ({ \
    bool bret = false; \
    int64_t cur_time = common::ObClockGenerator::getClock(); \
    int64_t last_time = ATOMIC_LOAD(last_ts_ptr); \
    if (OB_UNLIKELY((interval + last_time) < cur_time)) \
    { \
      if (last_time == ATOMIC_CAS(last_ts_ptr, last_time, cur_time)) { \
        bret = true; \
      } \
    } \
    bret; \
  })

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
