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

  static const int64_t TMP_FILE_MAX_LABEL_SIZE = 15;

  // SN_TMP_FILE_BLOCK
  static const int64_t INVALID_TMP_FILE_BLOCK_INDEX;

  // TMP_FILE_WRITE_BUFFER
  static const uint32_t INVALID_PAGE_ID;
  static const int64_t INVALID_VIRTUAL_PAGE_ID;

  // TMP_FILE_FLUSH_STAGE
  enum FlushCtxState
  {
    FSM_F1 = 0,  // flush data list L1
    FSM_F2 = 1,  // flush data list L2 & L3 & L4
    FSM_F3 = 2,  // flush data list L5
    FSM_F4 = 3,  // flush meta list non-rightmost pages
    FSM_F5 = 4,  // flush meta list rightmost pages
    FSM_FINISHED = 5
  };
  static int advance_flush_ctx_state(const FlushCtxState cur_stage, FlushCtxState &next_stage);
  static const int64_t INVALID_FLUSH_SEQUENCE = -1;
  static const int32_t FLUSH_TIMER_CNT = 4;
  static const int64_t MAX_FLUSHING_BLOCK_NUM = 200;

  enum FileList {
    INVALID = -1,
    L1 = 0, // [2MB, INFINITE)
    L2,     // [1MB, 2MB)
    L3,     // [128KB, 1MB)
    L4,     // data_list: [8KB, 128KB); meta_list: (0KB, 128KB)
    L5,     // data_list: (0, 8KB); meta_list: 0KB
    MAX
  };
  static int switch_data_list_level_to_flush_state(const FileList list_level, FlushCtxState &flush_state);
  static const int64_t TMP_FILE_STAT_FREQUENCY = 1 * 1000 * 1000; // 1s
#ifdef OB_BUILD_SHARED_STORAGE
  // SS_TMP_FILE
  static const int64_t SHARE_STORAGE_DIR_ID = 1;
  // Attention:
  // SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS is just a hint value.
  // the real wait timeout period is also depend on GCONF._data_storage_io_timeout and tenant_config->_object_storage_io_timeout
  static const int64_t SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS = 30 * 1000;   // 30s
  static constexpr double SS_TMP_FILE_FLUSH_PROP = 0.2;
  static constexpr double SS_TMP_FILE_SAFE_WBP_PROP = 0.8;
  static constexpr int64_t SS_BLOCK_SIZE = 2 << 20; // 2MB
  static constexpr int64_t SS_BLOCK_PAGE_NUMS =
                           SS_BLOCK_SIZE / PAGE_SIZE;   // 256 pages per macro block
#endif
};


}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
