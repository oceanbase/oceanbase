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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_global.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace tmp_file
{
const int64_t ObTmpFileGlobal::INVALID_TMP_FILE_FD = -1;
const int64_t ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID = -1;
const int64_t ObTmpFileGlobal::TMP_FILE_READ_BATCH_SIZE = 8 * 1024 * 1024;   // 8MB
const int64_t ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM = 16;
const int64_t ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_SIZE = TMP_FILE_WRITE_BATCH_PAGE_NUM * ObTmpFileGlobal::PAGE_SIZE;
const int64_t ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX = -1;
const uint32_t ObTmpFileGlobal::INVALID_PAGE_ID = UINT32_MAX;
const int64_t ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID = -1;

#ifdef OB_BUILD_SHARED_STORAGE
int ObTmpFileGlobal::advance_flush_ctx_state(const FlushCtxState cur_stage, FlushCtxState &next_stage)
{
  int ret = OB_SUCCESS;
  switch (cur_stage) {
    case FlushCtxState::FSM_F1:
      next_stage = FlushCtxState::FSM_F2;
      break;
    case FlushCtxState::FSM_F2:
      next_stage = FlushCtxState::FSM_F3;
      break;
    case FlushCtxState::FSM_F3:
      next_stage = FlushCtxState::FSM_FINISHED;
      break;
    default:
      next_stage = FlushCtxState::FSM_FINISHED;
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid flush ctx state", K(ret), K(cur_stage));
      break;
  }
  return ret;
}

int ObTmpFileGlobal::switch_data_list_level_to_flush_state(const FileList file_list, FlushCtxState &flush_state)
{
  int ret = OB_SUCCESS;
  switch (file_list) {
    case FileList::L1:
      flush_state = FlushCtxState::FSM_F1;
      break;
    case FileList::L2:
    case FileList::L3:
    case FileList::L4:
      flush_state = FlushCtxState::FSM_F2;
      break;
    case FileList::L5:
      flush_state = FlushCtxState::FSM_F3;
      break;
    default:
      flush_state = FlushCtxState::FSM_FINISHED;
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid flush ctx state", K(ret), K(file_list));
      break;
  }
  return ret;
}
#endif

} // end namespace tmp_file
} // end namespace oceanbase
