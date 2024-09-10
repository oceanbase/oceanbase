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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
#include  "deps/oblib/src/lib/ob_define.h"

namespace oceanbase
{
namespace tmp_file
{
struct ObTmpFileGlobal final
{
  // TMP_FILE
  static const int64_t INVALID_TMP_FILE_FD;
  static const int64_t INVALID_TMP_FILE_DIR_ID;

  static const int64_t PAGE_SIZE = 8 * 1024;  // 8KB
  static const int64_t BLOCK_PAGE_NUMS =
                       OB_DEFAULT_MACRO_BLOCK_SIZE / PAGE_SIZE;   // 256 pages per macro block

  static const int64_t TMP_FILE_READ_BATCH_SIZE;
  static const int64_t TMP_FILE_WRITE_BATCH_PAGE_NUM;

  static const int64_t TMP_FILE_MAX_LABEL_SIZE = 15;

  // TMP_FILE_BLOCK
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
  static const int64_t INVALID_FLUSH_SEQUENCE = -1;
};


}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_GLOBAL_H_
