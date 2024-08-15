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

namespace oceanbase
{
namespace tmp_file
{
const int64_t ObTmpFileGlobal::INVALID_TMP_FILE_FD = -1;
const int64_t ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID = -1;
const int64_t ObTmpFileGlobal::TMP_FILE_READ_BATCH_SIZE = 8 * 1024 * 1024;   // 8MB
const int64_t ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM = 16;
const int64_t ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX = -1;
const uint32_t ObTmpFileGlobal::INVALID_PAGE_ID = UINT32_MAX;
const int64_t ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID = -1;

} // end namespace tmp_file
} // end namespace oceanbase
