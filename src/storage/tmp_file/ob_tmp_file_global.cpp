/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tmp_file/ob_tmp_file_global.h"

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

} // end namespace tmp_file
} // end namespace oceanbase
