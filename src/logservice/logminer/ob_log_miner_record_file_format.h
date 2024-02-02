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

#ifndef OCEANBASE_LOG_MINER_RECORD_FILE_FORMAT_H_
#define OCEANBASE_LOG_MINER_RECORD_FILE_FORMAT_H_

#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace oblogminer
{
enum class RecordFileFormat
{
  INVALID = -1,
  CSV = 0,
  REDO_ONLY,
  UNDO_ONLY,
  JSON,
  PARQUET,
  AVRO
};
RecordFileFormat get_record_file_format(const common::ObString &file_format_str);

RecordFileFormat get_record_file_format(const char *file_format_str);

const char *record_file_format_str(const RecordFileFormat format);
}
}
#endif