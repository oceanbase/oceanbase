/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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