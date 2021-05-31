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

#include "ob_partition_schema_recorder.h"

#include "clog/ob_partition_log_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_pg_partition.h"

namespace oceanbase {

using namespace common;
using namespace clog;
using namespace share::schema;

namespace storage {

int ObPartitionSchemaRecorder::replay_schema_log(const char* buf, const int64_t size, const int64_t log_id)
{
  int ret = OB_SUCCESS;

  UNUSED(buf);
  UNUSED(size);
  UNUSED(log_id);
  return ret;
}

}  // namespace storage

}  // namespace oceanbase
