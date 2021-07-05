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

#ifndef OCEANBASE_STORAGE_PARTITION_SCHEMA_RECORDER_
#define OCEANBASE_STORAGE_PARTITION_SCHEMA_RECORDER_

#include <stdint.h>

#include "clog/ob_i_submit_log_cb.h"
#include "lib/container/ob_iarray.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {

namespace clog {
class ObIPartitionLogService;
}  // namespace clog

namespace common {
struct ObPartitionKey;
}

namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
class ObTableSchema;
}  // namespace schema
}  // namespace share

namespace storage {

class ObPGPartition;

class ObPartitionSchemaRecorder {

public:
  // follower
  int replay_schema_log(const char* buf, const int64_t size, const int64_t log_id);

  ObPartitionSchemaRecorder()
  {}
  ~ObPartitionSchemaRecorder()
  {}

  ObPartitionSchemaRecorder(const ObPartitionSchemaRecorder&) = delete;
  ObPartitionSchemaRecorder& operator=(const ObPartitionSchemaRecorder&) = delete;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_PARTITION_SCHEMA_RECORDER_ */
