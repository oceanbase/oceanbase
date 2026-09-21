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

#ifndef OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_PAYLOAD_H_
#define OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_PAYLOAD_H_

#include "lib/allocator/ob_allocator.h"
#include "ob_log_binlog_record.h"

namespace oceanbase
{
namespace libobcdc
{

// Serialize the complete DELETE with the BR library. The returned bytes borrow
// the thread-local message buffer and must be consumed synchronously before the
// next call on this thread or destruction of the DELETE BR. Do not free them.
int serialize_merge_delete_br(
    IBinlogRecord &del_data,
    const char *&data,
    int64_t &data_len);

// Parse a persisted DELETE and append its old columns to the INSERT. Values are
// copied into allocator, which must outlive the INSERT BR. Validate and prepare
// every column before changing the INSERT; preserve NULL, empty strings and origins.
int apply_serialized_merge_delete_br(
    const char *data,
    const int64_t data_len,
    common::ObIAllocator &allocator,
    IBinlogRecord &ins_data);

} // namespace libobcdc
} // namespace oceanbase

#endif
