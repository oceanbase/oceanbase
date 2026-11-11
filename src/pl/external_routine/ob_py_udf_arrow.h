/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PY_UDF_ARROW_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PY_UDF_ARROW_H_

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "sql/engine/basic/ob_arrow_basic.h"  // ObArrowMemPool
#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace pl
{

// ObObj column arrays → Arrow IPC stream bytes (for EXECUTE message body)
// The serialized buffer lifetime is managed by the caller (arrow::Buffer reference counting)
int ob_udf_args_to_arrow(
    int64_t udf_id,
    const common::ObString &mode,              // "scalar" or "arrow"
    const common::ObIArray<common::ObObjMeta> &arg_types,
    const common::ObIArray<common::ObIArray<common::ObObj>*> &args,
    int64_t batch_size,
    const common::ObObjMeta &result_meta,      // return type for Python-side hint
    sql::ObArrowMemPool &pool,
    std::shared_ptr<arrow::Buffer> &out_buf);

// Arrow IPC stream bytes → ObObj result array (parse RESULT_OK message body)
int ob_udf_result_from_arrow(
    const uint8_t *ipc_bytes,
    int64_t byte_len,
    const common::ObObjMeta &result_meta,
    sql::ObArrowMemPool &pool,
    common::ObIAllocator &allocator,
    common::ObIArray<common::ObObj> &results);

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PY_UDF_ARROW_H_
