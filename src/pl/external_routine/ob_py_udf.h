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

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UDF_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UDF_H_

#include "lib/hash/ob_hashset.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_sandbox_manager.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf.h"

namespace oceanbase
{
namespace pl
{

// Frame message type constants (must match ob_python_executor.py)
static constexpr uint32_t OB_PY_FRAME_MAGIC      = 0x50594F42;  // "PYOB"
static constexpr uint32_t OB_PY_MSG_LOAD          = 1;
static constexpr uint32_t OB_PY_MSG_EXECUTE       = 2;
static constexpr uint32_t OB_PY_MSG_RESULT_OK     = 3;
static constexpr uint32_t OB_PY_MSG_RESULT_ERR    = 4;
static constexpr uint32_t OB_PY_MSG_SHUTDOWN      = 7;

// Frame header size: magic(4) + msg_type(4) + body_len(4) = 12 bytes
static constexpr uint32_t OB_PY_FRAME_HEADER_SIZE = 12;
// Maximum allowed frame body size (32 MB) to prevent OOM from malicious sandbox
static constexpr uint32_t OB_PY_MAX_FRAME_BODY_SIZE = 32 * 1024 * 1024;
// Default recv timeout when worker timeout is unavailable
static constexpr int OB_PY_DEFAULT_RECV_TIMEOUT_MS = 30000;
// Default hash set bucket count for loaded UDF set
static constexpr int64_t OB_PY_LOADED_UDF_SET_BUCKET = 16;

// Sandbox root path (relative to cwd, must match ob_sandbox_protocol.h OB_SANDBOX_ROOT_PATH)
static constexpr const char *OB_SANDBOX_ROOT_DIR = "run/ob_sandbox";
// Python executor script filename (will be resolved to absolute path at runtime)
static constexpr const char *OB_PYTHON_EXECUTOR_SCRIPT_NAME = "lib/ob_python_executor.py";

// Opaque context for Python UDF sandbox process and loaded UDF set.
// Stored as void* in ObExecContext to keep sandbox details transparent.
class ObPyUDFContext
{
public:
  ObPyUDFContext() : sandbox_process_(nullptr), loaded_udf_set_() {}
  ~ObPyUDFContext();

  observer::ObSandboxProcess *get_sandbox_process() { return sandbox_process_; }
  void set_sandbox_process(observer::ObSandboxProcess *p) { sandbox_process_ = p; }

  common::hash::ObHashSet<int64_t> &get_loaded_udf_set() { return loaded_udf_set_; }
  bool is_loaded_set_inited() const { return loaded_udf_set_.created(); }
  int init_loaded_set();

  // Called from ObExecContext destructor to destroy the opaque py_udf_ctx_ pointer.
  // Sends SHUTDOWN to sandbox, destroys sandbox process and loaded set.
  static void destroy(void *py_udf_ctx, common::ObIAllocator &alloc);

private:
  observer::ObSandboxProcess *sandbox_process_;
  common::hash::ObHashSet<int64_t> loaded_udf_set_;
};

class ObPyUDFExecutor
{
public:
  ObPyUDFExecutor(sql::ObExecContext &exec_ctx, const sql::ObExprUDFInfo &udf_info)
    : exec_ctx_(exec_ctx), udf_info_(udf_info) {}
  ~ObPyUDFExecutor() {}

  // init() keeps semantic unchanged (lazy initialization, idempotent), external call pattern unchanged
  int init();

  int execute(int64_t batch_size,
              const common::ObIArray<common::ObObjMeta> &arg_types,
              const common::ObIArray<common::ObIArray<common::ObObj>*> &args,
              common::ObIAllocator &result_allocator,
              common::ObIArray<common::ObObj> &result);

private:
  int ensure_sandbox();       // lazy create sandbox process (idempotent)
  int ensure_udf_loaded();    // statement-level idempotent LOAD

  // Frame send/recv (based on ObSandboxProcess pipe)
  int send_frame(uint32_t msg_type, const uint8_t *body, uint32_t body_len);
  int recv_frame(uint32_t &msg_type, common::ObSqlString &body);
  void extract_and_set_user_error(const common::ObSqlString &resp_body);

  // Determine execution mode from udf_info_
  const char *get_mode_str() const;

  // Build JSON for LOAD message with manual escaping
  int build_load_json(const common::ObString &source,
                      common::ObSqlString &json_out);
  // JSON-escape a string and append to json_out
  int json_escape_append(const common::ObString &str, common::ObSqlString &json_out);

  sql::ObExecContext &exec_ctx_;
  const sql::ObExprUDFInfo &udf_info_;
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UDF_H_
