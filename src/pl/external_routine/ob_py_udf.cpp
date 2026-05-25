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

#define USING_LOG_PREFIX PL

#include <arpa/inet.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ob_py_udf.h"
#include "lib/worker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "pl/external_routine/ob_external_resource.h"
#include "pl/external_routine/ob_py_udf_arrow.h"
#include "share/config/ob_server_config.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{

using namespace common;
using namespace share::schema;

namespace pl
{

// ==================== ObPyUDFContext ====================

ObPyUDFContext::~ObPyUDFContext()
{
  // Note: sandbox_process_ cleanup (SHUTDOWN + destructor) is handled by
  // ObPyUDFContext::destroy() which clears sandbox_process_ before calling
  // this destructor to avoid double-destroy.
  if (OB_NOT_NULL(sandbox_process_)) {
    // Defensive: if destroy() was not used, still clean up
    uint32_t hdr[3] = {htonl(OB_PY_FRAME_MAGIC), htonl(OB_PY_MSG_SHUTDOWN), htonl(0)};
    IGNORE_RETURN sandbox_process_->send_msg(
        reinterpret_cast<char*>(hdr), sizeof(hdr));
    sandbox_process_->~ObSandboxProcess();
    sandbox_process_ = nullptr;
  }
  if (loaded_udf_set_.created()) {
    loaded_udf_set_.destroy();
  }
}

int ObPyUDFContext::init_loaded_set()
{
  return loaded_udf_set_.create(OB_PY_LOADED_UDF_SET_BUCKET);
}

void ObPyUDFContext::destroy(void *py_udf_ctx, ObIAllocator &alloc)
{
  if (OB_NOT_NULL(py_udf_ctx)) {
    ObPyUDFContext *ctx = static_cast<ObPyUDFContext*>(py_udf_ctx);
    // Take ownership of sandbox_process_ before calling destructor,
    // so ~ObPyUDFContext() will not double-destroy it.
    observer::ObSandboxProcess *sp = ctx->get_sandbox_process();
    ctx->set_sandbox_process(nullptr);
    // Send SHUTDOWN before destroying the sandbox process
    if (OB_NOT_NULL(sp)) {
      uint32_t hdr[3] = {htonl(OB_PY_FRAME_MAGIC), htonl(OB_PY_MSG_SHUTDOWN), htonl(0)};
      IGNORE_RETURN sp->send_msg(reinterpret_cast<char*>(hdr), sizeof(hdr));
    }
    ctx->~ObPyUDFContext();
    // Free the sandbox process memory (allocated separately from ObPyUDFContext)
    if (OB_NOT_NULL(sp)) {
      sp->~ObSandboxProcess();
      alloc.free(sp);
    }
    alloc.free(ctx);
  }
}

// ==================== ObPyUDFExecutor ====================

const char *ObPyUDFExecutor::get_mode_str() const
{
  // Default to scalar mode; arrow mode can be extended later
  return "scalar";
}

int ObPyUDFExecutor::init()
{
  return ensure_sandbox();
}

// Ensure parent directories exist and create an empty file as bind mount point.
// This is needed because minijail_bind() requires the destination to exist
// inside the sandbox root before pivot_root.
static int ensure_sandbox_mount_point(const char *sandbox_root, const char *path)
{
  int ret = OB_SUCCESS;
  // Validate path: must be absolute and must not contain ".." to prevent path traversal
  if (OB_ISNULL(sandbox_root) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null argument to ensure_sandbox_mount_point", K(ret));
  } else if (path[0] != '/') {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sandbox mount path must be absolute", K(ret), K(path));
  } else if (OB_NOT_NULL(strstr(path, "/.."))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sandbox mount path must not contain '..'", K(ret), K(path));
  }

  char full_path[common::MAX_PATH_SIZE];
  if (OB_SUCC(ret)) {
    int n = snprintf(full_path, sizeof(full_path), "%s%s", sandbox_root, path);
    if (OB_UNLIKELY(n < 0 || n >= static_cast<int>(sizeof(full_path)))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("sandbox mount full_path truncated in snprintf", K(ret), K(n),
               K(sizeof(full_path)), K(sandbox_root), K(path));
    }
  }

  // Ensure sandbox_root directory exists before realpath check
  if (OB_SUCC(ret)) {
    if (mkdir(sandbox_root, 0755) != 0 && errno != EEXIST) {
      ret = OB_IO_ERROR;
      int err = errno;
      LOG_WARN("failed to create sandbox root directory", K(ret), K(sandbox_root), K(err));
    }
  }

  // Resolve sandbox_root to absolute path, then verify full_path stays within it
  // to defend against symlink traversal
  if (OB_SUCC(ret)) {
    char resolved_root[common::MAX_PATH_SIZE];
    if (OB_ISNULL(realpath(sandbox_root, resolved_root))) {
      ret = OB_IO_ERROR;
      int err = errno;
      LOG_WARN("failed to resolve sandbox_root realpath", K(ret), K(err), K(sandbox_root));
    } else {
      size_t root_len = strlen(resolved_root);
      if (strncmp(full_path, resolved_root, root_len) != 0
          || (full_path[root_len] != '/' && full_path[root_len] != '\0')) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("mount point escapes sandbox root", K(ret), K(full_path), K(resolved_root));
      }
    }
  }

  // Create parent directories and mount point file atomically (avoid TOCTOU)
  if (OB_SUCC(ret)) {
    // mkdir -p for parent directories
    char dir_buf[common::MAX_PATH_SIZE];
    MEMCPY(dir_buf, full_path, strlen(full_path) + 1);
    char *dir = dirname(dir_buf);
    char tmp[common::MAX_PATH_SIZE];
    MEMCPY(tmp, dir, strlen(dir) + 1);
    for (char *p = tmp + 1; *p; ++p) {
      if (*p == '/') {
        *p = '\0';
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
          ret = OB_IO_ERROR;
          int err = errno;
          LOG_WARN("failed to create parent directory for mount point", K(ret), K(tmp), K(err));
        }
        *p = '/';
      }
    }
    if (OB_SUCC(ret) && mkdir(tmp, 0755) != 0 && errno != EEXIST) {
      ret = OB_IO_ERROR;
      int err = errno;
      LOG_WARN("failed to create parent directory for mount point", K(ret), K(tmp), K(err));
    }

    // Atomically create empty file as mount point; O_EXCL avoids truncating if it exists
    if (OB_SUCC(ret)) {
      int fd = open(full_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
      if (fd >= 0) {
        close(fd);
        LOG_TRACE("created sandbox mount point", K(full_path));
      } else if (errno == EEXIST) {
        // already exists, nothing to do
      } else {
        ret = OB_IO_ERROR;
        int err = errno;
        LOG_WARN("failed to create sandbox mount point", K(ret), K(full_path), K(err));
      }
    }
  }
  return ret;
}

int ObPyUDFExecutor::ensure_sandbox()
{
  int ret = OB_SUCCESS;
  ObPyUDFContext *udf_ctx = static_cast<ObPyUDFContext*>(exec_ctx_.get_py_udf_ctx());
  if (OB_NOT_NULL(udf_ctx) && OB_NOT_NULL(udf_ctx->get_sandbox_process())) {
    // Already created, check if still alive
    observer::ObSandboxProcess *sp = udf_ctx->get_sandbox_process();
    if (sp->get_state() != observer::SandboxState::STATE_RUNNING) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sandbox process not running", K(ret));
    }
  } else {
    char script_path[common::MAX_PATH_SIZE];
    char python_path[common::MAX_PATH_SIZE];
    char python_home_resolved[common::MAX_PATH_SIZE];
    char sandbox_root[common::MAX_PATH_SIZE];
    python_home_resolved[0] = '\0';

    // Scope cwd_buf so compiler can reuse its stack space after this block
    {
      char cwd_buf[common::MAX_PATH_SIZE];
      if (OB_ISNULL(getcwd(cwd_buf, sizeof(cwd_buf)))) {
        ret = OB_IO_ERROR;
        int err = errno;
        LOG_WARN("failed to get cwd", K(ret), K(err));
      } else {
        int n1 = snprintf(script_path, sizeof(script_path), "%s/%s", cwd_buf, OB_PYTHON_EXECUTOR_SCRIPT_NAME);
        int n2 = snprintf(sandbox_root, sizeof(sandbox_root), "%s/%s", cwd_buf, OB_SANDBOX_ROOT_DIR);
        if (OB_UNLIKELY(n1 < 0 || n1 >= static_cast<int>(sizeof(script_path))
                         || n2 < 0 || n2 >= static_cast<int>(sizeof(sandbox_root)))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("path truncated in snprintf", K(ret), K(n1), K(n2),
                   K(sizeof(script_path)), K(sizeof(sandbox_root)));
        }
      }
    }

    // Resolve python3 binary path from ob_python_home config
    if (OB_SUCC(ret)) {
      const ObString python_home = GCONF.ob_python_home.get_value_string();
      if (python_home.empty()) {
        ret = OB_PYTHON_PARAMS_ERROR;
        LOG_WARN("ob_python_home is not configured", K(ret));
        LOG_USER_ERROR(OB_PYTHON_PARAMS_ERROR, "ob_python_home was not configured");
      } else {
        char python3_symlink[common::MAX_PATH_SIZE];
        int n = snprintf(python3_symlink, sizeof(python3_symlink), "%.*s/bin/python3",
                         static_cast<int>(python_home.length()), python_home.ptr());
        if (OB_UNLIKELY(n < 0 || n >= static_cast<int>(sizeof(python3_symlink)))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("python3 path truncated", K(ret), K(n), K(python_home));
        } else if (OB_ISNULL(realpath(python3_symlink, python_path))) {
          ret = OB_IO_ERROR;
          int err = errno;
          LOG_WARN("failed to resolve python3 real path", K(ret), K(err),
                   K(python3_symlink), K(python_home));
        } else {
          LOG_TRACE("resolved python3 path", K(python3_symlink), K(python_path));
          // Resolve python_home to absolute path for mounting into sandbox
          // Python needs its lib/ directory (libpython3.13.so, stdlib, etc.)
          char python_home_buf[common::MAX_PATH_SIZE];
          int nh = snprintf(python_home_buf, sizeof(python_home_buf), "%.*s",
                            static_cast<int>(python_home.length()), python_home.ptr());
          if (OB_LIKELY(nh > 0 && nh < static_cast<int>(sizeof(python_home_buf)))) {
            if (OB_NOT_NULL(realpath(python_home_buf, python_home_resolved))) {
              LOG_TRACE("resolved python home path", K(python_home_resolved));
            } else {
              // realpath failed, use the raw config value
              MEMCPY(python_home_resolved, python_home_buf, nh + 1);
              LOG_WARN("failed to resolve python home realpath, using raw value",
                       K(python_home_resolved), K(errno));
            }
          }
        }
      }
    }

    // Ensure mount points exist in sandbox root for binary and script
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ensure_sandbox_mount_point(sandbox_root, python_path))) {
        LOG_WARN("failed to ensure python binary mount point", K(ret), K(python_path));
      } else if (OB_FAIL(ensure_sandbox_mount_point(sandbox_root, script_path))) {
        LOG_WARN("failed to ensure script mount point", K(ret), K(script_path));
      }
    }

    // Lazy create ObPyUDFContext if not yet created
    if (OB_SUCC(ret) && OB_ISNULL(udf_ctx)) {
      void *ctx_buf = exec_ctx_.get_allocator().alloc(sizeof(ObPyUDFContext));
      if (OB_ISNULL(ctx_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc ObPyUDFContext", K(ret));
      } else {
        udf_ctx = new(ctx_buf) ObPyUDFContext();
        exec_ctx_.set_py_udf_ctx(udf_ctx);
      }
    }

    // Lazy create sandbox process
    observer::ObSandboxProcess *sp = nullptr;
    void *buf = nullptr;
    if (OB_SUCC(ret)) {
      buf = exec_ctx_.get_allocator().alloc(sizeof(observer::ObSandboxProcess));
    }
    if (OB_FAIL(ret)) {
      // skip
    } else if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sandbox process", K(ret));
    } else {
      sp = new(buf) observer::ObSandboxProcess(
          python_path,
          script_path);
      sp->set_tenant_id(MTL_ID());
      // Mount Python home directory (libpython.so, stdlib, site-packages like pyarrow)
      if (python_home_resolved[0] != '\0') {
        if (OB_FAIL(sp->mount_path(python_home_resolved, python_home_resolved))) {
          LOG_WARN("failed to mount python home", K(ret), K(python_home_resolved));
        }
      }
      // Mount the executor script
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sp->mount_path(script_path, script_path))) {
          LOG_WARN("failed to mount script path", K(ret), K(script_path));
        }
      }
      if (OB_FAIL(ret)) {
        // mount failed, cleanup
        sp->~ObSandboxProcess();
        exec_ctx_.get_allocator().free(sp);
        sp = nullptr;
      } else if (OB_FAIL(sp->init_placeholder_mem_context())) {
        LOG_WARN("failed to init placeholder mem context", K(ret));
        sp->~ObSandboxProcess();
        exec_ctx_.get_allocator().free(sp);
        sp = nullptr;
      } else if (OB_FAIL(sp->start())) {
        LOG_WARN("failed to start sandbox process", K(ret));
        sp->~ObSandboxProcess();
        exec_ctx_.get_allocator().free(sp);
        sp = nullptr;
      } else {
        udf_ctx->set_sandbox_process(sp);
        LOG_TRACE("python sandbox process started", K(sp->get_pid()));
      }
    }
  }
  return ret;
}

int ObPyUDFExecutor::ensure_udf_loaded()
{
  int ret = OB_SUCCESS;
  ObPyUDFContext *udf_ctx = static_cast<ObPyUDFContext*>(exec_ctx_.get_py_udf_ctx());
  if (OB_ISNULL(udf_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("py_udf_ctx not initialized", K(ret));
  }

  // Initialize loaded set (lazy)
  if (OB_SUCC(ret) && !udf_ctx->is_loaded_set_inited()) {
    if (OB_FAIL(udf_ctx->init_loaded_set())) {
      LOG_WARN("failed to create loaded udf set", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    common::hash::ObHashSet<int64_t> &loaded_set = udf_ctx->get_loaded_udf_set();
    // Check if already loaded in this statement
    if (OB_HASH_EXIST == loaded_set.exist_refactored(udf_info_.udf_id_)) {
      // Already loaded, return directly
    } else {
      // Fetch source code
      ObSqlString source;
      if (udf_info_.external_routine_type_ ==
          ObExternalRoutineType::EXTERNAL_PY_UDF_FROM_URL) {
        if (OB_FAIL(ObExternalURLPy::fetch_source(
                udf_info_.external_routine_url_, source))) {
          LOG_WARN("failed to fetch py source from url", K(ret), K(udf_info_.external_routine_url_));
        }
      } else if (udf_info_.external_routine_type_ ==
                 ObExternalRoutineType::EXTERNAL_PY_UDF_FROM_RES) {
        ObSchemaGetterGuard *sg = nullptr;
        uint64_t database_id = OB_INVALID_ID;
        if (OB_ISNULL(exec_ctx_.get_sql_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL sql_ctx", K(ret));
        } else if (OB_ISNULL(sg = exec_ctx_.get_sql_ctx()->schema_guard_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL schema_guard", K(ret));
        } else if (OB_ISNULL(exec_ctx_.get_my_session())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL session", K(ret));
        } else if (OB_UNLIKELY(OB_INVALID_ID == (database_id = exec_ctx_.get_my_session()->get_database_id()))) {
          ret = OB_ERR_NO_DB_SELECTED;
          LOG_WARN("no db selected", K(ret));
        } else if (OB_FAIL(ObExternalSchemaPy::fetch_source(
                database_id, udf_info_.external_routine_resource_, *sg, source))) {
          LOG_WARN("failed to fetch py source from schema", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected external routine type for python", K(ret), K(udf_info_.external_routine_type_));
      }

      // Build and send LOAD message
      if (OB_SUCC(ret)) {
        ObSqlString json_str;
        if (OB_FAIL(build_load_json(source.string(), json_str))) {
          LOG_WARN("failed to build load json", K(ret));
        } else if (OB_FAIL(send_frame(OB_PY_MSG_LOAD,
                                       reinterpret_cast<const uint8_t*>(json_str.ptr()),
                                       static_cast<uint32_t>(json_str.length())))) {
          LOG_WARN("failed to send LOAD frame", K(ret));
        }
      }

      // Wait for LOAD acknowledgment from Python side
      if (OB_SUCC(ret)) {
        uint32_t resp_type = 0;
        ObSqlString resp_body;
        if (OB_FAIL(recv_frame(resp_type, resp_body))) {
          LOG_WARN("failed to receive LOAD ack", K(ret));
        } else if (resp_type == OB_PY_MSG_RESULT_ERR) {
          ret = OB_PYTHON_EXEC_ERROR;
          LOG_WARN("python LOAD error", K(ret), "body", resp_body.string());
          extract_and_set_user_error(resp_body);
        } else if (resp_type != OB_PY_MSG_RESULT_OK) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected LOAD response type", K(resp_type));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(loaded_set.set_refactored(udf_info_.udf_id_))) {
          LOG_WARN("failed to add udf_id to loaded set", K(ret), K(udf_info_.udf_id_));
        } else {
          LOG_TRACE("udf loaded to sandbox", K(udf_info_.udf_id_));
        }
      }
    }
  }
  return ret;
}

int ObPyUDFExecutor::json_escape_append(const ObString &str, ObSqlString &json_out)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < str.length(); ++i) {
    char c = str.ptr()[i];
    switch (c) {
      case '"':  ret = json_out.append("\\\""); break;
      case '\\': ret = json_out.append("\\\\"); break;
      case '\n': ret = json_out.append("\\n"); break;
      case '\r': ret = json_out.append("\\r"); break;
      case '\t': ret = json_out.append("\\t"); break;
      case '\b': ret = json_out.append("\\b"); break;
      case '\f': ret = json_out.append("\\f"); break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          ret = json_out.append(buf);
        } else {
          ret = json_out.append(&c, 1);
        }
        break;
    }
  }
  return ret;
}

int ObPyUDFExecutor::build_load_json(const ObString &source,
                                     ObSqlString &json_out)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(json_out.append("{\"udf_id\":"))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(json_out.append_fmt("%ld", udf_info_.udf_id_))) {
    LOG_WARN("failed to append udf_id", K(ret));
  } else if (OB_FAIL(json_out.append(",\"entry_name\":\""))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(json_escape_append(udf_info_.external_routine_entry_, json_out))) {
    LOG_WARN("failed to append entry_name", K(ret));
  } else if (OB_FAIL(json_out.append("\",\"source_code\":\""))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(json_escape_append(source, json_out))) {
    LOG_WARN("failed to append source_code", K(ret));
  } else if (OB_FAIL(json_out.append_fmt("\",\"mode\":\"%s\"}", get_mode_str()))) {
    LOG_WARN("failed to append mode", K(ret));
  }

  return ret;
}

int ObPyUDFExecutor::execute(int64_t batch_size,
                              const ObIArray<ObObjMeta> &arg_types,
                              const ObIArray<ObIArray<ObObj>*> &args,
                              ObIAllocator &result_allocator,
                              ObIArray<ObObj> &result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ensure_sandbox())) {
    LOG_WARN("failed to ensure sandbox", K(ret));
  } else if (OB_FAIL(ensure_udf_loaded())) {
    LOG_WARN("failed to ensure udf loaded", K(ret));
  }

  if (OB_SUCC(ret)) {
    // 1. Serialize args → Arrow IPC bytes
    sql::ObArrowMemPool mem_pool;
    mem_pool.init(MTL_ID());
    std::shared_ptr<arrow::Buffer> arrow_buf;
    ObString mode_str(get_mode_str());
    if (OB_FAIL(ob_udf_args_to_arrow(udf_info_.udf_id_, mode_str,
                                     arg_types, args, batch_size,
                                     udf_info_.result_type_.get_obj_meta(),
                                     mem_pool, arrow_buf))) {
      LOG_WARN("failed to serialize args to arrow", K(ret));
    }

    // 2. Send EXECUTE frame
    if (OB_SUCC(ret) && OB_ISNULL(arrow_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arrow buffer is null after serialization", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(arrow_buf->size() > OB_PY_MAX_FRAME_BODY_SIZE)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("arrow buffer too large to send", K(ret), K(arrow_buf->size()));
      } else if (OB_FAIL(send_frame(OB_PY_MSG_EXECUTE, arrow_buf->data(),
                             static_cast<uint32_t>(arrow_buf->size())))) {
        LOG_WARN("failed to send EXECUTE frame", K(ret));
      }
    }

    // 3. Receive response frame (with timeout)
    uint32_t resp_type = 0;
    ObSqlString resp_body;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(recv_frame(resp_type, resp_body))) {
        LOG_WARN("failed to receive response frame", K(ret));
      }
    }

    // 4. Parse response
    if (OB_SUCC(ret)) {
      if (resp_type == OB_PY_MSG_RESULT_OK) {
        if (OB_FAIL(ob_udf_result_from_arrow(
                reinterpret_cast<const uint8_t*>(resp_body.ptr()),
                resp_body.length(),
                udf_info_.result_type_.get_obj_meta(),
                mem_pool, result_allocator, result))) {
          LOG_WARN("failed to parse arrow result", K(ret));
        }
      } else if (resp_type == OB_PY_MSG_RESULT_ERR) {
        ret = OB_PYTHON_EXEC_ERROR;
        LOG_WARN("python udf execution error", K(ret), "body", resp_body.string());
        extract_and_set_user_error(resp_body);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected response type from python sandbox", K(resp_type));
      }
    }
  }
  return ret;
}

void ObPyUDFExecutor::extract_and_set_user_error(const ObSqlString &resp_body)
{
  // resp_body is JSON: {"error": "msg", "traceback": "..."}
  // Extract "error" field; if empty, extract last line of "traceback" (the exception type).
  char buf[512] = {0};
  bool found = false;

  // Try to extract "error" field
  const char *err_key = "\"error\": \"";
  const char *pos = strstr(resp_body.ptr(), err_key);
  if (OB_NOT_NULL(pos)) {
    pos += strlen(err_key);
    const char *end = strstr(pos, "\", \"traceback\"");
    if (OB_ISNULL(end)) {
      end = strstr(pos, "\"}");
    }
    if (OB_NOT_NULL(end) && end > pos) {
      int64_t len = MIN(static_cast<int64_t>(end - pos), static_cast<int64_t>(sizeof(buf) - 1));
      MEMCPY(buf, pos, len);
      buf[len] = '\0';
      found = true;
    }
  }

  // If "error" is empty, extract last non-empty line from "traceback"
  // (e.g. "AssertionError" or "ValueError: ...")
  if (!found) {
    const char *tb_key = "\"traceback\": \"";
    const char *tb_pos = strstr(resp_body.ptr(), tb_key);
    if (OB_NOT_NULL(tb_pos)) {
      tb_pos += strlen(tb_key);
      const char *tb_end = strstr(tb_pos, "\"}");
      if (OB_ISNULL(tb_end)) {
        tb_end = tb_pos + strlen(tb_pos);
      }
      // Walk backward from end to find last non-empty line
      const char *line_end = tb_end;
      // Skip trailing \\n and whitespace
      while (line_end > tb_pos) {
        if (line_end - 1 >= tb_pos && *(line_end - 1) == 'n'
            && line_end - 2 >= tb_pos && *(line_end - 2) == '\\') {
          line_end -= 2;
        } else if (*(line_end - 1) == ' ' || *(line_end - 1) == '"') {
          line_end -= 1;
        } else {
          break;
        }
      }
      // Find start of last line (after last \\n)
      const char *line_start = line_end;
      while (line_start > tb_pos) {
        if (line_start - 2 >= tb_pos && *(line_start - 2) == '\\' && *(line_start - 1) == 'n') {
          break;
        }
        line_start--;
      }
      if (line_end > line_start) {
        int64_t len = MIN(static_cast<int64_t>(line_end - line_start),
                               static_cast<int64_t>(sizeof(buf) - 1));
        MEMCPY(buf, line_start, len);
        buf[len] = '\0';
        found = true;
      }
    }
  }

  if (found) {
    LOG_USER_ERROR(OB_PYTHON_EXEC_ERROR, buf);
  } else {
    LOG_WARN_RET(OB_PYTHON_EXEC_ERROR, "failed to extract error from python response, full body logged",
                "resp_body", resp_body.ptr());
    LOG_USER_ERROR(OB_PYTHON_EXEC_ERROR, "Python UDF execution failed");
  }
}

static int recv_with_worker_check(observer::ObSandboxProcess *sp,
                                char *buf, size_t size, size_t &ret_size,
                                int total_timeout_ms)
{
  constexpr int CHECK_INTERVAL_MS = 1000;
  int ret = OB_SUCCESS;
  int remaining_ms = total_timeout_ms;
  ret_size = 0;
  common::ObWaitEventGuard wait_guard(ObWaitEventIds::UDF_PROCESS_EXECUTE_WAIT,
                                        total_timeout_ms,
                                        sp->get_pid());
  while (OB_SUCC(ret) && 0 == ret_size) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("worker status check failed while waiting for sandbox response",
               K(ret));
    } else if (sp->is_memory_limited()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("sandbox process is memory limited", "memory_usage", sp->get_memory_usage());
    } else {
      int once_ms = CHECK_INTERVAL_MS;
      if (total_timeout_ms >= 0) {
        once_ms = MIN(CHECK_INTERVAL_MS, remaining_ms);
      }
      int tmp_ret = sp->read_msg_with_timeout(buf, size, ret_size, once_ms);
      if (OB_TIMEOUT == tmp_ret) {
        if (total_timeout_ms >= 0) {
          remaining_ms -= once_ms;
          if (remaining_ms <= 0) {
            ret = OB_TIMEOUT;
          }
        }
        // otherwise continue waiting
      } else if (OB_ERR_SYS == tmp_ret) {
        ret = OB_PYTHON_EXEC_ERROR;
        char errmsg[512];
        snprintf(errmsg, sizeof(errmsg),
                 "Python UDF sandbox process (pid=%d, last_mem=%ldMB) crashed; "
                 "possible causes: out of memory, Python library internal error (e.g. PyArrow/jemalloc). "
                 "Search observer.log for pid=%d for details.",
                 sp->get_pid(), sp->get_memory_usage() / (1024 * 1024), sp->get_pid());
        LOG_USER_ERROR(OB_PYTHON_EXEC_ERROR, errmsg);
      } else {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObPyUDFExecutor::send_frame(uint32_t msg_type,
                                 const uint8_t *body, uint32_t body_len)
{
  int ret = OB_SUCCESS;
  ObPyUDFContext *udf_ctx = static_cast<ObPyUDFContext*>(exec_ctx_.get_py_udf_ctx());
  observer::ObSandboxProcess *sp = OB_NOT_NULL(udf_ctx) ? udf_ctx->get_sandbox_process() : nullptr;
  if (OB_ISNULL(sp)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sandbox process not initialized", K(ret));
  } else if (OB_UNLIKELY(body_len > OB_PY_MAX_FRAME_BODY_SIZE)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("frame body too large", K(ret), K(body_len), K(OB_PY_MAX_FRAME_BODY_SIZE));
  } else {
    uint32_t hdr[3] = {htonl(OB_PY_FRAME_MAGIC), htonl(msg_type), htonl(body_len)};
    static constexpr size_t PIPE_BUF_SIZE = 4096;
    if (body_len == 0) {
      // Header-only frame
      if (OB_FAIL(sp->send_msg(reinterpret_cast<char*>(hdr), sizeof(hdr)))) {
        LOG_WARN("failed to send frame header", K(ret));
      }
    } else if (sizeof(hdr) + body_len <= PIPE_BUF_SIZE) {
      // Small frame: merge header+body into single write for atomicity
      char merged[PIPE_BUF_SIZE];
      MEMCPY(merged, hdr, sizeof(hdr));
      MEMCPY(merged + sizeof(hdr), body, body_len);
      if (OB_FAIL(sp->send_msg(merged, sizeof(hdr) + body_len))) {
        LOG_WARN("failed to send merged frame", K(ret), K(body_len));
      }
    } else {
      // Large frame: header+body sent separately.
      // Safe because send and recv are on the same worker thread (no concurrent writes).
      if (OB_FAIL(sp->send_msg(reinterpret_cast<char*>(hdr), sizeof(hdr)))) {
        LOG_WARN("failed to send frame header", K(ret));
      } else if (OB_FAIL(sp->send_msg(reinterpret_cast<const char*>(body), body_len))) {
        LOG_WARN("failed to send frame body", K(ret), K(body_len));
      }
    }
    if (OB_FAIL(ret) && OB_ERR_SYS == ret) {
      ret = OB_PYTHON_EXEC_ERROR;
      LOG_WARN("python sandbox process exited unexpectedly during send", K(ret), KPC(sp));
      char errmsg[512];
      snprintf(errmsg, sizeof(errmsg),
               "Python UDF sandbox process (pid=%d) exited unexpectedly; "
               "Search observer.log for pid=%d for details.",
               sp->get_pid(), sp->get_pid());
      LOG_USER_ERROR(OB_PYTHON_EXEC_ERROR, errmsg);
    }
  }
  return ret;
}

int ObPyUDFExecutor::recv_frame(uint32_t &msg_type, ObSqlString &body)
{
  int ret = OB_SUCCESS;
  ObPyUDFContext *udf_ctx = static_cast<ObPyUDFContext*>(exec_ctx_.get_py_udf_ctx());
  observer::ObSandboxProcess *sp = OB_NOT_NULL(udf_ctx) ? udf_ctx->get_sandbox_process() : nullptr;
  if (OB_ISNULL(sp)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sandbox process not initialized", K(ret));
  }

  int timeout_ms = OB_PY_DEFAULT_RECV_TIMEOUT_MS;
  if (OB_SUCC(ret)) {
    int64_t timeout_remain = THIS_WORKER.get_timeout_remain();
    if (timeout_remain > 0) {
      int64_t timeout_ms_64 = timeout_remain / 1000;
      if (timeout_ms_64 > OB_PY_DEFAULT_RECV_TIMEOUT_MS) {
        timeout_ms = OB_PY_DEFAULT_RECV_TIMEOUT_MS;
      } else if (timeout_ms_64 <= 0) {
        timeout_ms = 1;
      } else {
        timeout_ms = static_cast<int>(timeout_ms_64);
      }
    }
  }

  // Read frame header
  char hdr_buf[OB_PY_FRAME_HEADER_SIZE];
  size_t received = 0;
  while (OB_SUCC(ret) && received < OB_PY_FRAME_HEADER_SIZE) {
    size_t n = 0;
    if (OB_FAIL(recv_with_worker_check(sp, hdr_buf + received,
                                    OB_PY_FRAME_HEADER_SIZE - received,
                                    n, timeout_ms))) {
      LOG_WARN("failed to read frame header", K(ret), K(received));
    } else {
      received += n;
    }
  }

  // Parse header and read body (use memcpy to avoid strict aliasing violation)
  if (OB_SUCC(ret)) {
    uint32_t raw_magic, raw_type, raw_len;
    MEMCPY(&raw_magic, hdr_buf, sizeof(uint32_t));
    MEMCPY(&raw_type, hdr_buf + 4, sizeof(uint32_t));
    MEMCPY(&raw_len, hdr_buf + 8, sizeof(uint32_t));
    uint32_t magic    = ntohl(raw_magic);
    msg_type          = ntohl(raw_type);
    uint32_t body_len = ntohl(raw_len);

    if (magic != OB_PY_FRAME_MAGIC) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid frame magic", K(ret), K(magic));
    } else if (OB_UNLIKELY(body_len > OB_PY_MAX_FRAME_BODY_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("frame body too large from sandbox", K(ret), K(body_len),
               K(OB_PY_MAX_FRAME_BODY_SIZE));
    } else if (body_len > 0) {
      if (OB_FAIL(body.reserve(body_len))) {
        LOG_WARN("failed to reserve body buffer", K(ret), K(body_len));
      } else {
        // Read body directly into ObSqlString's internal buffer
        static constexpr size_t READ_CHUNK_SIZE = 8192;
        char read_buf[READ_CHUNK_SIZE];
        size_t total_read = 0;
        while (OB_SUCC(ret) && total_read < body_len) {
          size_t to_read = MIN(static_cast<size_t>(sizeof(read_buf)),
                                    static_cast<size_t>(body_len - total_read));
          size_t n = 0;
          if (OB_FAIL(recv_with_worker_check(sp, read_buf, to_read, n, timeout_ms))) {
            LOG_WARN("failed to read frame body", K(ret), K(total_read), K(body_len));
          } else if (OB_FAIL(body.append(read_buf, n))) {
            LOG_WARN("failed to append body chunk", K(ret));
          } else {
            total_read += n;
          }
        }
      }
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
