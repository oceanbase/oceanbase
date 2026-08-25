/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/v2/loader/ob_ext_plugin_loader.h"

#include <dlfcn.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_ld_library_path_util.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{
namespace
{

using oceanbase::common::ObLdLibraryPathUtil;
using oceanbase::common::ObSqlString;

constexpr int kDlopenFlags = RTLD_NOW | RTLD_GLOBAL;

// The contract header hardcodes OB errno values (plugins compile without
// ob_errno.h); pin them here so an OB-side errno change is a compile error at
// this one spot instead of a silent cross-boundary mismatch.
static_assert(OB_EXT_SUCCESS == OB_SUCCESS, "contract errno drift");
static_assert(OB_EXT_INVALID_ARGUMENT == OB_INVALID_ARGUMENT, "contract errno drift");
static_assert(OB_EXT_NOT_SUPPORTED == OB_NOT_SUPPORTED, "contract errno drift");
static_assert(OB_EXT_IO_ERROR == OB_IO_ERROR, "contract errno drift");
static_assert(OB_EXT_ALLOCATE_MEMORY_FAILED == OB_ALLOCATE_MEMORY_FAILED, "contract errno drift");
static_assert(OB_EXT_ERR_UNEXPECTED == OB_ERR_UNEXPECTED, "contract errno drift");
static_assert(OB_EXT_ENTRY_NOT_EXIST == OB_ENTRY_NOT_EXIST, "contract errno drift");
static_assert(OB_EXT_FILE_NOT_EXIST == OB_FILE_NOT_EXIST, "contract errno drift");
static_assert(OB_EXT_DESERIALIZE_ERROR == OB_DESERIALIZE_ERROR, "contract errno drift");
static_assert(OB_EXT_DIR_NOT_EXIST == OB_DIR_NOT_EXIST, "contract errno drift");
static_assert(OB_EXT_INVALID_DATA == OB_INVALID_DATA, "contract errno drift");
static_assert(OB_EXT_OLD_SCHEMA_VERSION == OB_OLD_SCHEMA_VERSION, "contract errno drift");

} // namespace

int load_ext_plugin(const char *so_path, ObExtLoadedPlugin &out, ObSqlString &err_msg,
                    ObExtPluginLoadStatus *status_out)
{
  int ret = OB_SUCCESS;
  void *h = dlopen(so_path, kDlopenFlags);
  if (OB_ISNULL(h)) {
    ret = OB_ERR_UNEXPECTED;
    const char *dlerr = dlerror();
    err_msg.append_fmt("dlopen '%s' failed: %s", so_path, OB_NOT_NULL(dlerr) ? dlerr : "(null)");
    if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_DLOPEN_FAILED; }
    LOG_WARN("ext plugin dlopen failed", K(so_path), "dlerror", dlerr, K(ret));
  } else {
    ob_ext_table_plugin_get_api_fn get_api =
        reinterpret_cast<ob_ext_table_plugin_get_api_fn>(
            dlsym(h, "ob_ext_table_plugin_get_api"));
    if (OB_ISNULL(get_api)) {
      ret = OB_ENTRY_NOT_EXIST;
      const char *dlerr = dlerror();
      err_msg.append_fmt("symbol ob_ext_table_plugin_get_api not found in '%s': %s",
                         so_path, OB_NOT_NULL(dlerr) ? dlerr : "(null)");
      if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_NO_SYMBOL; }
      LOG_WARN("ext plugin dlsym ob_ext_table_plugin_get_api failed",
               K(so_path), "dlerror", dlerr, K(ret));
    } else {
      const ObExtTablePluginApi *api = get_api(OB_EXT_TABLE_PLUGIN_ABI_VERSION);
      if (OB_ISNULL(api)) {
        ret = OB_VERSION_NOT_MATCH;
        err_msg.append_fmt("plugin '%s' rejected ABI version %u", so_path,
                           OB_EXT_TABLE_PLUGIN_ABI_VERSION);
        if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_ABI_MISMATCH; }
        LOG_WARN("ext plugin rejected ABI version", K(so_path),
                 K(OB_EXT_TABLE_PLUGIN_ABI_VERSION), K(ret));
      } else if (OB_ISNULL(api->format_name) || OB_ISNULL(api->format_name())) {
        ret = OB_ERR_UNEXPECTED;
        err_msg.append_fmt("plugin '%s' missing format_name()", so_path);
        if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_NO_FORMAT_NAME; }
        LOG_WARN("ext plugin missing format_name", K(so_path), K(ret));
      } else {
        out.dl_handle_ = h;
        out.api_ = api;
        out.format_name_ = api->format_name();
        // Record the canonical absolute path actually dlopen'd — the diagnostic row
        // in __all_virtual_plugin_info shows it in the library column. realpath()
        // canonicalizes LD_LIBRARY_PATH-relative results like './lib/lib_ob_x.so';
        // on (unexpected) failure fall back to the raw so_path.
        char *resolved = realpath(so_path, nullptr);
        const char *path_to_record = OB_NOT_NULL(resolved) ? resolved : so_path;
        const int64_t path_len = std::min<int64_t>(static_cast<int64_t>(strlen(path_to_record)),
                                                   static_cast<int64_t>(sizeof(out.path_)) - 1);
        memcpy(out.path_, path_to_record, path_len);
        out.path_[path_len] = '\0';
        if (OB_NOT_NULL(resolved)) {
          free(resolved);
        }
        if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_SUCCESS; }
        LOG_INFO("ext plugin loaded", K(so_path), "format", out.format_name_);
      }
    }
    if (OB_FAIL(ret)) {
      dlclose(h);
    }
  }
  return ret;
}

namespace
{

// Build SONAME `lib_ob_<lower(plugin_name)>.so`. Lowercase because SQL idents
// are case-insensitive and the on-disk .so name is canonical-lowercase.
static int build_soname(const char *plugin_name, ObSqlString &soname)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(soname.append("lib_ob_"))) {
  } else {
    for (const char *p = plugin_name; OB_SUCC(ret) && *p; ++p) {
      char c = static_cast<char>(tolower(static_cast<unsigned char>(*p)));
      if (OB_FAIL(soname.append(&c, 1))) {
        // append with len 1
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(soname.append(".so"))) {
      LOG_WARN("append plugin soname suffix failed", K(ret));
    }
  }
  return ret;
}

} // namespace

int load_ext_plugin_by_name(const char *plugin_name,
                            const char *path_override,
                            ObExtLoadedPlugin &out,
                            ObSqlString &err_msg,
                            ObExtPluginLoadStatus *status_out)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plugin_name) || '\0' == plugin_name[0]) {
    ret = OB_INVALID_ARGUMENT;
    err_msg.append_fmt("empty plugin name");
    if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_INVALID_ARG; }
    LOG_WARN("load ext plugin invalid name", K(ret), KP(plugin_name));
  } else if (OB_NOT_NULL(path_override) && '\0' != path_override[0]) {
    ret = load_ext_plugin(path_override, out, err_msg, status_out);
  } else {
    // Ensure `_ob_additional_lib_path` into LD_LIBRARY_PATH first (':' -separated
    // multi-path OK), then search SONAME under the unified LD_LIBRARY_PATH.
    ObSqlString soname;
    ObSqlString search_paths;
    ObSqlString lib_path;
    const char *extra_paths = GCONF._ob_additional_lib_path.get_value();
    if (OB_FAIL(build_soname(plugin_name, soname))) {
      LOG_WARN("build plugin soname failed", K(ret), KCSTRING(plugin_name));
    } else if (OB_NOT_NULL(extra_paths) && '\0' != extra_paths[0]
               && OB_FAIL(ObLdLibraryPathUtil::ensure_dir_in_ld_library_path(extra_paths))) {
      err_msg.append_fmt("ensure _ob_additional_lib_path in LD_LIBRARY_PATH for %s failed",
                         soname.ptr());
      if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_NOT_FOUND; }
      LOG_WARN("ensure _ob_additional_lib_path in LD_LIBRARY_PATH failed",
               K(ret), KCSTRING(plugin_name), KCSTRING(extra_paths));
    } else if (OB_FAIL(ObLdLibraryPathUtil::build_ld_library_search_paths(search_paths))) {
      err_msg.append_fmt("neither LD_LIBRARY_PATH nor _ob_additional_lib_path is set, cannot find %s",
                         soname.ptr());
      if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_NOT_FOUND; }
      LOG_WARN("LD_LIBRARY_PATH not set after ensure, ext plugin unavailable",
               K(ret), KCSTRING(plugin_name), "soname", soname.ptr());
    } else if (OB_FAIL(ObLdLibraryPathUtil::get_lib_path(soname.ptr(), search_paths, lib_path))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        err_msg.append_fmt("%s not found under LD_LIBRARY_PATH", soname.ptr());
        if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_NOT_FOUND; }
        LOG_WARN("ext plugin soname not found on search path",
                 KCSTRING(plugin_name), "soname", soname.ptr(), K(ret));
      } else {
        err_msg.append_fmt("resolve %s path failed", soname.ptr());
        if (OB_NOT_NULL(status_out)) { *status_out = EXT_PLUGIN_DLOPEN_FAILED; }
        LOG_WARN("resolve ext plugin lib path failed",
                 KCSTRING(plugin_name), "soname", soname.ptr(), K(ret));
      }
    } else if (OB_FAIL(load_ext_plugin(lib_path.ptr(), out, err_msg, status_out))) {
      LOG_WARN("load ext plugin failed", K(ret), KCSTRING(plugin_name),
               "lib_path", lib_path.ptr());
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#undef USING_LOG_PREFIX
