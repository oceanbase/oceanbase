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

#define USING_LOG_PREFIX SHARE

#include "lib/utility/utility.h"
#include "plugin/sys/ob_plugin_dl_handle.h"
#include "plugin/sys/ob_plugin_utils.h"

#include <dlfcn.h>
#include <limits.h>

namespace oceanbase {
using namespace common;
namespace plugin {

ObPluginDlHandle::~ObPluginDlHandle()
{
  destroy();
}

int ObPluginDlHandle::init(const ObString &dl_dir, const ObString &dl_name)
{
  int ret = OB_SUCCESS;
  ObSqlString path_string;
  path_string.set_label(OB_PLUGIN_MEMORY_LABEL);

  void *dl_handle = nullptr;

  // we should use absolute path to stay safe
  char cwd[PATH_MAX + 1] = {0};
  if (!dl_dir.prefix_match("/") && OB_ISNULL(getcwd(cwd, PATH_MAX + 1))) {
    ret = OB_IO_ERROR;
    LOG_WARN("failed to get current work directory", KCSTRING(strerror(errno)), K(ret));
  }

  if (OB_NOT_NULL(dl_handle_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(path_string.assign(cwd))
             || OB_FAIL(path_string.append("/"))
             || OB_FAIL(path_string.append(dl_dir))
             || OB_FAIL(path_string.append("/"))
             || OB_FAIL(path_string.append(dl_name))) {
    LOG_WARN("failed to allocate string", K(dl_dir), K(dl_name), K(ret));
  } else if (OB_FAIL(dl_name_.assign(dl_name))) {
    LOG_WARN("failed to assign dl_name", K(dl_name), K(ret));
  } else if (OB_ISNULL(dl_handle_ = dlopen(path_string.ptr(), RTLD_GLOBAL | RTLD_NOW))) {
    ret = OB_PLUGIN_DLOPEN_FAILED;
    LOG_WARN("failed to open dl", K(path_string), K(ret), KCSTRING(dlerror()));
  }
  return ret;
}

void ObPluginDlHandle::destroy()
{
  dl_name_.reset();
  dl_handle_ = nullptr;
}

int ObPluginDlHandle::read_value(const char *symbol_name, void *ptr, int64_t size)
{
    int ret = OB_SUCCESS;
    void *address = nullptr;
    if (OB_FAIL(read_symbol(symbol_name, address))) {
      LOG_WARN("failed to find symbol from dl", K(ret));
    } else if (OB_ISNULL(address)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no such symbol or get null value");
    } else {
      MEMCPY(ptr, address, size);
    }
    return ret;
}

int ObPluginDlHandle::read_symbol(const char *symbol_name, void *&value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL((dl_handle_))) {
    ret = OB_NOT_INIT;
  } else if (FALSE_IT(dlerror())) { // clear last error
  } else if (OB_ISNULL(value = dlsym(dl_handle_, symbol_name))) {
    const char *error = dlerror();
    if (OB_NOT_NULL(error)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to find symbol", K_(dl_name), KCSTRING(symbol_name), KCSTRING(error));
    }
  }
  return ret;
}

} // namespace plugin
} // namespace oceanbase
