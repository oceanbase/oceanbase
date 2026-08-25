/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "lib/utility/ob_ld_library_path_util.h"

#include <cerrno>
#include <cstdlib>
#include <dirent.h>
#include <cstring>

#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace common
{

bool ObLdLibraryPathUtil::search_dir_file(const char *dir, const char *file_name)
{
  bool found = false;
  DIR *dirp = nullptr;
  if (OB_NOT_NULL(dir) && OB_NOT_NULL(file_name) && OB_NOT_NULL(dirp = opendir(dir))) {
    dirent *dp = nullptr;
    while (!found && OB_NOT_NULL(dp = readdir(dirp))) {
      if (DT_UNKNOWN == dp->d_type || DT_LNK == dp->d_type || DT_REG == dp->d_type) {
        found = (0 == strcasecmp(file_name, dp->d_name));
      }
    }
    closedir(dirp);
  }
  return found;
}

int ObLdLibraryPathUtil::build_ld_library_search_paths(ObSqlString &paths)
{
  int ret = OB_SUCCESS;
  const char *ld_path = std::getenv("LD_LIBRARY_PATH");
  if (OB_ISNULL(ld_path) || ld_path[0] == '\0') {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("LD_LIBRARY_PATH is not set", K(ret));
  } else if (OB_FAIL(paths.append_fmt("%s:", ld_path))) {
    LOG_WARN("failed to append LD_LIBRARY_PATH to search list", K(ret));
  }
  return ret;
}

int ObLdLibraryPathUtil::get_lib_path(const char *lib_name, const ObSqlString &search_paths,
                                      ObSqlString &path)
{
  int ret = OB_SUCCESS;
  bool found = false;
  LOG_INFO("lib search paths", KCSTRING(lib_name), K(search_paths.string()));
  ObString remaining(search_paths.string());
  while (OB_SUCC(ret) && !found && !remaining.empty()) {
    ObString dir = remaining.split_on(':');
    if (dir.empty() && OB_ISNULL(remaining.find(':'))) {
      dir = remaining;
      remaining.reset();
    }
    while (!dir.empty() && ' ' == *dir.ptr()) {
      dir.assign_ptr(dir.ptr() + 1, dir.length() - 1);
    }
    if (!dir.empty()) {
      ObSqlString dir_str;
      if (OB_FAIL(dir_str.append(dir))) { // C_str
        LOG_WARN("failed to copy dir to string", K(ret), K(dir));
      } else {
        found = search_dir_file(dir_str.ptr(), lib_name);
        LOG_INFO("searched dir for lib", K(dir), KCSTRING(lib_name), K(found));
      }

      if (OB_SUCC(ret) && found) {
        if (OB_FAIL(path.append(dir))) {
          LOG_WARN("failed to build lib path", K(ret), K(dir));
        } else if (OB_FAIL(path.append_fmt("/%s", lib_name))) {
          LOG_WARN("failed to append lib name to path", K(ret), KCSTRING(lib_name));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to resolve lib path", K(ret), KCSTRING(lib_name));
  } else if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("lib not found in any search path", K(ret), KCSTRING(lib_name), K(search_paths.string()));
  } else {
    LOG_INFO("resolved lib path", KCSTRING(lib_name), K(path.string()));
  }
  return ret;
}

bool ObLdLibraryPathUtil::dir_in_ld_library_path(const char *dir)
{
  bool found = false;
  const char *ld_path = std::getenv("LD_LIBRARY_PATH");
  if (OB_NOT_NULL(dir) && '\0' != dir[0]
      && OB_NOT_NULL(ld_path) && '\0' != ld_path[0]) {
    const size_t dir_len = strlen(dir);
    ObSqlString copy;
    if (OB_SUCCESS != copy.append(ld_path)) {
      // alloc failure: report not-found; caller falls back to appending
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "copy LD_LIBRARY_PATH failed");
    } else {
      char *cursor = copy.ptr();
      while (!found && OB_NOT_NULL(cursor) && '\0' != *cursor) {
        char *colon = strchr(cursor, ':');
        if (OB_NOT_NULL(colon)) {
          *colon = '\0';
        }
        found = (dir_len == strlen(cursor) && 0 == strcmp(cursor, dir));
        cursor = OB_NOT_NULL(colon) ? colon + 1 : nullptr;
      }
    }
  }
  return found;
}

static int ensure_one_dir_in_ld_library_path(const char *dir)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir) || '\0' == dir[0]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty lib dir", K(ret));
  } else if (ObLdLibraryPathUtil::dir_in_ld_library_path(dir)) {
    LOG_DEBUG("dir already in LD_LIBRARY_PATH, skip", KCSTRING(dir));
  } else {
    const char *ld_path = std::getenv("LD_LIBRARY_PATH");
    ObSqlString new_path;
    if (OB_NOT_NULL(ld_path) && '\0' != ld_path[0]
        && OB_FAIL(new_path.append_fmt("%s:", ld_path))) {
      LOG_WARN("failed to build new LD_LIBRARY_PATH", K(ret), KCSTRING(dir));
    } else if (OB_FAIL(new_path.append(dir))) {
      LOG_WARN("failed to append dir to new LD_LIBRARY_PATH", K(ret), KCSTRING(dir));
    } else if (0 != setenv("LD_LIBRARY_PATH", new_path.ptr(), 1 /*overwrite*/)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("setenv LD_LIBRARY_PATH failed", K(ret), KCSTRING(dir), K(errno));
    } else {
      LOG_INFO("LD_LIBRARY_PATH extended", KCSTRING(dir), "ld_library_path", new_path.ptr());
    }
  }
  return ret;
}

int ObLdLibraryPathUtil::ensure_dir_in_ld_library_path(const char *dirs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dirs) || '\0' == dirs[0]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty lib dirs", K(ret));
  } else {
    // `_ob_additional_lib_path` may carry several ':'-separated directories; ensure
    // each component independently so idempotency stays per-directory.
    ObSqlString copy;
    bool any = false;
    if (OB_FAIL(copy.append(dirs))) {
      LOG_WARN("copy lib dirs failed", K(ret), KCSTRING(dirs));
    } else {
      char *cursor = copy.ptr();
      while (OB_SUCC(ret) && OB_NOT_NULL(cursor)) {
        char *colon = strchr(cursor, ':');
        if (OB_NOT_NULL(colon)) {
          *colon = '\0';
        }
        if ('\0' != *cursor) {
          any = true;
          if (OB_FAIL(ensure_one_dir_in_ld_library_path(cursor))) {
            LOG_WARN("ensure one lib dir in LD_LIBRARY_PATH failed",
                     K(ret), KCSTRING(cursor), KCSTRING(dirs));
          }
        }
        cursor = OB_NOT_NULL(colon) ? colon + 1 : nullptr;
      }
      if (OB_SUCC(ret) && !any) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("lib dirs has no non-empty component", K(ret), KCSTRING(dirs));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#undef USING_LOG_PREFIX
