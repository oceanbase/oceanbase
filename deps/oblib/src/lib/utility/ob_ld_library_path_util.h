/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ld_library_path_util.h
/// \brief Resolve shared-library paths under LD_LIBRARY_PATH (libhdfs.so / libjvm.so style).

#ifndef OCEANBASE_LIB_UTILITY_OB_LD_LIBRARY_PATH_UTIL_H_
#define OCEANBASE_LIB_UTILITY_OB_LD_LIBRARY_PATH_UTIL_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{

class ObSqlString;

class ObLdLibraryPathUtil
{
public:
  /// Returns true if `file_name` exists as a regular file/symlink entry in `dir`.
  static bool search_dir_file(const char *dir, const char *file_name);

  /// Appends `LD_LIBRARY_PATH` plus trailing ':' to `paths`. No filesystem I/O.
  static int build_ld_library_search_paths(ObSqlString &paths);

  /// Scans colon-delimited `search_paths` for `lib_name`; writes `dir/lib_name` into `path`.
  /// Returns OB_ENTRY_NOT_EXIST when not found.
  static int get_lib_path(const char *lib_name, const ObSqlString &search_paths, ObSqlString &path);

  /// Returns true when `dir` is already one of the ':'-separated components of
  /// the current LD_LIBRARY_PATH environment variable (exact match). `dir` must
  /// be a single path component, not a ':'-joined list.
  static bool dir_in_ld_library_path(const char *dir);

  /// Idempotently expose directories to dlopen dependency resolution.
  /// `dirs` may be a single directory or a ':'-separated list (same form as
  /// `_ob_additional_lib_path` / LD_LIBRARY_PATH). Each non-empty component is
  /// appended via setenv only when absent from LD_LIBRARY_PATH. Returns
  /// OB_INVALID_ARGUMENT when `dirs` is empty / all-empty segments,
  /// OB_ERR_UNEXPECTED when setenv fails.
  static int ensure_dir_in_ld_library_path(const char *dirs);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_UTILITY_OB_LD_LIBRARY_PATH_UTIL_H_
