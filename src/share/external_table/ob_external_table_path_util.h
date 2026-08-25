/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_external_table_path_util.h
/// \brief Generic file-URI / path helpers for external-table plugins.
///
/// These are pure std::string utilities (normalize a file URI, join/parent paths,
/// build an ObString alias) with no format-specific knowledge, so every
/// external-table plugin host provider shares one copy.

#ifndef OB_EXTERNAL_TABLE_PATH_UTIL_H
#define OB_EXTERNAL_TABLE_PATH_UTIL_H

#include <string>
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

/// Build an ObString that aliases str's buffer (no copy). The returned ObString
/// must not outlive str, and str must not be mutated/reallocated while in use.
inline const common::ObString make_ob_stringview(const std::string &str)
{
  common::ObString ob_str;
  ob_str.assign_ptr(const_cast<char *>(str.c_str()), static_cast<int32_t>(str.length()));
  return ob_str;
}

/// True if path is a POSIX-style absolute path (leading '/').
inline bool is_absolute_path(const std::string &path)
{
  return !path.empty() && '/' == path[0];
}

/// Normalizes malformed local file URIs like file:/tmp/a to file:///tmp/a so they
/// are treated as full file URIs instead of relative paths.
inline std::string normalize_file_uri(const std::string &path)
{
  static const char malformed_prefix[] = "file:/";
  static const char normalized_prefix[] = "file://";
  const std::string::size_type malformed_len = sizeof(malformed_prefix) - 1;
  const std::string::size_type normalized_len = sizeof(normalized_prefix) - 1;
  std::string normalized_path(path);

  if (path.size() >= malformed_len
      && 0 == path.compare(0, malformed_len, malformed_prefix)
      && !(path.size() >= normalized_len
           && 0 == path.compare(0, normalized_len, normalized_prefix))) {
    normalized_path = std::string(normalized_prefix) + path.substr(sizeof("file:") - 1);
  }
  return normalized_path;
}

/// Length of the non-truncatable URI/filesystem root prefix of `path`.
inline std::string::size_type path_root_length(const std::string &path)
{
  std::string::size_type root_len = 0;
  const std::string::size_type scheme_pos = path.find("://");
  if (std::string::npos == scheme_pos) {
    root_len = is_absolute_path(path) ? 1 : 0;
  } else {
    const std::string::size_type authority_pos = scheme_pos + 3;
    if (authority_pos >= path.size()) {
      root_len = path.size();
    } else {
      const std::string::size_type slash_pos = path.find('/', authority_pos);
      if (std::string::npos == slash_pos) {
        root_len = path.size();
      } else if (slash_pos == authority_pos) {
        root_len = authority_pos;
        while (root_len < path.size() && '/' == path[root_len]) {
          ++root_len;
        }
      } else {
        root_len = slash_pos;
      }
    }
  }
  return root_len;
}

/// Concatenate two path segments with a single '/' if needed.
inline std::string join_path(const std::string &lhs, const std::string &rhs)
{
  if (lhs.empty()) {
    return rhs;
  }
  if (rhs.empty()) {
    return lhs;
  }
  if ('/' == lhs[lhs.size() - 1]) {
    return lhs + rhs;
  }
  return lhs + "/" + rhs;
}

/// Directory portion of `path`: strips the last non-root segment.
inline std::string parent_path(const std::string &path)
{
  if (path.empty()) {
    return "";
  }
  const std::string::size_type root_len = path_root_length(path);
  std::string::size_type end = path.size();
  while (end > root_len && '/' == path[end - 1]) {
    --end;
  }
  if (end <= root_len) {
    return 0 == root_len ? "" : path.substr(0, root_len);
  }
  const std::string::size_type pos = path.rfind('/', end - 1);
  if (std::string::npos == pos) {
    return "";
  } else if (0 == pos) {
    return "/";
  } else if (pos < root_len) {
    return 0 == root_len ? "" : path.substr(0, root_len);
  } else {
    return path.substr(0, pos);
  }
}

} // namespace share
} // namespace oceanbase

#endif // OB_EXTERNAL_TABLE_PATH_UTIL_H
