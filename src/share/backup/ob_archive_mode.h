/**
 * Copyright (c) 20222 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_MODE_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_MODE_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

struct ObArchiveMode final
{
  OB_UNIS_VERSION(1);

public:
  enum Mode
  {
    INVALID = 0,
    ARCHIVELOG = 1,
    NOARCHIVELOG = 2,
    MAX_MODE = 3,
  };

  ObArchiveMode() : mode_(Mode::INVALID) {}
  explicit ObArchiveMode(const Mode &mode) : mode_(mode) {}
  explicit ObArchiveMode(const ObString &str);
  ObArchiveMode(const ObArchiveMode &other) : mode_(other.mode_) {}
  bool operator==(const ObArchiveMode &other) const
  {
    return mode_ == other.mode_;
  }

  bool operator!=(const ObArchiveMode &other) const
  {
    return !(*this == other);
  }

  void operator=(const ObArchiveMode &other)
  {
    mode_ = other.mode_;
  }

  static ObArchiveMode archivelog();
  static ObArchiveMode noarchivelog();

  void reset() { mode_ = Mode::INVALID; }
  void set_archivelog() { mode_ = Mode::ARCHIVELOG; }
  void set_noarchivelog() { mode_ = Mode::NOARCHIVELOG; }
  bool is_archivelog() const { return Mode::ARCHIVELOG == mode_; }
  bool is_noarchivelog() const { return Mode::NOARCHIVELOG == mode_; }

  bool is_valid() const;
  const char *to_str() const;

  TO_STRING_KV("archive mode", to_str());

private:
  int64_t mode_;
};

static const ObArchiveMode INVALID_ARCHIVE_MODE(ObArchiveMode::Mode::INVALID);
static const ObArchiveMode ARCHIVE_MODE(ObArchiveMode::Mode::ARCHIVELOG);
static const ObArchiveMode NOARCHIVE_MODE(ObArchiveMode::Mode::NOARCHIVELOG);

}
}

#endif