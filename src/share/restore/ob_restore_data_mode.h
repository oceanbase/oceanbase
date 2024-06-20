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

#ifndef OCENABASE_SHARE_OB_RESTORE_DATA_MODE_H
#define OCENABASE_SHARE_OB_RESTORE_DATA_MODE_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
class ObRestoreDataMode final
{
  OB_UNIS_VERSION(1);
public:
  enum Mode
  {
    // tenant restoring from whole data, default retore mode
    NORMAL = 0,
    // just restore clog, minor and major macro blocks are in remote reference state
    REMOTE = 1,
    RESTORE_DATA_MODE_MAX
  };

public:
  ObRestoreDataMode() : mode_(Mode::NORMAL) {}
  ~ObRestoreDataMode() = default;
  explicit ObRestoreDataMode(const Mode &mode) : mode_(mode) {}
  explicit ObRestoreDataMode(const ObString &str);
  ObRestoreDataMode &operator=(const ObRestoreDataMode &restore_mode);
  ObRestoreDataMode &operator=(const Mode &mode);
  constexpr bool is_valid() const { return mode_ >= Mode::NORMAL && mode_ < Mode::RESTORE_DATA_MODE_MAX;}
  bool is_remote_mode() const { return Mode::REMOTE == mode_; }
  bool is_normal_mode() const { return Mode::NORMAL == mode_; }
  bool is_same_mode(const ObRestoreDataMode &other) const { return mode_ == other.mode_; }
  void reset() { mode_ = Mode::NORMAL; }
  const char *to_str() const;
  TO_STRING_KV("restore data mode", to_str());

public:
  int64_t mode_;
};

static const ObRestoreDataMode NORMAL_RESTORE_DATA_MODE(ObRestoreDataMode::Mode::NORMAL);
static const ObRestoreDataMode REMOTE_RESTORE_DATA_MODE(ObRestoreDataMode::Mode::REMOTE);

} //share
} //oceanbase

#endif