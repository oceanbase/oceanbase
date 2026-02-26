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

#ifndef OCENABASE_SHARE_OB_RESTORE_PROGRESS_DISPLAY_MODE_H
#define OCENABASE_SHARE_OB_RESTORE_PROGRESS_DISPLAY_MODE_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
struct ObRestoreProgressDisplayMode final
{
  OB_UNIS_VERSION(1);
public:
  enum Mode
  {
    TABLET_CNT = 0,
    BYTES = 1,
    MAX
  };

public:
  ObRestoreProgressDisplayMode() : mode_(Mode::TABLET_CNT) {}
  ~ObRestoreProgressDisplayMode() = default;
  explicit ObRestoreProgressDisplayMode(const Mode &mode) : mode_(mode) {}
  explicit ObRestoreProgressDisplayMode(const ObString &str);
  ObRestoreProgressDisplayMode &operator=(const ObRestoreProgressDisplayMode &mode);
  ObRestoreProgressDisplayMode &operator=(const Mode &mode);
  constexpr bool is_valid() const { return TABLET_CNT <= mode_ && mode_ < MAX; }
  bool is_tablet_cnt() const { return Mode::TABLET_CNT == mode_; }
  bool is_bytes() const { return Mode::BYTES == mode_; }
  const char *to_str() const;
  TO_STRING_KV("restore progress display mode", to_str());

private:
  int64_t mode_;
};
static const ObRestoreProgressDisplayMode TABLET_CNT_DISPLAY_MODE(ObRestoreProgressDisplayMode::Mode::TABLET_CNT);
static const ObRestoreProgressDisplayMode BYTES_DISPLAY_MODE(ObRestoreProgressDisplayMode::Mode::BYTES);
} //share
} //oceanbase

#endif