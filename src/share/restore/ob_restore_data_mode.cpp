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
#include "share/restore/ob_restore_data_mode.h"

using namespace oceanbase;
using namespace share;

OB_SERIALIZE_MEMBER(ObRestoreDataMode, mode_);

static const char *OB_RESTORE_DATA_MODE_STR[] = {"NORMAL", "REMOTE", "UNKNOWN"};

ObRestoreDataMode &ObRestoreDataMode::operator=(const ObRestoreDataMode &restore_data_mode)
{
  if (this != &restore_data_mode) {
    mode_ = restore_data_mode.mode_;
  }
  return *this;
}

ObRestoreDataMode &ObRestoreDataMode::operator=(const Mode &mode)
{
  mode_ = mode;
  return *this;
}

const char* ObRestoreDataMode::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(OB_RESTORE_DATA_MODE_STR) == RESTORE_DATA_MODE_MAX + 1, "array size mismatch");
  const char *str = OB_RESTORE_DATA_MODE_STR[RESTORE_DATA_MODE_MAX];
  if (OB_UNLIKELY(mode_ >= Mode::RESTORE_DATA_MODE_MAX
                  || mode_ < Mode::NORMAL)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown restore data mode", K_(mode));
  } else {
    str = OB_RESTORE_DATA_MODE_STR[mode_];
}
  return str;
}

ObRestoreDataMode::ObRestoreDataMode(const ObString &str)
{
  mode_ = Mode::RESTORE_DATA_MODE_MAX;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(OB_RESTORE_DATA_MODE_STR); i++) {
      if (0 == str.case_compare(OB_RESTORE_DATA_MODE_STR[i])) {
        mode_ = i;
        break;
      }
    }
  }

  if (Mode::RESTORE_DATA_MODE_MAX == mode_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid restore data mode", K_(mode), K(str));
  }
}