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
#include "share/backup/ob_archive_mode.h"

namespace oceanbase
{
namespace share
{

/**
 * ------------------------------ObArchiveMode---------------------
 */
OB_SERIALIZE_MEMBER(ObArchiveMode, mode_);

static const char *OB_ARCHIVE_MODE_STR[] = {"INVALID", "ARCHIVELOG", "NOARCHIVELOG"};

bool ObArchiveMode::is_valid() const
{
  return Mode::INVALID < mode_ && Mode::MAX_MODE > mode_;
}

const char* ObArchiveMode::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(OB_ARCHIVE_MODE_STR) == MAX_MODE, "array size mismatch");
  const char *str = OB_ARCHIVE_MODE_STR[0];
  if (OB_UNLIKELY(mode_ >= ARRAYSIZEOF(OB_ARCHIVE_MODE_STR)
                  || mode_ < Mode::INVALID)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown archive mode", K_(mode));
  } else {
    str = OB_ARCHIVE_MODE_STR[mode_];
  }
  return str;
}

ObArchiveMode::ObArchiveMode(const ObString &str)
{
  mode_ = Mode::INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(OB_ARCHIVE_MODE_STR); i++) {
      if (0 == str.case_compare(OB_ARCHIVE_MODE_STR[i])) {
        mode_ = i;
        break;
      }
    }
  }

  if (Mode::INVALID == mode_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid archive mode", K_(mode), K(str));
  }
}

}
}
