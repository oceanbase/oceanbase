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

#include "lib/oblog/ob_warning_buffer.h"

namespace oceanbase
{
namespace common
{
bool ObWarningBuffer::is_log_on_ = false;
_RLOCAL(ObWarningBuffer *, g_warning_buffer);

OB_SERIALIZE_MEMBER(ObWarningBuffer::WarningItem,
                    msg_,
                    code_,
                    log_level_,
                    line_no_,
                    column_no_);

ObWarningBuffer &ObWarningBuffer::operator= (const ObWarningBuffer &other)
{
  if (this != &other) {
    reset();
    int ret = item_.assign(other.item_);
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    } else {
      err_ = other.err_;
      append_idx_ = other.append_idx_;
      total_warning_count_ = other.total_warning_count_;
    }
  }
  return *this;
}

ObWarningBuffer::WarningItem &ObWarningBuffer::WarningItem::operator= (const WarningItem &other)
{
  if (this != &other) {
    STRCPY(msg_, other.msg_);
    timestamp_ = other.timestamp_;
    log_level_ = other.log_level_;
    line_no_ = other.line_no_;
    column_no_ = other.column_no_;
    code_ = other.code_;
    STRCPY(sql_state_, other.sql_state_);
  }
  return *this;
}

}
}
