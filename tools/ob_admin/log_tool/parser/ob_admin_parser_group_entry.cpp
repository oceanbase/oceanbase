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

#define USING_LOG_PREFIX CLOG
#define USING_LOG_PREFIX CLOG
#include "ob_admin_parser_group_entry.h"
#include "lib/ob_errno.h"
#include "logservice/palf/log_entry.h"
namespace oceanbase
{
using namespace palf;
namespace tools
{
ObAdminParserGroupEntry::ObAdminParserGroupEntry(const char *buf, const int64_t buf_len,
                                                 share::ObAdminMutatorStringArg &str_arg)
  : buf_(buf),
    curr_pos_(0),
    end_pos_(buf_len)
{
  str_arg_ = str_arg;
}

int ObAdminParserGroupEntry::get_next_log_entry(palf::LogEntry &log_entry)
{
  int ret = OB_SUCCESS;
  if (curr_pos_ >= end_pos_) {
    ret = OB_ITER_END;
    LOG_TRACE("parse one LogGroupEntry finished");
  } else if (OB_FAIL(do_parse_one_log_entry_(log_entry))) {
    LOG_WARN("parse one LogEntry failed", K(ret));
  } else {
    curr_pos_ += log_entry.get_serialize_size();

    LOG_TRACE("parse one LogEntry success", K(log_entry));
  }
  return ret;
}

int ObAdminParserGroupEntry::do_parse_one_log_entry_(palf::LogEntry &log_entry)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(log_entry.deserialize(buf_+curr_pos_, end_pos_- curr_pos_, pos))) {
    LOG_WARN("LogEntry deserialize failed", K(ret), K(curr_pos_), K(pos), K(end_pos_));
  } else {
    ob_assert(pos <= end_pos_);
    LOG_TRACE("do_parse_one_log_entry_ success", K(log_entry));
  }
  return ret;
}

}
}
