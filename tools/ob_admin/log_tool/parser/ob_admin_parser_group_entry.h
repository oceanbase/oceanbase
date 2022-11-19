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

#ifndef OB_ADMIN_PARSER_GROUP_ENTRY_H_
#define OB_ADMIN_PARSER_GROUP_ENTRY_H_
#include "logservice/palf/log_group_entry.h"
#include "share/ob_admin_dump_helper.h"
namespace oceanbase
{
namespace palf
{
class LogEntry;
}
namespace tools
{
class ObAdminParserGroupEntry
{
public:
  ObAdminParserGroupEntry(const char *buf, const int64_t buf_len,
                          share::ObAdminMutatorStringArg &str_arg);
  int get_next_log_entry(palf::LogEntry &log_entry);
private:
  int do_parse_one_log_entry_(palf::LogEntry &log_entry);
private:
  const char *buf_;
  int64_t curr_pos_;
  int64_t end_pos_;
  share::ObAdminMutatorStringArg str_arg_;
};
}
}
#endif
