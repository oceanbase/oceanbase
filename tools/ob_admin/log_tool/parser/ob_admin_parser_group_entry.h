/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
