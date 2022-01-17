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

#ifndef OB_ADMIN_CLOG_V2_EXECUTOR_H_
#define OB_ADMIN_CLOG_V2_EXECUTOR_H_

#include "../ob_admin_executor.h"
#include "ob_log_entry_filter.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
using namespace clog;

namespace tools {
class ObAdminClogV2Executor : public ObAdminExecutor {
public:
  ObAdminClogV2Executor();
  virtual ~ObAdminClogV2Executor();
  virtual int execute(int argc, char* argv[]);

private:
  int dump_all(int argc, char* argv[]);
  int dump_filter(int argc, char* argv[]);
  int dump_hex(int argc, char* argv[]);
  int dump_inner(int argc, char* argv[], bool is_hex);
  int dump_format(int argc, char* argv[]);
  int stat_clog(int argc, char* argv[]);
  int dump_ilog(int argc, char* argv[]);

  void print_usage();
  int parse_options(int argc, char* argv[]);

  int grep(int argc, char* argv[]);
  int encode_int(char* buf, int64_t& pos, int64_t buf_len, const char* encode_type, int64_t int_value);

  int dump_single_clog(const char* path, bool is_hex);
  int dump_single_ilog(const char* path);
  int dump_format_single_file(const char* path);
  int stat_single_log(const char* path);

  int mmap_log_file(const char* path, char*& buf_out, int64_t& buf_len, int& fd);
  int close_fd(const char* path, const int fd, char* buf, const int64_t buf_len);

private:
  ObLogEntryFilter filter_;
  common::ObString DB_host_;
  int32_t DB_port_;
  bool is_ofs_open_;
};

}  // namespace tools
}  // namespace oceanbase

#endif /* OB_ADMIN_CLOG_EXECUTOR_V2_H_ */
