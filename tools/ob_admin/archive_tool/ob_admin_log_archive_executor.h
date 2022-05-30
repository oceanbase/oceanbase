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

#ifndef OB_ADMIN_LOG_ARCHIVE_EXECUTOR_H_
#define OB_ADMIN_LOG_ARCHIVE_EXECUTOR_H_

#include "../ob_admin_executor.h"
#include "../clog_tool/ob_log_entry_filter.h"

namespace oceanbase
{
using namespace clog;

namespace tools
{
class ObAdminLogArchiveExecutor : public ObAdminExecutor
{
public:
  ObAdminLogArchiveExecutor(){}
  virtual ~ObAdminLogArchiveExecutor(){}
  virtual int execute(int argc, char *argv[]);
  int dump_single_file(const char *path);
  int dump_single_index_file(const char *path);
  int dump_single_key_file(const char *path);
private:
  int dump_log(int argc, char *argv[]);
  int dump_index(int argc, char *argv[]);
  int dump_key(int argc, char *argv[]);
  void print_usage();
  int mmap_log_file(const char *path, char *&buf_out, int64_t &buf_len, int &fd);
};

}
}

#endif /* OB_ADMIN_LOG_ARCHIVE_V2_H_ */
