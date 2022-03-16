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

#ifndef OB_ADMIN_SLOG_EXECUTOR_H_
#define OB_ADMIN_SLOG_EXECUTOR_H_
#include "../ob_admin_executor.h"

namespace oceanbase
{
namespace tools
{
class ObAdminSlogExecutor : public ObAdminExecutor
{
public:
  ObAdminSlogExecutor();
  virtual ~ObAdminSlogExecutor() = default;
  virtual int execute(int argc, char *argv[]);
  void reset();
private:
  int parse_cmd(int argc, char *argv[]);
  void print_usage();

private:
  char *log_dir_;
  int64_t log_file_id_;
};
}
}

#endif /* OB_ADMIN_SLOG_EXECUTOR_H_ */
