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

#ifndef OB_ADMIN_USEC_EXECUTOR_H_
#define OB_ADMIN_USEC_EXECUTOR_H_
#include "../ob_admin_executor.h"

namespace oceanbase {
namespace tools {

enum ObAdminUsecCmd {
  TO_TIME,
  MAX_CMD,
};

class ObAdminUsecExecutor : public ObAdminExecutor {
public:
  ObAdminUsecExecutor();
  virtual ~ObAdminUsecExecutor() = default;
  virtual int execute(int argc, char *argv[]);
  void reset();

private:
  int parse_cmd(int argc, char *argv[]);
  void print_usage();

private:
  ObAdminUsecCmd cmd_;
  int64_t usec_;
  common::ObString time_zone_;
  ObTimeZoneInfo tz_info_;
};
}  // namespace tools
}  // namespace oceanbase

#endif /* OB_ADMIN_USEC_EXECUTOR_H_ */
