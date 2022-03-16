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

#ifndef OB_ADMIN_EXECUTOR_H_
#define OB_ADMIN_EXECUTOR_H_
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include "share/ob_define.h"

namespace oceanbase {
namespace tools {
class ObAdminExecutor {
public:
  ObAdminExecutor() : DB_port_(-1), tenant_id_(0), config_file_(NULL), wallet_file_(NULL)
  {}
  virtual ~ObAdminExecutor()
  {}
  virtual int execute(int argc, char *argv[]) = 0;

protected:
  int parse_options(int argc, char *argv[]);

protected:
  common::ObString DB_host_;
  int32_t DB_port_;
  uint64_t tenant_id_;
  const char *config_file_;
  const char *wallet_file_;
};
}  // namespace tools
}  // namespace oceanbase

#endif /* OB_ADMIN_EXECUTOR_H_ */
