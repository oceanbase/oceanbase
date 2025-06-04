/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ADMIN_UNCOMPRESS_PLAN_EXECUTOR_H_
#define OB_ADMIN_UNCOMPRESS_PLAN_EXECUTOR_H_

#include "../ob_admin_executor.h"

namespace oceanbase
{
namespace tools
{

class ObAdminUncompressPlanExecutor : public ObAdminExecutor
{
public:
  ObAdminUncompressPlanExecutor() {};
  virtual ~ObAdminUncompressPlanExecutor() {};
  int execute(int argc, char *argv[]) override final;
private:
  int uncompress_plan();
  int get_compressed_plan(ObArenaAllocator &allocator, const std::string &hex_str,
                          char *&compressed_str);
  int parse_cmd(int argc, char *argv[]);
  void print_usage();
private:
  int64_t uncompress_len_{0};
  int64_t compressed_len_{0};
  std::string plan_str_;
};

} //namespace tools
} //namespace oceanbase

#endif /* OB_ADMIN_UNCOMPRESS_PLAN_EXECUTOR_H_ */
