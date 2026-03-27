/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
