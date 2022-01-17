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

#include "ob_admin_executor.h"

namespace oceanbase {
using namespace common;
namespace tools {
int ObAdminExecutor::parse_options(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int option_index = 0;
  struct option long_options[] = {{"host", 1, NULL, 'h'},
      {"port", 1, NULL, 'p'},
      {"config_file", 1, NULL, 'f'},
      {"tenant_id", 1, NULL, 't'},
      {"wallet", 1, NULL, 'w'},
      {NULL, 0, NULL, 0}};
  int c;
  while (-1 != (c = getopt_long(argc, argv, "h:p:f:t:w:", long_options, &option_index))) {
    switch (c) {
      case 'h':
        DB_host_.assign_ptr(optarg, strlen(optarg));
        break;
      case 'p':
        DB_port_ = static_cast<int32_t>(strtol(optarg, NULL, 10));
        break;
      case 'f':
        config_file_ = optarg;
        break;
      case 't':
        tenant_id_ = static_cast<uint64_t>(strtol(optarg, NULL, 10));
        break;
      case 'w':
        wallet_file_ = optarg;
        break;
      case '?':
      case ':':
        ret = OB_ERR_UNEXPECTED;
        break;
      default:
        break;
    }
  }
  return ret;
}
}  // namespace tools
}  // namespace oceanbase
