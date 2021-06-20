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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_ADDRS_PROVIDER_
#define OCEANBASE_SQL_EXECUTOR_OB_ADDRS_PROVIDER_

#include "lib/container/ob_iarray.h"
#include "lib/net/ob_addr.h"

namespace oceanbase {
namespace sql {
class ObAddrsProvider {
public:
  enum {
    INVALID_PROVIDER = 0,
    RANDOM_PROVIDER = 1,
  };

public:
  ObAddrsProvider()
  {}
  virtual ~ObAddrsProvider()
  {}

public:
  virtual int select_servers(int64_t select_count, common::ObIArray<common::ObAddr>& servers) = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_ADDRS_PROVIDER_ */
