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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_MEM_STAT_
#define OCEANBASE_ELECTION_OB_ELECTION_MEM_STAT_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace election {
class ObElectionMemStat {
public:
  ObElectionMemStat()
  {
    reset();
  }
  ~ObElectionMemStat()
  {
    destroy();
  }
  int init(const common::ObAddr& addr, const char* type_name, const int64_t alloc_count, const int64_t release_count);
  void reset();
  void destroy();

public:
  const char* get_type_name() const
  {
    return type_name_;
  }
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  int64_t get_alloc_count() const
  {
    return alloc_count_;
  }
  int64_t get_release_count() const
  {
    return release_count_;
  }
  ObElectionMemStat& operator=(const ObElectionMemStat& election_mem_stat);

  TO_STRING_KV("type_name", type_name_, K_(alloc_count), K_(release_count));

public:
  static const int64_t OB_ELECTION_TYPE_NAME_LENGTH = 64;

private:
  common::ObAddr addr_;
  char type_name_[OB_ELECTION_TYPE_NAME_LENGTH];
  int64_t alloc_count_;
  int64_t release_count_;
};
}  // namespace election
}  // namespace oceanbase
#endif  // OCEANBASE_ELECTION_OB_ELECTION_MEM_STAT_
