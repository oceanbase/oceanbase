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

#include "ob_election_mem_stat.h"
#include "ob_election_async_log.h"

namespace oceanbase {
namespace election {
using namespace oceanbase::common;

void ObElectionMemStat::reset()
{
  addr_.reset();
  type_name_[0] = '\0';
  alloc_count_ = 0;
  release_count_ = 0;
}

void ObElectionMemStat::destroy()
{
  addr_.reset();
  alloc_count_ = -1;
  release_count_ = -1;
  memset(type_name_, 0, OB_ELECTION_TYPE_NAME_LENGTH);
}

int ObElectionMemStat::init(
    const ObAddr& addr, const char* type_name, const int64_t alloc_count, const int64_t release_count)
{
  int ret = OB_SUCCESS;

  if (!addr.is_valid() || OB_ISNULL(type_name) || alloc_count < 0 || release_count < 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(addr), KP(type_name), K(alloc_count), K(release_count));
    ret = OB_INVALID_ARGUMENT;
  } else {
    addr_ = addr;
    int64_t len = strlen(type_name);
    len = (len > OB_ELECTION_TYPE_NAME_LENGTH - 1) ? OB_ELECTION_TYPE_NAME_LENGTH - 1 : len;
    strncpy(type_name_, type_name, len);
    type_name_[len] = '\0';
    alloc_count_ = alloc_count;
    release_count_ = release_count;
  }

  return ret;
}

ObElectionMemStat& ObElectionMemStat::operator=(const ObElectionMemStat& election_mem_stat)
{
  // avoid assign to self
  if (this != &election_mem_stat) {
    addr_ = election_mem_stat.get_addr();
    int64_t len = strlen(election_mem_stat.get_type_name());
    len = ((len > OB_ELECTION_TYPE_NAME_LENGTH - 1) ? OB_ELECTION_TYPE_NAME_LENGTH - 1 : len);
    strncpy(type_name_, election_mem_stat.get_type_name(), len);
    type_name_[len] = '\0';
    alloc_count_ = election_mem_stat.get_alloc_count();
    release_count_ = election_mem_stat.get_release_count();
  }

  return *this;
}

}  // namespace election
}  // namespace oceanbase
