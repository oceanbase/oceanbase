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

#define USING_LOG_PREFIX SHARE
#include "lib/allocator/page_arena.h"
#include "share/ls/ob_ls_replica_filter.h"
#include "share/ls/ob_ls_info.h" // ObLSReplica
#include "share/ob_errno.h"

namespace oceanbase
{
namespace share
{
int ObServerLSReplicaFilter::check(const ObLSReplica &replica, bool &pass) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!replica.is_valid() || !server_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica), K_(server));
  } else {
    pass = (server_ == replica.get_server());
  }
  return ret;
}

ObLSReplicaFilterHolder::~ObLSReplicaFilterHolder()
{
  reset();
}


void ObLSReplicaFilterHolder::reset()
{
  while (!filter_list_.is_empty()) {
    ObLSReplicaFilter *node = filter_list_.remove_first();
    if (OB_NOT_NULL(node)) {
      node->~ObLSReplicaFilter();
      filter_allocator_.free(node);
      node = nullptr;
    }
  }
  filter_allocator_.reset();
}

void ObLSReplicaFilterHolder::try_free_filter_(ObLSReplicaFilter *filter, void *ptr)
{
  if (OB_NOT_NULL(filter)) {
    filter->~ObLSReplicaFilter();
    filter_allocator_.free(filter);
  } else if (OB_NOT_NULL(ptr)) {
    filter_allocator_.free(ptr);
  }
}

int ObLSReplicaFilterHolder::add_(ObLSReplicaFilter &filter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!filter_list_.add_last(&filter))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add filter to filter list failed", KR(ret));
  }
  return ret;
}

int ObLSReplicaFilterHolder::set_reserved_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObServerLSReplicaFilter *server_filter = nullptr;
  void *ptr = filter_allocator_.alloc(sizeof(ObServerLSReplicaFilter));
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    server_filter = new (ptr) ObServerLSReplicaFilter(server);
    if (OB_ISNULL(server_filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server filter is null", KR(ret));
    } else if (OB_FAIL(add_(*server_filter))) {
      LOG_WARN("add filter failed", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    try_free_filter_(server_filter, ptr);
  }
  return ret;
}

int ObLSReplicaFilterHolder::check(const ObLSReplica &replica, bool &pass) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else {
    pass = true;
    DLIST_FOREACH(it, filter_list_) {
      if (OB_FAIL(it->check(replica, pass))) {
        LOG_WARN("check replica failed", KR(ret));
      } else {
        if (!pass) {
          break;
        }
      }
    }
  }
  return ret;
}
} // end namespace share
} // end namespace oceanbase
