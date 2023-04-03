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

#include "ob_tablet_filter.h"
#include "share/tablet/ob_tablet_info.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/utility/utility.h"
#include "common/ob_region.h"
#include "common/ob_zone.h"
#include "share/ob_iserver_trace.h"

namespace oceanbase
{
namespace share
{
using namespace schema;
using namespace common;

int ObTabletPermanentOfflineFilter::check(const ObTabletReplica &replica, bool &pass) const
{
  int ret = OB_SUCCESS;
  bool is_offline = false;
  if (OB_UNLIKELY(!replica.is_valid()) || OB_ISNULL(tracker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica), KP_(tracker));
  } else {
    if (OB_FAIL(tracker_->check_server_permanent_offline(replica.get_server(), is_offline))) {
      LOG_WARN("check_server_alive failed", "server", replica.get_server(), KR(ret));
    } else {
      pass = !is_offline;
    }
  }
  return ret;
}

int ObServerTabletReplicaFilter::check(const ObTabletReplica &replica, bool &pass) const
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

int ObTabletNotExistServerFilter::check(const ObTabletReplica &replica, bool &pass) const
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  if (OB_UNLIKELY(!replica.is_valid()) || OB_ISNULL(tracker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica), KP_(tracker));
  } else {
    if (OB_FAIL(tracker_->is_server_exist(replica.get_server(), is_exist))) {
      LOG_WARN("is_server_exist failed", "server", replica.get_server(), KR(ret));
    } else {
      pass = is_exist;
    }
  }
  return ret;
}

int ObTabletReplicaFilterHolder::add_(ObTabletReplicaFilter &filter)
{
  int ret = OB_SUCCESS;
  if (!filter_list_.add_last(&filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add filter to filter list failed", KR(ret));
  }
  return ret;
}

void ObTabletReplicaFilterHolder::reuse()
{
  while (!filter_list_.is_empty()) {
    filter_list_.remove_first();
  }
}

int ObTabletReplicaFilterHolder::set_filter_permanent_offline(const ObIServerTrace &tracker)
{
  int ret = OB_SUCCESS;
  ObTabletPermanentOfflineFilter *permanent_offline_filter = nullptr;
  void *ptr = filter_allocator_.alloc(sizeof(ObTabletPermanentOfflineFilter));
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    permanent_offline_filter = new (ptr) ObTabletPermanentOfflineFilter(&tracker);
    if (OB_ISNULL(permanent_offline_filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("permanent offline filter ptr is null", KR(ret));
    } else if (OB_FAIL(add_(*permanent_offline_filter))) {
      LOG_WARN("add filter failed", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    try_free_filter_(permanent_offline_filter, ptr);
  }
  return ret;
}

int ObTabletReplicaFilterHolder::check(const ObTabletReplica &replica, bool &pass) const
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

void ObTabletReplicaFilterHolder::try_free_filter_(ObTabletReplicaFilter *filter, void *ptr)
{
  if (OB_NOT_NULL(filter)) {
    filter->~ObTabletReplicaFilter();
    filter_allocator_.free(filter);
  } else if (OB_NOT_NULL(ptr)) {
    filter_allocator_.free(ptr);
  }
}

int ObTabletReplicaFilterHolder::set_reserved_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObServerTabletReplicaFilter *server_filter = nullptr;
  void *ptr = filter_allocator_.alloc(sizeof(ObServerTabletReplicaFilter));
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    server_filter = new (ptr) ObServerTabletReplicaFilter(server);
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

int ObTabletReplicaFilterHolder::set_filter_not_exist_server(const ObIServerTrace &tracker)
{
  int ret = OB_SUCCESS;
  ObTabletNotExistServerFilter *not_exist_server_filter = nullptr;
  void *ptr = filter_allocator_.alloc(sizeof(ObTabletNotExistServerFilter));
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    not_exist_server_filter = new (ptr) ObTabletNotExistServerFilter(&tracker);
    if (OB_ISNULL(not_exist_server_filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete server filter ptr is null", KR(ret));
    } else if (OB_FAIL(add_(*not_exist_server_filter))) {
      LOG_WARN("add filter failed", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    try_free_filter_(not_exist_server_filter, ptr);
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
