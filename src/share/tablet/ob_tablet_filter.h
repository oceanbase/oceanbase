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

#ifndef OCEANBASE_SHARE_OB_TABLET_FILTER_H_
#define OCEANBASE_SHARE_OB_TABLET_FILTER_H_

#include "lib/net/ob_addr.h"
#include "lib/list/ob_dlist.h"
#include "common/ob_zone.h"
#include "share/ob_primary_zone_util.h"

namespace oceanbase
{
namespace share
{
class ObIServerTrace;
class ObTabletReplica;

class ObITabletReplicaFilter
{
public:
  ObITabletReplicaFilter() {}
  virtual ~ObITabletReplicaFilter() {}
  virtual int check(const ObTabletReplica &replica, bool &pass) const = 0;
};

class ObTabletReplicaFilter : public ObITabletReplicaFilter, public common::ObDLinkBase<ObTabletReplicaFilter>
{
public:
  ObTabletReplicaFilter() {}
  virtual ~ObTabletReplicaFilter() {}
};

class ObTabletPermanentOfflineFilter : public ObTabletReplicaFilter
{
public:
  explicit ObTabletPermanentOfflineFilter(const ObIServerTrace *tracker) : tracker_(tracker) {}
  virtual ~ObTabletPermanentOfflineFilter() {}
  virtual int check(const ObTabletReplica &replica, bool &pass) const;
private:
  const ObIServerTrace *tracker_;
};

class ObTabletNotExistServerFilter : public ObTabletReplicaFilter
{
public:
  explicit ObTabletNotExistServerFilter(const ObIServerTrace *tracker) : tracker_(tracker) {}
  virtual ~ObTabletNotExistServerFilter() {}
  virtual int check(const ObTabletReplica &replica, bool &pass) const;
private:
  const ObIServerTrace *tracker_;
};

// Reserve tablet replica by server
class ObServerTabletReplicaFilter : public ObTabletReplicaFilter
{
public:
  explicit ObServerTabletReplicaFilter(const common::ObAddr &server) : server_(server) {}
  virtual ~ObServerTabletReplicaFilter() {}
  virtual int check(const ObTabletReplica &replica, bool &pass) const;
private:
  common::ObAddr server_;
};

class ObTabletReplicaFilterHolder : public ObTabletReplicaFilter
{
public:
  ObTabletReplicaFilterHolder() {}
  virtual ~ObTabletReplicaFilterHolder() {}
  virtual int check(const ObTabletReplica &replica, bool &pass) const;
  void reuse();

  int set_filter_permanent_offline(const ObIServerTrace &tracker);
  int set_reserved_server(const common::ObAddr &server);
  int set_filter_not_exist_server(const ObIServerTrace &tracker);

private:
  int add_(ObTabletReplicaFilter &filter);
  void try_free_filter_(ObTabletReplicaFilter *filter, void *ptr);

  common::ObDList<ObTabletReplicaFilter> filter_list_;
  common::ObArenaAllocator filter_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletReplicaFilterHolder);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_TABLET_FILTER_H_
