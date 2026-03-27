/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_LS_REPLICA_FILTER
#define OCEANBASE_SHARE_OB_LS_REPLICA_FILTER

#include "lib/list/ob_dlist.h" // ObDLinkBase
#include "lib/net/ob_addr.h" // ObAddr

namespace oceanbase
{
namespace share
{
class ObLSReplica;

// Base class of all ls replica filter
class ObLSReplicaFilter : public common::ObDLinkBase<ObLSReplicaFilter>
{
public:
  ObLSReplicaFilter() {}
  virtual ~ObLSReplicaFilter() {}
  virtual int check(const ObLSReplica &replica, bool &pass) const = 0;
};

class ObSSLOGReplicaFilter : public ObLSReplicaFilter
{
public:
  ObSSLOGReplicaFilter() {}
  virtual ~ObSSLOGReplicaFilter() {}
  virtual int check(const ObLSReplica &replica, bool &pass) const;
};

// reserve ls replica by server
class ObServerLSReplicaFilter : public ObLSReplicaFilter
{
public:
  explicit ObServerLSReplicaFilter(const common::ObAddr &server) : server_(server) {}
  virtual ~ObServerLSReplicaFilter() {}
  virtual int check(const ObLSReplica &replica, bool &pass) const;
private:
  common::ObAddr server_;
};

// Apply multiple filters at the same time
class ObLSReplicaFilterHolder : public ObLSReplicaFilter
{
public:
  ObLSReplicaFilterHolder() {}
  virtual ~ObLSReplicaFilterHolder();
  virtual int check(const ObLSReplica &replica, bool &pass) const;
  void reset();
  int set_reserved_server(const common::ObAddr &server);
private:
  int add_(ObLSReplicaFilter &filter);
  void try_free_filter_(ObLSReplicaFilter *filter, void *ptr);

  common::ObDList<ObLSReplicaFilter> filter_list_;
  common::ObArenaAllocator filter_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObLSReplicaFilterHolder);
};
} // end namespace share
} // end namespace oceanbase
#endif
