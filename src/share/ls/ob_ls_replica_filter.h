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
