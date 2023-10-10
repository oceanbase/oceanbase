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
#ifndef OCEANBASE_ROOTSERVER_OB_TABLET_BALANCE_ALLOCATOR_H_
#define OCEANBASE_ROOTSERVER_OB_TABLET_BALANCE_ALLOCATOR_H_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ls_id.h"
namespace oceanbase
{
namespace share
{
namespace schema
{

class ObNonPartitionedTableTabletCache
{
public:
  class Pair {
  public:
    Pair() : ls_id_(), tablet_cnt_(0) {}
    Pair(const share::ObLSID &ls_id, const int64_t tablet_cnt)
      : ls_id_(ls_id), tablet_cnt_(tablet_cnt) {}
    ~Pair() {}
    ObLSID get_ls_id() const { return ls_id_; }
    int64_t get_tablet_cnt() const { return tablet_cnt_; }
    void set_tablet_cnt(const int64_t tablet_cnt) { tablet_cnt_ = tablet_cnt; }
    TO_STRING_KV(K_(ls_id), K_(tablet_cnt));
  private:
    share::ObLSID ls_id_;
    int64_t tablet_cnt_;
  };
public:
  ObNonPartitionedTableTabletCache() = delete;
  ObNonPartitionedTableTabletCache(const uint64_t tenant_id,
                                   common::ObMySQLProxy &sql_proxy);
  ~ObNonPartitionedTableTabletCache() {}

  void reset_cache();
  int alloc_tablet(const common::ObIArray<share::ObLSID> &avaliable_ls_ids,
                   share::ObLSID &ls_id);
private:
  void inner_reset_cache_();
  bool should_reload_cache_(
       const common::ObIArray<share::ObLSID> &avaliable_ls_ids);
  int reload_cache_(
      const common::ObIArray<share::ObLSID> &avaliable_ls_ids);
  void dump_cache_();
private:
  const static int64_t PAGE_SIZE = 1024;
  const static int64_t ARRAY_BLOCK_SIZE = 256;
private:
  lib::ObMutex mutex_;
  uint64_t tenant_id_;
  common::ObMySQLProxy &sql_proxy_;
  common::ObArenaAllocator allocator_;
  common::ObArray<Pair> cache_;
  int64_t loaded_timestamp_;
  int64_t dump_timestamp_;
  DISALLOW_COPY_AND_ASSIGN(ObNonPartitionedTableTabletCache);
};

/*
 * alloc ls for non-partition table's tablet in parallel ddl
 *
 * when ls changed or non parallel ddl execute, cache will be automatically updated or reset.
 *
 * Related cache may be inaccurate in the following cases:
 * 1. transfer tablet occur
 * 2. backgroup try_statistic_balance_group_status_() occur
 * 3. ls changed
 * 4. alloc ls success but parallel ddl execute failed
 *
 */
class ObNonPartitionedTableTabletAllocator
{
public:
  ObNonPartitionedTableTabletAllocator();
  ~ObNonPartitionedTableTabletAllocator();

  int init(common::ObMySQLProxy &sql_proxy);
  void destroy();

  void reset_all_cache();
  int reset_cache(const uint64_t tenant_id);

  int alloc_tablet(const uint64_t tenant_id,
                   const common::ObIArray<share::ObLSID> &avaliable_ls_ids,
                   share::ObLSID &ls_id);
private:
  int try_init_cache_(const uint64_t tenant_id);
private:
  common::SpinRWLock rwlock_;
  common::ObArenaAllocator allocator_;
  // tenant_cache_ won't be erased()
  common::hash::ObHashMap<uint64_t, ObNonPartitionedTableTabletCache*, common::hash::ReadWriteDefendMode> tenant_cache_;
  common::ObMySQLProxy *sql_proxy_;
  bool inited_;
};

} // end schema
} // end share
} // end oceanbase

#endif//OCEANBASE_ROOTSERVER_OB_TABLET_BALANCE_ALLOCATOR_H_
