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

#ifndef _OB_TABLET_LOCATION_PROXY_H
#define _OB_TABLET_LOCATION_PROXY_H 1
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/net/ob_addr.h"
#include "share/location_cache/ob_location_struct.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common
namespace table
{
class ObITabletLocationGetter
{
public:
  ObITabletLocationGetter() = default;
  virtual ~ObITabletLocationGetter() = default;
  virtual int get_tablet_location(const common::ObString &tenant,
                                  const uint64_t tenant_id,
                                  const common::ObString &db,
                                  const common::ObString &table,
                                  const uint64_t table_id,
                                  const common::ObTabletID tablet_id,
                                  bool force_renew,
                                  share::ObTabletLocation &location) = 0;
};

typedef share::ObLSReplicaLocation ObTabletReplicaLocation;
class ObTabletLocationProxy: public ObITabletLocationGetter
{
public:
  ObTabletLocationProxy()
      :sql_client_(NULL)
  {}
  virtual ~ObTabletLocationProxy() = default;
  int init(common::ObMySQLProxy &proxy);
  virtual int get_tablet_location(const common::ObString &tenant,
                                  const uint64_t tenant_id,
                                  const common::ObString &db,
                                  const common::ObString &table,
                                  const uint64_t table_id,
                                  const common::ObTabletID tablet_id,
                                  bool force_renew,
                                  share::ObTabletLocation &location) override;
private:
  bool inited() const;
  int cons_replica_location(const common::sqlclient::ObMySQLResult &res,
                            ObTabletReplicaLocation &replica_location);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTabletLocationProxy);
private:
  common::ObMySQLProxy *sql_client_;
};

class ObTabletLocationCache: public ObITabletLocationGetter
{
public:
  ObTabletLocationCache()
      :is_inited_(false),
       location_proxy_(NULL),
       location_cache_(),
       sem_()
  {}
  virtual ~ObTabletLocationCache();
  int init(ObTabletLocationProxy &location_proxy);

  virtual int get_tablet_location(const common::ObString &tenant,
                                  const uint64_t tenant_id,
                                  const common::ObString &db,
                                  const common::ObString &table,
                                  const uint64_t table_id,
                                  const common::ObTabletID tablet_id,
                                  bool force_renew,
                                  share::ObTabletLocation &location) override;
private:
  typedef share::ObTabletLSCacheKey ObTabletLocationCacheKey; 
  typedef common::ObKVCache<ObTabletLocationCacheKey, share::ObTabletLocation> KVCache;
  static const int64_t DEFAULT_FETCH_LOCATION_TIMEOUT_US = 3 * 1000 * 1000;    //3s
  static const int64_t LOCATION_RENEW_CONCURRENCY = 10;
  constexpr static const char * const CACHE_NAME = "TABLE_API_LOCATION_CACHE";
  static const int64_t CACHE_PRIORITY = 1000;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTabletLocationCache);
  // function members
  static bool is_valid_key(const uint64_t tenant_id, const common::ObTabletID tablet_id);
  int get_from_cache(const uint64_t tenant_id,
                     const common::ObTabletID tablet_id,
                     share::ObTabletLocation &result);
  int put_to_cache(const uint64_t tenant_id,
                   const common::ObTabletID tablet_id,
                   const share::ObTabletLocation &location);
  int renew_get_tablet_location(const common::ObString &tenant,
                                const uint64_t tenant_id,
                                const common::ObString &db,
                                const common::ObString &table,
                                const uint64_t table_id,
                                const common::ObTabletID tablet_id,
                                share::ObTabletLocation &location);
private:
  // data members
  bool is_inited_;
  ObTabletLocationProxy *location_proxy_;
  KVCache location_cache_;
  share::ObLocationSem sem_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLET_LOCATION_PROXY_H */
