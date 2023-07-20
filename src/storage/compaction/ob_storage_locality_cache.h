//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_LOCALITY_CACHE_H_
#define OB_STORAGE_COMPACTION_LOCALITY_CACHE_H_

#include "share/ob_ls_id.h"
#include "deps/oblib/src/lib/net/ob_addr.h"
#include "deps/oblib/src/common/ob_zone.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace compaction
{

template<typename T>
struct ObLSLocalityStruct
{
public:
  ObLSLocalityStruct()
  : ls_id_(),
    svr_addr_list_()
  {}
  ~ObLSLocalityStruct()
  {
  }
  void set_attr(const int64_t tenant_id)
  {
    ObMemAttr attr(tenant_id, "LSLocCache");
    svr_addr_list_.set_attr(attr);
  }
  void reset()
  {
    ls_id_.reset();
    svr_addr_list_.reset();
  }
  bool is_valid() const
  {
    return ls_id_.is_valid() && svr_addr_list_.count() > 0;
  }

  TO_STRING_KV(K_(ls_id), K_(svr_addr_list));
  share::ObLSID ls_id_;
  common::ObArray<T> svr_addr_list_;
};

struct ObLSLocalityInCache : public ObLSLocalityStruct<ObAddr *>
{
public:
  int assgin(ObLSLocalityInCache &locality);
};

struct ObLSLocality : public ObLSLocalityStruct<ObAddr>
{
public:
  bool check_exist(const common::ObAddr &addr) const;
};

class ObStorageLocalityCache
{
public:
  ObStorageLocalityCache();
  ~ObStorageLocalityCache();
  int init(uint64_t tenant_id, ObMySQLProxy *sql_proxy);
  void reset();
  int refresh_ls_locality();
  int get_ls_locality(const share::ObLSID &ls_id, ObLSLocality &ls_locality);

private:
  int get_zone_list(ObIArray<common::ObZone> &zone_list);
  static int str2zone_list(
    const char *str,
    ObIArray<common::ObZone> &zone_list);

  int get_ls_locality_by_zone(const ObIArray<common::ObZone> &zone_list);
  static int append_zone_info(
    const ObIArray<common::ObZone> &zone_list,
    ObSqlString &sql);
  int get_ls_locality_cnt(
    ObMySQLTransaction &trans,
    const ObIArray<common::ObZone> &zone_list,
    int64_t &ls_locality_cnt);

  int generate_ls_locality(
    const int64_t ls_locality_cnt,
    sqlclient::ObMySQLResult &result);
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_locality_array));

private:
  bool is_inited_;
  lib::ObMutex lock_;
  uint64_t tenant_id_;
  ObMySQLProxy *sql_proxy_;
  void *alloc_buf_;
  common::ModulePageAllocator allocator_;
  common::ObArray<ObLSLocalityInCache> ls_locality_array_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_LOCALITY_CACHE_H_
