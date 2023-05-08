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

#ifndef OCEANBASE_SHARE_OB_SERVER_LOCALITY_CACHE_
#define OCEANBASE_SHARE_OB_SERVER_LOCALITY_CACHE_

#include "lib/net/ob_addr.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_zone_type.h"
#include "common/ob_idc.h"
#include "share/ob_server_status.h"

namespace oceanbase
{
namespace share
{
class ObServerLocality
{
public:
  ObServerLocality();
  virtual ~ObServerLocality();
  void reset();
  int assign(const ObServerLocality &other);
  int init(const char *svr_ip,
           const int32_t svr_port,
           const common::ObZone &zone,
           const common::ObZoneType zone_type,
           const common::ObIDC &idc,
           const common::ObRegion &region,
           bool is_active);
  const common::ObAddr &get_addr() const { return addr_; }
  const common::ObZone &get_zone() const { return zone_; }
  const common::ObZoneType &get_zone_type() const { return zone_type_; }
  const common::ObIDC &get_idc() const { return idc_; }
  const common::ObRegion &get_region() const { return region_; }
  bool is_init() const { return inited_; }
  bool is_active() const { return is_active_; }
  void set_start_service_time(int64_t start_service_time) { start_service_time_ = start_service_time; }
  void set_server_stop_time(int64_t stop_time) { server_stop_time_ = stop_time; }
  int set_server_status(const char *str);
  int64_t get_start_service_time() const { return start_service_time_; }
  int64_t get_server_stop_time() const { return server_stop_time_;  }
  ObServerStatus::DisplayStatus get_server_status() const { return server_status_; }
  TO_STRING_KV(K_(inited),
               K_(addr),
               K_(zone),
               K_(zone_type),
               K_(idc),
               K_(region),
               K_(is_active),
               K_(start_service_time),
               K_(server_stop_time),
               K_(server_status));
private:
  bool inited_;
  bool is_idle_;
  bool is_active_;
  common::ObAddr addr_;
  common::ObZone zone_;
  common::ObZoneType zone_type_;
  common::ObIDC idc_;
  common::ObRegion region_;
  int64_t start_service_time_;
  int64_t server_stop_time_;
  ObServerStatus::DisplayStatus server_status_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObServerLocality);
};

class ObServerLocalityCache
{
public:
  ObServerLocalityCache();
  virtual ~ObServerLocalityCache();
  int init();
  void reset();
  void destroy();
  int get_server_zone_type(const common::ObAddr &server, common::ObZoneType &zone_type) const;
  int get_server_region(const common::ObAddr &server, common::ObRegion &region) const;
  int get_server_idc(const common::ObAddr &server, common::ObIDC &idc) const;
  int get_server_zone(const common::ObAddr &server, common::ObZone &zone) const;
  int get_noempty_zone_region(const common::ObZone &zone, common::ObRegion &region) const;
  int get_server_locality_array(common::ObIArray<ObServerLocality> &server_locality_array, bool &has_readonly_zone) const;
  int set_server_locality_array(const common::ObIArray<ObServerLocality> &server_locality_array, bool has_readonly_zone);
  int get_server_cluster_id(const common::ObAddr &server, int64_t &cluster_id) const;
  int record_server_cluster_id(const common::ObAddr &server, const int64_t &cluster_id);
  int record_server_region(const common::ObAddr &server, const common::ObRegion &region);
  int record_server_idc(const common::ObAddr &server, const common::ObIDC &idc);
  TO_STRING_KV(K_(server_locality_array), K_(has_readonly_zone));
private:
  int get_server_region_from_map_(const common::ObAddr &server, common::ObRegion &region) const;
  int get_server_idc_from_map_(const common::ObAddr &server, common::ObIDC &idc) const;
private:
  mutable common::SpinRWLock rwlock_;
  common::ObSEArray<ObServerLocality, 32> server_locality_array_;
  common::ObLinearHashMap<common::ObAddr, int64_t> server_cid_map_;  // store <server, cluster_id>
  common::ObLinearHashMap<common::ObAddr, common::ObRegion> server_region_map_;
  common::ObLinearHashMap<common::ObAddr, common::ObIDC> server_idc_map_;
  bool has_readonly_zone_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObServerLocalityCache);
};
}
}
#endif /* OCEANBASE_SHARE_OB_SERVER_LOCALITY_CACHE_ */
