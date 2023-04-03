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

#ifndef OCEANBASE_SHARE_OB_LOCALITY_INFO_H_
#define OCEANBASE_SHARE_OB_LOCALITY_INFO_H_

#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "common/ob_region.h"
#include "common/ob_zone_type.h"
#include "common/ob_idc.h"
#include "common/ob_zone_status.h"
#include "share/ob_zone_info.h"
#include "share/ob_zone_merge_info.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace share
{
struct ObLocalityZone
{
public:
  ObLocalityZone() { reset(); }
  ~ObLocalityZone() {}
  void reset();
  int init(const uint64_t tenant_id, const uint64_t region_priority);
  ObLocalityZone &operator =(const ObLocalityZone &item);
  TO_STRING_KV(K_(tenant_id), K_(region_priority));
public:
  uint64_t get_tenant_id() { return tenant_id_; }
  uint64_t get_region_priority() { return region_priority_; }
public:
  uint64_t tenant_id_;
  uint64_t region_priority_;
};

struct ObLocalityRegion
{
public:
  ObLocalityRegion() { reset(); }
  ~ObLocalityRegion() {}
  void reset();
public:
  typedef common::ObFixedLengthString<common::MAX_ZONE_LENGTH> Zone;
  typedef common::ObSEArray<Zone, 5> OneZoneArray;
  TO_STRING_KV(K_(region), K_(region_priority), K_(zone_array));
public:
  common::ObRegion region_;
  uint64_t region_priority_;
  OneZoneArray zone_array_;
};

struct ObLocalityInfo
{
public:
  ObLocalityInfo() { reset(); }
  ~ObLocalityInfo() {}
  void reset();
  void destroy();
  int add_locality_zone(const ObLocalityZone &item);
  int get_locality_zone(const uint64_t tenant_id, ObLocalityZone &item);
  int get_region_priority(const uint64_t tenant_id, uint64_t &region_priority);
  void set_version(const int64_t version);
  int64_t get_version() const;
  void set_local_region(const char *region);
  void set_local_zone(const char *zone);
  void set_local_idc(const char *idc);
  void set_local_zone_type(const common::ObZoneType zone_type);
  const char *get_local_zone() const;
  const char *get_local_region() const;
  const char *get_local_idc() const;
  int copy_to(ObLocalityInfo &locality_info);
  common::ObZoneType get_local_zone_type();
  TO_STRING_KV(K_(version), K_(local_region), K_(local_zone), K_(local_idc),
               K_(local_zone_type), K_(local_zone_status), K_(locality_region_array),
               K_(locality_zone_array));
  bool is_valid();
public:
  typedef common::ObFixedLengthString<common::MAX_ZONE_LENGTH> Zone;
  typedef common::ObSEArray<ObLocalityZone, 32> ObLocalityZoneArray;
  typedef common::ObSEArray<ObLocalityRegion, 5> ObLocalityRegionArray;
public:
  int64_t version_;
  common::ObRegion local_region_; // local region
  Zone local_zone_; // local zone
  common::ObIDC local_idc_; //local idc
  common::ObZoneType local_zone_type_; // local zone_type
  ObLocalityRegionArray locality_region_array_; // all region
  ObLocalityZoneArray locality_zone_array_; //tenant level locality info
  ObZoneStatus::Status local_zone_status_;
};

} // end namespace share
} // end namespace oceanbase
#endif
