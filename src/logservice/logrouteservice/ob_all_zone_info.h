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

#ifndef OCEANBASE_logservice_H_
#define OCEANBASE_logservice_H_

#include "common/ob_zone.h"                               // ObZone
#include "common/ob_region.h"                             // ObRegin
#include "common/ob_zone_type.h"                          // ObZoneType
#include "share/ob_zone_info.h"                           // ObZoneStorageTyp

namespace oceanbase
{
namespace logservice
{
struct AllZoneRecord
{
  common::ObZone zone_;
  common::ObRegion region_;
  share::ObZoneInfo::StorageType storage_type_;

  AllZoneRecord() { reset(); }

  void reset()
  {
    zone_.reset();
    region_.reset();
    storage_type_ = share::ObZoneInfo::StorageType::STORAGE_TYPE_LOCAL;
  }

  int init(
      ObString &zone,
      ObString &region);

  void set_storage_type_by_str(common::ObString &storage_type_str) {
    storage_type_ = share::ObZoneInfo::get_storage_type(storage_type_str.ptr());
  }

  TO_STRING_KV(K_(zone), K_(region), K_(storage_type));
};

struct AllZoneTypeRecord
{
  common::ObZone zone_;
  common::ObZoneType zone_type_;

  AllZoneTypeRecord() { reset(); }

  void reset()
  {
    zone_.reset();
    zone_type_ = common::ZONE_TYPE_INVALID;
  }

  int init(ObString &zone,
      common::ObZoneType &zone_type);

  TO_STRING_KV(K_(zone), K_(zone_type));
};

class ObAllZoneInfo
{
public:
  static const int64_t DEFAULT_RECORDS_NUM = 16;
  typedef common::ObSEArray<AllZoneRecord, DEFAULT_RECORDS_NUM> AllZoneRecordArray;
  ObAllZoneInfo() { reset(); }
  virtual ~ObAllZoneInfo() { reset(); }

  int init(const int64_t cluster_id);
  void reset();
  inline int64_t get_cluster_id() { return cluster_id_; }
  inline AllZoneRecordArray &get_all_zone_array() { return all_zone_array_; }
  int add(AllZoneRecord &record);

  TO_STRING_KV(K_(cluster_id), K_(all_zone_array));

private:
  int64_t cluster_id_;
  AllZoneRecordArray all_zone_array_;

  DISALLOW_COPY_AND_ASSIGN(ObAllZoneInfo);
};

class ObAllZoneTypeInfo
{
public:
  static const int64_t DEFAULT_RECORDS_NUM = 16;
  typedef common::ObSEArray<AllZoneTypeRecord, DEFAULT_RECORDS_NUM> AllZoneTypeRecordArray;
  ObAllZoneTypeInfo() { reset(); }
  virtual ~ObAllZoneTypeInfo() { reset(); }

  int init(const int64_t cluster_id);
  void reset();
  inline int64_t get_cluster_id() { return cluster_id_; }
  inline AllZoneTypeRecordArray &get_all_zone_type_array() { return all_zone_type_array_; }
  int add(AllZoneTypeRecord &record);

  TO_STRING_KV(K_(cluster_id), K_(all_zone_type_array));

private:
  int64_t cluster_id_;
  AllZoneTypeRecordArray all_zone_type_array_;

  DISALLOW_COPY_AND_ASSIGN(ObAllZoneTypeInfo);
};

} // namespace logservice
} // namespace oceanbase

#endif

