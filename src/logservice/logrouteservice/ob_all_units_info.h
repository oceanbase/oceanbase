/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OB_UNITS_INFO_H_
#define OCEANBASE_OB_UNITS_INFO_H_

#include "lib/container/ob_se_array.h"              // ObSEArray
#include "common/ob_zone.h"                         // ObZone
#include "common/ob_region.h"                       // ObRegin
#include "common/ob_zone_type.h"                    // ObZoneType

namespace oceanbase
{
namespace logservice
{
// Records in table GV$OB_UNITS
struct ObUnitsRecord
{
  common::ObAddr server_;
  common::ObZone zone_;
  common::ObZoneType zone_type_;
  common::ObRegion region_;

  ObUnitsRecord() { reset(); }

  void reset()
  {
    server_.reset();
    zone_.reset();
    zone_type_ = common::ZONE_TYPE_INVALID;
    region_.reset();
  }

  int init(
      const common::ObAddr &server,
      ObString &zone,
      common::ObZoneType &zone_type,
      ObString &region);

  TO_STRING_KV(K_(server), K_(zone), K_(zone_type), K_(region));
};

class ObUnitsRecordInfo
{
public:
  static const int64_t ALL_SERVER_DEFAULT_RECORDS_NUM = 16;
  typedef common::ObSEArray<ObUnitsRecord, ALL_SERVER_DEFAULT_RECORDS_NUM> ObUnitsRecordArray;

  ObUnitsRecordInfo() { reset(); }
  virtual ~ObUnitsRecordInfo() { reset(); }

  int init(const int64_t cluster_id);
  void reset();
  inline int64_t get_cluster_id() { return cluster_id_; }
  inline ObUnitsRecordArray &get_units_record_array() { return units_record_array_; }
  int add(ObUnitsRecord &record);

  TO_STRING_KV(K_(cluster_id), K_(units_record_array));

private:
  int64_t cluster_id_;
  ObUnitsRecordArray units_record_array_;

  DISALLOW_COPY_AND_ASSIGN(ObUnitsRecordInfo);
};

} // namespace logservice
} // namespace oceanbase

#endif
