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

#define USING_LOG_PREFIX SHARE
#include <string.h>
#include "share/ob_define.h"
#include "share/ob_locality_priority.h"

namespace oceanbase
{
using namespace common;
namespace share
{
// calculate the region and region priority of primary zone
int ObLocalityPriority::get_primary_region_prioriry(const char *primary_zone,
    const ObIArray<ObLocalityRegion> &locality_region_array,
    ObIArray<ObLocalityRegion> &tenant_region_array)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(primary_zone)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char *str1, *str2;
    char *saveptr1, *saveptr2;
    char *token, *subtoken;
    char tmp_primary_zone[MAX_ZONE_LENGTH];

    size_t size = strlen(primary_zone) > (MAX_ZONE_LENGTH - 1) ? (MAX_ZONE_LENGTH - 1): strlen(primary_zone);
    memcpy(tmp_primary_zone, primary_zone, size);
    tmp_primary_zone[size] = '\0';
    str1 = tmp_primary_zone;

    for (uint64_t index = 0; OB_SUCC(ret); index++, str1 = NULL) {
      token = strtok_r(str1, ";", &saveptr1);
      if (OB_ISNULL(token))  {
        break;
      } else {
        ObRegion tmp_region;
        //tenant_region_array.at(index).region_priority_ = index * MAX_ZONE_NUM;
        for (str2 = token; OB_SUCC(ret); str2 = NULL) {
          subtoken = strtok_r(str2, ",", &saveptr2);
          char *p = NULL;
          char *saveptr3 = NULL;
          if (OB_ISNULL(subtoken)) {
            break;
          } else if (NULL == (p = strtok_r(subtoken, " ", &saveptr3))) {
            SHARE_LOG(WARN, "zone is empty", K(ret), K(subtoken));
          } else {
            tmp_region.reset();
            //1. get the region of this zone
            for (int64_t i = 0; tmp_region.is_empty() && i < locality_region_array.count(); i++) {
              for (int64_t j = 0; j < locality_region_array.at(i).zone_array_.count(); j++) {
                const char *zone = locality_region_array.at(i).zone_array_.at(j).ptr();
                if (strlen(zone) == strlen(zone) && (0 == strncmp(p, zone, strlen(zone)))) {
                  //locality_region.region_ = locality_region_array.at(i).region_;
                  tmp_region = locality_region_array.at(i).region_;
                  break;
                }
              } // for
            } // for

            //2. check if tenant_region_array contains this region, if so, push the zone back to array
            if (tmp_region.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              SHARE_LOG(WARN, "zone is not in locality_region", K(ret), K(p), K(locality_region_array));
            } else {
              int64_t k = 0;
              for (k = 0; OB_SUCC(ret) && k < tenant_region_array.count(); k++) {
                if (tmp_region == tenant_region_array.at(k).region_) {
                  if (OB_FAIL(tenant_region_array.at(k).zone_array_.push_back(p))) {
                    SHARE_LOG(WARN, "tenant_region_array zone_array push back error", K(ret), K(k), K(p));
                  }
                  break;
                }
              }
              //3. check if tenant_region_array contains this region, if not, push an ObLocalityRegion back to array
              if (OB_SUCC(ret)) {
                if (k == tenant_region_array.count()) {
                  ObLocalityRegion locality_region;
                  locality_region.zone_array_.reset();
                  locality_region.region_priority_ = index * MAX_ZONE_NUM;
                  locality_region.region_ = tmp_region;
                  if (OB_FAIL(locality_region.zone_array_.push_back(p))) {
                    SHARE_LOG(WARN, "locality_region push back error", K(ret), K(locality_region), "zone", p);
                  } else if (OB_FAIL(tenant_region_array.push_back(locality_region))) {
                    SHARE_LOG(WARN, "tenant_region_array push back error", K(ret), K(locality_region));
                  } else {
                    // do nothing
                  }
                }
              }
            }
          }
        } //for
      }
    } // for
  }

  return ret;
}

int ObLocalityPriority::get_region_priority(const ObLocalityInfo &locality_info,
    const ObIArray<ObLocalityRegion> &tenant_locality_region, uint64_t &region_priority)
{
  int ret = OB_SUCCESS;
  region_priority = UINT64_MAX;

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_locality_region.count(); i++) {
    if (tenant_locality_region.at(i).region_ == locality_info.local_region_) {
        region_priority = tenant_locality_region.at(i).region_priority_;
        break;
    }
  }

  return ret;
}


int ObLocalityPriority::get_zone_priority(const ObLocalityInfo &locality_info,
    const ObIArray<ObLocalityRegion> &tenant_locality_region, uint64_t &zone_priority)
{
  int ret = OB_SUCCESS;
  zone_priority = UINT64_MAX;

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_locality_region.count(); i++) {
    if (tenant_locality_region.at(i).region_ == locality_info.local_region_) {
      int64_t j = 0;
      const int64_t zone_count = tenant_locality_region.at(i).zone_array_.count();
      for (j = 0; OB_SUCC(ret) && j < zone_count; j++) {
        if (locality_info.local_zone_ == tenant_locality_region.at(i).zone_array_.at(j)) {
          break;
        }
      }
      if (j == zone_count) {
        zone_priority = tenant_locality_region.at(i).region_priority_ + MAX_ZONE_NUM - 1;
      } else {
        zone_priority = tenant_locality_region.at(i).region_priority_;
      }
      SHARE_LOG(INFO, "get_zone_priority", K(zone_priority), K(i), K(j), K(locality_info),
          K(tenant_locality_region));
      break;
    }
  }

  return ret;
}
} // end namespace share
} // end namespace oceanbase
