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

#ifndef OCEANBASE_LIB_GEO_OB_SRS_WKT_PARSER_
#define OCEANBASE_LIB_GEO_OB_SRS_WKT_PARSER_

#include "lib/geo/ob_srs_info.h"
#include "lib/string/ob_string_buffer.h"

namespace oceanbase
{

namespace  common
{

// read str from qi
struct ObQiString
{
  ObArenaAllocator allocator_;
  ObStringBuffer val_;

  ObQiString():allocator_("QiString"), val_(&allocator_) {}
  ObQiString(const ObQiString& other):allocator_("QiString"), val_(&allocator_) {
    this->val_.append(other.val_.string());
  }
};

class ObSrsWktParser final
{
public:
  ObSrsWktParser() {}
  ~ObSrsWktParser() {}

public:
  static int parse_srs_wkt(common::ObIAllocator& allocator, uint64_t srid,
                           const common::ObString &srs_str,
                           ObSpatialReferenceSystemBase *&srs);

  // for parser test currently
  static int parse_geog_srs_wkt(common::ObIAllocator& allocator, const common::ObString &srs_str, ObGeographicRs &result);
  static int parse_proj_srs_wkt(common::ObIAllocator& allocator, const common::ObString &srs_str, ObProjectionRs &result);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSrsWktParser);
};

} // common
} // oceanbase

#endif /* OCEANBASE_LIB_GIS_OB_SRS_WKT_PARSER_ */