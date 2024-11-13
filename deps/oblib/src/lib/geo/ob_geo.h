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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_
#define OCEANBASE_LIB_GEO_OB_GEO_

#include "lib/string/ob_string.h"
#include "lib/geo/ob_geo_common.h"

namespace oceanbase {
namespace common {

class ObIGeoVisitor;
class ObGeometry {
public:
    // constructor
    ObGeometry(uint32_t srid = 0)
        : srid_(srid),
          version_(ENCODE_GEO_VERSION(GEO_VESION_1))  {}
    virtual ~ObGeometry() = default;
    ObGeometry(const ObGeometry& g) = default;
    ObGeometry& operator=(const ObGeometry& g) = default;
    // wkb interface
    virtual void set_data(const ObString& data) = 0;
    virtual ObString to_wkb() const { return ObString(); }
    virtual uint64_t length() const { return 0; }
    // val interface, do cast outside by functor
    virtual const char* val() const = 0;
    // Geo interface
    virtual ObGeoType type() const = 0;
    virtual ObGeoCRS crs() const = 0;
    virtual bool is_tree() const = 0;
    virtual bool is_empty() const = 0;
    // visitor
    virtual int do_visit(ObIGeoVisitor &visitor) = 0;
    // srid
    uint32_t get_srid() const { return srid_; }
    void set_srid(uint32_t srid) { srid_ = srid; }
    // version
    uint8_t get_version() { return version_; }
    VIRTUAL_TO_STRING_KV(K_(srid));
protected:
    uint32_t srid_;
    uint8_t version_;
};


} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_OB_GEO_
