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
 * This file contains implementation for geometry cast.
 */

#ifndef OCEANBASE_LIB_OB_GEOMETRY_CAST
#define OCEANBASE_LIB_OB_GEOMETRY_CAST

#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_tree.h"

namespace oceanbase
{
namespace common
{

class ObGeometryTypeCastUtil
{
public:
  ObGeometryTypeCastUtil() {};
  virtual ~ObGeometryTypeCastUtil() {};
  static int get_tree(common::ObIAllocator &allocator,
                      const common::ObString &wkb,
                      common::ObGeometry *&geo_tree,
                      const common::ObSrsItem *srs,
                      common::ObGeoErrLogInfo &log_info,
                      const char *func_name);
  static int check_polygon_direction(common::ObIAllocator &allocator,
                                     const common::ObSrsItem *srs,
                                     common::ObGeometry *geo_tree);
  template<typename L>
  static bool is_line_can_ring(const L &line);
  template<typename P>
  static bool is_point_equal(const P &p_left, const P &p_right);
  static int check_longitude(double val_radian,
                             const common::ObSrsItem *srs,
                             double val);
  static int check_latitude(double val_radian,
                            const common::ObSrsItem *srs,
                            double val);
  static const char *get_cast_name(common::ObGeoType type);
  static bool is_sdo_geometry_type_compatible(uint64_t src_udt_id, uint64_t dst_udt_id);
  static bool is_sdo_geometry_udt(uint64_t udt_id);
private:
  static bool is_sdo_geometry_varray_type(uint64_t udt_id);
  DISALLOW_COPY_AND_ASSIGN(ObGeometryTypeCastUtil);
};

class ObGeometryTypeCast
{
public:
  ObGeometryTypeCast() {};
  virtual ~ObGeometryTypeCast() {};
  virtual int cast_geom(const common::ObGeometry &src,
                        common::ObGeometry &dst,
                        const common::ObSrsItem *srs,
                        common::ObGeoErrLogInfo &log_info,
                        common::ObIAllocator *allocator = NULL) const = 0;
  virtual int cast_geog(const common::ObGeometry &src,
                        common::ObGeometry &dst,
                        const common::ObSrsItem *srs,
                        common::ObGeoErrLogInfo &log_info,
                        common::ObIAllocator *allocator = NULL) const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGeometryTypeCast);
};

class ObPointTypeCast : public ObGeometryTypeCast
{
public:
  ObPointTypeCast() {};
  virtual ~ObPointTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianPoint, common::ObCartesianMultipoint,
        common::ObCartesianGeometrycollection>(src, dst, srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographPoint, common::ObGeographMultipoint,
        common::ObGeographGeometrycollection>(src, dst, srs, log_info, allocator);
  }
private:
  template<typename P, typename MPT, typename GC>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator = NULL) const;
  DISALLOW_COPY_AND_ASSIGN(ObPointTypeCast);
};

class ObLineStringTypeCast : public ObGeometryTypeCast
{
public:
  ObLineStringTypeCast() {};
  virtual ~ObLineStringTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianLineString, common::ObCartesianPolygon,
        common::ObCartesianMultipoint, common::ObCartesianMultilinestring,
        common::ObCartesianGeometrycollection>(src, dst, srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographLineString, common::ObGeographPolygon,
        common::ObGeographMultipoint, common::ObGeographMultilinestring,
        common::ObGeographGeometrycollection>(src, dst, srs, log_info, allocator);
  }
private:
  template<typename L, typename PL, typename MPT, typename ML, typename GC>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator = NULL) const;
  DISALLOW_COPY_AND_ASSIGN(ObLineStringTypeCast);
};

class ObPolygonTypeCast : public ObGeometryTypeCast
{
public:
  ObPolygonTypeCast() {};
  virtual ~ObPolygonTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianPolygon, common::ObCartesianLineString,
        common::ObCartesianMultilinestring, common::ObCartesianMultipolygon,
        common::ObCartesianGeometrycollection, common::ObCartesianLinearring>(src, dst,
        srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographPolygon, common::ObGeographLineString,
        common::ObGeographMultilinestring, common::ObGeographMultipolygon,
        common::ObGeographGeometrycollection, common::ObGeographLinearring>(src, dst,
        srs, log_info, allocator);
  }
private:
  template<typename PL, typename L, typename ML, typename MPL, typename GC, typename LR>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator) const;
  DISALLOW_COPY_AND_ASSIGN(ObPolygonTypeCast);
};

class ObMultiPointTypeCast : public ObGeometryTypeCast
{
public:
  ObMultiPointTypeCast() {};
  virtual ~ObMultiPointTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianMultipoint, common::ObCartesianPoint,
        common::ObCartesianLineString, common::ObCartesianGeometrycollection>(src, dst,
        srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographMultipoint, common::ObGeographPoint,
        common::ObGeographLineString, common::ObGeographGeometrycollection>(src, dst,
        srs, log_info, allocator);
  }
private:
  template<typename MPT, typename P, typename L, typename GC>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator = NULL) const;
  DISALLOW_COPY_AND_ASSIGN(ObMultiPointTypeCast);
};

class ObMultiLineStringTypeCast : public ObGeometryTypeCast
{
public:
  ObMultiLineStringTypeCast() {};
  virtual ~ObMultiLineStringTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianMultilinestring, common::ObCartesianLineString,
        common::ObCartesianPolygon, common::ObCartesianMultipolygon,
        common::ObCartesianGeometrycollection, common::ObCartesianLinearring>(src, dst,
        srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographMultilinestring, common::ObGeographLineString,
        common::ObGeographPolygon, common::ObGeographMultipolygon,
        common::ObGeographGeometrycollection, common::ObGeographLinearring>(src, dst,
        srs, log_info, allocator);
  }
private:
  template<typename ML, typename L, typename PL, typename MPL, typename GC, typename LR>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator) const;
  DISALLOW_COPY_AND_ASSIGN(ObMultiLineStringTypeCast);
};

class ObMultiPolygonTypeCast : public ObGeometryTypeCast
{
public:
  ObMultiPolygonTypeCast() {};
  virtual ~ObMultiPolygonTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianMultipolygon, common::ObCartesianPolygon,
        common::ObCartesianMultilinestring, common::ObCartesianGeometrycollection,
        common::ObCartesianLineString, common::ObCartesianLinearring>(src, dst,
        srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographMultipolygon, common::ObGeographPolygon,
        common::ObGeographMultilinestring, common::ObGeographGeometrycollection,
        common::ObGeographLineString, common::ObGeographLinearring>(src, dst,
        srs, log_info, allocator);
  }
private:
  template<typename MPL, typename PL, typename ML, typename GC, typename L, typename LR>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator) const;
  DISALLOW_COPY_AND_ASSIGN(ObMultiPolygonTypeCast);
};

class ObGeomcollectionTypeCast : public ObGeometryTypeCast
{
public:
  ObGeomcollectionTypeCast() {};
  virtual ~ObGeomcollectionTypeCast() {};
  int cast_geom(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObCartesianGeometrycollection, common::ObCartesianPoint,
        common::ObCartesianMultipoint, common::ObCartesianMultilinestring,
        common::ObCartesianMultipolygon>(src, dst, srs, log_info, allocator);
  }
  int cast_geog(const common::ObGeometry &src,
                common::ObGeometry &dst,
                const common::ObSrsItem *srs,
                common::ObGeoErrLogInfo &log_info,
                common::ObIAllocator *allocator = NULL) const override
  {
    return cast<common::ObGeographGeometrycollection, common::ObGeographPoint,
        common::ObGeographMultipoint, common::ObGeographMultilinestring,
        common::ObGeographMultipolygon>(src, dst, srs, log_info, allocator);
  }
private:
  template<typename GC, typename P, typename MPT, typename ML, typename MPL>
  int cast(const common::ObGeometry &src,
           common::ObGeometry &dst,
           const common::ObSrsItem *srs,
           common::ObGeoErrLogInfo &log_info,
           common::ObIAllocator *allocator) const;
  DISALLOW_COPY_AND_ASSIGN(ObGeomcollectionTypeCast);
};

class ObGeometryTypeCastFactory
{
public:
  ObGeometryTypeCastFactory() {};
  virtual ~ObGeometryTypeCastFactory() {};
  static int alloc(common::ObIAllocator &alloc, common::ObGeoType geo_type, ObGeometryTypeCast *&geo_cast);
private:
  DISALLOW_COPY_AND_ASSIGN(ObGeometryTypeCastFactory);
};

} // sql
} // oceanbase
#endif // OCEANBASE_LIB_OB_GEOMETRY_CAST