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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_VISITOR_
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_ibin.h"

namespace oceanbase
{
namespace common
{

class ObIGeoVisitor
{
public:
  ObIGeoVisitor() = default;
  virtual ~ObIGeoVisitor() = default;
  // for abstract class
  virtual bool prepare(ObGeometry *geo) = 0;
  virtual bool prepare(ObCurve *geo) = 0;
  virtual bool prepare(ObSurface *geo) = 0;
  virtual bool prepare(ObLineString *geo) = 0;
  virtual bool prepare(ObLinearring *geo) = 0;
  virtual bool prepare(ObPolygon *geo) = 0;
  virtual bool prepare(ObGeometrycollection *geo) = 0;
  virtual bool prepare(ObMultipoint *geo) = 0;
  virtual bool prepare(ObMulticurve *geo) = 0;
  virtual bool prepare(ObMultisurface *geo) = 0;
  virtual bool prepare(ObMultilinestring *geo) = 0;
  virtual bool prepare(ObMultipolygon *geo) = 0;

  virtual bool prepare(ObIWkbGeometry *geo) = 0;

  virtual int visit(ObGeometry *geo) = 0;
  virtual int visit(ObPoint *geo) = 0;
  virtual int visit(ObCurve *geo) = 0;
  virtual int visit(ObSurface *geo) = 0;
  virtual int visit(ObLineString *geo) = 0;
  virtual int visit(ObLinearring *geo) = 0;
  virtual int visit(ObPolygon *geo) = 0;
  virtual int visit(ObGeometrycollection *geo) = 0;
  virtual int visit(ObMultipoint *geo) = 0;
  virtual int visit(ObMulticurve *geo) = 0;
  virtual int visit(ObMultisurface *geo) = 0;
  virtual int visit(ObMultilinestring *geo) = 0;
  virtual int visit(ObMultipolygon *geo) = 0;

  virtual int visit(ObIWkbGeometry *geo) = 0;
  virtual int visit(ObIWkbPoint *geo) = 0;


  virtual bool is_end(ObGeometry *geo) = 0;
  virtual bool is_end(ObPoint *geo) = 0;
  virtual bool is_end(ObCurve *geo) = 0;
  virtual bool is_end(ObSurface *geo) = 0;
  virtual bool is_end(ObLineString *geo) = 0;
  virtual bool is_end(ObLinearring *geo) = 0;
  virtual bool is_end(ObPolygon *geo) = 0;
  virtual bool is_end(ObGeometrycollection *geo) = 0;
  virtual bool is_end(ObMultipoint *geo) = 0;
  virtual bool is_end(ObMulticurve *geo) = 0;
  virtual bool is_end(ObMultisurface *geo) = 0;
  virtual bool is_end(ObMultilinestring *geo) = 0;
  virtual bool is_end(ObMultipolygon *geo) = 0;

  virtual bool is_end(ObIWkbGeometry *geo) = 0;

  // for geo tree
  virtual bool prepare(ObCartesianMultipoint *geo) = 0;
  virtual bool prepare(ObGeographMultipoint *geo) = 0;
  virtual bool prepare(ObCartesianLineString *geo) = 0;
  virtual bool prepare(ObGeographLineString *geo) = 0;
  virtual bool prepare(ObCartesianLinearring *geo) = 0;
  virtual bool prepare(ObGeographLinearring *geo) = 0;
  virtual bool prepare(ObCartesianMultilinestring *geo) = 0;
  virtual bool prepare(ObGeographMultilinestring *geo) = 0;
  virtual bool prepare(ObCartesianPolygon *geo) = 0;
  virtual bool prepare(ObGeographPolygon *geo) = 0;
  virtual bool prepare(ObCartesianMultipolygon *geo) = 0;
  virtual bool prepare(ObGeographMultipolygon *geo) = 0;
  virtual bool prepare(ObCartesianGeometrycollection *geo) = 0;
  virtual bool prepare(ObGeographGeometrycollection *geo) = 0;

  virtual int visit(ObCartesianPoint *geo) = 0;
  virtual int visit(ObGeographPoint *geo) = 0;
  virtual int visit(ObCartesianMultipoint *geo) = 0;
  virtual int visit(ObGeographMultipoint *geo) = 0;
  virtual int visit(ObCartesianLineString *geo) = 0;
  virtual int visit(ObGeographLineString *geo) = 0;
  virtual int visit(ObCartesianLinearring *geo) = 0;
  virtual int visit(ObGeographLinearring *geo) = 0;
  virtual int visit(ObCartesianMultilinestring *geo) = 0;
  virtual int visit(ObGeographMultilinestring *geo) = 0;
  virtual int visit(ObCartesianPolygon *geo) = 0;
  virtual int visit(ObGeographPolygon *geo) = 0;
  virtual int visit(ObCartesianMultipolygon *geo) = 0;
  virtual int visit(ObGeographMultipolygon *geo) = 0;
  virtual int visit(ObCartesianGeometrycollection *geo) = 0;
  virtual int visit(ObGeographGeometrycollection *geo) = 0;

  virtual bool is_end(ObCartesianMultipoint *geo) = 0;
  virtual bool is_end(ObGeographMultipoint *geo) = 0;
  virtual bool is_end(ObCartesianLineString *geo) = 0;
  virtual bool is_end(ObGeographLineString *geo) = 0;
  virtual bool is_end(ObCartesianLinearring *geo) = 0;
  virtual bool is_end(ObGeographLinearring *geo) = 0;
  virtual bool is_end(ObCartesianMultilinestring *geo) = 0;
  virtual bool is_end(ObGeographMultilinestring *geo) = 0;
  virtual bool is_end(ObCartesianPolygon *geo) = 0;
  virtual bool is_end(ObGeographPolygon *geo) = 0;
  virtual bool is_end(ObCartesianMultipolygon *geo) = 0;
  virtual bool is_end(ObGeographMultipolygon *geo) = 0;
  virtual bool is_end(ObCartesianGeometrycollection *geo) = 0;
  virtual bool is_end(ObGeographGeometrycollection *geo) = 0;

  // for geo binary
  virtual bool prepare(ObIWkbGeogMultiPoint *geo) = 0;
  virtual bool prepare(ObIWkbGeomMultiPoint *geo) = 0;
  virtual bool prepare(ObIWkbGeogLineString *geo) = 0;
  virtual bool prepare(ObIWkbGeomLineString *geo) = 0;
  virtual bool prepare(ObIWkbGeogLinearRing *geo) = 0;
  virtual bool prepare(ObIWkbGeomLinearRing *geo) = 0;
  virtual bool prepare(ObIWkbGeogMultiLineString *geo) = 0;
  virtual bool prepare(ObIWkbGeomMultiLineString *geo) = 0;
  virtual bool prepare(ObIWkbGeogPolygon *geo) = 0;
  virtual bool prepare(ObIWkbGeomPolygon *geo) = 0;
  virtual bool prepare(ObIWkbGeogMultiPolygon *geo) = 0;
  virtual bool prepare(ObIWkbGeomMultiPolygon *geo) = 0;
  virtual bool prepare(ObIWkbGeogCollection *geo) = 0;
  virtual bool prepare(ObIWkbGeomCollection *geo) = 0;

  virtual int visit(ObIWkbGeogPoint *geo) = 0;
  virtual int visit(ObIWkbGeomPoint *geo) = 0;
  virtual int visit(ObIWkbGeogMultiPoint *geo) = 0;
  virtual int visit(ObIWkbGeomMultiPoint *geo) = 0;
  virtual int visit(ObIWkbGeogLineString *geo) = 0;
  virtual int visit(ObIWkbGeomLineString *geo) = 0;
  virtual int visit(ObIWkbGeogLinearRing *geo) = 0;
  virtual int visit(ObIWkbGeomLinearRing *geo) = 0;
  virtual int visit(ObIWkbGeogMultiLineString *geo) = 0;
  virtual int visit(ObIWkbGeomMultiLineString *geo) = 0;
  virtual int visit(ObIWkbGeogPolygon *geo) = 0;
  virtual int visit(ObIWkbGeomPolygon *geo) = 0;
  virtual int visit(ObIWkbGeogMultiPolygon *geo) = 0;
  virtual int visit(ObIWkbGeomMultiPolygon *geo) = 0;
  virtual int visit(ObIWkbGeogCollection *geo) = 0;
  virtual int visit(ObIWkbGeomCollection *geo) = 0;

  virtual bool is_end(ObIWkbGeogMultiPoint *geo) = 0;
  virtual bool is_end(ObIWkbGeomMultiPoint *geo) = 0;
  virtual bool is_end(ObIWkbGeogLineString *geo) = 0;
  virtual bool is_end(ObIWkbGeomLineString *geo) = 0;
  virtual bool is_end(ObIWkbGeogLinearRing *geo) = 0;
  virtual bool is_end(ObIWkbGeomLinearRing *geo) = 0;
  virtual bool is_end(ObIWkbGeogMultiLineString *geo) = 0;
  virtual bool is_end(ObIWkbGeomMultiLineString *geo) = 0;
  virtual bool is_end(ObIWkbGeogPolygon *geo) = 0;
  virtual bool is_end(ObIWkbGeomPolygon *geo) = 0;
  virtual bool is_end(ObIWkbGeogMultiPolygon *geo) = 0;
  virtual bool is_end(ObIWkbGeomMultiPolygon *geo) = 0;
  virtual bool is_end(ObIWkbGeogCollection *geo) = 0;
  virtual bool is_end(ObIWkbGeomCollection *geo) = 0;
  // currently just for collection
  virtual int finish(ObGeometry *geo) = 0;
  virtual int finish(ObGeometrycollection *geo) = 0;
  virtual int finish(ObMultisurface *geo) = 0;
  virtual int finish(ObMulticurve *geo) = 0;
  virtual int finish(ObMultipoint *geo) = 0;

  virtual int finish(ObMultilinestring *geo) = 0;
  virtual int finish(ObMultipolygon *geo) = 0;

  virtual int finish(ObCartesianGeometrycollection *geo) = 0;
  virtual int finish(ObGeographGeometrycollection *geo) = 0;
  virtual int finish(ObCartesianMultipoint *geo) = 0;
  virtual int finish(ObGeographMultipoint *geo) = 0;
  virtual int finish(ObCartesianMultilinestring *geo) = 0;
  virtual int finish(ObGeographMultilinestring *geo) = 0;
  virtual int finish(ObCartesianMultipolygon *geo) = 0;
  virtual int finish(ObGeographMultipolygon *geo) = 0;

  virtual int finish(ObIWkbGeometry *geo) = 0;
  virtual int finish(ObIWkbGeogMultiPoint *geo) = 0;
  virtual int finish(ObIWkbGeomMultiPoint *geo) = 0;
  virtual int finish(ObIWkbGeogMultiLineString *geo) = 0;
  virtual int finish(ObIWkbGeomMultiLineString *geo) = 0;
  virtual int finish(ObIWkbGeogMultiPolygon *geo) = 0;
  virtual int finish(ObIWkbGeomMultiPolygon *geo) = 0;
  virtual int finish(ObIWkbGeogCollection *geo) = 0;
  virtual int finish(ObIWkbGeomCollection *geo) = 0;

  virtual bool set_after_visitor() = 0;
};

class ObEmptyGeoVisitor : public ObIGeoVisitor
{
public:
  ObEmptyGeoVisitor() = default;
  virtual ~ObEmptyGeoVisitor() = default;

  virtual bool prepare(ObGeometry *geo) override { UNUSED(geo); return false; }
  virtual bool prepare(ObCurve *geo) override { return prepare(static_cast<ObGeometry *>(geo)); }
  virtual bool prepare(ObSurface *geo) override { return prepare(static_cast<ObGeometry *>(geo)); }
  virtual bool prepare(ObLineString *geo) override { return prepare(static_cast<ObCurve *>(geo)); }
  virtual bool prepare(ObLinearring *geo) override { return prepare(static_cast<ObLineString *>(geo)); }
  virtual bool prepare(ObPolygon *geo) override { return prepare(static_cast<ObSurface *>(geo)); }
  virtual bool prepare(ObGeometrycollection *geo) override { return prepare(static_cast<ObGeometry *>(geo)); }
  virtual bool prepare(ObMultipoint *geo) override { return prepare(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool prepare(ObMulticurve *geo) override { return prepare(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool prepare(ObMultisurface *geo) override { return prepare(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool prepare(ObMultilinestring *geo) override { return prepare(static_cast<ObMulticurve *>(geo)); }
  virtual bool prepare(ObMultipolygon *geo) override { return prepare(static_cast<ObMultisurface *>(geo)); }

  virtual bool prepare(ObIWkbGeometry *geo) override { return prepare(static_cast<ObGeometry *>(geo)); }

  virtual int visit(ObGeometry *geo) override;
  virtual int visit(ObPoint *geo) override { return visit(static_cast<ObGeometry *>(geo)); }
  virtual int visit(ObCurve *geo) override { return visit(static_cast<ObGeometry *>(geo)); }
  virtual int visit(ObSurface *geo) override { return visit(static_cast<ObGeometry *>(geo)); }
  virtual int visit(ObLineString *geo) override { return visit(static_cast<ObCurve *>(geo)); }
  virtual int visit(ObLinearring *geo) override { return visit(static_cast<ObLineString *>(geo)); }
  virtual int visit(ObPolygon *geo) override { return visit(static_cast<ObSurface *>(geo)); }
  virtual int visit(ObGeometrycollection *geo) override { return visit(static_cast<ObGeometry *>(geo)); }
  virtual int visit(ObMultipoint *geo) override { return visit(static_cast<ObGeometrycollection *>(geo)); }
  virtual int visit(ObMulticurve *geo) override { return visit(static_cast<ObGeometrycollection *>(geo)); }
  virtual int visit(ObMultisurface *geo) override { return visit(static_cast<ObGeometrycollection *>(geo)); }
  virtual int visit(ObMultilinestring *geo) override { return visit(static_cast<ObMulticurve *>(geo)); }
  virtual int visit(ObMultipolygon *geo) override { return visit(static_cast<ObMultisurface *>(geo)); }

  virtual int visit(ObIWkbGeometry *geo) override { return visit(static_cast<ObGeometry *>(geo)); }
  virtual int visit(ObIWkbPoint *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }

  virtual bool is_end(ObGeometry *geo) override { UNUSED(geo); return OB_SUCCESS; }
  virtual bool is_end(ObPoint *geo) override { return is_end(static_cast<ObGeometry *>(geo)); }
  virtual bool is_end(ObCurve *geo) override { return is_end(static_cast<ObGeometry *>(geo)); }
  virtual bool is_end(ObSurface *geo) override { return is_end(static_cast<ObGeometry *>(geo)); }
  virtual bool is_end(ObLineString *geo) override { return is_end(static_cast<ObCurve *>(geo)); }
  virtual bool is_end(ObLinearring *geo) override { return is_end(static_cast<ObLineString *>(geo)); }
  virtual bool is_end(ObPolygon *geo) override { return is_end(static_cast<ObSurface *>(geo)); }
  virtual bool is_end(ObGeometrycollection *geo) override { return is_end(static_cast<ObGeometry *>(geo)); }
  virtual bool is_end(ObMultipoint *geo) override { return is_end(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool is_end(ObMulticurve *geo) override { return is_end(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool is_end(ObMultisurface *geo) override { return is_end(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool is_end(ObMultilinestring *geo) override { return is_end(static_cast<ObMulticurve *>(geo)); }
  virtual bool is_end(ObMultipolygon *geo) override { return is_end(static_cast<ObMultisurface *>(geo)); }

  virtual bool is_end(ObIWkbGeometry *geo) override { return is_end(static_cast<ObGeometry *>(geo)); }

  // for geo tree
  virtual bool prepare(ObCartesianLineString *geo) override { return prepare(static_cast<ObLineString *>(geo)); }
  virtual bool prepare(ObGeographLineString *geo) override { return prepare(static_cast<ObLineString *>(geo)); }
  virtual bool prepare(ObCartesianLinearring *geo) override { return prepare(static_cast<ObLinearring *>(geo)); }
  virtual bool prepare(ObGeographLinearring *geo) override { return prepare(static_cast<ObLinearring *>(geo)); }
  virtual bool prepare(ObCartesianGeometrycollection *geo) override { return prepare(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool prepare(ObGeographGeometrycollection *geo) override { return prepare(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool prepare(ObCartesianMultipoint *geo) override { return prepare(static_cast<ObMultipoint *>(geo)); }
  virtual bool prepare(ObGeographMultipoint *geo) override { return prepare(static_cast<ObMultipoint *>(geo)); }
  virtual bool prepare(ObCartesianMultilinestring *geo) override { return prepare(static_cast<ObMultilinestring *>(geo)); }
  virtual bool prepare(ObGeographMultilinestring *geo) override { return prepare(static_cast<ObMultilinestring *>(geo)); }
  virtual bool prepare(ObCartesianPolygon *geo) override { return prepare(static_cast<ObPolygon *>(geo)); }
  virtual bool prepare(ObGeographPolygon *geo) override { return prepare(static_cast<ObPolygon *>(geo)); }
  virtual bool prepare(ObCartesianMultipolygon *geo) override { return prepare(static_cast<ObMultipolygon *>(geo)); }
  virtual bool prepare(ObGeographMultipolygon *geo) override { return prepare(static_cast<ObMultipolygon *>(geo)); }

  virtual int visit(ObCartesianPoint *geo) override { return visit(static_cast<ObPoint *>(geo)); }
  virtual int visit(ObGeographPoint *geo) override { return visit(static_cast<ObPoint *>(geo)); }
  virtual int visit(ObCartesianLineString *geo) override { return visit(static_cast<ObLineString *>(geo)); }
  virtual int visit(ObGeographLineString *geo) override { return visit(static_cast<ObLineString *>(geo)); }
  virtual int visit(ObCartesianLinearring *geo) override { return visit(static_cast<ObLinearring *>(geo)); }
  virtual int visit(ObGeographLinearring *geo) override { return visit(static_cast<ObLinearring *>(geo)); }
  virtual int visit(ObCartesianGeometrycollection *geo) override { return visit(static_cast<ObGeometrycollection *>(geo)); }
  virtual int visit(ObGeographGeometrycollection *geo) override { return visit(static_cast<ObGeometrycollection *>(geo)); }
  virtual int visit(ObCartesianMultipoint *geo) override { return visit(static_cast<ObMultipoint *>(geo)); }
  virtual int visit(ObGeographMultipoint *geo) override { return visit(static_cast<ObMultipoint *>(geo)); }
  virtual int visit(ObCartesianMultilinestring *geo) override { return visit(static_cast<ObMultilinestring *>(geo)); }
  virtual int visit(ObGeographMultilinestring *geo) override { return visit(static_cast<ObMultilinestring *>(geo)); }
  virtual int visit(ObCartesianPolygon *geo) override { return visit(static_cast<ObPolygon *>(geo)); }
  virtual int visit(ObGeographPolygon *geo) override { return visit(static_cast<ObPolygon *>(geo)); }
  virtual int visit(ObCartesianMultipolygon *geo) override { return visit(static_cast<ObMultipolygon *>(geo)); }
  virtual int visit(ObGeographMultipolygon *geo) override { return visit(static_cast<ObMultipolygon *>(geo)); }

  virtual bool is_end(ObCartesianLineString *geo) override { return is_end(static_cast<ObLineString *>(geo)); }
  virtual bool is_end(ObGeographLineString *geo) override { return is_end(static_cast<ObLineString *>(geo)); }
  virtual bool is_end(ObCartesianLinearring *geo) override { return is_end(static_cast<ObLinearring *>(geo)); }
  virtual bool is_end(ObGeographLinearring *geo) override { return is_end(static_cast<ObLinearring *>(geo)); }
  virtual bool is_end(ObCartesianGeometrycollection *geo) override { return is_end(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool is_end(ObGeographGeometrycollection *geo) override { return is_end(static_cast<ObGeometrycollection *>(geo)); }
  virtual bool is_end(ObCartesianMultipoint *geo) override { return is_end(static_cast<ObMultipoint *>(geo)); }
  virtual bool is_end(ObGeographMultipoint *geo) override { return is_end(static_cast<ObMultipoint *>(geo)); }
  virtual bool is_end(ObCartesianMultilinestring *geo) override { return is_end(static_cast<ObMultilinestring *>(geo)); }
  virtual bool is_end(ObGeographMultilinestring *geo) override { return is_end(static_cast<ObMultilinestring *>(geo)); }
  virtual bool is_end(ObCartesianPolygon *geo) override { return is_end(static_cast<ObPolygon *>(geo)); }
  virtual bool is_end(ObGeographPolygon *geo) override { return is_end(static_cast<ObPolygon *>(geo)); }
  virtual bool is_end(ObCartesianMultipolygon *geo) override { return is_end(static_cast<ObMultipolygon *>(geo)); }
  virtual bool is_end(ObGeographMultipolygon *geo) override { return is_end(static_cast<ObMultipolygon *>(geo)); }

  // for geo binary
  virtual bool prepare(ObIWkbGeogMultiPoint *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomMultiPoint *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeogLineString *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomLineString *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeogLinearRing *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomLinearRing *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeogMultiLineString *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomMultiLineString *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeogPolygon *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomPolygon *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeogMultiPolygon *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomMultiPolygon *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeogCollection *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool prepare(ObIWkbGeomCollection *geo) override { return prepare(static_cast<ObIWkbGeometry *>(geo)); }

  virtual int visit(ObIWkbGeogPoint *geo) override { return visit(static_cast<ObIWkbPoint *>(geo)); }
  virtual int visit(ObIWkbGeomPoint *geo) override { return visit(static_cast<ObIWkbPoint *>(geo)); }
  virtual int visit(ObIWkbGeogMultiPoint *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomMultiPoint *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeogLineString *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomLineString *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeogLinearRing *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomLinearRing *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeogMultiLineString *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomMultiLineString *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeogPolygon *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomPolygon *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeogMultiPolygon *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomMultiPolygon *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeogCollection *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int visit(ObIWkbGeomCollection *geo) override { return visit(static_cast<ObIWkbGeometry *>(geo)); }

  virtual bool is_end(ObIWkbGeogMultiPoint *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomMultiPoint *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeogLineString *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomLineString *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeogLinearRing *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomLinearRing *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeogMultiLineString *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomMultiLineString *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeogPolygon *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomPolygon *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeogMultiPolygon *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomMultiPolygon *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeogCollection *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }
  virtual bool is_end(ObIWkbGeomCollection *geo) override { return is_end(static_cast<ObIWkbGeometry *>(geo)); }

  // currently just for collection
  virtual int finish(ObGeometry *geo) override { UNUSED(geo); return OB_SUCCESS; }
  virtual int finish(ObGeometrycollection *geo) override { return finish(static_cast<ObGeometry *>(geo)); }
  virtual int finish(ObMultisurface *geo) override { return finish(static_cast<ObGeometrycollection *>(geo)); }
  virtual int finish(ObMulticurve *geo) override { return finish(static_cast<ObGeometrycollection *>(geo)); }
  virtual int finish(ObMultipoint *geo) override { return finish(static_cast<ObGeometrycollection *>(geo)); }
  virtual int finish(ObMultilinestring *geo) override { return finish(static_cast<ObMulticurve *>(geo)); }
  virtual int finish(ObMultipolygon *geo) override { return finish(static_cast<ObMultisurface *>(geo)); }
  virtual int finish(ObCartesianGeometrycollection *geo) override { return finish(static_cast<ObGeometrycollection *>(geo)); }
  virtual int finish(ObGeographGeometrycollection *geo) override { return finish(static_cast<ObGeometrycollection *>(geo)); }
  virtual int finish(ObCartesianMultipoint *geo) override { return finish(static_cast<ObMultipoint *>(geo)); }
  virtual int finish(ObGeographMultipoint *geo) override { return finish(static_cast<ObMultipoint *>(geo)); }
  virtual int finish(ObCartesianMultilinestring *geo) override { return finish(static_cast<ObMultilinestring *>(geo)); }
  virtual int finish(ObGeographMultilinestring *geo) override { return finish(static_cast<ObMultilinestring *>(geo)); }
  virtual int finish(ObCartesianMultipolygon *geo) override { return finish(static_cast<ObMultipolygon *>(geo)); }
  virtual int finish(ObGeographMultipolygon *geo) override { return finish(static_cast<ObMultipolygon *>(geo)); }

  virtual int finish(ObIWkbGeometry *geo) override { return finish(static_cast<ObGeometry *>(geo)); }
  virtual int finish(ObIWkbGeogMultiPoint *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeomMultiPoint *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeogMultiLineString *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeomMultiLineString *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeogMultiPolygon *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeomMultiPolygon *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeogCollection *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  virtual int finish(ObIWkbGeomCollection *geo) override { return finish(static_cast<ObIWkbGeometry *>(geo)); }
  // set true if the value of a point might be modified during linestring_do_visitor
  virtual bool set_after_visitor() override { return true; }
};

class ObIWkbVisitorImplement {
public:
  template<typename T_IBIN, typename T_BIN, typename T_IPOINT, typename T_POINT>
  static int linestring_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor);

  template<typename T_IBIN, typename T_BIN, typename T_IPOINT, typename T_POINT>
  static int multipoint_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor);

  template<typename T_IBIN, typename T_BIN, typename T_IRING, typename T_RING, typename T_INNER_RING>
  static int polygon_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor);

  template<typename T_IBIN, typename T_BIN, typename T_IITEM, typename T_ITEM>
  static int collection_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor);

};

template<typename T_IBIN, typename T_BIN, typename T_IPOINT, typename T_POINT>
int ObIWkbVisitorImplement::linestring_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor)
{
  INIT_SUCC(ret);
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      OB_LOG(WARN,"failed to do wkb line string visit", K(ret));
    } else if (visitor.is_end(geo) || geo->is_empty()) {
      // do nothing
    } else {
      const T_BIN *line = reinterpret_cast<const T_BIN*>(geo->val());
      // caution: T_POINT Bytes not aligned, wkb_point is just used as stack memory, can't read directly
      T_POINT wkb_point;
      wkb_point.byteorder(ObGeoWkbByteOrder::LittleEndian);
      typename T_BIN::iterator iter = line->begin();
      typename T_BIN::iterator iter_end = line->end();
      bool need_set = visitor.set_after_visitor();
      T_IPOINT point;
      for ( ; iter != iter_end && OB_SUCC(ret) && !visitor.is_end(geo); ++iter) {
        wkb_point.template set<0>(iter->get_x());
        wkb_point.template set<1>(iter->get_y());
        ObString data(sizeof(wkb_point), reinterpret_cast<char *>(&wkb_point));
        point.set_data(data);
        if (OB_FAIL(point.do_visit(visitor))) {
          OB_LOG(WARN,"failed to do wkb point visit", K(ret));
        } else if (need_set) {
          iter->template set<0>(point.x());
          iter->template set<1>(point.y());
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.finish(geo))) {
      OB_LOG(WARN,"failed to finish visit", K(ret));
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IPOINT, typename T_POINT>
int ObIWkbVisitorImplement::multipoint_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor)
{
  INIT_SUCC(ret);
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      OB_LOG(WARN,"failed to do multi point visit", K(ret));
    } else if (visitor.is_end(geo) || geo->is_empty()) {
      // do nothing
    } else {
      const T_BIN *multi_point = reinterpret_cast<const T_BIN*>(geo->val());
      typename T_BIN::iterator iter = multi_point->begin();
      T_IPOINT point;
      for ( ; iter != multi_point->end() && OB_SUCC(ret) && !visitor.is_end(geo); ++iter) {
        ObString data(sizeof(T_POINT), reinterpret_cast<char *>(iter.operator->()));
        point.set_data(data);
        if (OB_FAIL(point.do_visit(visitor))) {
          OB_LOG(WARN,"failed to do point visit", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.finish(geo))) {
      OB_LOG(WARN,"failed to finish visit", K(ret));
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IRING, typename T_RING, typename T_INNER_RING>
int ObIWkbVisitorImplement::polygon_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor)
{
  INIT_SUCC(ret);
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      OB_LOG(WARN,"failed to do wkb polygon visit", K(ret));
    } else if (visitor.is_end(geo) || geo->size() == 0) {
      // do nothing
    } else {
      const T_BIN *polygon = reinterpret_cast<const T_BIN*>(geo->val());
      T_IRING ring;
      ObString data(sizeof(T_RING), reinterpret_cast<const char *>(&polygon->exterior_ring()));
      ring.set_data(data);
      if (OB_FAIL(ring.do_visit(visitor))) {
        OB_LOG(WARN,"failed to do geog polygon exterior ring visit", K(ret));
      } else {
        const T_INNER_RING &rings = polygon->inner_rings();
        typename T_INNER_RING::iterator iter = rings.begin();
        for ( ; iter != rings.end() && OB_SUCC(ret) && !visitor.is_end(geo); ++iter) {
          data.assign_ptr(reinterpret_cast<const char *>(iter.operator->()), sizeof(T_RING));
          ring.set_data(data);
          if (OB_FAIL(ring.do_visit(visitor))) {
            OB_LOG(WARN,"failed to do geog polygon inner ring visit", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_IITEM, typename T_ITEM>
int ObIWkbVisitorImplement::collection_do_visitor(T_IBIN *geo, ObIGeoVisitor &visitor)
{
  INIT_SUCC(ret);
  if (visitor.prepare(geo)) {
    if (OB_FAIL(visitor.visit(geo))) {
      OB_LOG(WARN,"failed to do wkb multi visit", K(ret));
    } else if (visitor.is_end(geo) || geo->is_empty()) {
      // do nothing
    } else {
      const T_BIN *items = reinterpret_cast<const T_BIN*>(geo->val());
      typename T_BIN::iterator iter = items->begin();
      T_IITEM item;
      for ( ; iter != items->end() && OB_SUCC(ret) && !visitor.is_end(geo); ++iter) {
        ObString data(sizeof(T_ITEM), reinterpret_cast<char *>(iter.operator->()));
        item.set_data(data);
        if (OB_FAIL(item.do_visit(visitor))) {
          OB_LOG(WARN,"failed to do wkb item visit", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.finish(geo))) {
      OB_LOG(WARN,"failed to finish visit", K(ret));
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase

#endif