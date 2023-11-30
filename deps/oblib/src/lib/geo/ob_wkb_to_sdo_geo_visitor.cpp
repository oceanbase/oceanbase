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
#define USING_LOG_PREFIX LIB
#include "ob_wkb_to_sdo_geo_visitor.h"

namespace oceanbase
{
namespace common
{

template<typename T_IBIN>
int ObWkbToSdoGeoVisitor::append_point(T_IBIN *geo)
{
  INIT_SUCC(ret);
  // collection_visit may include both POINT(multi_visit = false) and MULTIPOINT(multi_visit = true)
  if (is_multi_visit_ || is_collection_visit_) {
    // elem_info = (start_idx, 1, 1)
    if (!is_multi_visit_ && OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 1, 1))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
    } else if (OB_FAIL(append_inner_point(geo->x(), geo->y()))) {
      LOG_WARN("fail to append inner point", K(ret), K(geo->x()), K(geo->y()));
    }
  } else {
    // point = (x, y, null)
    double x = geo->x();
    double y = geo->y();
    sdo_geo_->set_gtype(geo->type());
    if (std::isnan(x) || std::isnan(y) || std::isinf(x) || std::isinf(y)) {
      ret = OB_ERR_INVALID_NULL_SDO_GEOMETRY;
      LOG_WARN("x coordinate or y coordinate is NAN or INF", K(ret), K(x), K(y));
    } else if (!is_valid_to_represent(x) || !is_valid_to_represent(y)) {
      ret = OB_NUMERIC_OVERFLOW;
      LOG_WARN("number is out of range", K(ret), K(x), K(y));
    } else {
      sdo_geo_->get_point().set_x(x);
      sdo_geo_->get_point().set_y(y);
    }
  }
  return ret;
}

bool ObWkbToSdoGeoVisitor::is_valid_to_represent(double num)
{
  bool is_valid = false;
  if (num == 0) {
    is_valid = true;
  } else if (num >= MIN_NUMBER_ORACLE && num < MAX_NUMBER_ORACLE) {
    is_valid = true;
  } else if (num <= (-MIN_NUMBER_ORACLE) && num > (-MAX_NUMBER_ORACLE)) {
    is_valid = true;
  }
  return is_valid;
}

int ObWkbToSdoGeoVisitor::append_inner_point(double x, double y)
{
  INIT_SUCC(ret);
  if (!is_valid_to_represent(x)) {
    ret = OB_NUMERIC_OVERFLOW;
    LOG_WARN("number is out of range", K(ret), K(x));
  } else if (OB_FAIL(sdo_geo_->append_ori(x))) {
    LOG_WARN("fail to append sdo_elem_info", K(ret));
  } else if (!is_valid_to_represent(y)) {
    ret = OB_NUMERIC_OVERFLOW;
    LOG_WARN("number is out of range", K(ret), K(y));
  } else if (OB_FAIL(sdo_geo_->append_ori(y))) {
    LOG_WARN("fail to append sdo_elem_info", K(ret));
  }

  return ret;
}

int ObWkbToSdoGeoVisitor::append_elem_info(uint64_t offset, uint64_t etype, uint64_t interpretation)
{
  INIT_SUCC(ret);
  if (OB_FAIL(sdo_geo_->append_elem(offset))) {
    LOG_WARN("fail to append offset to sdo_elem_info", K(ret), K(offset));
  } else if (OB_FAIL(sdo_geo_->append_elem(etype))) {
    LOG_WARN("fail to append etype to sdo_elem_info", K(ret), K(etype));
  } else if (OB_FAIL(sdo_geo_->append_elem(interpretation))) {
    LOG_WARN("fail to append interpretation to sdo_elem_info", K(ret), K(interpretation));
  }
  return ret;
}

template<typename T_IBIN, typename T_BIN>
int ObWkbToSdoGeoVisitor::append_linestring(T_IBIN *geo)
{
  INIT_SUCC(ret);
  if (!is_multi_visit_ && !is_collection_visit_) {
    sdo_geo_->set_gtype(geo->type());
  }

  // elem_info = (start_idx, 2, 1)
  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(ret), K(geo->length()));
  } else if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 2, 1))) {
    LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
  } else {
    const T_BIN *line = reinterpret_cast<const T_BIN *>(geo->val());
    typename T_BIN::iterator iter = line->begin();
    for (; OB_SUCC(ret) && iter != line->end(); iter++) {
      // (x, y)
      if (OB_FAIL(append_inner_point(iter->template get<0>(), iter->template get<1>()))) {
        LOG_WARN("fail to append_inner_point",
            K(ret),
            K(iter->template get<0>()),
            K(iter->template get<1>()));
      }
    }
  }

  return ret;
}

template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
int ObWkbToSdoGeoVisitor::append_polygon(T_IBIN *geo)
{
  INIT_SUCC(ret);

  if (!is_multi_visit_ && !is_collection_visit_) {
    sdo_geo_->set_gtype(geo->type());
  }

  if (geo->length() < WKB_COMMON_WKB_HEADER_LEN) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid wkb length", K(ret), K(geo->length()));
  } else {
    T_BIN &poly = *(T_BIN *)(geo->val());
    T_BIN_RING &exterior = poly.exterior_ring();
    T_BIN_INNER_RING &inner_rings = poly.inner_rings();
    if (poly.size() == 0) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("invalid poly size", K(ret), K(poly.size()));
    } else if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 1003, 1))) {
      LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
    } else {
      typename T_BIN_RING::iterator ext_iter = exterior.begin();
      for (; OB_SUCC(ret) && ext_iter != exterior.end(); ++ext_iter) {
        if (OB_FAIL(append_inner_point(ext_iter->template get<0>(), ext_iter->template get<1>()))) {
          LOG_WARN("fail to append_inner_point",
              K(ret),
              K(ext_iter->template get<0>()),
              K(ext_iter->template get<1>()));
        }
      }

      typename T_BIN_INNER_RING::iterator inner_iter = inner_rings.begin();
      for (; OB_SUCC(ret) && inner_iter != inner_rings.end(); ++inner_iter) {
        // uint32_t size = inner_iter->size();
        if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 2003, 1))) {
          LOG_WARN("fail to append sdo_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()));
        }
        typename T_BIN_RING::iterator iter = (*inner_iter).begin();
        for (; OB_SUCC(ret) && iter != (*inner_iter).end(); ++iter) {
          if (OB_FAIL(append_inner_point(iter->template get<0>(), iter->template get<1>()))) {
            LOG_WARN("fail to append_inner_point",
                K(ret),
                K(iter->template get<0>()),
                K(iter->template get<1>()));
          }
        }
      }
    }
  }

  return ret;
}

template<typename T_IBIN>
void ObWkbToSdoGeoVisitor::append_gtype(T_IBIN *geo)
{
  if (!is_collection_visit_ || geo->type() == ObGeoType::GEOMETRYCOLLECTION) {
    sdo_geo_->set_gtype(geo->type());
  }
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeomLineString *geo)
{
  UNUSED(geo);
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeomPolygon *geo)
{
  UNUSED(geo);
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeomMultiPoint *geo)
{
  UNUSED(geo);
  is_multi_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeomMultiLineString *geo)
{
  UNUSED(geo);
  is_multi_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeomMultiPolygon *geo)
{
  UNUSED(geo);
  is_multi_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeomCollection *geo)
{
  UNUSED(geo);
  is_collection_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeogLineString *geo)
{
  UNUSED(geo);
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeogPolygon *geo)
{
  UNUSED(geo);
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeogMultiPoint *geo)
{
  UNUSED(geo);
  is_multi_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeogMultiLineString *geo)
{
  UNUSED(geo);
  is_multi_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeogMultiPolygon *geo)
{
  UNUSED(geo);
  is_multi_visit_ = true;
  return true;
}

bool ObWkbToSdoGeoVisitor::prepare(ObIWkbGeogCollection *geo)
{
  UNUSED(geo);
  is_collection_visit_ = true;
  return true;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((append_point<ObIWkbGeomPoint>(geo)))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((append_linestring<ObIWkbGeomLineString, ObWkbGeomLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((append_polygon<ObIWkbGeomPolygon,
          ObWkbGeomPolygon,
          ObWkbGeomLinearRing,
          ObWkbGeomPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomMultiPoint *geo)
{
  INIT_SUCC(ret);
  append_gtype(geo);
  if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 1, geo->size()))) {
    LOG_WARN(
        "fail to append_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()), K(geo->size()));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomMultiLineString *geo)
{
  append_gtype(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomMultiPolygon *geo)
{
  append_gtype(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeomCollection *geo)
{
  append_gtype(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogPoint *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((append_point<ObIWkbGeogPoint>(geo)))) {
    LOG_WARN("fail to append point", K(ret));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogLineString *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((append_linestring<ObIWkbGeogLineString, ObWkbGeogLineString>(geo)))) {
    LOG_WARN("fail to append line", K(ret));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogPolygon *geo)
{
  INIT_SUCC(ret);
  if (OB_FAIL((append_polygon<ObIWkbGeogPolygon,
          ObWkbGeogPolygon,
          ObWkbGeogLinearRing,
          ObWkbGeogPolygonInnerRings>(geo)))) {
    LOG_WARN("fail to append polygon", K(ret));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogMultiPoint *geo)
{
  INIT_SUCC(ret);
  append_gtype(geo);
  if (OB_FAIL(append_elem_info(sdo_geo_->get_ordinates().size() + 1, 1, geo->size()))) {
    LOG_WARN(
        "fail to append_elem_info", K(ret), K(sdo_geo_->get_ordinates().size()), K(geo->size()));
  }
  return ret;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogMultiLineString *geo)
{
  append_gtype(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogMultiPolygon *geo)
{
  append_gtype(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::visit(ObIWkbGeogCollection *geo)
{
  append_gtype(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeomMultiPoint *geo)
{
  is_multi_visit_ = false;
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeomMultiLineString *geo)
{
  UNUSED(geo);
  is_multi_visit_ = false;
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeomMultiPolygon *geo)
{
  UNUSED(geo);
  is_multi_visit_ = false;
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeomCollection *geo)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeogMultiPoint *geo)
{
  is_multi_visit_ = false;
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeogMultiLineString *geo)
{
  UNUSED(geo);
  is_multi_visit_ = false;
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeogMultiPolygon *geo)
{
  UNUSED(geo);
  is_multi_visit_ = false;
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::finish(ObIWkbGeogCollection *geo)
{
  UNUSED(geo);
  return OB_SUCCESS;
}

int ObWkbToSdoGeoVisitor::init(ObSdoGeoObject *geo, uint32_t srid)
{
  INIT_SUCC(ret);
  sdo_geo_ = geo;
  if (OB_ISNULL(sdo_geo_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSdoGeoObject ptr is null", K(ret));
  } else {
    sdo_geo_->set_srid(srid);
  }

  return ret;
}
}  // namespace common
}  // namespace oceanbase