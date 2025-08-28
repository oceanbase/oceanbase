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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_TO_JSON_BIN_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_TO_JSON_BIN_VISITOR_
#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/xml/ob_multi_mode_bin.h"

namespace oceanbase
{
namespace common
{
class ObWkbToJsonBinVisitor : public ObEmptyGeoVisitor
{
public:
  static const int MAX_DIGITS_IN_DOUBLE = 25;
  explicit ObWkbToJsonBinVisitor(
      ObIAllocator *allocator,
      uint32_t max_dec_digits = UINT_MAX32,
      uint8_t flag = 0,
      const ObGeoSrid srid = 0);

  ~ObWkbToJsonBinVisitor() {}
  bool prepare(ObGeometry *geo) { UNUSED(geo); return true; }
  int visit(ObIWkbGeogPoint *geo);
  int visit(ObIWkbGeomPoint *geo);
  int visit(ObIWkbGeogLineString *geo);
  int visit(ObIWkbGeomLineString *geo);  
  int visit(ObIWkbGeogMultiPoint *geo);  
  int visit(ObIWkbGeomMultiPoint *geo);
  int visit(ObIWkbGeogMultiLineString *geo);
  int visit(ObIWkbGeomMultiLineString *geo);
  int visit(ObIWkbGeogPolygon *geo);
  int visit(ObIWkbGeomPolygon *geo);
  int visit(ObIWkbGeogMultiPolygon *geo);
  int visit(ObIWkbGeomMultiPolygon *geo);
  int visit(ObIWkbGeogCollection *geo);
  int visit(ObIWkbGeomCollection *geo);

  bool is_end(ObGeometry *geo) { UNUSED(geo); return true; }
  int to_jsonbin(ObGeometry *geo, ObString &geojsonbin);

  void reset();
private:
  // for Point
  int appendCoordinatePoint(double x, double y, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx);
  template<typename T_IBIN>
  int appendPointObj(T_IBIN *geo, const ObString &type_name);
  // for LineString
  template<typename T_BIN>
  int appendLine(const T_BIN *line, ObJsonBin &bin, uint64_t &start_pos, uint64_t &val_idx);
  template<typename T_IBIN, typename T_BIN>
  int appendLineObj(T_IBIN *geo, const ObString &type_name);
  template<typename T_IBIN, typename T_BIN, typename T_BIN_LINE>
  int appendMultiLineObj(T_IBIN *geo, const ObString &type_name);
  // for Polygon
  template<typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
  int appendPolygon(const T_BIN *poly, ObJsonBin &bin, uint64_t &start_pos, uint64_t &val_idx);
  template<typename T_IBIN, typename T_BIN, typename T_BIN_RING, typename T_BIN_INNER_RING>
  int appendPolygonObj(T_IBIN *geo, const ObString &type_name);
  template<typename T_IBIN, typename T_BIN, typename T_BIN_POLY, typename T_BIN_RING, typename T_BIN_INNER_RING>
  int appendMultiPolygonObj(T_IBIN *geo, const ObString &type_name);
  // for Collection
  template<
      typename T_IPOINT,
      typename T_IMULTIPOINT,
      typename T_ILINE,
      typename T_IMULTILINE,
      typename T_IPOLY,
      typename T_IMULTIPOLY, 
      typename T_ICOLLC,
      typename T_POINT,
      typename T_MULTIPOINT,
      typename T_LINE,
      typename T_MULTILINE,
      typename T_POLY,
      typename T_LINEARRING,
      typename T_INNERRING,
      typename T_MULTIPOLY,
      typename T_COLLC>
  int appendCollectionSub(
      typename T_COLLC::const_pointer sub_ptr,
      const T_COLLC *collection,
      ObJsonBin &bin,
      uint64_t &start_pos,
      uint64_t &val_idx);

  int appendCollectionSubWrapper(
      ObWkbGeogCollection::const_pointer sub_ptr,
      const ObWkbGeogCollection *collection,
      ObJsonBin &bin,
      uint64_t &start_pos,
      uint64_t &val_idx);
  int appendCollectionSubWrapper(
      ObWkbGeomCollection::const_pointer sub_ptr,
      const ObWkbGeomCollection *collection,
      ObJsonBin &bin,
      uint64_t &start_pos,
      uint64_t &val_idx);
  template<typename T_IBIN, typename T_BIN>
  int appendCollectionObj(T_IBIN *geo, const ObString &type_name);
  // common
  int appendJsonCommon(
      const ObString type_name,
      ObGeometry *geo,
      ObJsonBin &bin,
      uint64_t &start_pos,
      uint64_t &val_idx);
  int appendMeta(ObJsonBin &bin, uint64_t &start_pos, int type_flag, uint64_t element_count, uint64_t size_size = 3);
  int appendObjKey(const ObString key_str, ObJsonBin &bin, uint64_t &start_pos, int &key_idx);
  int fillHeaderSize(ObJsonBin &bin, uint64_t start_pos);
  int appendCrs(ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx);
  int appendCrsProp(ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx);
  int appendBbox(ObGeometry *geo, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx);
  int appendString(const ObString &str, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx);
  int appendDouble(double value, ObJsonBin &bin, uint64_t start_pos, uint64_t &val_idx);
  int appendArrayHeader(
      ObJsonBin &bin,
      uint64_t start_pos,
      uint64_t val_idx,
      uint64_t size,
      ObJsonBin &array_bin,
      uint64_t &array_start_pos);

  ObJsonBuffer json_buf_;
  uint8_t flag_;
  ObGeoSrid srid_;
  ObIAllocator *allocator_;
  uint32_t max_dec_digits_;
  bool is_appendCrs;

  DISALLOW_COPY_AND_ASSIGN(ObWkbToJsonBinVisitor);
};

} // namespace common
} // namespace oceanbase

#endif