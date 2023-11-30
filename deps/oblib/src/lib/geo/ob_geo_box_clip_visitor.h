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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_BOX_CLIP_VISITOR_
#define OCEANBASE_LIB_GEO_OB_GEO_BOX_CLIP_VISITOR_

#include "lib/geo/ob_geo_visitor.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_common.h"

namespace oceanbase
{
namespace common
{
enum ObBoxPosition {
  INVALID = 0,
  INSIDE = 1,
  OUTSIDE = 2,
  LEFT_EDGE = 4,
  RIGHT_EDGE = 8,
  TOP_EDGE = 16,
  BOTTOM_EDGE = 32,
  TOPLEFT_CORNER = TOP_EDGE | LEFT_EDGE,
  TOPRIGHT_CORNER = TOP_EDGE | RIGHT_EDGE,
  BOTTOMLEFT_CORNER = BOTTOM_EDGE | LEFT_EDGE,
  BOTTOMRIGHT_CORNER = BOTTOM_EDGE | RIGHT_EDGE,
};

class ObGeoBoxClipVisitor : public ObEmptyGeoVisitor
{
public:
  explicit ObGeoBoxClipVisitor(const ObGeogBox &box, ObIAllocator &allocator)
      : xmin_(box.xmin),
        ymin_(box.ymin),
        xmax_(box.xmax),
        ymax_(box.ymax),
        res_geo_(nullptr),
        allocator_(&allocator)
  {}
  virtual ~ObGeoBoxClipVisitor()
  {}

  bool prepare(ObGeometry *geo) override;

  int visit(ObCartesianPoint *geo) override;
  int visit(ObCartesianLineString *geo) override;
  int visit(ObCartesianPolygon *geo) override;
  int visit(ObCartesianMultipoint *geo) override;
  int visit(ObCartesianMultilinestring *geo) override;
  int visit(ObCartesianMultipolygon *geo) override;
  int visit(ObCartesianGeometrycollection *geo)
  {
    UNUSED(geo);
    return OB_SUCCESS;
  }

  bool is_end(ObCartesianLineString *geo) override
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObCartesianPolygon *geo) override
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObCartesianMultipoint *geo) override
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObCartesianMultilinestring *geo) override
  {
    UNUSED(geo);
    return true;
  }
  bool is_end(ObCartesianMultipolygon *geo) override
  {
    UNUSED(geo);
    return true;
  }

  int finish(ObGeometry *geo) override
  {
    UNUSED(geo);
    return OB_SUCCESS;
  }

  int get_geometry(ObGeometry *&geo);

private:
  bool same_edge_positions(ObBoxPosition pos1, ObBoxPosition pos2);
  ObBoxPosition get_position(double x, double y);
  bool is_point_inside(double x, double y);
  void clip_point_to_single_edge(
      ObWkbGeomInnerPoint &out_pt, const ObWkbGeomInnerPoint &in_pt, double edge, bool is_x_edge);
  void clip_point_to_edges(
      const ObWkbGeomInnerPoint &out_pt, const ObWkbGeomInnerPoint &in_pt, ObWkbGeomInnerPoint &pt);
  int line_visit(
      const ObCartesianLineString &line, ObCartesianMultilinestring *&mls, bool &completely_inside);
  int line_visit_outside(const ObCartesianLineString &line, int &idx, ObCartesianMultilinestring *mls,
      ObBoxPosition pos, ObCartesianLineString &new_line);
  int line_visit_inside_or_edge(const ObCartesianLineString &line, int &idx,
      ObCartesianMultilinestring *mls, ObBoxPosition pos, ObCartesianLineString &new_line,
      bool &completely_inside);

  int visit_polygon(ObCartesianPolygon &poly, ObCartesianMultipolygon *&mpy);
  int reconnect_multi_line(ObCartesianMultilinestring &mls);
  int box_to_linearring(ObCartesianLinearring &ring);
  int make_polygons(ObCartesianMultilinestring &mls, ObCartesianMultipolygon &mpy,
      ObCartesianMultipolygon &new_mpy);
  int distance(double x1, double y1, double x2, double y2, double &dist);
  void to_next_edge(ObBoxPosition &pos);
  int close_ring(ObCartesianLinearring &ring, double x1, double y1, double x2, double y2);
  void reverse_ring(ObCartesianLineString &ring, uint32_t start, uint32_t end);
  void reorder_ring(ObCartesianLinearring &ring);
  void reverse_mls(ObCartesianMultilinestring &mls);
  ObGeoType get_result_basic_type();
  bool is_ring_ccw(ObCartesianLineString &ring);
  int construct_intersect_line(const ObCartesianLineString &line, int32_t first_inside_idx,
      int32_t last_idx, ObCartesianLineString &intersect_line);
  int visit_polygon_ext_ring(const ObCartesianLinearring &ext_ring, bool &is_inner_inside,
      bool &is_ext_inside, ObCartesianMultilinestring *&ext_mls);
  int visit_polygon_inner_ring(
      const ObCartesianLinearring &inner_ring, ObCartesianMultipolygon &ext_mpy, ObCartesianMultilinestring &ext_mls, bool &is_within);
  int make_polygon_ext_ring(ObCartesianMultilinestring &mls, ObCartesianMultipolygon &new_mpy);

  double xmin_;
  double ymin_;
  double xmax_;
  double ymax_;
  ObCartesianGeometrycollection *res_geo_;
  ObIAllocator *allocator_;
  bool keep_polygon_;
  DISALLOW_COPY_AND_ASSIGN(ObGeoBoxClipVisitor);
};

}  // namespace common
}  // namespace oceanbase
#endif