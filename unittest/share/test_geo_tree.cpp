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

#include <gtest/gtest.h>
#include <boost/geometry.hpp>
#include <boost/foreach.hpp>
#define private public
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_tree_traits.h"
#include "lib/geo/ob_geo_bin_traits.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/random/ob_random.h"
#undef private

#include <sys/time.h>
#include <stdexcept>
#include <exception>
#include <typeinfo>

namespace bg = boost::geometry;

namespace oceanbase {
namespace common {

class TestGeoTree : public ::testing::Test {
public:
  TestGeoTree()
  {}
  ~TestGeoTree()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestGeoTree);

};

TEST_F(TestGeoTree, iters)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ModulePageAllocator page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObGeomVector<int> gv(page_allocator_);
  const ObGeomVector<int> &c_gv = gv;

  gv.push_back(0);
  gv.push_back(1);
  gv.push_back(2);
  gv.push_back(3);
  gv.push_back(4);
  gv.push_back(5);
  gv.push_back(6);
  gv.push_back(7);
  gv.push_back(8);
  gv.push_back(9);

  ASSERT_EQ(gv.size(), 10);
  ObGeomVector<int>::iterator iter = gv.begin();
  ObGeomVector<int>::const_iterator c_iter = c_gv.begin();
  ObGeomVector<int>::iterator begin = gv.begin();
  ObGeomVector<int>::const_iterator c_begin = c_gv.begin();
  ObGeomVector<int>::iterator end = gv.end();
  ObGeomVector<int>::const_iterator c_end = c_gv.end();

  ObGeomVector<int>::iterator tmp_iter;
  ObGeomVector<int>::const_iterator tmp_c_iter;

  // test ++, --
  ASSERT_EQ(iter, begin);
  ASSERT_EQ(c_iter, c_begin);
  ASSERT_NE(iter, end);
  ASSERT_NE(c_iter, c_end);
  int i = 0;
  for (; i < 10; i++) {
    ASSERT_EQ(*iter, i);
    ASSERT_EQ(*c_iter, i);
    iter++;
    c_iter++;
  }
  ASSERT_EQ(iter, end);
  ASSERT_EQ(c_iter, c_end);

  iter = begin;
  c_iter = c_begin;

  for (i = 0; i < 10; i++) {
    ASSERT_EQ(*iter, i);
    ASSERT_EQ(*c_iter, i);
    iter++;
    c_iter++;
  }
  ASSERT_EQ(iter, end);
  ASSERT_EQ(c_iter, c_end);
  ASSERT_EQ(i, 10);
  i--;
  iter--;
  c_iter--;
  for (; i > 0; i--) {
    ASSERT_EQ(*iter, i);
    ASSERT_EQ(*c_iter, i);
    iter--;
    c_iter--;
  }
  ASSERT_EQ(iter, begin);
  ASSERT_EQ(c_iter, c_begin);

  tmp_iter = iter + 3;
  tmp_c_iter = c_iter + 3;

  ASSERT_EQ(*tmp_iter, 3);
  ASSERT_EQ(*tmp_c_iter, 3);

  tmp_iter = 4 + iter;
  tmp_c_iter = 4 + c_iter;

  ASSERT_EQ(*tmp_iter, 4);
  ASSERT_EQ(*tmp_c_iter, 4);

  tmp_iter = end - 3;
  tmp_c_iter = c_end - 3;
  ASSERT_EQ(*tmp_iter, *tmp_c_iter);
  ASSERT_EQ(*tmp_iter, 7);
  ASSERT_EQ(*tmp_c_iter, 7);

  tmp_iter += 2;
  tmp_c_iter += 2;
  ASSERT_EQ(*tmp_iter, *tmp_c_iter);
  ASSERT_EQ(*tmp_iter, 9);
  ASSERT_EQ(*tmp_c_iter, 9);

  tmp_iter -= 2;
  tmp_c_iter -= 2;
  ASSERT_EQ(*tmp_iter, *tmp_c_iter);
  ASSERT_EQ(*tmp_iter, 7);
  ASSERT_EQ(*tmp_c_iter, 7);

  tmp_iter = ++iter;
  tmp_c_iter = ++c_iter;
  ASSERT_EQ(*tmp_iter, *tmp_c_iter);
  ASSERT_EQ(*tmp_iter, 1);
  ASSERT_EQ(*tmp_c_iter, 1);

  tmp_iter = iter++;
  tmp_c_iter = c_iter++;
  ASSERT_EQ(*tmp_iter, *tmp_c_iter);
  ASSERT_EQ(*tmp_iter, 1);
  ASSERT_EQ(*tmp_c_iter, 1);
  ASSERT_EQ(*iter, *c_iter);
  ASSERT_EQ(*iter, 2);
  ASSERT_EQ(*c_iter, 2);

  ASSERT_EQ(c_end - c_begin, end - begin);
  ASSERT_TRUE(c_end != c_begin);
  ASSERT_TRUE(end != begin);

  ASSERT_FALSE((c_end < c_begin));
  ASSERT_FALSE(end < begin);

  ASSERT_TRUE(begin < end);
  ASSERT_TRUE(c_begin < c_end);
}

TEST_F(TestGeoTree, ObGeomVector)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ModulePageAllocator page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObGeomVector<int> gv(page_allocator_);
  ASSERT_TRUE(gv.empty());

  gv.push_back(0);
  gv.push_back(1);
  gv.push_back(2);
  gv.push_back(3);
  gv.push_back(4);
  gv.push_back(5);
  gv.push_back(6);
  gv.push_back(7);
  gv.push_back(8);
  gv.push_back(9);
  const ObGeomVector<int> &c_gv = gv;
  ASSERT_EQ(c_gv.front(), 0);
  ASSERT_EQ(c_gv.back(), 9);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(c_gv[i], i);
  }
  ASSERT_FALSE(gv.empty());
  ASSERT_EQ(gv.size(), 10);
  gv.pop_front();
  ASSERT_EQ(gv.front(), 1);
  gv.pop_front();
  ASSERT_EQ(gv.front(), 2);
  ASSERT_EQ(gv.back(), 9);
  gv.resize(3);
  ASSERT_EQ(gv.size(), 3);
  ASSERT_EQ(gv.front(), 2);
  ASSERT_EQ(gv[1], 3);
  ASSERT_EQ(gv.back(), 4);
  gv.resize(10);
  ASSERT_EQ(gv.size(), 10);
  gv.clear();
  ASSERT_TRUE(gv.empty());
}

TEST_F(TestGeoTree, point)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObCartesianPoint p(1.12, 2.32, 0, &allocator);
  ASSERT_EQ(1.12, p.get<0>());
  ASSERT_EQ(2.32, p.get<1>());
  ASSERT_EQ(ObGeoCRS::Cartesian, p.crs());
  p.set<0>(3.321);
  p.set<1>(4.444);
  ASSERT_EQ(3.321, p.get<0>());
  ASSERT_EQ(4.444, p.get<1>());

  p.x(-3.123);
  p.y(-4.567);
  ASSERT_EQ(-3.123, p.get<0>());
  ASSERT_EQ(-4.567, p.get<1>());

  p.x(1.7976931348623157e308);
  p.y(-1.7976931348623157e308);
  EXPECT_EQ(1.7976931348623157e308, p.x());
  EXPECT_EQ(-1.7976931348623157e308, p.y());

  ASSERT_FALSE(p.is_empty());
  ObCartesianPoint p1(0, &allocator);
  ASSERT_TRUE(std::isnan(p1.x()));
  ASSERT_TRUE(std::isnan(p1.y()));
  ASSERT_TRUE(p1.is_empty());
}

TEST_F(TestGeoTree, linestring)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObLineString *l = NULL;
  ASSERT_EQ(OB_SUCCESS, ObLineString::create_linestring(ObGeoCRS::Cartesian, 0, allocator, l));
  ObCartesianLineString *line = static_cast<ObCartesianLineString *>(l);
  ASSERT_EQ(ObGeoType::LINESTRING, line->type());
  ASSERT_EQ(1, line->dimension());
  uint32_t num = 10000;
  common::ObVector<double> xv;
  common::ObVector<double> yv;
  for (int i = 0; i < num; i++) {
    double x = static_cast<double>(rand())/static_cast<double>(rand());
    double y = static_cast<double>(rand())/static_cast<double>(rand());
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeomInnerPoint(x, y)));
    xv.push_back(x);
    yv.push_back(y);
  }
  ASSERT_EQ(10000, line->size());
  ObCartesianLineString::iterator iter = line->begin();
  for (int i = 0; iter != line->end(); ++iter, i++) {
    ASSERT_EQ(xv[i], iter->get<0>());
    ASSERT_EQ(yv[i], iter->get<1>());
  }

  line->clear();
  ASSERT_EQ(0, line->size());
  ASSERT_TRUE(line->is_empty());
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeomInnerPoint(i, i+1)));
  }
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(i, (*line)[i].get<0>());
    ASSERT_EQ(i+1, (*line)[i].get<1>());
  }
  ASSERT_EQ(0, line->front().get<0>());
  ASSERT_EQ(1, line->front().get<1>());
  ASSERT_EQ(3, line->back().get<0>());
  ASSERT_EQ(4, line->back().get<1>());
  line->pop_front();
  ASSERT_EQ(1, line->front().get<0>());
  ASSERT_EQ(2, line->front().get<1>());
}

TEST_F(TestGeoTree, linesarring)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObLinearring *r = NULL;
  ASSERT_EQ(OB_SUCCESS, ObLinearring::create_linearring(ObGeoCRS::Cartesian, 0, allocator, r));
  ASSERT_EQ(ObGeoType::LINESTRING, r->type());
  ASSERT_EQ(1, r->dimension());
  ObCartesianLinearring *ring = static_cast<ObCartesianLinearring *>(r);
  ASSERT_EQ(0, ring->size());
  ASSERT_TRUE(ring->is_empty());
  ObWkbGeomInnerPoint p1(0.0, 0.0);
  ASSERT_EQ(OB_SUCCESS, ring->push_back(p1));
  ObWkbGeomInnerPoint p2(0, 10);
  ASSERT_EQ(OB_SUCCESS, ring->push_back(p2));
  ObWkbGeomInnerPoint p3(10, 10);
  ASSERT_EQ(OB_SUCCESS, ring->push_back(p3));
  ObWkbGeomInnerPoint p4(10, 0.0);
  ASSERT_EQ(OB_SUCCESS, ring->push_back(p4));

  ObCartesianLinearring::iterator iter = ring->begin();
  for (int i = 0; iter != ring->end(); ++iter, i++) {
    std::cout << "(" << iter->get<0>() << ", " << iter->get<1>() << ")" << std::endl;
  }

  ASSERT_EQ(0, (*ring)[1].get<0>());
  ASSERT_EQ(10, (*ring)[1].get<1>());
  ASSERT_EQ(10, (*ring)[2].get<0>());
  ASSERT_EQ(10, (*ring)[2].get<1>());

  ASSERT_EQ(0, ring->front().get<0>());
  ASSERT_EQ(0, ring->front().get<1>());
  ASSERT_EQ(10, ring->back().get<0>());
  ASSERT_EQ(0, ring->back().get<1>());
  ring->pop_front();
  ASSERT_EQ(3, ring->size());
  ASSERT_EQ(0, ring->front().get<0>());
  ASSERT_EQ(10, ring->front().get<1>());
}

TEST_F(TestGeoTree, Polygon)
{
  ObPolygon *poly = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, ObPolygon::create_polygon(ObGeoCRS::Cartesian, 0, allocator, poly));
  ASSERT_EQ(ObGeoType::POLYGON, poly->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, poly->crs());
  ASSERT_EQ(0U, poly->size());
  ASSERT_TRUE(poly->empty());
  ASSERT_TRUE(poly->is_empty());
  ASSERT_EQ(ObGeoType::POLYGON, poly->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, poly->crs());
  ASSERT_EQ(0U, poly->size());
  ASSERT_TRUE(poly->empty());
  ASSERT_TRUE(poly->is_empty());

  ObCartesianLinearring outer_ring(0, allocator);

  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(10.0, 0.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 10.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  poly->push_back(outer_ring);
  ASSERT_FALSE(poly->empty());
  ASSERT_FALSE(poly->is_empty());

  ObCartesianLinearring inner_ring(0, allocator);
  inner_ring.push_back(ObWkbGeomInnerPoint(2.0, 2.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(2.0, 8.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(8.0, 8.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(8.0, 2.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(2.0, 2.0));
  poly->push_back(inner_ring);

  ObCartesianPolygon *polygon = static_cast<ObCartesianPolygon *>(poly);

  ASSERT_EQ(2U, polygon->size());
  ASSERT_FALSE(polygon->empty());
  ASSERT_EQ(1U, polygon->inner_ring_size());
  ASSERT_FALSE(polygon->inner_ring(0).empty());
  ASSERT_EQ(10, polygon->exterior_ring()[1].get<0>());
  ASSERT_EQ(0, polygon->exterior_ring()[1].get<1>());
  ASSERT_EQ(10, polygon->exterior_ring()[2].get<0>());
  ASSERT_EQ(10, polygon->exterior_ring()[2].get<1>());
  ASSERT_EQ(2, polygon->inner_ring(0)[1].get<0>());
  ASSERT_EQ(8, polygon->inner_ring(0)[1].get<1>());
  ASSERT_EQ(8, polygon->inner_ring(0)[2].get<0>());
  ASSERT_EQ(8, polygon->inner_ring(0)[2].get<1>());
}

TEST_F(TestGeoTree, Geometrycollection)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObGeometrycollection* gc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObGeometrycollection::create_collection(ObGeoCRS::Cartesian, 0, allocator, gc));

  ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, gc->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, gc->crs());
  ASSERT_TRUE(gc->empty());
  ASSERT_TRUE(gc->is_empty());

  ObCartesianGeometrycollection *cartesianGc = static_cast<ObCartesianGeometrycollection *>(gc);
  cartesianGc->push_back(ObCartesianGeometrycollection(0, allocator));
  ASSERT_FALSE(cartesianGc->empty());
  ASSERT_TRUE(cartesianGc->is_empty());

  cartesianGc->push_back(ObCartesianPoint(0.0, 0.0, 0, &allocator));
  cartesianGc->push_back(ObCartesianPoint(10.0, 0.0, 0, &allocator));
  cartesianGc->push_back(ObCartesianPoint(10.0, 10.0, 0, &allocator));
  cartesianGc->push_back(ObCartesianPoint(0.0, 10.0, 0, &allocator));
  cartesianGc->push_back(ObCartesianPoint(0.0, 0.0, 0, &allocator));

  ObCartesianLineString ls(0, allocator);
  ls.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ls.push_back(ObWkbGeomInnerPoint(10.0, 0.0));
  ls.push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  ls.push_back(ObWkbGeomInnerPoint(0.0, 10.0));
  ls.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  cartesianGc->push_back(ls);

  ObCartesianLinearring outer_ring(0, allocator);
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(10.0, 0.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 10.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ObCartesianPolygon py(0, allocator);
  py.push_back(outer_ring);
  cartesianGc->push_back(py);

  ObCartesianMultipoint mpt(0, allocator);
  mpt.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  cartesianGc->push_back(mpt);

  ObCartesianLineString ls2(0, allocator);
  ls2.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ls2.push_back(ObWkbGeomInnerPoint(1.0, 1.0));
  ObCartesianMultilinestring mls;
  mls.push_back(ls2);
  cartesianGc->push_back(mls);

  ObCartesianMultipolygon mpy(0, allocator);
  cartesianGc->push_back(mpy);

  ObCartesianGeometrycollection inner_gc(0, allocator);
  cartesianGc->push_back(inner_gc);

  ASSERT_EQ(12U, cartesianGc->size());
  ASSERT_FALSE(cartesianGc->empty());

  ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, cartesianGc->front().type());

  cartesianGc->pop_front();
  cartesianGc->pop_front();
  ASSERT_EQ(10U, cartesianGc->size());
  ASSERT_EQ(ObGeoType::POINT, cartesianGc->front().type());
}

TEST_F(TestGeoTree, Multipoint)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMultipoint *mpt_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObMultipoint::create_multipoint(ObGeoCRS::Cartesian, 0, allocator, mpt_ptr));

  ASSERT_EQ(ObGeoType::MULTIPOINT, mpt_ptr->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, mpt_ptr->crs());
  ASSERT_TRUE(mpt_ptr->empty());
  ASSERT_TRUE(mpt_ptr->is_empty());

  ObCartesianMultipoint *cartMtp = static_cast<ObCartesianMultipoint *>(mpt_ptr);
  cartMtp->push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ASSERT_EQ(1U, cartMtp->size());
  ASSERT_FALSE(cartMtp->empty());
  ASSERT_FALSE(cartMtp->is_empty());

  cartMtp->push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  ASSERT_EQ(2U, cartMtp->size());

  ASSERT_EQ(0.0, cartMtp->front().get<0>());
  ASSERT_EQ(0.0, cartMtp->front().get<1>());

  cartMtp->pop_front();
  ASSERT_EQ(1U, cartMtp->size());
  ASSERT_EQ(10.0, cartMtp->front().get<0>());
  ASSERT_EQ(10.0, cartMtp->front().get<1>());

}

TEST_F(TestGeoTree, Multilinestring)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMultilinestring *mls_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObMultilinestring::create_multilinestring(ObGeoCRS::Cartesian, 0, allocator, mls_ptr));

  ASSERT_EQ(ObGeoType::MULTILINESTRING, mls_ptr->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, mls_ptr->crs());
  ASSERT_TRUE(mls_ptr->empty());
  ASSERT_TRUE(mls_ptr->is_empty());
  ObCartesianMultilinestring *cartMls = static_cast<ObCartesianMultilinestring *>(mls_ptr);

  ObCartesianLineString ls(0, allocator);
  ls.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ls.push_back(ObWkbGeomInnerPoint(10.0, 0.0));
  ls.push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  ls.push_back(ObWkbGeomInnerPoint(0.0, 10.0));
  ls.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  cartMls->push_back(ls);
  ASSERT_EQ(1U, cartMls->size());


  ObCartesianMultilinestring *mls = cartMls;
  const ObCartesianMultilinestring *cmls = cartMls;

  ObCartesianMultilinestring::iterator iter = mls->begin();
  ObCartesianMultilinestring::const_iterator citer = cmls->begin();

  auto x = boost::begin(*mls);
  auto y = boost::begin(*cmls);

  ObCartesianLineString lsa(0, allocator);

  ASSERT_FALSE(cartMls->empty());
  ASSERT_FALSE(cartMls->is_empty());

  ObCartesianLineString ls2(0, allocator);
  ls2.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ls2.push_back(ObWkbGeomInnerPoint(20.0, 20.0));
  cartMls->push_back(ls2);
  ASSERT_EQ(2U, cartMls->size());

  ASSERT_EQ(5U, cartMls->front().size());
  ASSERT_EQ(10, cartMls->front()[2].get<0>());
  ASSERT_EQ(10, cartMls->front()[2].get<1>());

  cartMls->pop_front();
  ASSERT_EQ(1U, cartMls->size());
  ASSERT_EQ(2U, cartMls->front().size());
}

TEST_F(TestGeoTree, Multipolygon)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMultipolygon *mpy_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObMultipolygon::create_multipolygon(ObGeoCRS::Cartesian, 0, allocator, mpy_ptr));
  ObCartesianMultipolygon *cartMpy = static_cast<ObCartesianMultipolygon *>(mpy_ptr);
  ASSERT_EQ(ObGeoType::MULTIPOLYGON, cartMpy->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, cartMpy->crs());
  ASSERT_TRUE(cartMpy->empty());
  ASSERT_TRUE(cartMpy->is_empty());

  ObCartesianLinearring outer_ring(0, allocator);
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(10.0, 0.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 10.0));
  outer_ring.push_back(ObWkbGeomInnerPoint(0.0, 0.0));

  ObCartesianLinearring inner_ring(0, allocator);
  inner_ring.push_back(ObWkbGeomInnerPoint(2.0, 2.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(2.0, 8.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(8.0, 8.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(8.0, 2.0));
  inner_ring.push_back(ObWkbGeomInnerPoint(2.0, 2.0));

  ObCartesianPolygon py(0, allocator);
  py.push_back(outer_ring);
  py.push_back(inner_ring);
  cartMpy->push_back(py);
  ASSERT_EQ(1U, cartMpy->size());
  ASSERT_FALSE(cartMpy->empty());
  ASSERT_FALSE(cartMpy->is_empty());

  cartMpy->push_back(ObCartesianPolygon(0, allocator));
  ASSERT_EQ(2U, cartMpy->size());

  ASSERT_EQ(2U, cartMpy->front().size());

  cartMpy->pop_front();
  ASSERT_EQ(1U, cartMpy->size());
  ASSERT_EQ(0U, cartMpy->front().size());
}

TEST_F(TestGeoTree, intersection_op)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObCartesianLineString ls1(0, allocator);
  ls1.push_back(ObWkbGeomInnerPoint(1.0, 1.0));
  ls1.push_back(ObWkbGeomInnerPoint(3.0, 3.0));

  ObCartesianLineString ls2(0, allocator);
  ls2.push_back(ObWkbGeomInnerPoint(1.0, 3.0));
  ls2.push_back(ObWkbGeomInnerPoint(3.0, 1.0));
  ObCartesianMultilinestring ml(0, allocator);
	bool d = bg::intersection(ls1, ls2, ml);
  for (int i = 0; i < ml.size(); i++) {
		for (int j = 0; j < ml[i].size(); j++) {
      ASSERT_EQ(2U, ml[i][j].get<0>());
      ASSERT_EQ(2U, ml[i][j].get<1>());
		}
	}

  ObCartesianLinearring outer_ring1(0, allocator);
  outer_ring1.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  outer_ring1.push_back(ObWkbGeomInnerPoint(10.0, 0.0));
  outer_ring1.push_back(ObWkbGeomInnerPoint(10.0, 10.0));
  outer_ring1.push_back(ObWkbGeomInnerPoint(0.0, 10.0));
  outer_ring1.push_back(ObWkbGeomInnerPoint(0.0, 0.0));
  ObCartesianPolygon py1(0, allocator);
  py1.push_back(outer_ring1);

  ObCartesianLinearring outer_ring2(0, allocator);
  outer_ring2.push_back(ObWkbGeomInnerPoint(5.0, 0.0));
  outer_ring2.push_back(ObWkbGeomInnerPoint(15.0, 0.0));
  outer_ring2.push_back(ObWkbGeomInnerPoint(15.0, 10.0));
  outer_ring2.push_back(ObWkbGeomInnerPoint(5.0, 10.0));
  outer_ring2.push_back(ObWkbGeomInnerPoint(5.0, 0.0));
  ObCartesianPolygon py2(0, allocator);
  py2.push_back(outer_ring2);

  ObCartesianMultipolygon mpy(0, allocator);
	bool d1 = bg::intersection(py1, py2, mpy);
  ASSERT_EQ(1U, mpy.size());
  ASSERT_EQ(1U, mpy[0].size());

  int i = 0;
  BOOST_FOREACH(ObCartesianPolygon const& p, mpy)
  {
      std::cout << i++ << ": " << boost::geometry::area(p) << std::endl;
  }
}



} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
