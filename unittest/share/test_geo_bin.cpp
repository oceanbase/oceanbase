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
#define BOOST_GEOMETRY_DISABLE_DEPRECATED_03_WARNING 1
#define BOOST_ALLOW_DEPRECATED_HEADERS 1
#include <boost/geometry.hpp>
#define private public
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo_ibin.h"
#include "lib/geo/ob_geo_bin_traits.h"
#include "lib/geo/ob_geo_tree.h"
#include "lib/geo/ob_geo_tree_traits.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_coordinate_range_visitor.h"
#include "lib/geo/ob_geo_longtitude_correct_visitor.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_srs_info.h"
#include "lib/geo/ob_geo_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/random/ob_random.h"
#undef private

#include <sys/time.h>

namespace oceanbase {

using namespace oceanbase::share::schema;
using namespace omt;
namespace common {

class TestGeoBin : public ::testing::Test {
public:
  TestGeoBin()
  {}
  ~TestGeoBin()
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
  DISALLOW_COPY_AND_ASSIGN(TestGeoBin);
};

enum class GeogValueValidType {
    NOT_DEFINED = 0,
    IN_RANGE,
    OUT_RANGE
};

static const ObSrsItem *srs_item = NULL;
static ObArenaAllocator allocator_(ObModIds::TEST);

class ObMockProjectedSrsBase : public ObSpatialReferenceSystemBase
{
public:
  ObMockProjectedSrsBase(){}
  virtual ~ObMockProjectedSrsBase(){}
  ObSrsType srs_type() const { return ObSrsType::PROJECTED_SRS; }
  double prime_meridian() const { return 0.0; }
  double linear_unit() const { return 0.0; }
  double angular_unit() const { return 0.0; }
  double semi_major_axis() const { return 0.0; }
  double inverse_flattening() const { return 0.0; }
  bool is_wgs84() const { return true; }
  bool has_wgs84_value() const { return true; }
  void set_bounds(double min_x, double min_y, double max_x, double max_y) { UNUSEDx(min_x, min_y, max_x, max_y); }
  int set_proj4text(ObIAllocator &allocator, const ObString &src_proj4) { UNUSEDx(allocator, src_proj4); return OB_SUCCESS; }
  void set_proj4text(ObString &src_proj4) { UNUSED(src_proj4); }
  const ObSrsBoundsItem* get_bounds() const { return NULL; }
  ObString get_proj4text() { return ObString(); }

  ObAxisDirection axis_direction(uint8_t axis_index) const { UNUSED(axis_index); return ObAxisDirection::EAST; }
  int get_proj4_param(ObIAllocator *allocator, ObString &proj4_param) const { UNUSEDx(allocator, proj4_param); return OB_SUCCESS; }
  uint32_t get_srid() const { return 0; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObMockProjectedSrsBase);
};

static ObMockProjectedSrsBase mock_projected_srs;
static ObSrsItem project_srs(&mock_projected_srs);
static const ObSrsItem *mock_projected_srs_item = &project_srs;

int append_srid(ObJsonBuffer& data, uint32_t srid = 0, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
  INIT_SUCC(ret);
  uint32_t ssrid = static_cast<uint32_t>(srid);
  if (OB_FAIL(data.reserve(sizeof(uint32_t)))) {
  } else {
    char *ptr = data.ptr() + data.length();
    ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, ssrid, bo);
    ret = data.set_length(data.length() + sizeof(uint32_t));
  }
  return ret;
}

int append_bo(ObJsonBuffer& data, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
    uint8_t sbo = static_cast<uint8_t>(bo);
    return data.append(reinterpret_cast<char*>(&sbo), sizeof(uint8_t));
}

int append_type(ObJsonBuffer& data, ObGeoType type, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
    INIT_SUCC(ret);
    uint32_t stype = static_cast<uint32_t>(type);
    if (OB_FAIL(data.reserve(sizeof(uint32_t)))) {
    } else {
        char *ptr = data.ptr() + data.length();
        ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, stype, bo);
        ret = data.set_length(data.length() + sizeof(uint32_t));
    }
    return ret;
}

int append_uint32(ObJsonBuffer& data, uint32_t val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
    INIT_SUCC(ret);
    if (OB_FAIL(data.reserve(sizeof(uint32_t)))) {
    } else {
        char *ptr = data.ptr() + data.length();
        ObGeoWkbByteOrderUtil::write<uint32_t>(ptr, val, bo);
        ret = data.set_length(data.length() + sizeof(uint32_t));
    }
    return ret;
}

int append_double(ObJsonBuffer& data, double val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian)
{
    INIT_SUCC(ret);
    if (OB_FAIL(data.reserve(sizeof(double)))) {
    } else {
        char *ptr = data.ptr() + data.length();
        ObGeoWkbByteOrderUtil::write<double>(ptr, val, bo);
        ret = data.set_length(data.length() + sizeof(double));
    }
    return ret;
}

void append_random_inner_point(ObJsonBuffer& data, common::ObVector<double>& xv, common::ObVector<double>& yv,
  GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    if (type == GeogValueValidType::NOT_DEFINED) {
        double x = static_cast<double>(rand())/static_cast<double>(rand());
        double y = static_cast<double>(rand())/static_cast<double>(rand());
        ASSERT_EQ(OB_SUCCESS, append_double(data, x));
        ASSERT_EQ(OB_SUCCESS, append_double(data, y));
        xv.push_back(x);
        yv.push_back(y);
    } else if (type == GeogValueValidType::IN_RANGE) {
        ASSERT_TRUE(srs_item != NULL);
        double x = fmod(static_cast<double>(rand())/static_cast<double>(rand()), static_cast<double>(180));
        x *= srs_item->angular_unit();
        double y = fmod(static_cast<double>(rand())/static_cast<double>(rand()), static_cast<double>(90));
        y *= srs_item->angular_unit();
        ASSERT_EQ(OB_SUCCESS, append_double(data, x));
        ASSERT_EQ(OB_SUCCESS, append_double(data, y));
        xv.push_back(x);
        yv.push_back(y);
    } else {
        ASSERT_TRUE(srs_item != NULL);
        double x = static_cast<double>(rand())/static_cast<double>(rand());
        x += static_cast<double>(180);
        double y = static_cast<double>(rand())/static_cast<double>(rand());
        y += static_cast<double>(90);
        ASSERT_EQ(OB_SUCCESS, append_double(data, x));
        ASSERT_EQ(OB_SUCCESS, append_double(data, y));
        xv.push_back(x);
        yv.push_back(y);
    }
}

void append_random_point(ObJsonBuffer& data, common::ObVector<double>& xv, common::ObVector<double>& yv,
    GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    append_random_inner_point(data, xv, yv, type);
}

void append_ring(ObJsonBuffer& data, uint32_t pnum, common::ObVector<double>& xv, common::ObVector<double>& yv,
    GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
    for (int k = 0; k < pnum; k++) {
        if (type == GeogValueValidType::OUT_RANGE && k == pnum - 1) {
            append_random_inner_point(data, xv, yv, GeogValueValidType::OUT_RANGE);
        } else {
            append_random_inner_point(data, xv, yv, type);
        }

    }
}

void append_line(ObJsonBuffer& data, uint32_t pnum, common::ObVector<double>& xv, common::ObVector<double>& yv,
    GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    append_ring(data, pnum, xv, yv, type);
}

void append_poly(ObJsonBuffer& data, uint32_t lnum, uint32_t pnum, common::ObVector<double>& xv, common::ObVector<double>& yv,
    GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
    // push rings
    for (int j = 0; j < lnum; j++) {
        if (type == GeogValueValidType::OUT_RANGE && j == lnum - 1) {
            append_ring(data, pnum, xv, yv, GeogValueValidType::OUT_RANGE);
        } else {
            append_ring(data, pnum, xv, yv, type);
        }
    }
}

void append_multi_point(ObJsonBuffer& data, uint32_t pnum, common::ObVector<double>& xv, common::ObVector<double>& yv,
    GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
    for (int i = 0; i < pnum; i++) {
        if (type == GeogValueValidType::OUT_RANGE && i == pnum - 1) {
            append_random_point(data, xv, yv, GeogValueValidType::OUT_RANGE);
        } else {
            append_random_point(data, xv, yv, type);
        }
    }
}

void append_multi_line(ObJsonBuffer& data, uint32_t lnum, uint32_t pnum, common::ObVector<double>& xv, common::ObVector<double>& yv,
    GeogValueValidType type = GeogValueValidType::NOT_DEFINED)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
    // push lines
    for (int j = 0; j < lnum; j++) {
        append_line(data, pnum, xv, yv, type);
    }
}

template<typename T>
void check_lines(T& line, uint32_t& pc, common::ObVector<double>& xv, common::ObVector<double>& yv, bool is_const = false)
{
    auto lei = line.end();
    auto lbi = line.begin();
    if (is_const) {
        typename T::const_iterator iter = lbi;

        for (; iter != lei; ++iter, ++pc) {
            ASSERT_EQ(xv[pc], iter->template get<0>());
            ASSERT_EQ(yv[pc], iter->template get<1>());
        }
        uint32_t ii = pc;
        --iter;
        --ii;
        for (; iter >= lbi; --iter, --ii) {
            ASSERT_EQ(xv[ii], iter->template get<0>());
            ASSERT_EQ(yv[ii], iter->template get<1>());
        }
    } else {
        typename T::iterator iter = lbi;
        for (; iter != lei; ++iter, ++pc) {
            ASSERT_EQ(xv[pc], iter->template get<0>());
            ASSERT_EQ(yv[pc], iter->template get<1>());
        }
        uint32_t ii = pc;
        --iter;
        --ii;
        for (; iter >= lbi; --iter, --ii) {
            ASSERT_EQ(xv[ii], iter->template get<0>());
            ASSERT_EQ(yv[ii], iter->template get<1>());
        }
    }
}

// Cartesian
TEST_F(TestGeoBin, point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1.323));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 999.5456));
    ObWkbGeomPoint& p = *reinterpret_cast<ObWkbGeomPoint*>(data.ptr());
    ASSERT_EQ(1.323, p.get<0>());
    ASSERT_EQ(999.5456, p.get<1>());
    p.set<0>(3.321);
    p.set<1>(4.444);
    ObWkbGeomPoint& p2 = *reinterpret_cast<ObWkbGeomPoint*>(data.ptr());
    ASSERT_EQ(3.321, p2.get<0>());
    ASSERT_EQ(4.444, p2.get<1>());
}

TEST_F(TestGeoBin, linestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_line(data, num, xv, yv);

    ObWkbGeomLineString& line = *reinterpret_cast<ObWkbGeomLineString*>(data.ptr());
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomLineString::iterator iter = line.begin();
    auto ei = line.end();
    auto bi = line.begin();
    for (int i = 0; iter != ei; ++iter, i++) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = num - 1; iter >= bi; --iter, i--) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomLineString::const_iterator citer = line.begin();
    for (int i = 0; citer != ei; ++citer, i++) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    for (int i = num - 1; citer >= bi; --citer, i--) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 10001;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_poly(data, lnum, pnum, xv, yv);

    ObWkbGeomPolygon& poly = *reinterpret_cast<ObWkbGeomPolygon*>(data.ptr());
    ObWkbGeomLinearRing& exterior = poly.exterior_ring();
    ObWkbGeomPolygonInnerRings& inner_rings = poly.inner_rings();
    uint32_t pc = 0;
    // check exterior
    check_lines(exterior, pc, xv, yv);
    // check inner rings
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomPolygonInnerRings::iterator iter = inner_rings.begin();
    auto irei = inner_rings.end();
    auto irbi = inner_rings.begin();
    for (; iter != irei; iter++) {
        check_lines(*iter, pc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    pc -= pnum;
    for (; iter >= irbi; iter--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*iter, tpc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
    pc = pnum;
    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomPolygonInnerRings::const_iterator citer = inner_rings.begin();
    for (; citer != irei; citer++) {
        check_lines(*citer, pc, xv, yv, true);
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    pc -= pnum;
    for (; citer >= irbi; citer--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*citer, tpc, xv, yv, true);
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, multi_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_point(data, num, xv, yv);

    ObWkbGeomMultiPoint& mp = *reinterpret_cast<ObWkbGeomMultiPoint*>(data.ptr());
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomMultiPoint::iterator iter = mp.begin();
    auto mpbi = mp.begin();
    auto mpei = mp.end();
    for (int i = 0; iter != mpei; ++iter, i++) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = num - 1; iter >= mpbi; --iter, i--) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomMultiPoint::const_iterator citer = mp.begin();
    for (int i = 0; citer != mpei; ++citer, i++) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    for (int i = num - 1; citer >= mpbi; --citer, i--) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, multi_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 10000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_line(data, lnum, pnum, xv, yv);

    ObWkbGeomMultiLineString& ml = *reinterpret_cast<ObWkbGeomMultiLineString*>(data.ptr());
    uint32_t pc = 0;
    // check lines
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomMultiLineString::iterator iter = ml.begin();
    auto mlbi = ml.begin();
    auto mlei = ml.end();
    for (; iter != mlei; iter++) {
        check_lines(*iter, pc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    pc -= pnum;
    for (; iter >= mlbi; iter--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*iter, tpc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
    pc = 0;
    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomMultiLineString::const_iterator citer = ml.begin();
    for (; citer != mlei; citer++) {
        check_lines(*citer, pc, xv, yv, true);
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    pc -= pnum;
    for (; citer >= mlbi; citer--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*citer, tpc, xv, yv, true);
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, multi_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 99 inner line, every line has 100 point
    uint32_t polynum = 100;
    uint32_t lnum = 100;
    uint32_t pnum = 100;
    common::ObVector<double> xv[polynum];
    common::ObVector<double> yv[polynum];
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        append_poly(data, lnum, pnum, xv[i], yv[i]);
    }

    ObWkbGeomMultiPolygon& mp = *reinterpret_cast<ObWkbGeomMultiPolygon*>(data.ptr());
    ObWkbGeomMultiPolygon::iterator iter = mp.begin();
    auto mpei = mp.end();
    auto mpbi = mp.begin();
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; iter != mpei; ++iter, i++) {
        typename ObWkbGeomMultiPolygon::value_type& poly = *iter;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i]);
        auto& inner_rings = iter->inner_rings();
        ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
        auto irei = inner_rings.end();
        auto irbi = inner_rings.begin();
        for (; riter != irei; riter++) {
            check_lines(*riter, pc, xv[i], yv[i]);
        }
        --riter;
        pc -= pnum;
        for (; riter >= irbi; riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i]);
        }
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = polynum - 1; iter >= mpbi; --iter, --i) {
        typename ObWkbGeomMultiPolygon::value_type& poly = *iter;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i]);
        auto& inner_rings = iter->inner_rings();
        ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
        auto irei = inner_rings.end();
        auto irbi = inner_rings.begin();
        for (; riter != irei; riter++) {
            check_lines(*riter, pc, xv[i], yv[i]);
        }
        --riter;
        pc -= pnum;
        for (; riter >= irbi; riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i]);
        }
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
    // const_iter
    ObWkbGeomMultiPolygon::const_iterator citer = mp.begin();
    t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; citer != mpei; ++citer, i++) {
        typename ObWkbGeomMultiPolygon::value_type& poly = *citer;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i], true);
        auto& inner_rings = citer->inner_rings();
        ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
        auto irei = inner_rings.end();
        auto irbi = inner_rings.begin();
        for (; riter != irei; riter++) {
            check_lines(*riter, pc, xv[i], yv[i], true);
        }
        --riter;
        pc -= pnum;
        for (; riter >= irbi; riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i], true);
        }
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    for (int i = polynum - 1; citer >= mpbi; --citer, --i) {
        typename ObWkbGeomMultiPolygon::value_type& poly = *citer;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i], true);
        auto& inner_rings = citer->inner_rings();
        ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
        auto irei = inner_rings.end();
        auto irbi = inner_rings.begin();
        for (; riter != irei; riter++) {
            check_lines(*riter, pc, xv[i], yv[i], true);
        }
        --riter;
        pc -= pnum;
        for (; riter >= irbi; riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i], true);
        }
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, geom_collection)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data, xv[0], yv[0]);
    // line
    append_line(data, 100, xv[1], yv[1]);
    // polygon
    append_poly(data, 100, 100, xv[2], yv[2]);
    // multipoint
    append_multi_point(data, 100, xv[3], yv[3]);
    // multiline
    append_multi_line(data, 1000, 10, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data, 10, 100, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

    // check
    ObWkbGeomCollection& gc = *reinterpret_cast<ObWkbGeomCollection*>(data.ptr());
    ObWkbGeomCollection::iterator iter = gc.begin();
    for (int i = 0; iter != gc.end(); ++iter, ++i) {
        typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = gc.get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeomPoint* point = reinterpret_cast<const ObWkbGeomPoint*>(sub_ptr);
            ASSERT_EQ(xv[0][0], point->get<0>());
            ASSERT_EQ(yv[0][0], point->get<1>());
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeomLineString* line = reinterpret_cast<const ObWkbGeomLineString*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, line->size());
            check_lines(*line, pc, xv[1], yv[1]);
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeomPolygon* poly = reinterpret_cast<const ObWkbGeomPolygon*>(sub_ptr);
            uint32_t pc = 0;
            check_lines(poly->exterior_ring(), pc, xv[2], yv[2]);
            auto& inner_rings = poly->inner_rings();
            ASSERT_EQ(99, inner_rings.size());
            ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
            for (; riter != inner_rings.end(); riter++) {
                check_lines(*riter, pc, xv[2], yv[2]);
            }
            --riter;
            pc -= 100;
            for (; riter >= inner_rings.begin(); riter--, pc -= 100) {
                uint32_t tpc = pc;
                check_lines(*riter, tpc, xv[2], yv[2]);
            }
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeomMultiPoint* mp = reinterpret_cast<const ObWkbGeomMultiPoint*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, mp->size());
            check_lines(*mp, pc, xv[3], yv[3]);
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeomMultiLineString* ml = reinterpret_cast<const ObWkbGeomMultiLineString*>(sub_ptr);
            ASSERT_EQ(1000, ml->size());
            uint32_t pc = 0;
            ObWkbGeomMultiLineString::iterator liter = ml->begin();
            for (; liter != ml->end(); liter++) {
                check_lines(*liter, pc, xv[4], yv[4]);
            }
            liter--;
            pc -= 10;
            for (; liter >= ml->begin(); liter--, pc -= 10) {
                uint32_t tpc = pc;
                check_lines(*liter, tpc, xv[4], yv[4]);
            }
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeomMultiPolygon* mp = reinterpret_cast<const ObWkbGeomMultiPolygon*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(10, mp->size());
            ObWkbGeomMultiPolygon::iterator mpiter = mp->begin();
            for (; mpiter != mp->end(); ++mpiter) {
                ASSERT_EQ(10, mpiter->size());
                check_lines(mpiter->exterior_ring(), pc, xv[5], yv[5]);
                auto& inner_rings = mpiter->inner_rings();
                ASSERT_EQ(9, inner_rings.size());
                ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
                for (; riter != inner_rings.end(); riter++) {
                    check_lines(*riter, pc, xv[5], yv[5]);
                }
                uint32_t rpc = pc;
                --riter;
                rpc -= 100;
                for (; riter >= inner_rings.begin(); riter--, rpc -= 100) {
                    uint32_t tpc = rpc;
                    check_lines(*riter, tpc, xv[5], yv[5]);
                }
            }
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeomCollection* subgc = reinterpret_cast<const ObWkbGeomCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
    --iter;
    for (int i = 6; iter >= gc.begin(); --iter, --i) {
        typename ObWkbGeomCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = gc.get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeomPoint* point = reinterpret_cast<const ObWkbGeomPoint*>(sub_ptr);
            ASSERT_EQ(xv[0][0], point->get<0>());
            ASSERT_EQ(yv[0][0], point->get<1>());
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeomLineString* line = reinterpret_cast<const ObWkbGeomLineString*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, line->size());
            check_lines(*line, pc, xv[1], yv[1]);
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeomPolygon* poly = reinterpret_cast<const ObWkbGeomPolygon*>(sub_ptr);
            uint32_t pc = 0;
            check_lines(poly->exterior_ring(), pc, xv[2], yv[2]);
            auto& inner_rings = poly->inner_rings();
            ASSERT_EQ(99, inner_rings.size());
            ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
            for (; riter != inner_rings.end(); riter++) {
                check_lines(*riter, pc, xv[2], yv[2]);
            }
            --riter;
            pc -= 100;
            for (; riter >= inner_rings.begin(); riter--, pc -= 100) {
                uint32_t tpc = pc;
                check_lines(*riter, tpc, xv[2], yv[2]);
            }
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeomMultiPoint* mp = reinterpret_cast<const ObWkbGeomMultiPoint*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, mp->size());
            check_lines(*mp, pc, xv[3], yv[3]);
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeomMultiLineString* ml = reinterpret_cast<const ObWkbGeomMultiLineString*>(sub_ptr);
            ASSERT_EQ(1000, ml->size());
            uint32_t pc = 0;
            ObWkbGeomMultiLineString::iterator liter = ml->begin();
            for (; liter != ml->end(); liter++) {
                check_lines(*liter, pc, xv[4], yv[4]);
            }
            liter--;
            pc -= 10;
            for (; liter >= ml->begin(); liter--, pc -= 10) {
                uint32_t tpc = pc;
                check_lines(*liter, tpc, xv[4], yv[4]);
            }
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeomMultiPolygon* mp = reinterpret_cast<const ObWkbGeomMultiPolygon*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(10, mp->size());
            ObWkbGeomMultiPolygon::iterator mpiter = mp->begin();
            for (; mpiter != mp->end(); ++mpiter) {
                ASSERT_EQ(10, mpiter->size());
                check_lines(mpiter->exterior_ring(), pc, xv[5], yv[5]);
                auto& inner_rings = mpiter->inner_rings();
                ASSERT_EQ(9, inner_rings.size());
                ObWkbGeomPolygonInnerRings::iterator riter = inner_rings.begin();
                for (; riter != inner_rings.end(); riter++) {
                    check_lines(*riter, pc, xv[5], yv[5]);
                }
                uint32_t rpc = pc;
                --riter;
                rpc -= 100;
                for (; riter >= inner_rings.begin(); riter--, rpc -= 100) {
                    uint32_t tpc = rpc;
                    check_lines(*riter, tpc, xv[5], yv[5]);
                }
            }
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeomCollection* subgc = reinterpret_cast<const ObWkbGeomCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
}

// Geograpgic
TEST_F(TestGeoBin, Geo_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1.323));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 999.5456));
    ObWkbGeogPoint& p = *reinterpret_cast<ObWkbGeogPoint*>(data.ptr());
    ASSERT_EQ(1.323, p.get<0>());
    ASSERT_EQ(999.5456, p.get<1>());
    p.set<0>(3.321);
    p.set<1>(4.444);
    ObWkbGeogPoint& p2 = *reinterpret_cast<ObWkbGeogPoint*>(data.ptr());
    ASSERT_EQ(3.321, p2.get<0>());
    ASSERT_EQ(4.444, p2.get<1>());
}

TEST_F(TestGeoBin, Geo_linestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_line(data, num, xv, yv);

    ObWkbGeogLineString& line = *reinterpret_cast<ObWkbGeogLineString*>(data.ptr());
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogLineString::iterator iter = line.begin();
    for (int i = 0; iter != line.end(); ++iter, i++) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = num - 1; iter >= line.begin(); --iter, i--) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogLineString::const_iterator citer = line.begin();
    for (int i = 0; citer != line.end(); ++citer, i++) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    for (int i = num - 1; citer >= line.begin(); --citer, i--) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, Geo_polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 10001;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_poly(data, lnum, pnum, xv, yv);

    ObWkbGeogPolygon& poly = *reinterpret_cast<ObWkbGeogPolygon*>(data.ptr());
    ObWkbGeogLinearRing& exterior = poly.exterior_ring();
    ObWkbGeogPolygonInnerRings& inner_rings = poly.inner_rings();
    uint32_t pc = 0;
    // check exterior
    check_lines(exterior, pc, xv, yv);
    // check inner rings
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogPolygonInnerRings::iterator iter = inner_rings.begin();
    for (; iter != inner_rings.end(); iter++) {
        check_lines(*iter, pc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    pc -= pnum;
    for (; iter >= inner_rings.begin(); iter--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*iter, tpc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
    pc = pnum;
    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogPolygonInnerRings::const_iterator citer = inner_rings.begin();
    for (; citer != inner_rings.end(); citer++) {
        check_lines(*citer, pc, xv, yv, true);
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    pc -= pnum;
    for (; citer >= inner_rings.begin(); citer--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*citer, tpc, xv, yv, true);
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, Geo_multi_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_point(data, num, xv, yv);

    ObWkbGeogMultiPoint& mp = *reinterpret_cast<ObWkbGeogMultiPoint*>(data.ptr());
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogMultiPoint::iterator iter = mp.begin();
    for (int i = 0; iter != mp.end(); ++iter, i++) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = num - 1; iter >= mp.begin(); --iter, i--) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogMultiPoint::const_iterator citer = mp.begin();
    for (int i = 0; citer != mp.end(); ++citer, i++) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    for (int i = num - 1; citer >= mp.begin(); --citer, i--) {
        ASSERT_EQ(xv[i], citer->get<0>());
        ASSERT_EQ(yv[i], citer->get<1>());
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, Geo_multi_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 10000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_line(data, lnum, pnum, xv, yv);

    ObWkbGeogMultiLineString& ml = *reinterpret_cast<ObWkbGeogMultiLineString*>(data.ptr());
    uint32_t pc = 0;
    // check lines
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogMultiLineString::iterator iter = ml.begin();
    for (; iter != ml.end(); iter++) {
        check_lines(*iter, pc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    pc -= pnum;
    for (; iter >= ml.begin(); iter--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*iter, tpc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
    pc = 0;
    t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeogMultiLineString::const_iterator citer = ml.begin();
    for (; citer != ml.end(); citer++) {
        check_lines(*citer, pc, xv, yv, true);
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    pc -= pnum;
    for (; citer >= ml.begin(); citer--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*citer, tpc, xv, yv, true);
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, Geo_multi_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 99 inner line, every line has 100 point
    uint32_t polynum = 100;
    uint32_t lnum = 100;
    uint32_t pnum = 100;
    common::ObVector<double> xv[polynum];
    common::ObVector<double> yv[polynum];
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        append_poly(data, lnum, pnum, xv[i], yv[i]);
    }

    ObWkbGeogMultiPolygon& mp = *reinterpret_cast<ObWkbGeogMultiPolygon*>(data.ptr());
    ObWkbGeogMultiPolygon::iterator iter = mp.begin();
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; iter != mp.end(); ++iter, i++) {
        typename ObWkbGeogMultiPolygon::value_type& poly = *iter;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i]);
        auto& inner_rings = iter->inner_rings();
        ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
        for (; riter != inner_rings.end(); riter++) {
            check_lines(*riter, pc, xv[i], yv[i]);
        }
        --riter;
        pc -= pnum;
        for (; riter >= inner_rings.begin(); riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i]);
        }
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = polynum - 1; iter >= mp.begin(); --iter, --i) {
        typename ObWkbGeogMultiPolygon::value_type& poly = *iter;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i]);
        auto& inner_rings = iter->inner_rings();
        ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
        for (; riter != inner_rings.end(); riter++) {
            check_lines(*riter, pc, xv[i], yv[i]);
        }
        --riter;
        pc -= pnum;
        for (; riter >= inner_rings.begin(); riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i]);
        }
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
    // const_iter
    ObWkbGeogMultiPolygon::const_iterator citer = mp.begin();
    t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; citer != mp.end(); ++citer, i++) {
        typename ObWkbGeogMultiPolygon::value_type& poly = *citer;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i], true);
        auto& inner_rings = citer->inner_rings();
        ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
        for (; riter != inner_rings.end(); riter++) {
            check_lines(*riter, pc, xv[i], yv[i], true);
        }
        --riter;
        pc -= pnum;
        for (; riter >= inner_rings.begin(); riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i], true);
        }
    }
    t2 = std::chrono::high_resolution_clock::now();
    --citer;
    for (int i = polynum - 1; citer >= mp.begin(); --citer, --i) {
        typename ObWkbGeogMultiPolygon::value_type& poly = *citer;
        uint32_t pc = 0;
        check_lines(poly.exterior_ring(), pc, xv[i], yv[i], true);
        auto& inner_rings = citer->inner_rings();
        ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
        for (; riter != inner_rings.end(); riter++) {
            check_lines(*riter, pc, xv[i], yv[i], true);
        }
        --riter;
        pc -= pnum;
        for (; riter >= inner_rings.begin(); riter--, pc -= pnum) {
            uint32_t tpc = pc;
            check_lines(*riter, tpc, xv[i], yv[i], true);
        }
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("citer:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());
}

TEST_F(TestGeoBin, geog_collection)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data, xv[0], yv[0]);
    // line
    append_line(data, 100, xv[1], yv[1]);
    // polygon
    append_poly(data, 100, 100, xv[2], yv[2]);
    // multipoint
    append_multi_point(data, 100, xv[3], yv[3]);
    // multiline
    append_multi_line(data, 1000, 10, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data, 10, 100, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

    // check
    ObWkbGeogCollection& gc = *reinterpret_cast<ObWkbGeogCollection*>(data.ptr());
    ObWkbGeogCollection::iterator iter = gc.begin();
    for (int i = 0; iter != gc.end(); ++iter, ++i) {
        typename ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = gc.get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeogPoint* point = reinterpret_cast<const ObWkbGeogPoint*>(sub_ptr);
            ASSERT_EQ(xv[0][0], point->get<0>());
            ASSERT_EQ(yv[0][0], point->get<1>());
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeogLineString* line = reinterpret_cast<const ObWkbGeogLineString*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, line->size());
            check_lines(*line, pc, xv[1], yv[1]);
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
            uint32_t pc = 0;
            check_lines(poly->exterior_ring(), pc, xv[2], yv[2]);
            auto& inner_rings = poly->inner_rings();
            ASSERT_EQ(99, inner_rings.size());
            ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
            for (; riter != inner_rings.end(); riter++) {
                check_lines(*riter, pc, xv[2], yv[2]);
            }
            --riter;
            pc -= 100;
            for (; riter >= inner_rings.begin(); riter--, pc -= 100) {
                uint32_t tpc = pc;
                check_lines(*riter, tpc, xv[2], yv[2]);
            }
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeogMultiPoint* mp = reinterpret_cast<const ObWkbGeogMultiPoint*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, mp->size());
            check_lines(*mp, pc, xv[3], yv[3]);
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeogMultiLineString* ml = reinterpret_cast<const ObWkbGeogMultiLineString*>(sub_ptr);
            ASSERT_EQ(1000, ml->size());
            uint32_t pc = 0;
            ObWkbGeogMultiLineString::iterator liter = ml->begin();
            for (; liter != ml->end(); liter++) {
                check_lines(*liter, pc, xv[4], yv[4]);
            }
            liter--;
            pc -= 10;
            for (; liter >= ml->begin(); liter--, pc -= 10) {
                uint32_t tpc = pc;
                check_lines(*liter, tpc, xv[4], yv[4]);
            }
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeogMultiPolygon* mp = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(10, mp->size());
            ObWkbGeogMultiPolygon::iterator mpiter = mp->begin();
            for (; mpiter != mp->end(); ++mpiter) {
                ASSERT_EQ(10, mpiter->size());
                check_lines(mpiter->exterior_ring(), pc, xv[5], yv[5]);
                auto& inner_rings = mpiter->inner_rings();
                ASSERT_EQ(9, inner_rings.size());
                ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
                for (; riter != inner_rings.end(); riter++) {
                    check_lines(*riter, pc, xv[5], yv[5]);
                }
                uint32_t rpc = pc;
                --riter;
                rpc -= 100;
                for (; riter >= inner_rings.begin(); riter--, rpc -= 100) {
                    uint32_t tpc = rpc;
                    check_lines(*riter, tpc, xv[5], yv[5]);
                }
            }
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeogCollection* subgc = reinterpret_cast<const ObWkbGeogCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
    --iter;
    for (int i = 6; iter >= gc.begin(); --iter, --i) {
        typename ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = gc.get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeogPoint* point = reinterpret_cast<const ObWkbGeogPoint*>(sub_ptr);
            ASSERT_EQ(xv[0][0], point->get<0>());
            ASSERT_EQ(yv[0][0], point->get<1>());
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeogLineString* line = reinterpret_cast<const ObWkbGeogLineString*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, line->size());
            check_lines(*line, pc, xv[1], yv[1]);
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
            uint32_t pc = 0;
            check_lines(poly->exterior_ring(), pc, xv[2], yv[2]);
            auto& inner_rings = poly->inner_rings();
            ASSERT_EQ(99, inner_rings.size());
            ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
            for (; riter != inner_rings.end(); riter++) {
                check_lines(*riter, pc, xv[2], yv[2]);
            }
            --riter;
            pc -= 100;
            for (; riter >= inner_rings.begin(); riter--, pc -= 100) {
                uint32_t tpc = pc;
                check_lines(*riter, tpc, xv[2], yv[2]);
            }
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeogMultiPoint* mp = reinterpret_cast<const ObWkbGeogMultiPoint*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, mp->size());
            check_lines(*mp, pc, xv[3], yv[3]);
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeogMultiLineString* ml = reinterpret_cast<const ObWkbGeogMultiLineString*>(sub_ptr);
            ASSERT_EQ(1000, ml->size());
            uint32_t pc = 0;
            ObWkbGeogMultiLineString::iterator liter = ml->begin();
            for (; liter != ml->end(); liter++) {
                check_lines(*liter, pc, xv[4], yv[4]);
            }
            liter--;
            pc -= 10;
            for (; liter >= ml->begin(); liter--, pc -= 10) {
                uint32_t tpc = pc;
                check_lines(*liter, tpc, xv[4], yv[4]);
            }
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeogMultiPolygon* mp = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(10, mp->size());
            ObWkbGeogMultiPolygon::iterator mpiter = mp->begin();
            for (; mpiter != mp->end(); ++mpiter) {
                ASSERT_EQ(10, mpiter->size());
                check_lines(mpiter->exterior_ring(), pc, xv[5], yv[5]);
                auto& inner_rings = mpiter->inner_rings();
                ASSERT_EQ(9, inner_rings.size());
                ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
                for (; riter != inner_rings.end(); riter++) {
                    check_lines(*riter, pc, xv[5], yv[5]);
                }
                uint32_t rpc = pc;
                --riter;
                rpc -= 100;
                for (; riter >= inner_rings.begin(); riter--, rpc -= 100) {
                    uint32_t tpc = rpc;
                    check_lines(*riter, tpc, xv[5], yv[5]);
                }
            }
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeogCollection* subgc = reinterpret_cast<const ObWkbGeogCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
}


template<typename T>
void check_null_str(ObString& str)
{
    T obj;
    obj.set_data(str);
    ASSERT_EQ(0, obj.length());
    ASSERT_EQ(true, obj.is_empty());
}

// bin interface
TEST_F(TestGeoBin, i_null_ptr)
{
    ObString str(NULL);
    { // point
        check_null_str<ObIWkbGeomPoint>(str);
        check_null_str<ObIWkbGeogPoint>(str);
    }
    { // linestring
        check_null_str<ObIWkbGeomLineString>(str);
        check_null_str<ObIWkbGeogLineString>(str);
    }
    { // polygon
        check_null_str<ObIWkbGeomPolygon>(str);
        check_null_str<ObIWkbGeogPolygon>(str);
    }
    { // multipoint
        check_null_str<ObIWkbGeomMultiPoint>(str);
        check_null_str<ObIWkbGeogMultiPoint>(str);
    }
    { // multiline
        check_null_str<ObIWkbGeomMultiLineString>(str);
        check_null_str<ObIWkbGeogMultiLineString>(str);
    }
    { // multipoly
        check_null_str<ObIWkbGeomMultiPolygon>(str);
        check_null_str<ObIWkbGeogMultiPolygon>(str);
    }
    { // gc
        check_null_str<ObIWkbGeomCollection>(str);
        check_null_str<ObIWkbGeogCollection>(str);
    }
}

TEST_F(TestGeoBin, ipoint)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1.323));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 999.5456));
    ObIWkbGeomPoint p;
    p.set_data(data.string());
    ASSERT_EQ(false, p.is_empty());
    ASSERT_EQ(1.323, p.x());
    ASSERT_EQ(999.5456, p.y());
    ObWkbGeomPoint& bp = *reinterpret_cast<ObWkbGeomPoint*>(const_cast<char*>(p.val()));
    bp.set<0>(3.321);
    bp.set<1>(4.444);
    ASSERT_EQ(false, p.is_empty());
    ASSERT_EQ(3.321, p.x());
    ASSERT_EQ(4.444, p.y());
    bp.set<0>(NAN);
    bp.set<1>(NAN);
    ASSERT_EQ(true, p.is_empty());
}

TEST_F(TestGeoBin, ilinestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 100;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_line(data, 0, xv, yv);

    ObIWkbGeomLineString line;
    line.set_data(data.string());
    ASSERT_EQ(true, line.is_empty());

    ObJsonBuffer data2(&allocator);
    append_line(data2, num, xv, yv);

    line.set_data(data2.string());
    ASSERT_EQ(false, line.is_empty());
}

TEST_F(TestGeoBin, ipolygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 11;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_poly(data, 1, 0, xv, yv);

    ObIWkbGeomPolygon p;
    p.set_data(data.string());
    ASSERT_EQ(p.is_empty(), true);

    ObJsonBuffer data2(&allocator);
    append_poly(data2, lnum, pnum, xv, yv);
    p.set_data(data2.string());
    ASSERT_EQ(p.is_empty(), false);
}

// iter cost test
class testPoint{
public:
    testPoint(double ix, double iy) : x(ix), y(iy) {}
    double x;
    double y;
};

class testLine{
public:
    testLine() {}
    ~testLine() {}
    common::ObVector<testPoint> points;
};

class testPolygon{
public:
    testLine exterior;
    common::ObVector<testLine> inners;
};

TEST_F(TestGeoBin, cost_iter_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    common::ObVector<testPoint> pv;
    append_line(data, num, xv, yv);
    for (int i = 0; i < xv.size(); i++) {
        pv.push_back(testPoint(xv[i], yv[i]));
    }

    ObWkbGeomLineString& line = *reinterpret_cast<ObWkbGeomLineString*>(data.ptr());
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomLineString::iterator iter = line.begin();
    auto ei = line.end();
    auto bi = line.begin();
    for (int i = 0; iter != ei; ++iter, i++) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    for (int i = num - 1; iter >= bi; --iter, i--) {
        ASSERT_EQ(xv[i], iter->get<0>());
        ASSERT_EQ(yv[i], iter->get<1>());
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    auto pi = pv.begin();
    for (int i = 0; pi != pv.end(); ++pi, ++i) {
        ASSERT_EQ(xv[i], pi->x);
        ASSERT_EQ(yv[i], pi->y);
    }
    t2 = std::chrono::high_resolution_clock::now();
    --pi;
    for (int i = num - 1; pi >= pv.begin(); --pi, --i) {
        ASSERT_EQ(xv[i], pi->x);
        ASSERT_EQ(yv[i], pi->y);
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("vector:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    iter = line.begin();

    for (int i = 0; iter != ei; ++iter, i++) {
    }
    t2 = std::chrono::high_resolution_clock::now();
    --iter;

    for (int i = num - 1; iter >= bi; --iter, i--) {
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("iter only move:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    pi = pv.begin();
    auto pvb = pv.begin();
    auto pve = pv.end();
    for (int i = 0; pi != pve; ++pi, ++i) {
    }
    t2 = std::chrono::high_resolution_clock::now();
    --pi;
    for (int i = num - 1; pi >= pvb; --pi, --i) {
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("vector only move:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    iter = line.begin();
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(xv[0], iter->get<0>());
        ASSERT_EQ(yv[0], iter->get<1>());
    }
    t2 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    printf("iter only access: %lfms\n", b2e.count());

    t1 = std::chrono::high_resolution_clock::now();
    pi = pv.begin();
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(xv[0], pi->x);
        ASSERT_EQ(yv[0], pi->y);
    }
    t2 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    printf("vector only access: %lfms\n", b2e.count());
}

void check_test_lines(testLine& line, uint32_t& pc, common::ObVector<double>& xv, common::ObVector<double>& yv)
{
    auto lei = line.points.end();
    auto lbi = line.points.begin();

    auto iter = lbi;
    for (; iter != lei; ++iter, ++pc) {
        ASSERT_EQ(xv[pc], iter->x);
        ASSERT_EQ(yv[pc], iter->y);
    }
    uint32_t ii = pc;
    --iter;
    --ii;
    for (; iter >= lbi; --iter, --ii) {
        ASSERT_EQ(xv[ii], iter->x);
        ASSERT_EQ(yv[ii], iter->y);
    }
}
/*
TEST_F(TestGeoBin, cost_iter_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 100001;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    testPolygon tp;
    append_poly(data, lnum, pnum, xv, yv);
    for (int i = 0; i < lnum - 1; i++) {
        tp.inners.push_back(testLine());
    }
    for (int i = 0; i < xv.size(); i++) {
        if (i < pnum) {
            tp.exterior.points.push_back(testPoint(xv[i], yv[i]));
        } else {
            tp.inners[(i-pnum)/pnum].points.push_back(testPoint(xv[i], yv[i]));
        }
    }

    ObWkbGeomPolygon& poly = *reinterpret_cast<ObWkbGeomPolygon*>(data.ptr());
    ObWkbGeomLinearRing& exterior = poly.exterior_ring();
    ObWkbGeomPolygonInnerRings& inner_rings = poly.inner_rings();
    uint32_t pc = 0;
    // check exterior
    check_lines(exterior, pc, xv, yv);
    // check inner rings
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    ObWkbGeomPolygonInnerRings::iterator iter = inner_rings.begin();
    auto irei = inner_rings.end();
    auto irbi = inner_rings.begin();
    for (; iter != irei; iter++) {
        check_lines(*iter, pc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    --iter;
    pc -= pnum;
    for (; iter >= irbi; iter--, pc -= pnum) {
        uint32_t tpc = pc;
        check_lines(*iter, tpc, xv, yv);
    }
    std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> b2e = t2-t1;
    std::chrono::duration<double, std::milli> e2b = t3-t2;
    printf("iter:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    pc = pnum;
    t1 = std::chrono::high_resolution_clock::now();
    auto pi = tp.inners.begin();
    for (; pi != tp.inners.end(); ++pi) {
        check_test_lines(*pi, pc, xv, yv);
    }
    t2 = std::chrono::high_resolution_clock::now();
    --pi;
    pc -= pnum;
    for (; pi >= tp.inners.begin(); pi--, pc -= pnum) {
        uint32_t tpc = pc;
        check_test_lines(*pi, tpc, xv, yv);
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("vector:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    pc = pnum;
    t1 = std::chrono::high_resolution_clock::now();
    auto miterb = inner_rings.begin();
    for (; miterb != irei; miterb++) {
        auto lb = miterb->begin();
        auto le = miterb->end();
        for (auto li = lb; li != le; ++li) {}
    }
    t2 = std::chrono::high_resolution_clock::now();
    auto mitere = inner_rings.end();
    --mitere;
    pc -= pnum;
    for (; mitere >= irbi; mitere--, pc -= pnum) {
        auto lb = mitere->begin();
        auto le = mitere->end();
        for (auto li = lb; li != le; ++li) {}
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("iter only move:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    pc = pnum;
    t1 = std::chrono::high_resolution_clock::now();
    pi = tp.inners.begin();
    for (; pi != tp.inners.end(); ++pi) {
        auto lb = pi->points.begin();
        auto le = pi->points.end();
        for (auto li = lb; li != le; ++li) {}
    }
    t2 = std::chrono::high_resolution_clock::now();
    pi = tp.inners.end();
    --pi;
    pc -= pnum;
    for (; pi >= tp.inners.begin(); pi--, pc -= pnum) {
        auto lb = pi->points.begin();
        auto le = pi->points.end();
        for (auto li = lb; li != le; ++li) {}
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("vector only move:\nbegin to end : %lfms\nend to begin : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    auto aiterb = inner_rings.begin();
    auto aiterbb = aiterb->begin();
    for (int i = 0; i < lnum - 1; i++) {
        for (int j = 0; j < pnum; j++) {
            ASSERT_EQ(xv[pnum], aiterbb->get<0>());
            ASSERT_EQ(yv[pnum], aiterbb->get<1>());
        }
    }
    t2 = std::chrono::high_resolution_clock::now();
    auto aitere = inner_rings.end();
    --aitere;
    auto aiteree = aitere->end();
    --aiteree;
    uint32_t last_idx = xv.size() - 1;
    for (int i = 0; i < lnum - 1; i++) {
        for (int j = 0; j < pnum; j++) {
            ASSERT_EQ(xv[last_idx], aiteree->get<0>());
            ASSERT_EQ(yv[last_idx], aiteree->get<1>());
        }
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("iter only access:\nbegin : %lfms\nend : %lfms\n", b2e.count(), e2b.count());

    t1 = std::chrono::high_resolution_clock::now();
    auto tpb = tp.inners.begin();
    auto tpbb = tpb->points.begin();
    for (int i = 0; i < lnum - 1; i++) {
        for (int j = 0; j < pnum; j++) {
            ASSERT_EQ(xv[pnum], tpbb->x);
            ASSERT_EQ(yv[pnum], tpbb->y);
        }
    }
    t2 = std::chrono::high_resolution_clock::now();
    auto tpe = tp.inners.end();
    --tpe;
    auto tpee = tpe->points.end();
    --tpee;
    last_idx = xv.size() - 1;
    for (int i = 0; i < lnum - 1; i++) {
        for (int j = 0; j < pnum; j++) {
            ASSERT_EQ(xv[last_idx], tpee->x);
            ASSERT_EQ(yv[last_idx], tpee->y);
        }
    }
    t3 = std::chrono::high_resolution_clock::now();
    b2e = t2-t1;
    e2b = t3-t2;
    printf("vector only access:\nbegin : %lfms\nend : %lfms\n", b2e.count(), e2b.count());

}
*/
TEST_F(TestGeoBin, traits_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 0));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 0));
    ObWkbGeomPoint& p = *reinterpret_cast<ObWkbGeomPoint*>(data.ptr());

    ObJsonBuffer data2(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data2));
    ASSERT_EQ(OB_SUCCESS, append_type(data2, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 1));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 1));
    ObWkbGeomPoint& p2 = *reinterpret_cast<ObWkbGeomPoint*>(data2.ptr());

    double d = boost::geometry::distance(p, p2);
    ASSERT_DOUBLE_EQ(sqrt(2), d);
}

TEST_F(TestGeoBin, traits_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 2));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 0));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 0));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 99));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 99));
    ObWkbGeomLineString& line = *reinterpret_cast<ObWkbGeomLineString*>(data.ptr());

    ObJsonBuffer data2(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data2));
    ASSERT_EQ(OB_SUCCESS, append_type(data2, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, -1));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 1));
    ObWkbGeomPoint& p2 = *reinterpret_cast<ObWkbGeomPoint*>(data2.ptr());

    double d = boost::geometry::distance(line, p2);
    printf("d:%lf\n", d);
    ASSERT_DOUBLE_EQ(sqrt(2), d);
}

TEST_F(TestGeoBin, intersection_ml)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 2));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 3));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 3));
    ObWkbGeomLineString& line1 = *reinterpret_cast<ObWkbGeomLineString*>(data.ptr());

    ObJsonBuffer data2(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data2));
    ASSERT_EQ(OB_SUCCESS, append_type(data2, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data2, 2));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 1));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 3));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 3));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, 1));
    ObWkbGeomLineString& line2 = *reinterpret_cast<ObWkbGeomLineString*>(data2.ptr());

    ObCartesianMultilinestring ml(0, allocator);
    bool d = boost::geometry::intersection(line1, line2, ml);
    for (int i = 0; i < ml.size(); i++) {
		for (int j = 0; j < ml[i].size(); j++) {
            ASSERT_EQ(2U, ml[i][j].get<0>());
            ASSERT_EQ(2U, ml[i][j].get<1>());
		}
	}
}

TEST_F(TestGeoBin, wkb_size_visitor_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1.323));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 999.5456));

    ObIWkbGeomPoint iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    visitor.reset();
    ObIWkbGeogPoint iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

TEST_F(TestGeoBin, wkb_size_visitor_linestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_line(data, num, xv, yv);

    ObIWkbGeomLineString iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    visitor.reset();
    ObIWkbGeogLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

TEST_F(TestGeoBin, wkb_size_visitor_polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    uint32_t rnum = 100;
    uint32_t pnum = 100;
    append_poly(data, rnum, pnum, xv, yv);

    ObIWkbGeogPolygon iwkb_geog_poly;
    iwkb_geog_poly.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog_poly.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    ObIWkbGeomPolygon iwkb_geom_poly;
    iwkb_geom_poly.set_data(data.string());
    visitor.reset();
    ASSERT_EQ(OB_SUCCESS, iwkb_geom_poly.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

TEST_F(TestGeoBin, wkb_size_visitor_multi_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_point(data, num, xv, yv);

    ObIWkbGeogMultiPoint iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    ObIWkbGeomMultiPoint iwkb_geom;
    iwkb_geom.set_data(data.string());
    visitor.reset();
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

TEST_F(TestGeoBin, wkb_size_visitor_multi_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 10000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_line(data, lnum, pnum, xv, yv);

    ObIWkbGeogMultiLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    ObIWkbGeomMultiLineString iwkb_geom;
    iwkb_geom.set_data(data.string());
    visitor.reset();
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

TEST_F(TestGeoBin, wkb_size_visitor_multi_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 99 inner line, every line has 100 point
    uint32_t polynum = 100;
    uint32_t lnum = 100;
    uint32_t pnum = 100;
    common::ObVector<double> xv[polynum];
    common::ObVector<double> yv[polynum];
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        append_poly(data, lnum, pnum, xv[i], yv[i]);
    }

    ObIWkbGeogMultiPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    ObIWkbGeomMultiPolygon iwkb_geom;
    iwkb_geom.set_data(data.string());
    visitor.reset();
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

TEST_F(TestGeoBin, wkb_size_visitor_geom_collection)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data, xv[0], yv[0]);
    // line
    append_line(data, 100, xv[1], yv[1]);
    // polygon
    append_poly(data, 100, 100, xv[2], yv[2]);
    // multipoint
    append_multi_point(data, 100, xv[3], yv[3]);
    // multiline
    append_multi_line(data, 1000, 10, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data, 10, 100, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

    ObIWkbGeogCollection iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoWkbSizeVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());

    ObIWkbGeomCollection iwkb_geom;
    iwkb_geom.set_data(data.string());
    visitor.reset();
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(data.length(), visitor.geo_size());
}

int mock_get_tenant_srs_item(ObIAllocator &allocator, uint64_t tenant_id, uint64_t srs_id, const ObSrsItem *&srs_item)
{
    UNUSEDx( tenant_id);
    int ret = OB_SUCCESS;
    ObGeographicRs rs;
    rs.rs_name.assign_ptr("ED50", strlen("ED50"));
    rs.datum_info.name.assign_ptr("European Datum 1950", strlen("European Datum 1950"));
    rs.datum_info.spheroid.name.assign_ptr("International 1924", strlen("International 1924"));
    rs.datum_info.spheroid.inverse_flattening = 297;
    rs.datum_info.spheroid.semi_major_axis = 6378388;
    rs.primem.longtitude = 0;
    rs.unit.conversion_factor = 0.017453292519943278;
    rs.axis.x.direction = ObAxisDirection::NORTH;
    rs.axis.y.direction = ObAxisDirection::EAST;
    rs.datum_info.towgs84.value[0] = -157.89;
    rs.datum_info.towgs84.value[1] = -17.16;
    rs.datum_info.towgs84.value[2] = -78.41;
    rs.datum_info.towgs84.value[3] = 2.118;
    rs.datum_info.towgs84.value[4] = 2.697;
    rs.datum_info.towgs84.value[5] = -1.434;
    rs.datum_info.towgs84.value[6] = -5.38;
    rs.authority.is_valid = false;

    ObSpatialReferenceSystemBase *srs_info;
    if (OB_FAIL(ObSpatialReferenceSystemBase::create_geographic_srs(&allocator, srs_id, &rs, srs_info))) {
        printf("faild to create geographical srs, ret=%d", ret);
    }
    ObSrsItem *tmp_srs_item = NULL;
    if (OB_ISNULL(tmp_srs_item = OB_NEWx(ObSrsItem, (&allocator_), srs_info))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        printf("fail to alloc memory for srs item, ret=%d", ret);
    } else {
        srs_item = tmp_srs_item;
    }
    return ret;
}

void get_srs_item(ObIAllocator &allocator, uint64_t srs_id, const ObSrsItem *&srs_item)
{
  ASSERT_EQ(OB_SUCCESS, mock_get_tenant_srs_item(allocator, OB_SYS_TENANT_ID, srs_id, srs_item));
}

TEST_F(TestGeoBin, coordinate_range_visitor_point)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 179 * srs_item->angular_unit()));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 89 * srs_item->angular_unit()));

    ObGeoCoordinateRangeVisitor visitor(srs_item);
    ObIWkbGeogPoint iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 181 * srs_item->angular_unit()));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 89 * srs_item->angular_unit()));

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(visitor.value_out_of_range() - 181) < 0.001);

    data.reset();
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 180 * srs_item->angular_unit()));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 91 * srs_item->angular_unit()));

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(true, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(visitor.value_out_of_range() - 91) < 0.001);

    ObIWkbGeomPoint iwkb_geom;
    visitor.reset();
    iwkb_geom.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));
}

TEST_F(TestGeoBin, coordinate_range_visitor_linestring)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t p_num = 20;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_ring(data, p_num, xv, yv, GeogValueValidType::IN_RANGE);

    ObIWkbGeogLinearRing iwkb_geog;
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    append_ring(data, p_num, xv, yv, GeogValueValidType::OUT_RANGE);

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, coordinate_range_visitor_lineString)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    uint32_t p_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, p_num));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 179 * srs_item->angular_unit()));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 89 * srs_item->angular_unit()));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1 * srs_item->angular_unit()));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1 * srs_item->angular_unit()));

    ObIWkbGeogLineString iwkb_geog;
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    p_num = 20;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_line(data, p_num, xv, yv, GeogValueValidType::OUT_RANGE);

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, coordinate_range_visitor_polygon)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    uint32_t rnum = 100;
    uint32_t pnum = 100;
    append_poly(data, rnum, pnum, xv, yv, GeogValueValidType::IN_RANGE);

    ObIWkbGeogPolygon iwkb_geog;
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    append_poly(data, rnum, pnum, xv, yv, GeogValueValidType::OUT_RANGE);

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, coordinate_range_visitor_multipoint)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    uint32_t pnum = 100;
    append_multi_point(data, pnum, xv, yv, GeogValueValidType::IN_RANGE);

    ObIWkbGeogMultiPoint iwkb_geog;
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    append_multi_point(data, pnum, xv, yv, GeogValueValidType::OUT_RANGE);

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, coordinate_range_visitor_multi_line)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 100;
    uint32_t lnum = 10000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_line(data, lnum, pnum, xv, yv, GeogValueValidType::IN_RANGE);

    ObIWkbGeogMultiLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    append_multi_line(data, lnum, pnum, xv, yv, GeogValueValidType::OUT_RANGE);

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, coordinate_range_visitor_multi_poly)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 99 inner line, every line has 100 point
    uint32_t polynum = 100;
    uint32_t lnum = 100;
    uint32_t pnum = 100;
    common::ObVector<double> xv[polynum];
    common::ObVector<double> yv[polynum];
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        append_poly(data, lnum, pnum, xv[i], yv[i], GeogValueValidType::IN_RANGE);
    }

    ObIWkbGeogMultiPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        if (i < polynum - 1) {
            append_poly(data, lnum, pnum, xv[i], yv[i],  GeogValueValidType::IN_RANGE);
        } else {
            append_poly(data, lnum, pnum, xv[i], yv[i], GeogValueValidType::OUT_RANGE);
        }
    }

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, coordinate_range_visitor_geom_collection)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data, xv[0], yv[0], GeogValueValidType::IN_RANGE);
    // line
    append_line(data, 100, xv[1], yv[1], GeogValueValidType::IN_RANGE);
    // polygon
    append_poly(data, 100, 100, xv[2], yv[2], GeogValueValidType::IN_RANGE);
    // multipoint
    append_multi_point(data, 100, xv[3], yv[3], GeogValueValidType::IN_RANGE);
    // multiline
    append_multi_line(data, 1000, 10, xv[4], yv[4], GeogValueValidType::IN_RANGE);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data, 10, 100, xv[5], yv[5], GeogValueValidType::IN_RANGE);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

    ObIWkbGeogCollection iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoCoordinateRangeVisitor visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(isnan(visitor.value_out_of_range()));

    data.reset();
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    // point
    append_random_point(data, xv[0], yv[0],  GeogValueValidType::IN_RANGE);
    // line
    append_line(data, 100, xv[1], yv[1],  GeogValueValidType::IN_RANGE);
    // polygon
    append_poly(data, 100, 100, xv[2], yv[2], GeogValueValidType::IN_RANGE);
    // multipoint
    append_multi_point(data, 100, xv[3], yv[3], GeogValueValidType::IN_RANGE);
    // multiline
    append_multi_line(data, 1000, 10, xv[4], yv[4], GeogValueValidType::OUT_RANGE);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data, 10, 100, xv[5], yv[5],  GeogValueValidType::IN_RANGE);
    }

    visitor.reset();
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(false, visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, wkb_visitor_point)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 90));

    ObGeographPoint p(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit());
    char tmp[1024];
    ObString wkb(1024, 0, tmp);

    ObGeoWkbVisitor visitor(srs_item, &wkb);
    ASSERT_EQ(OB_SUCCESS, p.do_visit(visitor));
    ASSERT_TRUE(data.string().compare(wkb) == 0);

    // coordinate range visitor for geograph tree
    ObGeoCoordinateRangeVisitor range_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, p.do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

    ObGeoWkbSizeVisitor size_visitor;
    ObIWkbGeogLineString iwkb_geog;
    ASSERT_EQ(OB_SUCCESS, p.do_visit(size_visitor));
    ASSERT_EQ(sizeof(uint8_t) + sizeof(uint32_t) + 2* sizeof(double), size_visitor.geo_size());

    // cartesian test
    ObCartesianPoint p_c(181 , 90);
    size_visitor.reset();
    // tree wkb size visitor test
    ASSERT_EQ(OB_SUCCESS, p_c.do_visit(size_visitor));
    ASSERT_EQ(sizeof(uint8_t) + sizeof(uint32_t) + 2* sizeof(double), size_visitor.geo_size());

    ObString cart_wkb(1024, 0, tmp);
    visitor.set_wkb_buffer(&cart_wkb);
    visitor.set_srs(mock_projected_srs_item);
    // tree wkb visitor test
    ASSERT_EQ(OB_SUCCESS, p_c.do_visit(visitor));
    ASSERT_TRUE(data.string().compare(cart_wkb) == 0);

    // geograph longtitude correction for tree
    ObGeoLongtitudeCorrectVisitor longti_correction_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, p.do_visit(longti_correction_visitor));
    range_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, p.do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());
}

TEST_F(TestGeoBin, wkb_visitor_linestring)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ObGeoWkbSizeVisitor size_visitor;
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
    uint32_t pnum = 3;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 10));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 20));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 30));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 40));

    ObIWkbGeogLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(size_visitor));
    uint32_t wkb_size = size_visitor.geo_size();
    size_visitor.reset();

    ObLineString *l = NULL;
    ASSERT_EQ(OB_SUCCESS, ObLineString::create_linestring(ObGeoCRS::Geographic, 0, allocator, l));
    ObGeographLineString *line = static_cast<ObGeographLineString *>(l);
    ASSERT_EQ(ObGeoType::LINESTRING, line->type());
    ASSERT_EQ(1, line->dimension());
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeogInnerPoint(10 * srs_item->angular_unit(), 20 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeogInnerPoint(30 * srs_item->angular_unit(), 40 * srs_item->angular_unit())));

    ASSERT_EQ(OB_SUCCESS, line->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    char tmp[1024];
    ObString wkb(1024, 0, tmp);
    ObGeoWkbVisitor visitor(srs_item, &wkb);
    ASSERT_EQ(OB_SUCCESS, line->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(wkb) == 0);

    ObGeoCoordinateRangeVisitor range_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, line->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

    ObGeoLongtitudeCorrectVisitor long_correct_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, line->do_visit(long_correct_visitor));
    range_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, line->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());

    // cartesian test
    ASSERT_EQ(OB_SUCCESS, ObLineString::create_linestring(ObGeoCRS::Cartesian, 0, allocator, l));
    ObCartesianLineString *carte_line = static_cast<ObCartesianLineString *>(l);
    ASSERT_EQ(ObGeoType::LINESTRING, carte_line->type());
    ASSERT_EQ(1, carte_line->dimension());
    ASSERT_EQ(OB_SUCCESS, carte_line->push_back(ObWkbGeomInnerPoint(181, 90)));
    ASSERT_EQ(OB_SUCCESS, carte_line->push_back(ObWkbGeomInnerPoint(10, 20)));
    ASSERT_EQ(OB_SUCCESS, carte_line->push_back(ObWkbGeomInnerPoint(30, 40)));

    size_visitor.reset();
    // tree cartesian wkb size visitor test
    ASSERT_EQ(OB_SUCCESS, carte_line->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    ObString cart_wkb(1024, 0, tmp);
    visitor.set_wkb_buffer(&cart_wkb);
    visitor.set_srs(mock_projected_srs_item);
    // tree cartesian wkb visitor test
    ASSERT_EQ(OB_SUCCESS, carte_line->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(cart_wkb) == 0);
}

TEST_F(TestGeoBin, wkb_visitor_polygon)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    uint32_t ring_num = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, ring_num));
    for (uint32_t i = 0; i < ring_num; i++) {
        ASSERT_EQ(OB_SUCCESS, append_uint32(data, 3));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 10));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 20));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 30));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 40));
    }

    ObGeoWkbSizeVisitor size_visitor;
    ObIWkbGeogPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(size_visitor));
    uint32_t wkb_size = size_visitor.geo_size();

    ObPolygon *l = NULL;
    ASSERT_EQ(OB_SUCCESS, ObPolygon::create_polygon(ObGeoCRS::Geographic, 0, allocator, l));
    ObGeographPolygon *pol = static_cast<ObGeographPolygon *>(l);
    ASSERT_EQ(ObGeoType::POLYGON, pol->type());

    ObGeographLinearring out_ring(0, allocator);
    ASSERT_EQ(OB_SUCCESS, out_ring.push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, out_ring.push_back(ObWkbGeogInnerPoint(10 * srs_item->angular_unit(), 20 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, out_ring.push_back(ObWkbGeogInnerPoint(30 * srs_item->angular_unit(), 40 * srs_item->angular_unit())));
    pol->push_back(out_ring);

    ObGeographLinearring inner_ring(0, allocator);
    ASSERT_EQ(OB_SUCCESS, inner_ring.push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, inner_ring.push_back(ObWkbGeogInnerPoint(10 * srs_item->angular_unit(), 20 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, inner_ring.push_back(ObWkbGeogInnerPoint(30 * srs_item->angular_unit(), 40 * srs_item->angular_unit())));
    pol->push_back(inner_ring);

    char tmp[1024];
    ObString wkb(1024, 0, tmp);
    ObGeoWkbVisitor visitor(srs_item, &wkb);
    ASSERT_EQ(OB_SUCCESS, pol->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(wkb) == 0);

    size_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, pol->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    ObGeoCoordinateRangeVisitor range_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, pol->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

    ObGeoLongtitudeCorrectVisitor long_correct_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, pol->do_visit(long_correct_visitor));
    range_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, pol->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());

    // cartesian test
    ASSERT_EQ(OB_SUCCESS, ObPolygon::create_polygon(ObGeoCRS::Cartesian, 0, allocator, l));
    ObCartesianPolygon *cartesian_pol = static_cast<ObCartesianPolygon *>(l);
    ASSERT_EQ(ObGeoType::POLYGON, cartesian_pol->type());

    ObCartesianLinearring cart_out_ring(0, allocator);
    ASSERT_EQ(OB_SUCCESS, cart_out_ring.push_back(ObWkbGeomInnerPoint(181, 90)));
    ASSERT_EQ(OB_SUCCESS, cart_out_ring.push_back(ObWkbGeomInnerPoint(10, 20)));
    ASSERT_EQ(OB_SUCCESS, cart_out_ring.push_back(ObWkbGeomInnerPoint(30, 40)));
    cartesian_pol->push_back(cart_out_ring);

    ObCartesianLinearring cart_inner_ring(0, allocator);
    ASSERT_EQ(OB_SUCCESS, cart_inner_ring.push_back(ObWkbGeomInnerPoint(181, 90)));
    ASSERT_EQ(OB_SUCCESS, cart_inner_ring.push_back(ObWkbGeomInnerPoint(10, 20)));
    ASSERT_EQ(OB_SUCCESS, cart_inner_ring.push_back(ObWkbGeomInnerPoint(30, 40)));
    cartesian_pol->push_back(cart_inner_ring);

    size_visitor.reset();
    // tree cartesian wkb size visitor test
    ASSERT_EQ(OB_SUCCESS, cartesian_pol->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    ObString cart_wkb(1024, 0, tmp);
    visitor.set_wkb_buffer(&cart_wkb);
    visitor.set_srs(mock_projected_srs_item);
    // tree cartesian wkb visitor test
    ASSERT_EQ(OB_SUCCESS, cartesian_pol->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(cart_wkb) == 0);
}

TEST_F(TestGeoBin, wkb_visitor_multipoint)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
    uint32_t pnum = 3;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 10));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 20));
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 30));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 40));

    ObMultipoint *l = NULL;
    ASSERT_EQ(OB_SUCCESS, ObMultipoint::create_multipoint(ObGeoCRS::Geographic, 0, allocator, l));
    ObGeographMultipoint *line = static_cast<ObGeographMultipoint *>(l);
    ASSERT_EQ(ObGeoType::MULTIPOINT, line->type());
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeogInnerPoint(10 * srs_item->angular_unit(), 20 * srs_item->angular_unit())));
    ASSERT_EQ(OB_SUCCESS, line->push_back(ObWkbGeogInnerPoint(30 * srs_item->angular_unit(), 40 * srs_item->angular_unit())));

    char tmp[1024];
    ObString wkb(1024, 0, tmp);
    ObGeoWkbVisitor visitor(srs_item, &wkb);
    ASSERT_EQ(OB_SUCCESS, line->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(wkb) == 0);

    ObGeoWkbSizeVisitor size_visitor;
    ObIWkbGeogMultiPoint iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(size_visitor));
    uint32_t wkb_size = size_visitor.geo_size();

    size_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, line->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    // coordinate range visitor for geograph tree
    ObGeoCoordinateRangeVisitor range_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, line->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

    // geograph longtitude correction for tree
    ObGeoLongtitudeCorrectVisitor longti_correction_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, line->do_visit(longti_correction_visitor));
    range_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, line->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());

    // cartesian test
    ASSERT_EQ(OB_SUCCESS, ObMultipoint::create_multipoint(ObGeoCRS::Cartesian, 0, allocator, l));
    ObCartesianMultipoint *cartesian_multi_point = static_cast<ObCartesianMultipoint *>(l);
    ASSERT_EQ(ObGeoType::MULTIPOINT, cartesian_multi_point->type());

    ASSERT_EQ(OB_SUCCESS, cartesian_multi_point->push_back(ObWkbGeomInnerPoint(181, 90)));
    ASSERT_EQ(OB_SUCCESS, cartesian_multi_point->push_back(ObWkbGeomInnerPoint(10, 20)));
    ASSERT_EQ(OB_SUCCESS, cartesian_multi_point->push_back(ObWkbGeomInnerPoint(30, 40)));

    size_visitor.reset();
    // tree cartesian wkb size visitor test
    ASSERT_EQ(OB_SUCCESS, cartesian_multi_point->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    ObString cart_wkb(1024, 0, tmp);
    visitor.set_wkb_buffer(&cart_wkb);
    visitor.set_srs(mock_projected_srs_item);
    // tree cartesian wkb visitor test
    ASSERT_EQ(OB_SUCCESS, cartesian_multi_point->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(cart_wkb) == 0);
}

TEST_F(TestGeoBin, wkb_visitor_multiline)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
    uint32_t lnum = 2;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));

    uint32_t pnum = 3;
    for (uint32_t i = 0; i < lnum; i++) {
        ASSERT_EQ(OB_SUCCESS, append_bo(data));
        ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
        ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 180));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 180));
        ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
    }

    ObMultilinestring *mls_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, ObMultilinestring::create_multilinestring(ObGeoCRS::Geographic, 0, allocator, mls_ptr));
    ObGeographMultilinestring *mls = static_cast<ObGeographMultilinestring *>(mls_ptr);
    ASSERT_EQ(ObGeoType::MULTILINESTRING, mls_ptr->type());

    ObGeographLineString ls(0, allocator);
    ls.push_back(ObWkbGeogInnerPoint(180 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
    ls.push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
    ls.push_back(ObWkbGeogInnerPoint(180 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
    mls->push_back(ls);

    ObGeographLineString ls2(0, allocator);
    ls2.push_back(ObWkbGeogInnerPoint(180 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
    ls2.push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
    ls2.push_back(ObWkbGeogInnerPoint(180 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
    mls->push_back(ls2);

    char tmp[1024];
    ObString wkb(1024, 0, tmp);
    ObGeoWkbVisitor visitor(srs_item, &wkb);
    ASSERT_EQ(OB_SUCCESS, mls->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(wkb) == 0);

    ObGeoWkbSizeVisitor size_visitor;
    ObIWkbGeogMultiLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(size_visitor));
    uint32_t wkb_size = size_visitor.geo_size();

    size_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, mls->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    // coordinate range visitor for geograph tree
    ObGeoCoordinateRangeVisitor range_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, mls->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

    // geograph longtitude correction for tree
    ObGeoLongtitudeCorrectVisitor longti_correction_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, mls->do_visit(longti_correction_visitor));
    range_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, mls->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());

    // cartesian tree test
    ASSERT_EQ(OB_SUCCESS, ObMultilinestring::create_multilinestring(ObGeoCRS::Cartesian, 0, allocator, mls_ptr));
    ObCartesianMultilinestring *cart_mls = static_cast<ObCartesianMultilinestring *>(mls_ptr);
    ASSERT_EQ(ObGeoType::MULTILINESTRING, mls_ptr->type());

    ObCartesianLineString cart_ls(0, allocator);
    cart_ls.push_back(ObWkbGeomInnerPoint(180, 90));
    cart_ls.push_back(ObWkbGeomInnerPoint(181, 90));
    cart_ls.push_back(ObWkbGeomInnerPoint(180, 90));
    cart_mls->push_back(cart_ls);

    ObCartesianLineString cart_ls2(0, allocator);
    cart_ls2.push_back(ObWkbGeomInnerPoint(180, 90));
    cart_ls2.push_back(ObWkbGeomInnerPoint(181, 90));
    cart_ls2.push_back(ObWkbGeomInnerPoint(180, 90));
    cart_mls->push_back(cart_ls2);

    size_visitor.reset();
    // tree cartesian wkb size visitor test
    ASSERT_EQ(OB_SUCCESS, cart_mls->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    ObString cart_wkb(1024, 0, tmp);
    visitor.set_wkb_buffer(&cart_wkb);
    visitor.set_srs(mock_projected_srs_item);
    // tree cartesian wkb visitor test
    ASSERT_EQ(OB_SUCCESS, cart_mls->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(cart_wkb) == 0);
}

TEST_F(TestGeoBin, wkb_visitor_multi_poly)
{
    get_srs_item(allocator_, 4326, srs_item);
    ASSERT_TRUE(srs_item != NULL);

    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t polynum = 2;
    uint32_t lnum = 2;
    uint32_t pnum = 3;

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        ASSERT_EQ(OB_SUCCESS, append_bo(data));
        ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
        ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
        // push rings
        for (int j = 0; j < lnum; j++) {
            ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
            for (int k = 0; k < pnum; k++) {
                ASSERT_EQ(OB_SUCCESS, append_double(data, 181.0));
                ASSERT_EQ(OB_SUCCESS, append_double(data, 90.0));
            }
        }
    }

    ObMultipolygon *mpy_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, ObMultipolygon::create_multipolygon(ObGeoCRS::Geographic, 0, allocator, mpy_ptr));
    ObGeographMultipolygon *cartMpy = static_cast<ObGeographMultipolygon *>(mpy_ptr);
    ASSERT_EQ(ObGeoType::MULTIPOLYGON, cartMpy->type());
    ASSERT_EQ(ObGeoCRS::Geographic, cartMpy->crs());
    ASSERT_TRUE(cartMpy->empty());
    ASSERT_TRUE(cartMpy->is_empty());

    ObGeographLinearring inner_ring(0, allocator);
    inner_ring.push_back(ObWkbGeogInnerPoint(181.0 * srs_item->angular_unit(), 90.0 * srs_item->angular_unit()));
    inner_ring.push_back(ObWkbGeogInnerPoint(181.0 * srs_item->angular_unit(), 90.0 * srs_item->angular_unit()));
    inner_ring.push_back(ObWkbGeogInnerPoint(181.0 * srs_item->angular_unit(), 90.0 * srs_item->angular_unit()));

    ObGeographLinearring outer_ring(0, allocator);
    outer_ring.push_back(ObWkbGeogInnerPoint(181.0 * srs_item->angular_unit(), 90.0 * srs_item->angular_unit()));
    outer_ring.push_back(ObWkbGeogInnerPoint(181.0 * srs_item->angular_unit(), 90.0 * srs_item->angular_unit()));
    outer_ring.push_back(ObWkbGeogInnerPoint(181.0 * srs_item->angular_unit(), 90.0 * srs_item->angular_unit()));

    ObGeographPolygon py(0, allocator);
    py.push_back(outer_ring);
    py.push_back(inner_ring);
    cartMpy->push_back(py);
    cartMpy->push_back(py);
    ASSERT_EQ(2U, cartMpy->size());

    ObGeoWkbSizeVisitor size_visitor;
    ObIWkbGeogMultiPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(size_visitor));
    uint32_t wkb_size = size_visitor.geo_size();

    size_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, cartMpy->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    char tmp[1024];
    ObString wkb(1024, 0, tmp);
    ObGeoWkbVisitor visitor(srs_item, &wkb);
    ASSERT_EQ(OB_SUCCESS, cartMpy->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(wkb) == 0);

    // coordinate range visitor for geograph tree
    ObGeoCoordinateRangeVisitor range_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, cartMpy->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
    ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

    // geograph longtitude correction for tree
    ObGeoLongtitudeCorrectVisitor longti_correction_visitor(srs_item);
    ASSERT_EQ(OB_SUCCESS, cartMpy->do_visit(longti_correction_visitor));
    range_visitor.reset();
    ASSERT_EQ(OB_SUCCESS, cartMpy->do_visit(range_visitor));
    ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
    ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());

    // cartesian tree test
    ASSERT_EQ(OB_SUCCESS, ObMultipolygon::create_multipolygon(ObGeoCRS::Cartesian, 0, allocator, mpy_ptr));
    ObCartesianMultipolygon *cart_Mpy = static_cast<ObCartesianMultipolygon *>(mpy_ptr);
    ASSERT_EQ(ObGeoType::MULTIPOLYGON, cart_Mpy->type());
    ASSERT_EQ(ObGeoCRS::Cartesian, cart_Mpy->crs());
    ASSERT_TRUE(cart_Mpy->empty());
    ASSERT_TRUE(cart_Mpy->is_empty());

    ObCartesianLinearring cart_inner_ring(0, allocator);
    cart_inner_ring.push_back(ObWkbGeomInnerPoint(181.0, 90.0));
    cart_inner_ring.push_back(ObWkbGeomInnerPoint(181.0, 90.0));
    cart_inner_ring.push_back(ObWkbGeomInnerPoint(181.0, 90.0));

    ObCartesianLinearring cart_outer_ring(0, allocator);
    cart_outer_ring.push_back(ObWkbGeomInnerPoint(181.0, 90.0));
    cart_outer_ring.push_back(ObWkbGeomInnerPoint(181.0, 90.0));
    cart_outer_ring.push_back(ObWkbGeomInnerPoint(181.0, 90.0));

    ObCartesianPolygon cart_py(0, allocator);
    cart_py.push_back(cart_outer_ring);
    cart_py.push_back(cart_inner_ring);
    cart_Mpy->push_back(cart_py);
    cart_Mpy->push_back(cart_py);
    ASSERT_EQ(2U, cart_Mpy->size());

    size_visitor.reset();
    // tree cartesian wkb size visitor test
    ASSERT_EQ(OB_SUCCESS, cart_Mpy->do_visit(size_visitor));
    ASSERT_EQ(wkb_size, size_visitor.geo_size());

    ObString cart_wkb(1024, 0, tmp);
    visitor.set_wkb_buffer(&cart_wkb);
    visitor.set_srs(mock_projected_srs_item);
    // tree cartesian wkb visitor test
    ASSERT_EQ(OB_SUCCESS, cart_Mpy->do_visit(visitor));
    ASSERT_TRUE(data.string().compare(cart_wkb) == 0);
}

TEST_F(TestGeoBin, visitor_Geometrycollection)
{
  get_srs_item(allocator_, 4326, srs_item);
  ASSERT_TRUE(srs_item != NULL);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);

  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 6));

  // point
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
  append_double(data, 10.0);
  append_double(data, 0.0);
  // line
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 3));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 180));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 180));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 10));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 10));
  // polygon
  uint32_t lnum = 1;
  uint32_t pnum = 2;
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
  for (int j = 0; j < lnum; j++) {
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
    for (int k = 0; k < pnum; k++) {
      ASSERT_EQ(OB_SUCCESS, append_double(data, 55.0));
      ASSERT_EQ(OB_SUCCESS, append_double(data, 66.0));
    }
  }

  // multipoint
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 1));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 0.0));
  // multiline
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTILINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 1));
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::LINESTRING));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 2));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 1.0));
  ASSERT_EQ(OB_SUCCESS, append_double(data, 1.0));

  // empty geometry
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

  ObGeoWkbSizeVisitor size_visitor;
  ObIWkbGeogCollection iwkb_gc;
  iwkb_gc.set_data(data.string());
  ASSERT_EQ(OB_SUCCESS, iwkb_gc.do_visit(size_visitor));
  uint32_t wkb_size = size_visitor.geo_size();

  ObGeometrycollection* gc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObGeometrycollection::create_collection(ObGeoCRS::Geographic, 0, allocator, gc));

  ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, gc->type());
  ASSERT_EQ(ObGeoCRS::Geographic, gc->crs());

  ObGeographGeometrycollection *GeographGc = static_cast<ObGeographGeometrycollection *>(gc);
  // point
  GeographGc->push_back(ObGeographPoint(10.0 * srs_item->angular_unit(), 0.0 * srs_item->angular_unit(), 0, &allocator));
  // lineString
  ObGeographLineString ls(0, allocator);
  ls.push_back(ObWkbGeogInnerPoint(180 * srs_item->angular_unit(), 90 * srs_item->angular_unit() ));
  ls.push_back(ObWkbGeogInnerPoint(180 * srs_item->angular_unit(), 90 * srs_item->angular_unit()));
  ls.push_back(ObWkbGeogInnerPoint(10 * srs_item->angular_unit(), 10 * srs_item->angular_unit()));
  GeographGc->push_back(ls);
  // polygon
  ObGeographLinearring outer_ring(0, allocator);
  outer_ring.push_back(ObWkbGeogInnerPoint(55.0 * srs_item->angular_unit(), 66.0 * srs_item->angular_unit()));
  outer_ring.push_back(ObWkbGeogInnerPoint(55.0 * srs_item->angular_unit(), 66.0 * srs_item->angular_unit()));
  ObGeographPolygon py(0, allocator);
  py.push_back(outer_ring);
  GeographGc->push_back(py);

  // multi point
  ObGeographMultipoint mpt(0, allocator);
  mpt.push_back(ObWkbGeogInnerPoint(0.0 * srs_item->angular_unit() , 0.0 * srs_item->angular_unit()));
  GeographGc->push_back(mpt);

  // multi line
  ObGeographLineString ls2(0, allocator);
  ls2.push_back(ObWkbGeogInnerPoint(181 * srs_item->angular_unit() , 90 * srs_item->angular_unit()));
  ls2.push_back(ObWkbGeogInnerPoint(1.0 * srs_item->angular_unit() , 1.0 * srs_item->angular_unit()));
  ObGeographMultilinestring mls;
  mls.push_back(ls2);
  GeographGc->push_back(mls);

  // empty gc
  ObGeographGeometrycollection inner_gc(0, allocator);
  GeographGc->push_back(inner_gc);

  size_visitor.reset();
  ASSERT_EQ(OB_SUCCESS, GeographGc->do_visit(size_visitor));
  ASSERT_EQ(wkb_size, size_visitor.geo_size());

  char tmp[1024];
  ObString wkb(1024, 0, tmp);
  ObGeoWkbVisitor visitor(srs_item, &wkb);
  ASSERT_EQ(OB_SUCCESS, GeographGc->do_visit(visitor));
  ASSERT_TRUE(data.string().compare(wkb) == 0);

  // coordinate range visitor for geograph tree
  ObGeoCoordinateRangeVisitor range_visitor(srs_item);
  ASSERT_EQ(OB_SUCCESS, GeographGc->do_visit(range_visitor));
  ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
  ASSERT_EQ(true, range_visitor.is_longtitude_out_of_range());
  ASSERT_TRUE(std::abs(range_visitor.value_out_of_range() - 181) < 0.001);

  // geograph longtitude correction for tree
  ObGeoLongtitudeCorrectVisitor longti_correction_visitor(srs_item);
  ASSERT_EQ(OB_SUCCESS, GeographGc->do_visit(longti_correction_visitor));
  range_visitor.reset();
  ASSERT_EQ(OB_SUCCESS, GeographGc->do_visit(range_visitor));
  ASSERT_EQ(false, range_visitor.is_latitude_out_of_range());
  ASSERT_EQ(false, range_visitor.is_longtitude_out_of_range());
  // cartesian test
  ASSERT_EQ(OB_SUCCESS, ObGeometrycollection::create_collection(ObGeoCRS::Cartesian, 0, allocator, gc));

  ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, gc->type());
  ASSERT_EQ(ObGeoCRS::Cartesian, gc->crs());

  ObCartesianGeometrycollection *CartesianGc = static_cast<ObCartesianGeometrycollection *>(gc);
  // point
  CartesianGc->push_back(ObCartesianPoint(10.0, 0.0, 0, &allocator));
  // lineString
  ObCartesianLineString cart_ls(0, allocator);
  cart_ls.push_back(ObWkbGeomInnerPoint(180, 90 ));
  cart_ls.push_back(ObWkbGeomInnerPoint(180, 90));
  cart_ls.push_back(ObWkbGeomInnerPoint(10, 10));
  CartesianGc->push_back(cart_ls);
  // polygon
  ObCartesianLinearring cart_outer_ring(0, allocator);
  cart_outer_ring.push_back(ObWkbGeomInnerPoint(55.0, 66.0));
  cart_outer_ring.push_back(ObWkbGeomInnerPoint(55.0, 66.0));
  ObCartesianPolygon cart_py(0, allocator);
  cart_py.push_back(cart_outer_ring);
  CartesianGc->push_back(cart_py);

  // multi point
  ObCartesianMultipoint cart_mpt(0, allocator);
  cart_mpt.push_back(ObWkbGeomInnerPoint(0.0 , 0.0));
  CartesianGc->push_back(cart_mpt);

  // multi line
  ObCartesianLineString cart_ls2(0, allocator);
  cart_ls2.push_back(ObWkbGeomInnerPoint(181 , 90));
  cart_ls2.push_back(ObWkbGeomInnerPoint(1.0 , 1.0));
  ObCartesianMultilinestring cart_mls;
  cart_mls.push_back(cart_ls2);
  CartesianGc->push_back(cart_mls);

  // empty gc
  ObCartesianGeometrycollection cart_inner_gc(0, allocator);
  CartesianGc->push_back(cart_inner_gc);

  size_visitor.reset();
  // tree cartesian wkb size visitor test
  ASSERT_EQ(OB_SUCCESS, CartesianGc->do_visit(size_visitor));
  ASSERT_EQ(wkb_size, size_visitor.geo_size());

  ObString cart_wkb(1024, 0, tmp);
  visitor.set_wkb_buffer(&cart_wkb);
  visitor.set_srs(mock_projected_srs_item);
  // tree cartesian wkb visitor test
  ASSERT_EQ(OB_SUCCESS, CartesianGc->do_visit(visitor));
  ASSERT_TRUE(data.string().compare(cart_wkb) == 0);
}

TEST_F(TestGeoBin, wkb_invalid_visitor)
{
  get_srs_item(allocator_, 4326, srs_item);
  ASSERT_TRUE(srs_item != NULL);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);
  // 1 exterior line 100 inner line, every line has 100 point
  uint32_t pnum = 100;
  uint32_t lnum = 10000;
  common::ObVector<double> xv;
  common::ObVector<double> yv;
  append_multi_line(data, lnum, pnum, xv, yv);

  ObIWkbGeogMultiLineString iwkb_geog;
  iwkb_geog.set_data(data.string());

  char tmp[1024];
  ObString wkb(1024, 0, tmp);
  ObGeoWkbVisitor visitor(srs_item, &wkb);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, iwkb_geog.do_visit(visitor));
}

TEST_F(TestGeoBin, longti_correct_invalid_visitor)
{
  get_srs_item(allocator_, 4326, srs_item);
  ASSERT_TRUE(srs_item != NULL);

  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer data(&allocator);

  uint32_t polynum = 100;
  uint32_t lnum = 100;
  uint32_t pnum = 100;
  common::ObVector<double> xv[polynum];
  common::ObVector<double> yv[polynum];
  ASSERT_EQ(OB_SUCCESS, append_bo(data));
  ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
  ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
  for (int i = 0; i < polynum; i++) {
    append_poly(data, lnum, pnum, xv[i], yv[i]);
  }

  ObIWkbGeomMultiPolygon iwkb_geom;
  iwkb_geom.set_data(data.string());

  ObGeoLongtitudeCorrectVisitor correct_visitor(srs_item);
  // not support wkb geo convert to wkb
  ASSERT_EQ(OB_INVALID_ARGUMENT, iwkb_geom.do_visit(correct_visitor));
}

TEST_F(TestGeoBin, to_tree_visitor_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 1.323));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 999.5456));
    ObIWkbGeomPoint p;
    p.set_data(data.string());

    ObGeoToTreeVisitor cart_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, p.do_visit(cart_visitor));
    const ObCartesianPoint *point = static_cast<const ObCartesianPoint *>(cart_visitor.get_geometry());
    ASSERT_EQ(point->x(), 1.323);
    ASSERT_EQ(point->y(), 999.5456);

    ObIWkbGeogPoint p1;
    p1.set_data(data.string());
    ObGeoToTreeVisitor geog_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, p1.do_visit(geog_visitor));
    const ObGeographPoint *point1 = static_cast<const ObGeographPoint *>(geog_visitor.get_geometry());
    ASSERT_EQ(point1->x(), 1.323);
    ASSERT_EQ(point1->y(), 999.5456);
}

TEST_F(TestGeoBin, to_tree_visitor_linestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    uint32_t num = 1000000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_line(data, num, xv, yv);

    ObWkbGeogLineString& line = *reinterpret_cast<ObWkbGeogLineString*>(data.ptr());
    ObWkbGeogLineString::iterator iter = line.begin();

    ObIWkbGeogLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoToTreeVisitor geog_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(geog_visitor));
    const ObGeographLineString *line_tree = static_cast<const ObGeographLineString *>(geog_visitor.get_geometry());
    ASSERT_EQ(num, line_tree->size());
    for (int i = 0; iter != line.end(); ++iter, i++) {
        ASSERT_EQ((*line_tree)[i].get<0>(), iter->get<0>());
        ASSERT_EQ((*line_tree)[i].get<1>(), iter->get<1>());
    }

    ObIWkbGeomLineString iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoToTreeVisitor geom_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(geom_visitor));
    const ObCartesianLineString *cart_line_tree = static_cast<const ObCartesianLineString *>(geom_visitor.get_geometry());
    ASSERT_EQ(num, cart_line_tree->size());
    for (int i = 0; iter != line.end(); ++iter, i++) {
        ASSERT_EQ((*cart_line_tree)[i].get<0>(), iter->get<0>());
        ASSERT_EQ((*cart_line_tree)[i].get<1>(), iter->get<1>());
    }
}

template<typename T>
void check_tree_lines(T& line, uint32_t& pc, common::ObVector<double>& xv, common::ObVector<double>& yv)
{
    for (uint32_t i = 0; i < line.size(); i++) {
        ASSERT_EQ(xv[pc], line[i].template get<0>());
        ASSERT_EQ(yv[pc], line[i].template get<1>());
        pc++;
    }
}

TEST_F(TestGeoBin, to_tree_visitor_multi_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    uint32_t pnum = 100;
    uint32_t lnum = 10000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_line(data, lnum, pnum, xv, yv);

    ObIWkbGeogMultiLineString iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoToTreeVisitor geog_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(geog_visitor));
    ObGeographMultilinestring *geog_multiline = static_cast<ObGeographMultilinestring *>(geog_visitor.get_geometry());
    ASSERT_EQ(lnum, geog_multiline->size());
    uint32_t index = 0;
    for (uint32_t i = 0; i < lnum; i++) {
        ObGeographLineString& line = (*geog_multiline)[i];
        ASSERT_EQ(line.size(), pnum);
        check_tree_lines(line, index, xv, yv);
    }

    ObIWkbGeomMultiLineString iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoToTreeVisitor geom_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(geom_visitor));
    ObCartesianMultilinestring *geom_multiline = static_cast<ObCartesianMultilinestring *>(geom_visitor.get_geometry());
    ASSERT_EQ(lnum, geom_multiline->size());
    index = 0;
    for (uint32_t i = 0; i < lnum; i++) {
        ObCartesianLineString& line = (*geom_multiline)[i];
        ASSERT_EQ(line.size(), pnum);
        check_tree_lines(line, index, xv, yv);
    }
}

TEST_F(TestGeoBin, to_tree_visitor_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    uint32_t pnum = 100;
    uint32_t lnum = 10001;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_poly(data, lnum, pnum, xv, yv);

    ObIWkbGeogPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoToTreeVisitor geog_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(geog_visitor));
    ObGeographPolygon *geog_polygon = static_cast<ObGeographPolygon *>(geog_visitor.get_geometry());
    ASSERT_EQ(lnum, geog_polygon->size());
    ObGeographLinearring& ext = geog_polygon->exterior_ring();
    uint32_t index = 0;
    ASSERT_EQ(ext.size(), pnum);
    check_tree_lines(ext, index, xv, yv);
    for (uint32_t i = 0; i < lnum - 1; i++) {
        ObGeographLinearring& inner = geog_polygon->inner_ring(i);
        ASSERT_EQ(inner.size(), pnum);
        check_tree_lines(inner, index, xv, yv);
    }

    ObIWkbGeomPolygon iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoToTreeVisitor geom_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(geom_visitor));
    ObCartesianPolygon *geom_polygon = static_cast<ObCartesianPolygon *>(geom_visitor.get_geometry());
    ASSERT_EQ(lnum, geom_polygon->size());
    ObCartesianLinearring& cart_ext = geom_polygon->exterior_ring();
    index = 0;
    ASSERT_EQ(cart_ext.size(), pnum);
    check_tree_lines(cart_ext, index, xv, yv);
    for (uint32_t i = 0; i < lnum - 1; i++) {
        ObCartesianLinearring& inner = geom_polygon->inner_ring(i);
        ASSERT_EQ(inner.size(), pnum);
        check_tree_lines(inner, index, xv, yv);
    }
}

TEST_F(TestGeoBin, to_tree_visitor_multipoint)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOINT));
    uint32_t pnum = 3;
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 181));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 90));
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 10));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 20));
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 30));
    ASSERT_EQ(OB_SUCCESS, append_double(data, 40));

    ObIWkbGeogMultiPoint iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoToTreeVisitor visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ObGeographMultipoint *multi_point = static_cast<ObGeographMultipoint *>(visitor.get_geometry());
    ASSERT_EQ(3, multi_point->size());

    const ObWkbGeogInnerPoint &point1 =  (*multi_point)[0];
    ASSERT_EQ(181, point1.get<0>());
    ASSERT_EQ(90, point1.get<1>());
    const ObWkbGeogInnerPoint &point2 =  (*multi_point)[1];
    ASSERT_EQ(10, point2.get<0>());
    ASSERT_EQ(20, point2.get<1>());
    const ObWkbGeogInnerPoint &point3 =  (*multi_point)[2];
    ASSERT_EQ(30, point3.get<0>());
    ASSERT_EQ(40, point3.get<1>());
}

TEST_F(TestGeoBin, to_tree_visitor_multi_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    uint32_t polynum = 2;
    uint32_t lnum = 2;
    uint32_t pnum = 3;

    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, polynum));
    for (int i = 0; i < polynum; i++) {
        ASSERT_EQ(OB_SUCCESS, append_bo(data));
        ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
        ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
        // push rings
        for (int j = 0; j < lnum; j++) {
            ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
            for (int k = 0; k < pnum; k++) {
                ASSERT_EQ(OB_SUCCESS, append_double(data, 181.0));
                ASSERT_EQ(OB_SUCCESS, append_double(data, 90.0));
            }
        }
    }

    ObIWkbGeogMultiPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoToTreeVisitor visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ObGeographMultipolygon *multi_polygon = static_cast<ObGeographMultipolygon *>(visitor.get_geometry());
    ASSERT_EQ(2, multi_polygon->size());

    for (uint32_t i = 0; i < multi_polygon->size(); i++) {
      ObGeographPolygon &pol = (*multi_polygon)[i];
      ASSERT_EQ(lnum, pol.size());
      ObGeographLinearring &ext = pol.exterior_ring();
      for (uint32_t j = 0; j < pnum; j++) {
        const ObWkbGeogInnerPoint &point =  ext[j];
        ASSERT_EQ(181.0, point.get<0>());
        ASSERT_EQ(90.0, point.get<1>());
      }
      ObGeographLinearring &inner = pol.inner_ring(0);
      for (uint32_t j = 0; j < pnum; j++) {
        const ObWkbGeogInnerPoint &point =  inner[j];
        ASSERT_EQ(181.0, point.get<0>());
        ASSERT_EQ(90.0, point.get<1>());
      }
    }

    ObIWkbGeogMultiPolygon iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoToTreeVisitor geom_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(geom_visitor));
    ObCartesianMultipolygon *cart_multi_polygon = static_cast<ObCartesianMultipolygon *>(geom_visitor.get_geometry());
    ASSERT_EQ(2, cart_multi_polygon->size());

    for (uint32_t i = 0; i < cart_multi_polygon->size(); i++) {
      ObCartesianPolygon &pol = (*cart_multi_polygon)[i];
      ASSERT_EQ(lnum, pol.size());
      ObCartesianLinearring &ext = pol.exterior_ring();
      for (uint32_t j = 0; j < pnum; j++) {
        const ObWkbGeomInnerPoint &point =  ext[j];
        ASSERT_EQ(181.0, point.get<0>());
        ASSERT_EQ(90.0, point.get<1>());
      }
      ObCartesianLinearring &inner = pol.inner_ring(0);
      for (uint32_t j = 0; j < pnum; j++) {
        const ObWkbGeomInnerPoint &point =  inner[j];
        ASSERT_EQ(181.0, point.get<0>());
        ASSERT_EQ(90.0, point.get<1>());
      }
    }

}

TEST_F(TestGeoBin, to_tree_visitor_geom_collection)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data, xv[0], yv[0]);
    // line
    append_line(data, 100, xv[1], yv[1]);
    // polygon
    append_poly(data, 100, 100, xv[2], yv[2]);
    // multipoint
    append_multi_point(data, 100, xv[3], yv[3]);
    // multiline
    append_multi_line(data, 1000, 10, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data, 10, 100, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

    ObIWkbGeogCollection iwkb_geog;
    iwkb_geog.set_data(data.string());
    ObGeoToTreeVisitor visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ObGeographGeometrycollection *geog_coll = static_cast<ObGeographGeometrycollection *>(visitor.get_geometry());
    ASSERT_EQ(7, geog_coll->size());
    ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, geog_coll->type());

    ASSERT_EQ(ObGeoType::POINT, (*geog_coll)[0].type());
    {
        ObGeographPoint &point = static_cast<ObGeographPoint &>((*geog_coll)[0]);
        ASSERT_EQ(xv[0][0], point.x());
        ASSERT_EQ(yv[0][0], point.y());
    }
    ASSERT_EQ(ObGeoType::LINESTRING, (*geog_coll)[1].type());
    {
        ObGeographLineString &line = static_cast<ObGeographLineString &>((*geog_coll)[1]);
        ASSERT_EQ(100, line.size());
        uint32_t index = 0;
        check_tree_lines(line, index, xv[1], yv[1]);
    }
    ASSERT_EQ(ObGeoType::POLYGON, (*geog_coll)[2].type());
    {
        ObGeographPolygon &polygon = static_cast<ObGeographPolygon &>((*geog_coll)[2]);
        auto &exter_ring = polygon.exterior_ring();
        uint32_t index = 0;
        check_tree_lines(exter_ring, index, xv[2], yv[2]);
        uint32_t ring_num = polygon.size();
        ASSERT_EQ(100, ring_num);
        for (uint32_t i = 0; i < ring_num - 1; i++) {
            auto& inner = polygon.inner_ring(i);
            ASSERT_EQ(inner.size(), 100);
            check_tree_lines(inner, index, xv[2], yv[2]);
        }
    }
    ASSERT_EQ(ObGeoType::MULTIPOINT, (*geog_coll)[3].type());
    {
        ObGeographMultipoint &multi_point = static_cast<ObGeographMultipoint &>((*geog_coll)[3]);
        ASSERT_EQ(100, multi_point.size());
        uint32_t index = 0;
        check_tree_lines(multi_point, index, xv[3], yv[3]);
    }
    ASSERT_EQ(ObGeoType::MULTILINESTRING, (*geog_coll)[4].type());
    {
        ObGeographMultilinestring &multi_line = static_cast<ObGeographMultilinestring &>((*geog_coll)[4]);
        ASSERT_EQ(1000, multi_line.size());
        uint32_t index = 0;
        for (uint32_t i = 0; i < multi_line.size(); i++) {
            check_tree_lines(multi_line[i], index, xv[4], yv[4]);
        }
    }
    ASSERT_EQ(ObGeoType::MULTIPOLYGON, (*geog_coll)[5].type());
    {
        ObGeographMultipolygon &multi_polygon = static_cast<ObGeographMultipolygon &>((*geog_coll)[5]);
        ASSERT_EQ(10, multi_polygon.size());
        uint32_t index = 0;
        for (uint32_t i = 0; i < multi_polygon.size(); i++) {
            auto &exter_ring = multi_polygon[i].exterior_ring();
            check_tree_lines(exter_ring, index, xv[5], yv[5]);
            uint32_t ring_num = multi_polygon[i].size();
            ASSERT_EQ(10, ring_num);
            for (uint32_t j = 0; j < ring_num - 1; j++) {
                auto& inner = multi_polygon[i].inner_ring(j);
                ASSERT_EQ(inner.size(), 100);
                check_tree_lines(inner, index, xv[5], yv[5]);
            }
        }
    }
    ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, (*geog_coll)[6].type());
    {
        ObGeographGeometrycollection &collection = static_cast<ObGeographGeometrycollection &>((*geog_coll)[6]);
        ASSERT_EQ(0, collection.size());
    }


    ObIWkbGeomCollection iwkb_geom;
    iwkb_geom.set_data(data.string());
    ObGeoToTreeVisitor cart_visitor(&allocator);
    ASSERT_EQ(OB_SUCCESS, iwkb_geom.do_visit(cart_visitor));
    ObCartesianGeometrycollection *geom_coll = static_cast<ObCartesianGeometrycollection *>(cart_visitor.get_geometry());
    ASSERT_EQ(7, geom_coll->size());
    ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, geom_coll->type());

    ASSERT_EQ(ObGeoType::POINT, (*geom_coll)[0].type());
    {
        ObCartesianPoint &point = static_cast<ObCartesianPoint &>((*geom_coll)[0]);
        ASSERT_EQ(xv[0][0], point.x());
        ASSERT_EQ(yv[0][0], point.y());
    }
    ASSERT_EQ(ObGeoType::LINESTRING, (*geom_coll)[1].type());
    {
        ObCartesianLineString &line = static_cast<ObCartesianLineString &>((*geom_coll)[1]);
        ASSERT_EQ(100, line.size());
        uint32_t index = 0;
        check_tree_lines(line, index, xv[1], yv[1]);
    }
    ASSERT_EQ(ObGeoType::POLYGON, (*geom_coll)[2].type());
    {
        ObCartesianPolygon &polygon = static_cast<ObCartesianPolygon &>((*geom_coll)[2]);
        auto &exter_ring = polygon.exterior_ring();
        uint32_t index = 0;
        check_tree_lines(exter_ring, index, xv[2], yv[2]);
        uint32_t ring_num = polygon.size();
        ASSERT_EQ(100, ring_num);
        for (uint32_t i = 0; i < ring_num - 1; i++) {
            auto& inner = polygon.inner_ring(i);
            ASSERT_EQ(inner.size(), 100);
            check_tree_lines(inner, index, xv[2], yv[2]);
        }
    }
    ASSERT_EQ(ObGeoType::MULTIPOINT, (*geom_coll)[3].type());
    {
        ObCartesianMultipoint &multi_point = static_cast<ObCartesianMultipoint &>((*geom_coll)[3]);
        ASSERT_EQ(100, multi_point.size());
        uint32_t index = 0;
        check_tree_lines(multi_point, index, xv[3], yv[3]);
    }
    ASSERT_EQ(ObGeoType::MULTILINESTRING, (*geom_coll)[4].type());
    {
        ObCartesianMultilinestring &multi_line = static_cast<ObCartesianMultilinestring &>((*geom_coll)[4]);
        ASSERT_EQ(1000, multi_line.size());
        uint32_t index = 0;
        for (uint32_t i = 0; i < multi_line.size(); i++) {
            check_tree_lines(multi_line[i], index, xv[4], yv[4]);
        }
    }
    ASSERT_EQ(ObGeoType::MULTIPOLYGON, (*geom_coll)[5].type());
    {
        ObCartesianMultipolygon &multi_polygon = static_cast<ObCartesianMultipolygon &>((*geom_coll)[5]);
        ASSERT_EQ(10, multi_polygon.size());
        uint32_t index = 0;
        for (uint32_t i = 0; i < multi_polygon.size(); i++) {
            auto &exter_ring = multi_polygon[i].exterior_ring();
            check_tree_lines(exter_ring, index, xv[5], yv[5]);
            uint32_t ring_num = multi_polygon[i].size();
            ASSERT_EQ(10, ring_num);
            for (uint32_t j = 0; j < ring_num - 1; j++) {
                auto& inner = multi_polygon[i].inner_ring(j);
                ASSERT_EQ(inner.size(), 100);
                check_tree_lines(inner, index, xv[5], yv[5]);
            }
        }
    }
    ASSERT_EQ(ObGeoType::GEOMETRYCOLLECTION, (*geom_coll)[6].type());
    {
        ObCartesianGeometrycollection &collection = static_cast<ObCartesianGeometrycollection &>((*geom_coll)[6]);
        ASSERT_EQ(0, collection.size());
    }


}

TEST_F(TestGeoBin, reverse_coordinate_visitor_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ObGeoReverseCoordinateVisitor visitor;

    double x_val = 179.8;
    double y_val = 89.2;
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POINT));
    ASSERT_EQ(OB_SUCCESS, append_double(data, x_val));
    ASSERT_EQ(OB_SUCCESS, append_double(data, y_val));

    // wkb geographical point
    ObIWkbGeogPoint iwkb_geog;
    iwkb_geog.set_data(data.string());
    ASSERT_EQ(iwkb_geog.x(), x_val);
    ASSERT_EQ(iwkb_geog.y(), y_val);
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(iwkb_geog.x(), y_val);
    ASSERT_EQ(iwkb_geog.y(), x_val);

    // wkb cartesian point
    ObIWkbGeomPoint iwkb_geom;
    iwkb_geom.set_data(data.string());
    ASSERT_EQ(OB_INVALID_ARGUMENT, iwkb_geom.do_visit(visitor));

    // tree geographical point
    ObGeographPoint t_geog(x_val, y_val);
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geog.do_visit(visitor));

    // tree cartesian point
    ObCartesianPoint t_geom(x_val, y_val);
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geom.do_visit(visitor));
}

TEST_F(TestGeoBin, reverse_coordinate_visitor_linestring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buffer(&allocator);
    uint32_t num = 100;
    common::ObVector<double> xv;
    common::ObVector<double> yv;

    append_line(buffer, num, xv, yv);
    ObString data = buffer.string();
    ObWkbGeogLineString& line = *reinterpret_cast<ObWkbGeogLineString*>(data.ptr());

    // wkb geographical linestring
    ObIWkbGeogLineString iwkb_geog;
    iwkb_geog.set_data(data);
    ObGeoReverseCoordinateVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));

    ObWkbGeogLineString::iterator line_iter = line.begin();
    ObWkbGeogLineString::iterator line_end = line.end();
    ASSERT_EQ(line.size(), num);
    for (int i = 0; line_iter != line_end; ++line_iter, ++i) {
        ASSERT_EQ(line_iter->get<0>(), yv[i]);
        ASSERT_EQ(line_iter->get<1>(), xv[i]);
    }

    // wkb cartesian linestring
    ObIWkbGeomLineString iwkb_geom;
    iwkb_geom.set_data(data);
    ASSERT_EQ(OB_INVALID_ARGUMENT, iwkb_geom.do_visit(visitor));

    // tree geographical linestring
    ObGeographLineString t_geog;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geog.do_visit(visitor));

    // tree cartesian linestring
    ObCartesianLineString t_geom;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geom.do_visit(visitor));
}

TEST_F(TestGeoBin, reverse_coordinate_visitor_poly)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buffer(&allocator);
    uint32_t pnum = 100;
    uint32_t lnum = 10001;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_poly(buffer, lnum, pnum, xv, yv);
    ObString data = buffer.string();

    // wkb geographical polygon
    ObWkbGeogPolygon& poly = *reinterpret_cast<ObWkbGeogPolygon*>(data.ptr());
    ObIWkbGeogPolygon iwkb_geog;
    iwkb_geog.set_data(data);
    ObGeoReverseCoordinateVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(lnum, poly.size());
    ObWkbGeogLinearRing& ext = poly.exterior_ring();
    uint32_t index = 0;
    ASSERT_EQ(ext.size(), pnum);
    check_lines(ext, index, yv, xv);
    ObWkbGeogPolygonInnerRings::iterator iring_iter = poly.inner_rings().begin();
    ObWkbGeogPolygonInnerRings::iterator iring_end = poly.inner_rings().end();
    for (; iring_iter != iring_end; ++iring_iter) {
        ASSERT_EQ(iring_iter->size(), pnum);
        check_lines(*iring_iter, index, yv, xv);
    }

    // wkb cartesian polygon
    ObIWkbGeomPolygon iwkb_geom;
    iwkb_geom.set_data(data);
    ASSERT_EQ(OB_INVALID_ARGUMENT, iwkb_geom.do_visit(visitor));

    // tree geographical polygon
    ObGeographPolygon t_geog;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geog.do_visit(visitor));

    // tree cartesian polygon
    ObCartesianPolygon t_geom;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geom.do_visit(visitor));
}

TEST_F(TestGeoBin, reverse_coordinate_visitor_multi_point)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buffer(&allocator);
    uint32_t num = 100;
    common::ObVector<double> xv;
    common::ObVector<double> yv;

    append_multi_point(buffer, num, xv, yv);
    ObString data = buffer.string();
    ObWkbGeogMultiPoint& mpt = *reinterpret_cast<ObWkbGeogMultiPoint*>(data.ptr());

    // wkb geographical linestring
    ObIWkbGeogMultiPoint iwkb_geog;
    iwkb_geog.set_data(data);
    ObGeoReverseCoordinateVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));

    ObWkbGeogMultiPoint::iterator mpt_iter = mpt.begin();
    ObWkbGeogMultiPoint::iterator mpt_end = mpt.begin();
    ASSERT_EQ(mpt.size(), num);
    for (int i = 0; mpt_iter != mpt_end; ++mpt_iter, ++i) {
        ASSERT_EQ(mpt_iter->get<0>(), yv[i]);
        ASSERT_EQ(mpt_iter->get<1>(), xv[i]);
    }

    // wkb cartesian linestring
    ObIWkbGeomMultiPoint iwkb_geom;
    iwkb_geom.set_data(data);
    ASSERT_EQ(OB_INVALID_ARGUMENT, iwkb_geom.do_visit(visitor));

    // tree geographical linestring
    ObGeographMultipoint t_geog;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geog.do_visit(visitor));

    // tree cartesian linestring
    ObCartesianMultipoint t_geom;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geom.do_visit(visitor));
}

TEST_F(TestGeoBin, reverse_coordinate_visitor_multi_line)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buffer(&allocator);

    uint32_t pnum = 100;
    uint32_t lnum = 10000;
    common::ObVector<double> xv;
    common::ObVector<double> yv;
    append_multi_line(buffer, lnum, pnum, xv, yv);

    ObString data = buffer.string();
    ObWkbGeogMultiLineString& mline = *reinterpret_cast<ObWkbGeogMultiLineString*>(data.ptr());
    ObIWkbGeogMultiLineString iwkb_geog;
    iwkb_geog.set_data(data);
    ObGeoReverseCoordinateVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(lnum, mline.size());

    ObWkbGeogMultiLineString::iterator mline_iter = mline.begin();
    ObWkbGeogMultiLineString::iterator mline_end = mline.begin();
    uint32_t index = 0;
    for (; mline_iter != mline_end; ++mline_iter) {
        ASSERT_EQ(mline_iter->size(), pnum);
        check_lines(*mline_iter, index, yv, xv);
    }
}

TEST_F(TestGeoBin, reverse_coordinate_visitor_multi_polygon)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buffer(&allocator);
    uint32_t polynum = 100;
    uint32_t lnum = 100;
    uint32_t pnum = 100;
    common::ObVector<double> xv[polynum];
    common::ObVector<double> yv[polynum];
    ASSERT_EQ(OB_SUCCESS, append_bo(buffer));
    ASSERT_EQ(OB_SUCCESS, append_type(buffer, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(buffer, polynum));
    for (int i = 0; i < polynum; i++) {
        append_poly(buffer, lnum, pnum, xv[i], yv[i]);
    }

    ObString data = buffer.string();
    ObWkbGeogMultiPolygon& mp = *reinterpret_cast<ObWkbGeogMultiPolygon*>(data.ptr());
    ObIWkbGeogMultiPolygon iwkb_geog;
    iwkb_geog.set_data(data);
    ObGeoReverseCoordinateVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(polynum, mp.size());
    ObWkbGeogMultiPolygon::iterator iter = mp.begin();
    for (int i = 0; iter != mp.end(); ++iter, i++) {
        ASSERT_EQ(lnum, iter->size());
        uint32_t pc = 0;
        auto &ext_ring = iter->exterior_ring();
        ASSERT_EQ(pnum, ext_ring.size());
        check_lines(ext_ring, pc, yv[i], xv[i]);
        auto& inner_rings = iter->inner_rings();
        ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
        for (; riter != inner_rings.end(); riter++) {
            ASSERT_EQ(pnum, riter->size());
            check_lines(*riter, pc, yv[i], xv[i]);
        }
    }

    // wkb cartesian polygon
    ObIWkbGeomMultiPolygon iwkb_geom;
    iwkb_geom.set_data(data);
    ASSERT_EQ(OB_INVALID_ARGUMENT, iwkb_geom.do_visit(visitor));

    // tree geographical polygon
    ObGeographMultipolygon t_geog;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geog.do_visit(visitor));

    // tree cartesian polygon
    ObCartesianMultipolygon t_geom;
    ASSERT_EQ(OB_INVALID_ARGUMENT, t_geom.do_visit(visitor));
}

TEST_F(TestGeoBin, reverse_coordinate_visitor_gc)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer buffer(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(buffer));
    ASSERT_EQ(OB_SUCCESS, append_type(buffer, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(buffer, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(buffer, xv[0], yv[0]);
    // line
    append_line(buffer, 100, xv[1], yv[1]);
    // polygon
    append_poly(buffer, 100, 100, xv[2], yv[2]);
    // multipoint
    append_multi_point(buffer, 100, xv[3], yv[3]);
    // multiline
    append_multi_line(buffer, 1000, 10, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(buffer));
    ASSERT_EQ(OB_SUCCESS, append_type(buffer, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(buffer, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(buffer, 10, 100, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(buffer));
    ASSERT_EQ(OB_SUCCESS, append_type(buffer, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(buffer, 0));

    ObString data = buffer.string();
    ObWkbGeogCollection &gc = *reinterpret_cast<ObWkbGeogCollection*>(data.ptr());
    ObIWkbGeogCollection iwkb_geog;
    iwkb_geog.set_data(data);
    ObGeoReverseCoordinateVisitor visitor;
    ASSERT_EQ(OB_SUCCESS, iwkb_geog.do_visit(visitor));
    ASSERT_EQ(7, gc.size());

    ObWkbGeogCollection::iterator iter = gc.begin();
    for (int i = 0; iter != gc.end(); ++iter, ++i) {
        typename ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = gc.get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeogPoint* point = reinterpret_cast<const ObWkbGeogPoint*>(sub_ptr);
            ASSERT_EQ(yv[0][0], point->get<0>());
            ASSERT_EQ(xv[0][0], point->get<1>());
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeogLineString* line = reinterpret_cast<const ObWkbGeogLineString*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, line->size());
            check_lines(*line, pc, yv[1], xv[1]);
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
            uint32_t pc = 0;
            check_lines(poly->exterior_ring(), pc, yv[2], xv[2]);
            auto& inner_rings = poly->inner_rings();
            ASSERT_EQ(99, inner_rings.size());
            ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
            for (; riter != inner_rings.end(); riter++) {
                check_lines(*riter, pc, yv[2], xv[2]);
            }
            --riter;
            pc -= 100;
            for (; riter >= inner_rings.begin(); riter--, pc -= 100) {
                uint32_t tpc = pc;
                check_lines(*riter, tpc, yv[2], xv[2]);
            }
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeogMultiPoint* mp = reinterpret_cast<const ObWkbGeogMultiPoint*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(100, mp->size());
            check_lines(*mp, pc, yv[3], xv[3]);
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeogMultiLineString* ml = reinterpret_cast<const ObWkbGeogMultiLineString*>(sub_ptr);
            ASSERT_EQ(1000, ml->size());
            uint32_t pc = 0;
            ObWkbGeogMultiLineString::iterator liter = ml->begin();
            for (; liter != ml->end(); liter++) {
                check_lines(*liter, pc, yv[4], xv[4]);
            }
            liter--;
            pc -= 10;
            for (; liter >= ml->begin(); liter--, pc -= 10) {
                uint32_t tpc = pc;
                check_lines(*liter, tpc, yv[4], xv[4]);
            }
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeogMultiPolygon* mp = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
            uint32_t pc = 0;
            ASSERT_EQ(10, mp->size());
            ObWkbGeogMultiPolygon::iterator mpiter = mp->begin();
            for (; mpiter != mp->end(); ++mpiter) {
                ASSERT_EQ(10, mpiter->size());
                check_lines(mpiter->exterior_ring(), pc, yv[5], xv[5]);
                auto& inner_rings = mpiter->inner_rings();
                ASSERT_EQ(9, inner_rings.size());
                ObWkbGeogPolygonInnerRings::iterator riter = inner_rings.begin();
                for (; riter != inner_rings.end(); riter++) {
                    check_lines(*riter, pc, yv[5], xv[5]);
                }
                uint32_t rpc = pc;
                --riter;
                rpc -= 100;
                for (; riter >= inner_rings.begin(); riter--, rpc -= 100) {
                    uint32_t tpc = rpc;
                    check_lines(*riter, tpc, yv[5], xv[5]);
                }
            }
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeogCollection* subgc = reinterpret_cast<const ObWkbGeogCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
}

void create_polygon(ObJsonBuffer &data, int lnum, int pnum, std::vector< std::pair<double, double> > &value)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, lnum));
    int i = 0;
    for (int l = 0; l < lnum; l++) {
      ASSERT_EQ(OB_SUCCESS, append_uint32(data, pnum));
      for (int p = 0; p < pnum; p++) {
        ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].first));
        ASSERT_EQ(OB_SUCCESS, append_double(data, value[i].second));
        i++;
      }
    }
}

TEST_F(TestGeoBin, geo_close_ring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // 1 exterior line 100 inner line, every line has 100 point
    uint32_t pnum = 4;
    uint32_t lnum = 2;

    std::vector< std::pair<double, double> > val1;
    val1.push_back(std::make_pair(1.0, 0.5));
    val1.push_back(std::make_pair(3.0, 0.5));
    val1.push_back(std::make_pair(3.0, 1.0));
    val1.push_back(std::make_pair(1.0, 1.0));
    val1.push_back(std::make_pair(1.0, 0.5));
    val1.push_back(std::make_pair(3.0, 0.5));
    val1.push_back(std::make_pair(3.0, 1.0));
    val1.push_back(std::make_pair(1.0, 1.0));
    create_polygon(data, lnum, pnum, val1);

    ObWkbGeogPolygon& poly = *reinterpret_cast<ObWkbGeogPolygon*>(data.ptr());
    ObIWkbGeogPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());
    std::cout << "poly: " << boost::geometry::dsv(poly) << std::endl;
    ASSERT_EQ(OB_SUCCESS, ObGeoTypeUtil::geo_close_ring(iwkb_geog, allocator));
    ObWkbGeogPolygon *res_poly = reinterpret_cast<ObWkbGeogPolygon*>(iwkb_geog.val());
    std::cout << "after do close poly: " << boost::geometry::dsv(*res_poly) << std::endl;


  ObJsonBuffer data1(&allocator);
  ASSERT_EQ(OB_SUCCESS, append_bo(data1));
  ASSERT_EQ(OB_SUCCESS, append_type(data1, ObGeoType::MULTIPOLYGON));
  uint32_t polygon_num = 2;
  ASSERT_EQ(OB_SUCCESS, append_uint32(data1, polygon_num));
  for (uint32_t i = 0; i < polygon_num; i++) {
    std::vector< std::pair<double, double> > val;
    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));

    val.push_back(std::make_pair(0.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 0.0 + 2 * i));
    val.push_back(std::make_pair(2.0, 1.0 + 2 * i));
    val.push_back(std::make_pair(0.0, 1.0 + 2 * i));

    create_polygon(data1, 2, 4, val);
  }

  ObIWkbGeogMultiPolygon multi_poly;
  multi_poly.set_data(data1.string());

  const ObWkbGeogMultiPolygon *geo2 = reinterpret_cast<const ObWkbGeogMultiPolygon *>(multi_poly.val());
  ASSERT_EQ(OB_SUCCESS, ObGeoTypeUtil::geo_close_ring(multi_poly, allocator));
  std::cout << "after do close multipoly: " << multi_poly.size() << std::endl;
  ObWkbGeogMultiPolygon *res_multipoly = reinterpret_cast<ObWkbGeogMultiPolygon*>(multi_poly.val());
  std::cout << "after do close multipoly: " << boost::geometry::dsv(*res_multipoly) << std::endl;
}

TEST_F(TestGeoBin, collection_close_ring)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data, xv[0], yv[0]);
    // line
    append_line(data, 1, xv[1], yv[1]);
    // polygon
    append_poly(data, 2, 4, xv[2], yv[2]);
    // multipoint
    append_multi_point(data, 1, xv[3], yv[3]);
    // multiline
    append_multi_line(data, 1, 3, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 2));
    for (int i = 0; i < 2; i++) {
        append_poly(data, 2, 4, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 0));

    ObIWkbGeogCollection iwkb_geog;
    iwkb_geog.set_data(data.string());

    const ObWkbGeogCollection *geo2 = reinterpret_cast<const ObWkbGeogCollection *>(iwkb_geog.val());
    ObWkbGeogCollection::iterator iter = geo2->begin();
    for (int i = 0; iter != geo2->end(); ++iter, ++i) {
        typename ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeogPoint* point = reinterpret_cast<const ObWkbGeogPoint*>(sub_ptr);
            std::cout << "point: " << boost::geometry::dsv(*point) << std::endl;
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeogLineString* line = reinterpret_cast<const ObWkbGeogLineString*>(sub_ptr);
            std::cout << "line: " << boost::geometry::dsv(*line) << std::endl;
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
            std::cout << "poly: " << boost::geometry::dsv(*poly) << std::endl;
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeogMultiPoint* mp = reinterpret_cast<const ObWkbGeogMultiPoint*>(sub_ptr);
            std::cout << "multipoint: " << boost::geometry::dsv(*mp) << std::endl;
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeogMultiLineString* ml = reinterpret_cast<const ObWkbGeogMultiLineString*>(sub_ptr);
            std::cout << "multiline: " << boost::geometry::dsv(*ml) << std::endl;
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeogMultiPolygon* mp = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
            std::cout << "multipoly: " << boost::geometry::dsv(*mp) << std::endl;
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeogCollection* subgc = reinterpret_cast<const ObWkbGeogCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
    ASSERT_EQ(OB_SUCCESS, ObGeoTypeUtil::geo_close_ring(iwkb_geog, allocator));
    geo2 = reinterpret_cast<const ObWkbGeogCollection *>(iwkb_geog.val());
    iter = geo2->begin();
    for (int i = 0; iter != geo2->end(); ++iter, ++i) {
        typename ObWkbGeogCollection::const_pointer sub_ptr = iter.operator->();
        ObGeoType sub_type = geo2->get_sub_type(sub_ptr);
        ASSERT_EQ(i + 1, static_cast<int>(sub_type));
        if (sub_type == ObGeoType::POINT) {
            const ObWkbGeogPoint* point = reinterpret_cast<const ObWkbGeogPoint*>(sub_ptr);
            std::cout << "point: " << boost::geometry::dsv(*point) << std::endl;
        } else if (sub_type == ObGeoType::LINESTRING) {
            const ObWkbGeogLineString* line = reinterpret_cast<const ObWkbGeogLineString*>(sub_ptr);
            std::cout << "line: " << boost::geometry::dsv(*line) << std::endl;
        } else if (sub_type == ObGeoType::POLYGON) {
            const ObWkbGeogPolygon* poly = reinterpret_cast<const ObWkbGeogPolygon*>(sub_ptr);
            std::cout << "poly: " << boost::geometry::dsv(*poly) << std::endl;
        } else if (sub_type == ObGeoType::MULTIPOINT) {
            const ObWkbGeogMultiPoint* mp = reinterpret_cast<const ObWkbGeogMultiPoint*>(sub_ptr);
            std::cout << "multipoint: " << boost::geometry::dsv(*mp) << std::endl;
        } else if (sub_type == ObGeoType::MULTILINESTRING) {
            const ObWkbGeogMultiLineString* ml = reinterpret_cast<const ObWkbGeogMultiLineString*>(sub_ptr);
            std::cout << "multiline: " << boost::geometry::dsv(*ml) << std::endl;
        } else if (sub_type == ObGeoType::MULTIPOLYGON) {
            const ObWkbGeogMultiPolygon* mp = reinterpret_cast<const ObWkbGeogMultiPolygon*>(sub_ptr);
            std::cout << "multipoly: " << boost::geometry::dsv(*mp) << std::endl;
        } else if (sub_type == ObGeoType::GEOMETRYCOLLECTION) {
            const ObWkbGeogCollection* subgc = reinterpret_cast<const ObWkbGeogCollection*>(sub_ptr);
            ASSERT_EQ(0, subgc->size());
            ASSERT_EQ(subgc->begin(), subgc->end());
        }
    }
}

TEST_F(TestGeoBin, mbr_polygon_1)
{
    ObSrsBoundsItem srsbound_max;
    srsbound_max.minX_ = (double)-10000000;
    srsbound_max.maxX_ = (double)10000000;
    srsbound_max.minY_ = (double)-10000000;
    srsbound_max.maxY_ = (double)10000000;

    ObArenaAllocator allocator(ObModIds::TEST);

    ObJsonBuffer data(&allocator);
    // ------------------ test polygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 1));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 5));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)250000));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)2394090));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)355520));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)2394090));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)355520));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)2727385));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)250000));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)2727385));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)250000));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)2394090));

    ObIWkbGeomPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());

    ObGeometry *geo1 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog, geo1);
    ObWkbGeomPolygon *mbr_polygon1 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo1->val()));
    std::cout << "ob::poly" << boost::geometry::dsv(*mbr_polygon1) << std::endl;

    // change point order invalid test
    ObJsonBuffer data2(&allocator);
    // ------------------ test invalid polygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data2));
    ASSERT_EQ(OB_SUCCESS, append_type(data2, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data2, 1));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data2, 5));

    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)250000));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)2394090));

    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)355520));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)2394090));

    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)250000));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)2727385));

    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)355520));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)2727385));

    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)250000));
    ASSERT_EQ(OB_SUCCESS, append_double(data2, (double)2394090));

    ObIWkbGeomPolygon iwkb_geog2;
    iwkb_geog2.set_data(data2.string());

    ObGeometry *geo2 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog, geo2);
    ObWkbGeomPolygon *mbr_polygon2 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo2->val()));
    std::cout << "ob::invalid poly" << boost::geometry::dsv(*mbr_polygon2) << std::endl;

    // line string test2
    ObJsonBuffer data3b(&allocator);
    // ------------------ test linestring
    ASSERT_EQ(OB_SUCCESS, append_bo(data3b));
    ASSERT_EQ(OB_SUCCESS, append_type(data3b, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data3b, 2));

    ASSERT_EQ(OB_SUCCESS, append_double(data3b, (double)10));
    ASSERT_EQ(OB_SUCCESS, append_double(data3b, (double)10));

    ASSERT_EQ(OB_SUCCESS, append_double(data3b, (double)20));
    ASSERT_EQ(OB_SUCCESS, append_double(data3b, (double)20));

    ObIWkbGeomLineString iwkb_geog3b;
    iwkb_geog3b.set_data(data3b.string());

    ObGeometry *geo3b = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog3b, geo3b);
    ObWkbGeomPolygon *mbr_polygon3b = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo3b->val()));
    std::cout << "ob::linestring1" << boost::geometry::dsv(*mbr_polygon3b) << std::endl;

    // line string test
    ObJsonBuffer data3(&allocator);
    // ------------------ test linestring
    ASSERT_EQ(OB_SUCCESS, append_bo(data3));
    ASSERT_EQ(OB_SUCCESS, append_type(data3, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data3, 2));

    ASSERT_EQ(OB_SUCCESS, append_double(data3, (double)10));
    ASSERT_EQ(OB_SUCCESS, append_double(data3, (double)15));

    ASSERT_EQ(OB_SUCCESS, append_double(data3, (double)20));
    ASSERT_EQ(OB_SUCCESS, append_double(data3, (double)15));

    ObIWkbGeomLineString iwkb_geog3;
    iwkb_geog3.set_data(data3.string());

    ObGeometry *geo3 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog3, geo3);
    ASSERT_EQ(ObGeoType::LINESTRING, geo3->type());
    ObWkbGeomLineString *mbr_polygon3 = reinterpret_cast<ObWkbGeomLineString *>(const_cast<char *>(geo3->val()));
    std::cout << "ob::linestring2" << boost::geometry::dsv(*mbr_polygon3) << std::endl;

    // test point
    ObJsonBuffer data4(&allocator);
    // ------------------ test point
    ASSERT_EQ(OB_SUCCESS, append_bo(data4));
    ASSERT_EQ(OB_SUCCESS, append_type(data4, ObGeoType::POINT));

    ASSERT_EQ(OB_SUCCESS, append_double(data4, (double)-10));
    ASSERT_EQ(OB_SUCCESS, append_double(data4, (double)-15));

    ObIWkbGeomPoint iwkb_geog4;
    iwkb_geog4.set_data(data4.string());

    ObGeometry *geo4 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog4, geo4);
    ASSERT_EQ(ObGeoType::POINT, geo4->type());
    ObWkbGeomPoint *mbr_polygon4 = reinterpret_cast<ObWkbGeomPoint *>(const_cast<char *>(geo4->val()));
    std::cout << "ob::point" << boost::geometry::dsv(*mbr_polygon4) << std::endl;

    // test collection
    ObJsonBuffer data5(&allocator);
    // ------------------ test collection
    ASSERT_EQ(OB_SUCCESS, append_bo(data5));
    ASSERT_EQ(OB_SUCCESS, append_type(data5, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data5, 7));
    common::ObVector<double> xv[7];
    common::ObVector<double> yv[7];
    // point
    append_random_point(data5, xv[0], yv[0]);
    // line
    append_line(data5, 100, xv[1], yv[1]);
    // polygon
    append_poly(data5, 100, 100, xv[2], yv[2]);
    // multipoint
    append_multi_point(data5, 100, xv[3], yv[3]);
    // multiline
    append_multi_line(data5, 1000, 10, xv[4], yv[4]);
    // multipolygon
    ASSERT_EQ(OB_SUCCESS, append_bo(data5));
    ASSERT_EQ(OB_SUCCESS, append_type(data5, ObGeoType::MULTIPOLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data5, 10));
    for (int i = 0; i < 10; i++) {
        append_poly(data5, 10, 100, xv[5], yv[5]);
    }
    // empty geometry
    ASSERT_EQ(OB_SUCCESS, append_bo(data5));
    ASSERT_EQ(OB_SUCCESS, append_type(data5, ObGeoType::GEOMETRYCOLLECTION));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data5, 0));

    ObIWkbGeomCollection iwkb_geog5;
    iwkb_geog5.set_data(data5.string());

    ObGeometry *geo5 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog5, geo5);
    ASSERT_EQ(ObGeoType::POLYGON, geo5->type());
    ObWkbGeomPolygon *mbr_polygon5 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo5->val()));
    std::cout << "ob::mpoly5" << boost::geometry::dsv(*mbr_polygon5) << std::endl;
}

static void test_geo_bin_make_rect(ObIAllocator &allocator, ObJsonBuffer &data, double minx, double maxx, double miny, double maxy)
{
    ASSERT_EQ(OB_SUCCESS, append_bo(data));
    ASSERT_EQ(OB_SUCCESS, append_type(data, ObGeoType::POLYGON));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 1));
    ASSERT_EQ(OB_SUCCESS, append_uint32(data, 5));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)minx));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)miny));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)maxx));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)miny));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)maxx));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)maxy));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)minx));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)maxy));

    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)minx));
    ASSERT_EQ(OB_SUCCESS, append_double(data, (double)miny));
}

TEST_F(TestGeoBin, mbr_polygon_2)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer data(&allocator);
    // ------------------ test polygon
    test_geo_bin_make_rect(allocator, data, (double)-1, (double)1, (double)-1, (double)1);

    ObIWkbGeomPolygon iwkb_geog;
    iwkb_geog.set_data(data.string());

    ObSrsBoundsItem srsbound_max;
    srsbound_max.minX_ = (double)0;
    srsbound_max.maxX_ = (double)2;
    srsbound_max.minY_ = (double)0;
    srsbound_max.maxY_ = (double)2;

    ObGeometry *geo1 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog, geo1);
    ObWkbGeomPolygon *mbr_polygon1 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo1->val()));
    std::cout << "ob::poly 1" << boost::geometry::dsv(*mbr_polygon1) << std::endl;

    srsbound_max.minX_ = (double)-2;
    srsbound_max.maxX_ = (double)0;
    srsbound_max.minY_ = (double)0;
    srsbound_max.maxY_ = (double)2;

    ObGeometry *geo2 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog, geo2);
    ObWkbGeomPolygon *mbr_polygon2 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo2->val()));
    std::cout << "ob::poly 2" << boost::geometry::dsv(*mbr_polygon2) << std::endl;

    srsbound_max.minX_ = (double)-2;
    srsbound_max.maxX_ = (double)0;
    srsbound_max.minY_ = (double)-2;
    srsbound_max.maxY_ = (double)0;

    ObGeometry *geo3 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog, geo3);
    ObWkbGeomPolygon *mbr_polygon3 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo3->val()));
    std::cout << "ob::poly 3" << boost::geometry::dsv(*mbr_polygon3) << std::endl;

    srsbound_max.minX_ = (double)0;
    srsbound_max.maxX_ = (double)2;
    srsbound_max.minY_ = (double)-2;
    srsbound_max.maxY_ = (double)0;

    ObGeometry *geo4 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geog, geo4);
    ObWkbGeomPolygon *mbr_polygon4 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geo4->val()));
    std::cout << "ob::poly 4" << boost::geometry::dsv(*mbr_polygon4) << std::endl;


    srsbound_max.minX_ = (double)-1;
    srsbound_max.maxX_ = (double)1;
    srsbound_max.minY_ = (double)-1;
    srsbound_max.maxY_ = (double)1;
    // ------------------ test linestring
    ObJsonBuffer datal1(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(datal1));
    ASSERT_EQ(OB_SUCCESS, append_type(datal1, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(datal1, 2));

    ASSERT_EQ(OB_SUCCESS, append_double(datal1, (double)0));
    ASSERT_EQ(OB_SUCCESS, append_double(datal1, (double)-2));

    ASSERT_EQ(OB_SUCCESS, append_double(datal1, (double)0));
    ASSERT_EQ(OB_SUCCESS, append_double(datal1, (double)2));

    ObIWkbGeomLineString iwkb_geogl1;
    iwkb_geogl1.set_data(datal1.string());

    ObGeometry *geol1 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geogl1, geol1);
    ASSERT_EQ(ObGeoType::LINESTRING, geol1->type());
    ObWkbGeomLineString *mbr_polygonl1 = reinterpret_cast<ObWkbGeomLineString *>(const_cast<char *>(geol1->val()));
    std::cout << "ob::linestring1" << boost::geometry::dsv(*mbr_polygonl1) << std::endl;


    ObJsonBuffer datal2(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(datal2));
    ASSERT_EQ(OB_SUCCESS, append_type(datal2, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(datal2, 2));

    ASSERT_EQ(OB_SUCCESS, append_double(datal2, (double)-2));
    ASSERT_EQ(OB_SUCCESS, append_double(datal2, (double)0));

    ASSERT_EQ(OB_SUCCESS, append_double(datal2, (double)2));
    ASSERT_EQ(OB_SUCCESS, append_double(datal2, (double)0));

    ObIWkbGeomLineString iwkb_geogl2;
    iwkb_geogl2.set_data(datal2.string());

    ObGeometry *geol2 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geogl2, geol2);
    ASSERT_EQ(ObGeoType::LINESTRING, geol2->type());
    ObWkbGeomLineString *mbr_polygonl2 = reinterpret_cast<ObWkbGeomLineString *>(const_cast<char *>(geol2->val()));
    std::cout << "ob::linestring2" << boost::geometry::dsv(*mbr_polygonl2) << std::endl;


    ObJsonBuffer datal3(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(datal3));
    ASSERT_EQ(OB_SUCCESS, append_type(datal3, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(datal3, 2));

    ASSERT_EQ(OB_SUCCESS, append_double(datal3, (double)-10));
    ASSERT_EQ(OB_SUCCESS, append_double(datal3, (double)-10));

    ASSERT_EQ(OB_SUCCESS, append_double(datal3, (double)10));
    ASSERT_EQ(OB_SUCCESS, append_double(datal3, (double)10));

    ObIWkbGeomLineString iwkb_geogl3;
    iwkb_geogl3.set_data(datal3.string());

    ObGeometry *geol3 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geogl3, geol3);
    ASSERT_EQ(ObGeoType::POLYGON, geol3->type());
    ObWkbGeomPolygon *mbr_polygonl3 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geol3->val()));
    std::cout << "ob::linestring3(poly)" << boost::geometry::dsv(*mbr_polygonl3) << std::endl;


    ObJsonBuffer datal4(&allocator);
    ASSERT_EQ(OB_SUCCESS, append_bo(datal4));
    ASSERT_EQ(OB_SUCCESS, append_type(datal4, ObGeoType::LINESTRING));
    ASSERT_EQ(OB_SUCCESS, append_uint32(datal4, 2));

    ASSERT_EQ(OB_SUCCESS, append_double(datal4, (double)-2));
    ASSERT_EQ(OB_SUCCESS, append_double(datal4, (double)1));

    ASSERT_EQ(OB_SUCCESS, append_double(datal4, (double)2));
    ASSERT_EQ(OB_SUCCESS, append_double(datal4, (double)1));

    ObIWkbGeomLineString iwkb_geogl4;
    iwkb_geogl4.set_data(datal4.string());

    ObGeometry *geol4 = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geogl4, geol4);
    ASSERT_EQ(ObGeoType::POLYGON, geol4->type());
    ObWkbGeomPolygon *mbr_polygonl4 = reinterpret_cast<ObWkbGeomPolygon *>(const_cast<char *>(geol4->val()));
    std::cout << "ob::linestring5(poly)" << boost::geometry::dsv(*mbr_polygonl4) << std::endl;


    srsbound_max.minX_ = (double)0;
    srsbound_max.maxX_ = (double)2;
    srsbound_max.minY_ = (double)0;
    srsbound_max.maxY_ = (double)2;


    ObJsonBuffer datap(&allocator);
    // ------------------ test point1
    ASSERT_EQ(OB_SUCCESS, append_bo(datap));
    ASSERT_EQ(OB_SUCCESS, append_type(datap, ObGeoType::POINT));

    ASSERT_EQ(OB_SUCCESS, append_double(datap, (double)1));
    ASSERT_EQ(OB_SUCCESS, append_double(datap, (double)1));

    ObIWkbGeomPoint iwkb_geogp;
    iwkb_geogp.set_data(datap.string());

    ObGeometry *geop = NULL;
    ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geogp, geop);
    ASSERT_EQ(ObGeoType::POINT, geop->type());
    ObWkbGeomPoint *mbr_polygonp = reinterpret_cast<ObWkbGeomPoint *>(const_cast<char *>(geop->val()));
    std::cout << "ob::point1" << boost::geometry::dsv(*mbr_polygonp) << std::endl;


    ObJsonBuffer datap2(&allocator);
    // ------------------ test point2
    ASSERT_EQ(OB_SUCCESS, append_bo(datap2));
    ASSERT_EQ(OB_SUCCESS, append_type(datap2, ObGeoType::POINT));

    ASSERT_EQ(OB_SUCCESS, append_double(datap2, (double)-1));
    ASSERT_EQ(OB_SUCCESS, append_double(datap2, (double)-1));

    ObIWkbGeomPoint iwkb_geogp2;
    iwkb_geogp2.set_data(datap2.string());

    ObGeometry *geop2 = NULL;
    ASSERT_EQ(ObGeoTypeUtil::get_mbr_polygon(allocator, &srsbound_max, iwkb_geogp2, geop2), OB_EMPTY_RESULT);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_geo_bin.log");
  // OB_LOGGER.set_file_name("test_geo_bin.log");
  // OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}