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
#define private public
#include "lib/geo/ob_srs_info.h"
#include "lib/geo/ob_srs_wkt_parser.h"
#include "lib/random/ob_random.h"
#include "observer/omt/ob_tenant_srs.h"
#include "share/schema/ob_multi_version_schema_service.h"
#undef private
#include <sys/time.h>
namespace oceanbase {
using namespace oceanbase::share::schema;
using namespace omt;
namespace common {
static common::ObMySQLProxy sql_proxy;
class TestGeoSrs : public ::testing::Test {
public:
  TestGeoSrs()
  {}
  ~TestGeoSrs()
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
  DISALLOW_COPY_AND_ASSIGN(TestGeoSrs);
};
#if 0
TEST_F(TestGeoSrs, mock_srs_info)
{
  ObGeographicRs raw_rs;
  ObString wkt;
  ASSERT_EQ(OB_SUCCESS, srs_wkt_parser::mock_parse_geographic_coordinate_system(wkt, &raw_rs));

  ObSpatialReferenceSystemBase *srs_info = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, ObSpatialReferenceSystemBase::create_geographic_srs(&allocator, 4326, &raw_rs, srs_info));
  ASSERT_TRUE( srs_info != NULL );
}
TEST_F(TestGeoSrs, srs_mgr_srsid_4326)
{
  omt::ObTenantSrsMgr &tenant_srs_mgr_instance = OTSRS_MGR;
  ObAddr tmp_addr(ObAddr::IPV4, "127.0.0.1", 80);
  ObMultiVersionSchemaService &tmp_schema_service = ObMultiVersionSchemaService::get_instance();
  ASSERT_EQ(OB_SUCCESS, tenant_srs_mgr_instance.init(&sql_proxy, tmp_addr, &tmp_schema_service));
  const ObSrsItem *srs_item = NULL;
  uint64_t srs_id = 4326;
  ASSERT_EQ(OB_HASH_NOT_EXIST, tenant_srs_mgr_instance.get_tenant_srs_item(OB_SYS_TENANT_ID, 1213, srs_item));
  ASSERT_EQ(OB_HASH_NOT_EXIST, tenant_srs_mgr_instance.get_tenant_srs_item(1001, srs_id, srs_item));
  ASSERT_EQ(OB_SUCCESS, tenant_srs_mgr_instance.get_tenant_srs_item(OB_SYS_TENANT_ID, srs_id, srs_item));
  ASSERT_EQ(srs_item->is_wgs84(), true);
  ASSERT_EQ(srs_item->srs_type(), ObSrsType::GEOGRAPHIC_SRS);
  ASSERT_EQ(srs_item->is_lat_long_order(), true);
  ASSERT_EQ(srs_item->is_latitude_north(), true);
  ASSERT_EQ(srs_item->is_longtitude_east(), true);
  ASSERT_TRUE(std::abs(srs_item->prime_meridian() - 0.0) < 0.001);
  ASSERT_TRUE(std::abs(srs_item->linear_uint() - 1.0) < 0.001);
  ASSERT_TRUE(std::abs(srs_item->angular_unit() - 0.017453292519943278) < 0.001);
  ASSERT_TRUE(std::abs(srs_item->semi_major_axis() - 6378137) < 0.001);
  ASSERT_TRUE(std::abs(srs_item->semi_minor_axis() - 6356752.314245) < 0.001);

  double res = 0.0;
  double val = 0.26179938779914919;
  ASSERT_EQ(OB_SUCCESS, srs_item->latitude_convert_to_radians(15, res));
  ASSERT_TRUE(std::abs(res - val) < 0.001);

  ASSERT_EQ(OB_SUCCESS, srs_item->latitude_convert_from_radians(val, res));
  ASSERT_TRUE(std::abs(res - 15) < 0.001);
  ASSERT_EQ(OB_SUCCESS, srs_item->longtitude_convert_to_radians(15, res));
  ASSERT_TRUE(std::abs(res - val) < 0.001);

  ASSERT_EQ(OB_SUCCESS, srs_item->longtitude_convert_from_radians(val, res));
  ASSERT_TRUE(std::abs(res - 15) < 0.001);

  tenant_srs_mgr_instance.destroy();
}
#endif
} // namespace common
} // namespace oceanbase
int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_bin.log");
  OB_LOGGER.set_file_name("test_json_bin.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}