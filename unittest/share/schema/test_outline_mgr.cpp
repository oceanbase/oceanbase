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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#define private public
#include "share/schema/ob_outline_mgr.h"

namespace oceanbase {
using namespace share::schema;

namespace common {

class TestOutlineMgr : public ::testing::Test {};

#define GEN_OUTLINE_SCHEMA(outline_schema, tenant_id, outline_id, database_id, name, signature, schema_version) \
  outline_schema.reset();                                                                                       \
  outline_schema.set_tenant_id(tenant_id);                                                                      \
  outline_schema.set_outline_id(outline_id);                                                                    \
  outline_schema.set_database_id(database_id);                                                                  \
  outline_schema.set_name(name);                                                                                \
  outline_schema.set_signature(signature);                                                                      \
  outline_schema.set_schema_version(schema_version);

#define OUTLINE_EQUAL(a, b)                                    \
  ASSERT_EQ((a).get_tenant_id(), (b).get_tenant_id());         \
  ASSERT_EQ((a).get_outline_id(), (b).get_outline_id());       \
  ASSERT_EQ((a).get_database_id(), (b).get_database_id());     \
  ASSERT_EQ((a).get_name_str(), (b).get_name_str());           \
  ASSERT_EQ((a).get_signature_str(), (b).get_signature_str()); \
  ASSERT_EQ((a).get_schema_version(), (b).get_schema_version());

TEST_F(TestOutlineMgr, basic_interface)
{
  ObOutlineMgr outline_mgr;
  uint64_t tenant_id = 1;
  uint64_t outline_id = combine_id(1, 1);
  uint64_t database_id = combine_id(1, 1);
  ObString name;
  ObString signature;
  ObSimpleOutlineSchema outline_schema;
  ObArray<const ObSimpleOutlineSchema*> outlines;

  // add_outline
  outline_mgr.reset();
  tenant_id = 1;
  outline_id = combine_id(1, 1);
  database_id = combine_id(1, 1);
  name = "outline1";
  signature = "sig";
  const ObSimpleOutlineSchema* outline = NULL;
  GEN_OUTLINE_SCHEMA(outline_schema, tenant_id, outline_id, database_id, name, signature, 0);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
  // get outline by id
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schema(outline_id, outline));
  OUTLINE_EQUAL(outline_schema, *outline);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schema(combine_id(1, 2), outline));
  ASSERT_EQ(NULL, outline);
  // get outline by name
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schema_with_name(tenant_id, database_id, name, outline));
  OUTLINE_EQUAL(outline_schema, *outline);
  ASSERT_EQ(
      OB_SUCCESS, outline_mgr.get_outline_schema_with_signature(tenant_id, database_id, "outline_not_exist", outline));
  ASSERT_EQ(NULL, outline);
  // del_outline
  ASSERT_EQ(OB_SUCCESS, outline_mgr.del_outline(ObTenantOutlineId(tenant_id, outline_id)));
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schema(outline_id, outline));
  ASSERT_EQ(NULL, outline);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schema_with_name(tenant_id, database_id, name, outline));
  ASSERT_EQ(NULL, outline);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schema_with_signature(tenant_id, database_id, signature, outline));
  ASSERT_EQ(NULL, outline);
  // add outlines
  outline_mgr.reset();
  ObArray<ObSimpleOutlineSchema> outline_schemas;
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 1), combine_id(1, 1), "outline1", "sig1", 0);
  outline_schemas.push_back(outline_schema);
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 2), combine_id(1, 1), "outline2", "sig2", 0);
  outline_schemas.push_back(outline_schema);
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 3), combine_id(1, 1), "outline3", "sig3", 0);
  outline_schemas.push_back(outline_schema);
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 4), combine_id(1, 1), "outline4", "sig4", 0);
  outline_schemas.push_back(outline_schema);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outlines(outline_schemas));
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_tenant(tenant_id, outlines));
  ASSERT_EQ(4, outlines.count());
  for (int64_t i = 0; i < 4; ++i) {
    OUTLINE_EQUAL(*outlines.at(i), outline_schemas.at(i));
  }
  // del outlines
  ObArray<ObTenantOutlineId> tenant_outline_ids;
  tenant_outline_ids.push_back(ObTenantOutlineId(1, combine_id(1, 1)));
  tenant_outline_ids.push_back(ObTenantOutlineId(1, combine_id(1, 2)));
  tenant_outline_ids.push_back(ObTenantOutlineId(1, combine_id(1, 3)));
  tenant_outline_ids.push_back(ObTenantOutlineId(1, combine_id(1, 4)));
  ASSERT_EQ(OB_SUCCESS, outline_mgr.del_outlines(tenant_outline_ids));
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_tenant(tenant_id, outlines));
  ASSERT_EQ(0, outlines.count());

  // del schemas in tenant
  outline_mgr.reset();
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 1), combine_id(1, 1), "outline1", "sig1", 0);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 2), combine_id(1, 1), "outline2", "sig2", 0);
  ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_tenant(1, outlines));
  ASSERT_EQ(2, outlines.count());
  ASSERT_EQ(OB_SUCCESS, outline_mgr.del_schemas_in_tenant(1));
  ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_tenant(1, outlines));
  ASSERT_EQ(0, outlines.count());
}

TEST_F(TestOutlineMgr, iteration_interface)
{
  {
    // get outlines in tenant
    ObOutlineMgr outline_mgr;
    ObArray<ObSimpleOutlineSchema> outline_schemas1;
    ObArray<ObSimpleOutlineSchema> outline_schemas2;
    ObArray<const ObSimpleOutlineSchema*> outlines;
    ObSimpleOutlineSchema outline_schema;
    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 1), combine_id(1, 1), "outline1", "sig1", 0);
    outline_schemas1.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 2), combine_id(1, 1), "outline2", "sig2", 0);
    outline_schemas1.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    GEN_OUTLINE_SCHEMA(outline_schema, 2, combine_id(2, 1), combine_id(2, 1), "outline3", "sig3", 0);
    outline_schemas2.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    GEN_OUTLINE_SCHEMA(outline_schema, 2, combine_id(2, 2), combine_id(2, 1), "outline4", "sig4", 0);
    outline_schemas2.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_tenant(1, outlines));
    ASSERT_EQ(2, outlines.count());
    for (int64_t i = 1; i < outlines.count(); ++i) {
      const ObSimpleOutlineSchema* simple_outline = outlines.at(i);
      for (int64_t j = 1; j < outline_schemas1.count(); ++j) {
        const ObSimpleOutlineSchema& outline = outline_schemas1.at(j);
        if (simple_outline->get_outline_id() == outline.get_outline_id()) {
          OUTLINE_EQUAL((*simple_outline), outline);
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_tenant(2, outlines));
    ASSERT_EQ(2, outlines.count());
    for (int64_t i = 1; i < outlines.count(); ++i) {
      const ObSimpleOutlineSchema* simple_outline = outlines.at(i);
      for (int64_t j = 1; j < outline_schemas2.count(); ++j) {
        const ObSimpleOutlineSchema& outline = outline_schemas2.at(j);
        if (simple_outline->get_outline_id() == outline.get_outline_id()) {
          OUTLINE_EQUAL((*simple_outline), outline);
        }
      }
    }
  }

  {
    // get outlines in database
    ObOutlineMgr outline_mgr;
    ObArray<ObSimpleOutlineSchema> outline_schemas1;
    ObArray<ObSimpleOutlineSchema> outline_schemas2;
    ObArray<const ObSimpleOutlineSchema*> outlines;
    ObSimpleOutlineSchema outline_schema;
    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 1), combine_id(1, 1), "outline1", "sig1", 0);
    outline_schemas1.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 2), combine_id(1, 1), "outline2", "sig2", 0);
    outline_schemas1.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 3), combine_id(1, 2), "outline3", "sig3", 0);
    outline_schemas2.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 4), combine_id(1, 2), "outline4", "sig4", 0);
    outline_schemas2.push_back(outline_schema);
    ASSERT_EQ(OB_SUCCESS, outline_mgr.add_outline(outline_schema));
    ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_database(1, combine_id(1, 1), outlines));
    ASSERT_EQ(2, outlines.count());
    for (int64_t i = 1; i < outlines.count(); ++i) {
      const ObSimpleOutlineSchema* simple_outline = outlines.at(i);
      for (int64_t j = 1; j < outline_schemas1.count(); ++j) {
        const ObSimpleOutlineSchema& outline = outline_schemas1.at(j);
        if (simple_outline->get_outline_id() == outline.get_outline_id()) {
          OUTLINE_EQUAL((*simple_outline), outline);
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, outline_mgr.get_outline_schemas_in_database(1, combine_id(1, 2), outlines));
    ASSERT_EQ(2, outlines.count());
    for (int64_t i = 1; i < outlines.count(); ++i) {
      const ObSimpleOutlineSchema* simple_outline = outlines.at(i);
      for (int64_t j = 1; j < outline_schemas2.count(); ++j) {
        const ObSimpleOutlineSchema& outline = outline_schemas2.at(j);
        if (simple_outline->get_outline_id() == outline.get_outline_id()) {
          OUTLINE_EQUAL((*simple_outline), outline);
        }
      }
    }
  }
}

TEST_F(TestOutlineMgr, assign_and_deep_copy)
{
  ObMalloc global_allocator(ObModIds::OB_TEMP_VARIABLES);
  ObOutlineMgr mgr1(global_allocator);
  ObOutlineMgr mgr2;
  ObOutlineMgr mgr3;
  ObSimpleOutlineSchema outline_schema;
  GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 1), combine_id(1, 1), "sys_outline", "sig", 0);
  ASSERT_EQ(OB_SUCCESS, mgr1.add_outline(outline_schema));
  ASSERT_EQ(OB_SUCCESS, mgr2.assign(mgr1));
  ASSERT_EQ(0, mgr2.local_allocator_.used());
  ASSERT_EQ(OB_SUCCESS, mgr3.deep_copy(mgr1));
  ASSERT_NE(0, mgr3.local_allocator_.used());

  const ObSimpleOutlineSchema* outline = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr2.get_outline_schema(combine_id(1, 1), outline));
  ASSERT_TRUE(NULL != outline);
  ASSERT_EQ(outline_schema, *outline);
  ASSERT_EQ(OB_SUCCESS, mgr3.get_outline_schema(combine_id(1, 1), outline));
  ASSERT_TRUE(NULL != outline);
  ASSERT_EQ(outline_schema, *outline);
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
