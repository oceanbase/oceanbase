/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "deps/oblib/src/lib/allocator/page_arena.h"
#include "lib/oblog/ob_log_module.h"
#include "share/catalog/ob_catalog_properties.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace oceanbase;

class TestHMSCatalog : public ::testing::Test
{
public:
  TestHMSCatalog() = default;
  ~TestHMSCatalog() = default;
  common::ObArenaAllocator allocator_;
};

TEST_F(TestHMSCatalog, test_create_catalog)
{
  ParseNode uri;
  uri.str_len_ = 1;
  uri.str_value_ = " ";
  ParseNode *children[1];
  children[0] = &uri;

  ParseNode child_value;
  child_value.type_ = ObItemType::T_URI;
  child_value.num_child_ = 1;
  child_value.children_ = children;

  ParseNode *root_children[1];
  root_children[0] = &child_value;

  ParseNode root;
  root.num_child_ = 1;
  root.children_ = root_children;

  share::ObHMSCatalogProperties properties;
  ASSERT_EQ(OB_INVALID_ARGUMENT, properties.resolve_catalog_properties(root));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
