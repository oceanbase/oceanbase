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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/resolver/ddl/ob_outline_binding_rule.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

// ==========================================================================
// Test suite: ObOutlineRuleMapping deep-copy safety
//
// Background: ObOutlineRuleMapping owns its ObString fields via an internal
// ObArenaAllocator.  Previously the struct relied on the compiler-generated
// copy ctor/assign, which memberwise-copied the allocator and the ObStrings
// — causing use-after-free when the source went out of scope (its destructor
// reset the arena, invalidating the copy's string pointers).
//
// The fix adds explicit copy ctor / copy assign that delegate to assign(),
// which deep-copies each ObString into the destination's own arena.
//
// These tests verify the fix by exercising every path that previously hit
// the memberwise-copy bug:
//   1. Direct copy construction
//   2. Direct copy assignment (and self-assignment guard)
//   3. ObSEArray::push_back of a local that then goes out of scope
//   4. ObSEArray growth past inline capacity (forces element relocation)
//   5. ObOutlineBindingRule::add_map_item / set_map_items / deep_copy
// ==========================================================================

namespace {

// Build a fully-populated mapping so every ObString field is non-empty and
// thus every field has memory in the source arena that could go dangling
// under the old shallow-copy behavior.
void fill_mapping(ObOutlineRuleMapping &m)
{
  ASSERT_EQ(OB_SUCCESS, m.set_original_db_name(ObString::make_string("orig_db")));
  ASSERT_EQ(OB_SUCCESS, m.set_original_table_name(ObString::make_string("orig_tbl")));
  ASSERT_EQ(OB_SUCCESS, m.set_db_fixed_prefix(ObString::make_string("db_")));
  ASSERT_EQ(OB_SUCCESS, m.set_table_fixed_prefix(ObString::make_string("t_")));
  ASSERT_EQ(OB_SUCCESS, m.set_db_var_name(ObString::make_string("X")));
  ASSERT_EQ(OB_SUCCESS, m.set_db_var_regex(ObString::make_string("[a-z]+")));
  ASSERT_EQ(OB_SUCCESS, m.set_table_var_name(ObString::make_string("Y")));
  ASSERT_EQ(OB_SUCCESS, m.set_table_var_regex(ObString::make_string("[0-9]+")));
  ASSERT_EQ(OB_SUCCESS, m.set_db_placeholder(ObString::make_string("DB_1001$1")));
  ASSERT_EQ(OB_SUCCESS, m.set_tb_placeholder(ObString::make_string("TB_2002$1")));
  m.set_ast_position(42);
  m.set_db_obj_id(1001);
  m.set_tb_obj_id(2002);
}

// Verify every ObString field equals the reference values from fill_mapping().
// If any ObString pointer is dangling (pointing to a freed arena page), this
// check will typically segfault or return garbage under ASan/Valgrind; under
// glibc it may also simply mismatch.
void expect_fully_populated(const ObOutlineRuleMapping &m)
{
  EXPECT_EQ(ObString::make_string("orig_db"), m.get_original_db_name());
  EXPECT_EQ(ObString::make_string("orig_tbl"), m.get_original_table_name());
  EXPECT_EQ(ObString::make_string("db_"), m.get_db_fixed_prefix());
  EXPECT_EQ(ObString::make_string("t_"), m.get_table_fixed_prefix());
  EXPECT_EQ(ObString::make_string("X"), m.get_db_var_name());
  EXPECT_EQ(ObString::make_string("[a-z]+"), m.get_db_var_regex());
  EXPECT_EQ(ObString::make_string("Y"), m.get_table_var_name());
  EXPECT_EQ(ObString::make_string("[0-9]+"), m.get_table_var_regex());
  EXPECT_EQ(ObString::make_string("DB_1001$1"), m.get_db_placeholder());
  EXPECT_EQ(ObString::make_string("TB_2002$1"), m.get_tb_placeholder());
  EXPECT_EQ(42, m.get_ast_position());
  EXPECT_EQ(1001U, m.get_db_obj_id());
  EXPECT_EQ(2002U, m.get_tb_obj_id());
}

} // anonymous namespace

// --------------------------------------------------------------------------
// 1. Explicit copy construction copies strings into own arena
// --------------------------------------------------------------------------
TEST(ObOutlineRuleMappingCopy, CopyCtorIsDeep)
{
  ObOutlineRuleMapping src;
  fill_mapping(src);

  // Copy ctor — must deep-copy into dst.allocator_
  ObOutlineRuleMapping dst(src);

  // Pointer identity: dst must not reuse src's memory
  EXPECT_NE(src.get_original_db_name().ptr(), dst.get_original_db_name().ptr());
  EXPECT_NE(src.get_table_fixed_prefix().ptr(),    dst.get_table_fixed_prefix().ptr());
  EXPECT_NE(src.get_db_placeholder().ptr(),   dst.get_db_placeholder().ptr());

  expect_fully_populated(dst);

  // Destroy src's storage — dst must remain intact (this is the core bug case)
  src.reset();
  expect_fully_populated(dst);
}

// --------------------------------------------------------------------------
// 2. Source going out of scope must not corrupt the copy
// --------------------------------------------------------------------------
TEST(ObOutlineRuleMappingCopy, SourceOutOfScopeDoesNotDangleCopy)
{
  ObOutlineRuleMapping dst;
  {
    ObOutlineRuleMapping src;
    fill_mapping(src);
    dst = src;            // copy assignment
  }                       // src destructor fires: src.allocator_.reset()

  // If copy assign were shallow, dst's ObStrings would now point into freed pages.
  expect_fully_populated(dst);
}

// --------------------------------------------------------------------------
// 3. Self-assignment guard does not drop data
// --------------------------------------------------------------------------
TEST(ObOutlineRuleMappingCopy, SelfAssignPreservesData)
{
  ObOutlineRuleMapping m;
  fill_mapping(m);

  // Silence -Wself-assign-overloaded via a reference alias.
  ObOutlineRuleMapping &alias = m;
  m = alias;

  expect_fully_populated(m);
}

// --------------------------------------------------------------------------
// 4. ObSEArray::push_back of a local then access after local destruction
//    — this is the original use-after-free reproduction from the review.
// --------------------------------------------------------------------------
TEST(ObOutlineRuleMappingCopy, SEArrayPushBackThenLocalDies)
{
  common::ObSEArray<ObOutlineRuleMapping, 16> arr;
  {
    ObOutlineRuleMapping local;
    fill_mapping(local);
    ASSERT_EQ(OB_SUCCESS, arr.push_back(local));
  }                       // local destructor fires here

  ASSERT_EQ(1, arr.count());
  expect_fully_populated(arr.at(0));
}

// --------------------------------------------------------------------------
// 5. ObSEArray growth past inline capacity triggers element relocation
//    — previously every relocated element would shallow-copy again
//    and double-free / dangle.
// --------------------------------------------------------------------------
TEST(ObOutlineRuleMappingCopy, SEArrayGrowBeyondInlineCapacity)
{
  // Inline capacity of ObOutlineBindingRule::map_items_ is 16.  Push 32.
  common::ObSEArray<ObOutlineRuleMapping, 16> arr;
  const int64_t N = 32;
  for (int64_t i = 0; i < N; ++i) {
    ObOutlineRuleMapping local;
    fill_mapping(local);
    // Give each element a distinct ast_position_ so we can spot a mixup.
    local.set_ast_position(i);
    ASSERT_EQ(OB_SUCCESS, arr.push_back(local));
  }
  ASSERT_EQ(N, arr.count());

  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(i, arr.at(i).get_ast_position()) << "index " << i;
    EXPECT_EQ(ObString::make_string("orig_db"),  arr.at(i).get_original_db_name()) << "index " << i;
    EXPECT_EQ(ObString::make_string("orig_tbl"), arr.at(i).get_original_table_name()) << "index " << i;
    EXPECT_EQ(ObString::make_string("DB_1001$1"), arr.at(i).get_db_placeholder()) << "index " << i;
    EXPECT_EQ(ObString::make_string("TB_2002$1"), arr.at(i).get_tb_placeholder()) << "index " << i;
  }
}

// --------------------------------------------------------------------------
// 6. ObOutlineBindingRule::add_map_item path
// --------------------------------------------------------------------------
TEST(ObOutlineBindingRuleCopy, AddMapItemDeepCopies)
{
  ObOutlineBindingRule rule;
  rule.set_scope(obrpc::OUTLINE_SCOPE_TENANT);
  {
    ObOutlineRuleMapping local;
    fill_mapping(local);
    ASSERT_EQ(OB_SUCCESS, rule.add_map_item(local));
  }                                               // local destroyed here

  ASSERT_TRUE(rule.is_set());
  ASSERT_EQ(1, rule.get_map_item_count());
  expect_fully_populated(rule.get_map_item(0));
}

// --------------------------------------------------------------------------
// 7. ObOutlineBindingRule::set_map_items path
// --------------------------------------------------------------------------
TEST(ObOutlineBindingRuleCopy, SetMapItemsDeepCopies)
{
  ObOutlineBindingRule rule;
  {
    common::ObSEArray<ObOutlineRuleMapping, 4> src_items;
    for (int64_t i = 0; i < 4; ++i) {
      ObOutlineRuleMapping m;
      fill_mapping(m);
      m.set_ast_position(100 + i);
      ASSERT_EQ(OB_SUCCESS, src_items.push_back(m));
    }
    ASSERT_EQ(OB_SUCCESS, rule.set_map_items(src_items));
    // src_items and its elements go out of scope at the end of this block.
  }

  ASSERT_EQ(4, rule.get_map_item_count());
  for (int64_t i = 0; i < 4; ++i) {
    EXPECT_EQ(100 + i, rule.get_map_item(i).get_ast_position());
    EXPECT_EQ(ObString::make_string("orig_db"), rule.get_map_item(i).get_original_db_name());
    EXPECT_EQ(ObString::make_string("TB_2002$1"), rule.get_map_item(i).get_tb_placeholder());
  }
}

// --------------------------------------------------------------------------
// 8. ObOutlineBindingRule::deep_copy chain (after signature change)
//    — outer allocator param is ignored; inner mapping.deep_copy no longer
//    takes an allocator.  Verifies the chain still deep-copies correctly
//    and survives source destruction.
// --------------------------------------------------------------------------
TEST(ObOutlineBindingRuleCopy, DeepCopyChainSurvivesSourceDeath)
{
  ObArenaAllocator outer_alloc;
  ObOutlineBindingRule dst;
  {
    ObOutlineBindingRule src;
    src.set_scope(obrpc::OUTLINE_SCOPE_TENANT);
    ObOutlineRuleMapping m;
    fill_mapping(m);
    ASSERT_EQ(OB_SUCCESS, src.add_map_item(m));
    ASSERT_EQ(OB_SUCCESS, dst.deep_copy(outer_alloc, src));
  }                                               // src destroyed here

  EXPECT_TRUE(dst.is_tenant_scope());
  ASSERT_EQ(1, dst.get_map_item_count());
  expect_fully_populated(dst.get_map_item(0));
}

// --------------------------------------------------------------------------
// 9. Assign chain on an already-populated destination must reset cleanly.
// --------------------------------------------------------------------------
TEST(ObOutlineRuleMappingCopy, AssignOverPopulatedDst)
{
  ObOutlineRuleMapping dst;
  fill_mapping(dst);                    // dst already has data

  ObOutlineRuleMapping src;
  ASSERT_EQ(OB_SUCCESS, src.set_original_db_name(ObString::make_string("new_db")));
  ASSERT_EQ(OB_SUCCESS, src.set_original_table_name(ObString::make_string("new_tbl")));
  ASSERT_EQ(OB_SUCCESS, src.set_table_fixed_prefix(ObString::make_string("new_tbl")));

  dst = src;

  EXPECT_EQ(ObString::make_string("new_db"),  dst.get_original_db_name());
  EXPECT_EQ(ObString::make_string("new_tbl"), dst.get_original_table_name());
  EXPECT_EQ(ObString::make_string("new_tbl"), dst.get_table_fixed_prefix());
  // Previously-populated fields must be cleared by reset() inside assign().
  EXPECT_TRUE(dst.get_db_fixed_prefix().empty());
  EXPECT_TRUE(dst.get_db_var_name().empty());
  EXPECT_TRUE(dst.get_db_placeholder().empty());
  EXPECT_TRUE(dst.get_tb_placeholder().empty());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
