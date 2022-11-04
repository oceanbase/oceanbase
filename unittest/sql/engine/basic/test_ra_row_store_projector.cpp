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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include "sql/engine/basic/ob_ra_row_store.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

TEST(RARowStore, keep_projector)
{
  ObRARowStore rs(NULL, true);
  ASSERT_EQ(OB_SUCCESS, rs.init(100 << 20));
  const int64_t OBJ_CNT = 3;
  ObObj objs[OBJ_CNT];
  ObNewRow r;
  r.cells_ = objs;
  r.count_ = OBJ_CNT;
  int64_t val = 0;
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }

  int32_t projector[] = {0, 2};
  r.projector_ = projector;
  r.projector_size_ = ARRAYSIZEOF(projector);

  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));

  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }
  r.projector_size_--;
  ASSERT_NE(OB_SUCCESS, rs.add_row(r));
  r.projector_size_++;
  ASSERT_EQ(OB_SUCCESS, rs.add_row(r));

  const ObNewRow *rr = NULL;
  ASSERT_EQ(OB_SUCCESS, rs.get_row(1, rr));

  ASSERT_TRUE(NULL != rr->projector_);
  ASSERT_NE(projector, rr->projector_);
  ASSERT_EQ(ARRAYSIZEOF(projector), rr->get_count());
  ASSERT_EQ(OBJ_CNT, rr->get_cell(0).get_int());
  ASSERT_EQ(OBJ_CNT + 1, rr->cells_[1].get_int());
  ASSERT_EQ(OBJ_CNT + 2, rr->get_cell(1).get_int());

  // only fill cells, projector_ unchanged.
  ASSERT_EQ(OB_SUCCESS, rs.get_row(0, r));
  ASSERT_EQ(projector, r.projector_);
  ASSERT_EQ(0, r.get_cell(0).get_int());
  ASSERT_EQ(1, r.cells_[1].get_int());
  ASSERT_EQ(2, r.get_cell(1).get_int());
}

class ObEmptyAlloc : public ObIAllocator
{
  void *alloc(const int64_t) override { return NULL; }
  void *alloc(const int64_t, const ObMemAttr &) override { return NULL; }
  void free(void *ptr) override { UNUSED(ptr); }
};

TEST(RARowStore, alloc_project_fail)
{
  ObEmptyAlloc alloc;
  ObRARowStore rs(&alloc, true);
  ASSERT_EQ(OB_SUCCESS, rs.init(100 << 20));
  const int64_t OBJ_CNT = 3;
  ObObj objs[OBJ_CNT];
  ObNewRow r;
  r.cells_ = objs;
  r.count_ = OBJ_CNT;
  int64_t val = 0;
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }

  int32_t projector[] = {0, 2};
  r.projector_ = projector;
  r.projector_size_ = ARRAYSIZEOF(projector);

  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, rs.add_row(r));
}

} // end namespace sql
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
