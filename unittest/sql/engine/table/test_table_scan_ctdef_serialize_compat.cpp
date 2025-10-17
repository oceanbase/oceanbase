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

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#define private public
#include "sql/engine/table/ob_table_scan_op.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer

struct ObTableScanCtDef42x
{
  OB_UNIS_VERSION(2);
public:
    ObTableScanCtDef42x(common::ObIAllocator &allocator)
    : pre_query_range_(allocator),
      flashback_item_(),
      bnlj_param_idxs_(allocator),
      scan_flags_(),
      scan_ctdef_(allocator),
      lookup_ctdef_(nullptr),
      lookup_loc_meta_(nullptr),
      das_dppr_tbl_(nullptr),
      allocator_(allocator),
      calc_part_id_expr_(NULL),
      global_index_rowkey_exprs_(allocator),
      pre_range_graph_(allocator)
  { }
  ObQueryRange pre_query_range_;
  FlashBackItem flashback_item_;
  Int64FixedArray bnlj_param_idxs_;
  common::ObQueryFlag scan_flags_;
  ObDASScanCtDef scan_ctdef_;
  ObDASScanCtDef *lookup_ctdef_;
  ObDASTableLocMeta *lookup_loc_meta_;
  ObTableLocation *das_dppr_tbl_;
  common::ObIAllocator &allocator_;
  ObExpr *calc_part_id_expr_;
  ExprFixedArray global_index_rowkey_exprs_;
  ObPreRangeGraph pre_range_graph_;
  union {
    uint64_t flags_{0x12345678};
    struct {
      uint64_t is_das_keep_order_            : 1; // whether das need keep ordering
      uint64_t use_index_merge_              : 1; // whether use index merge
      uint64_t ordering_used_by_parent_      : 1; // whether tsc ordering used by parent
      uint64_t reserved_                     : 61;
    };
  };
};

OB_DEF_SERIALIZE(ObTableScanCtDef42x)
{
  int ret = OB_SUCCESS;
  bool has_lookup = (lookup_ctdef_ != nullptr);
  bool is_new_query_range = scan_flags_.is_new_query_range();
  OB_UNIS_ENCODE(pre_query_range_);
  OB_UNIS_ENCODE(flashback_item_.need_scn_);
  OB_UNIS_ENCODE(flashback_item_.flashback_query_expr_);
  OB_UNIS_ENCODE(flashback_item_.flashback_query_type_);
  OB_UNIS_ENCODE(bnlj_param_idxs_);
  OB_UNIS_ENCODE(scan_flags_);
  OB_UNIS_ENCODE(scan_ctdef_);
  OB_UNIS_ENCODE(has_lookup);
  if (OB_SUCC(ret) && has_lookup) {
    OB_UNIS_ENCODE(*lookup_ctdef_);
    OB_UNIS_ENCODE(*lookup_loc_meta_);
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_ENCODE(has_dppr_tbl);
  if (OB_SUCC(ret) && has_dppr_tbl) {
    OB_UNIS_ENCODE(*das_dppr_tbl_);
  }
  OB_UNIS_ENCODE(calc_part_id_expr_);
  OB_UNIS_ENCODE(global_index_rowkey_exprs_);
  OB_UNIS_ENCODE(flashback_item_.fq_read_tx_uncommitted_);
  if (is_new_query_range) {
    OB_UNIS_ENCODE(pre_range_graph_);
  }
  OB_UNIS_ENCODE(flags_);
  return ret;
}

int ObTableScanCtDef42x::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool has_lookup = false;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  OB_UNIS_DECODE(pre_query_range_);
  OB_UNIS_DECODE(flashback_item_.need_scn_);
  OB_UNIS_DECODE(flashback_item_.flashback_query_expr_);
  OB_UNIS_DECODE(flashback_item_.flashback_query_type_);
  OB_UNIS_DECODE(bnlj_param_idxs_);
  OB_UNIS_DECODE(scan_flags_);
  OB_UNIS_DECODE(scan_ctdef_);
  OB_UNIS_DECODE(has_lookup);
  if (OB_SUCC(ret) && has_lookup) {
    void *ctdef_buf = allocator_.alloc(sizeof(ObDASScanCtDef));
    if (OB_ISNULL(ctdef_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das scan ctdef buffer failed", K(ret), K(sizeof(ObDASScanCtDef)));
    } else {
      lookup_ctdef_ = new(ctdef_buf) ObDASScanCtDef(allocator_);
      OB_UNIS_DECODE(*lookup_ctdef_);
    }
    if (OB_SUCC(ret)) {
      void *loc_meta_buf = allocator_.alloc(sizeof(ObDASTableLocMeta));
      if (OB_ISNULL(loc_meta_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate table loc meta failed", K(ret));
      } else {
        lookup_loc_meta_ = new(loc_meta_buf) ObDASTableLocMeta(allocator_);
        OB_UNIS_DECODE(*lookup_loc_meta_);
      }
    }
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_DECODE(has_dppr_tbl);
  // mock das_dppr_tbl_ == nullptr
  // if (OB_SUCC(ret) && has_dppr_tbl) {
  //   OZ(allocate_dppr_table_loc());
  //   OB_UNIS_DECODE(*das_dppr_tbl_);
  // }
  OB_UNIS_DECODE(calc_part_id_expr_);
  OB_UNIS_DECODE(global_index_rowkey_exprs_);
  OB_UNIS_DECODE(flashback_item_.fq_read_tx_uncommitted_);
  bool is_new_query_range = scan_flags_.is_new_query_range();
  if (version == 3) {
    bool abandoned_always_false_aux_lookup = false;
    bool abandoned_always_false_text_ir = false;
    OB_UNIS_DECODE(abandoned_always_false_aux_lookup);
    OB_UNIS_DECODE(abandoned_always_false_text_ir);
    ObDASAttachSpec attach_spec(allocator_, nullptr);
    OB_UNIS_DECODE(attach_spec);
    OB_UNIS_DECODE(flags_);
    if (is_new_query_range) {
      OB_UNIS_DECODE(pre_range_graph_);
    }
  } else {
    if (is_new_query_range) {
      OB_UNIS_DECODE(pre_range_graph_);
    }
    OB_UNIS_DECODE(flags_);
  }
  return ret;
}

class TestTableScanCtDefSerializeCompat : public ::testing::Test
{
public:
  TestTableScanCtDefSerializeCompat() {}
  virtual ~TestTableScanCtDefSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void fill_table_scan_ctdef(ObTableScanCtDef &ctdef)
{
  ctdef.flashback_item_.need_scn_ = true;
  ctdef.flashback_item_.flashback_query_type_ = TableItem::USING_SCN;
  ctdef.flashback_item_.fq_read_tx_uncommitted_ = false;
  ctdef.scan_flags_.daily_merge_ = true;
  ctdef.flags_ = 0x12345678;
}

void verify_basic_fields_equal(const ObTableScanCtDef42x &ctdef1, const ObTableScanCtDef &ctdef2)
{
  ASSERT_EQ(ctdef1.pre_query_range_.range_size_, ctdef2.pre_query_range_.range_size_);
  ASSERT_EQ(ctdef1.flashback_item_.need_scn_, ctdef2.flashback_item_.need_scn_);
  ASSERT_EQ(ctdef1.flashback_item_.flashback_query_type_, ctdef2.flashback_item_.flashback_query_type_);
  ASSERT_EQ(ctdef1.flashback_item_.flashback_query_expr_, ctdef2.flashback_item_.flashback_query_expr_);
  ASSERT_EQ(ctdef1.flashback_item_.fq_read_tx_uncommitted_, ctdef2.flashback_item_.fq_read_tx_uncommitted_);
  ASSERT_EQ(ctdef1.scan_flags_.daily_merge_, ctdef2.scan_flags_.daily_merge_);
  ASSERT_EQ(ctdef1.flags_, ctdef2.flags_);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_42x_to_master)
{
  ObArenaAllocator allocator;
  ObTableScanCtDef42x ctdef_42x(allocator);
  ObTableScanCtDef ctdef_master(allocator);


  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(ctdef_42x, ctdef_master);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_master_to_42x)
{
  ObArenaAllocator allocator;
  ObTableScanCtDef ctdef_master(allocator);
  ObTableScanCtDef42x ctdef_42x(allocator);

  fill_table_scan_ctdef(ctdef_master);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(ctdef_42x, ctdef_master);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}