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

struct ObDASAttachCtDef : ObDASBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ExprFixedArray result_output_;
protected:
  ObDASAttachCtDef(common::ObIAllocator &allocator, ObDASOpType op_type)
    : ObDASBaseCtDef(op_type),
      result_output_(allocator)
  {
  }
};
struct ObDASAttachSpec
{
  OB_UNIS_VERSION(1);
public:
  ObDASAttachSpec(common::ObIAllocator &alloc, ObDASBaseCtDef *scan_ctdef)
    : attach_loc_metas_(alloc),
      scan_ctdef_(nullptr),
      allocator_(alloc),
      attach_ctdef_(nullptr)
  {
  }
  common::ObList<ObDASTableLocMeta*, common::ObIAllocator> attach_loc_metas_;
  ObDASBaseCtDef *scan_ctdef_; //This ctdef represents the main task information executed by the DAS Task.
  common::ObIAllocator &allocator_;
  ObDASBaseCtDef *attach_ctdef_; //The attach_ctdef represents the task information that is bound to and executed on the DAS Task.

  const ObDASTableLocMeta *get_attach_loc_meta(int64_t table_location_id, int64_t ref_table_id) const;
  int set_calc_exprs(const ExprFixedArray &calc_exprs, const int64_t max_batch_size);
  const ExprFixedArray &get_result_output() const
  {
    OB_ASSERT(attach_ctdef_ != nullptr);
    return static_cast<const ObDASAttachCtDef*>(attach_ctdef_)->result_output_;
  }

  TO_STRING_KV(K_(attach_loc_metas),
               K_(attach_ctdef));
private:
  int serialize_ctdef_tree(char *buf,
                           const int64_t buf_len,
                           int64_t &pos,
                           const ObDASBaseCtDef *root) const;
  int64_t get_ctdef_tree_serialize_size(const ObDASBaseCtDef *root) const;
  int deserialize_ctdef_tree(const char *buf,
                             const int64_t data_len,
                             int64_t &pos,
                             ObDASBaseCtDef *&root);
  int set_calc_exprs_tree(ObDASAttachCtDef *root,
                          const ExprFixedArray &calc_exprs,
                          const int64_t max_batch_size);
};

int ObDASAttachSpec::serialize_ctdef_tree(char *buf,
                                          const int64_t buf_len,
                                          int64_t &pos,
                                          const ObDASBaseCtDef *root) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root ctdef is nullptr", K(ret));
  } else {
    ObDASOpType op_type = root->op_type_;
    bool has_main_ctdef = (scan_ctdef_ == root);
    OB_UNIS_ENCODE(has_main_ctdef);
    if (!has_main_ctdef) {
      OB_UNIS_ENCODE(op_type);
      OB_UNIS_ENCODE(*root);
      OB_UNIS_ENCODE(root->children_cnt_);
      for (int i = 0; OB_SUCC(ret) && i < root->children_cnt_; ++i) {
        OZ(serialize_ctdef_tree(buf, buf_len, pos, root->children_[i]));
      }
    }
  }
  return ret;
}
#define OB_NEW_ARRAY(T, pool, count)            \
  ({                                            \
    T* ret = NULL;                              \
    if (OB_NOT_NULL(pool) && count > 0) {       \
      int64_t _size_ = sizeof(T) * count;       \
      void *_buf_ = (pool)->alloc(_size_);      \
      if (OB_NOT_NULL(_buf_))                   \
      {                                         \
        ret = new(_buf_) T[count];              \
      }                                         \
    }                                           \
    ret;                                        \
  })
int ObDASAttachSpec::deserialize_ctdef_tree(const char *buf,
                                            const int64_t data_len,
                                            int64_t &pos,
                                            ObDASBaseCtDef *&root)
{
  int ret = OB_SUCCESS;
  ObDASOpType op_type = DAS_OP_INVALID;
  bool has_main_ctdef = 0;
  OB_UNIS_DECODE(has_main_ctdef);
  if (!has_main_ctdef) {
    OB_UNIS_DECODE(op_type);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(op_type, allocator_, root))) {
        LOG_WARN("allooc das ctde failed", K(ret), K(op_type));
      }
    }
    OB_UNIS_DECODE(*root);
    OB_UNIS_DECODE(root->children_cnt_);
    if (OB_SUCC(ret) && root->children_cnt_ > 0) {
      if (OB_ISNULL(root->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator_, root->children_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc child buffer failed", K(ret), K(root->children_cnt_));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < root->children_cnt_; ++i) {
      OZ(deserialize_ctdef_tree(buf, data_len, pos, root->children_[i]));
    }
  } else {
    root = scan_ctdef_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDASAttachSpec)
{
  int ret = OB_SUCCESS;
  bool has_attach_ctdef = attach_ctdef_ != nullptr;
  OB_UNIS_ENCODE(has_attach_ctdef);
  if (has_attach_ctdef) {
    OB_UNIS_ENCODE(attach_loc_metas_.size());
    FOREACH_X(it, attach_loc_metas_, OB_SUCC(ret)) {
      const ObDASTableLocMeta *loc_meta = *it;
      OB_UNIS_ENCODE(*loc_meta);
    }
    OZ(serialize_ctdef_tree(buf, buf_len, pos, attach_ctdef_));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASAttachSpec)
{
  int ret = OB_SUCCESS;
  bool has_attach_ctdef = false;
  OB_UNIS_DECODE(has_attach_ctdef);
  if (OB_SUCC(ret) && has_attach_ctdef) {
    int64_t list_size = 0;
    OB_UNIS_DECODE(list_size);
    for (int i = 0; OB_SUCC(ret) && i < list_size; ++i) {
      ObDASTableLocMeta *loc_meta = OB_NEWx(ObDASTableLocMeta, &allocator_, allocator_);
      if (OB_ISNULL(loc_meta)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate table location meta failed", K(ret));
      } else if (OB_FAIL(attach_loc_metas_.push_back(loc_meta))) {
        LOG_WARN("store attach loc meta failed", K(ret));
      } else {
        OB_UNIS_DECODE(*loc_meta);
      }
    }
    OZ(deserialize_ctdef_tree(buf, data_len, pos, attach_ctdef_));
  }
  return ret;
}
struct ObTableScanCtDefMaster
{
  OB_UNIS_VERSION(3);
public:
    ObTableScanCtDefMaster(common::ObIAllocator &allocator)
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
      pre_range_graph_(allocator),
      attach_spec_(allocator_, &scan_ctdef_)
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
  ObDASAttachSpec attach_spec_;
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

OB_DEF_SERIALIZE(ObTableScanCtDefMaster)
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
   // abandoned fields, please remove me at next barrier version
  bool abandoned_always_false_aux_lookup = false;
  bool abandoned_always_false_text_ir = false;
  OB_UNIS_ENCODE(abandoned_always_false_aux_lookup);
  OB_UNIS_ENCODE(abandoned_always_false_text_ir);
  OB_UNIS_ENCODE(attach_spec_);
  OB_UNIS_ENCODE(flags_);
  if (is_new_query_range) {
    OB_UNIS_ENCODE(pre_range_graph_);
  }
  return ret;
}

int ObTableScanCtDefMaster::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool has_lookup = false;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  LOG_INFO("version", K(version), K(len));
  if (OB_SUCC(ret)) {
    if (len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't decode object with negative length", K(len));
    } else if (data_len < len + pos) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("buf length not enough", K(len), K(pos), K(data_len));
    }
  }
  if (OB_SUCC(ret)) {
    const_cast<int64_t&>(data_len) = len;
    int64_t pos_orig = pos;
    buf = buf + pos_orig;
    pos = 0;

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
    if (OB_SUCC(ret) && has_dppr_tbl) {
      // OZ(allocate_dppr_table_loc());
      OB_UNIS_DECODE(*das_dppr_tbl_);
    }
    OB_UNIS_DECODE(calc_part_id_expr_);
    OB_UNIS_DECODE(global_index_rowkey_exprs_);
    OB_UNIS_DECODE(flashback_item_.fq_read_tx_uncommitted_);

    if (version == 2) {
      bool is_new_query_range = scan_flags_.is_new_query_range();
      if (is_new_query_range) {
        OB_UNIS_DECODE(pre_range_graph_);
      }
      OB_UNIS_DECODE(flags_);
    } else {
      // abandoned fields, please remove me at next barrier version
      bool abandoned_always_false_aux_lookup = false;
      bool abandoned_always_false_text_ir = false;
      OB_UNIS_DECODE(abandoned_always_false_aux_lookup);
      OB_UNIS_DECODE(abandoned_always_false_text_ir);
      OB_UNIS_DECODE(attach_spec_);
      OB_UNIS_DECODE(flags_);
      bool is_new_query_range = scan_flags_.is_new_query_range();
      if (is_new_query_range) {
        OB_UNIS_DECODE(pre_range_graph_);
      }
    }

    pos = pos_orig + len;
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

void verify_basic_fields_equal(const ObTableScanCtDefMaster &ctdef1, const ObTableScanCtDef &ctdef2)
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
  ObTableScanCtDefMaster ctdef_master(allocator);
  ObTableScanCtDef ctdef_42x(allocator);

  fill_table_scan_ctdef(ctdef_42x);


  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(ctdef_master, ctdef_42x);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_master_to_42x)
{
  ObArenaAllocator allocator;
  ObTableScanCtDefMaster ctdef_master(allocator);
  ObTableScanCtDef ctdef_42x(allocator);


  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(ctdef_master, ctdef_42x);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_CURRENT_VERSION;
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}