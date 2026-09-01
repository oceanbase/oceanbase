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
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;
using share::SCN;

class TruncateNodeBuilder
{
public:
  enum CommitState { RUNNING, COMMITTED, ABORTED };
  static void build(ObMvccTransNode &node,
                    const uint8_t type,
                    const CommitState state,
                    const int64_t trans_version_v,
                    const int64_t scn_v)
  {
    node.type_ = type;
    SCN trans_version;
    SCN scn;
    EXPECT_EQ(OB_SUCCESS, trans_version.convert_for_tx(trans_version_v));
    EXPECT_EQ(OB_SUCCESS, scn.convert_for_tx(scn_v));
    node.trans_version_.atomic_store(trans_version);
    node.scn_.atomic_store(scn);
    switch (state) {
    case COMMITTED:
      node.flag_.set_committed();
      break;
    case ABORTED:
      node.flag_.set_aborted();
      break;
    case RUNNING:
    default:
      break;
    }
  }
};

class TestMvccNode
{
public:
  explicit TestMvccNode(const blocksstable::ObDmlFlag dml_flag)
    : node_(new (storage_) ObMvccTransNode())
  {
    new (node_->buf_) ObMemtableDataHeader(dml_flag, 0);
  }

  ~TestMvccNode()
  {
    node_->~ObMvccTransNode();
  }

  ObMvccTransNode &get() { return *node_; }

private:
  alignas(ObMvccTransNode) char storage_[sizeof(ObMvccTransNode) + sizeof(ObMemtableDataHeader)];
  ObMvccTransNode *node_;
};

static SCN make_scn(const int64_t v)
{
  SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_tx(v));
  return scn;
}

// ============================================================================
// is_valid / reset / ctor
// ============================================================================

TEST(TestMemtableTruncateFilter, default_ctor_is_invalid)
{
  ObMemtableTruncateFilter f;
  EXPECT_FALSE(f.is_valid());
}

TEST(TestMemtableTruncateFilter, both_dims_set_is_valid)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  EXPECT_TRUE(f.is_valid());
}

TEST(TestMemtableTruncateFilter, only_snapshot_version_set_is_valid)
{
  SCN invalid_scn;
  ObMemtableTruncateFilter f(100, invalid_scn);
  EXPECT_TRUE(f.is_valid());
}

TEST(TestMemtableTruncateFilter, only_end_scn_set_is_valid)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  EXPECT_TRUE(f.is_valid());
}

TEST(TestMemtableTruncateFilter, reset_clears_both_dims)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ASSERT_TRUE(f.is_valid());
  f.reset();
  EXPECT_FALSE(f.is_valid());
  EXPECT_EQ(0, f.snapshot_version_);
  EXPECT_FALSE(f.end_scn_.is_valid());
}

// ============================================================================
// is_truncated() - committed normal nodes
// ============================================================================
//
// truncate point: snapshot_version=100, end_scn=200
// Rule: committed normal node survives iff trans_version >= snapshot_version_.
// Nodes with trans_version < snapshot_version_ are old data cleaned by truncate.
// SCN is intentionally NOT consulted when snapshot_version_ is valid.

TEST(TestMemtableTruncateFilter, normal_committed_before_snapshot_is_truncated)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             50 /*trans_version*/, 50 /*scn*/);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_committed_at_snapshot_is_kept)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             100, 50);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_committed_after_snapshot_is_kept)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             150, 50);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_committed_uses_snapshot_not_scn)
{
  // A tx committed AFTER truncate (trans_version=150 > snapshot=100) survives
  // even though its redo SCN (50) < end_scn_ (200). Verifies snapshot_version_
  // is used, not SCN, for committed nodes.
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             150 /*trans_version > snapshot*/,
                             50  /*scn < end_scn*/);
  EXPECT_FALSE(f.is_truncated(node));
}

// SCN fallback only kicks in when snapshot_version_ is invalid.

TEST(TestMemtableTruncateFilter, normal_committed_scn_fallback_before_end_is_truncated)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             /*trans_version*/ 50, /*scn*/ 100);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_committed_scn_fallback_at_end_is_kept)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             50, 200);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_elr_before_snapshot_is_truncated)
{
  ObMemtableTruncateFilter f(100, make_scn(100));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             50 /*trans_version*/, 50 /*scn*/);
  node.trans_elr();
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_elr_after_snapshot_uses_version_not_scn)
{
  ObMemtableTruncateFilter f(100, make_scn(100));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             150 /*trans_version*/, 50 /*scn*/);
  node.trans_elr();
  EXPECT_FALSE(f.is_truncated(node));
}

// ============================================================================
// is_truncated() - uncommitted normal nodes (follower-side SCN-based fallback)
// ============================================================================

TEST(TestMemtableTruncateFilter, normal_running_scn_before_end_is_truncated)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             /*trans_version*/ 0, /*scn*/ 150);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_running_scn_at_end_is_kept)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             0, 200);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_running_when_end_scn_unset_is_kept)
{
  SCN invalid;
  ObMemtableTruncateFilter f(100, invalid);
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             0, 200);
  EXPECT_FALSE(f.is_truncated(node));
}

// ============================================================================
// is_truncated() - compact nodes
// ============================================================================

TEST(TestMemtableTruncateFilter, compact_committed_before_snapshot_is_truncated)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::COMMITTED,
                             50, 50);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_committed_at_snapshot_is_kept)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::COMMITTED,
                             100, 50);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_committed_after_snapshot_is_kept)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::COMMITTED,
                             150, 50);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_uncommitted_is_truncated)
{
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::RUNNING,
                             50, 50);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_scn_fallback_before_end_is_truncated)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::COMMITTED,
                             50, 100);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_scn_fallback_at_end_is_kept)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::COMMITTED,
                             50, 200);
  EXPECT_FALSE(f.is_truncated(node));
}

// ============================================================================
// is_truncated() - additional dimension-combination coverage
// ============================================================================

TEST(TestMemtableTruncateFilter, normal_committed_only_snapshot_set_uses_snapshot)
{
  SCN invalid;
  ObMemtableTruncateFilter f(100, invalid);
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             /*trans_version*/ 50, /*scn*/ 9999);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_committed_only_snapshot_set_at_boundary_is_kept)
{
  SCN invalid;
  ObMemtableTruncateFilter f(100, invalid);
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             100, 1);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_aborted_node_truncated_when_scn_before_end)
{
  // is_truncated() does NOT special-case aborted - it walks the running branch
  // (is_committed() is false). With scn < end_scn, the node is truncated.
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::ABORTED,
                             /*trans_version*/ 50, /*scn*/ 150);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_aborted_node_kept_when_scn_at_end)
{
  // Aborted node with scn >= end_scn: running branch keeps it by is_truncated().
  // The caller is responsible for dropping aborted nodes.
  ObMemtableTruncateFilter f(100, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::ABORTED,
                             50, 200);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_committed_only_snapshot_set_uses_snapshot)
{
  SCN invalid;
  ObMemtableTruncateFilter f(100, invalid);
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::COMMITTED,
                             /*trans_version*/ 50, /*scn*/ 9999);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, compact_uncommitted_scn_fallback_is_truncated)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_COMPACT, TruncateNodeBuilder::RUNNING,
                             /*trans_version*/ 0, /*scn*/ 50);
  EXPECT_TRUE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_running_only_end_scn_set_kept_when_scn_at_or_after)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             /*trans_version*/ 0, /*scn*/ 250);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, normal_running_only_end_scn_set_truncated_when_before)
{
  ObMemtableTruncateFilter f(0, make_scn(200));
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             0, 150);
  EXPECT_TRUE(f.is_truncated(node));
}

// ============================================================================
// reset / re-init lifecycle
// ============================================================================

TEST(TestMemtableTruncateFilter, reset_then_default_state_passes_all_normal_running)
{
  // After reset(), both dims invalid -> is_valid() is false and is_truncated()
  // falls into the running branch with end_scn_ invalid -> never truncates.
  ObMemtableTruncateFilter f(100, make_scn(200));
  f.reset();
  ASSERT_FALSE(f.is_valid());
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             0, 9999);
  EXPECT_FALSE(f.is_truncated(node));
}

TEST(TestMemtableTruncateFilter, reset_then_committed_normal_falls_into_scn_branch)
{
  // After reset(), snapshot_version_=0, so committed nodes fall into the
  // SCN-fallback branch; end_scn_ is default (val=0), so node.scn >= end_scn_
  // evaluates to true -> truncated = !(true) = false.
  ObMemtableTruncateFilter f;
  ASSERT_FALSE(f.is_valid());
  ObMvccTransNode node;
  TruncateNodeBuilder::build(node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             50, 50);
  EXPECT_FALSE(f.is_truncated(node));
}

class TestMultiVersionTruncateFilter : public testing::Test
{
protected:
  void SetUp() override
  {
    ctx_.set_truncate_filter(100, make_scn(200));
  }

  void init_iter(ObMvccRow &row, ObMultiVersionValueIterator &iter)
  {
    ASSERT_EQ(OB_SUCCESS, iter.init(&ctx_, version_range_, nullptr, &row));
    ASSERT_EQ(OB_SUCCESS, iter.init_multi_version_iter());
  }

  ObMvccAccessCtx ctx_;
  ObVersionRange version_range_{0, INT64_MAX};
};

class TestTruncateMultiVersionScanIterator
    : public ObMemtableMultiVersionScanIterator
{
public:
  int enter_committed_scan(ObMultiVersionValueIterator &value_iter)
  {
    value_iter_ = &value_iter;
    scan_state_ = SCAN_UNCOMMITTED_ROW;
    return switch_to_committed_scan_state();
  }

  int get_one_uncommitted_row(
      ObMultiVersionValueIterator &value_iter,
      const blocksstable::ObDatumRow *&row)
  {
    common::ObStoreRowkey rowkey;
    ObMemtableKey key(&rowkey);
    storage::ObTableAccessContext context;
    is_inited_ = true;
    context_ = &context;
    key_ = &key;
    value_iter_ = &value_iter;
    key_first_row_ = true;
    scan_state_ = SCAN_UNCOMMITTED_ROW;
    const int ret = inner_get_next_row(row);
    context_ = nullptr;
    key_ = nullptr;
    value_iter_ = nullptr;
    is_inited_ = false;
    return ret;
  }

  ScanState get_scan_state() const { return scan_state_; }

protected:
  int iterate_uncommitted_row(
      const common::ObStoreRowkey &key,
      blocksstable::ObDatumRow &row) override
  {
    UNUSED(key);
    int ret = OB_SUCCESS;
    const void *tnode = nullptr;
    SCN trans_version;
    transaction::ObTxSEQ sql_sequence;
    bool same_sql_sequence = false;
    if (OB_FAIL(value_iter_->get_next_uncommitted_node(
            tnode, row.trans_id_, trans_version, sql_sequence))) {
    } else if (OB_FAIL(value_iter_->check_next_sql_sequence(
                   row.trans_id_, sql_sequence, same_sql_sequence))) {
    } else {
      row.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
    }
    return ret;
  }
};

TEST_F(TestMultiVersionTruncateFilter,
       dump_iter_normalization_preserves_phase_specific_compact_rule)
{
  version_range_.multi_version_start_ = 120;
  ObMvccTransNode compact;
  ObMvccTransNode history;
  TruncateNodeBuilder::build(
      compact, NDT_COMPACT, TruncateNodeBuilder::COMMITTED, 150, 250);
  TruncateNodeBuilder::build(
      history, NDT_NORMAL, TruncateNodeBuilder::COMMITTED, 110, 210);
  compact.prev_ = &history;
  history.next_ = &compact;

  ObMvccRow row;
  row.list_head_ = &compact;
  ObMultiVersionValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&ctx_, version_range_, nullptr, &row));

  ObMvccTransNode *compact_cursor = &compact;
  ASSERT_EQ(OB_SUCCESS,
            iter.normalize_dump_iter_(
                compact_cursor,
                ObMultiVersionValueIterator::DumpIterPhase::COMPACT_ROW));
  EXPECT_EQ(&compact, compact_cursor);

  ObMvccTransNode *history_cursor = &compact;
  ASSERT_EQ(OB_SUCCESS,
            iter.normalize_dump_iter_(
                history_cursor,
                ObMultiVersionValueIterator::DumpIterPhase::MULTI_VERSION_ROW));
  EXPECT_EQ(&history, history_cursor);
}

TEST_F(TestMultiVersionTruncateFilter, uncommitted_output_closes_truncated_tail)
{
  storage::ObTxTable tx_table;
  tx_table.is_inited_ = true;
  tx_table.epoch_ = 1;
  tx_table.state_ = storage::ObTxTable::ONLINE;

  storage::ObTxTableGuard &tx_table_guard =
      ctx_.get_tx_table_guards().tx_table_guard_;
  ASSERT_EQ(OB_SUCCESS, tx_table_guard.init(&tx_table));

  const transaction::ObTransID tx_id(1);
  storage::ObTxCommitData tx_data;
  tx_data.tx_id_ = tx_id;
  tx_data.state_ = storage::ObTxData::COMMIT;
  tx_data.commit_version_ = make_scn(400);
  tx_data.end_scn_ = make_scn(400);
  tx_table_guard.get_mini_cache().set(tx_data);

  ObMvccTransNode running;
  ObMvccTransNode truncated;
  ObMvccTransNode compact;
  TruncateNodeBuilder::build(
      running, NDT_NORMAL, TruncateNodeBuilder::RUNNING, 0, 250);
  TruncateNodeBuilder::build(
      truncated, NDT_NORMAL, TruncateNodeBuilder::COMMITTED, 50, 150);
  TruncateNodeBuilder::build(
      compact, NDT_COMPACT, TruncateNodeBuilder::COMMITTED, 40, 140);
  running.tx_id_ = tx_id;
  running.seq_no_ = transaction::ObTxSEQ(1, 0);
  running.prev_ = &truncated;
  truncated.next_ = &running;
  truncated.prev_ = &compact;
  compact.next_ = &truncated;

  ObMvccRow mvcc_row;
  mvcc_row.list_head_ = &running;
  ObMultiVersionValueIterator value_iter;
  ASSERT_EQ(OB_SUCCESS,
            value_iter.init(&ctx_, version_range_, nullptr, &mvcc_row));
  value_iter.set_merge_scn(make_scn(300));

  TestTruncateMultiVersionScanIterator scan_iter;
  const blocksstable::ObDatumRow *output_row = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            scan_iter.get_one_uncommitted_row(value_iter, output_row));
  ASSERT_NE(nullptr, output_row);
  EXPECT_TRUE(output_row->is_uncommitted_row());
  EXPECT_TRUE(output_row->is_first_multi_version_row());
  EXPECT_TRUE(output_row->is_last_multi_version_row());
  EXPECT_EQ(ObMemtableMultiVersionScanIterator::SCAN_END,
            scan_iter.get_scan_state());
  tx_table_guard.reset();
}

TEST_F(TestMultiVersionTruncateFilter, uncommitted_cursor_filters_tail_on_next_call)
{
  storage::ObTxTable tx_table;
  tx_table.is_inited_ = true;
  tx_table.epoch_ = 1;
  tx_table.state_ = storage::ObTxTable::ONLINE;

  storage::ObTxTableGuard &tx_table_guard =
      ctx_.get_tx_table_guards().tx_table_guard_;
  ASSERT_EQ(OB_SUCCESS, tx_table_guard.init(&tx_table));

  const transaction::ObTransID tx_id(1);
  storage::ObTxCommitData tx_data;
  tx_data.tx_id_ = tx_id;
  tx_data.state_ = storage::ObTxData::COMMIT;
  tx_data.commit_version_ = make_scn(400);
  tx_data.end_scn_ = make_scn(400);
  tx_table_guard.get_mini_cache().set(tx_data);

  ObMvccTransNode running;
  ObMvccTransNode truncated;
  TruncateNodeBuilder::build(
      running, NDT_NORMAL, TruncateNodeBuilder::RUNNING, 0, 250);
  TruncateNodeBuilder::build(
      truncated, NDT_NORMAL, TruncateNodeBuilder::COMMITTED, 50, 150);
  running.tx_id_ = tx_id;
  running.prev_ = &truncated;
  truncated.next_ = &running;

  ObMvccRow row;
  row.list_head_ = &running;
  ObMultiVersionValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&ctx_, version_range_, nullptr, &row));
  iter.set_merge_scn(make_scn(300));

  const void *tnode = nullptr;
  transaction::ObTransID trans_id;
  SCN trans_version;
  transaction::ObTxSEQ sql_sequence;
  ASSERT_EQ(OB_SUCCESS,
            iter.get_next_uncommitted_node(
                tnode, trans_id, trans_version, sql_sequence));
  EXPECT_EQ(&running, tnode);
  EXPECT_FALSE(iter.is_compact_iter_end());

  EXPECT_EQ(OB_ITER_END,
            iter.get_next_uncommitted_node(
                tnode, trans_id, trans_version, sql_sequence));
  EXPECT_TRUE(iter.is_compact_iter_end());
  tx_table_guard.reset();
}

TEST_F(TestMultiVersionTruncateFilter, all_truncated_nodes_initialize_as_empty)
{
  ObMvccTransNode newest;
  ObMvccTransNode oldest;
  TruncateNodeBuilder::build(
      newest, NDT_NORMAL, TruncateNodeBuilder::COMMITTED, 80, 180);
  TruncateNodeBuilder::build(
      oldest, NDT_NORMAL, TruncateNodeBuilder::COMMITTED, 50, 150);
  newest.prev_ = &oldest;
  oldest.next_ = &newest;

  ObMvccRow row;
  row.list_head_ = &newest;
  ObMultiVersionValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&ctx_, version_range_, nullptr, &row));
  ASSERT_EQ(OB_SUCCESS, iter.init_multi_version_iter());

  EXPECT_TRUE(iter.is_compact_iter_end());
  EXPECT_TRUE(iter.is_multi_version_iter_end());
  EXPECT_FALSE(iter.has_multi_commit_trans());
  EXPECT_EQ(-1, iter.get_committed_max_trans_version());
  const void *tnode = nullptr;
  EXPECT_EQ(OB_ITER_END, iter.get_next_node_for_compact(tnode));
}

TEST_F(TestMultiVersionTruncateFilter, truncated_compact_barrier_switches_to_scan_end)
{
  ObMvccTransNode compact;
  ObMvccTransNode oldest;
  TruncateNodeBuilder::build(
      compact, NDT_COMPACT, TruncateNodeBuilder::COMMITTED, 50, 150);
  TruncateNodeBuilder::build(
      oldest, NDT_NORMAL, TruncateNodeBuilder::COMMITTED, 40, 140);
  compact.prev_ = &oldest;
  oldest.next_ = &compact;

  ObMvccRow row;
  row.list_head_ = &compact;
  ObMultiVersionValueIterator value_iter;
  ASSERT_EQ(OB_SUCCESS,
            value_iter.init(&ctx_, version_range_, nullptr, &row));
  ASSERT_FALSE(value_iter.is_compact_iter_end());

  TestTruncateMultiVersionScanIterator scan_iter;
  ASSERT_EQ(OB_SUCCESS, scan_iter.enter_committed_scan(value_iter));
  EXPECT_TRUE(value_iter.is_compact_iter_end());
  EXPECT_TRUE(value_iter.is_multi_version_iter_end());
  EXPECT_EQ(ObMemtableMultiVersionScanIterator::SCAN_END,
            scan_iter.get_scan_state());
}

TEST_F(TestMultiVersionTruncateFilter, single_visible_version_ignores_truncated_tail)
{
  ObMvccTransNode kept;
  ObMvccTransNode truncated;
  TruncateNodeBuilder::build(kept,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             150,
                             250);
  TruncateNodeBuilder::build(truncated,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  kept.prev_ = &truncated;
  truncated.next_ = &kept;

  ObMvccRow row;
  row.list_head_ = &kept;
  ObMultiVersionValueIterator iter;
  init_iter(row, iter);

  EXPECT_FALSE(iter.has_multi_commit_trans());
  EXPECT_TRUE(iter.is_multi_version_iter_end());
}

TEST_F(TestMultiVersionTruncateFilter, last_visible_version_closes_truncated_tail)
{
  ObMvccTransNode newest;
  ObMvccTransNode older;
  ObMvccTransNode truncated;
  TruncateNodeBuilder::build(newest,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             160,
                             260);
  TruncateNodeBuilder::build(older,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             150,
                             250);
  TruncateNodeBuilder::build(truncated,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  newest.prev_ = &older;
  older.next_ = &newest;
  older.prev_ = &truncated;
  truncated.next_ = &older;

  ObMvccRow row;
  row.list_head_ = &newest;
  ObMultiVersionValueIterator iter;
  init_iter(row, iter);
  ASSERT_TRUE(iter.has_multi_commit_trans());

  const void *tnode = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_multi_version_node(tnode));
  EXPECT_EQ(&newest, tnode);
  EXPECT_FALSE(iter.is_multi_version_iter_end());

  ASSERT_EQ(OB_SUCCESS, iter.get_next_multi_version_node(tnode));
  EXPECT_EQ(&older, tnode);
  EXPECT_TRUE(iter.is_multi_version_iter_end());
  EXPECT_EQ(OB_ITER_END, iter.get_next_multi_version_node(tnode));
}

TEST_F(TestMultiVersionTruncateFilter, same_commit_version_nodes_do_not_create_shadow_row)
{
  ObMvccTransNode newest_sql;
  ObMvccTransNode older_sql;
  ObMvccTransNode truncated;
  TruncateNodeBuilder::build(newest_sql,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             150,
                             260);
  TruncateNodeBuilder::build(older_sql,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             150,
                             250);
  TruncateNodeBuilder::build(truncated,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  newest_sql.prev_ = &older_sql;
  older_sql.next_ = &newest_sql;
  older_sql.prev_ = &truncated;
  truncated.next_ = &older_sql;

  ObMvccRow row;
  row.list_head_ = &newest_sql;
  ObMultiVersionValueIterator iter;
  init_iter(row, iter);

  EXPECT_FALSE(iter.has_multi_commit_trans());
  EXPECT_TRUE(iter.is_multi_version_iter_end());
}

TEST_F(TestMultiVersionTruncateFilter, truncated_compact_tail_does_not_keep_multi_version_open)
{
  ObMvccTransNode kept;
  ObMvccTransNode compact;
  ObMvccTransNode truncated;
  TruncateNodeBuilder::build(kept,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             160,
                             260);
  TruncateNodeBuilder::build(compact,
                             NDT_COMPACT,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  TruncateNodeBuilder::build(truncated,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  kept.prev_ = &compact;
  compact.next_ = &kept;
  compact.prev_ = &truncated;
  truncated.next_ = &compact;
  version_range_.multi_version_start_ = 100;

  ObMvccRow row;
  row.list_head_ = &kept;
  ObMultiVersionValueIterator iter;
  init_iter(row, iter);

  EXPECT_FALSE(iter.has_multi_commit_trans());
  EXPECT_TRUE(iter.is_multi_version_iter_end());
}

TEST_F(TestMultiVersionTruncateFilter, compact_cursor_filters_tail_on_next_call)
{
  ObMvccTransNode kept;
  ObMvccTransNode truncated;
  TruncateNodeBuilder::build(kept,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             150,
                             250);
  TruncateNodeBuilder::build(truncated,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  kept.prev_ = &truncated;
  truncated.next_ = &kept;

  ObMvccRow row;
  row.list_head_ = &kept;
  ObMultiVersionValueIterator iter;
  init_iter(row, iter);

  const void *tnode = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_node_for_compact(tnode));
  EXPECT_EQ(&kept, tnode);
  EXPECT_FALSE(iter.is_compact_iter_end());
  EXPECT_EQ(OB_ITER_END, iter.get_next_node_for_compact(tnode));
  EXPECT_TRUE(iter.is_compact_iter_end());
}

TEST_F(TestMultiVersionTruncateFilter, disabled_filter_keeps_legacy_multi_version_chain)
{
  ctx_.clear_truncate_filter();
  ObMvccTransNode newest;
  ObMvccTransNode older;
  TruncateNodeBuilder::build(newest,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             150,
                             250);
  TruncateNodeBuilder::build(older,
                             NDT_NORMAL,
                             TruncateNodeBuilder::COMMITTED,
                             50,
                             150);
  newest.prev_ = &older;
  older.next_ = &newest;

  ObMvccRow row;
  row.list_head_ = &newest;
  ObMultiVersionValueIterator iter;
  init_iter(row, iter);

  EXPECT_TRUE(iter.has_multi_commit_trans());
  EXPECT_FALSE(iter.is_multi_version_iter_end());
}

// ============================================================================
// MVCC write / lock integration
// ============================================================================

TEST(TestMemtableTruncateFilter, mvcc_write_ignores_committed_node_before_truncate)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  TestMvccNode writer_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  ObMvccTransNode &writer_node = writer_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.tx_id_ = transaction::ObTransID(1);
  writer_node.tx_id_ = transaction::ObTransID(2);

  ObMvccRow row;
  row.list_head_ = &old_node;
  storage::ObStoreCtx ctx;
  ctx.mvcc_acc_ctx_.tx_id_ = writer_node.tx_id_;
  ctx.mvcc_acc_ctx_.snapshot_.version_ = make_scn(200);
  ctx.mvcc_acc_ctx_.set_truncate_filter(100, make_scn(100));
  ObMvccWriteResult result;

  EXPECT_EQ(OB_SUCCESS, row.mvcc_write_(ctx, writer_node, result));
  EXPECT_EQ(&writer_node, row.get_list_head());
  EXPECT_FALSE(result.lock_state_.row_exist());
}

TEST(TestMemtableTruncateFilter, mvcc_write_without_filter_keeps_duplicate_check)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  TestMvccNode writer_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  ObMvccTransNode &writer_node = writer_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.tx_id_ = transaction::ObTransID(1);
  writer_node.tx_id_ = transaction::ObTransID(2);

  ObMvccRow row;
  row.list_head_ = &old_node;
  storage::ObStoreCtx ctx;
  ctx.mvcc_acc_ctx_.tx_id_ = writer_node.tx_id_;
  ctx.mvcc_acc_ctx_.snapshot_.version_ = make_scn(200);
  ObMvccWriteResult result;

  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, row.mvcc_write_(ctx, writer_node, result));
  EXPECT_EQ(&old_node, row.get_list_head());
}

TEST(TestMemtableTruncateFilter, mvcc_write_keeps_node_at_truncate_boundary)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  TestMvccNode writer_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  ObMvccTransNode &writer_node = writer_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             100 /*trans_version*/, 50 /*scn*/);
  old_node.tx_id_ = transaction::ObTransID(1);
  writer_node.tx_id_ = transaction::ObTransID(2);

  ObMvccRow row;
  row.list_head_ = &old_node;
  storage::ObStoreCtx ctx;
  ctx.mvcc_acc_ctx_.tx_id_ = writer_node.tx_id_;
  ctx.mvcc_acc_ctx_.snapshot_.version_ = make_scn(200);
  ctx.mvcc_acc_ctx_.set_truncate_filter(100, make_scn(100));
  ObMvccWriteResult result;

  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, row.mvcc_write_(ctx, writer_node, result));
  EXPECT_EQ(&old_node, row.get_list_head());
}

TEST(TestMemtableTruncateFilter, mvcc_write_filters_elr_node_before_truncate)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  TestMvccNode writer_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  ObMvccTransNode &writer_node = writer_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.trans_elr();
  old_node.tx_id_ = transaction::ObTransID(1);
  writer_node.tx_id_ = transaction::ObTransID(2);

  ObMvccRow row;
  row.list_head_ = &old_node;
  row.update_max_elr_trans_version(make_scn(50), old_node.tx_id_);
  storage::ObStoreCtx ctx;
  ctx.mvcc_acc_ctx_.tx_id_ = writer_node.tx_id_;
  ctx.mvcc_acc_ctx_.snapshot_.version_ = make_scn(200);
  ctx.mvcc_acc_ctx_.set_truncate_filter(100, make_scn(100));
  ObMvccWriteResult result;

  EXPECT_EQ(OB_SUCCESS, row.mvcc_write(ctx, writer_node, true, result));
  EXPECT_EQ(&writer_node, row.get_list_head());
  EXPECT_FALSE(result.lock_state_.row_exist());
  EXPECT_FALSE(result.is_mvcc_undo_);
}

TEST(TestMemtableTruncateFilter, mvcc_write_keeps_elr_node_after_truncate_by_version)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  TestMvccNode writer_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  ObMvccTransNode &writer_node = writer_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             150 /*trans_version*/, 50 /*scn*/);
  old_node.trans_elr();
  old_node.tx_id_ = transaction::ObTransID(1);
  writer_node.tx_id_ = transaction::ObTransID(2);

  ObMvccRow row;
  row.list_head_ = &old_node;
  storage::ObStoreCtx ctx;
  ctx.mvcc_acc_ctx_.tx_id_ = writer_node.tx_id_;
  ctx.mvcc_acc_ctx_.snapshot_.version_ = make_scn(200);
  ctx.mvcc_acc_ctx_.set_truncate_filter(100, make_scn(100));
  ObMvccWriteResult result;

  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE,
            row.mvcc_write(ctx, writer_node, true, result));
  EXPECT_EQ(&old_node, row.get_list_head());
  EXPECT_TRUE(result.lock_state_.row_exist());
  EXPECT_TRUE(result.is_mvcc_undo_);
}

TEST(TestMemtableTruncateFilter, mvcc_write_does_not_filter_running_node)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  TestMvccNode writer_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  ObMvccTransNode &writer_node = writer_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.tx_id_ = transaction::ObTransID(1);
  writer_node.tx_id_ = transaction::ObTransID(2);

  ObMvccRow row;
  row.list_head_ = &old_node;
  storage::ObStoreCtx ctx;
  ctx.mvcc_acc_ctx_.tx_id_ = writer_node.tx_id_;
  ctx.mvcc_acc_ctx_.snapshot_.version_ = make_scn(200);
  ctx.mvcc_acc_ctx_.set_truncate_filter(100, make_scn(100));
  ObMvccWriteResult result;

  EXPECT_EQ(OB_SUCCESS, row.mvcc_write_(ctx, writer_node, result));
  EXPECT_EQ(&old_node, row.get_list_head());
  EXPECT_TRUE(result.lock_state_.is_locked_);
  EXPECT_EQ(old_node.tx_id_, result.lock_state_.lock_trans_id_);
}

TEST(TestMemtableTruncateFilter, check_row_locked_ignores_committed_node_before_truncate)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::COMMITTED,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.tx_id_ = transaction::ObTransID(1);

  ObMvccRow row;
  row.list_head_ = &old_node;
  row.update_max_trans_version(make_scn(50), old_node.tx_id_);
  ObMvccAccessCtx ctx;
  ctx.tx_id_ = transaction::ObTransID(2);
  ctx.snapshot_.version_ = make_scn(200);
  ctx.set_truncate_filter(100, make_scn(100));
  storage::ObStoreRowLockState lock_state;

  EXPECT_EQ(OB_SUCCESS, row.check_row_locked(ctx, lock_state));
  EXPECT_FALSE(lock_state.is_locked_);
  EXPECT_FALSE(lock_state.row_exist());
  EXPECT_EQ(blocksstable::ObDmlFlag::DF_NOT_EXIST, lock_state.lock_dml_flag_);
}

TEST(TestMemtableTruncateFilter, check_row_locked_filters_elr_node_before_truncate)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.trans_elr();
  old_node.tx_id_ = transaction::ObTransID(1);

  ObMvccRow row;
  row.list_head_ = &old_node;
  row.update_max_elr_trans_version(make_scn(50), old_node.tx_id_);
  ObMvccAccessCtx ctx;
  ctx.tx_id_ = transaction::ObTransID(2);
  ctx.snapshot_.version_ = make_scn(200);
  ctx.set_truncate_filter(100, make_scn(100));
  storage::ObStoreRowLockState lock_state;

  EXPECT_EQ(OB_SUCCESS, row.check_row_locked(ctx, lock_state));
  EXPECT_FALSE(lock_state.is_locked_);
  EXPECT_FALSE(lock_state.row_exist());
  EXPECT_TRUE(lock_state.trans_version_.is_min());
  EXPECT_EQ(blocksstable::ObDmlFlag::DF_NOT_EXIST, lock_state.lock_dml_flag_);
}

TEST(TestMemtableTruncateFilter, check_row_locked_does_not_filter_elr_node_by_redo_scn)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             150 /*trans_version*/, 50 /*scn*/);
  old_node.trans_elr();
  old_node.tx_id_ = transaction::ObTransID(1);

  ObMvccRow row;
  row.list_head_ = &old_node;
  row.update_max_elr_trans_version(make_scn(150), old_node.tx_id_);
  ObMvccAccessCtx ctx;
  ctx.tx_id_ = transaction::ObTransID(2);
  ctx.snapshot_.version_ = make_scn(200);
  ctx.set_truncate_filter(100, make_scn(100));
  storage::ObStoreRowLockState lock_state;

  EXPECT_EQ(OB_SUCCESS, row.check_row_locked(ctx, lock_state));
  EXPECT_FALSE(lock_state.is_locked_);
  EXPECT_TRUE(lock_state.row_exist());
  EXPECT_EQ(blocksstable::ObDmlFlag::DF_INSERT, lock_state.lock_dml_flag_);
}

TEST(TestMemtableTruncateFilter, check_row_locked_does_not_filter_running_node)
{
  TestMvccNode old_holder(blocksstable::ObDmlFlag::DF_INSERT);
  ObMvccTransNode &old_node = old_holder.get();
  TruncateNodeBuilder::build(old_node, NDT_NORMAL, TruncateNodeBuilder::RUNNING,
                             50 /*trans_version*/, 50 /*scn*/);
  old_node.tx_id_ = transaction::ObTransID(1);

  ObMvccRow row;
  row.list_head_ = &old_node;
  ObMvccAccessCtx ctx;
  ctx.tx_id_ = transaction::ObTransID(2);
  ctx.snapshot_.version_ = make_scn(200);
  ctx.set_truncate_filter(100, make_scn(100));
  storage::ObStoreRowLockState lock_state;

  EXPECT_EQ(OB_SUCCESS, row.check_row_locked(ctx, lock_state));
  EXPECT_TRUE(lock_state.is_locked_);
  EXPECT_EQ(old_node.tx_id_, lock_state.lock_trans_id_);
  EXPECT_TRUE(lock_state.row_exist());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_memtable_truncate_filter.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_memtable_truncate_filter.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
