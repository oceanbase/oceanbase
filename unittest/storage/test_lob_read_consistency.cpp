#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/lob/ob_lob_iterator.h"
#include "storage/lob/ob_lob_handler.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_persistent_iterator.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_part_ctx.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{

struct LobReadState
{
  storage::ObTableScanIterator *scan_iter_ = nullptr;
  blocksstable::ObDatumRow *rows_ = nullptr;
  int64_t row_count_ = 0;
  int64_t row_index_ = 0;
  int64_t tx_state_ = storage::ObTxData::RUNNING;
  int state_ret_ = common::OB_SUCCESS;
  int read_ret_ = common::OB_SUCCESS;
  int context_ret_ = common::OB_SUCCESS;
  int scan_count_ = 0;
  int state_check_count_ = 0;
  int context_check_count_ = 0;
  storage::ObTableScanParam *scan_param_ = nullptr;
  bool abort_during_read_ = false;
  bool expire_during_read_ = false;
  int abort_after_state_check_ = 0;
};

static thread_local LobReadState read_state;

}

namespace transaction
{

int ObPartTransCtx::check_status()
{
  ++unittest::read_state.context_check_count_;
  return unittest::read_state.context_ret_;
}

}

namespace storage
{

int ObTxTableGuards::get_tx_state_with_scn(const transaction::ObTransID &tx_id,
                                         const share::SCN scn,
                                         int64_t &state,
                                         share::SCN &version)
{
  EXPECT_TRUE(tx_id.is_valid());
  EXPECT_EQ(share::SCN::max_scn(), scn);
  ++unittest::read_state.state_check_count_;
  state = unittest::read_state.tx_state_;
  version = share::SCN::max_scn();
  if (unittest::read_state.state_check_count_ == unittest::read_state.abort_after_state_check_) {
    // The current lookup has observed RUNNING; a subsequent lookup sees ABORT.
    unittest::read_state.tx_state_ = ObTxData::ABORT;
  }
  return unittest::read_state.state_ret_;
}

int ObTableScanIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  auto &state = unittest::read_state;
  int ret = state.read_ret_;
  if (OB_SUCC(ret)) {
    if (state.row_index_ == state.row_count_) {
      ret = common::OB_ITER_END;
    } else {
      row = &state.rows_[state.row_index_++];
    }
  }
  if (state.abort_during_read_) {
    state.tx_state_ = ObTxData::ABORT;
    auto *mem_ctx = get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_;
    if (nullptr != mem_ctx) {
      mem_ctx->set_tx_rollbacked();
    }
  }
  if (state.expire_during_read_) {
    state.scan_param_->timeout_ = 1;
  }
  if (OB_SUCC(ret) || common::OB_ITER_END == ret) {
    // Preserve the real table scan's post-read transaction check in this scan stub.
    const int check_ret = check_txn_status_if_read_uncommitted_();
    if (common::OB_SUCCESS != check_ret) {
      ret = check_ret;
    }
  }
  return ret;
}

int ObPersistentLobApator::scan_lob_meta(ObLobAccessParam &param, ObLobMetaIterator *&iter)
{
  int ret = common::OB_SUCCESS;
  ++unittest::read_state.scan_count_;
  iter = OB_NEWx(ObLobMetaIterator, param.allocator_, nullptr);
  if (OB_ISNULL(iter)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    iter->row_iter_ = unittest::read_state.scan_iter_;
    iter->scan_param_.timeout_ = param.timeout_;
    unittest::read_state.scan_param_ = &iter->scan_param_;
    ret = iter->scan_param_.snapshot_.assign(param.snapshot_);
    unittest::read_state.scan_iter_->get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.snapshot_
        = param.snapshot_.core_;
  }
  return ret;
}

int ObPersistentLobApator::revert_scan_iter(ObLobMetaIterator *iter)
{
  iter->row_iter_ = nullptr;
  iter->~ObLobMetaIterator();
  return common::OB_SUCCESS;
}

}

namespace unittest
{

using namespace common;
using namespace storage;
using namespace transaction;

class CountingLobMemtableCtx : public memtable::ObMemtableCtx
{
public:
  int64_t get_tx_status() const override
  {
    ++status_read_count_;
    return memtable::ObMemtableCtx::get_tx_status();
  }
  mutable int64_t status_read_count_ = 0;
};

class TestLobReadConsistency : public ::testing::Test
{
protected:
  TestLobReadConsistency() : manager_(OB_SYS_TENANT_ID) {}

  void SetUp() override
  {
    read_state = LobReadState();
    read_state.scan_iter_ = &scan_iter_;
    read_state.rows_ = rows_;
    param_.allocator_ = &allocator_;
    param_.timeout_ = INT64_MAX;
    param_.byte_size_ = 3;
    param_.offset_ = 0;
    param_.len_ = UINT64_MAX;
    param_.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    ObTxSnapshot snapshot;
    snapshot.tx_id_ = ObTransID(42);
    snapshot.version_ = share::SCN::max_scn();
    param_.snapshot_.init_ls_read(share::ObLSID(1), snapshot);
  }

  void TearDown() override
  {
    scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.tx_ctx_ = nullptr;
    scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_ = nullptr;
    read_state = LobReadState();
  }

  void add_row(const ObString &data, const uint32_t char_length)
  {
    const int64_t index = read_state.row_count_;
    ASSERT_LT(index, ARRAYSIZEOF(rows_));
    ASSERT_EQ(OB_SUCCESS, rows_[index].init(allocator_, ObLobMetaUtil::LOB_META_WITHOUT_TTL_COLUMN_CNT));
    ObLobMetaInfo &info = infos_[index];
    info.seq_id_ = ObString::make_string("seq");
    info.byte_len_ = data.length();
    info.char_len_ = char_length;
    info.lob_data_ = data;
    ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_info_to_row(info, &rows_[index], false));
    ++read_state.row_count_;
  }

  void expect_length_error(const int expected)
  {
    uint64_t length = UINT64_MAX;
    EXPECT_EQ(expected, manager_.getlength_local(param_, length));
    EXPECT_EQ(UINT64_MAX, length);
  }

  ObArenaAllocator allocator_;
  ObLobMetaManager manager_;
  ObLobAccessParam param_;
  ObTableScanIterator scan_iter_;
  CountingLobMemtableCtx mem_ctx_;
  blocksstable::ObDatumRow rows_[3];
  ObLobMetaInfo infos_[3];
};

TEST_F(TestLobReadConsistency, aborted_transaction_without_bound_context)
{
  ASSERT_TRUE(param_.snapshot_.parts_.empty());
  read_state.tx_state_ = ObTxData::ABORT;
  expect_length_error(OB_TRANS_KILLED);
  EXPECT_EQ(0, read_state.row_index_);
}

TEST_F(TestLobReadConsistency, rollback_during_empty_scan)
{
  read_state.abort_during_read_ = true;
  expect_length_error(OB_TRANS_KILLED);
}

TEST_F(TestLobReadConsistency, rollback_during_successful_row_read)
{
  add_row(ObString::make_string("abc"), 3);
  read_state.abort_during_read_ = true;
  expect_length_error(OB_TRANS_KILLED);
}

TEST_F(TestLobReadConsistency, missing_metadata_is_not_an_empty_lob)
{
  expect_length_error(OB_ERR_UNEXPECTED);
}

TEST_F(TestLobReadConsistency, incomplete_metadata_does_not_publish_partial_length)
{
  param_.byte_size_ = 6;
  add_row(ObString::make_string("abc"), 3);
  expect_length_error(OB_ERR_UNEXPECTED);
}

TEST_F(TestLobReadConsistency, scan_error_survives_concurrent_rollback)
{
  read_state.read_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  read_state.abort_during_read_ = true;
  expect_length_error(OB_ALLOCATE_MEMORY_FAILED);
}

TEST_F(TestLobReadConsistency, state_lookup_error_is_preserved)
{
  read_state.state_ret_ = OB_LS_OFFLINE;
  expect_length_error(OB_ERR_UNEXPECTED);
}

TEST_F(TestLobReadConsistency, bound_memtable_context_reports_rollback)
{
  scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_ = &mem_ctx_;
  mem_ctx_.set_tx_rollbacked();
  expect_length_error(OB_TRANS_KILLED);
  EXPECT_EQ(0, read_state.state_check_count_);
}

TEST_F(TestLobReadConsistency, healthy_memtable_context_does_not_check_write_eligibility)
{
  ObPartTransCtx context;
  auto &access_ctx = scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_;
  access_ctx.mem_ctx_ = &mem_ctx_;
  access_ctx.tx_ctx_ = &context;
  add_row(ObString::make_string("abc"), 3);
  const int states[] = {OB_TRANS_HAS_DECIDED, OB_NOT_MASTER, OB_TRANS_IS_EXITING, OB_NEED_RETRY};
  for (const int state : states) {
    read_state.context_ret_ = state;
    read_state.row_index_ = 0;
    uint64_t length = UINT64_MAX;
    EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
    EXPECT_EQ(3, length);
  }
  EXPECT_EQ(0, read_state.context_check_count_);
  EXPECT_EQ(0, read_state.state_check_count_);
  access_ctx.tx_ctx_ = nullptr;
}

TEST_F(TestLobReadConsistency, replay_context_rollback_preserves_not_master)
{
  scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_ = &mem_ctx_;
  mem_ctx_.set_for_replay(true);
  mem_ctx_.set_tx_rollbacked();
  expect_length_error(OB_NOT_MASTER);
  EXPECT_EQ(0, read_state.state_check_count_);
}

TEST_F(TestLobReadConsistency, partial_rollback_invalidates_the_bound_read)
{
  scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_ = &mem_ctx_;
  mem_ctx_.set_partial_rollbacked();
  expect_length_error(OB_TRANS_KILLED);
}

TEST_F(TestLobReadConsistency, bound_context_rollback_during_read_is_detected)
{
  scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_ = &mem_ctx_;
  add_row(ObString::make_string("abc"), 3);
  read_state.abort_during_read_ = true;
  expect_length_error(OB_TRANS_KILLED);
}

TEST_F(TestLobReadConsistency, bound_context_is_checked_once_per_storage_row_and_at_end)
{
  scan_iter_.get_ctx_guard().get_store_ctx().mvcc_acc_ctx_.mem_ctx_ = &mem_ctx_;
  param_.byte_size_ = 9;
  add_row(ObString::make_string("abc"), 3);
  add_row(ObString::make_string("def"), 3);
  add_row(ObString::make_string("ghi"), 3);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(9, length);
  EXPECT_EQ(3, mem_ctx_.status_read_count_);
  EXPECT_EQ(0, read_state.state_check_count_);
}

TEST_F(TestLobReadConsistency, negative_locator_length_is_rejected_before_scan)
{
  param_.byte_size_ = -1;
  expect_length_error(OB_INVALID_ARGUMENT);
  EXPECT_EQ(0, read_state.scan_count_);
}

TEST_F(TestLobReadConsistency, negative_locator_length_is_rejected_before_remote_content_scan)
{
  param_.byte_size_ = -1;
  ObLobMetaScanIter iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open_remote(param_));
  EXPECT_EQ(nullptr, iter.remote_ctx_);
}

TEST_F(TestLobReadConsistency, length_handler_normalizes_the_requested_range)
{
  param_.byte_size_ = 9;
  add_row(ObString::make_string("abc"), 3);
  add_row(ObString::make_string("def"), 3);
  add_row(ObString::make_string("ghi"), 3);
  alignas(ObLobData) char locator[sizeof(ObLobCommon) + sizeof(ObLobData)] = {};
  param_.lob_common_ = new (locator) ObLobCommon();
  param_.lob_common_->is_init_ = true;
  param_.lob_common_->in_row_ = false;
  param_.lob_data_ = new (param_.lob_common_->buffer_) ObLobData();
  param_.lob_data_->byte_size_ = param_.byte_size_;
  for (int64_t row_count = 2; row_count <= 3; ++row_count) {
    param_.offset_ = 2;
    param_.len_ = 1;
    read_state.row_count_ = row_count;
    read_state.row_index_ = 0;
    ObLobQueryLengthHandler handler(param_);
    ASSERT_EQ(OB_SUCCESS, handler.init(&manager_));
    EXPECT_EQ(3 == row_count ? OB_SUCCESS : OB_ERR_UNEXPECTED, handler.execute());
    EXPECT_EQ(0, param_.offset_);
    EXPECT_EQ(UINT64_MAX, param_.len_);
    EXPECT_EQ(3 == row_count ? 9 : 0, handler.result_);
    EXPECT_EQ(read_state.row_count_, read_state.row_index_);
  }
}

TEST_F(TestLobReadConsistency, missing_metadata_without_confirmed_abort_preserves_consistency_error)
{
  param_.byte_size_ = 6;
  add_row(ObString::make_string("abc"), 3);
  const int64_t states[] = {ObTxData::RUNNING, ObTxData::COMMIT, ObTxData::RUNNING};
  const int lookup_results[] = {OB_SUCCESS, OB_SUCCESS, OB_TRANS_CTX_NOT_EXIST};
  for (int64_t i = 0; i < ARRAYSIZEOF(states); ++i) {
    for (int64_t row_count = 0; row_count <= 1; ++row_count) {
      read_state.tx_state_ = states[i];
      read_state.state_ret_ = lookup_results[i];
      read_state.row_count_ = row_count;
      read_state.row_index_ = 0;
      expect_length_error(OB_ERR_UNEXPECTED);
    }
  }
}

TEST_F(TestLobReadConsistency, deadline_expiring_after_last_row_does_not_discard_length)
{
  add_row(ObString::make_string("abc"), 3);
  read_state.expire_during_read_ = true;
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(3, length);
}

TEST_F(TestLobReadConsistency, completed_content_and_end_are_not_reclassified_by_deadline)
{
  add_row(ObString::make_string("abc"), 3);
  read_state.expire_during_read_ = true;
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[3];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_SUCCESS, iter.get_next_row(data));
  EXPECT_EQ(ObString::make_string("abc"), data);
  data.assign_buffer(buffer, sizeof(buffer));
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  iter.reset();
}

TEST_F(TestLobReadConsistency, length_scan_checks_state_once_per_row_and_at_end)
{
  param_.byte_size_ = 9;
  add_row(ObString::make_string("abc"), 3);
  add_row(ObString::make_string("def"), 3);
  add_row(ObString::make_string("ghi"), 3);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(9, length);
  EXPECT_EQ(3, read_state.state_check_count_);
}

TEST_F(TestLobReadConsistency, complete_metadata_preserves_stored_zero_character_length)
{
  // Synthetic metadata checks compatibility, not reachability from a valid writer.
  add_row(ObString::make_string("abc"), 0);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(0, length);
}

TEST_F(TestLobReadConsistency, zero_character_length_does_not_allow_incomplete_bytes)
{
  param_.byte_size_ = 6;
  add_row(ObString::make_string("abc"), 0);
  expect_length_error(OB_ERR_UNEXPECTED);
}

TEST_F(TestLobReadConsistency, unrecorded_character_length_is_recomputed)
{
  param_.byte_size_ = 4;
  add_row(ObString::make_string("a\xe4\xb8\xad"), UINT32_MAX);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(2, length);
}

TEST_F(TestLobReadConsistency, stored_zero_character_length_does_not_hide_rollback)
{
  add_row(ObString::make_string("abc"), 0);
  read_state.abort_during_read_ = true;
  expect_length_error(OB_TRANS_KILLED);
}

TEST_F(TestLobReadConsistency, oversized_metadata_does_not_publish_length)
{
  add_row(ObString::make_string("abcdef"), 6);
  expect_length_error(OB_ERR_UNEXPECTED);
}

TEST_F(TestLobReadConsistency, absent_participant_is_not_treated_as_abort)
{
  read_state.state_ret_ = OB_TRANS_CTX_NOT_EXIST;
  add_row(ObString::make_string("abc"), 3);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(3, length);
}

TEST_F(TestLobReadConsistency, oversized_metadata_is_rejected_before_filling_output_buffer)
{
  add_row(ObString::make_string("abcdef"), 6);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[3];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(data));
  EXPECT_EQ(0, data.length());
  iter.reset();
}

TEST_F(TestLobReadConsistency, oversized_metadata_is_rejected_before_returning_short_range)
{
  param_.len_ = 1;
  add_row(ObString::make_string("abcdef"), 6);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[1];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(data));
  EXPECT_EQ(0, data.length());
  iter.reset();
}

TEST_F(TestLobReadConsistency, later_metadata_row_cannot_exceed_remaining_locator_bytes)
{
  param_.byte_size_ = 5;
  add_row(ObString::make_string("abc"), 3);
  add_row(ObString::make_string("def"), 3);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[3];
  ObString data(sizeof(buffer), 0, buffer);
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(data));
  ASSERT_EQ(ObString::make_string("abc"), data);
  data.assign_buffer(buffer, sizeof(buffer));
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(data));
  EXPECT_EQ(0, data.length());
  iter.reset();
}

TEST_F(TestLobReadConsistency, committed_data_is_readable)
{
  read_state.tx_state_ = ObTxData::COMMIT;
  add_row(ObString::make_string("abc"), 3);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(3, length);
}

TEST_F(TestLobReadConsistency, exact_byte_boundary_allows_multibyte_range_read)
{
  param_.byte_size_ = 4;
  param_.offset_ = 1;
  param_.len_ = 1;
  add_row(ObString::make_string("a\xe4\xb8\xad"), 2);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[3];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_SUCCESS, iter.get_next_row(data));
  EXPECT_EQ(ObString::make_string("\xe4\xb8\xad"), data);
  data.assign_buffer(buffer, sizeof(buffer));
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  iter.reset();
}

TEST_F(TestLobReadConsistency, mutation_scan_does_not_enable_read_size_validation)
{
  add_row(ObString::make_string("abcdef"), 6);
  ObLobMetaScanIter iter;
  ASSERT_EQ(OB_SUCCESS, manager_.local_scan(param_, iter));
  ObLobMetaInfo info;
  EXPECT_EQ(OB_SUCCESS, iter.get_next_row(info));
  EXPECT_EQ(6, info.byte_len_);
  iter.reset();
}

TEST_F(TestLobReadConsistency, nontransactional_read_does_not_query_transaction_state)
{
  param_.snapshot_.core_.tx_id_.reset();
  add_row(ObString::make_string("abc"), 3);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(3, length);
  EXPECT_EQ(0, read_state.state_check_count_);
}

TEST_F(TestLobReadConsistency, multibyte_length_is_not_byte_length)
{
  param_.byte_size_ = 4;
  add_row(ObString::make_string("a\xe4\xb8\xad"), 2);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(2, length);
}

TEST_F(TestLobReadConsistency, empty_locator_returns_zero)
{
  param_.byte_size_ = 0;
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(0, length);
}

TEST_F(TestLobReadConsistency, empty_outrow_locator_without_cached_length_needs_no_scan)
{
  alignas(ObLobData) char buffer[ObLobConstants::LOB_OUTROW_FULL_SIZE] = {};
  param_.lob_common_ = new (buffer) ObLobCommon();
  param_.lob_common_->in_row_ = false;
  param_.lob_common_->is_init_ = true;
  param_.lob_data_ = new (param_.lob_common_->buffer_) ObLobData();
  param_.handle_size_ = sizeof(buffer);
  param_.byte_size_ = 0;
  *param_.get_char_len_ptr() = UINT64_MAX;
  ObLobManager manager(OB_SYS_TENANT_ID);
  manager.is_inited_ = true;
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager.getlength(param_, length));
  EXPECT_EQ(0, length);
  EXPECT_EQ(0, read_state.scan_count_);
  manager.is_inited_ = false;
}

TEST_F(TestLobReadConsistency, content_read_rejects_missing_metadata)
{
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(data));
  iter.reset();
}

TEST_F(TestLobReadConsistency, rollback_after_length_before_content)
{
  add_row(ObString::make_string("abc"), 3);
  uint64_t length = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  ASSERT_EQ(3, length);
  read_state.row_index_ = 0;
  read_state.tx_state_ = ObTxData::ABORT;
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_TRANS_KILLED, iter.get_next_row(data));
  EXPECT_EQ(0, data.length());
  iter.reset();
}

TEST_F(TestLobReadConsistency, normal_range_end_does_not_require_full_lob_output)
{
  param_.byte_size_ = 6;
  param_.len_ = 2;
  add_row(ObString::make_string("abcdef"), 6);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_SUCCESS, iter.get_next_row(data));
  EXPECT_EQ(2, data.length());
  data.assign_buffer(buffer, sizeof(buffer));
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  iter.reset();
}

TEST_F(TestLobReadConsistency, range_end_before_last_metadata_row_is_valid)
{
  param_.byte_size_ = 9;
  param_.len_ = 2;
  add_row(ObString::make_string("abc"), 3);
  add_row(ObString::make_string("def"), 3);
  add_row(ObString::make_string("ghi"), 3);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_SUCCESS, iter.get_next_row(data));
  EXPECT_EQ(ObString::make_string("ab"), data);
  EXPECT_LT(read_state.row_index_, read_state.row_count_);
  data.assign_buffer(buffer, sizeof(buffer));
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  iter.reset();
}

TEST_F(TestLobReadConsistency, content_read_rejects_incomplete_metadata)
{
  param_.byte_size_ = 6;
  add_row(ObString::make_string("abc"), 3);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(data));
  iter.reset();
}

TEST_F(TestLobReadConsistency, complete_multiblock_length_is_published)
{
  param_.byte_size_ = 6;
  add_row(ObString::make_string("abc"), 3);
  add_row(ObString::make_string("def"), 3);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager_.getlength_local(param_, length));
  EXPECT_EQ(6, length);
}

TEST_F(TestLobReadConsistency, offset_beyond_end_is_a_valid_empty_range)
{
  param_.offset_ = 10;
  param_.len_ = 1;
  add_row(ObString::make_string("abc"), 3);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  EXPECT_EQ(0, data.length());
  iter.reset();
}

TEST_F(TestLobReadConsistency, zero_length_range_is_valid)
{
  param_.len_ = 0;
  add_row(ObString::make_string("abc"), 3);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  EXPECT_EQ(0, data.length());
  iter.reset();
}

TEST_F(TestLobReadConsistency, backward_read_preserves_length_checks)
{
  param_.scan_backward_ = true;
  add_row(ObString::make_string("abc"), 3);
  ObLobOutRowQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(param_, &manager_));
  char buffer[8];
  ObString data(sizeof(buffer), 0, buffer);
  EXPECT_EQ(OB_SUCCESS, iter.get_next_row(data));
  EXPECT_EQ(3, data.length());
  data.assign_buffer(buffer, sizeof(buffer));
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(data));
  iter.reset();
}

}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
