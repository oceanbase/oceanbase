/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public
#include "share/ob_lob_access_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_remote.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{

struct LobCharLengthReadState
{
  transaction::ObTxDesc *tx_desc_ = nullptr;
  int64_t query_timeout_ts_ = INT64_MAX;
  int64_t read_timeout_ts_ = 0;
  int build_ret_ = common::OB_SUCCESS;
  int read_ret_ = common::OB_SUCCESS;
  int query_ret_ = common::OB_SUCCESS;
  int stream_ret_ = common::OB_SUCCESS;
  int stream_read_count_ = 0;
  int block_ret_ = common::OB_SUCCESS;
  int read_count_ = 0;
  int block_read_count_ = 0;
  uint64_t length_ = 3;
  int64_t remote_length_ = 3;
  bool expire_during_read_ = false;
  bool expire_during_build_ = false;
  bool invalid_ls_ = false;
  bool remote_read_ = false;
  bool malformed_response_ = false;
  bool stream_has_more_ = false;
};

static thread_local LobCharLengthReadState read_state;

static void finish_read()
{
  if (read_state.expire_during_read_ && OB_NOT_NULL(read_state.tx_desc_)) {
    read_state.tx_desc_->expire_ts_ = 1;
  }
}

}

namespace obrpc
{

template <>
int SSHandle<ObStorageRpcProxy::ObRpc<OB_LOB_QUERY>>::get_more(common::ObDataBuffer &result)
{
  UNUSED(result);
  ++unittest::read_state.stream_read_count_;
  has_more_ = false;
  return unittest::read_state.stream_ret_;
}

}

namespace storage
{

class LobBlockReadIter : public ObLobQueryIter
{
public:
  int get_next_row(common::ObString &data) override
  {
    auto &state = unittest::read_state;
    ++state.block_read_count_;
    unittest::finish_read();
    int ret = state.block_ret_;
    if (OB_SUCC(ret) && data.write("abc", 3) != 3) {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    return ret;
  }
  void reset() override {}
};

int ObLobManager::query(ObLobAccessParam &param, ObLobQueryIter *&result)
{
  int ret = common::OB_SUCCESS;
  unittest::read_state.read_timeout_ts_ = param.timeout_;
  result = OB_NEW(LobBlockReadIter, common::ObMemAttr(OB_SYS_TENANT_ID, "LobQueryIter"));
  if (OB_ISNULL(result)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

int ObLobManager::query(ObLobAccessParam &param, common::ObString &result)
{
  unittest::read_state.read_timeout_ts_ = param.timeout_;
  LobBlockReadIter iter;
  return iter.get_next_row(result);
}

int ObLobManager::build_lob_param(ObLobAccessParam &param,
                                common::ObIAllocator &allocator,
                                common::ObCollationType coll_type,
                                uint64_t offset,
                                uint64_t len,
                                int64_t timeout,
                                common::ObLobLocatorV2 &lob)
{
  auto &state = unittest::read_state;
  param.allocator_ = &allocator;
  param.ls_id_ = state.invalid_ls_ ? share::ObLSID() : share::ObLSID(1);
  param.tablet_id_ = common::ObTabletID(1);
  param.timeout_ = MIN(timeout, state.query_timeout_ts_);
  param.byte_size_ = 53;
  if (state.expire_during_build_ && OB_NOT_NULL(state.tx_desc_)) {
    state.tx_desc_->expire_ts_ = 1;
  }
  return state.build_ret_;
}

int ObLobManager::getlength(ObLobAccessParam &param, uint64_t &len)
{
  auto &state = unittest::read_state;
  ++state.read_count_;
  state.read_timeout_ts_ = param.timeout_;
  unittest::finish_read();
  int ret = state.read_ret_;
  if (state.remote_read_) {
    ObLobMetaManager meta_manager(OB_SYS_TENANT_ID);
    ret = meta_manager.getlength_remote(param, len);
  } else if (OB_SUCC(ret)) {
    len = state.length_;
  }
  return ret;
}

int ObLobRemoteUtil::query(ObLobAccessParam &param,
                         const obrpc::ObLobQueryArg::QueryType qtype,
                         const common::ObAddr &dst_addr,
                         ObLobRemoteQueryCtx *&ctx)
{
  int ret = unittest::read_state.query_ret_;
  if (OB_SUCC(ret)) {
    ctx = OB_NEWx(ObLobRemoteQueryCtx, param.allocator_);
    char *buffer = static_cast<char *>(param.allocator_->alloc(64));
    if (OB_ISNULL(ctx) || OB_ISNULL(buffer)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ctx->rpc_buffer_.set_data(buffer, 64);
      ctx->handle_.has_more_ = unittest::read_state.stream_has_more_;
      if (!unittest::read_state.malformed_response_) {
        obrpc::ObLobQueryBlock header;
        header.size_ = unittest::read_state.remote_length_;
        ret = common::serialization::encode(buffer, 64, ctx->rpc_buffer_.get_position(), header);
      }
    }
  }
  return ret;
}

}

namespace unittest
{

using namespace common;
using namespace share;
using namespace sql;
using namespace storage;
using namespace transaction;

class TestLobCharLengthTimeout : public ::testing::Test
{
protected:
  TestLobCharLengthTimeout()
      : session_(OB_SYS_TENANT_ID),
        lob_manager_(OB_SYS_TENANT_ID),
        old_lob_manager_(nullptr),
        ctx_(locator_, &session_, &allocator_),
        iter_(ObLongTextType, CS_TYPE_UTF8MB4_GENERAL_CI, ObString(), true)
  {}

  void SetUp() override
  {
    read_state = LobCharLengthReadState();
    old_lob_manager_ = MTL(ObLobManager *);
    ObTenantEnv::get_tenant_local()->set(&lob_manager_);
    ASSERT_EQ(OB_SUCCESS, session_.init_tenant(ObString::make_string("sys"), OB_SYS_TENANT_ID));
    session_.get_tx_desc() = &tx_;
    tx_.state_ = ObTxDesc::State::ACTIVE;
    tx_.active_ts_ = ObTimeUtility::current_time();
    tx_.timeout_us_ = 60 * USECS_PER_SEC;
    tx_.expire_ts_ = tx_.active_ts_ + tx_.timeout_us_;
    read_state.tx_desc_ = &tx_;

    ObMemLobExternFlags flags;
    flags.set_empty();
    ObLobCommon lob_common;
    const uint32_t locator_size = ObLobLocatorV2::calc_locator_full_len(
        flags, 0, sizeof(ObLobCommon), 0, false);
    ASSERT_LE(locator_size, sizeof(locator_buf_));
    locator_.assign_buffer(locator_buf_, locator_size);
    ASSERT_EQ(OB_SUCCESS, locator_.fill(
        PERSISTENT_LOB, flags, ObString(), &lob_common, sizeof(lob_common), 0, 0, false));
    ctx_.locator_ = locator_;

    iter_.is_init_ = true;
    iter_.is_lob_ = true;
    iter_.is_outrow_ = true;
    iter_.ctx_ = &ctx_;
    iter_.state_ = TEXTSTRING_ITER_INIT;
  }

  void TearDown() override
  {
    read_state = LobCharLengthReadState();
    session_.get_tx_desc() = nullptr;
    ObTenantEnv::get_tenant_local()->set(old_lob_manager_);
  }

  void check_length_error(const int expected_ret)
  {
    int64_t length = -1;
    EXPECT_EQ(expected_ret, iter_.get_char_len(length));
    EXPECT_EQ(-1, length);
  }

  ObSQLSessionInfo session_;
  ObTxDesc tx_;
  ObLobManager lob_manager_;
  ObLobManager *old_lob_manager_;
  ObArenaAllocator allocator_;
  ObLobLocatorV2 locator_;
  ObLobTextIterCtx ctx_;
  ObTextStringIter iter_;
  alignas(ObMemLobCommon) char locator_buf_[512];
};

TEST_F(TestLobCharLengthTimeout, expired_session_preserves_storage_errors)
{
  tx_.expire_ts_ = 1;
  const int errors[] = {OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_KILLED,
                        OB_ERR_UNEXPECTED, OB_ALLOCATE_MEMORY_FAILED, OB_INVALID_ARGUMENT};
  for (const int error : errors) {
    read_state.read_ret_ = error;
    check_length_error(error);
  }
  EXPECT_EQ(ARRAYSIZEOF(errors), read_state.read_count_);
}

TEST_F(TestLobCharLengthTimeout, storage_errors_survive_expiry_during_read)
{
  read_state.expire_during_read_ = true;
  const int errors[] = {OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_KILLED, OB_ERR_UNEXPECTED};
  for (const int error : errors) {
    tx_.expire_ts_ = ObTimeUtility::current_time() + USECS_PER_SEC;
    read_state.read_ret_ = error;
    check_length_error(error);
    EXPECT_EQ(1, tx_.expire_ts_);
  }
  EXPECT_EQ(ARRAYSIZEOF(errors), read_state.read_count_);
}

TEST_F(TestLobCharLengthTimeout, successful_length_is_not_reclassified_after_expiry)
{
  read_state.expire_during_read_ = true;
  int64_t length = -1;
  EXPECT_EQ(OB_SUCCESS, iter_.get_char_len(length));
  EXPECT_EQ(3, length);
  EXPECT_EQ(1, tx_.expire_ts_);
}

TEST_F(TestLobCharLengthTimeout, receiver_does_not_clamp_access_deadline_to_session_transaction)
{
  tx_.expire_ts_ = 1;
  read_state.query_timeout_ts_ = ObTimeUtility::current_time() + 60 * USECS_PER_SEC;
  int64_t length = -1;
  EXPECT_EQ(OB_SUCCESS, iter_.get_char_len(length));
  EXPECT_EQ(3, length);
  EXPECT_EQ(read_state.query_timeout_ts_, read_state.read_timeout_ts_);
}

TEST_F(TestLobCharLengthTimeout, no_session_or_transaction_preserves_storage_error)
{
  read_state.read_ret_ = OB_TRANS_KILLED;
  ctx_.session_ = nullptr;
  check_length_error(OB_TRANS_KILLED);
  ctx_.session_ = &session_;
  session_.get_tx_desc() = nullptr;
  check_length_error(OB_TRANS_KILLED);
}

TEST_F(TestLobCharLengthTimeout, missing_context_is_an_error)
{
  iter_.ctx_ = nullptr;
  check_length_error(OB_INVALID_ARGUMENT);
}

TEST_F(TestLobCharLengthTimeout, initialization_error_survives_expiry)
{
  read_state.expire_during_build_ = true;
  read_state.build_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  check_length_error(OB_ALLOCATE_MEMORY_FAILED);
  EXPECT_EQ(1, tx_.expire_ts_);
  EXPECT_EQ(0, read_state.read_count_);
}

TEST_F(TestLobCharLengthTimeout, invalid_parameters_survive_expiry)
{
  read_state.expire_during_build_ = true;
  read_state.invalid_ls_ = true;
  check_length_error(OB_INVALID_ARGUMENT);
  EXPECT_EQ(1, tx_.expire_ts_);
  EXPECT_EQ(0, read_state.read_count_);
}

TEST_F(TestLobCharLengthTimeout, unsupported_locator_error_is_not_reclassified)
{
  tx_.expire_ts_ = 1;
  ctx_.locator_ = ObLobLocatorV2();
  check_length_error(OB_NOT_IMPLEMENT);
}

TEST_F(TestLobCharLengthTimeout, expression_helper_preserves_locator_error)
{
  tx_.expire_ts_ = 1;
  ObExecContext exec_ctx(allocator_);
  exec_ctx.set_my_session(&session_);
  ObEvalCtx eval_ctx(exec_ctx, &allocator_);
  alignas(ObLobData) char locator_buf[ObLobManager::LOB_OUTROW_FULL_SIZE] = {};
  ObLobCommon *lob_common = new (locator_buf) ObLobCommon();
  lob_common->in_row_ = false;
  lob_common->is_init_ = true;
  ObLobData *lob_data = new (lob_common->buffer_) ObLobData();
  lob_data->byte_size_ = 53;
  ObString data(sizeof(locator_buf), locator_buf);
  ObDatumMeta meta;
  meta.type_ = ObLongTextType;
  meta.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  int64_t length = -1;
  EXPECT_EQ(OB_NOT_IMPLEMENT, ObTextStringHelper::get_char_len(eval_ctx, data, meta, true, length));
  EXPECT_EQ(-1, length);
}

TEST_F(TestLobCharLengthTimeout, inrow_character_length_is_unchanged)
{
  tx_.expire_ts_ = 1;
  const ObString values[] = {ObString::make_string(""), ObString::make_string("abc"),
                            ObString::make_string("a\xe4\xb8\xad")};
  const int64_t expected_lengths[] = {0, 3, 2};
  for (int64_t index = 0; index < ARRAYSIZEOF(values); ++index) {
    ObTextStringIter iter(ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI, values[index], false);
    ASSERT_EQ(OB_SUCCESS, iter.init(0, &session_));
    int64_t length = -1;
    ASSERT_EQ(OB_SUCCESS, iter.get_char_len(length));
    EXPECT_EQ(expected_lengths[index], length);

    alignas(ObLobCommon) char locator_buf[32] = {};
    new (locator_buf) ObLobCommon();
    MEMCPY(locator_buf + sizeof(ObLobCommon), values[index].ptr(), values[index].length());
    ObString lob_data(sizeof(ObLobCommon) + values[index].length(), locator_buf);
    ObTextStringIter lob_iter(ObLongTextType, CS_TYPE_UTF8MB4_GENERAL_CI, lob_data, true);
    ASSERT_EQ(OB_SUCCESS, lob_iter.init(0, &session_));
    ASSERT_EQ(OB_SUCCESS, lob_iter.get_char_len(length));
    EXPECT_EQ(expected_lengths[index], length);
  }
}

TEST_F(TestLobCharLengthTimeout, remote_errors_are_forwarded_unchanged_after_expiry)
{
  read_state.remote_read_ = true;
  read_state.expire_during_read_ = true;
  const int errors[] = {OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_KILLED,
                        OB_RPC_CONNECT_ERROR, OB_ERR_UNEXPECTED};
  for (const int error : errors) {
    tx_.expire_ts_ = ObTimeUtility::current_time() + USECS_PER_SEC;
    read_state.query_ret_ = error;
    check_length_error(error);
  }
}

TEST_F(TestLobCharLengthTimeout, remote_zero_length_for_nonempty_locator_remains_invalid_after_expiry)
{
  read_state.remote_read_ = true;
  read_state.remote_length_ = 0;
  read_state.expire_during_read_ = true;
  check_length_error(OB_ERR_UNEXPECTED);
  EXPECT_EQ(1, tx_.expire_ts_);
}

TEST_F(TestLobCharLengthTimeout, streaming_errors_preserve_server_and_transport_codes)
{
  read_state.remote_read_ = true;
  read_state.stream_has_more_ = true;
  const int errors[] = {OB_TRANS_KILLED, OB_TRANS_TIMEOUT, OB_TIMEOUT,
                        OB_RPC_CONNECT_ERROR, OB_ALLOCATE_MEMORY_FAILED, OB_ERR_UNEXPECTED};
  for (const int error : errors) {
    read_state.stream_ret_ = error;
    check_length_error(OB_DATA_SOURCE_TIMEOUT);
  }
  EXPECT_EQ(ARRAYSIZEOF(errors), read_state.stream_read_count_);
}

TEST_F(TestLobCharLengthTimeout, negative_locator_length_is_rejected_before_remote_query)
{
  ObLobAccessParam param;
  param.allocator_ = &allocator_;
  param.byte_size_ = -1;
  read_state.query_ret_ = OB_RPC_CONNECT_ERROR;
  ObLobMetaManager manager(OB_SYS_TENANT_ID);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_INVALID_ARGUMENT, manager.getlength_remote(param, length));
  EXPECT_EQ(UINT64_MAX, length);
}

TEST_F(TestLobCharLengthTimeout, invalid_remote_header_is_not_reclassified_as_access_timeout)
{
  read_state.remote_read_ = true;
  read_state.remote_length_ = 0;
  read_state.query_timeout_ts_ = 1;
  tx_.expire_ts_ = 1;
  check_length_error(OB_ERR_UNEXPECTED);
  EXPECT_EQ(1, read_state.read_timeout_ts_);
}

TEST_F(TestLobCharLengthTimeout, remote_negative_length_remains_invalid)
{
  read_state.remote_read_ = true;
  read_state.remote_length_ = -1;
  read_state.expire_during_read_ = true;
  check_length_error(OB_ERR_UNEXPECTED);
}

TEST_F(TestLobCharLengthTimeout, remote_decode_error_is_not_reclassified_after_expiry)
{
  read_state.remote_read_ = true;
  read_state.malformed_response_ = true;
  int64_t length = -1;
  const int decode_ret = iter_.get_char_len(length);
  ASSERT_NE(OB_SUCCESS, decode_ret);
  ASSERT_NE(OB_TIMEOUT, decode_ret);
  ASSERT_NE(OB_TRANS_TIMEOUT, decode_ret);
  EXPECT_EQ(-1, length);
  read_state.expire_during_read_ = true;
  check_length_error(decode_ret);
}

TEST_F(TestLobCharLengthTimeout, remote_success_is_not_reclassified_after_expiry)
{
  read_state.remote_read_ = true;
  read_state.expire_during_read_ = true;
  int64_t length = -1;
  EXPECT_EQ(OB_SUCCESS, iter_.get_char_len(length));
  EXPECT_EQ(3, length);
  EXPECT_EQ(1, tx_.expire_ts_);
}

TEST_F(TestLobCharLengthTimeout, remote_empty_locator_remains_valid_with_expired_access_deadline)
{
  read_state.remote_length_ = 0;
  ObLobAccessParam param;
  param.allocator_ = &allocator_;
  param.timeout_ = 1;
  param.byte_size_ = 0;
  ObLobMetaManager manager(OB_SYS_TENANT_ID);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_SUCCESS, manager.getlength_remote(param, length));
  EXPECT_EQ(0, length);
}

TEST_F(TestLobCharLengthTimeout, empty_locator_does_not_allow_negative_remote_length)
{
  read_state.remote_length_ = -1;
  ObLobAccessParam param;
  param.allocator_ = &allocator_;
  param.byte_size_ = 0;
  ObLobMetaManager manager(OB_SYS_TENANT_ID);
  uint64_t length = UINT64_MAX;
  EXPECT_EQ(OB_ERR_UNEXPECTED, manager.getlength_remote(param, length));
  EXPECT_EQ(UINT64_MAX, length);
}

TEST_F(TestLobCharLengthTimeout, abort_after_length_is_reported_by_content_read)
{
  int64_t length = -1;
  ASSERT_EQ(OB_SUCCESS, iter_.get_char_len(length));
  ASSERT_EQ(3, length);
  tx_.expire_ts_ = 1;
  read_state.block_ret_ = OB_TRANS_KILLED;
  ObString data;
  EXPECT_EQ(TEXTSTRING_ITER_INVALID, iter_.get_next_block(data));
  EXPECT_EQ(OB_TRANS_KILLED, iter_.get_inner_ret());
  EXPECT_EQ(1, read_state.block_read_count_);
}

TEST_F(TestLobCharLengthTimeout, first_block_errors_are_forwarded_unchanged_after_expiry)
{
  read_state.expire_during_read_ = true;
  const int errors[] = {OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_KILLED, OB_ALLOCATE_MEMORY_FAILED};
  for (const int error : errors) {
    iter_.reset();
    tx_.expire_ts_ = ObTimeUtility::current_time() + USECS_PER_SEC;
    read_state.block_ret_ = error;
    ObString data;
    EXPECT_EQ(TEXTSTRING_ITER_INVALID, iter_.get_next_block(data));
    EXPECT_EQ(error, iter_.get_inner_ret());
    EXPECT_EQ(0, data.length());
  }
}

TEST_F(TestLobCharLengthTimeout, later_block_abort_is_forwarded_unchanged_after_expiry)
{
  ObString data;
  ASSERT_EQ(TEXTSTRING_ITER_NEXT, iter_.get_next_block(data));
  ASSERT_EQ(3, data.length());
  read_state.expire_during_read_ = true;
  read_state.block_ret_ = OB_TRANS_KILLED;
  EXPECT_EQ(TEXTSTRING_ITER_INVALID, iter_.get_next_block(data));
  EXPECT_EQ(OB_TRANS_KILLED, iter_.get_inner_ret());
  EXPECT_EQ(0, data.length());
  EXPECT_EQ(2, read_state.block_read_count_);
}

TEST_F(TestLobCharLengthTimeout, successful_block_is_not_reclassified_after_expiry)
{
  read_state.expire_during_read_ = true;
  ObString data;
  EXPECT_EQ(TEXTSTRING_ITER_NEXT, iter_.get_next_block(data));
  EXPECT_EQ(OB_SUCCESS, iter_.get_inner_ret());
  EXPECT_EQ(ObString::make_string("abc"), data);
  EXPECT_EQ(1, tx_.expire_ts_);
}

TEST_F(TestLobCharLengthTimeout, stream_end_is_not_reclassified_after_expiry)
{
  ObString data;
  ASSERT_EQ(TEXTSTRING_ITER_NEXT, iter_.get_next_block(data));
  read_state.expire_during_read_ = true;
  read_state.block_ret_ = OB_ITER_END;
  EXPECT_EQ(TEXTSTRING_ITER_END, iter_.get_next_block(data));
  EXPECT_EQ(OB_SUCCESS, iter_.get_inner_ret());
}

TEST_F(TestLobCharLengthTimeout, full_content_errors_are_forwarded_unchanged_after_expiry)
{
  read_state.expire_during_read_ = true;
  const int errors[] = {OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_KILLED, OB_ALLOCATE_MEMORY_FAILED};
  for (const int error : errors) {
    tx_.expire_ts_ = ObTimeUtility::current_time() + USECS_PER_SEC;
    read_state.block_ret_ = error;
    EXPECT_EQ(error, iter_.get_outrow_lob_full_data(nullptr));
  }
}

TEST_F(TestLobCharLengthTimeout, prefix_errors_are_forwarded_unchanged_after_expiry)
{
  read_state.expire_during_read_ = true;
  const int errors[] = {OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_KILLED, OB_ALLOCATE_MEMORY_FAILED};
  for (const int error : errors) {
    tx_.expire_ts_ = ObTimeUtility::current_time() + USECS_PER_SEC;
    read_state.block_ret_ = error;
    EXPECT_EQ(error, iter_.get_outrow_prefix_data(3));
  }
}

TEST_F(TestLobCharLengthTimeout, full_content_success_is_not_reclassified_after_expiry)
{
  read_state.expire_during_read_ = true;
  EXPECT_EQ(OB_SUCCESS, iter_.get_outrow_lob_full_data(nullptr));
  EXPECT_EQ(3, ctx_.content_byte_len_);
  EXPECT_EQ(1, tx_.expire_ts_);
}

TEST_F(TestLobCharLengthTimeout, prefix_success_is_not_reclassified_after_expiry)
{
  read_state.expire_during_read_ = true;
  EXPECT_EQ(OB_SUCCESS, iter_.get_outrow_prefix_data(3));
  EXPECT_EQ(3, ctx_.content_byte_len_);
  EXPECT_EQ(1, tx_.expire_ts_);
}

}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
