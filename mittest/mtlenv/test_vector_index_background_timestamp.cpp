// owner: vector index
// owner group: shenzhen
//
// Verify that vector-index background reads use the transaction snapshot on a
// leader and the LS weak-read snapshot on a follower.

#include <gtest/gtest.h>

#define private public
#define protected public
#include "mtlenv/mock_tenant_module_env.h"
#include "unittest/storage/test_dml_common.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#undef protected
#undef private

namespace oceanbase
{
namespace share
{

class ControlledTsMgr final : public transaction::ObTsMgr
{
public:
  ControlledTsMgr() : ret_(OB_SUCCESS), result_scn_(), call_count_(0) {}

  void set_result(const int ret, const SCN &result_scn)
  {
    ret_ = ret;
    result_scn_ = result_scn;
    call_count_ = 0;
  }

  int get_call_count() const { return call_count_; }

  int get_gts_sync(const uint64_t tenant_id,
                   const transaction::MonotonicTs stc,
                   const int64_t timeout_us,
                   SCN &scn,
                   transaction::MonotonicTs &receive_gts_ts) override
  {
    UNUSED(tenant_id);
    UNUSED(stc);
    UNUSED(timeout_us);
    ++call_count_;
    if (OB_SUCCESS == ret_) {
      scn = result_scn_;
      receive_gts_ts = transaction::MonotonicTs::current_time();
    }
    return ret_;
  }

private:
  int ret_;
  SCN result_scn_;
  int call_count_;
};

class ScopedTsMgrOverride
{
public:
  ScopedTsMgrOverride(transaction::ObTransService &txs, transaction::ObTsMgr &replacement)
      : txs_(txs), saved_(txs.ts_mgr_)
  {
    txs_.ts_mgr_ = &replacement;
  }

  ~ScopedTsMgrOverride()
  {
    txs_.ts_mgr_ = saved_;
  }

private:
  transaction::ObTransService &txs_;
  transaction::ObTsMgr *saved_;
};

class TestVectorIndexBackgroundTimestamp : public ::testing::Test
{
public:
  static const ObLSID LS_ID;

  static void SetUpTestCase()
  {
    // This test only needs local tenant/LS services; avoid initializing an
    // external object-storage device in the generic mock environment.
    GCTX.startup_mode_ = observer::ObServerMode::NORMAL_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    storage::ObServerStorageMetaService::get_instance().is_started_ = true;

    storage::ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS,
              storage::TestDmlCommon::create_ls(OB_SYS_TENANT_ID, LS_ID, ls_handle));
    ASSERT_NE(nullptr, ls_handle.get_ls());
  }

  static void TearDownTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->remove_ls(LS_ID));
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp() override
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
};

const ObLSID TestVectorIndexBackgroundTimestamp::LS_ID(1988);

TEST_F(TestVectorIndexBackgroundTimestamp, leader_uses_transaction_snapshot)
{
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  ASSERT_NE(nullptr, txs);

  const int64_t now_us = common::ObTimeUtility::fast_current_time();
  const int64_t one_minute_us = 60L * 1000L * 1000L;
  SCN wall_scn;
  SCN lower_scn;
  SCN upper_scn;
  ASSERT_EQ(OB_SUCCESS, wall_scn.convert_from_ts(now_us));
  ASSERT_EQ(OB_SUCCESS, lower_scn.convert_from_ts(now_us - one_minute_us));
  ASSERT_EQ(OB_SUCCESS, upper_scn.convert_from_ts(now_us + one_minute_us));
  ASSERT_LT(lower_scn, wall_scn);
  ASSERT_GT(upper_scn, wall_scn);

  ControlledTsMgr controlled_ts_mgr;
  ScopedTsMgrOverride ts_mgr_override(*txs, controlled_ts_mgr);
  ObLSID ls_id = LS_ID;
  SCN target_scn;

  controlled_ts_mgr.set_result(OB_SUCCESS, lower_scn);
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::get_read_scn(true, ls_id, target_scn));
  EXPECT_EQ(lower_scn, target_scn);
  EXPECT_EQ(1, controlled_ts_mgr.get_call_count());

  controlled_ts_mgr.set_result(OB_SUCCESS, upper_scn);
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::get_read_scn(true, ls_id, target_scn));
  EXPECT_EQ(upper_scn, target_scn);
  EXPECT_EQ(1, controlled_ts_mgr.get_call_count());
}

TEST_F(TestVectorIndexBackgroundTimestamp, follower_uses_ls_weak_read_snapshot)
{
  storage::ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
            MTL(storage::ObLSService *)->get_ls(LS_ID, ls_handle, storage::ObLSGetMod::SHARE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  storage::ObLSWRSHandler *wrs_handler = ls_handle.get_ls()->get_ls_wrs_handler();
  ASSERT_NE(nullptr, wrs_handler);

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS,
            weak_read_scn.convert_from_ts(common::ObTimeUtility::fast_current_time() - 30L * 1000L * 1000L));
  {
    common::ObSpinLockGuard guard(wrs_handler->lock_);
    wrs_handler->is_enabled_ = false;
    wrs_handler->ls_weak_read_ts_ = weak_read_scn;
  }

  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  ASSERT_NE(nullptr, txs);
  ControlledTsMgr controlled_ts_mgr;
  controlled_ts_mgr.set_result(OB_GTS_NOT_READY, SCN::invalid_scn());
  ScopedTsMgrOverride ts_mgr_override(*txs, controlled_ts_mgr);

  ObLSID ls_id = LS_ID;
  SCN target_scn;
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::get_read_scn(false, ls_id, target_scn));
  EXPECT_EQ(weak_read_scn, target_scn);
  EXPECT_EQ(0, controlled_ts_mgr.get_call_count());
}

TEST_F(TestVectorIndexBackgroundTimestamp, leader_propagates_snapshot_errors)
{
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  ASSERT_NE(nullptr, txs);

  SCN old_scn;
  ASSERT_EQ(OB_SUCCESS,
            old_scn.convert_from_ts(common::ObTimeUtility::fast_current_time() - 30L * 1000L * 1000L));
  const int expected_errors[] = {OB_GTS_NOT_READY, OB_ERR_UNEXPECTED};

  ControlledTsMgr controlled_ts_mgr;
  ScopedTsMgrOverride ts_mgr_override(*txs, controlled_ts_mgr);
  for (const int expected_error : expected_errors) {
    ObLSID ls_id = LS_ID;
    SCN target_scn = old_scn;
    controlled_ts_mgr.set_result(expected_error, SCN::invalid_scn());
    EXPECT_EQ(expected_error,
              ObPluginVectorIndexUtils::get_read_scn(true, ls_id, target_scn));
    EXPECT_EQ(1, controlled_ts_mgr.get_call_count());
  }
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_vector_index_background_timestamp.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
