/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan Pub v2.
 * You can use this software according to the terms and conditions of the License.
 */

#include <gtest/gtest.h>

#define private public
#define protected public
#define USING_LOG_PREFIX TRANS

#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_server.h"
#include "observer/report/ob_i_meta_report.h"
#include "share/ob_ddl_common.h"
#include "sql/ob_sql_trans_control.h"
#include "storage/access/ob_dml_param.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tablet/ob_tablet_start_transfer_mds_helper.h"
#include "storage/tx_storage/ob_access_service.h"
#include "tx_node.h"

namespace oceanbase
{
using namespace ::testing;
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share
{

ObTxDataThrottleGuard::~ObTxDataThrottleGuard() {}

int ObTenantTxDataAllocator::init(const char *label, TxShareThrottleTool *throttle_tool)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(
          storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ObTenantTxDataAllocator::ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}

int ObMemstoreAllocator::init()
{
  throttle_tool_ = &MTL_MEM_ALLOC_MGR.share_resource_throttle_tool();
  return arena_.init();
}

int ObMemstoreAllocator::AllocHandle::init()
{
  const uint64_t tenant_id = 1;
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->memstore_allocator();
  (void)host.init_handle(*this);
  return OB_SUCCESS;
}

} // namespace share

namespace concurrent_control
{

int check_sequence_set_violation(const concurrent_control::ObWriteFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t)
{
  return OB_SUCCESS;
}

} // namespace concurrent_control

namespace
{

class TestMetaReport final : public observer::ObIMetaReport
{
public:
  int submit_ls_update_task(const uint64_t, const ObLSID &) override
  {
    return OB_SUCCESS;
  }
};

// Invoke the production RPC body with a hand-built request.  This avoids a
// network stack in the unit test while preserving the get_ls() access
// attribute and every subsequent component access in process().
class ExposedTransferDestPrepareSCNP final
    : public obrpc::ObStorageGetTransferDestPrepareSCNP
{
public:
  ExposedTransferDestPrepareSCNP()
      : ObStorageGetTransferDestPrepareSCNP(nullptr)
  {}

  int process_with(const uint64_t tenant_id, const ObLSID &ls_id)
  {
    arg_.reset();
    arg_.tenant_id_ = tenant_id;
    arg_.ls_id_ = ls_id;
    return process();
  }
};

class ExposedFetchLSInfoP final : public obrpc::ObFetchLSInfoP
{
public:
  int process_with(const uint64_t tenant_id, const ObLSID &ls_id)
  {
    arg_.reset();
    arg_.tenant_id_ = tenant_id;
    arg_.ls_id_ = ls_id;
    arg_.version_ = DATA_CURRENT_VERSION;
    return process();
  }
};

class ExposedQueryLSIsValidMemberP final
    : public observer::ObQueryLSIsValidMemberP
{
public:
  ExposedQueryLSIsValidMemberP()
      : ObQueryLSIsValidMemberP(GCTX)
  {}

  int process_with(const uint64_t tenant_id,
                   const ObAddr &self_addr,
                   const ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    arg_.reset();
    result_.reset();
    arg_.tenant_id_ = tenant_id;
    arg_.self_addr_ = self_addr;
    if (OB_FAIL(arg_.ls_array_.push_back(ls_id))) {
      TRANS_LOG(WARN, "push ls id into query request failed", KR(ret), K(ls_id));
    } else {
      ret = process();
    }
    return ret;
  }

  const obrpc::ObQueryLSIsValidMemberResponse &response() const
  {
    return result_;
  }
};

class RegisteredLSGuard
{
public:
  RegisteredLSGuard(ObLSMap &ls_map, const ObLSID &ls_id)
      : ls_map_(ls_map), ls_id_(ls_id), registered_(true)
  {}

  ~RegisteredLSGuard()
  {
    if (registered_) {
      (void)ls_map_.del_ls(ls_id_);
    }
  }

private:
  ObLSMap &ls_map_;
  ObLSID ls_id_;
  bool registered_;
};

class TestLogonlyStorageBoundary : public ::testing::Test
{
public:
  void SetUp() override
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObAddr ip_port(ObAddr::VER::IPV4, "119.119.0.1", 2023);
    ObCurTraceId::init(ip_port);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    MTL_MEM_ALLOC_MGR.init();
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(1001));
  }

  void TearDown() override
  {
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }

  MsgBus bus_;
};

TEST_F(TestLogonlyStorageBoundary, ls_access_gate_rejects_explicit_disable_and_allows_log_paths)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.6", 2028);
  ObTxNode node(1, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10003);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_TRUE(logonly_ls.is_logonly_replica());

  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);
  const int32_t baseline_ref = logonly_ls.get_ref_mgr().get_total_ref_cnt();

  ObLSHandle default_handle;
  EXPECT_EQ(OB_LS_OFFLINE,
            node.ls_service_.get_ls(
                logonly_ls_id, default_handle, ObLSGetMod::STORAGE_MOD,
                ObLSAccessAttr::DISABLE_LOGONLY));
  EXPECT_FALSE(default_handle.is_valid());
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  ObLSHandle allow_handle;
  ASSERT_EQ(OB_SUCCESS,
            node.ls_service_.get_ls(
                logonly_ls_id, allow_handle, ObLSGetMod::LOG_MOD,
                ObLSAccessAttr::ALLOW_LOGONLY));
  ASSERT_TRUE(allow_handle.is_valid());
  EXPECT_EQ(&logonly_ls, allow_handle.get_ls());
  EXPECT_EQ(baseline_ref + 1, logonly_ls.get_ref_mgr().get_total_ref_cnt());
  allow_handle.reset();
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  ObLSHandle full_handle;
  EXPECT_EQ(OB_SUCCESS,
            node.ls_service_.get_ls(
                node.ls_id_, full_handle, ObLSGetMod::STORAGE_MOD,
                ObLSAccessAttr::DISABLE_LOGONLY));
  EXPECT_TRUE(full_handle.is_valid());
  ASSERT_NE(nullptr, full_handle.get_ls());
  ObLS *const full_ls = full_handle.get_ls();
  const int32_t full_ref_with_handle = full_ls->get_ref_mgr().get_total_ref_cnt();

  // A rejected lookup leaves a reused handle untouched: failure paths never
  // modify the output handle.
  EXPECT_EQ(OB_LS_OFFLINE,
            node.ls_service_.get_ls(
                logonly_ls_id, full_handle, ObLSGetMod::STORAGE_MOD,
                ObLSAccessAttr::DISABLE_LOGONLY));
  EXPECT_TRUE(full_handle.is_valid());
  EXPECT_EQ(full_ls, full_handle.get_ls());
  EXPECT_EQ(full_ref_with_handle, full_ls->get_ref_mgr().get_total_ref_cnt());
  full_handle.reset();
  EXPECT_EQ(full_ref_with_handle - 1, full_ls->get_ref_mgr().get_total_ref_cnt());
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  ObLSHandle missing_handle;
  EXPECT_EQ(OB_LS_NOT_EXIST,
            node.ls_service_.get_ls(
                ObLSID(logonly_ls_id.id() + 1), missing_handle,
                ObLSGetMod::STORAGE_MOD, ObLSAccessAttr::DISABLE_LOGONLY));
  EXPECT_FALSE(missing_handle.is_valid());

  // ID enumeration uses the same filter, and the output array is reusable.
  ObSArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(logonly_ls_id.id() + 2)));
  ASSERT_EQ(OB_SUCCESS, node.ls_service_.get_ls_ids(ls_ids, ObLSAccessAttr::ALLOW_LOGONLY));
  EXPECT_EQ(2, ls_ids.count());
  bool ids_contain_full = false;
  bool ids_contain_logonly = false;
  for (int64_t i = 0; i < ls_ids.count(); ++i) {
    ids_contain_full = ids_contain_full || ls_ids.at(i) == node.ls_id_;
    ids_contain_logonly = ids_contain_logonly || ls_ids.at(i) == logonly_ls_id;
  }
  EXPECT_TRUE(ids_contain_full);
  EXPECT_TRUE(ids_contain_logonly);
  ASSERT_EQ(OB_SUCCESS, node.ls_service_.get_ls_ids(ls_ids, ObLSAccessAttr::DISABLE_LOGONLY));
  ASSERT_EQ(1, ls_ids.count());
  EXPECT_EQ(node.ls_id_, ls_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, node.ls_service_.get_ls_ids(ls_ids, ObLSAccessAttr::ALLOW_LOGONLY));
  EXPECT_EQ(2, ls_ids.count());
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
  EXPECT_EQ(full_ref_with_handle - 1, full_ls->get_ref_mgr().get_total_ref_cnt());

  {
    ObArenaAllocator empty_allocator;
    ObLSMap empty_map;
    ObSArray<ObLSID> empty_ids;
    ASSERT_EQ(OB_SUCCESS, empty_map.init(node.tenant_id_, &empty_allocator));
    ASSERT_EQ(OB_SUCCESS, empty_map.get_all_ls_id(empty_ids, ObLSAccessAttr::DISABLE_LOGONLY));
    EXPECT_TRUE(empty_ids.empty());
    ASSERT_EQ(OB_SUCCESS, empty_map.get_all_ls_id(empty_ids, ObLSAccessAttr::ALLOW_LOGONLY));
    EXPECT_TRUE(empty_ids.empty());
  }

  ASSERT_EQ(OB_SUCCESS,
            node.ls_service_.iter_allocator_.init(
                common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                "AccessGateIter",
                node.tenant_id_,
                1024 * 1024));
  {
    common::ObSharedGuard<ObLSIterator> iter_guard;
    ASSERT_EQ(OB_SUCCESS,
              node.ls_service_.get_ls_iter(
                  iter_guard, ObLSGetMod::STORAGE_MOD,
                  ObLSAccessAttr::DISABLE_LOGONLY));
    ObLS *iter_ls = nullptr;
    bool saw_full = false;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter_guard->get_next(iter_ls))) {
      ASSERT_NE(nullptr, iter_ls);
      EXPECT_FALSE(iter_ls->is_logonly_replica());
      saw_full = saw_full || iter_ls->get_ls_id() == node.ls_id_;
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_TRUE(saw_full);
  }
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  {
    common::ObSharedGuard<ObLSIterator> iter_guard;
    ASSERT_EQ(OB_SUCCESS,
              node.ls_service_.get_ls_iter(
                  iter_guard, ObLSGetMod::LOG_MOD,
                  ObLSAccessAttr::ALLOW_LOGONLY));
    ObLS *iter_ls = nullptr;
    bool saw_logonly = false;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter_guard->get_next(iter_ls))) {
      ASSERT_NE(nullptr, iter_ls);
      saw_logonly = saw_logonly || iter_ls->get_ls_id() == logonly_ls_id;
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_TRUE(saw_logonly);
  }
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
}

TEST_F(TestLogonlyStorageBoundary, access_read_returns_retryable_error_before_store_context)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.12", 2034);
  ObTxNode node(7, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10009);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_TRUE(logonly_ls.is_logonly_replica());
  ASSERT_FALSE(logonly_ls.ls_tablet_svr_.is_inited_);
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);
  const int32_t baseline_ref = logonly_ls.get_ref_mgr().get_total_ref_cnt();

  ObAccessService access_service;
  ASSERT_EQ(OB_SUCCESS, access_service.init(&node.ls_service_));
  // E cannot be created in this branch, but is_log_replica() includes it.
  // The local access guard must reject every type filtered by LSMap.
  EXPECT_EQ(OB_REPLICA_NOT_READABLE,
            access_service.check_replica_type_accessible_(REPLICA_TYPE_ENCRYPTION_LOGONLY));
  EXPECT_EQ(OB_SUCCESS, access_service.check_replica_type_accessible_(REPLICA_TYPE_FULL));
  EXPECT_EQ(OB_SUCCESS, access_service.check_replica_type_accessible_(REPLICA_TYPE_READONLY));
  EXPECT_EQ(OB_SUCCESS, access_service.check_replica_type_accessible_(REPLICA_TYPE_COLUMNSTORE));
  const ObTabletID tablet_id(50002);
  ObTableScanParam scan_param;
  scan_param.tenant_id_ = node.tenant_id_;
  scan_param.ls_id_ = logonly_ls_id;
  scan_param.tablet_id_ = tablet_id;
  scan_param.index_id_ = 50002;
  scan_param.timeout_ = node.ts_after_ms(10000);
  scan_param.reserved_cell_count_ = 0;
  scan_param.schema_version_ = 0;
  scan_param.snapshot_.init_special_read(SCN::base_scn());
  scan_param.tx_id_ = ObTransID(1);
  ASSERT_TRUE(scan_param.is_valid());
  ObTabletHandle tablet_handle;
  ObStoreCtxGuard ctx_guard;
  const ObStoreAccessType access_types[] = {
      ObStoreAccessType::READ, ObStoreAccessType::READ_LATEST};

  // Exercise the production scan boundary and real LSMap, not just the
  // replica-type helper. Both scan modes must reject L before getting a
  // read context or a tablet, and retain the error that triggers DAS retry.
  for (const ObStoreAccessType access_type : access_types) {
    EXPECT_EQ(OB_REPLICA_NOT_READABLE,
              access_service.check_read_allowed_(
                  logonly_ls_id, tablet_id, access_type, scan_param,
                  tablet_handle, ctx_guard, SCN()));
    EXPECT_EQ(nullptr, ctx_guard.get_store_ctx().ls_);
    EXPECT_FALSE(ctx_guard.get_store_ctx().is_valid());
    EXPECT_FALSE(tablet_handle.is_valid());
    EXPECT_EQ(baseline_ref + 1, logonly_ls.get_ref_mgr().get_total_ref_cnt());
    ctx_guard.reset();
    EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
  }
  EXPECT_FALSE(logonly_ls.ls_tablet_svr_.is_inited_);

  EXPECT_EQ(OB_LS_NOT_EXIST,
            access_service.check_read_allowed_(
                ObLSID(logonly_ls_id.id() + 1), tablet_id, ObStoreAccessType::READ,
                scan_param, tablet_handle, ctx_guard, SCN()));
  EXPECT_FALSE(ctx_guard.get_ls_handle().is_valid());
  EXPECT_FALSE(tablet_handle.is_valid());
  ctx_guard.reset();
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
}

TEST_F(TestLogonlyStorageBoundary, access_write_returns_retryable_error_before_store_context)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.13", 2035);
  ObTxNode node(8, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10010);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_TRUE(logonly_ls.is_logonly_replica());
  ASSERT_FALSE(logonly_ls.ls_tablet_svr_.is_inited_);
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);
  const int32_t baseline_ref = logonly_ls.get_ref_mgr().get_total_ref_cnt();

  ObAccessService access_service;
  ASSERT_EQ(OB_SUCCESS, access_service.init(&node.ls_service_));
  ObTxDescGuard tx_guard = node.get_tx_guard();
  ASSERT_TRUE(tx_guard.is_valid());
  ObTxDesc &tx = tx_guard.get_tx_desc();
  ObTxParam tx_param;
  tx_param.timeout_us_ = 10000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ASSERT_EQ(OB_SUCCESS, node.start_tx(tx, tx_param));
  ASSERT_TRUE(tx.is_valid());
  ASSERT_TRUE(tx.is_clean());
  const auto initial_state = tx.state_;
  ObTxReadSnapshot snapshot;
  snapshot.init_none_read();
  concurrent_control::ObWriteFlag write_flag;
  ObStoreCtxGuard ctx_guard;

  // This public entry is used before DML and row locking. Reject L here,
  // before get_write_store_ctx can acquire a participant or touch LS data.
  // Repeating the request also checks that failure resets the output guard.
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(OB_REPLICA_NOT_READABLE,
              access_service.get_write_store_ctx_guard(
                  logonly_ls_id, node.ts_after_ms(10000), tx, snapshot,
                  0 /* branch_id */, write_flag, ctx_guard));
    EXPECT_FALSE(ctx_guard.is_inited_);
    EXPECT_FALSE(ctx_guard.get_ls_handle().is_valid());
    EXPECT_EQ(nullptr, ctx_guard.get_store_ctx().ls_);
    EXPECT_FALSE(ctx_guard.get_store_ctx().is_valid());
    EXPECT_EQ(initial_state, tx.state_);
    EXPECT_TRUE(tx.is_clean());
    EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
  }
  EXPECT_FALSE(logonly_ls.ls_tablet_svr_.is_inited_);

  EXPECT_EQ(OB_LS_NOT_EXIST,
            access_service.get_write_store_ctx_guard(
                ObLSID(logonly_ls_id.id() + 1), node.ts_after_ms(10000), tx,
                snapshot, 0 /* branch_id */, write_flag, ctx_guard));
  EXPECT_FALSE(ctx_guard.is_inited_);
  EXPECT_FALSE(ctx_guard.get_ls_handle().is_valid());
  EXPECT_TRUE(tx.is_clean());
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
  ASSERT_EQ(OB_SUCCESS, node.rollback_tx(tx));
}

TEST_F(TestLogonlyStorageBoundary, local_ls_readability_preserves_logonly_success_and_false)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.14", 2036);
  ObTxNode node(9, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());
  ObAddr &self_addr = observer::ObServer::get_instance().self_addr_;
  const ObAddr saved_self_addr(self_addr);
  DEFER(self_addr = saved_self_addr);
  self_addr = addr;

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10011);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_TRUE(logonly_ls.is_logonly_replica());
  ASSERT_FALSE(logonly_ls.ls_wrs_handler_.is_inited_);
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);
  const int32_t baseline_ref = logonly_ls.get_ref_mgr().get_total_ref_cnt();

  // A fresh timestamp must not make an L readable: its replica-type guard
  // runs before consulting the uninitialized weak-read handler.
  SCN fresh_scn;
  ASSERT_EQ(OB_SUCCESS, fresh_scn.convert_from_ts(node.ts_after_ms(60000)));
  logonly_ls.ls_wrs_handler_.ls_weak_read_ts_ = fresh_scn;
  const int64_t max_stale_time_us = 1000000;
  bool can_read = true;
  EXPECT_EQ(OB_SUCCESS,
            sql::ObSqlTransControl::check_ls_readable(
                node.tenant_id_, logonly_ls_id, addr, max_stale_time_us, can_read));
  EXPECT_FALSE(can_read);
  EXPECT_FALSE(logonly_ls.ls_wrs_handler_.is_inited_);
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  // The same public API must still use the timestamp for an F replica and
  // preserve lookup errors, rather than translating every failure to false.
  ASSERT_EQ(REPLICA_TYPE_FULL, node.fake_ls_.get_replica_type());
  const int32_t full_baseline_ref = node.fake_ls_.get_ref_mgr().get_total_ref_cnt();
  node.fake_ls_.ls_wrs_handler_.ls_weak_read_ts_ = fresh_scn;
  EXPECT_EQ(OB_SUCCESS,
            sql::ObSqlTransControl::check_ls_readable(
                node.tenant_id_, node.ls_id_, addr, max_stale_time_us, can_read));
  EXPECT_TRUE(can_read);
  node.fake_ls_.ls_wrs_handler_.ls_weak_read_ts_ = SCN::base_scn();
  EXPECT_EQ(OB_SUCCESS,
            sql::ObSqlTransControl::check_ls_readable(
                node.tenant_id_, node.ls_id_, addr, max_stale_time_us, can_read));
  EXPECT_FALSE(can_read);
  EXPECT_EQ(full_baseline_ref, node.fake_ls_.get_ref_mgr().get_total_ref_cnt());

  can_read = true;
  EXPECT_EQ(OB_LS_NOT_EXIST,
            sql::ObSqlTransControl::check_ls_readable(
                node.tenant_id_, ObLSID(logonly_ls_id.id() + 1), addr,
                max_stale_time_us, can_read));
  EXPECT_FALSE(can_read);
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
}

TEST_F(TestLogonlyStorageBoundary,
       query_valid_member_rejects_logonly_replica)
{
  ObAddr server_addr(ObAddr::VER::IPV4, "119.119.0.11", 2033);
  ObTxNode node(6, server_addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10008);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_TRUE(logonly_ls.is_logonly_replica());
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);
  const int32_t baseline_ref = logonly_ls.get_ref_mgr().get_total_ref_cnt();

  ExposedQueryLSIsValidMemberP processor;

  // Send a valid request to the processor while its local LS is a real
  // LOGONLY replica. DISABLE_LOGONLY rejects the L replica at the LS access
  // boundary and the processor preserves the RPC contract as OB_NOT_MASTER.
  ASSERT_TRUE(server_addr.is_valid());
  ASSERT_EQ(OB_SUCCESS,
            processor.process_with(1001, server_addr, logonly_ls_id));
  ASSERT_EQ(OB_SUCCESS, processor.response().ret_value_);
  ASSERT_EQ(1, processor.response().ls_array_.count());
  ASSERT_EQ(1, processor.response().ret_array_.count());
  EXPECT_EQ(logonly_ls_id, processor.response().ls_array_[0]);
  EXPECT_EQ(OB_NOT_MASTER, processor.response().ret_array_[0]);
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  // The compatibility mapping is specific to a physically present L replica;
  // a missing LS must retain its original error code.
  const ObLSID missing_ls_id(logonly_ls_id.id() + 1);
  ASSERT_EQ(OB_SUCCESS,
            processor.process_with(1001, server_addr, missing_ls_id));
  ASSERT_EQ(OB_SUCCESS, processor.response().ret_value_);
  ASSERT_EQ(1, processor.response().ret_array_.count());
  EXPECT_EQ(OB_LS_NOT_EXIST, processor.response().ret_array_[0]);
  EXPECT_EQ(baseline_ref, logonly_ls.get_ref_mgr().get_total_ref_cnt());
}

TEST_F(TestLogonlyStorageBoundary, initialized_logonly_requires_create_finish_before_data_probe)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.7", 2029);
  ObTxNode node(2, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10004);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);

  // This is the construction-time fact the following probes rely on: a
  // logonly ObLS has log lifecycle components but no tablet/transfer/DDL or
  // reserved-snapshot data-plane components.
  ASSERT_TRUE(logonly_ls.is_logonly_replica());
  EXPECT_FALSE(logonly_ls.ls_tablet_svr_.is_inited_);
  EXPECT_FALSE(logonly_ls.ls_transfer_status_.is_inited_);
  EXPECT_FALSE(logonly_ls.reserved_snapshot_mgr_.is_inited_);
  EXPECT_FALSE(logonly_ls.ls_ddl_log_handler_.is_inited_);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
            node.ls_service_.get_ls(logonly_ls_id,
                                    ls_handle,
                                    ObLSGetMod::STORAGE_MOD,
                                    ObLSAccessAttr::ALLOW_LOGONLY));
  ASSERT_EQ(&logonly_ls, ls_handle.get_ls());

  // Transfer's migration-status probe is metadata-only. It must remain
  // usable on L without initializing TabletService or transfer data state.
  const int32_t refs_before_status_probe = logonly_ls.get_ref_mgr().get_total_ref_cnt();
  bool can_skip = true;
  EXPECT_EQ(OB_SUCCESS,
            ObTabletStartTransferInHelper::check_transfer_dest_ls_status_(
                SCN::base_scn(), logonly_ls_id, can_skip));
  EXPECT_FALSE(can_skip); // The fixture starts in MIGRATION_STATUS_NONE.
  EXPECT_EQ(refs_before_status_probe, logonly_ls.get_ref_mgr().get_total_ref_cnt());
  EXPECT_FALSE(logonly_ls.ls_tablet_svr_.is_inited_);
  EXPECT_FALSE(logonly_ls.ls_transfer_status_.is_inited_);
  can_skip = true;
  EXPECT_EQ(OB_LS_NOT_EXIST,
            ObTabletStartTransferInHelper::check_transfer_dest_ls_status_(
                SCN::base_scn(), ObLSID(logonly_ls_id.id() + 1), can_skip));
  EXPECT_FALSE(can_skip);
  EXPECT_EQ(refs_before_status_probe, logonly_ls.get_ref_mgr().get_total_ref_cnt());

  // This lightweight fixture intentionally stops at ls.init(). Keep its
  // stopped-state assertion separate from the data-component boundary probe
  // in TestLSService::set_member_list_rpc_initializes_logonly_palf, which uses
  // a real created LS and verifies that LS/PALF metadata is available first.
  ASSERT_TRUE(logonly_ls.is_stopped());
  ObLSMetaPackage meta_package;
  ObSArray<ObTabletID> tablet_ids;
  EXPECT_EQ(OB_NOT_RUNNING,
            ls_handle.get_ls()->get_ls_meta_package_and_tablet_ids(
                false /* check_archive */, false /* need_sorted_tablet_id */, meta_package, tablet_ids));
  EXPECT_TRUE(tablet_ids.empty());

  // This is the corresponding DDL data-plane guard: an L can be obtained by
  // ALLOW_LOGONLY but must not yield a Tablet/DDLKV handle.
  ObTabletHandle tablet_handle;
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            ObDDLUtil::ddl_get_tablet(
                ls_handle, ObTabletID(50001), tablet_handle));
  EXPECT_FALSE(tablet_handle.is_valid());

  // The public entry of ReservedSnapshotMgr is safe today; this establishes
  // the expected characterization for a late timer/callback directed at L.
  EXPECT_EQ(OB_NOT_INIT,
            logonly_ls.reserved_snapshot_mgr_.try_sync_reserved_snapshot(
                100, true /* update_flag */));

}

TEST_F(TestLogonlyStorageBoundary,
       reserved_snapshot_prepare_directly_derefs_uninitialized_ls)
{
  // The preceding L-replica test establishes that its ReservedSnapshotMgr has
  // this same uninitialized state.  Do not start an ObTxNode in this probe:
  // the crash happens before any LS-service access is needed.
  ObLSReservedSnapshotMgr reserved_snapshot_mgr;
  ASSERT_FALSE(reserved_snapshot_mgr.is_inited_);

  auto invoke_prepare_without_init = [&reserved_snapshot_mgr]() {
    int64_t update_version = 100;
    char *clog_buf = nullptr;
    int64_t clog_len = 0;
    (void)reserved_snapshot_mgr.prepare_struct_in_lock(
        update_version, nullptr, clog_buf, clog_len);
  };

  // The virtual callback itself dereferences ls_ before it can call get_ls().
  // Once the production guard is added, replace this death expectation with
  // EXPECT_EQ(OB_NOT_INIT, ...).
  const std::string old_death_test_style = GTEST_FLAG(death_test_style);
  GTEST_FLAG(death_test_style) = "threadsafe";
  EXPECT_DEATH_IF_SUPPORTED(invoke_prepare_without_init(), "");
  GTEST_FLAG(death_test_style) = old_death_test_style;
}

TEST_F(TestLogonlyStorageBoundary, transfer_prepare_rpc_rejects_logonly_at_boundary)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.8", 2030);
  ObTxNode node(3, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10005);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);

  // Transfer prepare depends on data-plane state that is unavailable on an L
  // replica. DISABLE_LOGONLY must reject the request at the LS boundary.
  ExposedTransferDestPrepareSCNP processor;
  EXPECT_EQ(OB_LS_OFFLINE, processor.process_with(1001, logonly_ls_id));
}

TEST_F(TestLogonlyStorageBoundary, fetch_ls_info_rpc_rejects_missing_archive_service)
{
  ObAddr addr(ObAddr::VER::IPV4, "119.119.0.9", 2031);
  ObTxNode node(4, addr, bus_);
  ASSERT_EQ(OB_SUCCESS, node.start());

  TestMetaReport reporter;
  const ObLSID logonly_ls_id(node.ls_id_.id() + 10006);
  ObLS logonly_ls;
  ASSERT_EQ(OB_SUCCESS,
            node.init_ls_for_test(
                logonly_ls, logonly_ls_id, REPLICA_TYPE_LOGONLY, &reporter));
  ASSERT_EQ(OB_SUCCESS,
            logonly_ls.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, node.add_ls_for_test(logonly_ls));
  RegisteredLSGuard registered_ls_guard(node.ls_service_.ls_map_, logonly_ls_id);

  // The lightweight transaction fixture does not register ArchiveService.
  // Fetching HA metadata must return a deterministic error instead of
  // dereferencing a null tenant-local service.
  ExposedFetchLSInfoP processor;
  EXPECT_EQ(OB_ERR_UNEXPECTED, processor.process_with(1001, logonly_ls_id));
}

} // namespace
} // namespace oceanbase

int main(int argc, char **argv)
{
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_logonly_storage_boundary.log", true, false,
                       "test_logonly_storage_boundary.log",
                       "test_logonly_storage_boundary.log",
                       "test_logonly_storage_boundary.log");
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
