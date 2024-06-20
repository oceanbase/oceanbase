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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <thread>
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/mockcontainer/mock_ob_iterator.h"
#include "storage/mockcontainer/mock_ob_end_trans_callback.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{

namespace storage
{
int ObDMLRunningCtx::init(
    const common::ObIArray<uint64_t> *column_ids,
    const common::ObIArray<uint64_t> *upd_col_ids,
    ObMultiVersionSchemaService *schema_service,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!store_ctx_.is_valid())
      || OB_UNLIKELY(!dml_param_.is_valid())
      || OB_ISNULL(dml_param_.table_param_)
      || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(store_ctx_),
        K(dml_param_), KP(schema_service));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const uint64_t table_id = dml_param_.table_param_->get_data_table().get_table_id();
    const int64_t version = dml_param_.schema_version_;
    const int64_t tenant_schema_version = dml_param_.tenant_schema_version_;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(prepare_relative_table(
      dml_param_.table_param_->get_data_table(),
      tablet_handle,
      store_ctx_.mvcc_acc_ctx_.get_snapshot_version()))) {
    LOG_WARN("failed to get relative table", K(ret), K(dml_param_));
  } else if (NULL != column_ids && OB_FAIL(prepare_column_info(*column_ids))) {
    LOG_WARN("fail to get column descriptions and column map", K(ret), K(*column_ids));
  } else {
    store_ctx_.table_version_ = dml_param_.schema_version_;
    column_ids_ = column_ids;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    relative_table_.destroy();
  }
  return ret;
}
}
namespace transaction
{

int ObLocationAdapter::get_leader_(const int64_t cluster_id,
                                   const int64_t tenant_id,
                                   const ObLSID &ls_id,
                                   common::ObAddr &leader,
                                   const bool is_sync)
{
  UNUSED(cluster_id);
  UNUSED(tenant_id);
  UNUSED(ls_id);
  UNUSED(is_sync);
  leader = GCTX.self_addr();
  return OB_SUCCESS;
}
}

using namespace storage;

class TestTrans : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    LOG_INFO("SetUpTestCase");
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(MTL_ID()));
    uint64_t version = cal_version(4, 3, 0, 0);
    ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().init(version));
    ObClusterVersion::get_instance().tenant_config_mgr_ = &omt::ObTenantConfigMgr::get_instance();

    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  }
  static void TearDownTestCase()
  {
    LOG_INFO("TearDownTestCase");
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
  void create_ls(uint64_t tenant_id, ObLSID &ls_id, ObLS *&ls);
  void insert_rows(ObLSID &ls_id, ObTabletID &tablet_id, ObTxDesc &tx_desc, ObTxReadSnapshot snapshot, const char* in_str);
  void prepare_tx_desc(ObTxDesc *&tx_desc, ObTxReadSnapshot &snapshot);
private:
  static share::schema::ObTableSchema table_schema_;
  common::ObArenaAllocator allocator_;
};

share::schema::ObTableSchema TestTrans::table_schema_;

void TestTrans::create_ls(uint64_t tenant_id, ObLSID &ls_id, ObLS *&ls)
{
  LOG_INFO("create log stream");
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ObLSService* ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  // set member list
  LOG_INFO("set member list");
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ObMemberList member_list;
  int64_t paxos_replica_num = 1;
  (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
  GlobalLearnerList learner_list;
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list,
                                                    paxos_replica_num,
                                                    learner_list));

  // check leader
  LOG_INFO("check leader");
  for (int i = 0; i < 15; i++) {
    ObRole role;
    int64_t leader_epoch = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, leader_epoch));
    if (role == ObRole::LEADER) {
      break;
    }
    ::sleep(1);
  }
}

void TestTrans::insert_rows(ObLSID &ls_id, ObTabletID &tablet_id, ObTxDesc &tx_desc, ObTxReadSnapshot snapshot, const char* ins_str)
{
  int64_t affected_rows = 0;
  ObMockNewRowIterator ins_iter;
  ASSERT_EQ(OB_SUCCESS, ins_iter.from(ins_str));

  ObArenaAllocator allocator;
  share::schema::ObTableDMLParam table_dml_param(allocator);

  ObSEArray<const ObTableSchema *, 1> index_schema_array;
  ObSEArray<uint64_t, 1> column_ids;
  column_ids.push_back(16); // pk
  ASSERT_EQ(OB_SUCCESS, table_dml_param.convert(&table_schema_, 1000, column_ids));
  ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + 100000000;
  dml_param.schema_version_ = 1000;
  dml_param.table_param_ = &table_dml_param;
  dml_param.snapshot_ = snapshot;
  dml_param.store_ctx_guard_ = &store_ctx_guard;

  auto as = MTL(ObAccessService*);
  LOG_INFO("storage access by dml");
  ASSERT_EQ(OB_SUCCESS, as->get_write_store_ctx_guard(ls_id,
                                                      dml_param.timeout_,
                                                      tx_desc,
                                                      snapshot,
                                                      0,/*branch_id*/
                                                      store_ctx_guard));
  ASSERT_EQ(OB_SUCCESS, as->insert_rows(ls_id,
                                         tablet_id,
                                         tx_desc,
                                         dml_param,
                                         column_ids,
                                         &ins_iter,
                                         affected_rows));

}

void TestTrans::prepare_tx_desc(ObTxDesc *&tx_desc, ObTxReadSnapshot &snapshot)
{
  ObTxParam tx_p;
  tx_p.timeout_us_ = ObTimeUtility::current_time() + 100000000; // us
  tx_p.access_mode_ = transaction::ObTxAccessMode::RW;
  tx_p.isolation_ = transaction::ObTxIsolationLevel::RC;
  tx_p.cluster_id_ = 1;
  ObTransService *tx_service = MTL(ObTransService*);
  // alloc tx_desc
  ASSERT_EQ(OB_SUCCESS, tx_service->acquire_tx(tx_desc, 100));
  // init tx_desc and put in map
  ASSERT_EQ(OB_SUCCESS, tx_service->start_tx(*tx_desc, tx_p));

  LOG_INFO("get snapshot for dml");
  {
    int64_t expire_ts = ObTimeUtility::current_time() + 100000000;
    ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
    ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, snapshot));
  }
}

TEST_F(TestTrans, create_ls_and_tablet)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y1-1111111111111111-0-0"));
  ObLSID ls_id(100);
  ObLS *ls = nullptr;
  uint64_t tenant_id = MTL_ID();
  create_ls(tenant_id, ls_id, ls);

  LOG_INFO("create tablet");
  ObTabletID tablet_id(1001);
  ObLSTabletService *ls_tablet_svr = ls->get_tablet_svr();
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema_, table_id));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema_, allocator_));
}

TEST_F(TestTrans, basic)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  // create log stream
  ObLSID ls_id(100);
  ObTabletID tablet_id(1001);

  LOG_INFO("start transaction");
  ObTxDesc *tx_desc = NULL;
  ObTxReadSnapshot snapshot;
  prepare_tx_desc(tx_desc, snapshot);
  // prepare insert param
  const char *ins_str =
      "bigint    dml          \n"
      "50        T_DML_INSERT \n";
  insert_rows(ls_id, tablet_id, *tx_desc, snapshot, ins_str);

  ObTransService *tx_service = MTL(ObTransService*);
  ObPartTransCtx *part_ctx;
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.get_tx_ctx(ls_id, tx_desc->tx_id_, false, part_ctx));
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.revert_tx_ctx(part_ctx));

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 0));
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_protected_memtable_mgr_handle(protected_handle));
  ASSERT_EQ(1, protected_handle->memtable_mgr_handle_.get_memtable_mgr()->get_memtable_count_());
  ObTableHandleV2 mt_handle;
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_active_memtable(mt_handle));
  memtable::ObMemtable *mt;
  ASSERT_EQ(OB_SUCCESS, mt_handle.get_data_memtable(mt));

  printf("mt size=%ld occ_size=%ld bree_item_count=%ld rec_scn=%s end_scn=%s\n", mt->get_size(), mt->get_occupied_size(), mt->get_btree_item_count(), to_cstring(mt->get_rec_scn()),to_cstring(mt->get_max_end_scn()));

  /*
  ObStoreRow store_row;
  store_row.flag_.set_flag(ObDmlFlag::DF_INSERT);
  ObNewRow *new_row;
  ASSERT_EQ(OB_SUCCESS, ins_iter.get_next_row(new_row));
  store_row.row_val_ = *new_row;
  ObStoreRowkey tmp_key;
  memtable::ObMemtableKey mtk;

  ASSERT_EQ(OB_SUCCESS, tmp_key.assign(store_row.row_val_.cells_, 1));
  ASSERT_EQ(OB_SUCCESS, mtk.encode(columns, &tmp_key));
  mt->get_query_engine().get();
  */

  memtable::ObTransCallbackMgr &trans_mgr = part_ctx->mt_ctx_.trans_mgr_;
  printf("part_ctx trans_mgr:%ld\n",trans_mgr.callback_list_.get_length());
  auto cb = trans_mgr.callback_list_.head_.next_;
  for (int i = 0; i < trans_mgr.callback_list_.get_length(); i++) {
    memtable::ObMvccRowCallback *row_cb = dynamic_cast<memtable::ObMvccRowCallback*>(cb);
    if (row_cb != nullptr) {
      printf("mvcc row: ptr=%p node_cnt=%ld\n", &row_cb->value_, row_cb->value_.get_total_trans_node_cnt());
    } else {
      ObOBJLockCallback *tl_cb = dynamic_cast<ObOBJLockCallback*>(cb);
      if (tl_cb != nullptr) {
        printf("table lock node: lock_id=%ld op_type=%c\n", tl_cb->lock_op_->lock_op_.lock_id_.obj_id_, tl_cb->lock_op_->lock_op_.op_type_);
      } else {
        abort();
      }
    }
    cb = cb->next_;
  }
  LOG_INFO("commit transaction");
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, ObTimeUtility::current_time() + 100000000));

  LOG_INFO("release transaction");
  tx_service->release_tx(*tx_desc);

  //ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));
}

TEST_F(TestTrans, dist_trans)
{
  ASSERT_EQ(OB_SUCCESS, ObCurTraceId::get_trace_id()->set("Y2-2222222222222222-0-0"));
  uint64_t tenant_id = MTL_ID();
  ObLSID ls_id(100);
  ObTabletID tablet_id(1001);

  ObLSID ls_id2(101);
  ObLS *ls2 = nullptr;
  create_ls(tenant_id, ls_id2, ls2);

  // create tablet
  LOG_INFO("create tablet");
  ObTabletID tablet_id2(1002);

  ObLSTabletService *ls_tablet_svr = ls2->get_tablet_svr();
  ObLSService* ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle2;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id2, ls_handle2, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle2, tablet_id2, table_schema_, allocator_));


  LOG_INFO("start transaction");
  ObTxDesc *tx_desc = NULL;
  ObTxReadSnapshot snapshot;
  prepare_tx_desc(tx_desc, snapshot);

  const char *ins_str =
      "bigint    dml          \n"
      "100        T_DML_INSERT \n";
  insert_rows(ls_id, tablet_id, *tx_desc, snapshot, ins_str);

  const char *ins_str2 =
      "bigint    dml          \n"
      "200        T_DML_INSERT \n";
  insert_rows(ls_id2, tablet_id2, *tx_desc, snapshot, ins_str2);

  ObTransService *tx_service = MTL(ObTransService*);
  LOG_INFO("commit transaction");
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, ObTimeUtility::current_time() + 10000000000));

  LOG_INFO("release transaction");
  tx_service->release_tx(*tx_desc);
}

/*
TEST_F(TestTrans, freeze)
{
  ObLSID ls_id(100);
  ObLSHandle ls_handle;
  ObLS *ls;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_EQ(OB_SUCCESS, ls->logstream_freeze());
}
*/
TEST_F(TestTrans, transfer_block)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObLSID ls_id(100);
  ObTabletID tablet_id(1001);

  LOG_INFO("start transaction");
  ObTxDesc *tx_desc = NULL;
  ObTxReadSnapshot snapshot;
  prepare_tx_desc(tx_desc, snapshot);
  // prepare insert param
  const char *ins_str =
      "bigint    dml          \n"
      "300        T_DML_INSERT \n";
  insert_rows(ls_id, tablet_id, *tx_desc, snapshot, ins_str);

  ObTransService *tx_service = MTL(ObTransService*);
  ObPartTransCtx *part_ctx;
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.get_tx_ctx(ls_id, tx_desc->tx_id_, false, part_ctx));
  part_ctx->sub_state_.set_transfer_blocking();
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.revert_tx_ctx(part_ctx));

  std::thread th([part_ctx] () {
    ::sleep(3);
    part_ctx->sub_state_.clear_transfer_blocking();
  });

  LOG_INFO("commit transaction");
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, ObTimeUtility::current_time() + 100000000));

  LOG_INFO("release transaction");
  tx_service->release_tx(*tx_desc);

  th.join();
}

TEST_F(TestTrans, transfer_block2)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObLSID ls_id(100);
  ObTabletID tablet_id(1001);

  LOG_INFO("start transaction");
  ObTxDesc *tx_desc = NULL;
  ObTxReadSnapshot snapshot;
  prepare_tx_desc(tx_desc, snapshot);
  // prepare insert param
  const char *ins_str =
      "bigint    dml          \n"
      "400        T_DML_INSERT \n";
  insert_rows(ls_id, tablet_id, *tx_desc, snapshot, ins_str);

  ObTransService *tx_service = MTL(ObTransService*);
  ObPartTransCtx *part_ctx;
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.get_tx_ctx(ls_id, tx_desc->tx_id_, false, part_ctx));
  bool is_blocked = false;
  part_ctx->sub_state_.set_transfer_blocking();
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.revert_tx_ctx(part_ctx));

  std::thread th([part_ctx] () {
    ::sleep(3);
    part_ctx->sub_state_.clear_transfer_blocking();
  });

  LOG_INFO("rollback transaction");
  ASSERT_EQ(OB_SUCCESS, tx_service->rollback_tx(*tx_desc));

  LOG_INFO("release transaction");
  tx_service->release_tx(*tx_desc);
  th.join();
}

TEST_F(TestTrans, tablet_to_ls_cache)
{
  // 0. init
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObLSID ls_id_1(1201);
  ObLSID ls_id_2(1202);
  ObLS *ls1 = nullptr;
  ObLS *ls2 = nullptr;
  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ObTransService *tx_service = MTL(ObTransService*);
  ASSERT_EQ(true, tx_service->is_inited_);
  ASSERT_EQ(true, tx_service->tablet_to_ls_cache_.is_inited_);
  create_ls(tenant_id, ls_id_1, ls1);
  create_ls(tenant_id, ls_id_2, ls2);
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.get_ls_tx_ctx_mgr(ls_id_1, ls_tx_ctx_mgr));
  int64_t base_ref = ls_tx_ctx_mgr->get_ref();
  int64_t base_size = tx_service->tablet_to_ls_cache_.size();

  // 1. insert tablet
  const int TABLET_NUM = 10;
  ObSEArray<ObTabletID, TABLET_NUM> tablet_ids;
  ObSEArray<ObTabletID, TABLET_NUM> tablet_ids_2;
  for (int i = 0; i < TABLET_NUM; i++) {
    ObTabletID tablet_id(1300 + i);
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
    ObTabletID tablet_id_2(1600 + i);
    ASSERT_EQ(OB_SUCCESS, tablet_ids_2.push_back(tablet_id_2));
  }
  ASSERT_EQ(TABLET_NUM, tablet_ids.count());
  ARRAY_FOREACH(tablet_ids, i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    ASSERT_EQ(OB_SUCCESS, tx_service->create_tablet(tablet_id, i < TABLET_NUM/2 ? ls_id_1:ls_id_2));
  }
  ASSERT_EQ(TABLET_NUM + base_size, tx_service->tablet_to_ls_cache_.size());
  ASSERT_EQ(TABLET_NUM/2 + base_ref, ls_tx_ctx_mgr->get_ref());

  // repeated inserts will not fail, but cache entries count will not grow.
  ARRAY_FOREACH(tablet_ids, i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    ASSERT_EQ(OB_SUCCESS, tx_service->create_tablet(tablet_id, i < TABLET_NUM/2 ? ls_id_1:ls_id_2));
  }
  ASSERT_EQ(TABLET_NUM + base_size, tx_service->tablet_to_ls_cache_.size());
  ASSERT_EQ(TABLET_NUM/2 + base_ref, ls_tx_ctx_mgr->get_ref());

  // 2. check and get ls
  ObLSID ls_id;
  bool is_local = false;
  ARRAY_FOREACH(tablet_ids, i) {
    // tablet exist
    const ObTabletID &tablet_id = tablet_ids.at(i);
    ls_id.reset();
    is_local = false;
    ASSERT_EQ(OB_SUCCESS, tx_service->check_and_get_ls_info(tablet_id, ls_id, is_local));
    ASSERT_EQ((i < TABLET_NUM/2 ? ls_id_1.id():ls_id_2.id()), ls_id.id());
    ASSERT_EQ(true, is_local);
    // tablet not exist
    const ObTabletID &tablet_id_2 = tablet_ids_2.at(i);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, tx_service->check_and_get_ls_info(tablet_id_2, ls_id, is_local));
  }
  ASSERT_EQ(TABLET_NUM + base_size, tx_service->tablet_to_ls_cache_.size());

  // 3. remove tablet
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_2));
  ASSERT_EQ(TABLET_NUM/2, tx_service->tablet_to_ls_cache_.size());
  for (int i = 0; i < TABLET_NUM/2; i++) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    ls_id.reset();
    is_local = false;
    ASSERT_EQ(OB_SUCCESS, tx_service->check_and_get_ls_info(tablet_id, ls_id, is_local));
    ASSERT_EQ(ls_id_1, ls_id);
    ASSERT_EQ(true, is_local);
    tx_service->remove_tablet(tablet_id, ls_id);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, tx_service->check_and_get_ls_info(tablet_id, ls_id, is_local));
  }
  ASSERT_EQ(0, tx_service->tablet_to_ls_cache_.size());
  ASSERT_EQ(base_ref, ls_tx_ctx_mgr->get_ref());

  // 4. clear
  ASSERT_EQ(OB_SUCCESS, tx_service->tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_1));
}

TEST_F(TestTrans, remove_ls)
{
  ObLSID ls_id(100);
  ObLSID ls_id2(101);
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id2));
  ObTransService *tx_service = MTL(ObTransService*);
  ASSERT_EQ(0, tx_service->tablet_to_ls_cache_.size());
}

} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_trans.log*");
  OB_LOGGER.set_file_name("test_trans.log",true, false, "test_trans.log", "test_trans.log");
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
