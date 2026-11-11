// owner: jiahua.cjh
// owner group: storage

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

#include <gmock/gmock.h>
#include <cstdio>
#include <unistd.h>

#define protected public
#define private public

#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf_handle_guard.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/ob_storage_schema_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
int ObClusterVersion::get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version)
{
  // Keep one version for the entire process, including MTL/PALF worker threads.
  // The explicit 4.3.5.6 schemas below still exercise old migration parameters.
  data_version = DATA_CURRENT_VERSION;
  return OB_SUCCESS;
}
namespace storage
{
class TestLSMigrationParam : public ::testing::Test
{
public:
  TestLSMigrationParam();
  virtual ~TestLSMigrationParam() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp() override;
  virtual void TearDown() override;
public:
  static const uint64_t TEST_TENANT_ID = 1;
  static const int64_t TEST_LS_ID = 101;

  blocksstable::ObSSTableMeta sstable_meta_;
  common::ObArenaAllocator allocator_;
};

TestLSMigrationParam::TestLSMigrationParam()
  : sstable_meta_()
{
}

void TestLSMigrationParam::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  // Drive rebuild steps synchronously for this suite; never restart its worker.
  // This avoids races with the production scheduler while keeping its real
  // LS lookup, metadata persistence and ctx-map implementation under test.
  ASSERT_NE(nullptr, MTL(ObRebuildService*));
  MTL(ObRebuildService*)->stop();
  MTL(ObRebuildService*)->wait();
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestLSMigrationParam::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestLSMigrationParam::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  ObMetaDiskAddr addr;
  addr.set_none_addr();

  sstable_meta_.basic_meta_.row_count_ = 0;
  sstable_meta_.basic_meta_.occupy_size_ = 0;
  sstable_meta_.basic_meta_.data_checksum_ = 0;
  sstable_meta_.basic_meta_.index_type_ = 0;
  sstable_meta_.basic_meta_.rowkey_column_count_ = 0;
  sstable_meta_.basic_meta_.column_cnt_ = 0;
  sstable_meta_.basic_meta_.index_macro_block_count_ = 0;
  sstable_meta_.basic_meta_.use_old_macro_block_count_ = 0;
  sstable_meta_.basic_meta_.index_macro_block_count_ = 0;
  sstable_meta_.basic_meta_.sstable_format_version_ = 0;
  sstable_meta_.basic_meta_.schema_version_ = 0;
  sstable_meta_.basic_meta_.create_snapshot_version_ = 0;
  sstable_meta_.basic_meta_.progressive_merge_round_ = 0;
  sstable_meta_.basic_meta_.progressive_merge_step_ = 0;
  sstable_meta_.basic_meta_.upper_trans_version_ = 0;
  sstable_meta_.basic_meta_.max_merged_trans_version_ = 0;
  sstable_meta_.basic_meta_.ddl_scn_.set_min();
  sstable_meta_.basic_meta_.filled_tx_scn_.set_min();
  sstable_meta_.basic_meta_.rec_scn_.set_min();
  sstable_meta_.basic_meta_.contain_uncommitted_row_ = 0;
  sstable_meta_.data_root_info_.addr_ = addr;
  sstable_meta_.basic_meta_.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  sstable_meta_.basic_meta_.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  sstable_meta_.basic_meta_.data_index_tree_height_ = 0;
  sstable_meta_.macro_info_.macro_meta_info_.addr_ = addr;
  ASSERT_TRUE(sstable_meta_.check_meta());
  sstable_meta_.is_inited_ = true;
  ASSERT_TRUE(sstable_meta_.is_valid());
}

void TestLSMigrationParam::TearDown()
{
  int ret = OB_SUCCESS;
  sstable_meta_.reset();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  ObTabletMapKey key;
  key.ls_id_ = TEST_LS_ID;

  key.tablet_id_ = 1002;
  ret = t3m->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  key.tablet_id_ = 1003;
  ret = t3m->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSMigrationParam, test_migrate_sstable_param)
{
  int ret = OB_SUCCESS;

  ObMigrationSSTableParam sstable_param;
  ObITable::TableKey key;
  key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  key.tablet_id_ = 20221106;
  key.version_range_.base_version_ = 1;
  key.version_range_.snapshot_version_ = 11;

  sstable_param.basic_meta_.row_count_                     = sstable_meta_.basic_meta_.row_count_;
  sstable_param.basic_meta_.occupy_size_                   = sstable_meta_.basic_meta_.occupy_size_;
  sstable_param.basic_meta_.original_size_                 = sstable_meta_.basic_meta_.original_size_;
  sstable_param.basic_meta_.data_checksum_                 = sstable_meta_.basic_meta_.data_checksum_;
  sstable_param.basic_meta_.index_type_                    = sstable_meta_.basic_meta_.index_type_;
  sstable_param.basic_meta_.rowkey_column_count_           = sstable_meta_.basic_meta_.rowkey_column_count_;
  sstable_param.basic_meta_.column_cnt_                    = sstable_meta_.basic_meta_.column_cnt_;
  sstable_param.basic_meta_.data_macro_block_count_        = sstable_meta_.basic_meta_.data_macro_block_count_;
  sstable_param.basic_meta_.data_micro_block_count_        = sstable_meta_.basic_meta_.data_micro_block_count_;
  sstable_param.basic_meta_.use_old_macro_block_count_     = sstable_meta_.basic_meta_.use_old_macro_block_count_;
  sstable_param.basic_meta_.index_macro_block_count_       = sstable_meta_.basic_meta_.index_macro_block_count_;
  sstable_param.basic_meta_.sstable_format_version_        = sstable_meta_.basic_meta_.sstable_format_version_;
  sstable_param.basic_meta_.schema_version_                = sstable_meta_.basic_meta_.schema_version_;
  sstable_param.basic_meta_.create_snapshot_version_       = sstable_meta_.basic_meta_.create_snapshot_version_;
  sstable_param.basic_meta_.progressive_merge_round_       = sstable_meta_.basic_meta_.progressive_merge_round_;
  sstable_param.basic_meta_.progressive_merge_step_        = sstable_meta_.basic_meta_.progressive_merge_step_;
  sstable_param.basic_meta_.upper_trans_version_           = sstable_meta_.basic_meta_.upper_trans_version_;
  sstable_param.basic_meta_.ddl_scn_                       = sstable_meta_.basic_meta_.ddl_scn_;
  sstable_param.basic_meta_.filled_tx_scn_                 = sstable_meta_.basic_meta_.filled_tx_scn_;
  sstable_param.basic_meta_.rec_scn_                       = sstable_meta_.basic_meta_.rec_scn_;
  sstable_param.basic_meta_.max_merged_trans_version_      = sstable_meta_.basic_meta_.max_merged_trans_version_;
  sstable_param.basic_meta_.table_mode_                    = sstable_meta_.basic_meta_.table_mode_;
  sstable_param.basic_meta_.contain_uncommitted_row_       = sstable_meta_.basic_meta_.contain_uncommitted_row_;
  sstable_param.basic_meta_.root_row_store_type_           = sstable_meta_.basic_meta_.root_row_store_type_;
  sstable_param.basic_meta_.latest_row_store_type_         = sstable_meta_.basic_meta_.latest_row_store_type_;
  sstable_param.table_key_                     = key;
  sstable_param.basic_meta_.compressor_type_               = sstable_meta_.basic_meta_.compressor_type_;
  sstable_param.basic_meta_.encrypt_id_                    = sstable_meta_.basic_meta_.encrypt_id_;
  sstable_param.basic_meta_.master_key_id_                 = sstable_meta_.basic_meta_.master_key_id_;
  sstable_param.basic_meta_.rec_scn_                       = sstable_meta_.basic_meta_.rec_scn_;
  MEMCPY(sstable_param.basic_meta_.encrypt_key_, sstable_meta_.basic_meta_.encrypt_key_,
      share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  for (int64_t i = 0; i < sstable_meta_.get_col_checksum_cnt(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sstable_param.column_checksums_.push_back(sstable_meta_.get_col_checksum()[i]));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(sstable_meta_.is_valid());
  ASSERT_TRUE(sstable_param.is_valid());
}

TEST_F(TestLSMigrationParam, test_placeholder_storage_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObStorageSchema storage_schema;
  compaction::ObMediumCompactionInfoList medium_info_list;
  ObTabletFullMemoryMdsData full_memory_mds_data;

  ret = ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(allocator, storage_schema, medium_info_list, full_memory_mds_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  void *ptr = nullptr;
  ObTablet placeholder_tablet;
  ret = ObStorageSchemaUtil::alloc_storage_schema(allocator, placeholder_tablet.storage_schema_addr_.ptr_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = placeholder_tablet.storage_schema_addr_.get_ptr()->init(allocator, storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);

  placeholder_tablet.tablet_meta_.compat_mode_ = lib::Worker::get_compatibility_mode();
  ASSERT_NE(nullptr, ptr = allocator.alloc(sizeof(ObRowkeyReadInfo)));
  placeholder_tablet.rowkey_read_info_ = new (ptr) ObRowkeyReadInfo();
  placeholder_tablet.build_read_info(allocator, nullptr /*tablet*/, false /*is_cs_replica_compat*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSMigrationParam, test_migrate_tablet_param)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::STORAGE_MOD, ObLSAccessAttr::DISABLE_LOGONLY);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletMapKey src_key;
  src_key.ls_id_ = TEST_LS_ID;
  src_key.tablet_id_ = 1002;

  ObTabletHandle src_handle;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, src_key, allocator_, ls_handle, src_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;

  ret = create_tablet_schema.init(schema_allocator, table_schema, lib::Worker::CompatMode::MYSQL,
        false/*skip_column_info*/, MOCK_DATA_VERSION_4_3_5_6);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletID empty_tablet_id;
  SCN scn;
  scn.convert_from_ts(ObTimeUtility::current_time());
  ret = src_handle.get_obj()->init_for_first_time_creation(allocator_, src_key.ls_id_, src_key.tablet_id_, src_key.tablet_id_,
      scn, 2022, create_tablet_schema, true/*need_create_empty_major_sstable*/, SCN::invalid_scn()/*clog_checkpoint_scn*/,
      SCN::invalid_scn()/*mds_checkpoint_scn*/, false/*is_split_dest_tablet*/, ObTabletID()/*split_src_tablet_id*/,
      true/*micro_index_clustered*/, false/*need_generate_cs_replica_cg_array*/, false/*has_cs_replica*/, ls_handle.get_ls()->get_freezer());
  ASSERT_EQ(common::OB_SUCCESS, ret);
  src_handle.get_obj()->tablet_meta_.inc_major_snapshot_ = 100;

  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 50);
  // write data to mds table no.1 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = src_handle.get_obj()->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(create_commit_scn, create_commit_scn);
  }

  ObMigrationTabletParam tablet_param;
  ret = src_handle.get_obj()->build_migration_tablet_param(tablet_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet_param.is_valid());
  ASSERT_TRUE(tablet_param.micro_index_clustered_);
  ASSERT_EQ(100, tablet_param.inc_major_snapshot_);

  ObTabletMapKey dst_key;
  dst_key.ls_id_ = TEST_LS_ID;
  dst_key.tablet_id_ = 1003;

  ObTabletHandle dst_handle;
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, dst_key, allocator_, ls_handle, dst_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  const bool is_transfer = false;
  ret = dst_handle.get_obj()->init_with_migrate_param(allocator_, tablet_param, false, ls_handle.get_ls()->get_freezer(), is_transfer);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObTabletMeta &src_meta = src_handle.get_obj()->get_tablet_meta();
  const ObTabletMeta &dst_meta = dst_handle.get_obj()->get_tablet_meta();
  LOG_INFO("dump meta", K(src_meta));
  LOG_INFO("dump meta", K(dst_meta));
  ASSERT_TRUE(src_meta.is_valid());
  ASSERT_TRUE(dst_meta.is_valid());
  ASSERT_EQ(true, src_meta.micro_index_clustered_);
  ASSERT_EQ(true, dst_meta.micro_index_clustered_);
  ASSERT_EQ(100, dst_meta.inc_major_snapshot_);

  // check create_schema_version_ in tablet meta/migrate param
  ASSERT_TRUE(table_schema.get_schema_version() == src_meta.create_schema_version_);
  ASSERT_TRUE(table_schema.get_schema_version() == tablet_param.create_schema_version_);
}

TEST_F(TestLSMigrationParam, test_migration_param_compat)
{
  const int64_t buf_len = 4096;
  char buf[buf_len] = {};

  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::STORAGE_MOD, ObLSAccessAttr::DISABLE_LOGONLY);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletMapKey src_key;
  src_key.ls_id_ = TEST_LS_ID;
  src_key.tablet_id_ = 1002;

  ObTabletHandle src_handle;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, src_key, allocator_, ls_handle, src_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;

  ret = create_tablet_schema.init(schema_allocator, table_schema, lib::Worker::CompatMode::MYSQL,
        false/*skip_column_info*/, MOCK_DATA_VERSION_4_3_5_6);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletID empty_tablet_id;
  SCN scn;
  scn.convert_from_ts(ObTimeUtility::current_time());
  ret = src_handle.get_obj()->init_for_first_time_creation(allocator_, src_key.ls_id_, src_key.tablet_id_, src_key.tablet_id_,
      scn, 2022, create_tablet_schema, true/*need_create_empty_major_sstable*/, SCN::invalid_scn()/*clog_checkpoint_scn*/,
      SCN::invalid_scn()/*mds_checkpoint_scn*/, false/*is_split_dest_tablet*/, ObTabletID()/*split_src_tablet_id*/,
      true/*micro_index_clustered*/, false/*need_generate_cs_replica_cg_array*/, false/*has_cs_replica*/, ls_handle.get_ls()->get_freezer());
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 50);
  // write data to mds table no.1 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = src_handle.get_obj()->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(create_commit_scn, create_commit_scn);
  }

  ObMigrationTabletParam tablet_param;
  ret = src_handle.get_obj()->build_migration_tablet_param(tablet_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet_param.is_valid());

  // check create_schema_version_ in tablet meta/migrate param
  ASSERT_TRUE(table_schema.get_schema_version() == src_handle.get_obj()->get_tablet_meta().create_schema_version_);
  ASSERT_TRUE(table_schema.get_schema_version() == tablet_param.create_schema_version_);

  tablet_param.ddl_commit_scn_.convert_for_tx(20230403);
  int64_t pos = 0;
  ret = tablet_param.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "cooper", K(tablet_param));

  // normal deserialize
  ObMigrationTabletParam des_param;
  pos = 0;
  ret = des_param.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(des_param.is_valid());
  ASSERT_EQ(true, des_param.micro_index_clustered_);
  OB_LOG(INFO, "cooper", K(des_param));
}

TEST_F(TestLSMigrationParam, test_deleted_tablet_info)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  char buf[buf_len] = {};
  share::ObLSID ls_id(1);
  ObTabletID tablet_id(200001);
  set_compat_mode(Worker::CompatMode::MYSQL);
  ObMigrationTabletParam param;
  ret = param.build_deleted_tablet_info(ls_id, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(param.is_valid());

  int64_t pos = 0;
  ret = param.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  pos = 0;
  ObMigrationTabletParam des_param;
  ret = des_param.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
}

namespace
{

// Single-process production-component tests, not an observer/cluster restart.
// Only one real L/PALF exists per case. The other addresses are membership
// configuration, not simulated servers. No migration DAG is scheduled here.
class LogonlyRebuildTestLS
{
public:
  explicit LogonlyRebuildTestLS(const int64_t id)
    : ls_id_(id), created_(false), handle_()
  {}

  ~LogonlyRebuildTestLS()
  {
    handle_.reset();
    if (created_) {
      EXPECT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_));
    }
    EXPECT_EQ(OB_SUCCESS, MTL(ObRebuildService*)->remove_rebuild_ls(ls_id_));
  }

  int create(const bool self_is_member = true)
  {
    int ret = OB_SUCCESS;
    obrpc::ObCreateLSArg arg;
    ObMemberList members;
    GlobalLearnerList learners;
    const ObAddr &self = MockTenantModuleEnv::get_instance().self_addr_;
    const ObAddr peer1(ObAddr::IPV4, "127.0.0.2", self.get_port());
    const ObAddr peer2(ObAddr::IPV4, "127.0.0.3", self.get_port());
    const ObAddr peer3(ObAddr::IPV4, "127.0.0.4", self.get_port());
    const int64_t member_timestamp = ObTimeUtility::current_time();
    ObMember logonly_member(self_is_member ? self : peer3, member_timestamp);
    logonly_member.set_logonly();
    if (OB_FAIL(gen_create_ls_arg(MTL_ID(), ls_id_, arg))) {
    } else if (FALSE_IT(arg.replica_type_ = REPLICA_TYPE_LOGONLY)) {
    } else if (OB_FAIL(create_ls_with_gc_retry_(arg))) {
    } else if (FALSE_IT(created_ = true)) {
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(
        ls_id_, handle_, ObLSGetMod::HA_MOD, ObLSAccessAttr::ALLOW_LOGONLY))) {
    } else if (OB_FAIL(members.add_member(logonly_member))) {
    } else if (OB_FAIL(members.add_member(ObMember(peer1, member_timestamp)))) {
    } else if (OB_FAIL(members.add_member(ObMember(peer2, member_timestamp)))) {
    } else if (OB_FAIL(ls()->set_initial_member_list(members, 3, learners))) {
    }
    return ret;
  }

  int set_rebuild_info(const ObLSRebuildStatus::STATUS status)
  {
    ObLSRebuildInfo info;
    info.status_ = status;
    info.type_ = ObLSRebuildType::CLOG;
    return ls()->set_rebuild_info(info);
  }

  int get_ctx(ObLSRebuildCtx &ctx, int64_t &count)
  {
    int ret = OB_SUCCESS;
    ObArray<ObLSRebuildCtx> contexts;
    count = 0;
    if (OB_FAIL(MTL(ObRebuildService*)->get_ls_rebuild_ctx_array_(contexts))) {
    } else {
      for (int64_t i = 0; i < contexts.count(); ++i) {
        if (contexts.at(i).ls_id_ == ls_id_) {
          ctx = contexts.at(i);
          ++count;
        }
      }
    }
    return ret;
  }

  void expect_vote(const bool allow_vote)
  {
    palf::PalfStat stat;
    ASSERT_EQ(OB_SUCCESS, ls()->get_log_handler()->stat(stat));
    EXPECT_TRUE(ls()->is_logonly_replica());
    EXPECT_EQ(palf::LogReplicaType::LOGONLY_REPLICA, stat.replica_type_);
    EXPECT_EQ(3, stat.paxos_member_list_.get_member_number());
    EXPECT_EQ(1, stat.paxos_member_list_.get_logonly_replica_member_number());
    EXPECT_EQ(allow_vote, stat.allow_vote_);
  }

  void expect_rebuild(const ObLSRebuildStatus::STATUS status, const bool has_ctx)
  {
    ObLSRebuildInfo info;
    bool need_rebuild = !has_ctx;
    ASSERT_EQ(OB_SUCCESS, ls()->get_rebuild_info(info));
    EXPECT_EQ(status, info.status_.get_status());
    EXPECT_EQ(ObLSRebuildStatus::NONE == status ? ObLSRebuildType::NONE : ObLSRebuildType::CLOG,
              info.type_.get_type());
    ASSERT_EQ(OB_SUCCESS, MTL(ObRebuildService*)->check_ls_need_rebuild(ls_id_, need_rebuild));
    EXPECT_EQ(has_ctx, need_rebuild);
  }

  ObLS *ls() { return handle_.get_ls(); }
  const ObLSID &id() const { return ls_id_; }

private:
  int create_ls_with_gc_retry_(const obrpc::ObCreateLSArg &arg)
  {
    // remove_ls() queues asynchronous safe destruction. Pending LSs still
    // count against the tenant's clog-disk LS limit until the real GC runs
    // (normally every 10 seconds). Allow at most 30 seconds of retry delay,
    // without bypassing the resource check or masking any other create error.
    int ret = MTL(ObLSService*)->create_ls(arg);
    for (int64_t retry = 0; OB_TOO_MANY_TENANT_LS == ret && retry < 150; ++retry) {
      ob_usleep(200 * 1000);
      ret = MTL(ObLSService*)->create_ls(arg);
    }
    return ret;
  }

  ObLSID ls_id_;
  bool created_;
  ObLSHandle handle_;
  DISALLOW_COPY_AND_ASSIGN(LogonlyRebuildTestLS);
};

} // namespace

TEST_F(TestLSMigrationParam, logonly_rebuild_recovers_init_after_disable_vote)
{
  LogonlyRebuildTestLS replica(201);
  ASSERT_EQ(OB_SUCCESS, replica.create());
  ObRebuildService *service = MTL(ObRebuildService*);
  ASSERT_EQ(OB_SUCCESS, replica.set_rebuild_info(ObLSRebuildStatus::INIT));
  // Start at the crash boundary after PALF persisted disable_vote but before
  // INIT -> DOING. Do not fake a lost-log notification or PALF voting state.
  ASSERT_EQ(OB_SUCCESS, replica.ls()->disable_vote(false));
  replica.expect_vote(false);
  replica.expect_rebuild(ObLSRebuildStatus::INIT, false);

  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  ObLSRebuildCtx ctx;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, replica.get_ctx(ctx, count));
  ASSERT_EQ(1, count);
  ASSERT_TRUE(ctx.is_valid());
  EXPECT_EQ(ObLSRebuildType::CLOG, ctx.type_.get_type());
  ObLSRebuildMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(ctx, MTL(ObLSService*)));
  // Production CLOG INIT calls disable_vote(true), which must be reentrant
  // when voting was already disabled before the volatile ctx was recovered.
  ASSERT_EQ(OB_SUCCESS, mgr.process());
  replica.expect_rebuild(ObLSRebuildStatus::DOING, true);
  replica.expect_vote(false);
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  replica.expect_rebuild(ObLSRebuildStatus::DOING, true);
}

TEST_F(TestLSMigrationParam, logonly_rebuild_rescans_rebuild_and_wait_metadata)
{
  const ObMigrationStatus stages[] = {
    ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD,
    ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_WAIT
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(stages); ++i) {
    SCOPED_TRACE(i);
    LogonlyRebuildTestLS replica(202 + i);
    ASSERT_EQ(OB_SUCCESS, replica.create());
    ObRebuildService *service = MTL(ObRebuildService*);
    ASSERT_EQ(OB_SUCCESS, replica.set_rebuild_info(ObLSRebuildStatus::DOING));
    ASSERT_EQ(OB_SUCCESS, replica.ls()->disable_vote(false));
    ASSERT_EQ(OB_SUCCESS, replica.ls()->set_ls_rebuild());
    ASSERT_EQ(OB_SUCCESS, replica.ls()->set_migration_status(stages[i], replica.ls()->get_rebuild_seq()));
    ASSERT_EQ(OB_SUCCESS, service->add_rebuild_ls(replica.id(), ObLSRebuildType(ObLSRebuildType::CLOG)));
    ASSERT_EQ(OB_SUCCESS, service->remove_rebuild_ls(replica.id()));
    replica.expect_rebuild(ObLSRebuildStatus::DOING, false);

    // Re-scan persisted LS metadata after losing only the volatile ctx. This
    // does not claim to execute BUILD DAGs, slog replay or observer restart.
    ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
    ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
    ObLSRebuildCtx ctx;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, replica.get_ctx(ctx, count));
    ASSERT_EQ(1, count);
    ASSERT_TRUE(ctx.is_valid());
    EXPECT_EQ(ObLSRebuildType::CLOG, ctx.type_.get_type());
    ObLSRebuildMgr mgr;
    ASSERT_EQ(OB_SUCCESS, mgr.init(ctx, MTL(ObLSService*)));
    ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
    replica.expect_rebuild(ObLSRebuildStatus::DOING, true);
    replica.expect_vote(false);
    ObMigrationStatus migration_status;
    ASSERT_EQ(OB_SUCCESS, replica.ls()->get_migration_status(migration_status));
    EXPECT_EQ(stages[i], migration_status);
  }
}

TEST_F(TestLSMigrationParam, logonly_rebuild_retryable_failure_and_terminal_cleanup)
{
  LogonlyRebuildTestLS replica(204);
  ASSERT_EQ(OB_SUCCESS, replica.create());
  ObRebuildService *service = MTL(ObRebuildService*);
  ASSERT_EQ(OB_SUCCESS, replica.set_rebuild_info(ObLSRebuildStatus::DOING));
  ASSERT_EQ(OB_SUCCESS, replica.ls()->disable_vote(false));
  ASSERT_EQ(OB_SUCCESS, replica.ls()->set_ls_rebuild());
  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, service->finish_rebuild_ls(replica.id(), OB_EAGAIN));
    ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
    replica.expect_rebuild(ObLSRebuildStatus::DOING, true);
    replica.expect_vote(false);
    ObLSRebuildCtx ctx;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, replica.get_ctx(ctx, count));
    ASSERT_EQ(1, count);
    EXPECT_EQ(OB_EAGAIN, ctx.result_);
  }

  // Exercise the real terminal-result handoff. Establishing REBUILD_FAIL via
  // remote membership/deletion checks is outside this single-process test.
  ASSERT_EQ(OB_SUCCESS, replica.ls()->set_migration_status(
      ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL, replica.ls()->get_rebuild_seq()));
  ASSERT_EQ(OB_SUCCESS, service->finish_rebuild_ls(replica.id(), OB_EAGAIN));
  replica.expect_rebuild(ObLSRebuildStatus::CLEANUP, true);
  ASSERT_EQ(OB_SUCCESS, service->scheduler_rebuild_mgr_());
  replica.expect_rebuild(ObLSRebuildStatus::NONE, true);
  replica.expect_vote(true);
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  replica.expect_rebuild(ObLSRebuildStatus::NONE, false);
}

TEST_F(TestLSMigrationParam, logonly_rebuild_enable_vote_failure_keeps_cleanup)
{
  LogonlyRebuildTestLS replica(205);
  ASSERT_EQ(OB_SUCCESS, replica.create());
  ObRebuildService *service = MTL(ObRebuildService*);
  ASSERT_EQ(OB_SUCCESS, replica.ls()->disable_vote(false));
  ASSERT_EQ(OB_SUCCESS, replica.set_rebuild_info(ObLSRebuildStatus::CLEANUP));
  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  replica.expect_vote(false);
  ObLSRebuildCtx ctx;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, replica.get_ctx(ctx, count));
  ASSERT_EQ(1, count);
  ObLSRebuildMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(ctx, MTL(ObLSService*)));

  // A real stopped log handler makes enable_vote return OB_NOT_RUNNING.
  // Do not flip is_in_stop_state_ back or overwrite the production function.
  ASSERT_EQ(OB_SUCCESS, replica.ls()->get_log_handler()->stop());
  for (int64_t i = 0; i < 2; ++i) {
    EXPECT_EQ(OB_NOT_RUNNING, mgr.process());
    ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
    replica.expect_rebuild(ObLSRebuildStatus::CLEANUP, true);
  }
  // Inspect PALF independently of the stopped log handler: failed cleanup
  // must not have enabled voting while leaving the metadata at CLEANUP.
  palf::PalfHandleGuard palf_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(logservice::ObLogService*)->open_palf(replica.id(), palf_handle));
  ASSERT_TRUE(palf_handle.is_valid());
  palf::PalfStat stat;
  ASSERT_EQ(OB_SUCCESS, palf_handle.get_palf_handle()->stat(stat));
  EXPECT_EQ(palf::LogReplicaType::LOGONLY_REPLICA, stat.replica_type_);
  EXPECT_FALSE(stat.allow_vote_);
}

TEST_F(TestLSMigrationParam, logonly_rebuild_enable_vote_success_releases_context)
{
  LogonlyRebuildTestLS replica(206);
  ASSERT_EQ(OB_SUCCESS, replica.create());
  ObRebuildService *service = MTL(ObRebuildService*);
  ASSERT_EQ(OB_SUCCESS, replica.ls()->disable_vote(false));
  ASSERT_EQ(OB_SUCCESS, replica.set_rebuild_info(ObLSRebuildStatus::DOING));
  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  ASSERT_EQ(OB_SUCCESS, service->finish_rebuild_ls(replica.id(), OB_SUCCESS));
  replica.expect_rebuild(ObLSRebuildStatus::CLEANUP, true);
  replica.expect_vote(false);

  // Recover CLEANUP from metadata too; the task's in-memory result is not what
  // authorizes enabling voting or releasing the context.
  ASSERT_EQ(OB_SUCCESS, service->remove_rebuild_ls(replica.id()));
  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  replica.expect_rebuild(ObLSRebuildStatus::CLEANUP, true);
  ASSERT_EQ(OB_SUCCESS, service->scheduler_rebuild_mgr_());
  replica.expect_vote(true);
  replica.expect_rebuild(ObLSRebuildStatus::NONE, true);
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  replica.expect_rebuild(ObLSRebuildStatus::NONE, false);
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  replica.expect_rebuild(ObLSRebuildStatus::NONE, false);
}

TEST_F(TestLSMigrationParam, logonly_rebuild_deleted_ls_drops_stale_context)
{
  LogonlyRebuildTestLS replica(207);
  ASSERT_EQ(OB_SUCCESS, replica.create());
  ObRebuildService *service = MTL(ObRebuildService*);
  ASSERT_EQ(OB_SUCCESS, replica.set_rebuild_info(ObLSRebuildStatus::DOING));
  ASSERT_EQ(OB_SUCCESS, service->build_rebuild_ctx_map_());
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(replica.id()));
  ObLSHandle missing;
  EXPECT_EQ(OB_LS_NOT_EXIST, MTL(ObLSService*)->get_ls(
      replica.id(), missing, ObLSGetMod::HA_MOD, ObLSAccessAttr::ALLOW_LOGONLY));
  bool need_rebuild = true;
  ASSERT_EQ(OB_SUCCESS, service->check_ls_need_rebuild(replica.id(), need_rebuild));
  EXPECT_FALSE(need_rebuild); // ObLS::stop removes its existing context.

  // A late callback can enqueue an LS which is no longer in the service map.
  ASSERT_EQ(OB_SUCCESS, service->add_rebuild_ls(replica.id(), ObLSRebuildType(ObLSRebuildType::CLOG)));
  ASSERT_EQ(OB_SUCCESS, service->check_ls_need_rebuild(replica.id(), need_rebuild));
  ASSERT_TRUE(need_rebuild);
  ASSERT_EQ(OB_SUCCESS, service->build_ls_rebuild_info_());
  ASSERT_EQ(OB_SUCCESS, service->scheduler_rebuild_mgr_());
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  ASSERT_EQ(OB_SUCCESS, service->check_ls_need_rebuild(replica.id(), need_rebuild));
  EXPECT_FALSE(need_rebuild);
}

TEST_F(TestLSMigrationParam, logonly_rebuild_non_member_rejects_pending_context)
{
  LogonlyRebuildTestLS replica(208);
  // Model the local configuration after removal, not an RPC membership change.
  ASSERT_EQ(OB_SUCCESS, replica.create(false));
  palf::PalfStat stat;
  ASSERT_EQ(OB_SUCCESS, replica.ls()->get_log_handler()->stat(stat));
  const ObAddr &self = MockTenantModuleEnv::get_instance().self_addr_;
  EXPECT_FALSE(stat.paxos_member_list_.contains(self));
  EXPECT_FALSE(stat.learner_list_.contains(self));
  ObRebuildService *service = MTL(ObRebuildService*);
  ASSERT_EQ(OB_SUCCESS, service->add_rebuild_ls(replica.id(), ObLSRebuildType(ObLSRebuildType::CLOG)));
  ObLSRebuildCtx ctx;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, replica.get_ctx(ctx, count));
  ASSERT_EQ(1, count);
  bool can_rebuild = true;
  ASSERT_EQ(OB_SUCCESS, service->check_can_rebuild_(ctx, replica.ls(), can_rebuild));
  EXPECT_FALSE(can_rebuild);
  ASSERT_EQ(OB_SUCCESS, service->build_ls_rebuild_info_());
  replica.expect_rebuild(ObLSRebuildStatus::NONE, true);
  replica.expect_vote(true);
  ASSERT_EQ(OB_SUCCESS, service->check_rebuild_ctx_map_());
  replica.expect_rebuild(ObLSRebuildStatus::NONE, false);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  char log_path[128];
  snprintf(log_path, sizeof(log_path), "test_ls_migration_param_%d.log", static_cast<int>(getpid()));
  OB_LOGGER.set_file_name(log_path, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
