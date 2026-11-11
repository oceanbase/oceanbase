/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <vector>
#include "lib/utility/ob_defer.h"

#define private public
#define protected public

#include "storage/high_availability/ob_transfer_struct.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_binding_info.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"
#include "src/storage/compaction/ob_medium_list_checker.h"
#include "src/storage/multi_data_source/ob_abort_transfer_in_mds_ctx.h"
#include "src/storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h"
#include "src/storage/multi_data_source/ob_start_transfer_in_mds_ctx.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;
using namespace oceanbase::compaction;
using namespace oceanbase::storage::mds;

#define USING_LOG_PREFIX STORAGE


namespace oceanbase
{
namespace unittest
{
class TestTransferMdsCtx : public ::testing::Test
{
public:
  TestTransferMdsCtx() : tenant_base_(OB_SYS_TENANT_ID) {}
  virtual ~TestTransferMdsCtx() = default;
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
    ObTenantEnv::set_tenant(&tenant_base_);
  }
  void TearDown() override { ObTenantEnv::set_tenant(nullptr); }
  ObTenantBase tenant_base_;
};

TEST_F(TestTransferMdsCtx, start_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObStartTransferInMdsCtx start_ctx;
  start_ctx.version_ = ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = start_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStartTransferInMdsCtx de_start_ctx1;
  pos = 0;
  ret = de_start_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1, de_start_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStartTransferInMdsCtx de_start_ctx2;
  pos = 0;
  ret = de_start_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1, de_start_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObStartTransferInMdsCtx start_ctx2;
  ret = start_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStartTransferInMdsCtx de_start_ctx3;
  pos = 0;
  ret = de_start_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3, de_start_ctx3.version_);
}


TEST_F(TestTransferMdsCtx, finish_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObFinishTransferInMdsCtx finish_ctx;
  finish_ctx.version_ = ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = finish_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObFinishTransferInMdsCtx de_finish_ctx1;
  pos = 0;
  ret = de_finish_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1, de_finish_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFinishTransferInMdsCtx de_finish_ctx2;
  pos = 0;
  ret = de_finish_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1, de_finish_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObFinishTransferInMdsCtx finish_ctx2;
  ret = finish_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFinishTransferInMdsCtx de_finish_ctx3;
  pos = 0;
  ret = de_finish_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V2, de_finish_ctx3.version_);
}

TEST_F(TestTransferMdsCtx, abort_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObAbortTransferInMdsCtx abort_ctx;
  abort_ctx.version_ = ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = abort_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObAbortTransferInMdsCtx de_abort_ctx1;
  pos = 0;
  ret = de_abort_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1, de_abort_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAbortTransferInMdsCtx de_abort_ctx2;
  pos = 0;
  ret = de_abort_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1, de_abort_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObAbortTransferInMdsCtx abort_ctx2;
  ret = abort_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAbortTransferInMdsCtx de_abort_ctx3;
  pos = 0;
  ret = de_abort_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V2, de_abort_ctx3.version_);
}

// Reference the original macro-generated wire layout independently of the borrowed-array encoder.
struct LegacyStartTransferInInfo
{
  OB_UNIS_VERSION(1);
public:
  ObLSID src_ls_id_;
  ObLSID dest_ls_id_;
  SCN start_scn_;
  ObSArray<storage::ObMigrationTabletParam> tablet_meta_list_;
  ObTransferTaskID task_id_;
  uint64_t data_version_;
};
OB_SERIALIZE_MEMBER(LegacyStartTransferInInfo, src_ls_id_, dest_ls_id_, start_scn_, tablet_meta_list_, task_id_, data_version_);

static void check_transfer_wire(const storage::ObTXStartTransferInInfo &info)
{
  LegacyStartTransferInInfo legacy;
  legacy.src_ls_id_ = info.src_ls_id_;
  legacy.dest_ls_id_ = info.dest_ls_id_;
  legacy.start_scn_ = info.start_scn_;
  legacy.task_id_ = info.task_id_;
  legacy.data_version_ = info.data_version_;
  ASSERT_EQ(OB_SUCCESS, legacy.tablet_meta_list_.assign(info.get_tablet_meta_list()));
  // Size queries must be idempotent.
  ASSERT_EQ(legacy.get_serialize_size(), info.get_serialize_size());
  ASSERT_EQ(legacy.get_serialize_size(), info.get_serialize_size());
  std::vector<char> expected(legacy.get_serialize_size());
  std::vector<char> actual(info.get_serialize_size());
  int64_t expected_pos = 0;
  int64_t actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(expected.data(), expected.size(), expected_pos));
  ASSERT_EQ(OB_SUCCESS, info.serialize(actual.data(), actual.size(), actual_pos));
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_EQ(0, MEMCMP(expected.data(), actual.data(), actual_pos));
  ObSEArray<const storage::ObMigrationTabletParam *, 16> borrowed_metas;
  for (int64_t i = 0; i < info.get_tablet_meta_list().count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, borrowed_metas.push_back(&info.get_tablet_meta_list().at(i)));
  }
  // Match the sender: the header object owns no tablet params; all metadata comes from the argument.
  storage::ObTXStartTransferInInfo header;
  header.src_ls_id_ = info.src_ls_id_;
  header.dest_ls_id_ = info.dest_ls_id_;
  header.start_scn_ = info.start_scn_;
  header.task_id_ = info.task_id_;
  header.data_version_ = info.data_version_;
  ASSERT_EQ(header.get_serialize_size() - serialization::encoded_length_vi64(0), header.get_header_serialize_size());
  ASSERT_EQ(header.get_header_serialize_size(), info.get_header_serialize_size());
  int64_t borrowed_size = header.get_header_serialize_size() + serialization::encoded_length_vi64(borrowed_metas.count());
  for (int64_t i = 0; i < borrowed_metas.count(); ++i) {
    borrowed_size += borrowed_metas.at(i)->get_serialize_size();
  }
  ASSERT_EQ(expected_pos, borrowed_size);
  actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize_with_tablet_meta(actual.data(), actual.size(), actual_pos, borrowed_metas));
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_EQ(0, MEMCMP(expected.data(), actual.data(), actual_pos));
  ASSERT_TRUE(header.get_tablet_meta_list().empty());
  storage::ObTXStartTransferInInfo decoded;
  actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(actual.data(), actual.size(), actual_pos));
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_EQ(info.get_tablet_meta_list().count(), decoded.get_tablet_meta_list().count());
  ASSERT_EQ(info.src_ls_id_, decoded.src_ls_id_);
  ASSERT_EQ(info.dest_ls_id_, decoded.dest_ls_id_);
  ASSERT_EQ(info.task_id_, decoded.task_id_);
  ASSERT_EQ(info.data_version_, decoded.data_version_);
  for (int64_t i = 0; i < decoded.get_tablet_meta_list().count(); ++i) {
    ASSERT_TRUE(decoded.get_tablet_meta_list().at(i).is_valid());
    ASSERT_EQ(info.get_tablet_meta_list().at(i).tablet_id_, decoded.get_tablet_meta_list().at(i).tablet_id_);
  }
  // Schema decoding normalizes an unspecified original_merge_engine_type_. Compare both decoded
  // paths, while the earlier comparison still requires identical bytes before decoding.
  LegacyStartTransferInInfo legacy_decoded;
  int64_t legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy_decoded.deserialize(expected.data(), expected.size(), legacy_pos));
  ASSERT_EQ(expected_pos, legacy_pos);
  expected.resize(legacy_decoded.get_serialize_size());
  actual.resize(decoded.get_serialize_size());
  ASSERT_EQ(expected.size(), actual.size());
  legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy_decoded.serialize(expected.data(), expected.size(), legacy_pos));
  actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.serialize(actual.data(), actual.size(), actual_pos));
  ASSERT_EQ(legacy_pos, actual_pos);
  ASSERT_EQ(actual.size(), actual_pos);
  ASSERT_EQ(0, MEMCMP(expected.data(), actual.data(), actual_pos));
}

static void check_migration_short_buffer(const storage::ObMigrationTabletParam &param)
{
  const int64_t offset = 7;
  const int64_t size = param.get_serialize_size();
  std::vector<char> buffer(offset + size, '\x5a');
  const std::vector<char> untouched = buffer;
  for (int64_t available = 0; available < size; ++available) {
    int64_t pos = offset;
    ASSERT_EQ(OB_ERR_UNEXPECTED, param.serialize(buffer.data(), offset + available, pos)) << available;
    ASSERT_EQ(offset, pos);
    ASSERT_EQ(untouched, buffer); // Capacity is checked before writing any field.
  }
  int64_t pos = offset;
  ASSERT_EQ(OB_SUCCESS, param.serialize(buffer.data(), buffer.size(), pos));
  ASSERT_EQ(buffer.size(), pos);
  pos = offset;
  ASSERT_EQ(OB_INVALID_ARGUMENT, param.serialize(nullptr, buffer.size(), pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, param.serialize(buffer.data(), 0, pos));
  ASSERT_EQ(offset, pos);
}

TEST_F(TestTransferMdsCtx, transfer_in_wire_compatibility)
{
  using namespace storage;
  ObMigrationTabletParam param;
  param.ls_id_ = ObLSID(1001);
  param.tablet_id_ = ObTabletID(200001);
  param.data_tablet_id_ = param.tablet_id_;
  param.create_scn_ = SCN::base_scn();
  param.start_scn_ = SCN::base_scn();
  param.clog_checkpoint_scn_ = SCN::base_scn();
  param.snapshot_version_ = 1;
  param.multi_version_start_ = 1;
  param.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  param.is_empty_shell_ = true;
  param.last_persisted_committed_tablet_status_.tablet_status_ = ObTabletStatus::NORMAL;
  param.last_persisted_committed_tablet_status_.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  ASSERT_EQ(OB_SUCCESS, param.ha_status_.init_status());
  ASSERT_EQ(OB_SUCCESS, param.transfer_info_.init());
  ASSERT_TRUE(param.is_valid());
  check_migration_short_buffer(param);

  ObTXStartTransferInInfo info;
  info.src_ls_id_ = ObLSID(1001);
  info.dest_ls_id_ = ObLSID(1002);
  info.start_scn_ = SCN::base_scn();
  info.task_id_ = ObTransferTaskID(1);
  ASSERT_EQ(OB_SUCCESS, info.reserve_tablet_meta(130));
  check_transfer_wire(info);
  for (int64_t i = 0; i < 129; ++i) {
    param.tablet_id_ = ObTabletID(200001 + i);
    ASSERT_EQ(OB_SUCCESS, info.add_tablet_meta(param));
    if (i == 0 || i >= 126) {
      check_transfer_wire(info); // Includes the 127 -> 128 array count prefix change.
    }
  }
  ObTabletFullMemoryMdsData full_memory_mds_data;
  ASSERT_EQ(OB_SUCCESS, ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(
      param.allocator_, param.storage_schema_, param.medium_info_list_, full_memory_mds_data));
  param.is_empty_shell_ = false;
  ASSERT_TRUE(param.is_valid());
  check_migration_short_buffer(param);
  ASSERT_EQ(OB_SUCCESS, info.add_tablet_meta(param));
  check_transfer_wire(info);
  info.task_id_ = ObTransferTaskID(16384);
  info.data_version_ = DATA_VERSION_4_3_2_0;
  check_transfer_wire(info); // Header changes preserve wire compatibility.

  ObTXStartTransferInInfo copied;
  ASSERT_EQ(OB_SUCCESS, copied.assign(info));
  check_transfer_wire(copied);
  std::vector<char> buffer(info.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, info.serialize(buffer.data(), buffer.size(), pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, copied.deserialize(buffer.data(), buffer.size(), pos));
  check_transfer_wire(copied); // Replacing a populated list must preserve its encoding.
  ASSERT_EQ(OB_SUCCESS, copied.add_tablet_meta(param));
  check_transfer_wire(copied);
  ObMigrationTabletParam invalid_param;
  ASSERT_NE(OB_SUCCESS, copied.add_tablet_meta(invalid_param));
  check_transfer_wire(copied); // Failed additions must preserve the existing wire payload.
  copied.reset();
  check_transfer_wire(copied);
  ASSERT_EQ(OB_SUCCESS, copied.add_tablet_meta(param));
  check_transfer_wire(copied);

  ObSEArray<const ObMigrationTabletParam *, 1> borrowed_metas;
  ASSERT_EQ(OB_SUCCESS, borrowed_metas.push_back(&param));
  pos = 0;
  ASSERT_NE(OB_SUCCESS, info.serialize_with_tablet_meta(buffer.data(), 1, pos, borrowed_metas));
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.serialize_with_tablet_meta(nullptr, buffer.size(), pos, borrowed_metas));
  borrowed_metas.at(0) = nullptr;
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.serialize_with_tablet_meta(buffer.data(), buffer.size(), pos, borrowed_metas));
}

TEST_F(TestTransferMdsCtx, migration_param_schema_handle_lifecycle)
{
  using namespace storage;
  ObMigrationTabletParam param;
  param.ls_id_ = ObLSID(1001);
  param.tablet_id_ = ObTabletID(200001);
  param.data_tablet_id_ = param.tablet_id_;
  param.create_scn_ = SCN::base_scn();
  param.start_scn_ = SCN::base_scn();
  param.clog_checkpoint_scn_ = SCN::base_scn();
  param.snapshot_version_ = 1;
  param.multi_version_start_ = 1;
  param.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  param.is_empty_shell_ = true;
  param.last_persisted_committed_tablet_status_.tablet_status_ = ObTabletStatus::NORMAL;
  param.last_persisted_committed_tablet_status_.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  ASSERT_EQ(OB_SUCCESS, param.ha_status_.init_status());
  ASSERT_EQ(OB_SUCCESS, param.transfer_info_.init());
  ASSERT_TRUE(param.is_valid());

  ASSERT_FALSE(param.storage_schema_.is_valid());
  param.is_empty_shell_ = false;
  ASSERT_FALSE(param.is_valid());
  ASSERT_EQ(OB_SUCCESS, param.ha_status_.set_restore_status(ObTabletRestoreStatus::PENDING));
  ASSERT_TRUE(param.is_valid()); // Restore pending does not require a schema.
  ASSERT_EQ(OB_SUCCESS, param.ha_status_.init_status());
  ASSERT_FALSE(param.is_valid());
  ASSERT_EQ(OB_SUCCESS, param.storage_schema_.init_placeholder(param.allocator_));
  ASSERT_EQ(OB_SUCCESS, param.medium_info_list_.init(param.allocator_));
  ASSERT_TRUE(param.is_valid());
  ObMigrationTabletParam copied;
  ASSERT_EQ(OB_SUCCESS, copied.assign(param));
  ASSERT_NE(&param.storage_schema_.get_schema(), &copied.storage_schema_.get_schema());
  param.reset();
  ASSERT_FALSE(param.storage_schema_.is_valid());
  ASSERT_TRUE(copied.is_valid());
  ASSERT_EQ(OB_SUCCESS, copied.storage_schema_.update_column_cnt_and_schema_version(2, 2, 10));
  ASSERT_TRUE(copied.is_valid());
  ObStorageSchema invalid;
  ASSERT_EQ(OB_INVALID_ARGUMENT, copied.storage_schema_.assign(copied.allocator_, invalid));
  ASSERT_FALSE(copied.is_valid());
}

TEST_F(TestTransferMdsCtx, deleted_tablet_schema_prepared_before_publication)
{
  storage::ObMigrationTabletParam param;
  ASSERT_EQ(OB_SUCCESS, param.build_deleted_tablet_info(ObLSID(1001), ObTabletID(200001)));
  const auto &schema = param.storage_schema_;
  ASSERT_TRUE(schema.is_valid());
  ASSERT_GT(schema.serialize_size_, 0);
  ASSERT_EQ(schema.get_schema().get_serialize_size(), schema.serialize_size_);
  std::vector<char> buf(schema.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, schema.serialize(buf.data(), buf.size(), pos));
  ASSERT_EQ(schema.serialize_size_, pos);
}

TEST_F(TestTransferMdsCtx, transfer_publication_preserves_completed_schema)
{
  using namespace storage;
  ObTransferBuildTabletInfoCtx ctx;
  auto &mgr = ctx.mgr_;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve_tablet_info(1));
  ASSERT_EQ(OB_SUCCESS, mgr.storage_schema_mgr_.init(1));
  ObMigrationTabletParam *param = OB_NEW(ObMigrationTabletParam, ObMemAttr(MTL_ID(), "TransferParam"));
  DEFER(OB_DELETE(ObMigrationTabletParam, "TransferParam", param));
  ASSERT_NE(nullptr, param);
  param->ls_id_ = ObLSID(1001);
  param->tablet_id_ = ObTabletID(200001);
  param->data_tablet_id_ = param->tablet_id_;
  param->create_scn_ = SCN::base_scn();
  param->start_scn_ = SCN::base_scn();
  param->clog_checkpoint_scn_ = SCN::base_scn();
  param->snapshot_version_ = 1;
  param->multi_version_start_ = 1;
  param->compat_mode_ = lib::Worker::CompatMode::MYSQL;
  param->is_empty_shell_ = false;
  param->last_persisted_committed_tablet_status_.tablet_status_ = ObTabletStatus::NORMAL;
  param->last_persisted_committed_tablet_status_.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  ASSERT_EQ(OB_SUCCESS, param->ha_status_.init_status());
  ASSERT_EQ(OB_SUCCESS, param->transfer_info_.init());
  ASSERT_EQ(OB_SUCCESS, param->storage_schema_.init_placeholder(param->allocator_));
  ASSERT_EQ(OB_SUCCESS, param->medium_info_list_.init(param->allocator_));
  ASSERT_TRUE(param->is_valid());
  ObMigrationTabletParam *owned_param = param;
  const ObStorageSchema *candidate = nullptr;
  ASSERT_EQ(OB_HASH_NOT_EXIST, ctx.get_storage_schema(param->tablet_id_, candidate));
  ASSERT_EQ(nullptr, candidate);
  ASSERT_EQ(0, ctx.get_tablet_info_num());

  ObStorageSchema *latest_schema = nullptr;
  auto &schema_mgr = mgr.storage_schema_mgr_;
  ASSERT_EQ(OB_SUCCESS, ObStorageSchemaUtil::alloc_storage_schema(schema_mgr.allocator_, latest_schema));
  ASSERT_EQ(OB_SUCCESS, schema_mgr.storage_schema_map_.set_refactored(param->tablet_id_, latest_schema));
  ASSERT_EQ(OB_SUCCESS, latest_schema->init_placeholder(schema_mgr.allocator_));
  ASSERT_EQ(OB_SUCCESS, latest_schema->update_column_cnt_and_schema_version(2, 2, 10));
  ASSERT_EQ(OB_SUCCESS, ctx.get_storage_schema(param->tablet_id_, candidate));
  ASSERT_EQ(latest_schema, candidate);
  // Publishing must not replace the completed schema, even when the map has a wider candidate.
  const int64_t completed_column_count = param->storage_schema_.get_schema().column_cnt_;
  ASSERT_EQ(OB_SUCCESS, param->storage_schema_.prepare_serialize_size());
  ASSERT_EQ(OB_SUCCESS, ctx.add_tablet_info(param));
  ASSERT_EQ(nullptr, param);
  ASSERT_EQ(1, ctx.get_tablet_info_num());
  const ObStorageSchemaHandle &schema = owned_param->storage_schema_;
  ASSERT_EQ(completed_column_count, schema.get_schema().column_cnt_);
  ASSERT_TRUE(schema.is_valid());
  ASSERT_EQ(schema.get_schema().get_serialize_size(), schema.serialize_size_);
  const int64_t prepared_size = schema.get_serialize_size();

  // Deliberately change the candidate after publication: the getter must preserve the snapshot.
  ASSERT_EQ(OB_SUCCESS, latest_schema->update_column_cnt_and_schema_version(3, 3, 11));
  const ObTransferBuildTabletInfoCtx &reader = ctx;
  const ObMigrationTabletParam *published = nullptr;
  ASSERT_EQ(OB_SUCCESS, reader.get_tablet_info(0, published));
  ASSERT_EQ(owned_param, published);
  ASSERT_EQ(completed_column_count, published->storage_schema_.get_schema().column_cnt_);
  ASSERT_EQ(prepared_size, published->storage_schema_.get_serialize_size());
  std::vector<char> wire(published->get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, published->serialize(wire.data(), wire.size(), pos));
  ASSERT_EQ(wire.size(), pos);
}

TEST_F(TestTransferMdsCtx, migration_param_v1_schema_decode)
{
  using namespace storage;
  ObMigrationTabletParam param;
  param.ls_id_ = ObLSID(1001);
  param.tablet_id_ = ObTabletID(200001);
  param.data_tablet_id_ = param.tablet_id_;
  param.create_scn_ = SCN::base_scn();
  param.start_scn_ = SCN::base_scn();
  param.clog_checkpoint_scn_ = SCN::base_scn();
  param.snapshot_version_ = 1;
  param.multi_version_start_ = 1;
  param.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  param.is_empty_shell_ = true;
  param.last_persisted_committed_tablet_status_.tablet_status_ = ObTabletStatus::NORMAL;
  param.last_persisted_committed_tablet_status_.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  ASSERT_EQ(OB_SUCCESS, param.ha_status_.init_status());
  ASSERT_EQ(OB_SUCCESS, param.transfer_info_.init());
  ASSERT_TRUE(param.is_valid());

  ASSERT_EQ(OB_SUCCESS, param.storage_schema_.init_placeholder(param.allocator_));
  ASSERT_EQ(OB_SUCCESS, param.medium_info_list_.init(param.allocator_));
  ObTabletAutoincSeq auto_inc_seq;
  ObTabletTxMultiSourceDataUnit tx_data;
  tx_data.tablet_status_ = ObTabletStatus::NORMAL;
  ObTabletBindingInfo ddl_data;
  const int64_t offset = 7;
  std::vector<char> bytes(65536, '\x5a');
  char *buf = bytes.data();
  const int64_t len = bytes.size();
  int64_t pos = offset;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, ObMigrationTabletParam::MAGIC_NUM));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, ObMigrationTabletParam::PARAM_VERSION));
  const int64_t length_pos = pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, 0));
  ASSERT_EQ(OB_SUCCESS, param.ls_id_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.tablet_id_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.data_tablet_id_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.ref_tablet_id_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.create_scn_.fixed_serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.start_scn_.fixed_serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.clog_checkpoint_scn_.fixed_serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.ddl_checkpoint_scn_.fixed_serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.snapshot_version_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.multi_version_start_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i8(buf, len, pos, static_cast<int8_t>(param.compat_mode_)));
  ASSERT_EQ(OB_SUCCESS, auto_inc_seq.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.ha_status_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.report_status_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, tx_data.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, ddl_data.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.storage_schema_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.medium_info_list_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.table_store_flag_.serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, param.ddl_start_scn_.fixed_serialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.ddl_snapshot_version_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.max_sync_storage_schema_version_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.ddl_execution_id_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.ddl_data_format_version_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, pos, param.max_serialized_medium_scn_));
  ASSERT_EQ(OB_SUCCESS, param.ddl_commit_scn_.fixed_serialize(buf, len, pos));
  int64_t fill_pos = length_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, len, fill_pos, pos - offset));
  ObMigrationTabletParam decoded;
  int64_t decode_pos = offset;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  ASSERT_EQ(static_cast<int64_t>(ObMigrationTabletParam::PARAM_VERSION_V2), decoded.version_);
  ASSERT_TRUE(decoded.storage_schema_.is_valid());
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_EQ(param.tablet_id_, decoded.tablet_id_);
  ASSERT_EQ(1, decoded.storage_schema_.get_schema().column_cnt_);
}

TEST_F(TestTransferMdsCtx, empty_macro_info_matches_aggregation)
{
  using namespace storage;
  ObArenaAllocator allocator;
  ObBlockInfoSet empty_set;
  ASSERT_EQ(OB_SUCCESS, empty_set.init(1, 1, 1, 1));
  ObTabletMacroInfo aggregated;
  ObTabletMacroInfo empty;
  ASSERT_EQ(OB_SUCCESS, aggregated.init(allocator, empty_set, nullptr));
  ASSERT_EQ(OB_SUCCESS, empty.init_empty());
  ASSERT_TRUE(empty.is_inited());
  ASSERT_TRUE(empty.is_valid());
  ASSERT_EQ(aggregated.get_serialize_size(), empty.get_serialize_size());
  std::vector<char> expected(aggregated.get_serialize_size());
  std::vector<char> actual(empty.get_serialize_size());
  int64_t expected_pos = 0;
  int64_t actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, aggregated.serialize(expected.data(), expected.size(), expected_pos));
  ASSERT_EQ(OB_SUCCESS, empty.serialize(actual.data(), actual.size(), actual_pos));
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_EQ(0, MEMCMP(expected.data(), actual.data(), actual_pos));
  ASSERT_EQ(OB_INIT_TWICE, empty.init_empty());
  empty.reset();
  ASSERT_EQ(OB_SUCCESS, empty.init_empty());
  ObTabletMacroInfo decoded;
  actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, actual.data(), actual.size(), actual_pos));
  ASSERT_TRUE(decoded.is_valid());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_transfer_mds_ctx.log*");
  OB_LOGGER.set_file_name("test_transfer_mds_ctx.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
