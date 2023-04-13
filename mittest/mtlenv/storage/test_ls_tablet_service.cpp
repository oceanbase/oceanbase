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
#include <gmock/gmock.h>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#include "storage/schema_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/mock_ob_meta_report.h"
#include "storage/mock_disk_usage_report.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/file/file_directory_utils.h"
#include "share/ob_device_manager.h"
#include "share/ob_local_device.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "observer/ob_safe_destroy_thread.h"

namespace oceanbase
{
using namespace share::schema;
using namespace share;
using namespace common;
namespace storage
{

class TestLSTabletService : public ::testing::Test
{
public:
  TestLSTabletService();
  virtual ~TestLSTabletService() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();

  void construct_and_get_tablet_list(
      const ObTabletID &tablet_id,
      const ObTabletID &node_tablet_id,
      ObTabletHandle &tablet_handle_head,
      ObTabletHandle &tablet_handle_tail);

public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;
  static const int64_t TEST_LS_ID = 101;
  static const int64_t INNER_TABLET_CNT = 3;
public:
  const uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObLSTabletService *ls_tablet_service_;
};

TestLSTabletService::TestLSTabletService()
  : tenant_id_(TestSchemaUtils::TEST_TENANT_ID),
    ls_id_(TEST_LS_ID),
    ls_tablet_service_(nullptr)
{
}

void TestLSTabletService::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  SAFE_DESTROY_INSTANCE.init();
  SAFE_DESTROY_INSTANCE.start();
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  ObIOManager::get_instance().add_tenant_io_manager(
      TestSchemaUtils::TEST_TENANT_ID, ObTenantIOConfig::default_instance());

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TestSchemaUtils::TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestLSTabletService::SetUp()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();

  ls_tablet_service_ = ls->get_tablet_svr();
}

void TestLSTabletService::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

  SAFE_DESTROY_INSTANCE.stop();
  SAFE_DESTROY_INSTANCE.wait();
  SAFE_DESTROY_INSTANCE.destroy();
  MockTenantModuleEnv::get_instance().destroy();
}

void TestLSTabletService::TearDown()
{
  ls_tablet_service_ = nullptr;
}

void TestLSTabletService::construct_and_get_tablet_list(
    const ObTabletID &tablet_id,
    const ObTabletID &node_tablet_id,
    ObTabletHandle &tablet_handle_head,
    ObTabletHandle &tablet_handle_tail)
{
  int ret = OB_SUCCESS;
  // create two tablets
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  obrpc::ObCreateTabletInfo create_tablet_info;
  create_tablet_info.data_tablet_id_ = tablet_id;
  ret = create_tablet_info.tablet_ids_.push_back(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  share::schema::ObTableSchema node_schema;
  TestSchemaUtils::prepare_data_schema(node_schema);

  obrpc::ObCreateTabletInfo create_node_tablet_info;
  create_node_tablet_info.data_tablet_id_ = node_tablet_id;
  ret = create_node_tablet_info.tablet_ids_.push_back(node_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_node_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  create_node_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  obrpc::ObBatchCreateTabletArg arg;
  bool is_replay = true; // does not write clog
  arg.id_ = ls_id_;
  arg.major_frozen_scn_ = share::SCN::min_scn();
  ret = arg.table_schemas_.push_back(schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.table_schemas_.push_back(node_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_node_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // get two tablets and consrtuct linked list
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle_head);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_service_->get_tablet(node_tablet_id, tablet_handle_tail);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle_head.get_obj()->set_next_tablet_guard(tablet_handle_tail);
}

TEST_F(TestLSTabletService, test_create_tablet_without_index)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000001);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  obrpc::ObCreateTabletInfo create_tablet_info;
  create_tablet_info.data_tablet_id_ = tablet_id;
  ret = create_tablet_info.tablet_ids_.push_back(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  obrpc::ObBatchCreateTabletArg arg;
  arg.id_ = ls_id_;
  arg.major_frozen_scn_ = share::SCN::min_scn();
  ret = arg.table_schemas_.push_back(schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_serialize_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  ObTabletID tablet_id(101);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  obrpc::ObCreateTabletInfo create_tablet_info;
  create_tablet_info.data_tablet_id_ = tablet_id;
  ret = create_tablet_info.tablet_ids_.push_back(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  obrpc::ObBatchCreateTabletArg arg;
  bool is_replay = true; // does not write clog
  arg.id_ = ls_id_;
  arg.major_frozen_scn_ = share::SCN::min_scn();
  ret = arg.table_schemas_.push_back(schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // test serialize and deserialize tablet
  ObTabletHandle orig_tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *orig_tablet = orig_tablet_handle.get_obj();

  ObTabletHandle new_tablet_handle;
  const ObTabletMapKey key(ls_id_, tablet_id);
  ret = ObTabletCreateDeleteHelper::acquire_tablet(key, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();

  int64_t tablet_length = orig_tablet->get_serialize_size();
  int64_t pos = 0;
  char *buf = static_cast<char *>(ob_malloc(tablet_length, ObNewModIds::TEST));
  ret = orig_tablet->serialize(buf, tablet_length, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObIAllocator &allocator = MTL(ObTenantMetaMemMgr*)->get_tenant_allocator();
  int64_t de_pos = 0;
  ret = new_tablet->deserialize(allocator, buf, tablet_length, de_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(tablet_id, new_tablet->tablet_meta_.tablet_id_);
  ob_free(buf);

  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_serialize_linked_list_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(1001);
  const ObTabletID node_tablet_id(1002);
  ObTabletHandle tablet_handle_head;
  ObTabletHandle tablet_handle_tail;
  construct_and_get_tablet_list(tablet_id, node_tablet_id, tablet_handle_head, tablet_handle_tail);
  ObTablet *tablet_head = tablet_handle_head.get_obj();

  // serialize and deserialize linked list
  int64_t tablets_length = tablet_head->get_serialize_size();
  int64_t se_pos = 0;
  char *buf = static_cast<char *>(ob_malloc(tablets_length, ObNewModIds::TEST));
  ret = tablet_head->serialize(buf, tablets_length, se_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle new_tablet_handle;
  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ObTabletCreateDeleteHelper::acquire_tablet(key, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();

  ObIAllocator &allocator = MTL(ObTenantMetaMemMgr*)->get_tenant_allocator();
  int64_t de_pos = 0;
  ret = new_tablet->deserialize(allocator, buf, tablets_length, de_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, new_tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(tablet_id, new_tablet->tablet_meta_.tablet_id_);

  ObTablet *next_tablet = new_tablet->next_tablet_guard_.get_obj();
  ASSERT_EQ(false, next_tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(node_tablet_id, next_tablet->tablet_meta_.tablet_id_);

  key.tablet_id_ = tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = node_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_deserialize_tablet_with_allocator)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(1001);
  const ObTabletID node_tablet_id(1002);
  ObTabletHandle tablet_handle_head;
  ObTabletHandle tablet_handle_tail;
  construct_and_get_tablet_list(tablet_id, node_tablet_id, tablet_handle_head, tablet_handle_tail);
  ObTablet *tablet_head = tablet_handle_head.get_obj();

  // serialize and deserialize linked list
  int64_t tablets_length = tablet_head->get_serialize_size();
  int64_t se_pos = 0;
  char *buf = static_cast<char *>(ob_malloc(tablets_length, ObNewModIds::TEST));
  ret = tablet_head->serialize(buf, tablets_length, se_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  common::ObArenaAllocator allocator;
  ObTabletHandle new_tablet_handle;
  ObTabletMapKey key(ls_id_, tablet_id);
  const ObTabletID test_tablet_id(12345678);
  ObTabletMapKey test_key(ls_id_, test_tablet_id);
  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet(WashTabletPriority::WTP_HIGH, test_key, allocator, new_tablet_handle, false);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);

  ret = ObTabletCreateDeleteHelper::acquire_tablet(key, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != new_tablet_handle.get_obj());
  new_tablet_handle.reset();

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet(WashTabletPriority::WTP_HIGH, key, allocator, new_tablet_handle, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();

  int64_t de_pos = 0;
  const int64_t used_tablet_cnt = MTL(ObTenantMetaMemMgr*)->tablet_pool_.used_obj_cnt_;
  const int64_t used_sstable_cnt = MTL(ObTenantMetaMemMgr*)->sstable_pool_.used_obj_cnt_;
  const int64_t used_size = MTL(ObTenantMetaMemMgr*)->get_tenant_allocator().used();
  ret = new_tablet->load_deserialize(allocator, buf, tablets_length, de_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_tablet->deserialize_post_work();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(used_tablet_cnt, MTL(ObTenantMetaMemMgr*)->tablet_pool_.used_obj_cnt_);
  ASSERT_EQ(used_sstable_cnt, MTL(ObTenantMetaMemMgr*)->sstable_pool_.used_obj_cnt_);
  ASSERT_EQ(used_size, MTL(ObTenantMetaMemMgr*)->get_tenant_allocator().used());

  ASSERT_EQ(true, new_tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(tablet_id, new_tablet->tablet_meta_.tablet_id_);

  ObTablet *next_tablet = new_tablet->next_tablet_guard_.get_obj();
  ASSERT_EQ(false, next_tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(node_tablet_id, next_tablet->tablet_meta_.tablet_id_);

  key.tablet_id_ = tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = node_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_trim_rebuild_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(2001);
  const ObTabletID node_tablet_id(2002);
  ObTabletHandle tablet_handle_head;
  ObTabletHandle tablet_handle_tail;
  construct_and_get_tablet_list(tablet_id, node_tablet_id, tablet_handle_head, tablet_handle_tail);

  ret = ls_tablet_service_->trim_rebuild_tablet(tablet_id, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_EQ(false, tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(node_tablet_id, tablet->tablet_meta_.tablet_id_);

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = node_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_commit_rebuild_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(3001);
  const ObTabletID node_tablet_id(3002);
  ObTabletHandle tablet_handle_head;
  ObTabletHandle tablet_handle_tail;
  construct_and_get_tablet_list(tablet_id, node_tablet_id, tablet_handle_head, tablet_handle_tail);

  ret = ls_tablet_service_->trim_rebuild_tablet(tablet_id, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_EQ(false, tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(tablet_id, tablet->tablet_meta_.tablet_id_);

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = node_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_create_tablet_with_index)
{
  int ret = OB_SUCCESS;
  ObTabletID data_tablet_id(4001);
  ObTabletID index_tablet_id(4002);
  share::schema::ObTableSchema data_schema;
  share::schema::ObTableSchema index_schema;

  TestSchemaUtils::prepare_data_schema(data_schema);
  TestSchemaUtils::prepare_index_schema(index_schema);

  obrpc::ObCreateTabletInfo create_tablet_info;
  create_tablet_info.data_tablet_id_ = data_tablet_id;
  ret = create_tablet_info.tablet_ids_.push_back(data_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.tablet_ids_.push_back(index_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(1);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  obrpc::ObBatchCreateTabletArg arg;
  const bool is_replay = true; // does not write clog
  arg.id_ = ls_id_;
  arg.major_frozen_scn_ = share::SCN::min_scn();
  ret = arg.table_schemas_.push_back(data_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.table_schemas_.push_back(index_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  //ASSERT_EQ(OB_TABLET_EXIST, ret);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = data_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = index_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_create_index_tablet)
{
  int ret = OB_SUCCESS;
  ObTabletID data_tablet_id(5001);
  share::schema::ObTableSchema data_schema;
  TestSchemaUtils::prepare_index_schema(data_schema);

  obrpc::ObCreateTabletInfo create_tablet_info;
  create_tablet_info.data_tablet_id_ = data_tablet_id;
  ret = create_tablet_info.tablet_ids_.push_back(data_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  obrpc::ObBatchCreateTabletArg arg;
  const bool is_replay = true; // does not write clog
  arg.id_ = ls_id_;
  arg.major_frozen_scn_ = share::SCN::min_scn();
  ret = arg.table_schemas_.push_back(data_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  //ASSERT_EQ(OB_TABLET_EXIST, ret);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletID index_tablet_id(5002);
  share::schema::ObTableSchema index_schema;

  TestSchemaUtils::prepare_index_schema(index_schema);

  create_tablet_info.reset();
  create_tablet_info.data_tablet_id_ = data_tablet_id;
  ret = create_tablet_info.tablet_ids_.push_back(index_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tablet_info.table_schema_index_.push_back(0);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;

  arg.reset();
  arg.id_ = ls_id_;
  arg.major_frozen_scn_ = share::SCN::min_scn();
  ret = arg.table_schemas_.push_back(index_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arg.tablets_.push_back(create_tablet_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  //ASSERT_EQ(OB_TABLET_EXIST, ret);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_pool_.inner_used_num_);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = data_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = index_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // end storage
} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ls_tablet_service.log*");
  OB_LOGGER.set_file_name("test_ls_tablet_service.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
