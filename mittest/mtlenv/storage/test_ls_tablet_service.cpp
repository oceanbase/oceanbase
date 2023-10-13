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
#include "storage/tablet/ob_tablet_persister.h"
#include "unittest/storage/slog/simple_ob_storage_redo_module.h"

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
  void construct_sstable(
      const ObTabletID &tablet_id,
      blocksstable::ObSSTable &ob_sstable,
      common::ObArenaAllocator &allocator);
  void valid_tablet_num(const int64_t &inner_tablet_count);

public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;
  static const int64_t TEST_LS_ID = 101;
  static const int64_t INNER_TABLET_CNT = 3;
public:
  const uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObLSTabletService *ls_tablet_service_;
  common::ObArenaAllocator allocator_;
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
  LOG_INFO("TestLSTabletService::SetUpTestCase");
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
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
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  LOG_INFO("TestLSTabletService::SetUp");
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();

  ls_tablet_service_ = ls->get_tablet_svr();

  while (true) {
    if (nullptr != MTL(ObTenantMetaMemMgr*)->gc_head_) {
      LOG_INFO("wait t3m gc tablet clean");
      usleep(300 * 1000); // wait 300ms
    } else {
      break;
    }
  }
}

void TestLSTabletService::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

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

  share::schema::ObTableSchema node_schema;
  TestSchemaUtils::prepare_data_schema(node_schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, node_tablet_id, node_schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tmp_tablet_handle_head;
  ObTabletHandle tmp_tablet_handle_tail;
  // get two tablets and consrtuct linked list
  ret = ls_tablet_service_->get_tablet(tablet_id, tmp_tablet_handle_head);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_service_->get_tablet(node_tablet_id, tmp_tablet_handle_tail);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTabletPersister::persist_and_transform_tablet(*tmp_tablet_handle_head.get_obj(), tablet_handle_head);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletPersister::persist_and_transform_tablet(*tmp_tablet_handle_tail.get_obj(), tablet_handle_tail);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle_head.get_obj()->set_next_tablet_guard(tablet_handle_tail);

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle old_handle;

  ObTabletMapKey head_key(ls_id_, tablet_id);
  ret = t3m->get_tablet(WashTabletPriority::WTP_LOW, head_key, old_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = t3m->compare_and_swap_tablet(head_key, old_handle, tablet_handle_head);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletMapKey node_key(ls_id_, node_tablet_id);
  ret = t3m->get_tablet(WashTabletPriority::WTP_LOW, node_key, old_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = t3m->compare_and_swap_tablet(node_key, old_handle, tablet_handle_tail);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_bucket_cnt)
{
  const int64_t unify_bucket_num = common::hash::cal_next_prime(ObTabletCommon::BUCKET_LOCK_BUCKET_CNT);
  const int64_t lock_bkt_cnt = ls_tablet_service_->bucket_lock_.bucket_cnt_;
  const int64_t id_set_lock_bkt_cnt = ls_tablet_service_->tablet_id_set_.bucket_lock_.bucket_cnt_;
  const int64_t id_set_bkt_cnt = ls_tablet_service_->tablet_id_set_.id_set_.ht_.get_bucket_count();

  ASSERT_NE(unify_bucket_num, 0);
  // ASSERT_EQ(unify_bucket_num, lock_bkt_cnt);
  ASSERT_EQ(unify_bucket_num, id_set_lock_bkt_cnt);
  ASSERT_EQ(unify_bucket_num, id_set_bkt_cnt);
}

void TestLSTabletService::construct_sstable(
    const ObTabletID &tablet_id,
    blocksstable::ObSSTable &sstable,
    common::ObArenaAllocator &allocator)
{
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObTabletCreateSSTableParam param;
  TestTabletHelper::prepare_sstable_param(tablet_id, schema, param);

  int ret = sstable.init(param, &allocator);
  ASSERT_EQ(common::OB_SUCCESS, ret);
}

void TestLSTabletService::valid_tablet_num(const int64_t &inner_tablet_count)
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool all_tablet_cleaned = false;
  while (!all_tablet_cleaned) {
    t3m->gc_tablets_in_queue(all_tablet_cleaned);
  }
  ASSERT_EQ(inner_tablet_count, t3m->tablet_buffer_pool_.inner_used_num_);
}

TEST_F(TestLSTabletService, test_create_tablet_without_index)
{
  int ret = OB_SUCCESS;
  const int64_t inner_tablet_count = INNER_TABLET_CNT;
  ObTabletID tablet_id(10000001);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(1 + inner_tablet_count, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(1 + inner_tablet_count, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_serialize_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  ObTabletID tablet_id(101);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // test serialize and deserialize tablet
  ObTabletHandle orig_tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObTablet *orig_tablet = orig_tablet_handle.get_obj();

  ObTabletHandle tiny_tablet_handle;
  ret = ObTabletPersister::persist_and_transform_tablet(*orig_tablet, tiny_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tiny_tablet = tiny_tablet_handle.get_obj();
  int64_t tablet_length = tiny_tablet->get_serialize_size();
  int64_t pos = 0;
  char *buf = static_cast<char *>(ob_malloc(tablet_length, ObNewModIds::TEST));
  const ObMetaDiskAddr &tablet_addr = tiny_tablet->tablet_addr_;
  ret = tiny_tablet->serialize(buf, tablet_length, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObTabletMapKey key(ls_id_, tablet_id);

  ObTabletHandle new_4k_tablet_handle;
  ret = ObTabletCreateDeleteHelper::acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, key, new_4k_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_4k_tablet = new_4k_tablet_handle.get_obj();

  int64_t de_pos = 0;
  ret = new_4k_tablet->deserialize(buf, tablet_length, de_pos);
  new_4k_tablet->tablet_addr_ = tablet_addr;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tablet_id, new_4k_tablet->tablet_meta_.tablet_id_);
  ASSERT_EQ(OB_SUCCESS, new_4k_tablet->inc_macro_ref_cnt());

  ObTabletHandle new_tmp_tablet_handle;
  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator_, new_tmp_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tmp_tablet = new_tmp_tablet_handle.get_obj();

  de_pos = 0;
  new_tmp_tablet->tablet_addr_ = tablet_addr;
  ret = new_tmp_tablet->deserialize(allocator_, buf, tablet_length, de_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tablet_id, new_tmp_tablet->tablet_meta_.tablet_id_);


  ob_free(buf);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

/**
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
  char *buf = static_cast<char *>(ob_malloc(tablets_length, "TestBuff"));
  const ObMetaDiskAddr &tablet_addr = tablet_head->tablet_addr_;
  ret = tablet_head->serialize(buf, tablets_length, se_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle new_tablet_handle;
  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator_, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();

  int64_t de_pos = 0;
  new_tablet->tablet_addr_ = tablet_addr;
  ret = new_tablet->deserialize(allocator_, buf, tablets_length, de_pos);
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

TEST_F(TestLSTabletService, test_serialize_linked_list_tablet_deserialize_4k)
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
  ret = ObTabletCreateDeleteHelper::acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, key, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();

  int64_t de_pos = 0;
  ret = new_tablet->deserialize(buf, tablets_length, de_pos);
  new_tablet->tablet_addr_ = tablet_head->tablet_addr_;
  new_tablet->next_tablet_guard_.get_obj()->tablet_addr_ = tablet_head->tablet_addr_;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, new_tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(tablet_id, new_tablet->tablet_meta_.tablet_id_);
  ASSERT_EQ(OB_SUCCESS, new_tablet->inc_macro_ref_cnt());

  ObTablet *next_tablet = new_tablet->next_tablet_guard_.get_obj();
  next_tablet->tablet_addr_ = tablet_head->tablet_addr_;
  ASSERT_EQ(false, next_tablet->tablet_meta_.has_next_tablet_);
  ASSERT_EQ(node_tablet_id, next_tablet->tablet_meta_.tablet_id_);
  ASSERT_EQ(OB_SUCCESS, next_tablet->inc_macro_ref_cnt());

  key.tablet_id_ = tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  key.tablet_id_ = node_tablet_id;
  ret = MTL(ObTenantMetaMemMgr*)->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_deserialize_linked_tablet_with_allocator)
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
  const ObMetaDiskAddr &tablet_addr = tablet_head->tablet_addr_;
  ret = tablet_head->serialize(buf, tablets_length, se_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle new_tablet_handle;
  ObTabletMapKey key(ls_id_, tablet_id);
  const ObTabletID test_tablet_id(12345678);
  ObTabletMapKey test_key(ls_id_, test_tablet_id);
  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(test_key, allocator_, new_tablet_handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator_, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != new_tablet_handle.get_obj());
  new_tablet_handle.reset();

  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator_, new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tablet = new_tablet_handle.get_obj();

  int64_t de_pos = 0;
  const int64_t used_tablet_cnt = MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.used_obj_cnt_;
  new_tablet->tablet_addr_ = tablet_addr;
  ret = new_tablet->deserialize(allocator_, buf, tablets_length, de_pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(used_tablet_cnt, MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.used_obj_cnt_);

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
*/

TEST_F(TestLSTabletService, test_create_tablet_with_index)
{
  int ret = OB_SUCCESS;
  const int64_t inner_tablet_count = INNER_TABLET_CNT;
  ObTabletID data_tablet_id(4001);
  ObTabletID index_tablet_id(4002);
  share::schema::ObTableSchema data_schema;
  share::schema::ObTableSchema index_schema;

  TestSchemaUtils::prepare_data_schema(data_schema);
  TestSchemaUtils::prepare_index_schema(index_schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ret = TestTabletHelper::create_tablet(ls_handle, data_tablet_id, data_schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = TestTabletHelper::create_tablet(ls_handle, index_tablet_id, index_schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(ls_handle, data_tablet_id, data_schema, allocator_);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = data_tablet_id;
  ret = ls_tablet_service_->do_remove_tablet(key);
  key.tablet_id_ = index_tablet_id;
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_create_index_tablet)
{
  int ret = OB_SUCCESS;
  const int64_t inner_tablet_count = INNER_TABLET_CNT;
  ObTabletID data_tablet_id(5001);
  share::schema::ObTableSchema data_schema;
  TestSchemaUtils::prepare_index_schema(data_schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ret = TestTabletHelper::create_tablet(ls_handle, data_tablet_id, data_schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(ls_handle, data_tablet_id, data_schema, allocator_);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletID index_tablet_id(5002);
  share::schema::ObTableSchema index_schema;

  TestSchemaUtils::prepare_index_schema(index_schema);

  ret = TestTabletHelper::create_tablet(ls_handle, index_tablet_id, index_schema, allocator_);
  //ret = TestTabletHelper::create_tablet(*ls_tablet_service_, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ret = TestTabletHelper::create_tablet(ls_handle, index_tablet_id, index_schema, allocator_);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);

  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(2 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = data_tablet_id;
  ret = ls_tablet_service_->do_remove_tablet(key);
  key.tablet_id_ = index_tablet_id;
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_get_ls_min_end_scn)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000009);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletMapKey key(ls_id_, tablet_id);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle tablet_handle;
  ret = t3m->get_tablet(WashTabletPriority::WTP_HIGH, key, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN test_scn = share::SCN::min_scn();
  share::SCN expect_scn;
  share::SCN orig_scn;
  expect_scn.val_ = 0;

  share::SCN min_end_scn_from_latest_tablets = SCN::max_scn();
  share::SCN min_end_scn_from_old_tablets = SCN::max_scn();
  ret = ls_tablet_service_->get_ls_min_end_scn(min_end_scn_from_latest_tablets, min_end_scn_from_old_tablets);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(min_end_scn_from_latest_tablets, expect_scn);

  orig_scn = tablet_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_;
  tablet_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_ = test_scn; // modify scn of tablet

  min_end_scn_from_latest_tablets.set_max();
  min_end_scn_from_old_tablets.set_max();
  ret = ls_tablet_service_->get_ls_min_end_scn(min_end_scn_from_latest_tablets, min_end_scn_from_old_tablets);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(min_end_scn_from_latest_tablets, expect_scn); // still get from major sstable

  tablet_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_ = orig_scn; // set orig_scn to del tablet

  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_replay_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000009);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service_->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle test_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, test_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  test_tablet_handle.get_obj()->fetch_table_store(wrapper);
  ASSERT_EQ(nullptr, wrapper.get_member()->get_major_sstables().get_boundary_table(true));

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogCursor replay_start_cursor_;
  ObLogCursor replay_finish_cursor_;
  SimpleObStorageModule tenant_storage_;
  replay_start_cursor_.file_id_ = 1;
  replay_start_cursor_.log_id_ = 1;
  replay_start_cursor_.offset_ = 0;
  blocksstable::ObLogFileSpec log_file_spec_;
  log_file_spec_.retry_write_policy_ = "normal";
  log_file_spec_.log_create_policy_ = "normal";
  log_file_spec_.log_write_policy_ = "truncate";
  ObStorageLogReplayer replayer_;
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  SimpleObStorageModule redo_module;
  ASSERT_EQ(OB_SUCCESS, replayer_.init(slogger->get_dir(), log_file_spec_));
  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, TestSchemaUtils::TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *empty_shell_tablet = tablet_handle.get_obj();
  ASSERT_EQ(true, empty_shell_tablet->is_empty_shell());

  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_cover_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000010);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle old_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, old_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationTabletParam tablet_meta;
  ret = old_tablet_handle.get_obj()->build_migration_tablet_param(tablet_meta);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletCreateDeleteMdsUserData data;
  ObTabletStatus status(ObTabletStatus::NORMAL);
  data.tablet_status_ = status;
  const int64_t data_serialize_size = data.get_serialize_size();
  int64_t pos = 0;
  char *buf = static_cast<char *>(allocator_.alloc(data_serialize_size));
  ASSERT_EQ(OB_SUCCESS, data.serialize(buf, data_serialize_size, pos));
  tablet_meta.mds_data_.tablet_status_committed_kv_.v_.user_data_.assign_ptr(buf, data_serialize_size);

  ret = ls_tablet_service_->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletHandle test_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, test_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletCreateDeleteMdsUserData user_data;
  ASSERT_EQ(OB_SUCCESS, test_tablet_handle.get_obj()->get_tablet_status(share::SCN::max_scn(), user_data));
  ASSERT_EQ(ObTabletStatus::DELETED, user_data.tablet_status_);

  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->create_transfer_in_tablet(ls_id_, tablet_meta, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_tablet_status(share::SCN::max_scn(), user_data));
  ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_);

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_migrate_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000011);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator allocator;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service_->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle old_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, old_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationTabletParam tablet_meta;
  tablet_meta.reset();
  ret = old_tablet_handle.get_obj()->build_migration_tablet_param(tablet_meta);
  ASSERT_EQ(OB_SUCCESS, ret);

  char buf[4096] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(buf, 4096, pos));
  ASSERT_EQ(pos, tablet_meta.get_serialize_size());

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationTabletParam new_tablet_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_tablet_meta.deserialize(buf, 4096, pos));
  LOG_INFO("test_migrate_empty_shell", K(pos), "size", tablet_meta.get_serialize_size());
  ASSERT_EQ(pos, tablet_meta.get_serialize_size());

  ret = ls_tablet_service_->create_or_update_migration_tablet(new_tablet_meta, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *empty_shell_tablet = tablet_handle.get_obj();
  ASSERT_EQ(true, empty_shell_tablet->is_empty_shell());

  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_serialize_sstable_full_and_shell)
{
  ObTabletID tablet_id(99999);
  blocksstable::ObSSTable sstable;

  construct_sstable(tablet_id, sstable, allocator_);

  const int64_t size = sstable.get_serialize_size();
  char *full_buf = static_cast<char *>(allocator_.alloc(size));
  int64_t pos = 0;
  int ret = sstable.serialize(full_buf, size, pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMetaDiskAddr addr;
  MacroBlockId macro_id;
  macro_id.set_block_index(1001);
  macro_id.set_write_seq(111);
  addr.set_block_addr(macro_id, 0, 4096);
  ret = sstable.set_addr(addr);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  const int64_t shell_size = sstable.get_serialize_size();
  char *shell_buf = static_cast<char *>(allocator_.alloc(size));
  pos = 0;
  ret = sstable.serialize(shell_buf, shell_size, pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  pos = 0;
  ret = sstable.deserialize(allocator_, shell_buf, shell_size, pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  pos = 0;
  ret = sstable.deserialize(allocator_, full_buf, size, pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_migrate_param)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000011);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator allocator;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle old_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, old_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationTabletParam tablet_meta;
  tablet_meta.reset();
  ret = old_tablet_handle.get_obj()->build_migration_tablet_param(tablet_meta);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t serialize_size = tablet_meta.get_serialize_size();
  char *buf = static_cast<char*>(allocator.alloc(serialize_size));
  int64_t pos = 0;
  ASSERT_NE(nullptr, buf);
  ASSERT_TRUE(tablet_meta.is_valid());
  ASSERT_FALSE(tablet_meta.is_empty_shell());
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(buf, serialize_size, pos));
  pos = 0;
  ObMigrationTabletParam de_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, de_tablet_meta.deserialize(buf, serialize_size, pos));
  ASSERT_FALSE(de_tablet_meta.is_empty_shell());
  ASSERT_TRUE(de_tablet_meta.storage_schema_.is_valid());
  ASSERT_TRUE(de_tablet_meta.is_valid());
  ASSERT_FALSE(de_tablet_meta.is_empty_shell());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, de_tablet_meta.serialize(buf, serialize_size, pos));

  ObMigrationTabletParam other_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, other_tablet_meta.assign(de_tablet_meta));
  /*
  ASSERT_NE(OB_SUCCESS, tablet_meta.build_deleted_tablet_info(ls_id_, tablet_id));
  tablet_meta.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_meta.build_deleted_tablet_info(ls_id_, tablet_id));
  */

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_migrate_param_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000011);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator allocator;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service_->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle old_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, old_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationTabletParam tablet_meta;
  tablet_meta.reset();
  ret = old_tablet_handle.get_obj()->build_migration_tablet_param(tablet_meta);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t serialize_size = tablet_meta.get_serialize_size();
  char *buf = static_cast<char*>(allocator.alloc(serialize_size));
  int64_t pos = 0;
  ASSERT_NE(nullptr, buf);
  ASSERT_TRUE(tablet_meta.is_valid());
  ASSERT_TRUE(tablet_meta.is_empty_shell());
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(buf, serialize_size, pos));
  pos = 0;
  ObMigrationTabletParam de_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, de_tablet_meta.deserialize(buf, serialize_size, pos));
  ASSERT_TRUE(de_tablet_meta.is_empty_shell());
  ASSERT_FALSE(de_tablet_meta.storage_schema_.is_valid());
  ASSERT_TRUE(de_tablet_meta.is_valid());
  ASSERT_TRUE(de_tablet_meta.is_empty_shell());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, de_tablet_meta.serialize(buf, serialize_size, pos));

  ObMigrationTabletParam other_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, other_tablet_meta.assign(de_tablet_meta));
  /*
  ObMigrationTabletParam other_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.build_deleted_tablet_info(ls_id_, tablet_id));
  */

  ObTabletMapKey key(ls_id_, tablet_id);
  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_update_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000009);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_service_->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletMapKey key(ls_id_, tablet_id);
  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *empty_shell_tablet = tablet_handle.get_obj();
  ASSERT_TRUE(empty_shell_tablet->is_empty_shell());

  ret = ls_tablet_service_->do_remove_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, update_tablet_release_memtable_for_offline)
{
  int ret = OB_SUCCESS;
  const int64_t inner_tablet_count = INNER_TABLET_CNT;
  ObTabletID data_tablet_id(9000000111);
  share::schema::ObTableSchema data_schema;

  TestSchemaUtils::prepare_data_schema(data_schema);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ret = TestTabletHelper::create_tablet(ls_handle, data_tablet_id, data_schema, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());
  ret = TestTabletHelper::create_tablet(ls_handle, data_tablet_id, data_schema, allocator_);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);
  valid_tablet_num(inner_tablet_count);
  ASSERT_EQ(1 + INNER_TABLET_CNT, MTL(ObTenantMetaMemMgr*)->tablet_map_.map_.size());

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->get_tablet(data_tablet_id, tablet_handle));
  ASSERT_EQ(0, tablet_handle.get_obj()->memtable_count_);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->create_memtable(data_tablet_id, 100));
  ASSERT_EQ(1, tablet_handle.get_obj()->memtable_count_);

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_release_memtable_for_offline(data_tablet_id, SCN::max_scn()));
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ls_handle.get_ls()->get_tablet_svr()->update_tablet_release_memtable_for_offline(data_tablet_id, SCN::max_scn()));

  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = data_tablet_id;
  ret = ls_tablet_service_->do_remove_tablet(key);
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
