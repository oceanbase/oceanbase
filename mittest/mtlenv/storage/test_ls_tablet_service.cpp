// owner: yunshan.tys
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

#include <gmock/gmock.h>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "unittest/storage/slog/simple_ob_storage_redo_module.h"

namespace oceanbase
{
using namespace share::schema;
using namespace share;
using namespace common;
int ObClusterVersion::get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version)
{
  data_version = DATA_VERSION_4_3_2_0;
  return OB_SUCCESS;
}
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
  SERVER_STORAGE_META_SERVICE.is_started_ = true;

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
    if (!MTL(ObTenantMetaMemMgr*)->tablet_gc_queue_.is_empty()) {
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
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
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
  const ObTabletPersisterParam persist_param(ls_id_, ls_handle.get_ls()->get_ls_epoch(), tablet_id, tmp_tablet_handle_head.get_obj()->get_transfer_seq());

  ret = ObTabletPersister::persist_and_transform_tablet(persist_param, *tmp_tablet_handle_head.get_obj(), tablet_handle_head);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletPersister::persist_and_transform_tablet(persist_param, *tmp_tablet_handle_tail.get_obj(), tablet_handle_tail);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle_head.get_obj()->set_next_tablet_guard(tablet_handle_tail);

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle old_handle;

  ObTabletMapKey head_key(ls_id_, tablet_id);
  ObUpdateTabletPointerParam param;
  ret = t3m->get_tablet(WashTabletPriority::WTP_LOW, head_key, old_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_handle_head.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = t3m->compare_and_swap_tablet(head_key, old_handle, tablet_handle_head, param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletMapKey node_key(ls_id_, node_tablet_id);
  ret = t3m->get_tablet(WashTabletPriority::WTP_LOW, node_key, old_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_handle_tail.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = t3m->compare_and_swap_tablet(node_key, old_handle, tablet_handle_tail, param);
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

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
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
  const ObTabletPersisterParam persist_param(ls_id_,  ls_handle.get_ls()->get_ls_epoch(), tablet_id, orig_tablet->get_transfer_seq());
  ret = ObTabletPersister::persist_and_transform_tablet(persist_param, *orig_tablet, tiny_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tiny_tablet = tiny_tablet_handle.get_obj();
  int64_t tablet_length = tiny_tablet->get_serialize_size();
  int64_t pos = 0;
  char *buf = static_cast<char *>(ob_malloc(tablet_length, ObNewModIds::TEST));
  const ObMetaDiskAddr &tablet_addr = tiny_tablet->tablet_addr_;
  ret = tiny_tablet->serialize(buf, tablet_length, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  orig_tablet_handle.reset();
  tiny_tablet_handle.reset();

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
  new_4k_tablet_handle.reset();

  ObTabletHandle new_tmp_tablet_handle;
  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator_, new_tmp_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *new_tmp_tablet = new_tmp_tablet_handle.get_obj();

  de_pos = 0;
  new_tmp_tablet->tablet_addr_ = tablet_addr;
  ret = new_tmp_tablet->deserialize(allocator_, buf, tablet_length, de_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tablet_id, new_tmp_tablet->tablet_meta_.tablet_id_);
  new_tmp_tablet_handle.reset();

  ob_free(buf);
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
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

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, data_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, index_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
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

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, data_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, index_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
}

TEST_F(TestLSTabletService, test_get_ls_min_end_scn)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000019);
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
  tablet_handle.reset();
  share::SCN expect_scn;
  expect_scn.val_ = 0;

  share::SCN min_end_scn_from_latest_tablets = SCN::max_scn();
  share::SCN min_end_scn_from_old_tablets = SCN::max_scn();
  ret = ls_tablet_service_->get_ls_min_end_scn(min_end_scn_from_latest_tablets, min_end_scn_from_old_tablets);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(min_end_scn_from_latest_tablets, expect_scn);

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
}

TEST_F(TestLSTabletService, test_replay_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000009);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  LOG_INFO("FEIDU test_replay_empty_shell", K(ls_id_), K(tablet_id));

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
  ObMetaDiskAddr tablet_addr = test_tablet_handle.get_obj()->tablet_addr_;

  // validate that empty shell can be washed
  ObTabletHandle normal_tablet_hdl;
  void *free_obj = nullptr;
  ObTabletPointer *tablet_ptr = test_tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr();
  ObTablet *empty_tablet = test_tablet_handle.get_obj();
  test_tablet_handle.reset(); // release the ref cnt of tablet
  ret = tablet_ptr->dump_meta_obj(normal_tablet_hdl, free_obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, free_obj);
  ASSERT_EQ((char *)(empty_tablet), (char *)(free_obj)+32); // 32Bytes header

  // gc empty tablet
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ret = t3m->push_tablet_into_gc_queue(empty_tablet);
  ASSERT_EQ(OB_SUCCESS, ret);
  bool cleared = false;
  ret = t3m->gc_tablets_in_queue(cleared);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(cleared);


  // test load empty tablet
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, test_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_tablet_handle.get_obj()->fetch_table_store(wrapper);
  ASSERT_EQ(nullptr, wrapper.get_member()->get_major_sstables().get_boundary_table(true));
  ASSERT_EQ(tablet_addr, test_tablet_handle.get_obj()->tablet_addr_);
  test_tablet_handle.reset();
  LOG_INFO("FEIDU first remove", K(ls_id_), K(tablet_id));
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  LOG_INFO("FEIDU first success", K(ls_id_), K(tablet_id));
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
  ObStorageLogger &slogger = MTL(ObTenantStorageMetaService*)->get_slogger();
  SimpleObStorageModule redo_module;
  ASSERT_EQ(OB_SUCCESS, replayer_.init(slogger.get_dir(), log_file_spec_));
  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, TestSchemaUtils::TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *empty_shell_tablet = tablet_handle.get_obj();
  ASSERT_EQ(true, empty_shell_tablet->is_empty_shell());
  tablet_handle.reset();

  LOG_INFO("FEIDU seconde remove", K(ls_id_), K(tablet_id));
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  LOG_INFO("FEIDU seconde success", K(ls_id_), K(tablet_id));
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
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

  share::SCN create_commit_scn = share::SCN::plus(share::SCN::base_scn(), 100);
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED, create_commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle old_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, old_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationTabletParam param;
  ret = old_tablet_handle.get_obj()->build_migration_tablet_param(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  old_tablet_handle.reset();

  ret = ls_tablet_service_->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletHandle test_tablet_handle;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, test_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletCreateDeleteMdsUserData user_data;
  ASSERT_EQ(OB_SUCCESS, test_tablet_handle.get_obj()->get_latest_committed(user_data));
  ASSERT_EQ(ObTabletStatus::DELETED, user_data.tablet_status_);
  test_tablet_handle.reset();

  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->create_transfer_in_tablet(ls_id_, param, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_EMPTY_RESULT, tablet_handle.get_obj()->get_latest_committed(user_data));

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle.reset();
  valid_tablet_num(3);
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
  old_tablet_handle.reset();

  char buf[4096] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(buf, 4096, pos));
  ASSERT_EQ(pos, tablet_meta.get_serialize_size());

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
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
  tablet_handle.reset();

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
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
  addr.set_block_addr(macro_id, 0, 4096, ObMetaDiskAddr::DiskType::BLOCK);
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
  valid_tablet_num(3);
}

TEST_F(TestLSTabletService, test_migrate_param)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000012);
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
  old_tablet_handle.reset();

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
  ASSERT_EQ(tablet_meta.micro_index_clustered_, de_tablet_meta.micro_index_clustered_);
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

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
}

TEST_F(TestLSTabletService, test_migrate_param_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000013);
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
  old_tablet_handle.reset();

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

  //ObMigrationTabletParam other_tablet_meta;
  //ASSERT_EQ(OB_SUCCESS, tablet_meta.build_deleted_tablet_info(ls_id_, tablet_id));


  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
}

TEST_F(TestLSTabletService, test_update_empty_shell)
{
  // create_tablet_without_index
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000014);
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
  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *empty_shell_tablet = tablet_handle.get_obj();
  ASSERT_TRUE(empty_shell_tablet->is_empty_shell());
  tablet_handle.reset();

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  valid_tablet_num(3);
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
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->create_memtable(data_tablet_id, 100, false, false));
  ASSERT_EQ(1, tablet_handle.get_obj()->memtable_count_);

  LOG_WARN("FEIDU: release memtable 1");
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_release_memtable_for_offline(data_tablet_id, SCN::max_scn()));
  LOG_WARN("FEIDU: release memtable 2");
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ls_handle.get_ls()->get_tablet_svr()->update_tablet_release_memtable_for_offline(data_tablet_id, SCN::max_scn()));
  LOG_WARN("FEIDU: release memtable done");
  tablet_handle.reset();

  ret = ls_tablet_service_->do_remove_tablet(ls_id_, data_tablet_id);
  LOG_WARN("FEIDU: release memtable remove tablet");
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, update_tablet_ddl_commit_scn)
{
  int ret = OB_SUCCESS;
  const int64_t inner_tablet_count = INNER_TABLET_CNT;
  ObTabletID data_tablet_id(90000002222);
  ObTabletMapKey key;
  key.ls_id_ = ls_id_;
  key.tablet_id_ = data_tablet_id;
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
  ASSERT_EQ(SCN::min_scn(), tablet_handle.get_obj()->tablet_meta_.ddl_commit_scn_);
  share::SCN ddl_commit_scn;
  ddl_commit_scn.convert_for_logservice(100);
  ASSERT_EQ(OB_NOT_SUPPORTED, ls_handle.get_ls()->get_tablet_svr()->update_tablet_ddl_commit_scn(data_tablet_id, ddl_commit_scn));

  ObTabletHandle new_tablet_hdl;
  ObUpdateTabletPointerParam param;
  const ObTabletPersisterParam persist_param(ls_id_, ls_handle.get_ls()->get_ls_epoch(), data_tablet_id, tablet_handle.get_obj()->get_transfer_seq());
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(persist_param, *tablet_handle.get_obj(), new_tablet_hdl));
  ret = new_tablet_hdl.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr *)->compare_and_swap_tablet(key, tablet_handle, new_tablet_hdl, param));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_ddl_commit_scn(data_tablet_id, ddl_commit_scn));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->get_tablet(data_tablet_id, tablet_handle));
  ASSERT_EQ(ddl_commit_scn, tablet_handle.get_obj()->tablet_meta_.ddl_commit_scn_);

  tablet_handle.reset();
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, data_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
}


TEST_F(TestLSTabletService, test_empty_shell_mds_compat)
{
  // create an empty shell tablet
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
  ObTabletHandle tablet_handle;
  ret = ls_tablet_service_->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet &empty_shell_tablet = *tablet_handle.get_obj();
  ASSERT_TRUE(empty_shell_tablet.is_empty_shell());
  ASSERT_TRUE(nullptr == empty_shell_tablet.mds_data_);
  ASSERT_TRUE(ObTabletStatus::Status::DELETED == empty_shell_tablet.tablet_meta_.last_persisted_committed_tablet_status_.tablet_status_);

  ObArenaAllocator compat_allocator;
  ObTableHandleV2 empty_mds_sstable_hdl;
  ObTablet upgrade_tablet;
  ret = upgrade_tablet.init_for_compat(compat_allocator, empty_shell_tablet, empty_mds_sstable_hdl);
  // mds data is null
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  // mock an old tablet
  empty_shell_tablet.version_ = ObTablet::VERSION_V3;
  empty_shell_tablet.mds_data_ = OB_NEWx(ObTabletMdsData, &compat_allocator);
  ASSERT_TRUE(nullptr != empty_shell_tablet.mds_data_);
  empty_shell_tablet.mds_data_->tablet_status_cache_.assign(empty_shell_tablet.tablet_meta_.last_persisted_committed_tablet_status_);
  empty_shell_tablet.tablet_meta_.last_persisted_committed_tablet_status_.on_init();
  ASSERT_TRUE(ObTabletStatus::Status::DELETED == empty_shell_tablet.mds_data_->tablet_status_cache_.tablet_status_);

  // compat to new format
  upgrade_tablet.assign_pointer_handle(empty_shell_tablet.get_pointer_handle());
  upgrade_tablet.log_handler_ = empty_shell_tablet.log_handler_;
  ret = upgrade_tablet.init_for_compat(compat_allocator, empty_shell_tablet, empty_mds_sstable_hdl);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(ObTablet::VERSION_V4 == upgrade_tablet.version_);
  ASSERT_TRUE(nullptr == upgrade_tablet.mds_data_);
  ASSERT_TRUE(ObTabletStatus::Status::DELETED == upgrade_tablet.tablet_meta_.last_persisted_committed_tablet_status_.tablet_status_);

  // release tmp memory and tablet
  empty_shell_tablet.mds_data_->~ObTabletMdsData();
  compat_allocator.free(empty_shell_tablet.mds_data_);
  empty_shell_tablet.mds_data_ = nullptr;
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSTabletService, test_serialize_sstable_with_min_filled_tx_scn)
{
  ObTabletID tablet_id(99999);
  blocksstable::ObSSTable sstable;

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObTabletCreateSSTableParam param;
  TestTabletHelper::prepare_sstable_param(tablet_id, schema, param);
  //update sstable param table key
  param.table_key_.table_type_ = ObITable::MINOR_SSTABLE;
  param.filled_tx_scn_ = param.table_key_.get_end_scn();

  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));

  //modified sstable filled tx scn as min
  sstable.meta_->basic_meta_.filled_tx_scn_.set_min();
  sstable.meta_cache_.filled_tx_scn_.set_min();

  const int64_t size = sstable.get_serialize_size();
  char *full_buf = static_cast<char *>(allocator_.alloc(size));
  int64_t pos = 0;
  int ret = sstable.serialize(full_buf, size, pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  pos = 0;
  ret = sstable.deserialize(allocator_, full_buf, size, pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(sstable.meta_->basic_meta_.filled_tx_scn_, sstable.get_key().get_end_scn());
}


TEST_F(TestLSTabletService, test_new_tablet_has_backup_table_with_ha_status)
{

  //create tablet
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(10000014);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLS *ls = nullptr;
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ls = ls_handle.get_ls();
  ASSERT_EQ(true, OB_NOT_NULL(ls));

  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::NORMAL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ret = ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_EQ(true, OB_NOT_NULL(tablet));


  //create backup sstable
  blocksstable::ObSSTable sstable;
  ObTabletCreateSSTableParam param;
  TestTabletHelper::prepare_sstable_param(tablet_id, schema, param);
  param.table_key_.table_type_ = ObITable::MINOR_SSTABLE;
  param.filled_tx_scn_ = param.table_key_.get_end_scn();
  param.table_backup_flag_.set_has_backup();
  param.table_backup_flag_.set_no_local();
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));

  ObTableHandleV2 table_handle;
  ret = table_handle.set_sstable(&sstable, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle new_table_handle;
  const int64_t update_snapshot_version = sstable.get_snapshot_version();
  const int64_t update_multi_version_start = tablet->get_multi_version_start();
  ObStorageSchema *storage_schema = nullptr;
  ret = tablet->load_storage_schema(allocator_, storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBatchUpdateTableStoreParam update_table_store_param;
  update_table_store_param.tablet_meta_ = nullptr;
  update_table_store_param.rebuild_seq_ = ls->get_rebuild_seq();
  update_table_store_param.need_replace_remote_sstable_ = false;
  update_table_store_param.release_mds_scn_.set_min();
  ret = update_table_store_param.tables_handle_.add_table(table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service_->build_tablet_with_batch_tables(tablet_id, update_table_store_param);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  tablet_handle.reset();
  new_table_handle.reset();
  ret = ls_tablet_service_->do_remove_tablet(ls_id_, tablet_id);
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
