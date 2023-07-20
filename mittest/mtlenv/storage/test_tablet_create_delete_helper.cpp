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
#include <algorithm>

#define USING_LOG_PREFIX STORAGE

#define private public
#define protected public

#include "share/ob_rpc_struct.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/init_basic_struct.h"
#include "share/scn.h"
#include "storage/memtable/ob_memtable.h"

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace storage
{
class TestTabletCreateDeleteHelper : public ::testing::Test
{
public:
  class TabletInfo
  {
  public:
    ObTabletID data_tablet_id_;
    ObSArray<ObTabletID> index_tablet_id_array_;
    TO_STRING_KV(K_(data_tablet_id), K_(index_tablet_id_array));
  };
public:
  TestTabletCreateDeleteHelper();
  ~TestTabletCreateDeleteHelper() = default;
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int build_create_pure_data_tablet_arg(
      const ObLSID &ls_id,
      const ObIArray<ObTabletID> &tablet_id_array,
      ObBatchCreateTabletArg &arg);
  static int build_create_mixed_tablet_arg(
      const ObLSID &ls_id,
      const ObIArray<TabletInfo> &tablet_id_array,
      ObBatchCreateTabletArg &arg);
  static int build_create_pure_index_tablet_arg(
      const ObLSID &ls_id,
      const ObIArray<TabletInfo> &tablet_id_array,
      ObBatchCreateTabletArg &arg);
  static int build_create_hidden_and_lob_tablet_arg(
      const ObLSID &ls_id,
      const ObIArray<TabletInfo> &tablet_id_array,
      ObBatchCreateTabletArg &arg);
  static int build_remove_tablet_arg(
      const ObLSID &ls_id,
      const ObIArray<ObTabletID> &tablet_id_array,
      ObBatchRemoveTabletArg &arg);
public:
  static constexpr int64_t TEST_LS_ID = 1001;
  share::ObLSID ls_id_;
};

TestTabletCreateDeleteHelper::TestTabletCreateDeleteHelper()
  : ls_id_(TEST_LS_ID)
{
}

void TestTabletCreateDeleteHelper::SetUp()
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletCreateDeleteHelper::TearDown()
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
}

void TestTabletCreateDeleteHelper::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TestSchemaUtils::TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletCreateDeleteHelper::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_id_array,
    ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ret = arg.table_schemas_.push_back(table_schema);

  arg.id_ = ls_id;
  arg.major_frozen_scn_.set_min();

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
    const ObTabletID &tablet_id = tablet_id_array.at(i);

    obrpc::ObCreateTabletInfo create_tablet_info;
    create_tablet_info.data_tablet_id_ = tablet_id;
    create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
    if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
    } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(0))) {
      LOG_WARN("failed to push back schema index", K(ret));
    } else if (OB_FAIL(arg.tablets_.push_back(create_tablet_info))) {
      LOG_WARN("failed to push back create tablet info", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(arg));
  }

  return ret;
}

int TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
    const ObLSID &ls_id,
    const ObIArray<TabletInfo> &tablet_id_array,
    ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ret = arg.table_schemas_.push_back(table_schema);

  arg.id_ = ls_id;
  arg.major_frozen_scn_.set_min();

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
    const TabletInfo &tablet_info = tablet_id_array.at(i);
    const ObTabletID &data_tablet_id = tablet_info.data_tablet_id_;
    const ObSArray<ObTabletID> &index_tablet_id_array = tablet_info.index_tablet_id_array_;

    obrpc::ObCreateTabletInfo create_tablet_info;
    create_tablet_info.data_tablet_id_ = data_tablet_id;
    create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
    if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(data_tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(data_tablet_id));
    } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(0))) {
      LOG_WARN("failed to push back schema index", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_id_array.count(); ++j) {
        const ObTabletID &index_tablet_id = index_tablet_id_array.at(j);
        if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(index_tablet_id))) {
          LOG_WARN("failed to push back tablet id", K(ret), K(index_tablet_id));
        } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(0))) {
          LOG_WARN("failed to push back schema index", K(ret));
        }
      }

      if (OB_FAIL(arg.tablets_.push_back(create_tablet_info))) {
        LOG_WARN("failed to push back create tablet info", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(arg));
  }

  return ret;
}

int TestTabletCreateDeleteHelper::build_create_pure_index_tablet_arg(
    const ObLSID &ls_id,
    const ObIArray<TabletInfo> &tablet_id_array,
    ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ret = arg.table_schemas_.push_back(table_schema);

  arg.id_ = ls_id;
  arg.major_frozen_scn_.set_min();

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
    const TabletInfo &tablet_info = tablet_id_array.at(i);
    const ObTabletID &data_tablet_id = tablet_info.data_tablet_id_;
    const ObSArray<ObTabletID> &index_tablet_id_array = tablet_info.index_tablet_id_array_;

    obrpc::ObCreateTabletInfo create_tablet_info;
    create_tablet_info.data_tablet_id_ = data_tablet_id;
    create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
    for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_id_array.count(); ++j) {
      const ObTabletID &index_tablet_id = index_tablet_id_array.at(j);
      if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(index_tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(index_tablet_id));
      } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(0))) {
        LOG_WARN("failed to push back schema index", K(ret));
      }
    }

    if (OB_FAIL(arg.tablets_.push_back(create_tablet_info))) {
      LOG_WARN("failed to push back create tablet info", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(arg));
  }

  return ret;
}

int TestTabletCreateDeleteHelper::build_create_hidden_and_lob_tablet_arg(
    const ObLSID &ls_id,
    const ObIArray<TabletInfo> &tablet_id_array,
    ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema0;
  ObTableSchema table_schema1;
  ObTableSchema table_schema2;
  TestSchemaUtils::prepare_data_schema(table_schema0);
  TestSchemaUtils::prepare_data_schema(table_schema1);
  table_schema1.table_type_ = ObTableType::AUX_LOB_META;
  TestSchemaUtils::prepare_data_schema(table_schema2);
  table_schema2.table_type_ = ObTableType::AUX_LOB_PIECE;

  if (tablet_id_array.count() != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table id array size should be 2", K(ret));
  } else if (tablet_id_array.at(1).index_tablet_id_array_.count() != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table id array 2 should have 3 tablets, 1 hidden table and 2 lob tablets", K(ret));
  }

  arg.id_= ls_id;
  arg.major_frozen_scn_.set_min();
  if (OB_FAIL(arg.table_schemas_.push_back(table_schema0))) {
    LOG_WARN("failed to push back table schema 0 ", K(ret));
  } else if (OB_FAIL(arg.table_schemas_.push_back(table_schema1))) {
    LOG_WARN("failed to push back table schema 1", K(ret));
  } else if (OB_FAIL(arg.table_schemas_.push_back(table_schema2))) {
    LOG_WARN("failed to push back table schema 2", K(ret));
  }

  if (OB_SUCC(ret)) {
    const TabletInfo &tablet_info = tablet_id_array.at(0);
    const ObTabletID &data_tablet_id = tablet_info.data_tablet_id_;
    const ObSArray<ObTabletID> &index_tablet_id_array = tablet_info.index_tablet_id_array_;

    obrpc::ObCreateTabletInfo create_tablet_info;
    create_tablet_info.data_tablet_id_ = data_tablet_id;
    create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
    create_tablet_info.is_create_bind_hidden_tablets_ = true;
    for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_id_array.count(); ++j) {
      const ObTabletID &index_tablet_id = index_tablet_id_array.at(j);
      if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(index_tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(index_tablet_id));
      } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(0))) {
        LOG_WARN("failed to push back schema index", K(ret));
      }
    }

    if (OB_FAIL(arg.tablets_.push_back(create_tablet_info))) {
      LOG_WARN("failed to push back create tablet info", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const TabletInfo &tablet_info = tablet_id_array.at(1);
    const ObTabletID &data_tablet_id = tablet_info.data_tablet_id_;
    const ObSArray<ObTabletID> &index_tablet_id_array = tablet_info.index_tablet_id_array_;

    obrpc::ObCreateTabletInfo create_tablet_info;
    create_tablet_info.data_tablet_id_ = data_tablet_id;
    create_tablet_info.compat_mode_ = lib::Worker::CompatMode::MYSQL;
    create_tablet_info.is_create_bind_hidden_tablets_ = false;

    const ObTabletID &lob_meta_tablet_id = index_tablet_id_array.at(0);
    const ObTabletID &lob_piece_tablet_id = index_tablet_id_array.at(1);
    if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(lob_meta_tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(lob_meta_tablet_id));
    } else if (OB_FAIL(create_tablet_info.tablet_ids_.push_back(lob_piece_tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(lob_piece_tablet_id));
    } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(1))) {
      LOG_WARN("failed to push back schema index", K(ret));
    } else if (OB_FAIL(create_tablet_info.table_schema_index_.push_back(2))) {
      LOG_WARN("failed to push back schema index", K(ret));
    }

    if (OB_FAIL(arg.tablets_.push_back(create_tablet_info))) {
      LOG_WARN("failed to push back create tablet info", K(ret));
    }
  }

  return ret;
}

int TestTabletCreateDeleteHelper::build_remove_tablet_arg(
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_id_array,
    ObBatchRemoveTabletArg &arg)
{
  int ret = OB_SUCCESS;

  arg.id_ = ls_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
    const ObTabletID &tablet_id = tablet_id_array.at(i);
    if (OB_FAIL(arg.tablet_ids_.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(arg));
  }

  return ret;
}

TEST_F(TestTabletCreateDeleteHelper, create_pure_data_tablets)
{
  int ret = OB_SUCCESS;
  ObSArray<ObTabletID> tablet_id_array;
  tablet_id_array.push_back(ObTabletID(1));
  tablet_id_array.push_back(ObTabletID(2));
  tablet_id_array.push_back(ObTabletID(3));

  ObBatchCreateTabletArg arg1;
  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);

  ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
      ls_id_, tablet_id_array, arg1);
  ASSERT_EQ(OB_SUCCESS, ret);

  {
    ObSArray<ObTabletCreateInfo> array;
    ret = ObTabletCreateDeleteHelper::verify_tablets_absence(arg1, array);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, array.count());
    ASSERT_TRUE(array[0].create_data_tablet_);
    ASSERT_EQ(ObTabletID(1), array[0].data_tablet_id_);
    ASSERT_TRUE(array[0].index_tablet_id_array_.empty());
    ASSERT_TRUE(array[1].create_data_tablet_);
    ASSERT_EQ(ObTabletID(2), array[1].data_tablet_id_);
    ASSERT_TRUE(array[1].index_tablet_id_array_.empty());
    ASSERT_TRUE(array[2].create_data_tablet_);
    ASSERT_EQ(ObTabletID(3), array[2].data_tablet_id_);
    ASSERT_TRUE(array[2].index_tablet_id_array_.empty());
  }

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  ret = ls_tablet_service.on_prepare_create_tablets(arg1, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check
  {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::CREATING, tx_data.tablet_status_);

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::CREATING, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::CREATING, tx_data.tablet_status_);
  }

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(3, pinned_tablet_set.size());

  trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
  ret = ls_tablet_service.on_commit_create_tablets(arg1, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  // check
  {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
  }

  ObBatchCreateTabletArg arg2;
  tablet_id_array.reset();
  tablet_id_array.push_back(ObTabletID(4));
  ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
      ls_id_, tablet_id_array, arg2);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.tx_id_ = 2;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);

  ret = ls_tablet_service.on_prepare_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, pinned_tablet_set.size());

  ret = ls_tablet_service.on_redo_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, pinned_tablet_set.size());

  trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
  ret = ls_tablet_service.on_commit_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  // check
  {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;
    key.tablet_id_ = 4;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
  }
}

TEST_F(TestTabletCreateDeleteHelper, create_mixed_tablets)
{
  int ret = OB_SUCCESS;
  ObSArray<TabletInfo> array;
  TabletInfo info1;
  info1.data_tablet_id_ = 1;
  info1.index_tablet_id_array_.push_back(ObTabletID(2));
  info1.index_tablet_id_array_.push_back(ObTabletID(3));
  TabletInfo info2;
  info2.data_tablet_id_ = 100;
  array.push_back(info1);
  array.push_back(info2);

  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
      ls_id_, array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
  {
    ObSArray<ObTabletCreateInfo> info_array;
    ret = ObTabletCreateDeleteHelper::verify_tablets_absence(arg, info_array);
    ASSERT_EQ(2, info_array.count());
    ASSERT_TRUE(info_array[0].create_data_tablet_);
    ASSERT_EQ(1, info_array[0].data_tablet_id_.id_);
    ASSERT_EQ(2, info_array[0].index_tablet_id_array_.count());
    ASSERT_EQ(2, info_array[0].index_tablet_id_array_[0].id_);
    ASSERT_EQ(3, info_array[0].index_tablet_id_array_[1].id_);
    ASSERT_TRUE(info_array[1].create_data_tablet_);
    ASSERT_EQ(100, info_array[1].data_tablet_id_.id_);
    ASSERT_EQ(0, info_array[1].index_tablet_id_array_.count());
  }

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
  ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  // check
  {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());
  }
}

TEST_F(TestTabletCreateDeleteHelper, two_level_create_arg)
{
  int ret = OB_SUCCESS;

  ObBatchCreateTabletArg arg1;
  ObBatchCreateTabletArg arg2;

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(1));

    ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
        ls_id_, tablet_id_array, arg1);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = 1;
    info1.index_tablet_id_array_.push_back(ObTabletID(10));
    TabletInfo info2;
    info2.data_tablet_id_ = 10;
    info2.index_tablet_id_array_.push_back(ObTabletID(100));
    array.push_back(info1);
    array.push_back(info2);

    ret = TestTabletCreateDeleteHelper::build_create_pure_index_tablet_arg(
        ls_id_, array, arg2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // mock hidden tablets
  arg2.tablets_[0].is_create_bind_hidden_tablets_ = true;
  arg2.tablets_[1].is_create_bind_hidden_tablets_ = false;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_.convert_for_logservice(100);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create data tablet
  ret = ls_tablet_service.on_prepare_create_tablets(arg1, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service.on_redo_create_tablets(arg1, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.scn_ = share::SCN::scn_inc(trans_flags.scn_);
  ret = ls_tablet_service.on_tx_end_create_tablets(arg1, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.scn_ = share::SCN::scn_inc(trans_flags.scn_);
  ret = ls_tablet_service.on_commit_create_tablets(arg1, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create aux tablets, tow level
  trans_flags.tx_id_ = 2;
  trans_flags.scn_.convert_for_logservice(200);

  ret = ls_tablet_service.on_prepare_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service.on_redo_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.scn_ = share::SCN::scn_inc(trans_flags.scn_);
  ret = ls_tablet_service.on_tx_end_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.scn_ = share::SCN::scn_inc(trans_flags.scn_);
  ret = ls_tablet_service.on_commit_create_tablets(arg2, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletCreateDeleteHelper, slog_create_partital_tablets_and_replay)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  // mock replay slog, create tablet 1 and tablet 2
  // status is DELETED
  {
    ObSArray<TabletInfo> array;
    TabletInfo info;
    info.data_tablet_id_ = 1;
    info.index_tablet_id_array_.push_back(ObTabletID(2));
    array.push_back(info);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, pinned_tablet_set.size());

    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());

    // set status to DELETED
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    ObTabletTxMultiSourceDataUnit tx_data;
    tx_data.tx_id_ = ObTabletCommon::FINAL_TX_ID;
    uint64_t val = OB_MAX_SCN_TS_NS  - 100;
    tx_data.tx_scn_.convert_for_logservice(val);
    tx_data.tablet_status_ = ObTabletStatus::DELETED;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    ret = tablet->set_tx_data(tx_data, true/*for_replay*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    ret = tablet->set_tx_data(tx_data, true/*for_replay*/);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // replay clog create tablets
  ObSArray<TabletInfo> array;
  TabletInfo info;
  info.data_tablet_id_ = 1;
  info.index_tablet_id_array_.push_back(ObTabletID(2));
  info.index_tablet_id_array_.push_back(ObTabletID(3));
  array.push_back(info);

  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
      ls_id_, array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  const share::SCN val_1 = share::SCN::minus(share::SCN::max_scn(), 100);
  const share::SCN val_2 = share::SCN::minus(share::SCN::max_scn(), 200);
  trans_flags.scn_ = val_2; // log ts should be smaller than that in existed tablets

  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, pinned_tablet_set.size()); // only tablet 3 is created

  ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, pinned_tablet_set.size());
  trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
  ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  // check
  {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(val_1, tx_data.tx_scn_);

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(val_1, tx_data.tx_scn_);

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(val_2.get_val_for_logservice()+1, tx_data.tx_scn_.get_val_for_logservice()); // log ts inc 1 when commit
  }
}

TEST_F(TestTabletCreateDeleteHelper, create_pure_index_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  {
    ObBatchCreateTabletArg arg;
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = 1;
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = 100;
    array.push_back(info1);
    array.push_back(info2);

    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, pinned_tablet_set.size());

    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());
  }

  {
    ObBatchCreateTabletArg arg;
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = 1;
    info1.index_tablet_id_array_.push_back(ObTabletID(4));
    TabletInfo info2;
    info2.data_tablet_id_ = 100;
    info2.index_tablet_id_array_.push_back(ObTabletID(200));
    info2.index_tablet_id_array_.push_back(ObTabletID(201));
    array.push_back(info1);
    array.push_back(info2);

    ret = TestTabletCreateDeleteHelper::build_create_pure_index_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, pinned_tablet_set.size());

    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, pinned_tablet_set.size());
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());
  }

  // check
  {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());
    ASSERT_EQ(0, tx_data.tx_id_);

    ObTabletBindingInfo ddl_data;
    ret = tablet_handle.get_obj()->get_ddl_data(ddl_data);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(0, tx_data.tx_id_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(0, tx_data.tx_id_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 4;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(0, tx_data.tx_id_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());
    ASSERT_EQ(0, tx_data.tx_id_);

    ddl_data.reset();
    ret = tablet_handle.get_obj()->get_ddl_data(ddl_data);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = ObTabletID(200);
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(0, tx_data.tx_id_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = ObTabletID(201);
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(0, tx_data.tx_id_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());
  }
}

TEST_F(TestTabletCreateDeleteHelper, abort_create_tablets)
{
  int ret = OB_SUCCESS;
  ObSArray<TabletInfo> array;
  TabletInfo info1;
  info1.data_tablet_id_ = 1;
  info1.index_tablet_id_array_.push_back(ObTabletID(2));
  info1.index_tablet_id_array_.push_back(ObTabletID(3));
  TabletInfo info2;
  info2.data_tablet_id_ = 100;
  array.push_back(info1);
  array.push_back(info2);

  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
      ls_id_, array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
  {
    ObSArray<ObTabletCreateInfo> info_array;
    ret = ObTabletCreateDeleteHelper::verify_tablets_absence(arg, info_array);
    ASSERT_EQ(2, info_array.count());
    ASSERT_TRUE(info_array[0].create_data_tablet_);
    ASSERT_EQ(1, info_array[0].data_tablet_id_.id_);
    ASSERT_EQ(2, info_array[0].index_tablet_id_array_.count());
    ASSERT_EQ(2, info_array[0].index_tablet_id_array_[0].id_);
    ASSERT_EQ(3, info_array[0].index_tablet_id_array_[1].id_);
    ASSERT_TRUE(info_array[1].create_data_tablet_);
    ASSERT_EQ(100, info_array[1].data_tablet_id_.id_);
    ASSERT_EQ(0, info_array[1].index_tablet_id_array_.count());
  }

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
  ret = ls_tablet_service.on_abort_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());
}

TEST_F(TestTabletCreateDeleteHelper, abort_create_tablets_no_redo)
{
  int ret = OB_SUCCESS;
  ObSArray<TabletInfo> array;
  TabletInfo info1;
  info1.data_tablet_id_ = 1;
  info1.index_tablet_id_array_.push_back(ObTabletID(2));
  info1.index_tablet_id_array_.push_back(ObTabletID(3));
  TabletInfo info2;
  info2.data_tablet_id_ = 100;
  array.push_back(info1);
  array.push_back(info2);

  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
      ls_id_, array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::invalid_scn();
  trans_flags.redo_submitted_ = true;
  trans_flags.redo_synced_ = true;

  // mock, no prepare(as if prepare create failed)
  // and log ts is invalid
  ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_service.on_abort_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  // mock, no prepare(as if prepare create failed)
  // but log ts is valid
  ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  trans_flags.scn_.convert_for_logservice(123);
  ret = ls_tablet_service.on_abort_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());

  // mock prepare succeeded
  // log ts is valid
  trans_flags.scn_ = share::SCN::invalid_scn();
  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  trans_flags.scn_.convert_for_logservice(888);
  ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
  ret = ls_tablet_service.on_abort_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());
}

TEST_F(TestTabletCreateDeleteHelper, remove_pure_data_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 200);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  // create tablet
  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(1));
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(3));

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, pinned_tablet_set.size());

    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());
  }

  // remove tablet
  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(1));
    tablet_id_array.push_back(ObTabletID(2));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, pinned_tablet_set.size());

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());

    // check
    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 2;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
  }
}

TEST_F(TestTabletCreateDeleteHelper, remove_mixed_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 200);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(6, pinned_tablet_set.size());

    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(6, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(100));
    tablet_id_array.push_back(ObTabletID(101));
    tablet_id_array.push_back(ObTabletID(102));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, pinned_tablet_set.size());

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());


    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());

    // check
    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());
  }
}

TEST_F(TestTabletCreateDeleteHelper, remove_pure_index_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 200);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = 1;
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    info1.index_tablet_id_array_.push_back(ObTabletID(4));
    TabletInfo info2;
    info2.data_tablet_id_ = 100;
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(7, pinned_tablet_set.size());

    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(7, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(3));
    tablet_id_array.push_back(ObTabletID(101));
    tablet_id_array.push_back(ObTabletID(102));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, pinned_tablet_set.size());

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    tx_data.reset();
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    ObTabletBindingInfo ddl_data;
    ret = tablet_handle.get_obj()->get_ddl_data(ddl_data);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 4;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    ddl_data.reset();
    ret = tablet_handle.get_obj()->get_ddl_data(ddl_data);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, pinned_tablet_set.size());

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, pinned_tablet_set.size());

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    ddl_data.reset();
    ret = tablet_handle.get_obj()->get_ddl_data(ddl_data);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 4;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    ddl_data.reset();
    ret = tablet_handle.get_obj()->get_ddl_data(ddl_data);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
  }
}

TEST_F(TestTabletCreateDeleteHelper, remove_unbatched_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  const share::SCN val_2 = share::SCN::minus(share::SCN::max_scn(), 200);
  trans_flags.scn_ = val_2;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObSArray<ObTabletID> tablet_id_array1;
    ObSArray<ObTabletID> tablet_id_array2;
    tablet_id_array1.push_back(ObTabletID(2));
    tablet_id_array1.push_back(ObTabletID(3));
    tablet_id_array2.push_back(ObTabletID(1));

    ObBatchRemoveTabletArg arg1;
    ObBatchRemoveTabletArg arg2;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array1, arg1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array2, arg2);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    const share::SCN val_1 = share::SCN::minus(share::SCN::max_scn(), 100);
    trans_flags.scn_ = val_1;
    ret = ls_tablet_service.on_prepare_remove_tablets(arg1, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_prepare_remove_tablets(arg2, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

    ret = ls_tablet_service.on_redo_remove_tablets(arg1, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_redo_remove_tablets(arg2, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_remove_tablets(arg1, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_commit_remove_tablets(arg2, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);
  }
}

TEST_F(TestTabletCreateDeleteHelper, roll_back_prepare_remove_tablet)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.tx_id_ = 1;
  const share::SCN val = share::SCN::minus(share::SCN::max_scn(), 300);
  trans_flags.scn_ = val;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(1));
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(3));

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // remove tablet
  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(1));
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(4));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    const share::SCN val = share::SCN::minus(share::SCN::max_scn(), 200);
    trans_flags.scn_ = val;
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_NE(OB_SUCCESS, ret);

    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ret = ls_tablet_service.get_tablet(ObTabletID(1), tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ret = ls_tablet_service.get_tablet(ObTabletID(2), tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_NE(OB_SUCCESS, ret);
  }
}

TEST_F(TestTabletCreateDeleteHelper, abort_remove_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::invalid_scn();
  trans_flags.redo_synced_ = true;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    const share::SCN val_1 = share::SCN::minus(share::SCN::max_scn(), 100);
    const share::SCN val_2 = share::SCN::minus(share::SCN::max_scn(), 200);
    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = val_1;
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = val_1;
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(100));
    tablet_id_array.push_back(ObTabletID(101));
    tablet_id_array.push_back(ObTabletID(102));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    const share::SCN val = share::SCN::minus(share::SCN::max_scn(), 100);
    trans_flags.scn_ = val;
    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_tx_end_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = val;
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(trans_flags.scn_, tx_data.tx_scn_);

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(trans_flags.scn_, tx_data.tx_scn_);

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(trans_flags.scn_, tx_data.tx_scn_);

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_EQ(trans_flags.scn_, tx_data.tx_scn_);
  }
}

TEST_F(TestTabletCreateDeleteHelper, abort_remove_tablets_no_redo)
{
  int ret = OB_SUCCESS;

  int64_t init_log_ts = OB_INVALID_TIMESTAMP;
  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::invalid_scn();
  trans_flags.redo_synced_ = false;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    const share::SCN val = share::SCN::minus(share::SCN::max_scn(), 100);
    trans_flags.scn_ = val;
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = val;
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    init_log_ts = trans_flags.scn_.get_val_for_logservice();
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(100));
    tablet_id_array.push_back(ObTabletID(101));
    tablet_id_array.push_back(ObTabletID(102));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_tx_end_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::invalid_scn();
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(init_log_ts, tx_data.tx_scn_.get_val_for_logservice());

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(init_log_ts, tx_data.tx_scn_.get_val_for_logservice());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(init_log_ts, tx_data.tx_scn_.get_val_for_logservice());
  }
}

TEST_F(TestTabletCreateDeleteHelper, replay_abort_remove_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
  trans_flags.redo_synced_ = true;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(100));
    tablet_id_array.push_back(ObTabletID(101));
    tablet_id_array.push_back(ObTabletID(102));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());


    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTabletCreateDeleteHelper, partial_prepare_remove_and_full_abort_remove)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(100));
    tablet_id_array.push_back(ObTabletID(101));

    ObBatchRemoveTabletArg partial_arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, partial_arg);
    ASSERT_EQ(OB_SUCCESS, ret);


    tablet_id_array.push_back(ObTabletID(102));
    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 95);
    // mock partial prepare remove
    ret = ls_tablet_service.on_prepare_remove_tablets(partial_arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(trans_flags.tx_id_, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    // tablet 102 has not been touched in prepare remove procedure
    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 90);
    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    // use full arg to do abort remove
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTabletCreateDeleteHelper, abort_create_tablets_for_switch_leader)
{
  int ret = OB_SUCCESS;
  ObSArray<TabletInfo> array;
  TabletInfo info1;
  info1.data_tablet_id_ = 1;
  info1.index_tablet_id_array_.push_back(ObTabletID(2));
  info1.index_tablet_id_array_.push_back(ObTabletID(3));
  TabletInfo info2;
  info2.data_tablet_id_ = 100;
  array.push_back(info1);
  array.push_back(info2);

  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
      ls_id_, array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = SCN::minus(SCN::max_scn(), 100);
  {
    ObSArray<ObTabletCreateInfo> info_array;
    ret = ObTabletCreateDeleteHelper::verify_tablets_absence(arg, info_array);
    ASSERT_EQ(2, info_array.count());
    ASSERT_TRUE(info_array[0].create_data_tablet_);
    ASSERT_EQ(1, info_array[0].data_tablet_id_.id_);
    ASSERT_EQ(2, info_array[0].index_tablet_id_array_.count());
    ASSERT_EQ(2, info_array[0].index_tablet_id_array_[0].id_);
    ASSERT_EQ(3, info_array[0].index_tablet_id_array_[1].id_);
    ASSERT_TRUE(info_array[1].create_data_tablet_);
    ASSERT_EQ(100, info_array[1].data_tablet_id_.id_);
    ASSERT_EQ(0, info_array[1].index_tablet_id_array_.count());
  }

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // check t3m
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr::PinnedTabletSet &pinned_tablet_set = t3m->pinned_tablet_set_;
  ASSERT_EQ(0, pinned_tablet_set.size());

  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, pinned_tablet_set.size());

  trans_flags.scn_ = SCN::minus(SCN::max_scn(), 99);
  trans_flags.redo_synced_ = false;
  trans_flags.for_replay_ = true;
  ret = ls_tablet_service.on_abort_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, pinned_tablet_set.size());
}

TEST_F(TestTabletCreateDeleteHelper, abort_remove_tablets_for_switch_leader)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_.reset();

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(100);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_.reset();
    ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 99);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(2));
    tablet_id_array.push_back(ObTabletID(100));
    tablet_id_array.push_back(ObTabletID(101));
    tablet_id_array.push_back(ObTabletID(102));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.tx_id_ = 2;
    trans_flags.scn_.reset();
    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id_;

    key.tablet_id_ = 1;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 3;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    trans_flags.redo_synced_ = false;
    trans_flags.redo_submitted_ = false;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 50);
    trans_flags.for_replay_ = true;
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    key.tablet_id_ = 2;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, tx_data.get_unsync_cnt_for_multi_data());
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 100;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, tx_data.get_unsync_cnt_for_multi_data());
    ASSERT_TRUE(tablet_handle.get_obj()->is_data_tablet());

    key.tablet_id_ = 101;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, tx_data.get_unsync_cnt_for_multi_data());
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());

    key.tablet_id_ = 102;
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_tx_data(tx_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, tx_data.get_unsync_cnt_for_multi_data());
    ASSERT_TRUE(tablet_handle.get_obj()->is_local_index_tablet());
  }
}

TEST_F(TestTabletCreateDeleteHelper, get_tablet_with_timeout)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletID tablet_id(101);
  ObTabletMapKey key(ls_id_, tablet_id);
  ObTabletHandle tablet_handle;
  ret = t3m->acquire_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, tablet_handle, false/*only_acquire*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle handle;
  ret = ObTabletCreateDeleteHelper::get_tablet(key, handle);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
}

TEST_F(TestTabletCreateDeleteHelper, migrate_lob_tablets)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 888);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create data tablet
  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(1));

    ObBatchCreateTabletArg arg;

    ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // mock lob tablet already exists, but data tablet does not exist
  // this scene occurs when migration happens
  {
    ObTableSchema table_schema1;
    ObTableSchema table_schema2;
    TestSchemaUtils::prepare_data_schema(table_schema1);
    table_schema1.table_type_ = ObTableType::AUX_LOB_META;
    TestSchemaUtils::prepare_data_schema(table_schema2);
    table_schema2.table_type_ = ObTableType::AUX_LOB_PIECE;

    trans_flags.for_replay_ = true;
    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 666);

    ObTabletCreateDeleteHelper helper(*ls, ls_tablet_service.tablet_id_set_);
    ObBatchCreateTabletArg arg;
    arg.id_ = ls_id_;
    arg.major_frozen_scn_.set_min();
    ObTabletHandle tablet_handle;
    // lob meta tablet
    const ObSArray<ObTabletID> index_tablet_array;
    const ObTabletID tablet_id(2);
    const ObTabletID lob_meta_tablet_id(101);
    ret = helper.do_create_tablet(lob_meta_tablet_id, tablet_id, lob_meta_tablet_id, ObTabletID(),
        index_tablet_array, arg, trans_flags, table_schema1, lib::Worker::CompatMode::MYSQL, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    // reset tx data to normal state
    tablet_handle.get_obj()->tablet_meta_.tx_data_.tx_id_ = 0;
    tablet_handle.get_obj()->tablet_meta_.tx_data_.tablet_status_ = ObTabletStatus::NORMAL;
    tablet_handle.get_obj()->tablet_meta_.tx_data_.tx_scn_ = share::SCN::minus(share::SCN::max_scn(), 98);

    const ObTabletID lob_piece_tablet_id(102);
    ret = helper.do_create_tablet(lob_piece_tablet_id, tablet_id, ObTabletID(), lob_piece_tablet_id,
        index_tablet_array, arg, trans_flags, table_schema2, lib::Worker::CompatMode::MYSQL, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    // reset tx data to normal state
    tablet_handle.get_obj()->tablet_meta_.tx_data_.tx_id_ = 0;
    tablet_handle.get_obj()->tablet_meta_.tx_data_.tablet_status_ = ObTabletStatus::NORMAL;
    tablet_handle.get_obj()->tablet_meta_.tx_data_.tx_scn_ = share::SCN::minus(share::SCN::max_scn(), 98);
  }

  // create tablet
  {
    ObSArray<TabletInfo> array;
    // hidden tablets
    TabletInfo info1;
    info1.data_tablet_id_ = ObTabletID(1);
    info1.index_tablet_id_array_.push_back(ObTabletID(2));
    info1.index_tablet_id_array_.push_back(ObTabletID(3));

    // lob tablets
    TabletInfo info2;
    info2.data_tablet_id_ = ObTabletID(2);
    info2.index_tablet_id_array_.push_back(ObTabletID(101));
    info2.index_tablet_id_array_.push_back(ObTabletID(102));
    array.push_back(info1);
    array.push_back(info2);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_hidden_and_lob_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    // mock lob tablets
    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 666);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 99);;
    ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 98);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // check binding info
    ObTabletHandle tablet_handle;
    ObTabletBindingInfo binding_info;
    const ObTabletMapKey key(ls_id_, ObTabletID(2));
    ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_handle.get_obj()->get_ddl_data(binding_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(101, binding_info.lob_meta_tablet_id_.id());
    ASSERT_EQ(102, binding_info.lob_piece_tablet_id_.id());
  }
}

TEST_F(TestTabletCreateDeleteHelper, tablet_not_exist_commit)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = true;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 888);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  // create data tablet
  {
    ObSArray<TabletInfo> array;
    TabletInfo info;
    info.data_tablet_id_ = 1;
    info.index_tablet_id_array_.push_back(ObTabletID(2));
    array.push_back(info);

    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_mixed_tablet_arg(
        ls_id_, array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    // skip prepare and redo procedure
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTabletCreateDeleteHelper, force_kill_create_tablet_tx)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.redo_submitted_ = false;
  trans_flags.redo_synced_ = false;
  trans_flags.is_force_kill_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::invalid_scn();

  ObSArray<ObTabletID> tablet_id_array;
  tablet_id_array.push_back(ObTabletID(100));
  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
      ls_id_, tablet_id_array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.redo_submitted_ = true;
  trans_flags.redo_synced_ = true;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
  ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mock transaction force killed
  trans_flags.is_force_kill_ = true;
  trans_flags.scn_.set_invalid();
  ret = ls_tablet_service.on_abort_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletCreateDeleteHelper, force_kill_remove_tablet_tx)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.redo_submitted_ = false;
  trans_flags.redo_synced_ = false;
  trans_flags.is_force_kill_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::invalid_scn();

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(100));
    ObBatchCreateTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.redo_submitted_ = true;
    trans_flags.redo_synced_ = true;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100);
    ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1);
    ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // remove tablets
  {
    ObSArray<ObTabletID> tablet_id_array;
    tablet_id_array.push_back(ObTabletID(100));

    ObBatchRemoveTabletArg arg;
    ret = TestTabletCreateDeleteHelper::build_remove_tablet_arg(
        ls_id_, tablet_id_array, arg);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.for_replay_ = false;
    trans_flags.redo_submitted_ = false;
    trans_flags.redo_synced_ = false;
    trans_flags.tx_id_ = 2;
    trans_flags.scn_ = share::SCN::invalid_scn();

    ret = ls_tablet_service.on_prepare_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    trans_flags.redo_submitted_ = true;
    trans_flags.redo_synced_ = true;
    trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 90);
    ret = ls_tablet_service.on_redo_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);

    // mock transaction force killed
    trans_flags.is_force_kill_ = true;
    trans_flags.scn_.set_invalid();
    ret = ls_tablet_service.on_abort_remove_tablets(arg, trans_flags);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTabletCreateDeleteHelper, empty_memtable_replay_commit)
{
  int ret = OB_SUCCESS;

  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.for_replay_ = false;
  trans_flags.redo_submitted_ = false;
  trans_flags.redo_synced_ = false;
  trans_flags.is_force_kill_ = false;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::invalid_scn();

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;

  ObSArray<ObTabletID> tablet_id_array;
  tablet_id_array.push_back(ObTabletID(12306));
  ObBatchCreateTabletArg arg;
  ret = TestTabletCreateDeleteHelper::build_create_pure_data_tablet_arg(
      ls_id_, tablet_id_array, arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_tablet_service.on_prepare_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  trans_flags.redo_submitted_ = true;
  trans_flags.redo_synced_ = true;
  trans_flags.scn_.convert_for_gts(100);
  ret = ls_tablet_service.on_redo_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tablet_handle;
  const ObTabletMapKey key(ls_id_, ObTabletID(12306));
  ret = ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTableHandleV2 handle;
  memtable::ObIMemtable *memtable;
  ret = tablet_handle.get_obj()->get_active_memtable(handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = handle.get_memtable(memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ((memtable::ObMemtable*)memtable)->set_is_tablet_freeze();

  trans_flags.scn_ = share::SCN::max_scn();
  ret = ls_tablet_service.on_tx_end_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);
  trans_flags.for_replay_ = true;
  trans_flags.scn_.convert_for_gts(102);
  ret = ls_tablet_service.on_commit_create_tablets(arg, trans_flags);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 new_handle;
  memtable::ObIMemtable *new_memtable;
  ret = tablet_handle.get_obj()->get_active_memtable(new_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_handle.get_memtable(new_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN left_scn;
  left_scn.convert_for_gts(101);
  share::SCN right_scn = ((memtable::ObMemtable*)new_memtable)->get_max_end_scn();
  ASSERT_EQ(left_scn, right_scn);
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_create_delete_helper.log*");
  OB_LOGGER.set_file_name("test_tablet_create_delete_helper.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
