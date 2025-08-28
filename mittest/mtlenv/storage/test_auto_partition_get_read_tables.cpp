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
#define private public
#define protected public

#include "src/storage/ob_storage_struct.h"
#include "src/share/ob_rpc_struct.h"
#include "src/storage/meta_mem/ob_tablet_handle.h"
#include "src/storage/tx_storage/ob_ls_handle.h"
#include "src/storage/tx_storage/ob_ls_service.h"
#include "src/storage/tablet/ob_tablet.h"


#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/test_tablet_helper.h"
#include "storage/init_basic_struct.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "observer/ob_safe_destroy_thread.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace unittest
{

static uint64_t TEST_TENANT_ID = 1001;
static int64_t  TEST_LS_ID = 101;
static int64_t  TEST_SRC_TABLET_ID = 10001;
static int64_t  TEST_ORI_TABLET_ID = 10002;

class FakeObGetReadTables : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();

public:
  FakeObGetReadTables() : 
    snapshot_version_(0),
    allow_not_ready_(false),
    split_type_(ObTabletSplitType::MAX_TYPE)
  {}

  ~FakeObGetReadTables()
  {}

  int set_tablet_split_info(ObTabletID &src_tablet_id, ObTabletID &origin_tablet_id, ObLSID &ls_id);
  int gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, ObDatumRowkey &datum_rowkey);
  int64_t get_read_tables_count(ObTabletID &src_tablet_id);

  void set_snapshot_version(int64_t snapshot_version) { snapshot_version_ = snapshot_version;  }
  void set_split_type(ObTabletSplitType split_type) { split_type_ = split_type; }
  void set_allow_not_ready(bool need) { allow_not_ready_ = need; }

private:
  ObTabletTableIterator iterator_;
  static ObArenaAllocator allocator_;
  int64_t snapshot_version_;
  bool allow_not_ready_;
  ObTabletSplitType split_type_;
};

ObArenaAllocator FakeObGetReadTables::allocator_;

void FakeObGetReadTables::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  const ObLSID ls_id(TEST_LS_ID);
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ls_id, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t table_id = 12345;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));

  // create tablet_1
  ObTabletID src_tablet_id(TEST_SRC_TABLET_ID);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, src_tablet_id, table_schema, allocator_));

  // create tablet_2
  ObTabletID ori_tablet_id(TEST_ORI_TABLET_ID);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ori_tablet_id, table_schema, allocator_));
}

void FakeObGetReadTables::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);
  MockTenantModuleEnv::get_instance().destroy();
}

// return tables count
int64_t FakeObGetReadTables::get_read_tables_count(ObTabletID &src_tablet_id) 
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  int64_t tables_count = 0;
  ObLSID test_ls_id(TEST_LS_ID);

  if (OB_FAIL(MTL(ObLSService *)->get_ls(test_ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "fail to get log stream", K(ret), K(ls_handle));
  } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_read_tables(
      src_tablet_id, 
      snapshot_version_, 
      iterator_, 
      allow_not_ready_,
      true /* get split src table if need */))) {
    STORAGE_LOG(WARN, "fail to get tablet tables", K(ret), K(src_tablet_id));
  } else {
    if (!iterator_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid iterator.", K(ret), K(src_tablet_id));
    } else {
      tables_count = iterator_.table_iter()->count();
    }
  }

  return tables_count;
}

int FakeObGetReadTables::gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, ObDatumRowkey &datum_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *key_val_obj = NULL;
  ObRowkey rowkey;
  if (NULL == (key_val_obj = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * key_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "out of memory", K(ret));
  } else {
    for (int64_t i = 0; i < key_cnt; ++i) {
      key_val_obj[i].set_int(key_val);
      // key_val_obj[i].set_min();
    }
    rowkey.assign(key_val_obj, key_cnt);
    if (OB_FAIL(datum_rowkey.from_rowkey(rowkey, allocator_))) {
      STORAGE_LOG(WARN, "fail to from rowkey", K(ret));
    }
  }
  return ret;
}

int FakeObGetReadTables::set_tablet_split_info(ObTabletID &src_tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletSplitTscInfo split_info;
  ObLSHandle ls_handle;
  ObLSID test_ls_id(TEST_LS_ID);
  if (OB_FAIL(MTL(ObLSService *)->get_ls(test_ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "fail to get log stream", K(ret), K(ls_handle));
  } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(src_tablet_id, tablet_handle))) {
    STORAGE_LOG(WARN, "fail to get tablet", K(ret), K(src_tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet handle obj is null", K(ret), K(tablet_handle));
  } else if (OB_FAIL(gen_datum_rowkey(1, 1, split_info.start_key_))) {
    STORAGE_LOG(WARN, "gen start key fail.", K(ret));
  } else if (OB_FAIL(gen_datum_rowkey(2, 1, split_info.end_key_))) {
    STORAGE_LOG(WARN, "gen end key fail.", K(ret));
  } else {
    split_info.split_cnt_= 1;
    split_info.src_tablet_handle_ = tablet_handle;
    split_info.split_type_ = ObTabletSplitType::RANGE;
  }
  return ret;
}

TEST_F(FakeObGetReadTables, test_right_set_split_info)
{
  int ret = OB_SUCCESS;
  set_snapshot_version(INT64_MAX);
  set_allow_not_ready(false);

  ObTabletID src_tablet_id(TEST_SRC_TABLET_ID);
  ret = set_tablet_split_info(src_tablet_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = get_read_tables_count(src_tablet_id);
  ASSERT_EQ(ret, 2); // both src tablet and origin tables, major;
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_auto_partition_get_read_tables.log");
  OB_LOGGER.set_file_name("test_auto_partition_get_read_tables.log", true);
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_auto_partition_get_read_tables");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
