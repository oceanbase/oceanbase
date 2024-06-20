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
#include <iostream>
#include <codecvt>
#include <stdio.h>
#define private public
#define protected public

#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/lob/ob_lob_meta.h"
#include "lib/number/ob_number_v2.h"
#include "share/schema/ob_column_schema.h"
#include "share/ob_ls_id.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
namespace storage
{

class TestLobMetaIterator : public ::testing::Test
{
public:
  TestLobMetaIterator() = default;
  virtual ~TestLobMetaIterator() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();

  int fill_lob_sstable_slice_mock(
                          const ObLobId &lob_id,
                          const transaction::ObTransID &trans_id,
                          const int64_t trans_version,
                          const int64_t sql_no,
                          const bool has_lob_header,
                          const int64_t read_snapshot,
                          const ObCollationType collation_type,
                          blocksstable::ObStorageDatum &datum);
  int build_lob_data(ObObj &obj, std::string &st, common::ObArenaAllocator &allocator);
  int build_lob_data_not_common(ObObj &obj, std::string &str);
};

void TestLobMetaIterator::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestLobMetaIterator::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

int TestLobMetaIterator::fill_lob_sstable_slice_mock(
                          const ObLobId &lob_id,
                          const transaction::ObTransID &trans_id,
                          const int64_t trans_version,
                          const int64_t sql_no,
                          const bool has_lob_header,
                          const int64_t read_snapshot,
                          const ObCollationType collation_type,
                          blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t ils_id = 1001;
  int64_t itablet_id = 1001;
  common::ObArenaAllocator allocator;
  share::ObLSID ls_id(ils_id);
  common::ObTabletID tablet_id(itablet_id);
  const blocksstable::ObDatumRow *new_row = nullptr;
  ObLobMetaInfo lob_meta_info;
  const int64_t timeout_ts = ObTimeUtility::current_time();
  ObString data = datum.get_string();
  ObLobMetaWriteIter iter(data, &allocator, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
  ObLobMetaRowIterator row_iterator;
  if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(
    allocator, nullptr, ls_id, tablet_id, lob_id, collation_type, datum, timeout_ts, has_lob_header, iter))) {
    STORAGE_LOG(WARN, "fail to insert lob column", K(ret), K(ls_id), K(tablet_id), K(lob_id));
  } else if (OB_FAIL(row_iterator.init(&iter, trans_id, trans_version, sql_no))) {
    STORAGE_LOG(WARN, "fail to init lob meta row iterator", K(ret), K(trans_id), K(trans_version));
  } else if (OB_FAIL(row_iterator.get_next_row(new_row))) {
    STORAGE_LOG(WARN, "get_next_row failed", K(ret));
  } else if (OB_ISNULL(new_row)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "new_row is nullptr", K(ret));
  } else if (OB_FAIL(ObLobMetaUtil::transform_from_row_to_info(new_row, lob_meta_info, true))) {
    STORAGE_LOG(WARN, "transform failed", K(ret));
  } else if (lob_meta_info.lob_id_.lob_id_ != lob_id.lob_id_) {
    //STORAGE_LOG("lob_meta_info", K(lob_meta_info));
    STORAGE_LOG(WARN, "error info", K(ret), K(lob_meta_info.lob_id_.lob_id_), K(lob_id.lob_id_));
  }
  return ret;
}
int TestLobMetaIterator::build_lob_data(ObObj &obj, std::string &st, common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObLobCommon *value = NULL;
  void *buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLobCommon) + 1000000))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for ObLobData", K(ret));
  } else {
    // ObLobIndex index;
    value = new (buf) ObLobCommon();
    // value->version_ = 1;
    // value->reserve_ = 0;
    // value->is_init_ = 1;
    int64_t byte_size = 1000000;
    value->in_row_ = 1;
    MEMCPY(value->buffer_, st.c_str(), st.length());

    obj.meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
    obj.set_type(ObMediumTextType);
    obj.set_lob_value(ObMediumTextType, value, value->get_handle_size(byte_size));
    obj.set_has_lob_header();
  }
  return ret;
}

int TestLobMetaIterator::build_lob_data_not_common(ObObj &obj, std::string &str)
{
  int ret = OB_SUCCESS;
  obj.meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  obj.set_type(ObMediumTextType);
  obj.set_lob_value(ObMediumTextType, str.c_str(), str.length());
  return ret;
}

TEST_F(TestLobMetaIterator, test_not_lob_common)
{
  for (int x = 2; x <= 10; x++) {
    std::string st = "";
    for (int i = 0; i < 1000000; i++) {
      st += static_cast<char>(i % 26 + 'a');
    }
    ObDatumRow row;
    ObObj obj;
    ASSERT_EQ(OB_SUCCESS, build_lob_data_not_common(obj, st));
    ObStorageDatum datum;
    ASSERT_EQ(OB_SUCCESS, datum.from_obj_enhance(obj));
    transaction::ObTransID trans_id(10);
    ObLobId lob_id;
    lob_id.lob_id_ = x;
    lob_id.tablet_id_ = x;
    int64_t sql_no = 2;
    int64_t read_snapshot = 3;
    ObCollationType collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    ASSERT_EQ(OB_SUCCESS, fill_lob_sstable_slice_mock(lob_id, trans_id, 100, sql_no,
                                                      false, read_snapshot, collation_type, datum));
  }
}

TEST_F(TestLobMetaIterator, test_lob_common)
{
  for (int x = 2; x <= 10; x++) {
    std::string st = "";
    for (int i = 0; i < 1000000; i++) {
      st += static_cast<char>(i % 26 + 'a');
    }
    ObDatumRow row;
    ObObj obj;
    common::ObArenaAllocator allocator;
    ASSERT_EQ(OB_SUCCESS, build_lob_data(obj, st, allocator));
    ObStorageDatum datum;
    ASSERT_EQ(OB_SUCCESS, datum.from_obj_enhance(obj));
    transaction::ObTransID trans_id(10);
    ObLobId lob_id;
    lob_id.lob_id_ = x;
    lob_id.tablet_id_ = x;
    int64_t sql_no = 2;
    int64_t read_snapshot = 3;
    //ObLobMetaRowIterator iter;
    ObCollationType collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    ASSERT_EQ(OB_SUCCESS, fill_lob_sstable_slice_mock(lob_id, trans_id, 100, sql_no,
                                                      true, read_snapshot, collation_type, datum));
  }
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_lob_meta_iterator.log*");
  OB_LOGGER.set_file_name("test_lob_meta_iterator.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
