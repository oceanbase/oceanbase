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
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include <gtest/gtest.h>
#define private public
#define protected public
// #include "test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_define.h"

namespace oceanbase
{

namespace unittest
{

using namespace std;
using namespace sslog;

unittest::ObMockPalfKV PALF_KV;

class TestSSLogKVLogicalRow : public ::testing::Test
{
public:
  TestSSLogKVLogicalRow(){};
  virtual ~TestSSLogKVLogicalRow(){};
  virtual void SetUp() { PALF_KV.clear(); }
  virtual void TearDown() { PALF_KV.clear(); }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSSLogKVLogicalRow);
};

#define INIT_AND_INSERT_ROW(N, INSERT_RET)                                                     \
  do {                                                                                         \
    ObSSLogMetaType META_TYPE_##N = ObSSLogMetaType::SSLOG_LS_META;                            \
    std::string meta_key_str_##N("ID_META_" #N);                                               \
    ObString META_KEY_##N;                                                                     \
    META_KEY_##N.assign_ptr(meta_key_str_##N.c_str(), meta_key_str_##N.size());                \
                                                                                               \
    std::string meta_val_str_##N("ID_META_TEST_VAL_" #N);                                      \
    std::string extra_info_str_##N("ID_META_EXTRA_INFO_" #N);                                  \
    ObString META_VAL_##N;                                                                     \
    ObString EXTRA_INFO_##N;                                                                   \
    META_VAL_##N.assign_ptr(meta_val_str_##N.c_str(), meta_val_str_##N.size());                \
    EXTRA_INFO_##N.assign_ptr(extra_info_str_##N.c_str(), extra_info_str_##N.size());          \
                                                                                               \
    const bool IS_PREFIX_READ_##N = false;                                                     \
                                                                                               \
    ObSSLogTableLogicalRow logical_row_##N;                                                    \
    ASSERT_EQ(logical_row_##N.init(&PALF_KV, META_TYPE_##N, META_KEY_##N, IS_PREFIX_READ_##N), \
              OB_SUCCESS);                                                                     \
    ASSERT_EQ(logical_row_##N.insert_new_row(META_VAL_##N, EXTRA_INFO_##N), INSERT_RET);       \
  } while (0)

#define INIT_AND_UPDATE_ROW(KEY_ID, VAL_VERSION, UPDATE_PARAM)                                 \
  {                                                                                            \
    ObSSLogMetaType META_TYPE_##KEY_ID = ObSSLogMetaType::SSLOG_LS_META;                       \
    std::string meta_key_str_##KEY_ID("ID_META_" #KEY_ID);                                     \
    ObString META_KEY_##KEY_ID;                                                                \
    META_KEY_##KEY_ID.assign_ptr(meta_key_str_##KEY_ID.c_str(), meta_key_str_##KEY_ID.size()); \
                                                                                               \
    std::string meta_val_str_##KEY_ID("ID_META_TEST_VAL_" #VAL_VERSION);                       \
    std::string extra_info_str_##KEY_ID("ID_META_EXTRA_INFO_" #VAL_VERSION);                   \
    ObString META_VAL_##KEY_ID;                                                                \
    ObString EXTRA_INFO_##KEY_ID;                                                              \
    META_VAL_##KEY_ID.assign_ptr(meta_val_str_##KEY_ID.c_str(), meta_val_str_##KEY_ID.size()); \
    EXTRA_INFO_##KEY_ID.assign_ptr(extra_info_str_##KEY_ID.c_str(),                            \
                                   extra_info_str_##KEY_ID.size());                            \
                                                                                               \
    const bool IS_PREFIX_READ_##KEY_ID = false;                                                \
                                                                                               \
    ObSSLogTableLogicalRow logical_row_##KEY_ID;                                               \
    ASSERT_EQ(logical_row_##KEY_ID.init(&PALF_KV, META_TYPE_##KEY_ID, META_KEY_##KEY_ID,       \
                                        IS_PREFIX_READ_##KEY_ID),                              \
              OB_SUCCESS);                                                                     \
    ASSERT_EQ(                                                                                 \
        logical_row_##KEY_ID.update_row(UPDATE_PARAM, META_VAL_##KEY_ID, EXTRA_INFO_##KEY_ID), \
        OB_SUCCESS);                                                                           \
  }

TEST_F(TestSSLogKVLogicalRow, basic_insert)
{
  // ObSSLogMetaType META_TYPE = ObSSLogMetaType::SSLOG_LS_META;
  // std::string meta_key_str("ID_META");
  // ObString META_KEY;
  // META_KEY.assign_ptr(meta_key_str.c_str(), meta_key_str.size());
  //
  // std::string meta_val_str("ID_META_TEST_VAL");
  // std::string extra_info_str("ID_META_EXTRA_INFO");
  // ObString META_VAL;
  // ObString EXTRA_INFO;
  // META_VAL.assign_ptr(meta_val_str.c_str(), meta_val_str.size());
  // EXTRA_INFO.assign_ptr(extra_info_str.c_str(), extra_info_str.size());
  //
  // const bool IS_PREFIX_READ = false;
  //
  // ObSSLogTableLogicalRow logical_row;
  //
  // ASSERT_EQ(logical_row.init(&PALF_KV, META_TYPE, META_KEY, IS_PREFIX_READ), OB_SUCCESS);
  // ASSERT_EQ(logical_row.insert_new_row(META_VAL, EXTRA_INFO), OB_SUCCESS);
  // ASSERT_EQ(logical_row.insert_new_row(META_VAL, EXTRA_INFO), OB_ENTRY_EXIST);

  INIT_AND_INSERT_ROW(1, OB_SUCCESS);
  INIT_AND_INSERT_ROW(1, OB_ENTRY_EXIST);

  PALF_KV.print_all_kv("INSERT_NEW_ROW");
}

TEST_F(TestSSLogKVLogicalRow, basic_update)
{
  INIT_AND_INSERT_ROW(1, OB_SUCCESS);

  std::string extra_info_for_check("ID_META_EXTRA_INFO_V2");
  ObString EXTRA_INFO_FOR_CHECK;
  EXTRA_INFO_FOR_CHECK.assign_ptr(extra_info_for_check.c_str(), extra_info_for_check.length());
  ObSSLogWriteParam update_param(false, false, EXTRA_INFO_FOR_CHECK);
  INIT_AND_UPDATE_ROW(1, 2, update_param);

  INIT_AND_UPDATE_ROW(1, 3, update_param);

  INIT_AND_UPDATE_ROW(1, 4, update_param);

  // ObSSLogMetaType META_TYPE = ObSSLogMetaType::SSLOG_LS_META;
  // std::string meta_key_str("ID_META_1");
  // ObString META_KEY;
  // META_KEY.assign_ptr(meta_key_str.c_str(), meta_key_str.size());
  //
  // std::string meta_val_str("ID_META_TEST_VAL");
  // std::string meta_val_str_V2("ID_META_TEST_VAL_V2");
  // std::string meta_val_str_V3("ID_META_TEST_VAL_V3");
  // std::string meta_val_str_V4("ID_META_TEST_VAL_V4");
  // std::string extra_info_str("ID_META_EXTRA_INFO");
  // std::string extra_info_str_V2("ID_META_EXTRA_INFO_V2");
  // std::string extra_info_str_V4("ID_META_EXTRA_INFO_V4");
  // ObString META_VAL;
  // ObString META_VAL_V2;
  // ObString META_VAL_V3;
  // ObString META_VAL_V4;
  // ObString EXTRA_INFO;
  // ObString EXTRA_INFO_V2;
  // ObString EXTRA_INFO_V4;
  // META_VAL.assign_ptr(meta_val_str.c_str(), meta_val_str.size());
  // META_VAL_V2.assign_ptr(meta_val_str_V2.c_str(), meta_val_str_V2.size());
  // META_VAL_V3.assign_ptr(meta_val_str_V3.c_str(), meta_val_str_V3.size());
  // META_VAL_V4.assign_ptr(meta_val_str_V4.c_str(), meta_val_str_V4.size());
  // EXTRA_INFO.assign_ptr(extra_info_str.c_str(), extra_info_str.size());
  // EXTRA_INFO_V2.assign_ptr(extra_info_str_V2.c_str(), extra_info_str_V2.size());
  // EXTRA_INFO_V4.assign_ptr(extra_info_str_V4.c_str(), extra_info_str_V4.size());
  //
  // const bool IS_PREFIX_READ = false;
  //
  // ObSSLogTableLogicalRow logical_row;
  //
  // ASSERT_EQ(logical_row.init(&PALF_KV, META_TYPE, META_KEY, IS_PREFIX_READ), OB_SUCCESS);
  // ASSERT_EQ(logical_row.insert_new_row(META_VAL, EXTRA_INFO), OB_ENTRY_EXIST);
  //
  // std::string extra_info_for_check("ID_META_EXTRA_INFO_V2");
  // ObString EXTRA_INFO_FOR_CHECK;
  // EXTRA_INFO_FOR_CHECK.assign_ptr(extra_info_for_check.c_str(), extra_info_for_check.length());
  // ObSSLogWriteParam update_param(false, false, EXTRA_INFO_FOR_CHECK);
  //
  // ASSERT_EQ(logical_row.update_row(update_param, META_VAL_V2, EXTRA_INFO_V2), OB_SUCCESS);
  // ASSERT_EQ(logical_row.update_row(update_param, META_VAL_V3, EXTRA_INFO), OB_SUCCESS);
  // ASSERT_EQ(logical_row.update_row(update_param, META_VAL_V4, EXTRA_INFO_V4), OB_SUCCESS);

  PALF_KV.print_all_kv("UPDATE_VAL");
}

TEST_F(TestSSLogKVLogicalRow, basic_delete)
{

  INIT_AND_INSERT_ROW(1, OB_SUCCESS);

  std::string extra_info_for_check("ID_META_EXTRA_INFO_V2");
  ObString EXTRA_INFO_FOR_CHECK;
  EXTRA_INFO_FOR_CHECK.assign_ptr(extra_info_for_check.c_str(), extra_info_for_check.length());
  ObSSLogWriteParam update_param(false, false, EXTRA_INFO_FOR_CHECK);
  INIT_AND_UPDATE_ROW(1, 2, update_param);

  INIT_AND_UPDATE_ROW(1, 3, update_param);

  INIT_AND_UPDATE_ROW(1, 4, update_param);

  ObSSLogMetaType META_TYPE = ObSSLogMetaType::SSLOG_LS_META;
  std::string meta_key_str("ID_META_1");
  ObString META_KEY;
  META_KEY.assign_ptr(meta_key_str.c_str(), meta_key_str.size());
  const bool IS_PREFIX_READ = false;

  ObSSLogTableLogicalRow logical_row;
  ASSERT_EQ(logical_row.init(&PALF_KV, META_TYPE, META_KEY, IS_PREFIX_READ), OB_SUCCESS);
  ASSERT_EQ(logical_row.delete_row(), OB_SUCCESS);

  PALF_KV.print_all_kv("DELETE ROW");
}

TEST_F(TestSSLogKVLogicalRow, basic_read)
{
  INIT_AND_INSERT_ROW(1, OB_SUCCESS);
  INIT_AND_INSERT_ROW(2, OB_SUCCESS);
  INIT_AND_INSERT_ROW(3, OB_SUCCESS);
  INIT_AND_INSERT_ROW(4, OB_SUCCESS);
  INIT_AND_INSERT_ROW(5, OB_SUCCESS);

  PALF_KV.print_all_kv("BASIC_READ_ROW");

  char META_KEY_RES[ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN];
  char META_VAL_RES[ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN];
  char EXTRA_INFO_RES[ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN];
  ObString META_KEY_STR_RES;
  ObString META_VAL_STR_RES;
  ObString EXTRA_INFO_STR_RES;
  ObSSLogMetaType META_TYPE_RES;
  META_KEY_STR_RES.assign_ptr(META_KEY_RES, ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN);
  META_VAL_STR_RES.assign_ptr(META_VAL_RES, ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN);
  EXTRA_INFO_STR_RES.assign_ptr(EXTRA_INFO_RES, ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN);

  const bool IS_PREFIX_READ = false;
  ObSSLogTableLogicalRow logical_row;
  ObSSLogMetaType META_TYPE = ObSSLogMetaType::SSLOG_LS_META;
  std::string meta_key_str("ID_META");
  ObString META_KEY;
  META_KEY.assign_ptr(meta_key_str.c_str(), meta_key_str.size());

  ASSERT_EQ(logical_row.init(&PALF_KV, META_TYPE, META_KEY, IS_PREFIX_READ), OB_SUCCESS);

  share::SCN row_scn_res;
  ASSERT_EQ(logical_row.read_row(share::SCN::max_scn()), OB_SUCCESS);
  logical_row.get_row_scn(row_scn_res);
  logical_row.get_meta_type(META_TYPE_RES);
  logical_row.get_meta_key(META_KEY_STR_RES);
  logical_row.get_meta_value(META_VAL_STR_RES);
  logical_row.get_extra_info(EXTRA_INFO_STR_RES);

  // ASSERT_EQ(share::SCN::invalid_scn(), row_scn_res); TODO : fix with row_version
  ASSERT_EQ(ObSSLogMetaType::SSLOG_LS_META, META_TYPE_RES);
  ASSERT_TRUE(META_KEY_STR_RES.prefix_match(ObString("ID_META_")));
  ASSERT_TRUE(META_VAL_STR_RES.prefix_match(ObString("ID_META_TEST_VAL_")));
  ASSERT_TRUE(EXTRA_INFO_STR_RES.prefix_match(ObString("ID_META_EXTRA_INFO_")));

  TRANS_LOG(INFO, "basic read result", K(META_VAL_STR_RES), K(EXTRA_INFO_STR_RES),
            K(META_KEY_STR_RES), K(row_scn_res), K(META_TYPE_RES));
}

TEST_F(TestSSLogKVLogicalRow, basic_multi_version_read)
{
  INIT_AND_INSERT_ROW(1, OB_SUCCESS);
  INIT_AND_INSERT_ROW(2, OB_SUCCESS);
  INIT_AND_INSERT_ROW(3, OB_SUCCESS);
  INIT_AND_INSERT_ROW(4, OB_SUCCESS);
  INIT_AND_INSERT_ROW(5, OB_SUCCESS);

  PALF_KV.print_all_kv("BASIC_READ_ROW");

  char META_VAL_RES[ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN];
  char EXTRA_INFO_RES[ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN];
  ObString META_VAL_STR_RES;
  ObString EXTRA_INFO_STR_RES;
  META_VAL_STR_RES.assign_ptr(META_VAL_RES, ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN);
  EXTRA_INFO_STR_RES.assign_ptr(EXTRA_INFO_RES, ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN);

  const bool IS_PREFIX_READ = false;
  ObSSLogTableLogicalRow logical_row;
  ObSSLogMetaType META_TYPE = ObSSLogMetaType::SSLOG_LS_META;
  std::string meta_key_str("ID_META");
  ObString META_KEY;
  META_KEY.assign_ptr(meta_key_str.c_str(), meta_key_str.size());

  ASSERT_EQ(logical_row.init(&PALF_KV, META_TYPE, META_KEY, IS_PREFIX_READ), OB_SUCCESS);

  ObArray<ObSSLogTableLogicalRow *> logical_rows;
  ObSSLogMultiVersionReadParam param(share::SCN::max_scn(), share::SCN::min_scn(), false);

  ASSERT_EQ(logical_row.read_multi_version_row(param, logical_rows), OB_SUCCESS);

  for (int64_t i = 0; i < logical_rows.count(); i++) {
    TRANS_LOG(INFO, "basic multi_version read result", K(*logical_rows[i]));
    ASSERT_TRUE(
        logical_rows[i]->physical_key_.user_key_.meta_key_.prefix_match(ObString("ID_META")));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_sslog_kv_logical_row.log*");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_sslog_kv_logical_row.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
