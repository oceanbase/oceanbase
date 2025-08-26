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
#include <stdio.h>
#define private public
#define protected public
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_define.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#include "share/scn.h"
#include "test_mock_palf_kv.h"
#include "src/share/config/ob_server_config.h"

namespace oceanbase
{
using namespace std;
using namespace sslog;


namespace unittest
{

unittest::ObMockPalfKV PALF_KV;

class TestSSLogKVProxy : public ::testing::Test
{
public:
  TestSSLogKVProxy(){};
  virtual ~TestSSLogKVProxy(){};
  virtual void SetUp()
  {
    PALF_KV.clear();
    GCONF.cluster_id = 1;
  }
  virtual void TearDown()
  {
    PALF_KV.clear();
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSSLogKVProxy);
};

#define MULTI_VERSION_READ_INIT                                         \
  ObSSLogProxyGuard guard;                                              \
  char *key_buf = (char *)share::mtl_malloc(meta_key1.length()+1, "MITTEST"); \
  key_buf[meta_key1.length()] = '\0';                                   \
  ASSERT_EQ(true, key_buf != NULL);                                     \
  ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_PALF_KV, MTL_ID(), guard)); \
  sslog::ObISSLogProxy *proxy_ptr = guard.get_sslog_proxy();            \
  MEMCPY(key_buf, meta_key1.ptr(), meta_key1.length());                 \
  const common::ObString meta_key_malloc(meta_key1.length(), key_buf);  \
  ObSSLogIteratorGuard iter(false/*read_unfinish*/, true/*read_mark_delete*/);

#define SSLOG_TABLE_READ_INIT                                           \
  ObSSLogIteratorGuard iter(true/*read_unfinish*/, true/*read_mark_delete*/);

TEST_F(TestSSLogKVProxy, basic_sslog_kv_basic)
{
  ObSSLogProxyGuard guard;
  ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_PALF_KV,
                                                     1002,
                                                     guard));
  sslog::ObISSLogProxy *sslog_table_proxy = guard.get_sslog_proxy();

  int64_t affected_rows = 0;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key1 = "1002;1001;200001";
  const common::ObString meta_value1 = "qc_example1";
  const common::ObString extra_info1= "qc_extra1";
  const common::ObString meta_value1_new = "qc_example1_new";
  const common::ObString meta_value1_new_2 = "qc_example1_new_2";
  const common::ObString meta_value1_new_3 = "qc_example1_new_3";
  const common::ObString meta_value1_new_4 = "qc_example1_new_4";
  const common::ObString extra_info1_new = "qc_extra1_new";
  const common::ObString extra_info1_new_2 = "qc_extra1_new_2";
  const common::ObString extra_info1_new_3 = "qc_extra1_new_3";
  const common::ObString extra_info1_new_4 = "qc_extra1_new_4";

  // ========== Example 2 ==========
  const sslog::ObSSLogMetaType meta_type2 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key2 = "1002;1001;200002";
  const common::ObString meta_value2 = "qc_example2";
  const common::ObString extra_info2= "qc_extra2";

  // ========== Universal ==========
  common::ObString meta_key_ret;
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  share::SCN row_scn_ret;
  const common::ObString meta_key_prefix = "1002;1001;";

  // ========== Test 1: insert one row =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type1,
                                                      meta_key1,
                                                      meta_value1,
                                                      extra_info1,
                                                      affected_rows));

  ASSERT_EQ(1, affected_rows);

    // ========== Test 2: read the row =========
  {
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 3: insert another row =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type2,
                                                     meta_key2,
                                                     meta_value2,
                                                     extra_info2,
                                                     affected_rows));
  ASSERT_EQ(1, affected_rows);

    // ========== Test 4: read the prefix =========
  {
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key_prefix,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }
  {
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key_prefix,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_key_v2(row_scn_ret, meta_key_ret, extra_info_ret));
    ASSERT_EQ(meta_key1, meta_key_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_key_v2(row_scn_ret, meta_key_ret, extra_info_ret));
    ASSERT_EQ(meta_key2, meta_key_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_key_v2(row_scn_ret, meta_key_ret, extra_info_ret));
  }

  share::SCN snapshot_version1;
  ASSERT_EQ(OB_SUCCESS, dynamic_cast<ObSSLogKVProxy*>(sslog_table_proxy)->palf_kv_->get_gts(snapshot_version1));

  // ========== Test 5: update and read the row =========
  {
    ObSSLogWriteParam write_param(false, false, ObString());
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                        meta_type1,
                                                        meta_key1,
                                                        meta_value1_new,
                                                        extra_info1_new,
                                                        affected_rows));
  }

  {
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key1,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 6: delete and read the prefix =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->delete_row(meta_type2,
                                                      meta_key2,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  {
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key_prefix,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

    // ========== Test 7: read the snapshot =========
  {
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/, snapshot_version1);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key1,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/, snapshot_version1);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key_prefix,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

    // ========== Test 8: read the multi_version =========
  share::SCN snapshot_version2;
  ASSERT_EQ(OB_SUCCESS, dynamic_cast<ObSSLogKVProxy*>(sslog_table_proxy)->palf_kv_->get_gts(snapshot_version2));

  ObSSLogWriteParam write_param8(false, false, ObString());

  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param8,
                                                      meta_type1,
                                                      meta_key1,
                                                      meta_value1_new_2,
                                                      extra_info1_new_2,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param8,
                                                      meta_type1,
                                                      meta_key1,
                                                      meta_value1_new_3,
                                                      extra_info1_new_3,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       snapshot_version2,
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       snapshot_version2,
                                       true /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(snapshot_version2,
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }


  // ========== Test 9: only update extra info =========
  {
    ObSSLogWriteParam write_param(true, false, ObString());
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                      meta_type1,
                                                      meta_key1,
                                                      ObString(),
                                                      extra_info1_new_4,
                                                      affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 10: update with extra info check =========
  {
    ObSSLogWriteParam write_param(false, true, extra_info1_new_4);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                        meta_type1,
                                                        meta_key1,
                                                        meta_value1_new_4,
                                                        extra_info1_new_4,
                                                        affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_4, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 11: after all check =========
  sslog::ObSSLogMetaType meta_type_ret;
  {
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    SSLOG_TABLE_READ_INIT

    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key_prefix,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row_v2(row_scn_ret,
                                                    meta_type_ret,
                                                    meta_key_ret,
                                                    meta_value_ret,
                                                    extra_info_ret));
    ASSERT_EQ(meta_type1, meta_type_ret);
    ASSERT_EQ(meta_key1, meta_key_ret);
    ASSERT_EQ(meta_value1_new_4, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_4, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }
}
} // unittest

namespace sslog
{
int get_sslog_table_guard(const ObSSLogTableType type,
                          const int64_t tenant_id,
                          ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;
  SSLOG_LOG(INFO, "qianchen get_sslog_table_guard");

  switch (type)
  {
    case ObSSLogTableType::SSLOG_TABLE: {
      ret = OB_INVALID_ARGUMENT;
      SSLOG_LOG(WARN, "invalid sslog type", K(type));

      break;
    }
    case ObSSLogTableType::SSLOG_PALF_KV: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObPalfKVAdpaterInterface *palf_kv = &unittest::PALF_KV; // TODO: add it
        ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(palf_kv);
        guard.set_sslog_proxy((ObISSLogProxy *)proxy);
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      SSLOG_LOG(WARN, "invalid sslog type", K(type));
      break;
    }
  }

  return ret;
}

} // namespace sslog
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_sslog_kv_proxy.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_sslog_kv_proxy.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
