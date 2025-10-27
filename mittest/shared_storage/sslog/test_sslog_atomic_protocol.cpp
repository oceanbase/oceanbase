/**
 * Copyright (c) 2025 OceanBase
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
#include <thread>
#include <iostream>
#define protected public
#define private public

#include "storage/schema_utils.h"
#include "storage/incremental/sslog/ob_sslog_table_proxy.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_private_block_gc_task.h"
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/scn.h"
#include "mittest/simple_server/env/ob_simple_server_restart_helper.h"
#include "mittest/shared_storage/atomic_protocol/test_ss_atomic_util.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/test_tablet_helper.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "close_modules/shared_storage/storage/incremental/sslog/notify/ob_sslog_notify_adapter.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"


static const char *TEST_FILE_NAME = "test_sslog_atomic_protocol";
static const char *BORN_CASE_NAME = "ObTestSSLogAtomicProtocol";
static const char *RESTART_CASE_NAME = "ObSSLogAfterRestartTest";

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{

oceanbase::unittest::ObMockPalfKV PALF_KV;

int get_sslog_table_guard(const ObSSLogTableType type,
                          const int64_t tenant_id,
                          ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;

  switch (type)
  {
    case ObSSLogTableType::SSLOG_TABLE: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogTableProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogTableProxy *sslog_table_proxy = new (proxy) ObSSLogTableProxy(tenant_id);
        if (OB_FAIL(sslog_table_proxy->init())) {
          SSLOG_LOG(WARN, "fail to inint", K(ret));
        } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        }
      }
      break;
    }
    case ObSSLogTableType::SSLOG_PALF_KV: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(&PALF_KV);
        // if (OB_FAIL(sslog_kv_proxy->init(GCONF.cluster_id, tenant_id))) {
        //   SSLOG_LOG(WARN, "init palf kv failed", K(ret));
        // } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        // }
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

int ObSSLogNotifyAdapter::generate_notify_task_on_trans_ctx(NotifyPath ,
                                                            memtable::ObMvccTransNode *,
                                                            memtable::ObMemtableCtx *) {
  return OB_SUCCESS;
}
}

ObSSTableInfoList write_ss_list;
const int64_t OB_MAX_PARALLE_OP_COUNT = 6;

char *shared_storage_info = NULL;
namespace common {
bool is_shared_storage_sslog_table(const uint64_t tid)
{
  return OB_ALL_SSLOG_TABLE_TID == tid;
}

bool is_shared_storage_sslog_exist()
{
  return true;
}
}

namespace storage {

typedef ObDefaultSSMetaSSLogValue<ObSSLSMeta> ObSSLSMetaSSLogValue;
typedef ObAtomicDefaultFile<ObSSLSMetaSSLogValue> ObAtomicLSMetaFile;
static int64_t lease_epoch = 1;
static bool global_is_sswriter = true;


void mock_switch_sswriter()
{
  ATOMIC_INC(&lease_epoch);
  TRANS_LOG(INFO, "mock switch sswriter", K(lease_epoch));
}

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  is_sswriter = global_is_sswriter;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

int ObTablet::check_meta_addr() const
{
  int ret = OB_SUCCESS;
  return ret;
}

}

namespace unittest
{

using namespace common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::storage::checkpoint;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

class ObTestSSLogAtomicProtocol : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObTestSSLogAtomicProtocol() : ObSimpleClusterTestBase("test_shared_storage_sslog_proto", "50G", "50G", "50G")
  {}

  void wait_sys_to_leader()
  {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));
    ObLS *ls = nullptr;
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    ObLSHandle handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(ls_id, ls->get_ls_id());
    for (int i=0; i<100; i++) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
      if (role == ObRole::LEADER) {
        ret = OB_SUCCESS;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    wait_sys_to_leader();
    int ret = OB_SUCCESS;
    int retry_cnt = 0;
    do {
      if (OB_FAIL(create_tenant("tt1"))) {
        TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret));
        ob_usleep(15 * 1000 * 1000); // 15s
      }
      retry_cnt++;
    } while (OB_FAIL(ret) && retry_cnt < 10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  }
};

uint64_t test_tenant_id = 0;

TEST_F(ObTestSSLogAtomicProtocol, test_read_write_interface)
{
  ObAtomicFile::ENABLE_PARALLEL_OP = true;
  int ret = OB_SUCCESS;
  storage::ObAtomicFile::ENABLE_PARALLEL_OP = true;
  TRANS_LOG(INFO, "create tenant start");
  create_test_tenant(test_tenant_id);
  TRANS_LOG(INFO, "create tenant end");

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));
  // ========== Example 1 ==========
  int64_t affected_rows = 0;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  const sslog::ObSSLogMetaType meta_type2 = sslog::ObSSLogMetaType::SSLOG_LS_META;
  SCN transfer_scn;
  transfer_scn.set_min();
  ObSSMetaReadParam param;
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                               ObSSMetaReadResultType::READ_WHOLE_ROW,
                               false, /*try read local*/
                               meta_type1, ObLSID(1088), ObTabletID(200001), transfer_scn);

  ObSSMetaReadParam param2;
  SCN transfer_scn2;
  transfer_scn2.set_max();
  param2.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                false, /*try read local*/
                                meta_type1, ObLSID(1088), ObTabletID(200002), transfer_scn2);

  ObSSMetaReadParam param3;
  SCN transfer_scn3;
  transfer_scn3.set_max();
  param3.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                false, /*try read local*/
                                meta_type1, ObLSID(1077), ObTabletID(200003), transfer_scn3);

  ObSSMetaReadParam param4;
  param4.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                            ObSSMetaReadResultType::READ_WHOLE_ROW,
                            false, /*read_local*/
                            meta_type2, ObLSID(1078));

  ObSSMetaReadParam param5;
  param5.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                            ObSSMetaReadResultType::READ_WHOLE_ROW,
                            false, /*read_local*/
                            meta_type2, ObLSID(1079));

  ObSSMetaReadParam param6;
  param6.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                            ObSSMetaReadResultType::READ_WHOLE_ROW,
                            false, /*read_local*/
                            meta_type2, ObLSID(1080));

  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_value1 = "jianyue_example1";
  const common::ObString meta_value2 = "jianyue_example2";
  const common::ObString meta_value3 = "jianyue_example3";
  const common::ObString meta_value4 = "jianyue_example4";
  const common::ObString meta_value5 = "jianyue_example5";
  const common::ObString meta_value6 = "jianyue_example6";

  ObAtomicExtraInfo extra_info1;
  extra_info1.meta_info_.op_id_ = 0;
  extra_info1.meta_info_.epoch_id_ = 1;
  extra_info1.meta_info_.state_ = ObAtomicMetaInfo::State::COMMITTED;
  extra_info1.meta_info_.not_exist_ = false;

  ObAtomicExtraInfo extra_info2;
  extra_info2.meta_info_.op_id_ = 2;
  extra_info2.meta_info_.epoch_id_ = 1;
  extra_info2.meta_info_.state_ = ObAtomicMetaInfo::State::COMMITTED;
  extra_info2.meta_info_.not_exist_ = false;

  ObAtomicExtraInfo extra_info3;
  extra_info3.meta_info_.op_id_ = 3;
  extra_info3.meta_info_.epoch_id_ = 1;
  extra_info3.meta_info_.state_ = ObAtomicMetaInfo::State::COMMITTED;
  extra_info3.meta_info_.not_exist_ = false;
  char extra_info_buf3[100];
  int64_t ser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, extra_info3.serialize(extra_info_buf3, sizeof(extra_info_buf3), ser_pos));
  ObString extra_info_string_3(ser_pos, extra_info_buf3);

  ObAtomicExtraInfo extra_info4;
  extra_info4.meta_info_.op_id_ = 4;
  extra_info4.meta_info_.epoch_id_ = 1;
  extra_info4.meta_info_.state_ = ObAtomicMetaInfo::State::COMMITTED;
  extra_info4.meta_info_.not_exist_ = false;
  char extra_info_buf4[100];
  ser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, extra_info4.serialize(extra_info_buf4, sizeof(extra_info_buf4), ser_pos));
  ObString extra_info_string_4(ser_pos, extra_info_buf4);


  ObAtomicExtraInfo extra_info5;
  extra_info5.meta_info_.op_id_ = 5;
  extra_info5.meta_info_.epoch_id_ = 1;
  extra_info5.meta_info_.state_ = ObAtomicMetaInfo::State::COMMITTED;
  extra_info5.meta_info_.not_exist_ = false;
  char extra_info_buf5[100];
  ser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, extra_info5.serialize(extra_info_buf5, sizeof(extra_info_buf5), ser_pos));
  ObString extra_info_string_5(ser_pos, extra_info_buf5);

  ObAtomicExtraInfo extra_info6;
  extra_info6.meta_info_.op_id_ = 6;
  extra_info6.meta_info_.epoch_id_ = 1;
  extra_info6.meta_info_.state_ = ObAtomicMetaInfo::State::INIT;
  extra_info6.meta_info_.not_exist_ = true;



  // ========== Universal ==========
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  ObAtomicExtraInfo extra_info_deserialize_ret;
  SCN row_scn_ret;

  // ========== Test 1: insert one row =========
  ObAtomicMetaKey meta_key1;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param, meta_key1));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_sslog_row(param.meta_type_,
                                                       meta_key1.get_string_key(),
                                                       meta_value1,
                                                       extra_info1,
                                                       affected_rows));

  ASSERT_EQ(1, affected_rows);

  int64_t current_time = ObTimeUtility::current_time();
  share::SCN snapshot_version1;
  ASSERT_EQ(OB_SUCCESS, snapshot_version1.convert_from_ts(current_time));
  ob_usleep(1_s);

  ObAtomicMetaKey meta_key2;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param2, meta_key2));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_sslog_row(param2.meta_type_,
                                                      meta_key2.get_string_key(),
                                                      meta_value2,
                                                      extra_info2,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObAtomicMetaKey meta_key3;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param3, meta_key3));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_meta_row(param3.meta_type_,
                                                       meta_key3.get_string_key(),
                                                       meta_value3,
                                                       extra_info_string_3));
  ASSERT_EQ(1, affected_rows);

  ObAtomicMetaKey meta_key4;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param4, meta_key4));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_meta_row(param4.meta_type_,
                                                       meta_key4.get_string_key(),
                                                       meta_value4,
                                                       extra_info_string_4));
  ASSERT_EQ(1, affected_rows);

  ObAtomicMetaKey meta_key5;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param5, meta_key5));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_meta_row(param5.meta_type_,
                                                      meta_key5.get_string_key(),
                                                      meta_value5,
                                                      extra_info_string_5));
  ASSERT_EQ(1, affected_rows);

  ObAtomicMetaKey meta_key6;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param6, meta_key6));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_sslog_row(param6.meta_type_,
                                                      meta_key6.get_string_key(),
                                                      meta_value6,
                                                      extra_info6,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  // ========== Test 2: read the row =========
  {
    char buf[100];
    ObAtomicFileBuffer meta_value_buffer;
    meta_value_buffer.assign(buf, sizeof(buf));
    sslog::ObSSLogIteratorGuard iter(false, true);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_file_content(param,
                                                          share::SCN::invalid_scn(),
                                                          row_scn_ret,
                                                          meta_value_buffer));
    ASSERT_EQ(ObString(meta_value_buffer.data_len_, meta_value_buffer.buf_), meta_value1);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  {
    char buf[100];
    ObAtomicFileBuffer meta_value_buffer;
    meta_value_buffer.assign(buf, sizeof(buf));
    sslog::ObSSLogIteratorGuard iter(false, true);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_file_content(param2,
                                                          share::SCN::invalid_scn(),
                                                          row_scn_ret,
                                                          meta_value_buffer));
    ASSERT_EQ(ObString(meta_value_buffer.data_len_, meta_value_buffer.buf_), meta_value2);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  {
    ObSSLogIteratorGuard iter(false, true);
    param3.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                  ObSSMetaReadResultType::READ_ONLY_KEY,
                                  false, /*try read local*/
                                  meta_type1, ObLSID(1077), ObTabletID(200003), transfer_scn3);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param3,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    sslog::ObSSLogMetaType type;
    ObString key;
    ObString value;
    ObString info;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, type, key, value, info));
    ASSERT_EQ(type, param3.meta_type_);
    ASSERT_EQ(key, meta_key3.get_string_key());
    ASSERT_EQ(value, meta_value3);
    ASSERT_EQ(info, extra_info_string_3);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  {
    ObSSLogIteratorGuard iter(false, true);
    param4.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                              ObSSMetaReadResultType::READ_ONLY_KEY,
                              false, /*read_local*/
                              meta_type2, ObLSID(1078));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param4,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    sslog::ObSSLogMetaType type;
    ObString key;
    ObString value;
    ObString info;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, type, key, value, info));
    ASSERT_EQ(type, param4.meta_type_);
    ASSERT_EQ(key, meta_key4.get_string_key());
    ASSERT_EQ(value, meta_value4);
    ASSERT_EQ(info, extra_info_string_4);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  {
    ObSSLogIteratorGuard iter(false, true);
    param4.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                              ObSSMetaReadResultType::READ_ONLY_KEY,
                              false, /*read_local*/
                              meta_type2, ObLSID(1078));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param5,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    sslog::ObSSLogMetaType type;
    ObString key;
    ObString value;
    ObString info;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, type, key, value, info));
    ASSERT_EQ(type, param5.meta_type_);
    ASSERT_EQ(key, meta_key5.get_string_key());
    ASSERT_EQ(value, meta_value5);
    ASSERT_EQ(info, extra_info_string_5);
  }

  {
    ObSSLogIteratorGuard iter(false, true);
    param4.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                              ObSSMetaReadResultType::READ_ONLY_KEY,
                              false, /*read_local*/
                              meta_type2, ObLSID(1078));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param6,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    sslog::ObSSLogMetaType type;
    ObString key;
    ObString value;
    ObString info;
    ASSERT_EQ(OB_ITER_END, iter.get_next_row(row_scn_ret, type, key, value, info));
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  // ========== Test 3: update and read the row =========
  {
    char buf[100];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info1.serialize(buf, sizeof(buf), pos));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::update_sslog_row(param.meta_type_,
                                                       meta_key1.get_string_key(),
                                                       meta_value2,
                                                       ObString(pos, buf),
                                                       extra_info1,
                                                       false,
                                                       affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    char buf[100];
    ObAtomicFileBuffer meta_value_buffer;
    meta_value_buffer.assign(buf, sizeof(buf));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_file_content(param,
                                                          share::SCN::invalid_scn(),
                                                          row_scn_ret,
                                                          meta_value_buffer));
    ASSERT_EQ(ObString(meta_value_buffer.data_len_, meta_value_buffer.buf_), meta_value2);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  // ========== Test 4: multi version read =========
  {
    char buf[100];
    ObAtomicFileBuffer meta_value_buffer;
    meta_value_buffer.assign(buf, sizeof(buf));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_file_content(param,
                                                          snapshot_version1,
                                                          row_scn_ret,
                                                          meta_value_buffer));
    ASSERT_EQ(ObString(meta_value_buffer.data_len_, meta_value_buffer.buf_), meta_value1);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
  }

  {
    int64_t current_time2 = ObTimeUtility::current_time();
    share::SCN snapshot_version2;
    ASSERT_EQ(OB_SUCCESS, snapshot_version2.convert_from_ts(current_time2));
    ob_usleep(1_s);
    char buf[100];
    ObAtomicFileBuffer meta_value_buffer;
    meta_value_buffer.assign(buf, sizeof(buf));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_file_content(param,
                                                          snapshot_version2,
                                                          row_scn_ret,
                                                          meta_value_buffer));
    ASSERT_EQ(ObString(meta_value_buffer.data_len_, meta_value_buffer.buf_), meta_value2);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
    TRANS_LOG(INFO, "jianyue debug", K(ObString(meta_value_buffer.size_, meta_value_buffer.buf_)));
  }

  {
    SCN max_decided_scn;
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_max_decided_scn(false, share::SYS_LS, max_decided_scn));
    TRANS_LOG(INFO, "jianyue debug", K(max_decided_scn));
    ASSERT_EQ(true, max_decided_scn >= snapshot_version1);
  }

  {
    TRANS_LOG(INFO, "jianyue debug, get range meta value");
    int64_t pos = 0;
    ObMetaVersionRange version_range;
    version_range.version_start_ = snapshot_version1;
    ObSSLogIteratorGuard multi_version_iter(false, true);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_range_meta_value(param, version_range, multi_version_iter));
    ASSERT_EQ(OB_SUCCESS, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));

    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);
    ASSERT_EQ(OB_ITER_END, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    int64_t pos = 0;
    ObMetaVersionRange version_range;
    version_range.version_end_ = snapshot_version1;
    ObSSLogIteratorGuard multi_version_iter(false, true);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_range_meta_value(param, version_range, multi_version_iter));
    ASSERT_EQ(OB_SUCCESS, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);
    ASSERT_EQ(OB_ITER_END, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    int64_t pos = 0;
    int64_t current_time2 = ObTimeUtility::current_time();
    share::SCN snapshot_version2;
    ASSERT_EQ(OB_SUCCESS, snapshot_version2.convert_from_ts(current_time2));
    ob_usleep(1_s);
    ObMetaVersionRange version_range;
    version_range.version_end_ = snapshot_version2;
    ObSSLogIteratorGuard multi_version_iter(false, true);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_range_meta_value(param, version_range, multi_version_iter));
    ASSERT_EQ(OB_SUCCESS, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);
    SCN last_row_scn = row_scn_ret;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(true, row_scn_ret < last_row_scn);
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);
    ASSERT_EQ(OB_ITER_END, multi_version_iter.get_next_meta(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 5: read meta key =========
  {
    TRANS_LOG(INFO, "jianyue debug for segment query");
    ObSSLogIteratorGuard iter(false, true);
    param.set_tablet_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                 ObSSMetaReadResultType::READ_ONLY_KEY,
                                 false, /*try read local*/
                                 meta_type1, ObLSID(1088), ObTabletID(200001), transfer_scn, true, 1);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    ObString key1;

    ASSERT_EQ(OB_SUCCESS, iter.get_next_key(row_scn_ret, key1, extra_info_ret));
    ObSSLogMetaKey sslog_meta_key_ret1;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));
    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);
    ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_);
    ObString key2;

    ASSERT_EQ(OB_SUCCESS, iter.get_next_key(row_scn_ret, key2, extra_info_ret));
    ObSSLogMetaKey sslog_meta_key_ret2;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret2.deserialize(key2.ptr(), key2.length(), pos));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));
    ASSERT_EQ(extra_info2, extra_info_deserialize_ret);
    ASSERT_EQ(param2.tablet_level_param_.ls_id_, sslog_meta_key_ret2.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param2.tablet_level_param_.tablet_id_, sslog_meta_key_ret2.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param2.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret2.tablet_meta_key_.reorganization_scn_);

    ASSERT_EQ(OB_ITER_END, iter.get_next_key(row_scn_ret, key2, extra_info_ret));
  }

  {
    ObSSLogIteratorGuard iter(false, true);
    param.set_tablet_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                 ObSSMetaReadResultType::READ_ONLY_KEY,
                                 false, /*try read local*/
                                 meta_type1, ObLSID(1088), ObTabletID(200001), transfer_scn);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                      snapshot_version1,
                                                      iter));
    ObString key1;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_key(row_scn_ret, key1, extra_info_ret));
    ObSSLogMetaKey sslog_meta_key_ret1;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(extra_info_ret.ptr(), extra_info_ret.length(), pos));
    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);
    ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_);
    ObString key2;
    ASSERT_EQ(OB_ITER_END, iter.get_next_key(row_scn_ret, key2, extra_info_ret));
  }

  // ========== Test 6: read meta row =========
  {
    ObSSLogIteratorGuard iter(false, true);
    param.set_tablet_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                 ObSSMetaReadResultType::READ_WHOLE_ROW,
                                 false, /*try read local*/
                                 meta_type1, ObLSID(1088), ObTabletID(200001), transfer_scn);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    ObSSLogMetaType meta_type_ret1;
    ObAtomicMetaKey meta_key_ret1;
    ObString key1;
    ObString value1;
    ObString info1;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret1, key1, value1, info1));
    ObSSLogMetaKey sslog_meta_key_ret1;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_);
    ASSERT_EQ(true, meta_type_ret1 == param.meta_type_);
    ASSERT_EQ(true, value1 == meta_value2);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(info1.ptr(), info1.length(), pos));
    ASSERT_EQ(extra_info1, extra_info_deserialize_ret);

    ObSSLogMetaType meta_type_ret2;
    ObString key2;
    ObString value2;
    ObString info2;
    pos = 0;
    ObSSLogMetaKey sslog_meta_key_ret2;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret2, key2, value2, info2));
    ASSERT_EQ(true, meta_type_ret2 == param2.meta_type_);
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret2.deserialize(key2.ptr(), key2.length(), pos));
    ASSERT_EQ(param2.tablet_level_param_.ls_id_, sslog_meta_key_ret2.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param2.tablet_level_param_.tablet_id_, sslog_meta_key_ret2.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param2.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret2.tablet_meta_key_.reorganization_scn_);
    ASSERT_EQ(true, value2 == meta_value2);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(info2.ptr(), info2.length(), pos));
    ASSERT_EQ(extra_info2, extra_info_deserialize_ret);
  }

  {
    ObSSLogIteratorGuard iter(false, true);
    param4.set_ls_level_param(ObSSMetaReadParamType::TENANT_PREFIX,
                              ObSSMetaReadResultType::READ_WHOLE_ROW,
                              false, /*read_local*/
                              meta_type2, ObLSID(1078));
    TRANS_LOG(INFO, "jianyue debug read meta", K(param4));
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param4,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    ObSSLogMetaType meta_type_ret1;
    ObAtomicMetaKey meta_key_ret1;
    ObString key1;
    ObString value1;
    ObString info1;
    int64_t pos = 0;
    ObSSLogMetaKey sslog_meta_key_ret1;
    do {
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret1, key1, value1, info1));
      ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    } while (sslog_meta_key_ret1.tablet_meta_key_.ls_id_ < param4.ls_level_param_.ls_id_);
    ASSERT_EQ(param4.ls_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(true, meta_type_ret1 == param4.meta_type_);
    ASSERT_EQ(true, value1 == meta_value4);
    pos = 0;
    ASSERT_EQ(extra_info_string_4, info1);

    ObSSLogMetaType meta_type_ret2;
    ObString key2;
    ObString value2;
    ObString info2;
    pos = 0;
    ObSSLogMetaKey sslog_meta_key_ret2;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret2, key2, value2, info2));
    ASSERT_EQ(true, meta_type_ret2 == param5.meta_type_);
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret2.deserialize(key2.ptr(), key2.length(), pos));
    ASSERT_EQ(param5.ls_level_param_.ls_id_, sslog_meta_key_ret2.ls_meta_key_.ls_id_);
    ASSERT_EQ(true, value2 == meta_value5);
    pos = 0;
    ASSERT_EQ(extra_info_string_5, info2);
  }

  // ========== Test 7: delete meta row =========
  {
    {
      // 1. delete tablet sslog row
      //   a. mark delete
      ObSSMetaDeleteParam delete_param3;
      delete_param3.set_mark_delete();
      delete_param3.set_tablet_level_param(param3.meta_type_,
                                          param3.tablet_level_param_.ls_id_,
                                          param3.tablet_level_param_.tablet_id_,
                                          param3.tablet_level_param_.reorganization_scn_);
      TRANS_LOG(INFO, "mark delete row", K(param4));
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::delete_meta_row(delete_param3, affected_rows));
      ASSERT_EQ(1, affected_rows);
      ObSSLogIteratorGuard iter(true, true);
      param3.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_ONLY_KEY,
                                    false, /*try read local*/
                                    meta_type1, ObLSID(1077), ObTabletID(200003), transfer_scn3);
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param3,
                                                        share::SCN::invalid_scn(),
                                                        iter));
      sslog::ObSSLogMetaType type;
      ObString key;
      ObString value;
      ObString info;
      ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, type, key, value, info));
      ObAtomicExtraInfo extra_info_ret;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info.ptr(), info.length(), pos));
      ASSERT_EQ(true, extra_info_ret.meta_info_.mark_delete_);
    }
    //  b. really delete
    {
      ObSSMetaDeleteParam delete_param3;
      delete_param3.set_really_delete();
      delete_param3.set_tablet_level_param(param3.meta_type_,
                                          param3.tablet_level_param_.ls_id_,
                                          param3.tablet_level_param_.tablet_id_,
                                          param3.tablet_level_param_.reorganization_scn_);
      TRANS_LOG(INFO, "really delete row", K(param3));
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::delete_meta_row(delete_param3, affected_rows));
      ASSERT_EQ(1, affected_rows);
      ObSSLogIteratorGuard iter(true, true);
      param3.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_ONLY_KEY,
                                    false, /*try read local*/
                                    meta_type1, ObLSID(1077), ObTabletID(200003), transfer_scn3);
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param3,
                                                        share::SCN::invalid_scn(),
                                                        iter));
      sslog::ObSSLogMetaType type;
      ObString key;
      ObString value;
      ObString info;
      ASSERT_EQ(OB_ITER_END, iter.get_next_row(row_scn_ret, type, key, value, info));
    }
  }

  {
    {
      // 1. delete ls sslog row
      //   a. mark delete
      ObSSMetaDeleteParam delete_param5;
      delete_param5.set_mark_delete();
      delete_param5.set_ls_level_param(param5.meta_type_,
                                       param5.ls_level_param_.ls_id_);
      TRANS_LOG(INFO, "mark delete ls row", K(param4));
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::delete_meta_row(delete_param5, affected_rows));
      ASSERT_EQ(1, affected_rows);
      ObSSLogIteratorGuard iter(true, true);
      param5.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                                ObSSMetaReadResultType::READ_ONLY_KEY,
                                false, /*read_local*/
                                meta_type2, ObLSID(1079));
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param5,
                                                        share::SCN::invalid_scn(),
                                                        iter));
      sslog::ObSSLogMetaType type;
      ObString key;
      ObString value;
      ObString info;
      ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, type, key, value, info));
      ObAtomicExtraInfo extra_info_ret;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info.ptr(), info.length(), pos));
      ASSERT_EQ(true, extra_info_ret.meta_info_.mark_delete_);
    }
    //  b. really delete
    {
      ObSSMetaDeleteParam delete_param5;
      delete_param5.set_really_delete();
      delete_param5.set_ls_level_param(param5.meta_type_,
                                       param5.ls_level_param_.ls_id_);
      TRANS_LOG(INFO, "really delete ls row", K(param4));
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::delete_meta_row(delete_param5, affected_rows));
      ASSERT_EQ(1, affected_rows);
      ObSSLogIteratorGuard iter(true, true);
      param5.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                                ObSSMetaReadResultType::READ_ONLY_KEY,
                                false, /*read_local*/
                                meta_type2, ObLSID(1079));
      ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param5,
                                                        share::SCN::invalid_scn(),
                                                        iter));
      sslog::ObSSLogMetaType type;
      ObString key;
      ObString value;
      ObString info;
      ASSERT_EQ(OB_ITER_END, iter.get_next_row(row_scn_ret, type, key, value, info));
    }
  }
}

TEST_F(ObTestSSLogAtomicProtocol, test_ls_meta_write_op)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));
  uint64_t tenant_id = MTL_ID();
  ObLSService *ls_svr = MTL(ObLSService*);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
  ASSERT_NE(nullptr, atomic_file_mgr);
  const int64_t LS_ID = 1002;
  ObLSID ls_id(LS_ID);
  SimpleServerHelper::create_ls(tenant_id, GCTX.self_addr());
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();

  uint64_t tablet_cur_op_id = 0;
  {
    LOG_INFO("write ls meta with sswriter");
    GET_LS_META_HANLDE(file_handle, 2, 1);
    ObAtomicLSMetaFile *ls_meta_file = file_handle1.get_atomic_file();
    ASSERT_NE(nullptr, ls_meta_file);
    // test write ls meta and read ls meta
    // create op
    LOG_INFO("read ls meta", K(tablet_cur_op_id));
    CREATE_LS_META_WRITE_OP_WITH_RECONFIRM(op_handle, true);
    LOG_INFO("read ls meta", K(tablet_cur_op_id));
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    ObLSMeta ls_meta = ls->get_ls_meta();
    ObSSLSMetaSSLogValue ssls_meta_v;
    ObSSMetaUpdateMetaInfo meta_info;
    ssls_meta_v.meta_value_.init_for_ss(ls_meta);
    meta_info.set(ObMetaUpdateReason::CREATE_LS,
                  ssls_meta_v.meta_value_.get_acquire_scn());
    ASSERT_EQ(OB_SUCCESS, ssls_meta_v.set_meta_info(meta_info));
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_task_info(ssls_meta_v));
    // finish op and flush buffer to share storage
    ASSERT_EQ(OB_SUCCESS, ls_meta_file->finish_op(op_handle));
    // read ls meta
    ObSSLSMetaSSLogValue read_ls_meta_v;
    ObSSMetaUpdateMetaInfo read_meta_info;
    LOG_INFO("read ls meta");
    ASSERT_EQ(OB_SUCCESS, ls_meta_file->read_file_info(read_ls_meta_v));
    ASSERT_EQ(ls_id, read_ls_meta_v.meta_value_.ls_id_);
    read_meta_info = read_ls_meta_v.get_meta_info();
    ASSERT_EQ(ObMetaUpdateReason::CREATE_LS, read_meta_info.update_reason_);
    LOG_INFO("read ls meta", K(read_ls_meta_v));
  }

  {
    LOG_INFO("write ls meta without sswriter");
    tablet_cur_op_id = 0;
    GET_LS_META_HANLDE(file_handle, 3, 1);
    ObAtomicLSMetaFile *ls_meta_file = file_handle1.get_atomic_file();
    ASSERT_NE(nullptr, ls_meta_file);
    // op_id=0 no sswriter
    global_is_sswriter = false;
    CREATE_LS_META_WRITE_OP_WITH_RECONFIRM(op_handle, false);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    ObLSMeta ls_meta = ls->get_ls_meta();
    ObSSLSMetaSSLogValue ssls_meta_v;
    ssls_meta_v.meta_value_.init_for_ss(ls_meta);
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_task_info(ssls_meta_v));
    // finish op and flush buffer to share storage

    ASSERT_EQ(OB_SUCCESS, ls_meta_file->finish_op(op_handle));
    // read sstable list
    ObSSLSMetaSSLogValue read_ls_meta_v;
    LOG_INFO("read ls meta");
    ASSERT_EQ(OB_SUCCESS, ls_meta_file->read_file_info(read_ls_meta_v));
    ASSERT_EQ(ls_id, read_ls_meta_v.meta_value_.ls_id_);
    ObSSLSMetaSSLogValue read_ls_meta_task_info_v;
    // ASSERT_EQ(OB_SUCCESS, ls_meta_file->get_task_info(0, read_ls_meta_task_info));
    // ASSERT_EQ(ls_id, read_ls_meta_task_info.ls_id_);
    global_is_sswriter = true;
  }
}

TEST_F(ObTestSSLogAtomicProtocol, test_tablet_meta_write_op)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));

  TRANS_LOG(INFO, "test_tablet_meta_write_op");
  ObLSService *ls_svr = MTL(ObLSService*);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObTabletHandle tablet_handle;
  ObTablet *tablet =NULL;

  ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
  ASSERT_NE(nullptr, atomic_file_mgr);
  const int64_t TABLET_ID = 200001;
  const int64_t LS_ID = 1;
  ObTabletID tablet_id(TABLET_ID);
  ObLSID ls_id(LS_ID);

  // create ls
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  // create a tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator));

  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_EQ(tablet_id, tablet->get_tablet_meta().tablet_id_);

  GET_TABLET_META_HANLDE_DEFAULT(file_handle, 1, LS_ID, TABLET_ID);
  ObAtomicTabletMetaFile *tablet_meta_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, tablet_meta_file);
  uint64_t tablet_cur_op_id = 0;
  {
    TRANS_LOG(INFO, "test_tablet_meta_write_op 1");
    // test write tablet meta and read tablet meta when preceding tablet meta is empty
    // create op
    CREATE_TABLET_META_WRITE_OP_WITH_RECONFIRM(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    const uint64_t data_version = DATA_CURRENT_VERSION;
    // write tablet_meta and sub_tablet_meta
    MTL(ObSSMetaService*)->persist_tablet_(data_version, tablet, ObMetaUpdateReason::CREATE_TABLET, -1, op_handle, tablet_meta_file);

    // read tablet meta
    tablet = nullptr;
    ObTabletHandle tablet_handle;
    common::ObArenaAllocator allocator("TestTabletMeta",
                                       OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
    share::SCN row_scn;
    ASSERT_EQ(OB_SUCCESS, tablet_meta_file->get_tablet(allocator, tablet_handle, row_scn));
    ASSERT_EQ(tablet_id, tablet_handle.get_obj()->get_tablet_id());
    ObSSTabletSSLogValue tablet_v;
    ObSSMetaUpdateMetaInfo read_meta_info;
    ASSERT_EQ(OB_SUCCESS, tablet_meta_file->get_tablet(allocator, tablet_v));
    read_meta_info = tablet_v.get_meta_info();
    ASSERT_EQ(ObMetaUpdateReason::CREATE_TABLET, read_meta_info.update_reason_);
  }
}

TEST_F(ObTestSSLogAtomicProtocol, test_sstable_list_write_op)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));

  TRANS_LOG(INFO, "test_sstable_list_write_op");
  ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
  ASSERT_NE(nullptr, atomic_file_mgr);
  const int64_t TABLET_ID = 200001;
  const int64_t LS_ID = 1;

  SCN create_scn;
  int64_t current_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, create_scn.convert_from_ts(current_time));
  ObTabletID tablet_id(TABLET_ID);
  ObLSID ls_id(LS_ID);
  ObSSMetaReadParam param;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE;
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                               ObSSMetaReadResultType::READ_WHOLE_ROW,
                               false, /*try read local*/
                               meta_type1, ls_id, tablet_id, create_scn);

  GET_MINI_SSTABLE_LIST_HANDLE_V2(file_handle, LS_ID, TABLET_ID, create_scn, 1);
  int64_t tablet_cur_op_id = 0;
  {
    ob_usleep(1_s);
    // do one successful op
    char gc_info_char[] = "gc_info1";
    ObString gc_info(sizeof(gc_info_char), gc_info_char);

    TRANS_LOG(INFO, "do sstable list add");
    DO_ONE_SSTABLE_LIST_ADD_OP_V2(sstablelist_file1,
                                  gc_info,
                                  false,
                                  1)
    ASSERT_EQ(tablet_cur_op_id, sstablelist_file1->finish_op_id_);

    ObSSLogIteratorGuard iter(false, true);
    param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                 ObSSMetaReadResultType::READ_WHOLE_ROW,
                                 false, /*try read local*/
                                 meta_type1, ls_id, tablet_id, create_scn);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    ObSSLogMetaType meta_type_ret1;
    ObAtomicMetaKey meta_key_ret1;
    SCN row_scn_ret;
    ObString key1;
    ObString value1;
    ObString info1;
    ObAtomicExtraInfo extra_info_ret;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret1, key1, value1, info1));
    ObSSLogMetaKey sslog_meta_key_ret1;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info1.ptr(), info1.length(), pos));
    ASSERT_EQ(1, extra_info_ret.meta_info_.op_id_);
    ASSERT_EQ(true, 0 < extra_info_ret.meta_info_.epoch_id_);
    ASSERT_EQ(ObAtomicMetaInfo::State::COMMITTED, extra_info_ret.meta_info_.state_);
    ASSERT_EQ(false, extra_info_ret.meta_info_.not_exist_);
    TRANS_LOG(INFO, "jianyue debug555-2", K(extra_info_ret), K(gc_info), K(extra_info_ret.gc_info_.get_ob_string()));
    ASSERT_EQ(gc_info, extra_info_ret.gc_info_.get_ob_string());
    ASSERT_EQ(true, row_scn_ret > create_scn);
  }

  {
    // do one abort op
    char gc_info_char[] = "gc_info2";
    ObString gc_info(sizeof(gc_info_char), gc_info_char);
    TRANS_LOG(INFO, "jianyue debug666");

    DO_ONE_SSTABLE_LIST_ADD_OP_V2(sstablelist_file1,
                                  gc_info,
                                  true,
                                  1)

    ObSSLogIteratorGuard iter(true, true);
    param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                 ObSSMetaReadResultType::READ_WHOLE_ROW,
                                 false, /*try read local*/
                                 meta_type1, ls_id, tablet_id, create_scn);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    ObSSLogMetaType meta_type_ret1;
    ObAtomicMetaKey meta_key_ret1;
    SCN row_scn_ret;
    ObString key1;
    ObString value1;
    ObString info1;
    ObAtomicExtraInfo extra_info_ret;
    int64_t pos = 0;
    SCN last_row_scn = row_scn_ret;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret1, key1, value1, info1));
    ObSSLogMetaKey sslog_meta_key_ret1;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info1.ptr(), info1.length(), pos));
    ASSERT_EQ(2, extra_info_ret.meta_info_.op_id_);
    ASSERT_EQ(true, 0 < extra_info_ret.meta_info_.epoch_id_);
    ASSERT_EQ(ObAtomicMetaInfo::State::INIT, extra_info_ret.meta_info_.state_);
    ASSERT_EQ(false, extra_info_ret.meta_info_.not_exist_);
    ASSERT_EQ(gc_info, extra_info_ret.gc_info_.get_ob_string());
    ASSERT_EQ(true, row_scn_ret > last_row_scn);
  }


  {
    char buf1[20] = "schema buf";
    ObString gc_info = ObString(strlen(buf1), buf1);
    {
      GET_ONE_SSTABLE_INFO(write_info1, tablet_cur_op_id)
      TRANS_LOG(INFO, "jianyue debug777", K(lbt()));
      // test write sstable list and read sstable list when preceding sstable list is empty
      // create op
      CREATE_SSTABLE_LIST_ADD_OP_WITH_GC_INFO(op_handle, 1, gc_info)
      uint64_t op_id = 0;
      ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
      ASSERT_EQ(tablet_cur_op_id, op_id);
      // finish op and flush buffer to share storage, generate sstable list obj
      ASSERT_EQ(OB_SUCCESS, sstablelist_file1->finish_op(op_handle));
    }

    param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                 ObSSMetaReadResultType::READ_WHOLE_ROW,
                                 false, /*try read local*/
                                 meta_type1, ls_id, tablet_id, create_scn);
    int64_t current_time2 = ObTimeUtility::current_time();
    share::SCN snapshot_version2;
    ASSERT_EQ(OB_SUCCESS, snapshot_version2.convert_from_ts(current_time2));
    ObMetaVersionRange version_range;
    version_range.version_end_ = snapshot_version2;
    ob_usleep(1_s);
    ObSSLogIteratorGuard iter(true, true);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_range_meta_value(param,
                                                      version_range,
                                                      iter));
    ObSSTableGCInfo gc_info_ret;
    SCN row_scn_ret;
    ObString value1;
    ObString info1;
    ObAtomicExtraInfo extra_info_ret;
    int64_t pos = 0;
    SCN last_row_scn = row_scn_ret;

    {
      // check gc info, last version
      ASSERT_EQ(OB_SUCCESS, iter.get_next_meta(row_scn_ret, value1, info1));
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info1.ptr(), info1.length(), pos));
      ASSERT_EQ(3, extra_info_ret.meta_info_.op_id_);
      ASSERT_EQ(true, 0 < extra_info_ret.meta_info_.epoch_id_);
      ASSERT_EQ(ObAtomicMetaInfo::State::COMMITTED, extra_info_ret.meta_info_.state_);
      ASSERT_EQ(false, extra_info_ret.meta_info_.not_exist_);
      ASSERT_EQ(gc_info, extra_info_ret.gc_info_.get_ob_string());
    }

    {
      // check gc info, prev version
      TRANS_LOG(INFO, "jianyue debug888");
      ASSERT_EQ(OB_SUCCESS, iter.get_next_meta(row_scn_ret, value1, info1));
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info1.ptr(), info1.length(), pos));
      ASSERT_EQ(3, extra_info_ret.meta_info_.op_id_);
      ASSERT_EQ(true, 0 < extra_info_ret.meta_info_.epoch_id_);
      ASSERT_EQ(ObAtomicMetaInfo::State::INIT, extra_info_ret.meta_info_.state_);
      ASSERT_EQ(false, extra_info_ret.meta_info_.not_exist_);
      ASSERT_EQ(gc_info, extra_info_ret.gc_info_.get_ob_string());
    }

  }

  {
    // do one fail op
    char gc_info_char[] = "gc_info3";
    ObString gc_info(sizeof(gc_info_char), gc_info_char);
    TRANS_LOG(INFO, "fail op start");

    DO_ONE_SSTABLE_LIST_FAIL_OP(sstablelist_file1,
                                gc_info,
                                1)

    ObSSLogIteratorGuard iter(true, true);
    param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                 ObSSMetaReadResultType::READ_WHOLE_ROW,
                                 false, /*try read local*/
                                 meta_type1, ls_id, tablet_id, create_scn);
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                      share::SCN::invalid_scn(),
                                                      iter));
    ObSSLogMetaType meta_type_ret1;
    ObAtomicMetaKey meta_key_ret1;
    SCN row_scn_ret;
    ObString key1;
    ObString value1;
    ObString info1;
    ObAtomicExtraInfo extra_info_ret;
    int64_t pos = 0;
    SCN last_row_scn = row_scn_ret;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret1, key1, value1, info1));
    ObSSLogMetaKey sslog_meta_key_ret1;
    ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos));
    ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_);
    ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_);
    ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, extra_info_ret.deserialize(info1.ptr(), info1.length(), pos));
    ASSERT_EQ(4, extra_info_ret.meta_info_.op_id_);
    ASSERT_EQ(true, 0 < extra_info_ret.meta_info_.epoch_id_);
    ASSERT_EQ(ObAtomicMetaInfo::State::ABORTED, extra_info_ret.meta_info_.state_);
    ASSERT_EQ(false, extra_info_ret.meta_info_.not_exist_);
    ASSERT_EQ(gc_info, extra_info_ret.gc_info_.get_ob_string());
    ASSERT_EQ(true, row_scn_ret > last_row_scn);
  }
}

TEST_F(ObTestSSLogAtomicProtocol, test_sslog_meta_key)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));

  TRANS_LOG(INFO, "test_sslog_meta_key");
  char str_partial_meta_key1[] = "id_1:1;";
  char str_partial_meta_key2[] = "id_1:1002;id_2:1002;";
  char str_partial_meta_key3[] = "id_1:1003;id_2:1003;id_3:0;";
  char str_partial_meta_key4[] = "id_1:1004;id_2:1002;id_3:200050;id_4:100;";
  ObSSLogMetaKey key1;
  ObSSLogMetaKey key2;
  ObSSLogMetaKey key3;
  ObSSLogMetaKey key4;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, key1.deserialize(str_partial_meta_key1, strlen(str_partial_meta_key1), pos));
  ASSERT_EQ(1, key1.first_id_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, key2.deserialize(str_partial_meta_key2, strlen(str_partial_meta_key2), pos));
  ASSERT_EQ(1002, key2.first_id_);
  ASSERT_EQ(1002, key2.second_id_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, key3.deserialize(str_partial_meta_key3, strlen(str_partial_meta_key3), pos));
  ASSERT_EQ(1003, key3.first_id_);
  ASSERT_EQ(1003, key3.second_id_);
  ASSERT_EQ(0, key3.third_id_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, key4.deserialize(str_partial_meta_key4, strlen(str_partial_meta_key4), pos));
  ASSERT_EQ(1004, key4.first_id_);
  ASSERT_EQ(1002, key4.second_id_);
  ASSERT_EQ(200050, key4.third_id_);
  ASSERT_EQ(100, key4.fourth_id_);
}

TEST_F(ObTestSSLogAtomicProtocol, test_meta_value_write)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));
  TRANS_LOG(INFO, "test_meta_value_write");
  // ========== Example 1 ==========
  int64_t affected_rows = 0;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  SCN transfer_scn;
  transfer_scn.set_min();
  ObSSMetaReadParam param;
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                               ObSSMetaReadResultType::READ_WHOLE_ROW,
                               false, /*try read local*/
                               meta_type1, ObLSID(1088), ObTabletID(200081), transfer_scn);

  const int STR_LEN = 819200;
  char *value1_str = (char *)mtl_malloc(STR_LEN, "MITTEST");
  MEMSET(value1_str, 'E', STR_LEN);
  value1_str[STR_LEN - 100] = '\0';
  value1_str[STR_LEN - 2] = 't';
  value1_str[STR_LEN - 1] = '\0';

  TRANS_LOG(INFO, "test meta value storage");

  const common::ObString meta_value1(STR_LEN, value1_str);
  ObAtomicExtraInfo extra_info1;
  extra_info1.meta_info_.op_id_ = 0;
  extra_info1.meta_info_.epoch_id_ = 1;
  extra_info1.meta_info_.state_ = ObAtomicMetaInfo::State::COMMITTED;
  extra_info1.meta_info_.not_exist_ = false;
  char extra_info_buf1[100];
  int64_t ser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, extra_info1.serialize(extra_info_buf1, sizeof(extra_info_buf1), ser_pos));
  ObString extra_info_string_1(ser_pos, extra_info_buf1);

  // ========== Universal ==========
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  ObAtomicExtraInfo extra_info_deserialize_ret;
  SCN row_scn_ret;

  // ========== Test 1: insert one row =========
  ObAtomicMetaKey meta_key1;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param, meta_key1));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_sslog_row(param.meta_type_,
                                                       meta_key1.get_string_key(),
                                                       meta_value1,
                                                       extra_info1,
                                                       affected_rows));

  ASSERT_EQ(1, affected_rows);

  // ========== Test 2: read the row =========
  {
    char *buf = (char *)mtl_malloc(STR_LEN, "MITTEST");
    ObAtomicFileBuffer meta_value_buffer;
    meta_value_buffer.assign(buf, STR_LEN);
    sslog::ObSSLogIteratorGuard iter(false, true);
    TRANS_LOG(INFO, "read begin");
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_file_content(param,
                                                          share::SCN::invalid_scn(),
                                                          row_scn_ret,
                                                          meta_value_buffer));
    TRANS_LOG(INFO, "read end", K(meta_value_buffer), K(meta_value1), K(meta_value1.length()));
    ASSERT_EQ(true, meta_value_buffer.data_len_ == STR_LEN);
    ASSERT_EQ(true, ObString(meta_value_buffer.data_len_, meta_value_buffer.buf_) == meta_value1);
    ASSERT_EQ(true, row_scn_ret > SCN::min_scn());
    mtl_free(buf);
  }
  mtl_free(value1_str);

}

TEST_F(ObTestSSLogAtomicProtocol, test_parallel_op)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(test_tenant_id));

  TRANS_LOG(INFO, "test_parallel_op");
  ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
  ASSERT_NE(nullptr, atomic_file_mgr);
  const int64_t TABLET_ID = 200001;
  const int64_t LS_ID = 1096;

  SCN create_scn;
  int64_t current_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, create_scn.convert_from_ts(current_time));
  ObTabletID tablet_id(TABLET_ID);
  ObLSID ls_id(LS_ID);
  ObSSMetaReadParam param;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE;
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                               ObSSMetaReadResultType::READ_WHOLE_ROW,
                               true,
                               meta_type1, ls_id, tablet_id, create_scn);

  GET_MINI_SSTABLE_LIST_HANDLE_V2(file_handle, LS_ID, TABLET_ID, create_scn, 1);
  int64_t tablet_cur_op_id = 0;
  {
    /* do 5 parallel op,
      | op1 create |
      | op2 create |
      | op3 create |
      | op2 fail   |
      | op4 create |
      | op3 finish |
      | op4 finish |
      | op5 create |
      | op1 abort  |
      | op5 fail   |
    */
    ObSSTableGCInfo gc_info1(16, 10, 3);
    ObSSTableGCInfo gc_info2(40, 10, 4);
    ObSSTableGCInfo gc_info3(16, 15, 4);
    ObSSTableGCInfo gc_info4(32, 32, 8);
    ObSSTableGCInfo gc_info5(64, 64, 16);

    CREATE_SSTABLELIST_ADD_PARALLEL_OP(op_handle1,
                                       1,
                                       true,
                                       gc_info1)

    CREATE_SSTABLELIST_ADD_PARALLEL_OP(op_handle2,
                                       1,
                                       true,
                                       gc_info2)

    CREATE_SSTABLELIST_ADD_PARALLEL_OP(op_handle3,
                                       1,
                                       true,
                                       gc_info3)

    FAIL_SSTABLELIST_ADD_PARALLEL_OP(1, op_handle2)

    CREATE_SSTABLELIST_ADD_PARALLEL_OP(op_handle4,
                                       1,
                                       true,
                                       gc_info4)

    FINISH_SSTABLELIST_ADD_PARALLEL_OP(1, op_handle3)
    FINISH_SSTABLELIST_ADD_PARALLEL_OP(1, op_handle4)

    CREATE_SSTABLELIST_ADD_PARALLEL_OP(op_handle5,
                                       1,
                                       true,
                                       gc_info5)

    ABORT_SSTABLELIST_ADD_PARALLEL_OP(1, op_handle1)
    FAIL_SSTABLELIST_ADD_PARALLEL_OP(1, op_handle5)

    ObSSLogIteratorGuard iter(true, true);
    ObMetaVersionRange version_range;
    ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_range_meta_value(param,
                                                      version_range,
                                                      iter));
    #define GET_AND_CHECK_NEXT_ROW(state, op_id, end_op_id, gc_info)    \
    { \
      ObAtomicExtraInfo extra_info_deserialize_ret;\
      SCN row_scn_ret; \
      ObSSLogMetaType meta_type_ret1; \
      ObAtomicMetaKey meta_key_ret1; \
      ObSSTableGCInfo gc_info_ret1; \
      ObString key1; \
      ObString value1; \
      ObString info1; \
      int64_t pos = 0; \
      ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row_scn_ret, meta_type_ret1, key1, value1, info1)); \
      ObSSLogMetaKey sslog_meta_key_ret1; \
      ASSERT_EQ(OB_SUCCESS, sslog_meta_key_ret1.deserialize(key1.ptr(), key1.length(), pos)); \
      ASSERT_EQ(param.tablet_level_param_.ls_id_, sslog_meta_key_ret1.tablet_meta_key_.ls_id_); \
      ASSERT_EQ(param.tablet_level_param_.tablet_id_, sslog_meta_key_ret1.tablet_meta_key_.tablet_id_); \
      ASSERT_EQ(param.tablet_level_param_.reorganization_scn_, sslog_meta_key_ret1.tablet_meta_key_.reorganization_scn_); \
      ASSERT_EQ(true, meta_type_ret1 == param.meta_type_); \
      pos = 0; \
      ASSERT_EQ(OB_SUCCESS, extra_info_deserialize_ret.deserialize(info1.ptr(), info1.length(), pos)); \
      ASSERT_EQ(op_id, extra_info_deserialize_ret.meta_info_.op_id_); \
      ASSERT_EQ(end_op_id, extra_info_deserialize_ret.meta_info_.end_op_id_); \
      ASSERT_EQ(state, extra_info_deserialize_ret.meta_info_.state_); \
      pos = 0; \
      ASSERT_EQ(OB_SUCCESS, gc_info_ret1.deserialize(extra_info_deserialize_ret.gc_info_.get_ob_string().ptr(), extra_info_deserialize_ret.gc_info_.get_ob_string().length(), pos)); \
      ASSERT_EQ(gc_info, gc_info_ret1); \
    }
    // check multi version row
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::ABORTED, 5, 5, gc_info5)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::INIT, 5, 4, gc_info5)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::COMMITTED, 4, 4, gc_info4)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::COMMITTED, 4, 3, gc_info3)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::INIT, 4, 2, gc_info4)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::ABORTED, 3, 2, gc_info2)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::INIT, 3, 0, gc_info3)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::INIT, 2, 0, gc_info2)
    GET_AND_CHECK_NEXT_ROW(ObAtomicMetaInfo::State::INIT, 1, 0, gc_info1)
  }

  {
    // create more than the limited num of op
  }
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf), "%s/%lu_atomic_proto?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=2000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  LOG_INFO("main>>>", K(cur_time_ns));
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
