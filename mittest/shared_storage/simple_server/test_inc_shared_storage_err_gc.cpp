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

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST

#include "storage/tx_storage/ob_ls_service.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "close_modules/shared_storage/storage/incremental/ob_shared_meta_service.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_sstablelist_define.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_ss_garbage_collector.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"

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
} // namespace sslog
} // namespace oceanbase

namespace oceanbase
{
char *shared_storage_info = NULL;
namespace storage
{
class MockSSGC : public ObSSGarbageCollector
{
public:
  static int build_gc_task_with_gc_start_and_end_scn_(
    const SCN &snapshot,
    const bool is_for_sslog_table,
    const LastSuccSCNs &last_succ_scns,
    const GCType &gc_type,
    const SCN &gc_start_scn,
    const SCN &gc_end_scn,
    ObArenaAllocator &allocator,
    ObIArray<ObFunction<SSGCTaskRet(const ObLSID, const ObTabletID, const SCN)>> &gc_task_execute_list)
  {
    int ret = OB_SUCCESS;
    void *ptr = nullptr;
    FailGCTask *gc_task = nullptr;
    ptr = static_cast<FailGCTask *>(allocator.alloc(sizeof(FailGCTask)));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate gc_task", KR(ret));
    } else {
      gc_task = new (ptr) FailGCTask(gc_type, snapshot);
      ptr = nullptr;
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(gc_task->gc_start_scn_ = gc_start_scn)) {
    } else if (FALSE_IT(gc_task->gc_end_scn_ = gc_end_scn)) {
    } else if (OB_FAIL(gc_task_execute_list.push_back(*gc_task))) {
      LOG_WARN("push gc_task into gc_task_execute_list failed",
               KR(ret),
               K(last_succ_scns),
               K(is_for_sslog_table),
               KPC(gc_task));
    }
    return ret;
  }

  static int gc_tenant_in_ss_with_gc_start_and_end_scn_(const bool is_for_sslog_table,
                                                        LastSuccSCNs &last_succ_scns,
                                                        const GCType &gc_type,
                                                        const SCN &gc_start_scn,
                                                        const SCN &gc_end_scn)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    SCN snapshot;
    SCN tenant_gc_scn;
    tenant_gc_scn.set_max();
  ObArray<SSGCTaskRet> tenant_results;

    ObArray<ObFunction<SSGCTaskRet(const ObLSID, const ObTabletID, const SCN)>> gc_task_execute_list;
    ObArray<ObLSID> ls_id_list;

    if (OB_FAIL(MTL(ObSSMetaService *)->get_max_committed_meta_scn(SYS_LS, snapshot))) {
      LOG_WARN("get max_committed_meta_scn from ss_meta_srv failed", KR(ret), K(snapshot));
    } else if (OB_FAIL(get_ls_id_list_(snapshot, ls_id_list))) {
      LOG_WARN("get ls_id_list failed", KR(ret), K(snapshot));
    } else if (OB_FAIL(build_gc_task_with_gc_start_and_end_scn_(snapshot,
                                                                is_for_sslog_table,
                                                                last_succ_scns,
                                                                gc_type,
                                                                gc_start_scn,
                                                                gc_end_scn,
                                                                allocator,
                                                                gc_task_execute_list))) {
      LOG_WARN("build gc_task with gc_start_scn and gc_end_scn failed", KR(ret), K(snapshot));
    } else {
      for (int64_t i = 0; i < ls_id_list.count() && OB_SUCC(ret); i++) {
        const ObLSID ls_id = ls_id_list.at(i);
        SCN ls_gc_scn;
        ObArray<SSGCTaskRet> ls_results;
        if (OB_FAIL(gc_log_stream_in_ss_(snapshot, ls_id, gc_task_execute_list, ls_gc_scn, ls_results))) {
          LOG_WARN("gc log_streams in ss failed", KR(ret), K(snapshot));
        } else if (ls_gc_scn.is_valid()) {
          tenant_gc_scn = SCN::min(tenant_gc_scn, ls_gc_scn);
        }
        if (OB_SUCC(ret) && !ls_results.empty()) {
          if (OB_FAIL(process_ls_results_(ls_results, last_succ_scns))) {
            LOG_WARN("process ls results failed", KR(ret), K(ls_id));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < ls_results.count(); j++) {
              if (OB_FAIL(tenant_results.push_back(ls_results.at(j)))) {
                LOG_WARN("push ls result to all results failed", KR(ret), K(ls_id));
                break;
              }
            }
          }
          ls_results.reset();
        }

        FLOG_INFO("finish gc log stream", KR(ret), K(snapshot), K(ls_id), K(last_succ_scns));
      }
    }
    if (OB_SUCC(ret) && !tenant_results.empty()) {
      if (OB_FAIL(set_last_succ_scns_(tenant_gc_scn, tenant_results, last_succ_scns))) {
        LOG_WARN("set last success scns failed", KR(ret), K(tenant_gc_scn));
      }
    }

    return ret;
  }

  static int gc_tenant_in_ss_(const SCN &snapshot,
                              const bool is_for_sslog_table,
                              const GCType &gc_type,
                              LastSuccSCNs &last_succ_scns)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SsGcTask"));
    ObArray<ObFunction<SSGCTaskRet(const ObLSID, const ObTabletID, const SCN)>> gc_task_execute_list;
    ObArray<ObFuture<SSGCTaskRet>> results;
    ObArray<ObLSID> ls_id_list;
    SCN tenant_gc_scn = SCN::max_scn();

    if (OB_FAIL(build_gc_task_into_list_(
          snapshot, is_for_sslog_table, last_succ_scns, gc_type, allocator, gc_task_execute_list))) {
      LOG_WARN("build gc_task list failed", KR(ret), K(snapshot), K(is_for_sslog_table), K(last_succ_scns));
    } else if (is_for_sslog_table) {
      ObLSID ls_id = SSLOG_LS;
      if (OB_UNLIKELY(OB_SYS_TENANT_ID != MTL_ID())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not recycle sslog_table if tenant is not sys", KR(ret));
      } else if (OB_FAIL(ls_id_list.push_back(ls_id))) {
          LOG_WARN("add ls_id into ls_id_list failed", KR(ret), K(ls_id), K(last_succ_scns));
      }
    } else {
      ObSSMetaService *ss_meta_srv = nullptr;
      SCN snapshot;
      if (OB_ISNULL(ss_meta_srv = MTL(ObSSMetaService *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ObSSMetaService should not be null", KR(ret));
      } else if (OB_FAIL(ss_meta_srv->get_max_committed_meta_scn(SYS_LS, snapshot))) {
          LOG_WARN("get snapshot failed", KR(ret));
      } else if (OB_FAIL(get_ls_id_list_(snapshot, ls_id_list))) {
          LOG_WARN("get ls_id_list failed", KR(ret), K(snapshot));
      }
    }

    if (FAILEDx(OB_FAIL(gc_log_streams_in_ss_(ls_id_list, is_for_sslog_table, last_succ_scns)))) {
      LOG_WARN("gc ts in ss failed", KR(ret), K(last_succ_scns));
    }
    return ret;
  }
};
}  // namespace storage
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
  ObLS *ls_;
};

TestRunCtx RunCtx;

class ObCheckDirEmptOp : public ObBaseDirEntryOperator
{
public:
  ObCheckDirEmptOp() : file_cnt_(0) {}
  ~ObCheckDirEmptOp() {}
  void reset() { file_cnt_ = 0; }
  int func(const dirent *entry) override;
  int64_t get_file_cnt() {return file_cnt_;}
  TO_STRING_KV(K_(file_cnt));
private:
  int64_t file_cnt_;
};

int ObCheckDirEmptOp::func(const dirent *entry)
{
  if (OB_ISNULL(entry->d_name)) {
    SERVER_LOG(INFO, "d_name is null");
  } else {
    SERVER_LOG(INFO, "dir_entry", K(entry->d_name));
  }
  file_cnt_++;
  return OB_ERR_EXIST_OBJECT;
}

static void insert_sslog(
    const sslog::ObSSLogMetaType sslog_type,
    const int64_t op_id,
    const ObAtomicMetaInfo::State state,
    const ObSSTableGCInfo &gc_info)
{
  int64_t affected_rows = 0;
  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog_type, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  ObAtomicExtraInfo extra_info;
  extra_info.meta_info_.op_id_ = op_id;
  extra_info.meta_info_.epoch_id_ = 1;
  extra_info.meta_info_.state_ = state;
  extra_info.meta_info_.not_exist_ = false;
  int64_t gc_info_size = gc_info.get_serialize_size();
  char *gc_info_buf = (char *)mtl_malloc(gc_info_size, "gc_unittest");
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, gc_info.serialize(gc_info_buf, gc_info_size, pos));
  ASSERT_EQ(OB_SUCCESS, extra_info.gc_info_.assign(ObString(pos, gc_info_buf)));

  ObAtomicMetaKey meta_key;
  const common::ObString meta_value = "bizhu_test";


  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param, meta_key));
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::insert_sslog_row(param.meta_type_,
                                                       meta_key.get_string_key(),
                                                       meta_value,
                                                       extra_info,
                                                       affected_rows));
}

static void gen_block_id(
    const int64_t op_id,
    const ObStorageObjectType type,
    const int64_t seq,
    MacroBlockId &block_id,
    const int64_t cg_id = 0)
{
  blocksstable::ObStorageObjectOpt opt;
  if (ObStorageObjectType::SHARED_TABLET_SUB_META == type) {
    opt.set_ss_tablet_sub_meta_opt(RunCtx.ls_id_.id(), RunCtx.tablet_id_.id(), op_id,
        seq /* start_seq */, RunCtx.tablet_id_.is_ls_inner_tablet(), 0);

  } else if (ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == type) {
    opt.set_ss_share_object_opt(type, RunCtx.tablet_id_.is_ls_inner_tablet(), RunCtx.ls_id_.id(),
        RunCtx.tablet_id_.id(), seq, cg_id /* column_group_id */, 0);
  } else {
    opt.set_ss_share_object_opt(type, RunCtx.tablet_id_.is_ls_inner_tablet(), RunCtx.ls_id_.id(),
        RunCtx.tablet_id_.id(), (op_id << 32) | (seq & 0xFFFFFFFF), cg_id /* column_group_id */, 0);

  }
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
  ASSERT_TRUE(block_id.is_valid());
}

static void write_block(
    const MacroBlockId &block_id)
{

  ObStorageObjectWriteInfo write_info;

  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(block_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
}

template<typename T>
static void update_sslog(
    const sslog::ObSSLogMetaType sslog_type,
    const int64_t op_id,
    const ObAtomicMetaInfo::State state,
    const T *gc_info = NULL,
    const ObSSMetaUpdateMetaInfo *meta_info = NULL)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  int64_t pos = 0;
  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog_type, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  ObAtomicMetaKey meta_key;
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::get_meta_key(param, meta_key));

  // read
  share::SCN row_scn_ret;
  ObString value;
  ObSSLogIteratorGuard iter(true, true, true);
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog_type, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::read_meta_row(param,
                                                    share::SCN::invalid_scn(),
                                                    iter));
  sslog::ObSSLogMetaType type;
  ObString key;
  ObString info;

  ret = iter.get_next_row(row_scn_ret, type, key, value, info);
  if (OB_FAIL(ret)) {
    SERVER_LOG(WARN, "failed to get_next_row", K(key), K(value), K(sslog_type), K(op_id), K(state));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(type, param.meta_type_);
  ASSERT_EQ(key, meta_key.get_string_key());
  ASSERT_EQ(true, row_scn_ret > SCN::min_scn());


  ObAtomicExtraInfo check_extra_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, check_extra_info.deserialize(info.ptr(), info.length(), pos));
  SERVER_LOG(INFO, "read for update sslog row", K(key), K(value), K(check_extra_info), K(sslog_type), K(op_id), K(state));

  // update
  ObAtomicExtraInfo extra_info;
  extra_info.assign(check_extra_info);
  extra_info.meta_info_.op_id_ = op_id;
  extra_info.meta_info_.state_ = state;

  if (NULL != gc_info)
  {
    const int64_t gc_info_size = gc_info->get_serialize_size();
    char *gc_info_buf = (char *)mtl_malloc(gc_info_size, "gc_unittest");
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, gc_info->serialize(gc_info_buf, gc_info_size, pos));
    ASSERT_EQ(OB_SUCCESS, extra_info.gc_info_.assign(ObString(pos, gc_info_buf)));
  }

  if (NULL != meta_info) {
    extra_info.meta_info_.not_exist_ = false;
    pos = 0;
    ObSSTabletSSLogValue ret_meta_value;
    ObMetaDiskAddr addr;
    addr.type_ = 2;
    addr.second_id_ = 0;
    addr.size_ = ObLogConstants::MAX_LOG_FILE_SIZE;
    ASSERT_TRUE(addr.is_valid());

    common::ObArenaAllocator allocator;
    ASSERT_EQ(OB_SUCCESS, ret_meta_value.deserialize(allocator, addr, value.ptr(), value.length(), pos));
    SERVER_LOG(INFO, "read for update sslog row (update_meta_info)", K(key), K(value), K(check_extra_info), K(ret_meta_value.get_meta_info()), K(sslog_type), K(op_id), K(state));

    ObSSTabletSSLogValue meta_value;
    meta_value.set_meta_info(*meta_info);
    meta_value.tablet_ = ret_meta_value.tablet_;
    meta_value.data_version_ = DATA_CURRENT_VERSION;
    meta_value.macro_info_ = ret_meta_value.tablet_->macro_info_addr_.get_ptr();
    const int64_t meta_value_size = meta_value.get_serialize_size();
    char *meta_value_buf = (char*)mtl_malloc(meta_value_size, "gc_unittest");
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, meta_value.serialize(meta_value_buf, meta_value_size, pos));
    value.assign_ptr(meta_value_buf, meta_value_size);
  }

  // write
  ASSERT_EQ(OB_SUCCESS, ObAtomicFile::update_sslog_row(param.meta_type_,
                                                       meta_key.get_string_key(),
                                                       value,
                                                       info,
                                                       extra_info,
                                                       false,
                                                       affected_rows));
}

class ObSharedStorageTest : public ObSimpleClusterTestBase
{
public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageTest()
    : ObSimpleClusterTestBase("test_inc_shared_storage_err_gc_", "50G", "50G", "50G")
  {}
  void wait_upload_sstable(const int64_t ss_checkpoint_scn);
  void wait_minor_finish();
  void wait_major_finish();
  void wait_tablet_gc_finish();
  void wait_shared_tablet_gc_finish();
  void get_ss_checkpoint_scn(int64_t &ss_checkpoint_scn);
  void set_ls_and_tablet_id_for_run_ctx();

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


  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }
};

TEST_F(ObSharedStorageTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

#define EXE_SQL(sql_str)                                            \
  LOG_INFO("exe sql start", K(sql_str));      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows)); \
  LOG_INFO("exe sql end", K(sql_str));

#define SYS_EXE_SQL(sql_str)                                            \
  LOG_INFO("sys exe sql start", K(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows));  \
  LOG_INFO("sys exe sql end", K(sql_str));

TEST_F(ObSharedStorageTest, add_tenant)
{
    TRANS_LOG(INFO, "create_tenant start");
    wait_sys_to_leader();
    int ret = OB_SUCCESS;
    int retry_cnt = 0;
    do {
      if (OB_FAIL(create_tenant("tt1"))) {
        TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret), K(retry_cnt));
        ob_usleep(15 * 1000 * 1000); // 15s
      }
      retry_cnt++;
    } while (OB_FAIL(ret) && retry_cnt < 10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObSharedStorageTest, test_timeout_block_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  share::SCN ss_checkpoint_scn;
  share::SCN gc_start;
  share::SCN gc_end;
  share::SCN snapshot;

  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");

  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze;");
  RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
  wait_minor_finish();
  wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());

  MTL(ObSSMetaService*)->get_max_committed_meta_scn(gc_start);

  // 1. sstable block write
  MacroBlockId block_id_100_1;
  MacroBlockId block_id_100_2;
  MacroBlockId block_id_100_3;
  MacroBlockId not_exist_block_id_101_1;
  MacroBlockId not_exist_block_id_101_2;
  MacroBlockId not_exist_block_id_101_3;
  MacroBlockId block_id_102_1;
  MacroBlockId block_id_102_2;
  MacroBlockId block_id_102_3;
  MacroBlockId block_id_103_1;
  MacroBlockId block_id_103_2;
  MacroBlockId block_id_103_3;
  // todo  not delete
  MacroBlockId block_id_104_1;
  MacroBlockId block_id_104_2;
  MacroBlockId block_id_104_3;
  MacroBlockId block_id_105_1;
  MacroBlockId block_id_105_2;
  MacroBlockId block_id_105_3;

  ObSSTableGCInfo gc_info_100;
  gc_info_100.parallel_cnt_ = 3;
  gc_info_100.seq_step_ = 1000;
  gc_info_100.data_seq_bits_ = 32;
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_100_1);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_100_2);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_100_3);

  ObSSTableGCInfo gc_info_101;
  gc_info_101.parallel_cnt_ = 3;
  gc_info_101.seq_step_ = 1000;
  gc_info_101.data_seq_bits_ = 32;
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, not_exist_block_id_101_1);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, not_exist_block_id_101_2);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, not_exist_block_id_101_3);

  ObSSTableGCInfo gc_info_102;
  gc_info_102.parallel_cnt_ = 3;
  gc_info_102.seq_step_ = 1000;
  gc_info_102.data_seq_bits_ = 32;
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_102_1);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_102_2);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_102_3);

  ObSSTableGCInfo gc_info_103;
  gc_info_103.parallel_cnt_ = 3;
  gc_info_103.seq_step_ = 1000;
  gc_info_103.data_seq_bits_ = 32;
  gen_block_id(103, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_103_1);
  gen_block_id(103, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_103_2);
  gen_block_id(103, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_103_3);

  ObSSTableGCInfo gc_info_104;
  gc_info_104.parallel_cnt_ = 3;
  gc_info_104.seq_step_ = 1000;
  gc_info_104.data_seq_bits_ = 32;
  gen_block_id(104, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_104_1);
  gen_block_id(104, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_104_2);
  gen_block_id(104, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_104_3);

  ObSSTableGCInfo gc_info_105;
  gc_info_105.parallel_cnt_ = 3;
  gc_info_105.seq_step_ = 1000;
  gc_info_104.data_seq_bits_ = 32;
  gen_block_id(105, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_105_1);
  gen_block_id(105, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_105_2);
  gen_block_id(105, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_105_3);

  write_block(block_id_100_1);
  write_block(block_id_100_2);
  write_block(block_id_100_3);
  write_block(not_exist_block_id_101_1);
  write_block(not_exist_block_id_101_2);
  write_block(not_exist_block_id_101_3);
  write_block(block_id_102_1);
  write_block(block_id_102_2);
  write_block(block_id_102_3);
  write_block(block_id_103_1);
  write_block(block_id_103_2);
  write_block(block_id_103_3);
  write_block(block_id_104_1);
  write_block(block_id_104_2);
  write_block(block_id_104_3);
  write_block(block_id_105_1);
  write_block(block_id_105_2);
  write_block(block_id_105_3);

  // sstable sslog write
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &gc_info_100);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::COMMITTED, &gc_info_100);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 101, ObAtomicMetaInfo::State::INIT, &gc_info_101);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 102, ObAtomicMetaInfo::State::INIT, &gc_info_102);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 103, ObAtomicMetaInfo::State::INIT, &gc_info_103);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 103, ObAtomicMetaInfo::State::COMMITTED, &gc_info_103);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 104, ObAtomicMetaInfo::State::INIT, &gc_info_104);

  sleep(1);
  MTL(ObSSMetaService*)->get_max_committed_meta_scn(gc_end);

  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 105, ObAtomicMetaInfo::State::INIT, &gc_info_105);


  // tablet_meta sslog write
  ObSSMetaUpdateMetaInfo update_info_102;
  update_info_102.acquire_scn_.set_min();
  update_info_102.update_reason_ = ObMetaUpdateReason::TABLET_UPLOAD_DATA_MINI_SSTABLE;
  update_info_102.sstable_op_id_ = 102;
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 100, ObAtomicMetaInfo::State::INIT, NULL, &update_info_102);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 101, ObAtomicMetaInfo::State::INIT, NULL, &update_info_102);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 101, ObAtomicMetaInfo::State::COMMITTED, NULL, &update_info_102);


  ObSSMetaUpdateMetaInfo update_info_104;
  update_info_104.update_reason_ = ObMetaUpdateReason::TABLET_UPLOAD_DATA_MINI_SSTABLE;
  update_info_104.sstable_op_id_ = 104;
  update_info_104.acquire_scn_.set_min();
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 102, ObAtomicMetaInfo::State::INIT, NULL, &update_info_104);

  // error gc
  LastSuccSCNs last_succ_scns;
  LOG_INFO("start to do timeout_sstable_block gc");
  ASSERT_EQ(OB_SUCCESS,
            MockSSGC::gc_tenant_in_ss_with_gc_start_and_end_scn_(
              false, last_succ_scns, GCType::TIMEOUT_SSTABLE_BLOCK_GC, gc_start, gc_end));

  ASSERT_TRUE(last_succ_scns.get_gc_timeout_sstable_block_scn() < gc_end);
  bool is_exist = false;

  // check sstable block gc
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_100_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_100_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_100_3, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id_101_1, 0, is_exist));
  ASSERT_TRUE(!is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id_101_2, 0, is_exist));
  ASSERT_TRUE(!is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id_101_3, 0, is_exist));
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_102_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_102_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_102_3, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_103_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_103_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_103_3, 0, is_exist));
  ASSERT_TRUE(is_exist);

  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_104_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_104_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_104_3, 0, is_exist));
  ASSERT_TRUE(is_exist);

  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_105_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_105_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_105_3, 0, is_exist));
  ASSERT_TRUE(is_exist);

  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 105, ObAtomicMetaInfo::State::COMMITTED, &gc_info_105);

  MTL(ObSSMetaService*)->get_max_committed_meta_scn(gc_end);
  // error gc
  LOG_INFO("start to do timeout_sstable_block gc");
  ASSERT_EQ(OB_SUCCESS,
            MockSSGC::gc_tenant_in_ss_with_gc_start_and_end_scn_(
              false, last_succ_scns, GCType::TIMEOUT_SSTABLE_BLOCK_GC, gc_start, gc_end));
  ASSERT_EQ(last_succ_scns.get_gc_timeout_sstable_block_scn(), gc_end);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");

}

TEST_F(ObSharedStorageTest, test_abort_block_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  share::SCN ss_checkpoint_scn;
  share::SCN gc_start;
  share::SCN snapshot;

  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");

  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze;");
  RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
  wait_minor_finish();
  wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());

  MTL(ObSSMetaService*)->get_max_committed_meta_scn(gc_start);

  // sstable block write
  MacroBlockId block_id_100_1;
  MacroBlockId block_id_100_2;
  MacroBlockId block_id_100_3;
  MacroBlockId not_exist_block_id_101_1;
  MacroBlockId not_exist_block_id_101_2;
  MacroBlockId not_exist_block_id_101_3;
  MacroBlockId block_id_102_1;
  MacroBlockId block_id_102_2;
  MacroBlockId block_id_102_3;
  MacroBlockId block_id_103_1;
  MacroBlockId block_id_103_2;
  MacroBlockId block_id_103_3;
  MacroBlockId block_id_104_1;
  MacroBlockId block_id_104_2;
  MacroBlockId block_id_104_3;

  ObSSTableGCInfo gc_info_100;
  gc_info_100.parallel_cnt_ = 3;
  gc_info_100.seq_step_ = 1000;
  gc_info_100.data_seq_bits_ = 32;
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_100_1);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_100_2);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_100_3);

  ObSSTableGCInfo gc_info_101;
  gc_info_101.parallel_cnt_ = 3;
  gc_info_101.seq_step_ = 1000;
  gc_info_101.data_seq_bits_ = 32;
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, not_exist_block_id_101_1);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, not_exist_block_id_101_2);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, not_exist_block_id_101_3);

  ObSSTableGCInfo gc_info_102;
  gc_info_102.parallel_cnt_ = 3;
  gc_info_102.seq_step_ = 1000;
  gc_info_102.data_seq_bits_ = 32;
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_102_1);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_102_2);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_102_3);

  ObSSTableGCInfo gc_info_103;
  gc_info_103.parallel_cnt_ = 3;
  gc_info_103.seq_step_ = 1000;
  gc_info_103.data_seq_bits_ = 32;
  gen_block_id(103, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_103_1);
  gen_block_id(103, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_103_2);
  gen_block_id(103, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_103_3);

  ObSSTableGCInfo gc_info_104;
  gc_info_104.parallel_cnt_ = 3;
  gc_info_104.seq_step_ = 1000;
  gc_info_104.data_seq_bits_ = 32;
  gen_block_id(104, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, block_id_104_1);
  gen_block_id(104, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, block_id_104_2);
  gen_block_id(104, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, block_id_104_3);

  write_block(block_id_100_1);
  write_block(block_id_100_2);
  write_block(block_id_100_3);
  write_block(not_exist_block_id_101_1);
  write_block(not_exist_block_id_101_2);
  write_block(not_exist_block_id_101_3);
  write_block(block_id_102_1);
  write_block(block_id_102_2);
  write_block(block_id_102_3);
  write_block(block_id_103_1);
  write_block(block_id_103_2);
  write_block(block_id_103_3);

  // sstable sslog write
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &gc_info_100);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::COMMITTED, &gc_info_100);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 101, ObAtomicMetaInfo::State::INIT, &gc_info_101);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 101, ObAtomicMetaInfo::State::ABORTED, &gc_info_101);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 102, ObAtomicMetaInfo::State::INIT, &gc_info_102);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 102, ObAtomicMetaInfo::State::COMMITTED, &gc_info_102);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 103, ObAtomicMetaInfo::State::INIT, &gc_info_103);

  // tablet_meta block write
  MacroBlockId meta_block_id1;
  MacroBlockId not_exist_meta_block_id2;
  MacroBlockId meta_block_id3;
  MacroBlockId meta_block_id4;
  MacroBlockId not_exist_meta_block_id5;
  MacroBlockId meta_block_id6;
  gen_block_id(100, ObStorageObjectType::SHARED_TABLET_SUB_META, 0, meta_block_id1);
  gen_block_id(101, ObStorageObjectType::SHARED_TABLET_SUB_META, 0, not_exist_meta_block_id2);
  gen_block_id(101, ObStorageObjectType::SHARED_TABLET_SUB_META, 2000, meta_block_id3);
  gen_block_id(102, ObStorageObjectType::SHARED_TABLET_SUB_META, 0, meta_block_id4);
  gen_block_id(103, ObStorageObjectType::SHARED_TABLET_SUB_META, 0, not_exist_meta_block_id5);
  gen_block_id(104, ObStorageObjectType::SHARED_TABLET_SUB_META, 0, meta_block_id6);
  write_block(meta_block_id1);
  write_block(not_exist_meta_block_id2);
  write_block(meta_block_id3);
  write_block(meta_block_id4);
  write_block(not_exist_meta_block_id5);
  write_block(meta_block_id6);

  // tablet_meta sslog write
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 100, ObAtomicMetaInfo::State::INIT);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 100, ObAtomicMetaInfo::State::COMMITTED);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 101, ObAtomicMetaInfo::State::INIT);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 101, ObAtomicMetaInfo::State::ABORTED);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 102, ObAtomicMetaInfo::State::INIT);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 102, ObAtomicMetaInfo::State::COMMITTED);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 103, ObAtomicMetaInfo::State::INIT);
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 104, ObAtomicMetaInfo::State::INIT);

  // error gc
  LastSuccSCNs last_succ_scns;
  last_succ_scns.gc_abort_tablet_meta_block_scn_ = gc_start;
  MTL(ObSSMetaService*)->get_max_committed_meta_scn(snapshot);
  LOG_INFO("start to do abort_tablet_meta_block gc");
  ASSERT_EQ(OB_SUCCESS, MockSSGC::gc_tenant_in_ss_(snapshot, false, GCType::ABORT_TABLET_META_BLOCK_GC, last_succ_scns));
  LOG_INFO("start to do abort_sstable_block gc");
  ASSERT_EQ(OB_SUCCESS, MockSSGC::gc_tenant_in_ss_(snapshot, false, GCType::ABORT_SSTABLE_BLOCK_GC, last_succ_scns));

  bool is_exist = false;

  // check sstable block gc
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_100_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_100_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_100_3, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id_101_1, 0, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id_101_2, 0, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id_101_3, 0, is_exist));
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_102_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_102_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_102_3, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_103_1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_103_2, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id_103_3, 0, is_exist));
  ASSERT_TRUE(is_exist);

  // check tablet meta block gc
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(meta_block_id1, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_meta_block_id2, 0, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(meta_block_id3, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(meta_block_id4, 0, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_meta_block_id5, 0, is_exist));
  ASSERT_FALSE(is_exist);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
}

TEST_F(ObSharedStorageTest, test_tablet_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  share::SCN ss_checkpoint_scn;

  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");
  EXE_SQL("alter system set _ss_garbage_collect_interval = '10s';");
  EXE_SQL("alter system set _ss_garbage_collect_file_expiration_time = '10s';");
  EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");


  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze;");
  RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
  wait_minor_finish();
  wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());

  SYS_EXE_SQL("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;");
  EXE_SQL("alter system major freeze;");

  wait_major_finish();

  // major sstable block write
  MacroBlockId major_block_id11;
  MacroBlockId major_block_id12;
  MacroBlockId major_block_id13;

  MacroBlockId major_block_id21;
  MacroBlockId major_block_id22;
  MacroBlockId major_block_id23;

  MacroBlockId major_block_id31;
  MacroBlockId major_block_id32;
  MacroBlockId major_block_id33;
  ObSSMajorGCInfo major_gc_info;

  major_gc_info.parallel_cnt_ = 3;
  major_gc_info.seq_step_ = 1000;
  major_gc_info.start_seq_ = 0;
  major_gc_info.cg_cnt_ = 3;
  major_gc_info.snapshot_version_ = INT64_MAX;

  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 0, major_block_id11, 0);
  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1000, major_block_id12, 0);
  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2000, major_block_id13, 0);

  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 0, major_block_id21, 1);
  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1000, major_block_id22, 1);
  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2000, major_block_id23, 1);

  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 0, major_block_id31, 2);
  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1000, major_block_id32, 2);
  gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2000, major_block_id33, 2);

  write_block(major_block_id11);
  write_block(major_block_id12);
  write_block(major_block_id13);

  write_block(major_block_id21);
  write_block(major_block_id22);
  write_block(major_block_id23);

  write_block(major_block_id31);
  write_block(major_block_id32);
  write_block(major_block_id33);


  // major sstable sslog write
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MAJOR_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &major_gc_info);


  // sstable block write
  MacroBlockId mini_block_id1;
  MacroBlockId mini_block_id2;
  MacroBlockId mini_block_id3;

  ObSSTableGCInfo mini_gc_info;
  mini_gc_info.parallel_cnt_ = 3;
  mini_gc_info.seq_step_ = 1000;
  mini_gc_info.data_seq_bits_ = 32;
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, mini_block_id1);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 1000, mini_block_id2);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 2000, mini_block_id3);


  write_block(mini_block_id1);
  write_block(mini_block_id2);
  write_block(mini_block_id3);

  // sstable sslog write
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &mini_gc_info);

  // tablet_meta block write
  MacroBlockId meta_block_id1;

  gen_block_id(100, ObStorageObjectType::SHARED_TABLET_SUB_META, 0, meta_block_id1);
  write_block(meta_block_id1);

  // tablet_meta sslog write
  update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 100, ObAtomicMetaInfo::State::INIT);


  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
  EXE_SQL("alter system minor freeze;");
  wait_tablet_gc_finish();
  wait_shared_tablet_gc_finish();
}

// TEST_F(ObSharedStorageTest, test_gc_major_for_drop_tablet)
// {
//   share::ObTenantSwitchGuard tguard;
//   ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
//   ObSqlString sql;
//   int64_t affected_rows = 0;
//   int64_t tablet_version1 = 0;
//   int64_t tablet_version2 = 0;
//   int64_t tablet_version3 = 0;
//   EXE_SQL("create table test_table (a int)");
//   LOG_INFO("create_table finish");
//
//   set_ls_and_tablet_id_for_run_ctx();
//
//   share::SCN ss_checkpoint_scn;
//
//   ObSSMetaReadParam param;
//   share::SCN transfer_scn;
//   transfer_scn.set_min();
//   param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);
//
//   EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
//   EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");
//   EXE_SQL("alter system set _ss_garbage_collect_interval = '10s';");
//   EXE_SQL("alter system set _ss_garbage_collect_file_expiration_time = '10s';");
//   EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");
//
//
//   sleep(5);
//   EXE_SQL("insert into test_table values (1)");
//   LOG_INFO("insert data finish");
//
//   EXE_SQL("alter system minor freeze;");
//   RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
//   wait_minor_finish();
//   wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());
//
//   SYS_EXE_SQL("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;");
//   EXE_SQL("alter system major freeze;");
//
//   wait_major_finish();
//
//   // major sstable block write
//   MacroBlockId major_block_id11;
//   MacroBlockId major_block_id12;
//   MacroBlockId major_block_id13;
//
//   MacroBlockId major_block_id21;
//   MacroBlockId major_block_id22;
//   MacroBlockId major_block_id23;
//
//   MacroBlockId major_block_id31;
//   MacroBlockId major_block_id32;
//   MacroBlockId major_block_id33;
//   ObSSMajorGCInfo major_gc_info;
//
//   major_gc_info.parallel_cnt_ = 3;
//   major_gc_info.snapshot_version_ = 1000;
//   major_gc_info.seq_step_ = 1000;
//   major_gc_info.start_seq_ = 0;
//   major_gc_info.cg_cnt_ = 3;
//
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 0, major_block_id11, 0);
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1000, major_block_id12, 0);
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2000, major_block_id13, 0);
//
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 0, major_block_id21, 1);
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1000, major_block_id22, 1);
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2000, major_block_id23, 1);
//
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 0, major_block_id31, 2);
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1000, major_block_id32, 2);
//   gen_block_id(100, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2000, major_block_id33, 2);
//
//   write_block(major_block_id11);
//   write_block(major_block_id12);
//   write_block(major_block_id13);
//
//   write_block(major_block_id21);
//   write_block(major_block_id22);
//   write_block(major_block_id23);
//
//   write_block(major_block_id31);
//   write_block(major_block_id32);
//   write_block(major_block_id33);
//
//
//   // major sstable sslog write
//   update_sslog(sslog::ObSSLogMetaType::SSLOG_MAJOR_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &major_gc_info);
//
//   // tablet_meta sslog write
//   update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 100, ObAtomicMetaInfo::State::INIT);
//
//   // tablet_meta sslog write
//   ObSSMetaUpdateMetaInfo update_info_1000;
//   update_info_1000.acquire_scn_.set_min();
//   update_info_1000.update_reason_ = ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE;
//   // snapshot_version
//   update_info_1000.sstable_op_id_ = 1000;
//   update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 101, ObAtomicMetaInfo::State::INIT, NULL, &update_info_1000);
//   update_sslog<ObSSTableGCInfo>(sslog::ObSSLogMetaType::SSLOG_TABLET_META, 101, ObAtomicMetaInfo::State::COMMITTED, NULL, &update_info_1000);
//
//   share::SCN snapshot;
//   MTL(ObSSMetaService*)->get_max_committed_meta_scn(snapshot);
//   ObSSGarbageCollector::gc_failed_task_major_block_(snapshot, RunCtx.ls_id_, RunCtx.tablet_id_, SCN::min_scn());
//
//   // check sstable block gc
//
//   bool is_exist = false;
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id11, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id12, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id13, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id21, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id22, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id33, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id31, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id32, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//   ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(major_block_id33, 0, is_exist));
//   ASSERT_TRUE(is_exist);
//
//   EXE_SQL("drop table test_table;");
// }

void ObSharedStorageTest::wait_major_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  ObSqlString sql;
  int64_t affected_rows = 0;
  static int64_t old_major_scn = 1;
  int64_t new_major_scn = 1;
  int64_t scn = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select frozen_scn, (frozen_scn - last_scn) as result from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu;",
          RunCtx.tenant_id_));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("result", scn));
      ASSERT_EQ(OB_SUCCESS, result->get_int("frozen_scn", new_major_scn));
    }
    LOG_INFO("shared result", K(scn), K(new_major_scn));
    usleep(100 * 1000); // 100_ms
  } while (0 != scn || old_major_scn == new_major_scn);
  old_major_scn = new_major_scn;
  LOG_INFO("major finished", K(new_major_scn));
}

void ObSharedStorageTest::wait_shared_tablet_gc_finish()
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  ObCheckDirEmptOp shared_macro_op;
  ObMemAttr mem_attr(MTL_ID(), "test_gc");
  ObArenaAllocator allocator(mem_attr);
  do {
    shared_macro_op.reset();
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_dir(dir_path, sizeof(dir_path), RunCtx.tablet_id_.id()));
    ret = MTL(ObTenantFileManager*)->list_remote_files(dir_path, shared_macro_op);
    LOG_INFO("shared tablet dir", K(dir_path), K(ret), K(shared_macro_op));
    usleep(100 * 1000);
  } while (0 != shared_macro_op.get_file_cnt());

  do {
	ObTabletHandle ss_tablet;
    ret = MTL(ObSSMetaService*)->get_tablet(RunCtx.ls_id_, RunCtx.tablet_id_, share::SCN::min_scn(), allocator, ss_tablet);
    LOG_INFO("wait tablet meta", K(dir_path), K(ret));
  } while (OB_SUCCESS == ret);
}

void ObSharedStorageTest::wait_tablet_gc_finish()
{
  bool is_exist = false;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, RunCtx.tablet_id_.id()));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    LOG_INFO("private tablet_data dir exist", K(dir_path));
    usleep(100 *1000);
  } while (is_exist);

  memset(dir_path, 0, sizeof(dir_path));
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, RunCtx.ls_id_.id(), RunCtx.ls_epoch_, RunCtx.tablet_id_.id()));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    usleep(100 * 1000);
    LOG_INFO("private tablet_meta dir exist", K(dir_path));
  } while (is_exist);
}

void ObSharedStorageTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
          RunCtx.tenant_id_, RunCtx.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt > 0);
  LOG_INFO("minor finished", K(row_cnt));
}

void ObSharedStorageTest::wait_upload_sstable(const int64_t ss_checkpoint_scn)
{
  int64_t new_ss_checkpoint_scn = 0;
  while (new_ss_checkpoint_scn <= ss_checkpoint_scn) {
    get_ss_checkpoint_scn(new_ss_checkpoint_scn);
    LOG_INFO("wait upload sstable", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_), K(new_ss_checkpoint_scn), K(ss_checkpoint_scn));
    usleep(100 * 1000);
  }
}

void ObSharedStorageTest::get_ss_checkpoint_scn(int64_t &ss_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ss_checkpoint_scn = 0;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ss_checkpoint_scn from oceanbase.__all_virtual_ss_ls_meta where tenant_id=%lu and ls_id=%ld;",
        RunCtx.tenant_id_, RunCtx.ls_id_.id()));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ss_checkpoint_scn", ss_checkpoint_scn));
  }
}

void ObSharedStorageTest::set_ls_and_tablet_id_for_run_ctx()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t uid = 0;
  int64_t id = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign("select tablet_id from oceanbase.__all_virtual_table where table_name='test_table';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", uid));
  }
  RunCtx.tablet_id_ = uid;

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", uid));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  RunCtx.ls_id_ = ls->get_ls_id();
  RunCtx.ls_epoch_ = ls->get_ls_epoch();
  RunCtx.tenant_epoch_ = MTL_EPOCH_ID();
  RunCtx.ls_ = ls;
  LOG_INFO("finish set run ctx", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_));
}

TEST_F(ObSharedStorageTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf), "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
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

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
