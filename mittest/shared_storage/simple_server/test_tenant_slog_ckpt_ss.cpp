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
#define USING_LOG_PREFIX STORAGETEST

#define protected public
#define private public
#include "storage/tx_storage/ob_ls_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/shared_storage/simple_server/test_gc_util.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "storage/schema_utils.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#include "storage/slog_ckpt/ob_tenant_slog_checkpoint_workflow.h"
#include <thread>
#include <atomic>
#undef private
#undef protected

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
} // end namespace sslog

char *shared_storage_info = NULL;
using namespace common;
using namespace storage;
using omt::ObTenant;

static const bool VERBOSE = true;
static FILE *VERBOSE_OUT = stdout;
#define ASSERT_SUCC(expr)              \
do {                                   \
  ASSERT_EQ(OB_SUCCESS, ret = (expr)); \
} while(0);                            \

#define ASSERT_FAIL(expr)              \
do {                                   \
  ASSERT_NE(OB_SUCCESS, ret = (expr)); \
} while(0);                            \

#define EXPECT_SUCC(expr)              \
do {                                   \
  EXPECT_EQ(OB_SUCCESS, ret = (expr)); \
} while(0);                            \

#define LOG_AND_PRINT(level, fmt, args...)\
do {                                      \
  LOG_##level(fmt, args);                 \
  if (VERBOSE) {                          \
    fprintf(VERBOSE_OUT, fmt"\n");        \
  }                                       \
} while(0);                               \

namespace storage
{
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
  TRANS_LOG(INFO, "check lease sswriter", K(lease_epoch));
  return OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  sswriter_addr.addr_ = GCTX.self_addr();
  return ret;
}
} // end namespace storage

namespace unittest
{
class TestRunCtx
{
public:
  uint64_t TENANT_ID = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

const auto get_tablet_from_ls = std::bind(
  &ObLS::get_tablet,
  std::placeholders::_1,
  std::placeholders::_2,
  std::placeholders::_3,
  ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US * 10,
  ObMDSGetTabletMode::READ_WITHOUT_CHECK);

class TestTenantSlogCkptSS : public ObSimpleClusterTestBase
{
public:
  TestTenantSlogCkptSS()
    : ObSimpleClusterTestBase("test_tenant_slog_ckpt_", "50G", "50G", "50G")
  {
  }
  virtual ~TestTenantSlogCkptSS() = default;
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
      } else {
        break;
      }
      retry_cnt++;
    } while (OB_FAIL(ret) && retry_cnt < 10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  }

  void create_ls(int64_t &ls_id, ObLSHandle &ls_handle)
  {
    int ret = OB_SUCCESS;
    static int64_t inner_ls_id = 1000;
    ObCreateLSArg arg;
    const ObLSID id(++inner_ls_id);
    ASSERT_SUCC(gen_create_ls_arg(MTL_ID(), id, arg));
    ObLSService *ls_svr = nullptr;
    ASSERT_NE(nullptr, ls_svr = MTL(ObLSService *));
    ASSERT_SUCC(ls_svr->create_ls(arg));
    ASSERT_SUCC(ls_svr->get_ls(id, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls_handle.get_ls());
    ls_id = ls_handle.get_ls()->get_ls_id().id();

     // set member list
    ObMemberList member_list;
    ObLS *ls = ls_handle.get_ls();
    const int64_t paxos_replica_num = 1;
    (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
    GlobalLearnerList learner_list;
    ASSERT_SUCC(ls->set_initial_member_list(member_list, paxos_replica_num, learner_list));

    ObRole role;
    for (int i = 0; OB_SUCC(ret) && i < 15; i++) {
      int64_t proposal_id = 0;
      if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
        STORAGE_LOG(WARN, "failed to get role", K(ret));
      } else if (role == ObRole::LEADER) {
        break;
      }
      ::sleep(1);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, role is not leader", K(ret), K(role));
    }
  }

  void create_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      bool persist_tablet,
      const ObTabletStatus::Status tablet_status = ObTabletStatus::NORMAL)
  {
    int ret = OB_SUCCESS;
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, ls);
    tablet_handle.reset();

    ObArenaAllocator allocator;
    share::schema::ObTableSchema schema;
    TestSchemaUtils::prepare_data_schema(schema);
    ASSERT_SUCC(TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator, tablet_status));
    ASSERT_SUCC(get_tablet_from_ls(ls_handle.get_ls(), tablet_id, tablet_handle));
    ObTablet *tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
    if (persist_tablet) {
      uint64_t data_version = 0;
      ASSERT_SUCC(GET_MIN_DATA_VERSION(MTL_ID(), data_version));
      const ObTabletPersisterParam persist_param(data_version, ls->get_ls_id(), ls->get_ls_epoch(), tablet_id,
          tablet->get_transfer_seq(), 0);
      ObTabletHandle tmp_handle;
      ASSERT_SUCC(ObTabletPersister::persist_and_transform_tablet(persist_param, *tablet, tmp_handle));

      ObUpdateTabletPointerParam param;
      ASSERT_SUCC(tmp_handle.get_obj()->get_updating_tablet_pointer_param(param));
      ASSERT_SUCC(MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(ObTabletMapKey(ls->get_ls_id(), tablet_id), tablet_handle, tmp_handle, param));
    }
  }

  void create_ls_and_ntablets(
    int64_t &ls_id,
    const int64_t tablet_cnt)
  {
    int ret = OB_SUCCESS;
    ls_id = -1;
    ObLSHandle ls_handle;
    create_ls(ls_id, ls_handle);
    static int64_t base_tablet_id = 200100;
    ObTabletHandle tablet_handle;
    for (int64_t tablet_id = base_tablet_id; tablet_id < base_tablet_id + tablet_cnt; ++tablet_id) {
      tablet_handle.reset();
      const ObTabletID id(tablet_id);
      create_tablet(ls_handle, id, tablet_handle, /*persist*/true);
    }
    base_tablet_id += tablet_cnt;
  }
};

class SlogCkptGuard final
{
public:
  SlogCkptGuard()
    : is_write_ckpt_(nullptr),
      err_code_(OB_SUCCESS)
  {
    int ret = OB_SUCCESS;
    ObTenantStorageMetaService *tsms = nullptr;
    if (!SERVER_STORAGE_META_SERVICE.is_started_) {
      err_code_ = ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "server storage meta service is not started", K(ret));
    } else if (OB_ISNULL(tsms = MTL(ObTenantStorageMetaService *))) {
      err_code_ = ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null tenant storage meta service", K(ret));
    } else {
      fprintf(VERBOSE_OUT, "disabling background checkpoint...\n");
      bool &is_write_checkpoint = tsms->ckpt_slog_handler_.is_writing_checkpoint_;
      while (!ATOMIC_BCAS(&is_write_checkpoint, false, true)) {
        ob_usleep(100 * 1000);
      }
      fprintf(VERBOSE_OUT, "succeed to disable background checkpoint.\n");
      is_write_ckpt_ = &is_write_checkpoint;
    }
  }
  ~SlogCkptGuard()
  {
    if (OB_NOT_NULL(is_write_ckpt_)) {
      ATOMIC_STORE(is_write_ckpt_, false);
      fprintf(VERBOSE_OUT, "restore background checkpoint\n");
    }
  }
  int get_ret() const { return err_code_; }

private:
  bool *is_write_ckpt_;
  int err_code_;
};

static void get_tenant_min_max_file_ids(
  const uint64_t tenant_id,
  std::pair<int64_t, int64_t> &id_range)
{
  int ret = OB_SUCCESS;
  id_range = {-1, -1};
  omt::ObTenant *tenant = nullptr;
  ASSERT_SUCC(GCTX.omt_->get_tenant(tenant_id, tenant));

  SMART_VAR(ObTenantSuperBlock, super_block){
    super_block = tenant->get_super_block();
    id_range.first = super_block.min_file_id_;
    id_range.second = super_block.max_file_id_;
    ASSERT_GE(id_range.second, id_range.first);
  }
}

template<typename T>
static void convert_to_stdvec_and_sort(
  const ObIArray<T> &ob_vec,
  std::vector<T> &std_vec)
{
  int64_t count = ob_vec.count();
  std_vec.clear();
  std_vec.reserve(count);
  for (int64_t i = 0; i < count; ++i) {
    std_vec.push_back(ob_vec.at(i));
  }
  std::sort(std_vec.begin(), std_vec.end());
}

static uint64_t TENANT_ID = -1;

TEST_F(TestTenantSlogCkptSS, create_tenant)
{
  create_test_tenant(TENANT_ID);
  ASSERT_TRUE(is_user_tenant(TENANT_ID));
}

TEST_F(TestTenantSlogCkptSS, test_fd_dispenser)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_user_tenant(TENANT_ID));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_SUCC(tenant_guard.switch_to(TENANT_ID));

  ObSlogCheckpointFdDispenser fd_dispenser;
  int64_t file_id = 0;
  int64_t min_file_id = 0, max_file_id = 0;
  ASSERT_FAIL(fd_dispenser.init(-1));
  ASSERT_FAIL(fd_dispenser.acquire_new_file_id(file_id));
  ASSERT_FAIL(fd_dispenser.assign_to(min_file_id, max_file_id));

  const int64_t cur_file_id = 1;
  const int64_t N = 100;

  ASSERT_SUCC(fd_dispenser.init(cur_file_id));
  ASSERT_FAIL(fd_dispenser.init(cur_file_id));

  ASSERT_FAIL(fd_dispenser.assign_to(min_file_id, max_file_id));

  int64_t expected_val = cur_file_id + 1;
  for (int64_t i = 0; i < N; ++i) {
    ASSERT_SUCC(fd_dispenser.acquire_new_file_id(file_id));
    ASSERT_EQ(file_id, expected_val);
    ++expected_val;
  }

  ASSERT_SUCC(fd_dispenser.assign_to(min_file_id, max_file_id));
  ASSERT_EQ(min_file_id, cur_file_id + 1);
  ASSERT_EQ(max_file_id, expected_val - 1);
}

TEST_F(TestTenantSlogCkptSS, test_basic)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_user_tenant(TENANT_ID));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_SUCC(tenant_guard.switch_to(TENANT_ID));
  SlogCkptGuard ckpt_guard;
  ASSERT_SUCC(ckpt_guard.get_ret());
  ObTenantFileManager *tfm = nullptr;
  ASSERT_NE(nullptr, tfm = MTL(ObTenantFileManager *));
  ObTenantCheckpointSlogHandler &ckpt_slog_handler = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_;

  // create 3 ls and 10 tablets for each.
  int64_t ls_id;
  const int64_t tablet_cnt = 100;
  LOG_AND_PRINT(INFO, "create log streams and tablets", KR(ret));
  create_ls_and_ntablets(ls_id, tablet_cnt);


  std::pair<int64_t, int64_t> last_id_range = {-1, -1}, cur_id_range = {-1, -1};

  LOG_AND_PRINT(INFO, "[1st]checkpoint tenant slog");
  ASSERT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::Type::FORCE, ckpt_slog_handler));
  LOG_AND_PRINT(INFO, "[1st]tenant slog checkpoint finished, check result...");

  get_tenant_min_max_file_ids(TENANT_ID, cur_id_range);

  ObSArray<int64_t> tmp_file_ids;
  vector<int64_t> file_ids;
  ASSERT_SUCC(tfm->list_private_ckpt_file(MTL_ID(), MTL_EPOCH_ID(), tmp_file_ids));
  convert_to_stdvec_and_sort(tmp_file_ids, file_ids);
  // check min/max file ids
  ASSERT_EQ(cur_id_range.first, file_ids.front());
  ASSERT_EQ(cur_id_range.second, file_ids.back());
  // check continuity
  for (size_t i = 1; i < file_ids.size(); ++i) {
    ASSERT_EQ(file_ids[i - 1], file_ids[i] - 1);
  }
  LOG_AND_PRINT(INFO, "[1st]check finished, gc ckpt files");
  ASSERT_SUCC(ckpt_slog_handler.gc_checkpoint_file());
  tmp_file_ids.reset();

  LOG_AND_PRINT(INFO, "[2nd]checkpoint tenant slog");
  ASSERT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::Type::FORCE, ckpt_slog_handler));
  LOG_AND_PRINT(INFO, "[2nd]tenant slog checkpoint finished, check result...");
  // clean last ckpt files.
  ASSERT_SUCC(ckpt_slog_handler.gc_checkpoint_file());

  last_id_range = cur_id_range;
  get_tenant_min_max_file_ids(TENANT_ID, cur_id_range);
  // last max file id + 1 == cur min file id
  ASSERT_TRUE(last_id_range.second + 1 == cur_id_range.first);
  ASSERT_SUCC(tfm->list_private_ckpt_file(MTL_ID(), MTL_EPOCH_ID(), tmp_file_ids));
  convert_to_stdvec_and_sort(tmp_file_ids, file_ids);
  // check min/max file ids
  ASSERT_EQ(cur_id_range.first, file_ids.front());
  ASSERT_EQ(cur_id_range.second, file_ids.back());
  // check continuity
  for (size_t i = 1; i < file_ids.size(); ++i) {
    ASSERT_EQ(file_ids[i - 1], file_ids[i] - 1);
  }
}

} //end namespace unittest
} //end namespace oceanbase

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
  oceanbase::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}