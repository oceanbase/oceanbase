/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <unistd.h>

#define private public
#define protected public
#include "logservice/archiveservice/ob_archive_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#undef protected
#undef private

#include "basic_archive.h"
#include "mittest/env/ob_simple_server_helper.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::archive;
using namespace oceanbase::palf;
using namespace oceanbase::share;

namespace
{

static const int64_t WAIT_ARCHIVE_STATE_RETRY_COUNT = 120;
static const int64_t WAIT_ARCHIVE_READY_RETRY_COUNT = 360;
static const useconds_t WAIT_ARCHIVE_STATE_INTERVAL_US = 500 * 1000;

int configure_archive_dest(ObSimpleArchive &test, const bool is_mandatory)
{
  int ret = OB_SUCCESS;
  char current_dir[OB_MAX_FILE_NAME_LENGTH] = {0};
  ObSqlString sql;
  int64_t affected_rows = 0;
  const char *binding = is_mandatory ? "mandatory" : "optional";
  const char *test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  common::ObMySQLProxy &sql_proxy = test.get_curr_simple_server().get_sql_proxy2();

  if (OB_ISNULL(getcwd(current_dir, sizeof(current_dir)))) {
    ret = OB_ERR_SYS;
    ARCHIVE_LOG(WARN, "get current directory failed", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "alter system set log_archive_dest = "
      "'location=file://%s/log_arch_suspend_new_ls_%d_%s binding=%s'",
      current_dir,
      static_cast<int>(getpid()),
      test_name,
      binding))) {
    ARCHIVE_LOG(WARN, "build log archive dest sql failed", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    ARCHIVE_LOG(WARN, "configure log archive dest failed", K(ret), K(sql));
  }
  return ret;
}

int enable_archive(ObSimpleArchive &test)
{
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = test.get_curr_simple_server().get_sql_proxy2();
  return sql_proxy.write("alter system archivelog", affected_rows);
}

int disable_archive(ObSimpleArchive &test)
{
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = test.get_curr_simple_server().get_sql_proxy2();
  return sql_proxy.write("alter system noarchivelog", affected_rows);
}

int defer_archive(ObSimpleArchive &test)
{
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = test.get_curr_simple_server().get_sql_proxy2();
  return sql_proxy.write("alter system set log_archive_dest_state = 'DEFER'", affected_rows);
}

int enable_archive_dest(ObSimpleArchive &test)
{
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = test.get_curr_simple_server().get_sql_proxy2();
  return sql_proxy.write("alter system set log_archive_dest_state = 'ENABLE'", affected_rows);
}

int wait_archive_ready(ObSimpleArchive &test,
    const uint64_t tenant_id,
    const int64_t round_id,
    const bool require_tenant_doing)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < WAIT_ARCHIVE_READY_RETRY_COUNT; ++i) {
    bool archive_ready = false;
    {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
        return ret;
      }

      ObArchiveService *archive_service = MTL(ObArchiveService *);
      if (OB_ISNULL(archive_service)) {
        return OB_ERR_UNEXPECTED;
      }

      ObTenantArchiveRoundAttr attr;
      if (OB_FAIL(archive_service->persist_mgr_.load_archive_round_attr(attr))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret), K(tenant_id));
        }
      } else {
        ArchiveKey local_key;
        ObArchiveRoundState local_state;
        archive_service->archive_round_mgr_.get_archive_round_info(local_key, local_state);
        const bool tenant_ready = attr.state_.is_doing()
            || (! require_tenant_doing && attr.state_.is_beginning());
        archive_ready = attr.round_id_ >= round_id
            && tenant_ready
            && local_key.is_valid()
            && local_state.is_doing();
      }
    }

    if (archive_ready) {
      return OB_SUCCESS;
    }

    test.signal_worker(tenant_id);
    usleep(WAIT_ARCHIVE_STATE_INTERVAL_US);
  }
  return OB_TIMEOUT;
}

int wait_archive_suspend(ObSimpleArchive &test, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < WAIT_ARCHIVE_STATE_RETRY_COUNT; ++i) {
    bool tenant_suspend = false;
    bool local_suspend = false;
    {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
        return ret;
      }

      ObArchiveService *archive_service = MTL(ObArchiveService *);
      if (OB_ISNULL(archive_service)) {
        return OB_ERR_UNEXPECTED;
      }

      ObTenantArchiveRoundAttr attr;
      if (OB_FAIL(archive_service->persist_mgr_.load_archive_round_attr(attr))) {
        ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret), K(tenant_id));
      } else {
        ArchiveKey key;
        ObArchiveRoundState local_state;
        archive_service->archive_round_mgr_.get_archive_round_info(key, local_state);
        tenant_suspend = attr.state_.is_suspend();
        local_suspend = local_state.is_suspend();
      }
    }

    if (tenant_suspend && local_suspend) {
      return OB_SUCCESS;
    }

    test.signal_worker(tenant_id);
    usleep(WAIT_ARCHIVE_STATE_INTERVAL_US);
  }
  return OB_TIMEOUT;
}

int wait_archive_stop(ObSimpleArchive &test, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < WAIT_ARCHIVE_STATE_RETRY_COUNT; ++i) {
    bool archive_stopped = false;
    {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
        return ret;
      }

      ObArchiveService *archive_service = MTL(ObArchiveService *);
      if (OB_ISNULL(archive_service)) {
        return OB_ERR_UNEXPECTED;
      }

      ObTenantArchiveRoundAttr attr;
      ret = archive_service->persist_mgr_.load_archive_round_attr(attr);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        archive_stopped = true;
      } else if (OB_SUCCESS != ret) {
        ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret), K(tenant_id));
      } else {
        ArchiveKey local_key;
        ObArchiveRoundState local_state;
        archive_service->archive_round_mgr_.get_archive_round_info(local_key, local_state);
        archive_stopped = attr.state_.is_stop() && local_state.is_stop();
      }
    }

    if (archive_stopped) {
      return OB_SUCCESS;
    }

    test.signal_worker(tenant_id);
    usleep(WAIT_ARCHIVE_STATE_INTERVAL_US);
  }
  return OB_TIMEOUT;
}

int wait_ls_archive_progress(ObSimpleArchive &test,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObTenantArchiveRoundAttr &persisted_attr,
    ObLSArchivePersistInfo &persisted_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < WAIT_ARCHIVE_READY_RETRY_COUNT; ++i) {
    bool record_exist = false;
    {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
        return ret;
      }

      ObArchiveService *archive_service = MTL(ObArchiveService *);
      if (OB_ISNULL(archive_service)) {
        return OB_ERR_UNEXPECTED;
      }

      if (OB_FAIL(archive_service->persist_mgr_.load_archive_round_attr(persisted_attr))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret), K(tenant_id));
        }
      } else {
        ArchiveKey persisted_key(
            persisted_attr.incarnation_, persisted_attr.dest_id_, persisted_attr.round_id_);
        if (OB_FAIL(archive_service->persist_mgr_.load_ls_archive_progress_(
                ls_id, persisted_key, persisted_info, record_exist))) {
          ARCHIVE_LOG(WARN, "load ls archive progress failed", K(ret), K(tenant_id), K(ls_id));
        } else if (record_exist && persisted_info.is_valid()) {
          return OB_SUCCESS;
        }
      }
    }

    test.signal_worker(tenant_id);
    usleep(WAIT_ARCHIVE_STATE_INTERVAL_US);
  }
  return OB_TIMEOUT;
}

} // namespace

class TestArchiveSuspendNewLS : public ObSimpleArchive
{
public:
  TestArchiveSuspendNewLS()
      : ObSimpleArchive(), archive_service_stopped_(false), created_ls_id_() {}

protected:
  void SetUp() override
  {
    ObSimpleArchive::SetUp();
    if (! tenant_prepared_) {
      ASSERT_EQ(OB_SUCCESS, prepare());
      ASSERT_EQ(1, tenant_ids_.count());
      tenant_id_ = tenant_ids_.at(0);
      tenant_prepared_ = true;
    }
  }

  void TearDown() override
  {
    if (tenant_prepared_) {
      if (archive_service_stopped_) {
        MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
        if (OB_SUCCESS == tenant_guard.switch_to(tenant_id_)) {
          ObArchiveService *archive_service = MTL(ObArchiveService *);
          if (OB_NOT_NULL(archive_service)) {
            EXPECT_EQ(OB_SUCCESS, archive_service->start());
          }
        }
        archive_service_stopped_ = false;
      }

      const int ret = disable_archive(*this);
      if (OB_SUCCESS == ret) {
        EXPECT_EQ(OB_SUCCESS, wait_archive_stop(*this, tenant_id_));
      } else {
        EXPECT_EQ(OB_ALREADY_IN_NOARCHIVE_MODE, ret);
      }

      EXPECT_EQ(OB_SUCCESS, cleanup_created_ls_());
    }
    ObSimpleArchive::TearDown();
  }

  int start_archive_(const bool is_mandatory)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(configure_archive_dest(*this, is_mandatory))) {
      ARCHIVE_LOG(WARN, "configure archive dest failed", K(ret), K(is_mandatory));
    } else if (OB_FAIL(enable_archive_dest(*this))) {
      ARCHIVE_LOG(WARN, "enable archive dest failed", K(ret), K(is_mandatory));
    } else if (OB_FAIL(enable_archive(*this))) {
      ARCHIVE_LOG(WARN, "enable archive failed", K(ret), K(is_mandatory));
    } else if (OB_FAIL(wait_archive_ready(*this, tenant_id_, 1, true))) {
      ARCHIVE_LOG(WARN, "wait archive ready failed", K(ret), K(is_mandatory));
    }
    return ret;
  }

  void test_missing_progress_while_suspend_(const bool is_mandatory)
  {
    ASSERT_EQ(OB_SUCCESS, start_archive_(is_mandatory));
    ASSERT_EQ(OB_SUCCESS, defer_archive(*this));
    ASSERT_EQ(OB_SUCCESS, wait_archive_suspend(*this, tenant_id_));

    ASSERT_EQ(OB_SUCCESS, SSH::create_ls(tenant_id_, get_curr_observer().get_self()));
    int64_t new_ls_id = 0;
    ASSERT_EQ(OB_SUCCESS,
        SSH::g_select_int64(tenant_id_, "select max(ls_id) val from __all_ls", new_ls_id));
    ASSERT_GT(new_ls_id, 1);
    created_ls_id_ = ObLSID(new_ls_id);

    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

    ObArchiveService *archive_service = MTL(ObArchiveService *);
    ASSERT_NE(nullptr, archive_service);

    ArchiveKey key;
    ObArchiveRoundState local_state;
    archive_service->archive_round_mgr_.get_archive_round_info(key, local_state);
    ASSERT_TRUE(key.is_valid());
    ASSERT_TRUE(local_state.is_suspend());

    bool actual_mandatory = ! is_mandatory;
    ASSERT_EQ(OB_SUCCESS, archive_service->persist_mgr_.load_dest_mode_(actual_mandatory));
    ASSERT_EQ(is_mandatory, actual_mandatory);

    ObArchivePersistValue *value = nullptr;
    const int map_ret = archive_service->persist_mgr_.map_.get(ObLSID(new_ls_id), value);
    if (OB_SUCCESS == map_ret) {
      archive_service->persist_mgr_.map_.revert(value);
    }
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, map_ret);

    ObLSArchivePersistInfo persisted_info;
    bool record_exist = true;
    ASSERT_EQ(OB_SUCCESS, archive_service->persist_mgr_.load_ls_archive_progress_(
        ObLSID(new_ls_id), key, persisted_info, record_exist));
    ASSERT_FALSE(record_exist);

    LSN archive_lsn;
    SCN archive_scn;
    bool force = false;
    bool ignore = false;
    const int progress_ret = archive_service->get_ls_archive_progress(
        ObLSID(new_ls_id), archive_lsn, archive_scn, force, ignore);

    if (is_mandatory) {
      EXPECT_EQ(OB_EAGAIN, progress_ret);
      EXPECT_TRUE(force);
      EXPECT_FALSE(ignore);
    } else {
      EXPECT_EQ(OB_SUCCESS, progress_ret);
      EXPECT_FALSE(force);
      EXPECT_TRUE(ignore);
    }
  }

  void test_restart_before_archive_init_(const bool is_mandatory)
  {
    ASSERT_EQ(OB_SUCCESS, start_archive_(is_mandatory));

    ObTenantArchiveRoundAttr persisted_attr;
    ObLSArchivePersistInfo persisted_info;
    ASSERT_EQ(OB_SUCCESS, wait_ls_archive_progress(
        *this, tenant_id_, ObLSID(1), persisted_attr, persisted_info));
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

    ObArchiveService *archive_service = MTL(ObArchiveService *);
    ASSERT_NE(nullptr, archive_service);

    bool actual_mandatory = ! is_mandatory;
    ASSERT_EQ(OB_SUCCESS, archive_service->persist_mgr_.load_dest_mode_(actual_mandatory));
    ASSERT_EQ(is_mandatory, actual_mandatory);

    archive_service_stopped_ = true;
    archive_service->stop();
    archive_service->wait();

    archive_service->archive_round_mgr_.destroy();
    ASSERT_EQ(OB_SUCCESS, archive_service->archive_round_mgr_.init());
    archive_service->persist_mgr_.map_.destroy();
    ASSERT_EQ(OB_SUCCESS, archive_service->persist_mgr_.map_.init("ArcPersistMap"));
    {
      ObArchivePersistMgr::WLockGuard guard(archive_service->persist_mgr_.state_rwlock_);
      archive_service->persist_mgr_.tenant_key_.reset();
      archive_service->persist_mgr_.dest_no_ = -1;
      archive_service->persist_mgr_.state_.set_invalid();
    }

    ArchiveKey local_key;
    ObArchiveRoundState local_state;
    archive_service->archive_round_mgr_.get_archive_round_info(local_key, local_state);
    ASSERT_FALSE(local_key.is_valid());
    ASSERT_TRUE(local_state.is_invalid());

    ObArchivePersistValue *value = nullptr;
    const int map_ret = archive_service->persist_mgr_.map_.get(ObLSID(1), value);
    if (OB_SUCCESS == map_ret) {
      archive_service->persist_mgr_.map_.revert(value);
    }
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, map_ret);

    LSN first_archive_lsn;
    SCN first_archive_scn;
    bool first_force = true;
    bool first_ignore = true;
    EXPECT_EQ(OB_EAGAIN, archive_service->get_ls_archive_progress(
        ObLSID(1), first_archive_lsn, first_archive_scn, first_force, first_ignore));
    EXPECT_FALSE(first_force);
    EXPECT_FALSE(first_ignore);

    LSN second_archive_lsn;
    SCN second_archive_scn;
    bool second_force = ! is_mandatory;
    bool second_ignore = true;
    EXPECT_EQ(OB_SUCCESS, archive_service->get_ls_archive_progress(
        ObLSID(1), second_archive_lsn, second_archive_scn, second_force, second_ignore));
    EXPECT_EQ(is_mandatory, second_force);
    EXPECT_FALSE(second_ignore);
    EXPECT_EQ(LSN(persisted_info.lsn_), second_archive_lsn);
    EXPECT_EQ(persisted_info.checkpoint_scn_, second_archive_scn);
  }

private:
  int cleanup_created_ls_()
  {
    int ret = OB_SUCCESS;
    if (created_ls_id_.is_valid()) {
      int64_t affected_rows = 0;
      ObSqlString sql;
      common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;

      if (OB_ISNULL(sql_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "sql proxy is NULL", K(ret), K(created_ls_id_));
      } else if (OB_FAIL(sql.assign_fmt(
              "delete from __all_ls where ls_id = %ld", created_ls_id_.id()))) {
        ARCHIVE_LOG(WARN, "build delete all ls sql failed", K(ret), K(created_ls_id_));
      } else if (OB_FAIL(sql_proxy->write(tenant_id_, sql.ptr(), affected_rows))) {
        ARCHIVE_LOG(WARN, "delete all ls failed", K(ret), K(tenant_id_), K(created_ls_id_));
      }

      if (OB_SUCC(ret)) {
        MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
        if (OB_FAIL(tenant_guard.switch_to(tenant_id_))) {
          ARCHIVE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id_));
        } else {
          storage::ObLSService *ls_service = MTL(storage::ObLSService *);
          if (OB_ISNULL(ls_service)) {
            ret = OB_ERR_UNEXPECTED;
            ARCHIVE_LOG(ERROR, "ls service is NULL", K(ret), K(tenant_id_));
          } else if (OB_FAIL(ls_service->remove_ls(created_ls_id_))) {
            ARCHIVE_LOG(WARN, "remove local ls failed", K(ret), K(tenant_id_), K(created_ls_id_));
          }
        }
      }

      if (OB_SUCC(ret)) {
        sql.reset();
        if (OB_FAIL(sql.assign_fmt(
                "delete from __all_ls_status where tenant_id = %lu and ls_id = %ld",
                tenant_id_, created_ls_id_.id()))) {
          ARCHIVE_LOG(WARN, "build delete all ls status sql failed", K(ret),
              K(tenant_id_), K(created_ls_id_));
        } else if (OB_FAIL(sql_proxy->write(
                gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
          ARCHIVE_LOG(WARN, "delete all ls status failed", K(ret),
              K(tenant_id_), K(created_ls_id_));
        }
      }

      if (OB_SUCC(ret)) {
        sql.reset();
        if (OB_FAIL(sql.assign_fmt(
                "delete from __all_ls_election_reference_info where tenant_id = %lu and ls_id = %ld",
                tenant_id_, created_ls_id_.id()))) {
          ARCHIVE_LOG(WARN, "build delete all ls election reference info sql failed", K(ret),
              K(tenant_id_), K(created_ls_id_));
        } else if (OB_FAIL(sql_proxy->write(
                gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
          ARCHIVE_LOG(WARN, "delete all ls election reference info failed", K(ret),
              K(tenant_id_), K(created_ls_id_));
        }
      }

      if (OB_SUCC(ret)) {
        created_ls_id_.reset();
      }
    }
    return ret;
  }

  static bool tenant_prepared_;
  static uint64_t tenant_id_;
  bool archive_service_stopped_;
  ObLSID created_ls_id_;
};

bool TestArchiveSuspendNewLS::tenant_prepared_ = false;
uint64_t TestArchiveSuspendNewLS::tenant_id_ = OB_INVALID_TENANT_ID;

TEST_F(TestArchiveSuspendNewLS, optional_suspend_new_ls_without_progress)
{
  test_missing_progress_while_suspend_(false);
}

TEST_F(TestArchiveSuspendNewLS, mandatory_suspend_new_ls_without_progress)
{
  test_missing_progress_while_suspend_(true);
}

TEST_F(TestArchiveSuspendNewLS, optional_restart_before_archive_init)
{
  test_restart_before_archive_init_(false);
}

TEST_F(TestArchiveSuspendNewLS, mandatory_restart_before_archive_init)
{
  test_restart_before_archive_init_(true);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_archive_suspend_new_ls.log", true, false,
                          "test_archive_suspend_new_ls_rs.log",
                          "test_archive_suspend_new_ls_election.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
