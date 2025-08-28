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

#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <set>

#define private public
#define protected public

#include "deps/oblib/src/lib/ob_errno.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "src/observer/ob_service.h"
#include "src/rootserver/backup/ob_backup_clean_selector.h"
#include "src/rootserver/backup/ob_backup_clean_scheduler.h"
#include "unittest/storage/backup/test_backup_clean_selector_include.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace testing;

namespace oceanbase {
namespace rootserver {

class TestBackupCleanSelectorBase : public ::testing::Test {
protected:
  void SetUp() override {
    mock_schema_service_ = std::make_unique<MockObMultiVersionSchemaService>();
    mock_rpc_proxy_ = std::make_unique<MockObSrvRpcProxy>();
    mock_delete_mgr_ = std::make_unique<MockObUserTenantBackupDeleteMgr>();
  }

  void create_backup_set(ObBackupSetFileDesc &desc, int64_t id,
                         ObBackupType::BackupType type, int64_t prev_full,
                         int64_t prev_inc, int64_t dest_id, const char *path,
                         int64_t end_time,
                         ObBackupSetFileDesc::BackupSetStatus status =
                             ObBackupSetFileDesc::SUCCESS) {
    desc.reset();
    desc.backup_set_id_ = id;
    desc.incarnation_ = 1;
    desc.tenant_id_ = 1002;
    desc.dest_id_ = dest_id;
    desc.backup_type_.type_ = type;
    desc.prev_full_backup_set_id_ = prev_full;
    desc.prev_inc_backup_set_id_ = prev_inc;
    desc.status_ = status;
    desc.encryption_mode_ = ObBackupEncryptionMode::NONE;
    desc.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
    desc.backup_path_.assign(path);
    desc.date_ = 20240101;
    desc.start_time_ = 0;
    desc.end_time_ = end_time;
  }


  void run_delete_test(const std::initializer_list<int64_t> &ids_to_delete,
                       int expected_ret,
                       const std::set<int64_t> &expected_deleted_ids,
                       const char *fail_substr,
                       const ObIArray<ObBackupSetFileDesc> &test_sets,
                       const ObBackupPathString &test_current_path,
                       bool policy_exist,
                       int test_connectivity_result = OB_SUCCESS) {
    ObBackupCleanJobAttr job_attr;
    job_attr.reset();
    job_attr.job_id_ = 1001;
    job_attr.tenant_id_ = 1002;
    job_attr.incarnation_id_ = 1;
    job_attr.clean_type_ = ObNewBackupCleanType::DELETE_BACKUP_SET;

    for (const auto &id : ids_to_delete) {
      ASSERT_EQ(OB_SUCCESS, job_attr.backup_set_ids_.push_back(id));
    }

    ObBackupDeleteSelector selector;
    ASSERT_EQ(OB_SUCCESS,
              selector.init(mock_sql_proxy_, *mock_schema_service_, job_attr,
                            *mock_rpc_proxy_, *mock_delete_mgr_));
    MockBackupDataProvider *mock_data_provider =
        OB_NEW(MockBackupDataProvider, "BackupProvider");
    MockConnectivityChecker *mock_connectivity_checker =
        OB_NEW(MockConnectivityChecker, "ConnChecker");

    ASSERT_NE(nullptr, mock_data_provider);
    ASSERT_NE(nullptr, mock_connectivity_checker);

    ASSERT_EQ(OB_SUCCESS, mock_data_provider->set_backup_sets(test_sets));
    ASSERT_EQ(OB_SUCCESS,
              mock_data_provider->set_current_path(test_current_path));
    mock_connectivity_checker->set_connectivity_result(
        test_connectivity_result);
    mock_data_provider->set_policy_exist(policy_exist);
    OB_DELETE(IObBackupDataProvider, "BackupProvider", selector.data_provider_);
    OB_DELETE(IObConnectivityChecker, "ConnChecker",
              selector.connectivity_checker_);
    selector.data_provider_ = mock_data_provider;
    selector.connectivity_checker_ = mock_connectivity_checker;
    mock_data_provider = nullptr;
    mock_connectivity_checker = nullptr;

    ObArray<ObBackupSetFileDesc> result_list;
    int ret = selector.get_delete_backup_set_infos(result_list);

    ASSERT_EQ(expected_ret, ret);
    if (OB_SUCC(ret)) {
      ASSERT_EQ(expected_deleted_ids.size(), result_list.count());
      std::set<int64_t> actual_ids;
      for (const auto &item : result_list) {
        actual_ids.insert(item.backup_set_id_);
      }
      ASSERT_EQ(expected_deleted_ids, actual_ids);
    }
    if (fail_substr != nullptr) {
      ASSERT_THAT(job_attr.failure_reason_.ptr(), HasSubstr(fail_substr));
    }
  }

  MockObMySQLProxy mock_sql_proxy_;
  std::unique_ptr<MockObMultiVersionSchemaService> mock_schema_service_;
  std::unique_ptr<MockObSrvRpcProxy> mock_rpc_proxy_;
  std::unique_ptr<MockObUserTenantBackupDeleteMgr> mock_delete_mgr_;
};

// =================================================================================
// Fixture 1: basic cases
// =================================================================================
class TestBackupCleanSelector_BasicCases : public TestBackupCleanSelectorBase {
protected:
  void SetUp() override {
    TestBackupCleanSelectorBase::SetUp(); // Call base class SetUp first
    ObBackupSetFileDesc desc;
    create_backup_set(desc, 1, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 100, ObBackupSetFileDesc::SUCCESS);
    test_sets_.push_back(desc);
    create_backup_set(desc, 2, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 200, ObBackupSetFileDesc::DOING);
    test_sets_.push_back(desc);
    create_backup_set(desc, 3, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 300, ObBackupSetFileDesc::FAILED);
    test_sets_.push_back(desc);
    create_backup_set(desc, 4, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 400, ObBackupSetFileDesc::SUCCESS);
    test_sets_.push_back(desc);
    create_backup_set(desc, 5, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 500, ObBackupSetFileDesc::SUCCESS);
    desc.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETED;
    test_sets_.push_back(desc);
    create_backup_set(desc, 6, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 600, ObBackupSetFileDesc::SUCCESS);
    desc.file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETING;
    test_sets_.push_back(desc);
    create_backup_set(desc, 7, ObBackupType::FULL_BACKUP, 0, 0, 1,
                      "file:///path", 700, ObBackupSetFileDesc::SUCCESS);
    test_sets_.push_back(desc);
    test_current_path_.assign("file:///path");

  }
  ObArray<ObBackupSetFileDesc> test_sets_;
  ObBackupPathString test_current_path_;
};

// Test: Delete a FAILED backup set (id=3) - should succeed
TEST_F(TestBackupCleanSelector_BasicCases, SuccessToDeleteFailedSet) {
  run_delete_test({3}, OB_SUCCESS, {3}, nullptr, test_sets_, test_current_path_, false);
}

// Test: Delete a DOING backup set (id=2) - should fail
TEST_F(TestBackupCleanSelector_BasicCases, FailToDeleteDoingSet) {
  run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "status=DOING", test_sets_, test_current_path_, false);
}

// Test: Empty request list - should fail
TEST_F(TestBackupCleanSelector_BasicCases, FailOnEmptyRequestList) {
  run_delete_test({}, OB_INVALID_ARGUMENT, {}, nullptr, test_sets_, test_current_path_, false);
}

// Test: Delete a single backup set (id=1) - should succeed
TEST_F(TestBackupCleanSelector_BasicCases, SuccessToDeleteSingleSet) {
  run_delete_test({1}, OB_SUCCESS, {1}, nullptr, test_sets_, test_current_path_, false);
}

// Test: Skip already DELETED backup set (id=5) - should fail
TEST_F(TestBackupCleanSelector_BasicCases, FailToDeleteAlreadyDeletedSet) {
  run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "already_deleted", test_sets_, test_current_path_, false);
}

// TEST : Fail to delete a newer full backup set than the latest full backup set on the current path
TEST_F(TestBackupCleanSelector_BasicCases, FailToDeleteNewerFullBackupSet) {
  run_delete_test({6}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
      "cannot_delete_backup_set_in_current_path_when_delete_policy_is_set", test_sets_, test_current_path_, true);
}

// Test: Delete a DELETING backup set (id=6) - should succeed
TEST_F(TestBackupCleanSelector_BasicCases, SuccessToDeleteDeletingSet) {
  run_delete_test({6}, OB_SUCCESS, {6}, nullptr, test_sets_, test_current_path_, false);
}

// Test: Mixed delete (including already deleted set id=5) - only valid sets are deleted
TEST_F(TestBackupCleanSelector_BasicCases, SuccessToDeleteMixedListWithDeletedSet) {
  run_delete_test({1, 4, 5}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "already_deleted", test_sets_, test_current_path_, false);
}

// Test: Destination connectivity check failed - should fail
TEST_F(TestBackupCleanSelector_BasicCases, FailOnConnectivityCheck) {
  run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "dest_connectivity_check_failed", test_sets_, test_current_path_, false, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED);
}

// =================================================================================
// Fixture 2: dependency cases
// =================================================================================
class TestBackupCleanSelector_DependencyCases : public TestBackupCleanSelectorBase {
protected:
  void SetUp() override {
    TestBackupCleanSelectorBase::SetUp();
    ObBackupSetFileDesc desc;
    const char *path = "file:///obbackup/chain_path";
    create_backup_set(desc, 1, ObBackupType::FULL_BACKUP, 0, 0, 1, path, 100);
    test_sets_.push_back(desc);
    create_backup_set(desc, 2, ObBackupType::INCREMENTAL_BACKUP, 1, 1, 1, path, 200);
    test_sets_.push_back(desc);
    create_backup_set(desc, 3, ObBackupType::INCREMENTAL_BACKUP, 1, 2, 1, path, 300);
    test_sets_.push_back(desc);
    create_backup_set(desc, 4, ObBackupType::INCREMENTAL_BACKUP, 1, 3, 1, path, 400);
    test_sets_.push_back(desc);
    create_backup_set(desc, 5, ObBackupType::FULL_BACKUP, 0, 0, 1, path, 500);
    test_sets_.push_back(desc);
    test_current_path_.assign(path);
  }
  ObArray<ObBackupSetFileDesc> test_sets_;
  ObBackupPathString test_current_path_;
};

// Test: Delete backup set 2 (middle of dependency chain) - should fail due to dependency
TEST_F(TestBackupCleanSelector_DependencyCases, FailToDeleteMiddleOfChain) {
  run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "dependent_by", test_sets_, test_current_path_, false);
}

// Test: Delete backup set 1 (head of dependency chain) - should fail due to dependency
TEST_F(TestBackupCleanSelector_DependencyCases, FailToDeleteHeadOfChain) {
  run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "dependent_by", test_sets_, test_current_path_, false);
}

// Test: Delete backup set 4 (tail of dependency chain) - should succeed
TEST_F(TestBackupCleanSelector_DependencyCases, SuccessToDeleteTailOfChain) {
  run_delete_test({4}, OB_SUCCESS, {4}, nullptr, test_sets_, test_current_path_, false);
}

// Test: Delete backup set 4 (tail of dependency chain) - should fail
TEST_F(TestBackupCleanSelector_DependencyCases, FailToDeleteTailOfChain) {
  run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
      "cannot_delete_backup_set_in_current_path_when_delete_policy_is_set", test_sets_, test_current_path_, true);
}


// Test: Delete backup sets 3,4 (consecutive part of dependency chain) - should succeed
TEST_F(TestBackupCleanSelector_DependencyCases, SuccessToDeleteConsecutivePartOfChain) {
  run_delete_test({3,4}, OB_SUCCESS, {3,4}, nullptr, test_sets_, test_current_path_, false);
}

// Test: Delete backup sets 1,2,3,4 (entire dependency chain) - should succeed
TEST_F(TestBackupCleanSelector_DependencyCases, SuccessToDeleteEntireChain) {
  run_delete_test({1,2,3,4}, OB_SUCCESS, {1,2,3,4}, nullptr, test_sets_, test_current_path_, false);
}

// =================================================================================
// Fixture 3: retention and path cases
// =================================================================================
class TestBackupCleanSelector_RetentionAndPathCases
    : public TestBackupCleanSelectorBase {
protected:
  void SetUp() override {
    TestBackupCleanSelectorBase::SetUp();
    ObBackupSetFileDesc desc;
    const char *path_A = "file:///obbackup/path_A";
    const char *path_B = "file:///obbackup/path_B";

    create_backup_set(desc, 1, ObBackupType::FULL_BACKUP, 0, 0, 1, path_A, 100);
    test_sets_.push_back(desc);
    create_backup_set(desc, 2, ObBackupType::INCREMENTAL_BACKUP, 1, 1, 1,
                      path_A, 200);
    test_sets_.push_back(desc);
    create_backup_set(desc, 3, ObBackupType::FULL_BACKUP, 0, 0, 2, path_B, 300);
    test_sets_.push_back(desc);
    create_backup_set(desc, 4, ObBackupType::FULL_BACKUP, 0, 0, 2, path_B, 400);
    test_sets_.push_back(desc);
    create_backup_set(desc, 5, ObBackupType::INCREMENTAL_BACKUP, 4, 4, 2,
                      path_B, 500);
    test_sets_.push_back(desc);
    test_current_path_.assign(path_B);
  }

  ObArray<ObBackupSetFileDesc> test_sets_;
  ObBackupPathString test_current_path_;
};

// Test: Delete backup sets 1,3 from different destinations - should fail due to multi-dest restriction
TEST_F(TestBackupCleanSelector_RetentionAndPathCases, FailOnMixedDest) {
  run_delete_test({1, 3}, OB_NOT_SUPPORTED, {},
                  "multiple dest is not supported", test_sets_,
                  test_current_path_, true);
}

// =================================================================================
// Fixture 4: mega scenario
// =================================================================================
class TestBackupCleanSelector_MegaScenario
    : public TestBackupCleanSelectorBase {
protected:
  void SetUp() override {
    TestBackupCleanSelectorBase::SetUp();
    ObBackupSetFileDesc desc;
    const char *path_A = "file:///obbackup/mega_A";
    const char *path_B = "file:///obbackup/mega_B";

    create_backup_set(desc, 1, ObBackupType::FULL_BACKUP, 0, 0, 1, path_A, 100);
    test_sets_.push_back(desc);
    create_backup_set(desc, 2, ObBackupType::INCREMENTAL_BACKUP, 1, 1, 1,
                      path_A, 200);
    test_sets_.push_back(desc);
    create_backup_set(desc, 3, ObBackupType::INCREMENTAL_BACKUP, 1, 2, 1,
                      path_A, 300);
    test_sets_.push_back(desc);
    create_backup_set(desc, 4, ObBackupType::INCREMENTAL_BACKUP, 1, 3, 1,
                      path_A, 400);
    test_sets_.push_back(desc);
    create_backup_set(desc, 5, ObBackupType::FULL_BACKUP, 0, 0, 2, path_B, 500);
    test_sets_.push_back(desc);
    create_backup_set(desc, 6, ObBackupType::INCREMENTAL_BACKUP, 5, 5, 2,
                      path_B, 600);
    test_sets_.push_back(desc);
    create_backup_set(desc, 7, ObBackupType::INCREMENTAL_BACKUP, 5, 6, 2,
                      path_B, 700);
    test_sets_.push_back(desc);
    create_backup_set(desc, 8, ObBackupType::INCREMENTAL_BACKUP, 5, 7, 2,
                      path_B, 800);
    test_sets_.push_back(desc);
    create_backup_set(desc, 10, ObBackupType::INCREMENTAL_BACKUP, 1, 4, 1,
                      path_A, 1000);
    test_sets_.push_back(desc);
    create_backup_set(desc, 11, ObBackupType::FULL_BACKUP, 0, 0, 1, path_A, 1100);
    test_sets_.push_back(desc);
    create_backup_set(desc, 12, ObBackupType::INCREMENTAL_BACKUP, 11, 11, 1,
                      path_A, 1200);
    test_sets_.push_back(desc);
    create_backup_set(desc, 20, ObBackupType::FULL_BACKUP, 0, 0, 2, path_B, 2000);
    test_sets_.push_back(desc);
    create_backup_set(desc, 21, ObBackupType::INCREMENTAL_BACKUP, 20, 20, 2,
                      path_B, 2100);
    test_sets_.push_back(desc);
    test_current_path_.assign(path_B);
  }

  ObArray<ObBackupSetFileDesc> test_sets_;
  ObBackupPathString test_current_path_;
};

// Test: Delete backup sets 2,4 (interleaved on main chain) - should fail due to dependency
TEST_F(TestBackupCleanSelector_MegaScenario,
       InterleavedDeleteOnMainChain_Fail) {
  run_delete_test({2, 4}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
                  "dependent_by_backup_set", test_sets_, test_current_path_, false);
}

// Test: Delete backup sets 10,11,12 (complete sub-chain) - should succeed
TEST_F(TestBackupCleanSelector_MegaScenario, DeleteCompleteSubChain_Success) {
  run_delete_test({10, 11, 12}, OB_SUCCESS, {10, 11, 12}, nullptr, test_sets_,
                  test_current_path_, false);
}

// Test: Delete backup set 30 (non-existent) - should fail
TEST_F(TestBackupCleanSelector_MegaScenario, DeleteNotValid_Fail) {
  run_delete_test({30}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
                  "get_set_failed", test_sets_, test_current_path_, false);
}

// =================================================================================
// Fixture 5: advanced path cases
// =================================================================================
class TestBackupCleanSelector_AdvancedPathCases
    : public TestBackupCleanSelectorBase {
protected:
  void SetUp() override {
    TestBackupCleanSelectorBase::SetUp();
    ObArray<ObBackupSetFileDesc> sets;
    ObBackupSetFileDesc desc;
    const char *path_A = "file:///obbackup/adv_path_A";
    const char *path_B = "file:///obbackup/adv_path_B";

    create_backup_set(desc, 1, ObBackupType::FULL_BACKUP, 0, 0, 1, path_A, 100);
    sets.push_back(desc);
    create_backup_set(desc, 2, ObBackupType::INCREMENTAL_BACKUP, 1, 1, 1,
                      path_A, 200);
    sets.push_back(desc);
    create_backup_set(desc, 3, ObBackupType::FULL_BACKUP, 0, 0, 2, path_B, 300);
    sets.push_back(desc);
    create_backup_set(desc, 4, ObBackupType::FULL_BACKUP, 0, 0, 2, path_B, 400);
    sets.push_back(desc);
    create_backup_set(desc, 5, ObBackupType::INCREMENTAL_BACKUP, 4, 4, 2,
                      path_B, 500);
    sets.push_back(desc);
    create_backup_set(desc, 6, ObBackupType::FULL_BACKUP, 0, 0, 1, path_A, 600);
    sets.push_back(desc);

    test_sets_ = sets;
    test_current_path_.assign(path_B);
  }

  ObArray<ObBackupSetFileDesc> test_sets_;
  ObBackupPathString test_current_path_;
};

// Test: Delete backup set 1 on reused path with dependency - should fail due to dependency
TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteOnReusedPathWithDependency) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
                  "dependent_by_backup_set", test_sets_, new_current_path, false);
}

// Test: Delete backup set 1 on reused path with dependency - should fail due to dependency
TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteOnReusedPathWithDependency2) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({2}, OB_SUCCESS, {2}, nullptr, test_sets_, new_current_path, false);
}


TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteOnReusedPathWithDependency3) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
                  "cannot_delete_backup_set_in_current_path_when_delete_policy_is_set", test_sets_, new_current_path, true);
}

TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteOnReusedPathWithDependency4) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({3}, OB_SUCCESS, {3}, nullptr, test_sets_, new_current_path, true);
}

TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteOnReusedPathWithDependency5) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({3,4}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {}, "dependent_by_backup_set", test_sets_, new_current_path, true);
}


TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteOnReusedPathWithDependency6) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({3,4,5}, OB_SUCCESS, {3,4,5}, nullptr, test_sets_, new_current_path, true);
}


// Test: Delete backup set 6 (latest on reused path) - should fail due to retention policy
TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       FailToDeleteLatestOnReusedPath) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_A");
  run_delete_test({6}, OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED, {},
                  "cannot_delete_backup_set_in_current_path_when_delete_policy_is_set", test_sets_, new_current_path, true);
}

// Test: Delete backup set 6 (latest on old path) - should succeed
TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       SuccessToDeleteLatestOnOldPath) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_B");
  run_delete_test({6}, OB_SUCCESS, {6}, nullptr, test_sets_, new_current_path, true);
}


// Test: Delete backup sets 1,2 (old chain on reused path) - should succeed
TEST_F(TestBackupCleanSelector_AdvancedPathCases,
       SuccessToDeleteOldChainOnReusedPath) {
  ObBackupPathString new_current_path;
  new_current_path.assign("file:///obbackup/adv_path_B");
  run_delete_test({1, 2}, OB_SUCCESS, {1, 2}, nullptr, test_sets_,
                  new_current_path, true);
}

} // namespace backup
} // namespace oceanbase

int main(int argc, char **argv) {
  oceanbase::common::ObLogger &logger =
      oceanbase::common::ObLogger::get_logger();
  system("rm -f test_backup_clean_selector.log*");
  logger.set_file_name("test_backup_clean_selector.log", true, true);
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}