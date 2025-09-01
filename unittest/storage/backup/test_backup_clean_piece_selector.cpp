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
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <set>

#define private public
#define protected public

#include "src/rootserver/backup/ob_backup_clean_scheduler.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "src/observer/ob_service.h"
#include "src/share/backup/ob_backup_helper.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "src/rootserver/backup/ob_backup_clean_selector.h"
#include "unittest/storage/backup/test_backup_clean_selector_include.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::obrpc;
using namespace testing;

namespace oceanbase {
namespace backup {
class TestBackupCleanPieceSelectorBase : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_EQ(OB_SUCCESS, mock_sql_proxy_.init(nullptr));
        mock_schema_service_ = std::make_unique<MockObMultiVersionSchemaService>();
        mock_rpc_proxy_ = std::make_unique<MockObSrvRpcProxy>();
        mock_delete_mgr_ = std::make_unique<MockObUserTenantBackupDeleteMgr>();
    }

    void create_piece(share::ObTenantArchivePieceAttr &piece, int64_t piece_id, int64_t dest_id,
                      const char* path, SCN checkpoint_scn,
                      share::ObArchivePieceStatus::Status status = share::ObArchivePieceStatus::Status::FROZEN,
                      share::ObBackupFileStatus::STATUS file_status = share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE) {
        piece.reset();
        piece.key_.tenant_id_ = 1002;
        piece.key_.dest_id_ = dest_id;
        piece.key_.round_id_ = 1;
        piece.key_.piece_id_ = piece_id;
        piece.incarnation_ = 1;
        piece.dest_no_ = 1;
        piece.file_count_ = 10;
        piece.start_scn_ = SCN::base_scn();
        piece.checkpoint_scn_ = checkpoint_scn;
        piece.max_scn_ = checkpoint_scn;
        piece.end_scn_ = checkpoint_scn;
        piece.compatible_.version_ = ObArchiveCompatible::Compatible::COMPATIBLE_VERSION_1;
        piece.input_bytes_ = 1024;
        piece.output_bytes_ = 1024;
        piece.status_.status_ = status;
        piece.file_status_ = file_status;
        piece.cp_file_id_ = 0;
        piece.cp_file_offset_ = 0;
        piece.path_.assign(path);
    }

    void create_backup_set(share::ObBackupSetFileDesc &desc, int64_t id, share::ObBackupType::BackupType type,
                           int64_t prev_full, int64_t prev_inc, int64_t dest_id, const char* path,
                           int64_t expired_time,
                           share::ObBackupSetFileDesc::BackupSetStatus status = share::ObBackupSetFileDesc::SUCCESS,
                           SCN start_replay_scn = SCN::base_scn()) {
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
        desc.file_status_ = share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
        desc.backup_path_.assign(path);
        desc.date_ = 20240101;
        desc.end_time_ = expired_time;
        desc.start_time_ = ObTimeUtility::current_time() - 86400000000L * (30 - id);
        desc.start_replay_scn_ = start_replay_scn;
    }

    // run_delete_test parameter description:
    // ids_to_delete: List of piece ids to delete
    // expected_ret: Expected return value
    // expected_deleted_ids: Expected list of deleted piece ids
    // fail_substr: Expected failure reason
    void run_delete_test(const std::initializer_list<int64_t>& ids_to_delete, int expected_ret,
                         const std::set<int64_t>& expected_deleted_ids = {}, const char* fail_substr = nullptr,
                         const ObArray<share::ObBackupSetFileDesc>& test_sets = ObArray<share::ObBackupSetFileDesc>(),
                         const ObBackupPathString& test_current_path = ObBackupPathString(),
                         const ObArray<share::ObTenantArchivePieceAttr>& test_pieces = ObArray<share::ObTenantArchivePieceAttr>(),
                         const ObArray<std::pair<int64_t, int64_t>>& test_dest_pairs = ObArray<std::pair<int64_t, int64_t>>(),
                         const bool policy_exist = false) {
        ObBackupCleanJobAttr job_attr;
        job_attr.reset();
        job_attr.job_id_ = 1001;
        job_attr.tenant_id_ = 1002;
        job_attr.incarnation_id_ = 1;
        job_attr.clean_type_ = ObNewBackupCleanType::DELETE_BACKUP_PIECE;
        for (const auto& id : ids_to_delete) {
            ASSERT_EQ(OB_SUCCESS, job_attr.backup_piece_ids_.push_back(id));
        }

        ObBackupDeleteSelector selector;

        // Use standard init method
        ASSERT_EQ(OB_SUCCESS, selector.init(mock_sql_proxy_, *mock_schema_service_, job_attr,
                                           *mock_rpc_proxy_, *mock_delete_mgr_));

        // Modify pointers
        MockBackupDataProvider *mock_data_provider = OB_NEW(MockBackupDataProvider, "BackupProvider");
        MockArchivePersistHelper *mock_archive_helper = OB_NEW(MockArchivePersistHelper, "ArchiveHelper");
        MockConnectivityChecker *mock_connectivity_checker = OB_NEW(MockConnectivityChecker, "ConnChecker");
        ASSERT_NE(nullptr, mock_data_provider);
        ASSERT_NE(nullptr, mock_archive_helper);
        ASSERT_NE(nullptr, mock_connectivity_checker);

        ASSERT_EQ(OB_SUCCESS, mock_data_provider->set_backup_sets(test_sets));
        ASSERT_EQ(OB_SUCCESS, mock_data_provider->set_current_path(test_current_path));
        ASSERT_EQ(OB_SUCCESS, mock_archive_helper->set_valid_dest_pairs(test_dest_pairs));
        ASSERT_EQ(OB_SUCCESS, mock_archive_helper->set_all_pieces(test_pieces));
        mock_connectivity_checker->set_connectivity_result(OB_SUCCESS);
        mock_data_provider->set_policy_exist(policy_exist);

        selector.data_provider_ = mock_data_provider;
        selector.archive_helper_ = mock_archive_helper;
        selector.connectivity_checker_ = mock_connectivity_checker;
        mock_data_provider = nullptr;
        mock_archive_helper = nullptr;
        mock_connectivity_checker = nullptr;

        ObArray<share::ObTenantArchivePieceAttr> result_list;
        int ret = selector.get_delete_backup_piece_infos(result_list);

        ASSERT_EQ(expected_ret, ret);
        if (OB_SUCC(ret)) {
            ASSERT_EQ(expected_deleted_ids.size(), result_list.count());
            std::set<int64_t> actual_ids;
            for (const auto& item : result_list) {
                actual_ids.insert(item.key_.piece_id_);
            }
            ASSERT_EQ(expected_deleted_ids, actual_ids);
        }
        if (fail_substr) {
            ASSERT_THAT(job_attr.failure_reason_.ptr(), HasSubstr(fail_substr));
        }
    }

    MockObMySQLProxy mock_sql_proxy_;
    std::unique_ptr<MockObMultiVersionSchemaService> mock_schema_service_;
    std::unique_ptr<MockObSrvRpcProxy> mock_rpc_proxy_;
    std::unique_ptr<MockObUserTenantBackupDeleteMgr> mock_delete_mgr_;
};


// =================================================================================
// Fixture 1: Test basic cases for single path
// =================================================================================
class TestBackupCleanPieceSelector_BasicCases : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        // Create test data
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        share::ObTenantArchivePieceAttr piece;

        // Create some simple pieces for testing different statuses
        create_piece(piece, 1, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 2, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 3, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_DELETED);
        pieces.push_back(piece);

        create_piece(piece, 4, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_DELETING);
        pieces.push_back(piece);

        create_piece(piece, 5, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::ACTIVE, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // Setup test data
        test_pieces_ = pieces;
        test_current_path_.assign("file:///backup_path");
        test_dest_pairs_.push_back(std::make_pair(1002, 1));
        }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_; // Empty backup sets
};

// Delete fails because no backup sets exist
// Test: Delete piece 1 but no backup sets exist - should fail
TEST_F(TestBackupCleanPieceSelector_BasicCases, FailToDeletePieceWhenNoBackupSetsExist) {
    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no_full_backup_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete an active piece (piece 5) - should fail
TEST_F(TestBackupCleanPieceSelector_BasicCases, FailToDeleteActivePiece) {
    run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "is_active_piece",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete with empty piece list - should fail
TEST_F(TestBackupCleanPieceSelector_BasicCases, FailOnEmptyRequestList) {
    run_delete_test({}, OB_INVALID_ARGUMENT,
                    std::set<int64_t>(), nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete a non-existent piece (piece 99) - should fail
TEST_F(TestBackupCleanPieceSelector_BasicCases, FailToDeleteNonExistentPiece) {
    run_delete_test({99}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "not_exist",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Two different paths contain pieces, deletion has no impact as long as it's not the current writing path
class TestBackupCleanPieceSelector_TwoPathCases : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        share::ObTenantArchivePieceAttr piece;

        // Create some simple pieces for testing different statuses
        create_piece(piece, 1, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 2, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 3, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 4, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 5, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);
        // second path
        create_piece(piece, 6, 2, "file:///path2", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 7, 2, "file:///path2", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 8, 2, "file:///path2", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 9, 2, "file:///path2", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // Setup test data
        test_pieces_ = pieces;
        test_current_path_.assign("file:///backup_path");
        test_dest_pairs_.push_back(std::make_pair(1002, 2));
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_; // Empty backup sets
};

// the expired time is 0, means no recovery window
// Test: Delete piece 1 from dest 1 - should succeed
TEST_F(TestBackupCleanPieceSelector_TwoPathCases, SuccessToDeleteSinglePiece) {
    run_delete_test({1}, OB_SUCCESS, {1}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete piece 2 without deleting piece 1 first - should fail (sequential constraint)
TEST_F(TestBackupCleanPieceSelector_TwoPathCases, FailToDeletePieceWithoutSequentialOrder) {
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller_piece_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete pieces 1,2,4,5 with gap (missing piece 3) - should fail (sequential constraint)
TEST_F(TestBackupCleanPieceSelector_TwoPathCases, FailToDeletePiecesWithGap) {
    run_delete_test({1,2,4,5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller_piece_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete consecutive pieces 1,2,3 - should succeed
TEST_F(TestBackupCleanPieceSelector_TwoPathCases, SuccessToDeleteConsecutivePieces) {
    run_delete_test({1,2,3},OB_SUCCESS, {1,2,3}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}


// =================================================================================
// Fixture 2: Test retention policy constraints
// =================================================================================
class TestBackupCleanPieceSelector_RetentionPolicyCases : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        share::ObTenantArchivePieceAttr piece;

        // Create some simple pieces for testing different statuses
        create_piece(piece, 1, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 2, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 3, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::ACTIVE, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // Setup test data
        test_pieces_ = pieces;
        test_current_path_.assign("file:///backup_path");
        test_dest_pairs_.push_back(std::make_pair(1002, 1));
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_; // Empty backup sets
};

// Delete fails because no backup sets exist
// Test: Delete old piece 1 when backup dest exists but no backup sets - should fail
TEST_F(TestBackupCleanPieceSelector_RetentionPolicyCases, FailToDeleteOldPieceWhenNoBackupSets) {
    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no_full_backup_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete old pieces 1,2 when backup dest exists but no backup sets - should fail
TEST_F(TestBackupCleanPieceSelector_RetentionPolicyCases, FailToDeleteOldPiecesWhenNoBackupSets) {
    run_delete_test({1, 2 }, OB_ENTRY_NOT_EXIST, {}, "no_full_backup_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete old pieces 1,2 when new archive path exists - should succeed
TEST_F(TestBackupCleanPieceSelector_RetentionPolicyCases, SuccessToDeleteOldPiecesWhenNewArchivePathExists) {
    ObArray<std::pair<int64_t, int64_t>> archive_pairs;
    archive_pairs.push_back(std::make_pair(1003, 2));
    run_delete_test({1, 2}, OB_SUCCESS, {1, 2}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, archive_pairs, 0);
}

// Test: Delete old pieces 1,2 when new archive path exists - should succeed
TEST_F(TestBackupCleanPieceSelector_RetentionPolicyCases, FailToDeleteOldPiecebecausseNotSequential) {
    ObArray<std::pair<int64_t, int64_t>> archive_pairs;
    archive_pairs.push_back(std::make_pair(1003, 2));
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller_piece_exists",
                    test_sets_, test_current_path_, test_pieces_, archive_pairs, 0);
}


// // =================================================================================
// // Fixture 3: Delete pieces from multiple old paths simultaneously
// =================================================================================
class TestBackupCleanPieceSelector_MultiPathCases : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        share::ObTenantArchivePieceAttr piece;
        // old path1
        create_piece(piece, 1, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 2, 1, "file:///path", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // old path2
        create_piece(piece, 3, 2, "file:///path2", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 4, 2, "file:///path2", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // current path
        create_piece(piece, 5, 3, "file:///path3", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 6, 3, "file:///path3", SCN::base_scn(), share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // Setup test data
        test_pieces_ = pieces;
        test_current_path_.assign("file:///backup_path");
        test_dest_pairs_.push_back(std::make_pair(1004, 3));
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_; // Empty backup sets
};

// Delete pieces on path 1002,1, no impact
// Test: Delete pieces 1,2 from same dest(old path) (dest_id=1) - should succeed
TEST_F(TestBackupCleanPieceSelector_MultiPathCases, SuccessToDeletePiecesFromSameDest) {
    run_delete_test({1, 2}, OB_SUCCESS, {1, 2}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}

// Test: Delete pieces 1,3 from different old dests (dest_id=1 and dest_id=2) - should fail
TEST_F(TestBackupCleanPieceSelector_MultiPathCases, FailToDeletePiecesFromDifferentDests) {
    run_delete_test({1, 3}, OB_NOT_SUPPORTED, {}, "multiple dest is not supported",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}

// Test: Delete pieces 1,5 from different dests (dest_id=1 and dest_id=2) - should fail
TEST_F(TestBackupCleanPieceSelector_MultiPathCases, FailToDeletePiecesFromMultipleDests) {
    run_delete_test({1, 5}, OB_NOT_SUPPORTED, {}, "multiple dest is not supported",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}

// Test: Delete piece 5 from current path but no backup sets exist - should fail
TEST_F(TestBackupCleanPieceSelector_MultiPathCases, FailToDeletePieceWhenNoBackupSetsExist) {
    run_delete_test({5}, OB_ENTRY_NOT_EXIST, {}, "no_full_backup_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}


// Test: Delete piece 5 from current path but no backup sets exist - should fail
TEST_F(TestBackupCleanPieceSelector_MultiPathCases, FailToDeletePieceWhenNoBackupSetsExist2) {
    run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {},
                        "cannot_delete_backup_piece_in_current_path_when_delete_policy_is_set",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}

// =================================================================================
// Fixture 4: Test current path retention policy (Active Path Retention Policy)
// =================================================================================
class TestBackupCleanPieceSelector_ActivePathRetention : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        share::ObTenantArchivePieceAttr piece;
        share::ObBackupSetFileDesc backup_set;

        const char* active_piece_path = "file:///archive_active";
        const char* active_backup_path = "file:///backup_active";
        const int64_t active_piece_dest_id = 1;
        const int64_t active_backup_dest_id = 10;
        const int64_t latest_full_start_replay_scn = 350;

        // --- Mock Pieces on Active Path ---
        SCN scn_100, scn_200, scn_300, scn_400, scn_500, scn_350;
        scn_100.convert_for_gts(100);
        scn_200.convert_for_gts(200);
        scn_300.convert_for_gts(300);
        scn_400.convert_for_gts(400);
        scn_500.convert_for_gts(500);
        scn_350.convert_for_gts(latest_full_start_replay_scn);

        create_piece(piece, 1, active_piece_dest_id, active_piece_path, scn_100); test_pieces_.push_back(piece);
        create_piece(piece, 2, active_piece_dest_id, active_piece_path, scn_200); test_pieces_.push_back(piece);
        create_piece(piece, 3, active_piece_dest_id, active_piece_path, scn_300); test_pieces_.push_back(piece);
        create_piece(piece, 4, active_piece_dest_id, active_piece_path, scn_400); test_pieces_.push_back(piece); // This piece should be protected
        create_piece(piece, 5, active_piece_dest_id, active_piece_path, scn_500); test_pieces_.push_back(piece); // This piece should be protected

        // --- Mock Backup Set on Active Path ---
        create_backup_set(backup_set, 10, ObBackupType::FULL_BACKUP, 0, 0, active_backup_dest_id, active_backup_path,
                            1000, ObBackupSetFileDesc::SUCCESS, scn_350);
        test_sets_.push_back(backup_set);

        // --- Setup Test Data ---
        test_current_path_.assign(active_backup_path);
        test_dest_pairs_.push_back(std::make_pair(1, active_piece_dest_id));
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_;
};

// Test: Delete pieces 1,2,3 (all before retention SCN 350) - should succeed
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessToDeleteBeforeRetentionSCN) {
    run_delete_test({1, 2, 3}, OB_SUCCESS, {1, 2, 3}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 3,4 (piece 4's SCN 400 >= retention SCN 350) - should fail due to protection
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, FailToDeleteAcrossRetentionSCN) {
    run_delete_test({3, 4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed_for_oldest_full_backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 4 (protected piece with SCN 400 >= retention SCN 350) - should fail
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, FailToDeleteSingleProtectedPiece) {
    run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed_for_oldest_full_backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 1,3 (with gap, skipping piece 2) - should fail due to sequential deletion rule
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, FailToDeleteWithGap) {
    run_delete_test({1, 3}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller_piece_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 2 (without deleting piece 1 first) - should fail due to sequential deletion rule
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessWhenProtectedPieceNotRequested) {
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller_piece_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 1 (first piece in sequence) - should succeed
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessWhenFirstPieceIsDeleted) {
    run_delete_test({1}, OB_SUCCESS, {1}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 1 (first piece in sequence) - should succeed
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessWhenFirstPieceIsDeleted2) {
    run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot_delete_backup_piece_in_current_path_when_delete_policy_is_set",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}


// =================================================================================
// Fixture 5: Test current path retention policy (Active Path Retention Policy) + old paths can be deleted
// =================================================================================
class TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        ObArray<share::ObBackupSetFileDesc> backup_sets;
        share::ObTenantArchivePieceAttr piece;
        share::ObBackupSetFileDesc backup_set;

        // --- Paths and Constants ---
        const char* inactive_piece_path = "file:///archive_inactive";
        const char* active_piece_path = "file:///archive_active";
        const char* active_backup_path = "file:///backup_active";

        const int64_t inactive_piece_dest_id = 1;
        const int64_t active_piece_dest_id = 2;
        const int64_t active_backup_dest_id = 10;
        const int64_t latest_full_start_replay_scn = 350;

        // --- Mock Pieces on Inactive Path (dest_id = 1) ---
        // These pieces should not be affected by the retention policy of the active path.
        SCN scn_100, scn_200, scn_600, scn_300, scn_400, scn_500, scn_350;
        scn_100.convert_for_gts(100);
        scn_200.convert_for_gts(200);
        scn_600.convert_for_gts(600);
        scn_300.convert_for_gts(300);
        scn_400.convert_for_gts(400);
        scn_500.convert_for_gts(500);
        scn_350.convert_for_gts(latest_full_start_replay_scn);

        create_piece(piece, 1, inactive_piece_dest_id, inactive_piece_path, scn_100); pieces.push_back(piece);
        create_piece(piece, 2, inactive_piece_dest_id, inactive_piece_path, scn_200); pieces.push_back(piece);
        create_piece(piece, 3, inactive_piece_dest_id, inactive_piece_path, scn_600); pieces.push_back(piece); // SCN is high, but on inactive path, so it's deletable

        // --- Mock Pieces on Active Path (dest_id = 2) ---
        // These are subject to retention policy.
        create_piece(piece, 4, active_piece_dest_id, active_piece_path, scn_300); pieces.push_back(piece); // Deletable
        create_piece(piece, 5, active_piece_dest_id, active_piece_path, scn_400); pieces.push_back(piece); // Protected by retention
        create_piece(piece, 6, active_piece_dest_id, active_piece_path, scn_500); pieces.push_back(piece); // Protected by retention

        // --- Mock Backup Set on Active Path ---
        create_backup_set(backup_set, 10, ObBackupType::FULL_BACKUP, 0, 0, active_backup_dest_id, active_backup_path,
                          1000, ObBackupSetFileDesc::SUCCESS, scn_350);
        backup_sets.push_back(backup_set);

        // --- Setup Test Data ---
        test_pieces_ = pieces;
        test_sets_ = backup_sets;
        test_current_path_.assign(active_backup_path);
        test_dest_pairs_.push_back(std::make_pair(1, active_piece_dest_id)); // Active archive dest is 2
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_;
};

// Test: Delete pieces 1,2,3 on inactive path (all pieces can be deleted even with high SCN) - should succeed
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, SuccessToDeleteAllOnInactivePath) {
    // Attempt to delete all pieces (1, 2, 3) on the inactive path.
    // Piece 3 has a high SCN (600), but since it's on an inactive path, it is not protected.
    // This should succeed.
    run_delete_test({1, 2, 3}, OB_SUCCESS, {1, 2, 3}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, SuccessToDeleteAllOnInactivePath2) {
    run_delete_test({1, 2, 3}, OB_SUCCESS, {1, 2, 3}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}

TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, SuccessToDeleteAllOnInactivePath3) {
    run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot_delete_backup_piece_in_current_path_when_delete_policy_is_set",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}


// Test: Delete pieces 1,2,4 from different destinations - should fail due to multi-dest restriction
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailWhenMultiDest1) {
    run_delete_test({1, 2, 4}, OB_NOT_SUPPORTED, {}, "multiple dest is not supported",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 1,5 from different destinations - should fail due to multi-dest restriction
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailWhenMultiDest2) {
    run_delete_test({1, 5}, OB_NOT_SUPPORTED, {}, "multiple dest is not supported",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 5 (protected piece on active path) - should fail due to retention policy
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailWhenMixingProtectedPiece2) {
    run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed_for_oldest_full_backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 3,4 from different destinations - should fail due to multi-dest restriction
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailWhenMultiDest3) {
    run_delete_test({3, 4}, OB_NOT_SUPPORTED, {}, "multiple dest is not supported",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 4,5,6 (including protected pieces) - should fail due to retention policy
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailDueToBackupsetNeeded2) {
    run_delete_test({4, 5, 6}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed_for_oldest_full_backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// =================================================================================
// Fixture 6: Some special cases (Special Cases)
// =================================================================================
// Test deleting pieces with deleted, deleting, non-existent, and active status
class TestBackupCleanPieceSelector_SpecialCases : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        share::ObTenantArchivePieceAttr piece;

        const char* path = "file:///archive_special";
        const int64_t dest_id = 1;

        // --- Create pieces with various statuses ---
        SCN scn_100, scn_200, scn_300, scn_400;
        scn_100.convert_for_gts(100);
        scn_200.convert_for_gts(200);
        scn_300.convert_for_gts(300);
        scn_400.convert_for_gts(400);

        create_piece(piece, 1, dest_id, path, scn_100, share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_DELETED);
        pieces.push_back(piece);

        create_piece(piece, 2, dest_id, path, scn_200, share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_DELETING);
        pieces.push_back(piece);

        create_piece(piece, 3, dest_id, path, scn_300, share::ObArchivePieceStatus::Status::FROZEN, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        create_piece(piece, 4, dest_id, path, scn_400, share::ObArchivePieceStatus::Status::ACTIVE, share::ObBackupFileStatus::BACKUP_FILE_AVAILABLE);
        pieces.push_back(piece);

        // --- Setup Test Data ---
        test_pieces_ = pieces;
        test_current_path_.assign("file:///backup_path");
        test_dest_pairs_.push_back(std::make_pair(1, 99)); // Set a different active dest_id
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_; // Empty backup sets
};

// Test: Delete piece 1 (already marked as DELETED) - should fail
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteAlreadyDeletedPiece) {
    // Attempt to delete piece 2, which is already marked as DELETED.
    // The system should reject this as an invalid operation.
    run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "already_deleted",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete piece 2 (DELETING status, can be retried) - should fail
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteDeletingPiece) {
    ObArray<std::pair<int64_t, int64_t>> active_dest_pairs;
    active_dest_pairs.push_back(std::make_pair(1, 1));
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot_delete_backup_piece_in_current_path_when_delete_policy_is_set",
                    test_sets_, test_current_path_, test_pieces_, active_dest_pairs, true);
}

// Test: Delete piece 2 (DELETING status, can be retried) - should succeed
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteDeletingPiece2) {
    run_delete_test({2}, OB_SUCCESS, {2}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete piece 99 (non-existent piece) - should fail
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteNonExistentPiece) {
    // Attempt to delete piece 99, which does not exist in the metadata.
    // The system should fail fast with an unexpected error.
    run_delete_test({99}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "not_exist",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete piece 4 (active piece) - should fail
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteActivePiece) {
    // Attempt to delete piece 4, which is in ACTIVE state.
    // This is strictly forbidden.
    run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "is_active_piece",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete pieces 1,2 (mix of valid and invalid pieces) - should fail due to already deleted piece
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailWhenListContainsValidAndInvalidPieces) {
    // Attempt to delete a valid piece (1) and an already deleted piece (2).
    // The presence of the invalid piece should cause the entire job to fail.
    run_delete_test({1, 2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "already_deleted",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_);
}


// =================================================================================
// Fixture 7 (New): Testing scenarios without backup sets
// =================================================================================
class TestBackupCleanPieceSelector_NoBackupCases : public TestBackupCleanPieceSelectorBase {
protected:
    void SetUp() override {
        TestBackupCleanPieceSelectorBase::SetUp();
        ObArray<share::ObTenantArchivePieceAttr> pieces;
        share::ObTenantArchivePieceAttr piece;

        const char* active_piece_path = "file:///archive_active";
        const int64_t active_piece_dest_id = 1;

        SCN scn_100, scn_200;
        scn_100.convert_for_gts(100);
        scn_200.convert_for_gts(200);

        create_piece(piece, 1, active_piece_dest_id, active_piece_path, scn_100); pieces.push_back(piece);
        create_piece(piece, 2, active_piece_dest_id, active_piece_path, scn_200); pieces.push_back(piece);

        // Setup test data
        test_pieces_ = pieces;
        test_dest_pairs_.push_back(std::make_pair(1, active_piece_dest_id));
    }

    ObArray<share::ObTenantArchivePieceAttr> test_pieces_;
    ObBackupPathString test_current_path_;
    ObArray<std::pair<int64_t, int64_t>> test_dest_pairs_;
    ObArray<share::ObBackupSetFileDesc> test_sets_; // Empty backup sets
};

// Test: Delete piece 1 when backup dest exists but no backup set - should fail due to no retention SCN
TEST_F(TestBackupCleanPieceSelector_NoBackupCases, FailWhenBackupDestExistsButNoBackupSet) {
    ObBackupPathString backup_path;
    backup_path.assign("file:///backup_active");
    test_current_path_ = backup_path;
    // No backup sets are added to test_sets_

    run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot_delete_backup_piece_in_current_path_when_delete_policy_is_set",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}


TEST_F(TestBackupCleanPieceSelector_NoBackupCases, FailWhenBackupDestExistsButNoBackupSet2) {
    ObBackupPathString backup_path;
    backup_path.assign("file:///backup_active");
    test_current_path_ = backup_path;
    // No backup sets are added to test_sets_

    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no_full_backup_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

TEST_F(TestBackupCleanPieceSelector_NoBackupCases, FailWhenNoBackupDestExists2) {
    ObBackupPathString backup_path;
    backup_path.assign("file:///backup_active");
    test_current_path_ = backup_path;
    // No backup sets are added to test_sets_
    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no_full_backup_exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

} // namespace backup
} // namespace oceanbase

// --- Main function ---
int main(int argc, char **argv) {
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  system("rm -f test_backup_clean_piece_selector.log*");
  logger.set_file_name("test_backup_clean_piece_selector.log", true, true);
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}