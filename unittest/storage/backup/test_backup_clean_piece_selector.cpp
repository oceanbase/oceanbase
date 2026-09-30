/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

    // Create a piece with FULL control over its four SCN fields, so that the cross-boundary log
    // group scenario(start_scn_ > scn while the piece still really covers scn) can be expressed.
    void create_piece_with_scns(share::ObTenantArchivePieceAttr &piece, int64_t piece_id, int64_t dest_id,
                      const char* path, int64_t start_scn, int64_t checkpoint_scn, int64_t end_scn,
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
        piece.start_scn_.convert_for_gts(start_scn);
        piece.checkpoint_scn_.convert_for_gts(checkpoint_scn);
        piece.max_scn_.convert_for_gts(checkpoint_scn);
        piece.end_scn_.convert_for_gts(end_scn);
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
    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no full backup exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete an active piece (piece 5) - should fail
TEST_F(TestBackupCleanPieceSelector_BasicCases, FailToDeleteActivePiece) {
    run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "is active piece",
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
    run_delete_test({99}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "not exist",
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
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller piece exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete pieces 1,2,4,5 with gap (missing piece 3) - should fail (sequential constraint)
TEST_F(TestBackupCleanPieceSelector_TwoPathCases, FailToDeletePiecesWithGap) {
    run_delete_test({1,2,4,5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller piece exists",
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
    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no full backup exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete old pieces 1,2 when backup dest exists but no backup sets - should fail
TEST_F(TestBackupCleanPieceSelector_RetentionPolicyCases, FailToDeleteOldPiecesWhenNoBackupSets) {
    run_delete_test({1, 2 }, OB_ENTRY_NOT_EXIST, {}, "no full backup exists",
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
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller piece exists",
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
    run_delete_test({5}, OB_ENTRY_NOT_EXIST, {}, "no full backup exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}


// Test: Delete piece 5 from current path but no backup sets exist - should fail
TEST_F(TestBackupCleanPieceSelector_MultiPathCases, FailToDeletePieceWhenNoBackupSetsExist2) {
    run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {},
                        "cannot delete backup piece in current path when delete policy is set",
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
    run_delete_test({3, 4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed for oldest full backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 4 (protected piece with SCN 400 >= retention SCN 350) - should fail
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, FailToDeleteSingleProtectedPiece) {
    run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed for oldest full backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 1,3 (with gap, skipping piece 2) - should fail due to sequential deletion rule
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, FailToDeleteWithGap) {
    run_delete_test({1, 3}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller piece exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 2 (without deleting piece 1 first) - should fail due to sequential deletion rule
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessWhenProtectedPieceNotRequested) {
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "smaller piece exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 1 (first piece in sequence) - should succeed
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessWhenFirstPieceIsDeleted) {
    run_delete_test({1}, OB_SUCCESS, {1}, nullptr,
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete piece 1 (first piece in sequence) - should succeed
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention, SuccessWhenFirstPieceIsDeleted2) {
    run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot delete backup piece in current path when delete policy is set",
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
    run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot delete backup piece in current path when delete policy is set",
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
    run_delete_test({5}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed for oldest full backup",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 3,4 from different destinations - should fail due to multi-dest restriction
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailWhenMultiDest3) {
    run_delete_test({3, 4}, OB_NOT_SUPPORTED, {}, "multiple dest is not supported",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// Test: Delete pieces 4,5,6 (including protected pieces) - should fail due to retention policy
TEST_F(TestBackupCleanPieceSelector_ActivePathRetention_OldPathCanDelete, FailDueToBackupsetNeeded2) {
    run_delete_test({4, 5, 6}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "needed for oldest full backup",
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
    run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "already deleted",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete piece 2 (DELETING status, can be retried) - should fail
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteDeletingPiece) {
    ObArray<std::pair<int64_t, int64_t>> active_dest_pairs;
    active_dest_pairs.push_back(std::make_pair(1, 1));
    run_delete_test({2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot delete backup piece in current path when delete policy is set",
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
    run_delete_test({99}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "not exist",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete piece 4 (active piece) - should fail
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailToDeleteActivePiece) {
    // Attempt to delete piece 4, which is in ACTIVE state.
    // This is strictly forbidden.
    run_delete_test({4}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "is active piece",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, 0);
}

// Test: Delete pieces 1,2 (mix of valid and invalid pieces) - should fail due to already deleted piece
TEST_F(TestBackupCleanPieceSelector_SpecialCases, FailWhenListContainsValidAndInvalidPieces) {
    // Attempt to delete a valid piece (1) and an already deleted piece (2).
    // The presence of the invalid piece should cause the entire job to fail.
    run_delete_test({1, 2}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "already deleted",
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

    run_delete_test({1}, OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, {}, "cannot delete backup piece in current path when delete policy is set",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, true);
}


TEST_F(TestBackupCleanPieceSelector_NoBackupCases, FailWhenBackupDestExistsButNoBackupSet2) {
    ObBackupPathString backup_path;
    backup_path.assign("file:///backup_active");
    test_current_path_ = backup_path;
    // No backup sets are added to test_sets_

    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no full backup exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

TEST_F(TestBackupCleanPieceSelector_NoBackupCases, FailWhenNoBackupDestExists2) {
    ObBackupPathString backup_path;
    backup_path.assign("file:///backup_active");
    test_current_path_ = backup_path;
    // No backup sets are added to test_sets_
    run_delete_test({1}, OB_ENTRY_NOT_EXIST, {}, "no full backup exists",
                    test_sets_, test_current_path_, test_pieces_, test_dest_pairs_, false);
}

// =================================================================================
// Fixture 8: clean point coverage in check_piece_can_be_deleted_
// =================================================================================
// A piece whose nominal range [start_scn_, end_scn_) covers the clean point(start_replay_scn) is
// required by the restore path even when all the log it really contains is before the clean point:
// ObArchiveStore::get_piece_paths_in_range accepts a piece list only if the FIRST piece satisfies
// "start_scn_ <= clean point < end_scn_", and check_piece_continuity_between_two_scn requires a
// not-deleted floor piece with "start_scn <= clean point". So such a piece can be reclaimed only if
// ANOTHER kept AVAILABLE piece also nominally covers the clean point(possible when the scn ranges of
// two rounds overlap), see check_scn_covered_by_other_piece_.
//
// This fixture directly drives ObBackupDeleteSelector::check_piece_can_be_deleted_(), which is the
// path that get_one_dest_deletable_backup_piece_infos_() takes for every candidate piece.
class TestBackupCleanPieceSelector_CrossBoundaryCoverage : public TestBackupCleanPieceSelectorBase {
protected:
    // Init a selector, ready to call check_piece_can_be_deleted_ / check_scn_covered_by_other_piece_.
    void build_selector(ObBackupDeleteSelector &selector) {
        job_attr_.reset();
        job_attr_.job_id_ = 1001;
        job_attr_.tenant_id_ = 1002;
        job_attr_.incarnation_id_ = 1;
        job_attr_.clean_type_ = ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP;
        ASSERT_EQ(OB_SUCCESS, selector.init(mock_sql_proxy_, *mock_schema_service_, job_attr_,
                                            *mock_rpc_proxy_, *mock_delete_mgr_));
    }

    // Sort the pieces in ascending order of piece id, the same as get_one_dest_deletable_backup_piece_infos_
    // does before passing the pieces down to check_piece_can_be_deleted_.
    void sort_pieces(ObArray<share::ObTenantArchivePieceAttr> &pieces) {
        ObBackupDeleteSelector::CompareBackupPieceInfo cmp;
        lib::ob_sort(pieces.begin(), pieces.end(), cmp);
    }

    ObBackupCleanJobAttr job_attr_;
};

// The old piece(#1) is judged deletable via the nominal boundary shortcut: its whole nominal range is
// before the clean point.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, DeletableWhenWholeNominalRangeBeforeCleanPoint) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 200, 300]; p2: [300, 500, 600]. clean point = 400.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 200, 300);
    create_piece_with_scns(p2, 2, 1, "file:///path", 300, 500, 600);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(400);
    bool can_be_deleted = false;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_TRUE(can_be_deleted);
}

// The old piece(#1) is judged NOT deletable because it really contains log needed by the clean point
// (its checkpoint_scn_/max_scn_ is after the clean point).
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotDeletableWhenReallyContainsNeededLog) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 500, 600]; clean point = 400 < checkpoint_scn_ 500.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 500, 600);
    create_piece_with_scns(p2, 2, 1, "file:///path", 600, 800, 900);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(400);
    bool can_be_deleted = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_FALSE(can_be_deleted);
}

// The nominal range of the old piece(#1) still covers the clean point, its real upper bound
// (checkpoint_scn_/max_scn_) is already before it, and the log BYTES at the clean point physically
// live in the FIRST cross-boundary log group of piece#2(p1.end_scn_ == p2.start_scn_ == 500 and
// p1.checkpoint_scn_(300) < 450, so the log at 450 is inside p2's first log group). p1 must still be
// KEPT: it is the only piece whose nominal range covers 450, and the restore path locates the first
// piece by "start_scn_ <= start_replay_scn"(see ObArchiveStore::get_piece_paths_in_range), it has no
// cross-boundary tolerance at the START boundary. Deleting p1 would fail the restore of the retained
// backup set with "No enough log for restore" although every needed log byte still exists in p2.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotDeletableWhenOnlyCrossBoundaryGroupContainsCleanPoint) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 300, 500]; p2: [500, 800, 900]. clean point = 450.
    // p1.max_scn_/checkpoint_scn_(300) < 450 < p1.end_scn_(500), so p1 no longer really contains 450,
    // but p1 is still the only piece whose nominal range covers 450.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 500);
    create_piece_with_scns(p2, 2, 1, "file:///path", 500, 800, 900);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);

    // First verify the coverage helper directly.
    bool is_scn_covered_by_other_kept_piece = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_FALSE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_FALSE(can_be_deleted);
}

// Negative case: the nominal range of the old piece(#1) covers the clean point, its real upper bound
// is before it, but NO other piece nominally covers the clean point, so p1 must be kept(deleting it
// would break the restorability of the retained backup set).
// p2.start_scn_(500) > clean point(450), so p2 does not cover 450.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotDeletableWhenNoOtherPieceCovers) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 300, 480]; p2: [500, 800, 900]. clean point = 450.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 480);
    create_piece_with_scns(p2, 2, 1, "file:///path", 500, 800, 900);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);

    bool is_scn_covered_by_other_kept_piece = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_FALSE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_FALSE(can_be_deleted);
}

// Negative case: the "other" piece neither nominally covers the clean point(start_scn_ 500 > 450) nor
// has really archived past it(checkpoint_scn_ 400 < 450, it is itself a candidate to be deleted).
// Either alone disqualifies it as a covering kept piece.
// p1: [100, 300, 500]; p2: [500, 400, 900].
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotCoveredWhenOtherPieceCheckpointBeforeCleanPoint) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 500);
    create_piece_with_scns(p2, 2, 1, "file:///path", 500, 400, 900);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);
    bool is_scn_covered_by_other_kept_piece = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_FALSE(is_scn_covered_by_other_kept_piece);
}

// Regular coverage: another kept AVAILABLE piece#2 nominally covers the clean point
// (start_scn_ <= clean point < end_scn_) and has really archived past it(checkpoint_scn_ > clean
// point), so p1 is reclaimable. This is e.g. the case when round 1 was stopped in the middle of p1
// (nominal end_scn_ ahead of the real progress) and round 2 started before that nominal end.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, DeletableWhenCoveredByRegularPiece) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 300, 500]; p2: [400, 800, 900]. clean point = 450, p2.start_scn_(400) <= 450 < 800.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 500);
    create_piece_with_scns(p2, 2, 1, "file:///path", 400, 800, 900);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);
    bool can_be_deleted = false;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_TRUE(can_be_deleted);
}

// Boundary: piece#2 has checkpoint_scn_ EXACTLY equal to the clean point. Such a piece is itself a
// deletion candidate of the very same job(get_candidate_obsolete_backup_pieces selects
// "checkpoint_scn <= start_replay_scn"), so it may be deleted in the same round and must NOT be
// treated as a covering piece which will be kept, otherwise two such pieces could cover for each
// other and both get deleted, leaving the clean point uncovered. This guards the strict
// `checkpoint_scn_ > scn` requirement in check_scn_covered_by_other_piece_.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotCoveredWhenOtherPieceCheckpointEqualsCleanPoint) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 300, 500]; p2: [400, 450, 900]. clean point = 450 == p2.checkpoint_scn_.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 500);
    create_piece_with_scns(p2, 2, 1, "file:///path", 400, 450, 900);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);
    bool is_scn_covered_by_other_kept_piece = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_FALSE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_FALSE(can_be_deleted);
}

// Coverage still holds when the pieces are fed to get_pieces() OUT OF ORDER. Same data as
// DeletableWhenCoveredByRegularPiece, but pushed as [p2, p1] instead of [p1, p2].
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, DeletableWhenCoveredByRegularPieceUnordered) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 300, 500]; p2: [400, 800, 900]. clean point = 450.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 500);
    create_piece_with_scns(p2, 2, 1, "file:///path", 400, 800, 900);
    // Push in reverse order on purpose, then sort just like
    // get_one_dest_deletable_backup_piece_infos_ does before passing the pieces down.
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);

    bool is_scn_covered_by_other_kept_piece = false;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_TRUE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = false;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_TRUE(can_be_deleted);
}

// The covering piece must be AVAILABLE: the restore path skips every piece whose file_status is not
// AVAILABLE(see ObArchiveStore::get_piece_paths_in_range), so a piece being deleted(DELETING) can not
// be treated as a covering kept piece even if its nominal range and checkpoint_scn_ qualify.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotCoveredWhenCoveringPieceNotAvailable) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: [100, 300, 500]; p2: [400, 800, 900] but DELETING. clean point = 450.
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 300, 500);
    create_piece_with_scns(p2, 2, 1, "file:///path", 400, 800, 900,
                           share::ObArchivePieceStatus::Status::FROZEN,
                           share::ObBackupFileStatus::BACKUP_FILE_DELETING);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(450);
    bool is_scn_covered_by_other_kept_piece = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_FALSE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_FALSE(can_be_deleted);
}

// The covering piece must belong to the SAME archive dest. Restore only ever uses the pieces of one
// dest(ObArchiveStore::get_piece_paths_in_range takes the dest_id of its first piece and skips every
// piece with a different dest_id), so a piece of dest B can never be the proof that the clean point
// is still covered on dest A.
//
// This guards the following interleaving: get_all_dest_backup_piece_infos_() reads the (dest_no,
// dest_id) pairs and the path of a dest_no with two separate unlocked SQLs, so a concurrent
// "alter system set log_archive_dest_n"(allowed while archive is stopped, and NOT excluded by the
// is_cleaning check, which only DELETE_BACKUP_ALL sets) which repoints one dest_no from dest A to
// dest B can make the candidate pieces come from the path of B while the piece list used to judge the
// coverage comes from A. get_one_dest_deletable_backup_piece_infos_() rejects such a candidate by
// dest_id, and this check here is the same invariant at the place where it matters.
// Without the dest_id check, p1 of dest B below(<100, 140, 200>, stopped in the middle so its nominal
// end_scn_ stays ahead of its real progress) would be judged deletable by p2 of dest A
// (<130, 250, 300>), and restoring from B would then fail with "No enough log for restore" because
// the first remaining piece of B starts after the clean point.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, NotCoveredWhenCoveringPieceBelongsToOtherDest) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    // p1: dest 1(path B) [100, 140, 200]; p2: dest 2(path A) [130, 250, 300]. clean point = 150.
    create_piece_with_scns(p1, 1, 1, "file:///path_b", 100, 140, 200);
    create_piece_with_scns(p2, 2, 2, "file:///path_a", 130, 250, 300);
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(150);

    bool is_scn_covered_by_other_kept_piece = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_FALSE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = true;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_FALSE(can_be_deleted);
}

// Same scn layout as NotCoveredWhenCoveringPieceBelongsToOtherDest, but the covering piece belongs to
// the same dest as the piece to delete(both dest 1, two overlapping rounds of one dest). Then the
// coverage does hold and p1 is reclaimable. This makes sure the dest_id check above does not reject
// the legitimate cross-ROUND coverage inside one dest.
TEST_F(TestBackupCleanPieceSelector_CrossBoundaryCoverage, CoveredWhenCoveringPieceOfSameDestAnotherRound) {
    ObArray<share::ObTenantArchivePieceAttr> pieces;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path_b", 100, 140, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path_b", 130, 250, 300);
    p2.key_.round_id_ = 2;
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, pieces.push_back(p2));
    sort_pieces(pieces);

    ObBackupDeleteSelector selector;
    build_selector(selector);

    SCN clean_point; clean_point.convert_for_gts(150);

    bool is_scn_covered_by_other_kept_piece = false;
    ASSERT_EQ(OB_SUCCESS, selector.check_scn_covered_by_other_piece_(p1, clean_point, pieces, is_scn_covered_by_other_kept_piece));
    ASSERT_TRUE(is_scn_covered_by_other_kept_piece);

    bool can_be_deleted = false;
    ASSERT_EQ(OB_SUCCESS, selector.check_piece_can_be_deleted_(p1, clean_point, pieces, can_be_deleted));
    ASSERT_TRUE(can_be_deleted);
}

// =================================================================================
// Fixture 9: the archive log the restore of the retained backup set starts from
// =================================================================================
// The restore does NOT start to fetch the archive log at the lsn of start_replay_scn, but at the start
// of the 64M palf block that lsn falls in: what the backup writes into the backup set is
// palf_meta_.curr_lsn_, which ObLogHandler::get_palf_base_info has rounded DOWN to a block boundary,
// and the restore advances palf to exactly that lsn and asks the archive for the log from there.
// Meanwhile the archive splits a block across two pieces when the piece switches in the middle of it
// (the file id is "lsn / 64M + 1", the new piece starts a new file with the same file id at offset 0 and
// the first half of the block is not re-archived). So the piece which holds the first half of that block
// must be kept, although every SCN based check says it is obsolete. This fixture drives
// ObBackupDeleteSelector::filter_pieces_needed_by_restore_(), the check which prevents it.
class TestBackupCleanPieceSelector_RestoreStartLSN : public TestBackupCleanPieceSelectorBase {
protected:
    static const uint64_t BLOCK_SIZE = 64L * 1024L * 1024L;  // palf::PALF_BLOCK_SIZE

    // Init a selector and replace its data provider with a mock one, which is returned so that the
    // test can feed the ls meta of the backup set and the piece infos into it.
    MockBackupDataProvider *build_selector(ObBackupDeleteSelector &selector) {
        job_attr_.reset();
        job_attr_.job_id_ = 1001;
        job_attr_.tenant_id_ = 1002;
        job_attr_.incarnation_id_ = 1;
        job_attr_.clean_type_ = ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP;
        EXPECT_EQ(OB_SUCCESS, selector.init(mock_sql_proxy_, *mock_schema_service_, job_attr_,
                                            *mock_rpc_proxy_, *mock_delete_mgr_));
        MockBackupDataProvider *mock_data_provider = OB_NEW(MockBackupDataProvider, "BackupProvider");
        EXPECT_NE(nullptr, mock_data_provider);
        if (nullptr != selector.data_provider_) {
            OB_DELETE(IObBackupDataProvider, "BackupClean", selector.data_provider_);
            selector.data_provider_ = nullptr;
        }
        selector.data_provider_ = mock_data_provider;
        return mock_data_provider;
    }

    // The retained backup set, whose restore the pieces have to stay usable for.
    void create_clean_point(share::ObBackupSetFileDesc &desc, const bool plus_archivelog = false) {
        SCN start_replay_scn;
        start_replay_scn.convert_for_gts(450);
        create_backup_set(desc, 10, ObBackupType::FULL_BACKUP, 0, 0, 10/*dest_id*/,
                          "file:///backup_active", 1000, ObBackupSetFileDesc::SUCCESS, start_replay_scn);
        desc.plus_archivelog_ = plus_archivelog;
    }

    void add_ls_start_lsn(ObArray<ObLSRestoreStartLSN> &ls_start_lsn_array, const int64_t ls_id,
                          const uint64_t start_lsn) {
        ObLSRestoreStartLSN ls_start_lsn;
        ls_start_lsn.ls_id_ = ObLSID(ls_id);
        ls_start_lsn.start_lsn_ = palf::LSN(start_lsn);
        ASSERT_EQ(OB_SUCCESS, ls_start_lsn_array.push_back(ls_start_lsn));
    }

    // Record that the log stream `ls_id` has archived [min_lsn, max_lsn) into the piece `piece_id`.
    // A log stream which is not added to a piece has no archived data in it at all.
    void add_ls_info_to_piece(ObArray<share::ObPieceInfoDesc> &piece_info_descs, const int64_t piece_id,
                              const int64_t ls_id, const uint64_t min_lsn, const uint64_t max_lsn) {
        share::ObPieceInfoDesc *target = nullptr;
        for (int64_t i = 0; nullptr == target && i < piece_info_descs.count(); ++i) {
            if (piece_info_descs.at(i).piece_id_ == piece_id) {
                target = &piece_info_descs.at(i);
            }
        }
        if (nullptr == target) {
            share::ObPieceInfoDesc desc;
            desc.dest_id_ = 1;
            desc.round_id_ = 1;
            desc.piece_id_ = piece_id;
            ASSERT_EQ(OB_SUCCESS, piece_info_descs.push_back(desc));
            target = &piece_info_descs.at(piece_info_descs.count() - 1);
        }
        share::ObSingleLSInfoDesc ls_info;
        ls_info.dest_id_ = 1;
        ls_info.round_id_ = 1;
        ls_info.piece_id_ = piece_id;
        ls_info.ls_id_ = ObLSID(ls_id);
        ls_info.min_lsn_ = min_lsn;
        ls_info.max_lsn_ = max_lsn;
        ASSERT_EQ(OB_SUCCESS, target->filelist_.push_back(ls_info));
    }

    void collect_piece_ids(const ObIArray<share::ObTenantArchivePieceAttr> &piece_list,
                           std::set<int64_t> &piece_ids) {
        piece_ids.clear();
        for (int64_t i = 0; i < piece_list.count(); ++i) {
            piece_ids.insert(piece_list.at(i).key_.piece_id_);
        }
    }

    ObBackupCleanJobAttr job_attr_;
};

// All the log of the only log stream is already before the lsn the restore starts from, so every
// candidate piece is reclaimable. Only the newest candidate piece has to be read to know that: the
// archived lsn range of a log stream grows monotonically with the piece id, so once it is cleared by a
// piece, no older piece of the same dest can hold the log the restore needs.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, DeletableWhenAllLogBeforeRestoreStartLSN) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 200, 300);
    create_piece_with_scns(p2, 2, 1, "file:///path", 300, 400, 450);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));

    // The restore starts from the beginning of the 3rd block, both pieces are entirely before it.
    add_ls_start_lsn(ls_start_lsn_array, 1001, 2 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1001, 0, BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 2, 1001, BLOCK_SIZE, 2 * BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1, 2}), piece_ids);
    ASSERT_EQ(1, mock_data_provider->get_load_piece_info_desc_count());
}

// The core case this check exists for: the 64M block the restore starts from was split by the piece
// switch, so the pieces holding it still contain the log the restore asks for although all of their log
// is before start_replay_scn(every SCN based check has already judged them obsolete). Piece#2 and #3
// hold log at or after the restore start lsn and must be kept, piece#1 is reclaimable.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, KeepPiecesHoldingTheBlockTheRestoreStartsFrom) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    share::ObTenantArchivePieceAttr p1, p2, p3;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 150, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path", 200, 250, 300);
    create_piece_with_scns(p3, 3, 1, "file:///path", 300, 350, 400);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p3));

    // The restore starts from the beginning of the 3rd block(2 * 64M), which piece#2 has been writing
    // when the piece switched, so the first half of that block lives in piece#2 and the second half in
    // piece#3.
    add_ls_start_lsn(ls_start_lsn_array, 1001, 2 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1001, 0, BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 2, 1001, BLOCK_SIZE, 2 * BLOCK_SIZE + 100);
    add_ls_info_to_piece(piece_info_descs, 3, 1001, 2 * BLOCK_SIZE + 100, 3 * BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1}), piece_ids);
}

// The pieces to keep are the suffix starting at the oldest piece which still holds needed log. A log
// stream which archived nothing into a piece is NOT missing from its piece info, it is listed with
// "min_lsn_ == max_lsn_"(see record_piece_info), i.e. its idle entry still reports the lsn it has reached,
// so it is judged exactly like a busy one. Here log stream 1002 has already been archived past the
// restore start lsn, while log stream 1001, which went idle after piece#2, still has the block the
// restore starts from in piece#2, and its idle entry in piece#3 reports the very same lsn.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, KeepTheWholeSuffixWhenAnOlderPieceIsNeeded) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    share::ObTenantArchivePieceAttr p1, p2, p3;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 150, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path", 200, 250, 300);
    create_piece_with_scns(p3, 3, 1, "file:///path", 300, 350, 400);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p3));

    add_ls_start_lsn(ls_start_lsn_array, 1001, 2 * BLOCK_SIZE);
    add_ls_start_lsn(ls_start_lsn_array, 1002, 2 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1001, 0, BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1002, 0, BLOCK_SIZE / 2);
    add_ls_info_to_piece(piece_info_descs, 2, 1001, BLOCK_SIZE, 2 * BLOCK_SIZE + 100);
    // ls 1002 archived nothing into piece#2 and ls 1001 nothing into piece#3, both are still listed with
    // an empty(min_lsn_ == max_lsn_) range.
    add_ls_info_to_piece(piece_info_descs, 2, 1002, BLOCK_SIZE / 2, BLOCK_SIZE / 2);
    add_ls_info_to_piece(piece_info_descs, 3, 1001, 2 * BLOCK_SIZE + 100, 2 * BLOCK_SIZE + 100);
    add_ls_info_to_piece(piece_info_descs, 3, 1002, BLOCK_SIZE / 2, BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1}), piece_ids);
}

// Many candidates at once: the pieces to keep are the suffix starting at the first kept one, no matter
// how long that suffix is. Piece#4(index 3) is the oldest one still holding needed log, so piece#1 ~ #3
// are the only reclaimable ones and piece#4 ~ #10 must all be kept.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, KeepTheWholeSuffixOfALongCandidateList) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    const int64_t piece_cnt = 10;
    for (int64_t piece_id = 1; piece_id <= piece_cnt; ++piece_id) {
        share::ObTenantArchivePieceAttr piece;
        create_piece_with_scns(piece, piece_id, 1, "file:///path", 100 * piece_id, 100 * piece_id + 50,
                               100 * (piece_id + 1));
        ASSERT_EQ(OB_SUCCESS, piece_list.push_back(piece));
        // The log stream crosses a block boundary in every piece, so the block the restore starts from
        // (the 5th one) is split between piece#4 and piece#5, and piece#4 still holds its first half.
        add_ls_info_to_piece(piece_info_descs, piece_id, 1001, (piece_id - 1) * BLOCK_SIZE,
                             piece_id * BLOCK_SIZE + 100);
    }
    add_ls_start_lsn(ls_start_lsn_array, 1001, 4 * BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1, 2, 3}), piece_ids);
    // The walk stops at piece#3, which has archived all of its log before the restore start lsn, so
    // piece#10 ~ #3 have been read and piece#2, #1 have not.
    ASSERT_EQ(8, mock_data_provider->get_load_piece_info_desc_count());
}

// A log stream of the retained backup set which was created AFTER every candidate piece is absent from
// all of them, so no candidate can ever report that its log has been archived past the restore start lsn.
// The walk must not keep reading every candidate piece info because of it: the piece info file of a
// frozen piece lists every log stream which had started archiving at or before that piece, so a log
// stream which is absent from it had not been created yet and none of the older pieces of the same round
// can hold its log either. Here piece#10 clears log stream 1001 by lsn and log stream 1002 by absence, so
// exactly ONE piece info is read and every candidate is reclaimed.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, StopLookingBackwardsWhenALSIsAbsentFromThePiece) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    const int64_t piece_cnt = 10;
    for (int64_t piece_id = 1; piece_id <= piece_cnt; ++piece_id) {
        share::ObTenantArchivePieceAttr piece;
        create_piece_with_scns(piece, piece_id, 1, "file:///path", 100 * piece_id, 100 * piece_id + 50,
                               100 * (piece_id + 1));
        ASSERT_EQ(OB_SUCCESS, piece_list.push_back(piece));
        add_ls_info_to_piece(piece_info_descs, piece_id, 1001, (piece_id - 1) * BLOCK_SIZE,
                             piece_id * BLOCK_SIZE);
    }
    // ls 1001 has been archived exactly up to the lsn the restore starts from, so every candidate piece
    // is obsolete for it. ls 1002 was created after piece#10 and does not appear in any candidate piece.
    add_ls_start_lsn(ls_start_lsn_array, 1001, piece_cnt * BLOCK_SIZE);
    add_ls_start_lsn(ls_start_lsn_array, 1002, 0);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), piece_ids);
    ASSERT_EQ(1, mock_data_provider->get_load_piece_info_desc_count());
}

// "The log stream is absent from the piece, so it had not started archiving yet" is only conclusive
// inside ONE archive round: a new round restarts the archive of every log stream, and a log stream which
// starts archiving late in the new round(e.g. its leader was not available when the round started) is
// absent from the first pieces of that round although it does have log in the pieces of the previous
// round. So the walk skips the rest of the round it is in, but goes on with the newest piece of the
// previous round instead of stopping. Here log stream 1002 is absent from piece#3(round 2) but still
// holds the block the restore starts from in piece#2(round 1), which must be kept.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, AbsenceOnlyClearsTheLSInsideOneArchiveRound) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    share::ObTenantArchivePieceAttr p1, p2, p3, p4;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 150, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path", 200, 250, 300);
    create_piece_with_scns(p3, 3, 1, "file:///path", 300, 350, 400);
    create_piece_with_scns(p4, 4, 1, "file:///path", 400, 450, 500);
    // The archive was stopped after piece#2 and started again, so piece#3 and piece#4 are in round 2.
    p3.key_.round_id_ = 2;
    p4.key_.round_id_ = 2;
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p3));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p4));

    // ls 1001 is archived in every piece and is cleared by piece#4 right away.
    add_ls_start_lsn(ls_start_lsn_array, 1001, 10 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1001, 0, BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 2, 1001, BLOCK_SIZE, 2 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 3, 1001, 2 * BLOCK_SIZE, 3 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 4, 1001, 3 * BLOCK_SIZE, 4 * BLOCK_SIZE);
    // ls 1002 started archiving in round 2 only at piece#4, but it does have log in round 1.
    add_ls_start_lsn(ls_start_lsn_array, 1002, 2 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1002, 0, BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 2, 1002, BLOCK_SIZE, 2 * BLOCK_SIZE + 100);
    add_ls_info_to_piece(piece_info_descs, 4, 1002, 2 * BLOCK_SIZE + 100, 3 * BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1}), piece_ids);
    // piece#4 ~ #1 have all been read: the absence of ls 1002 from piece#3 ends the walk of round 2, and
    // it is looked at again in piece#2, the newest piece of round 1.
    ASSERT_EQ(4, mock_data_provider->get_load_piece_info_desc_count());
}

// A piece whose archived range of a log stream starts at the very beginning of palf(min lsn 0) holds the
// first log that log stream ever archived, so no older piece holds any of its log. That clears the log
// stream even though the piece itself still holds needed log and has to be kept. Here piece#4 holds the
// first log of ls 1001 and the block the restore starts from, so the walk stops there after TWO reads
// although piece#1 ~ #3 are reclaimed.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, StopLookingBackwardsAtTheFirstArchivedPieceOfTheLS) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    const int64_t piece_cnt = 5;
    for (int64_t piece_id = 1; piece_id <= piece_cnt; ++piece_id) {
        share::ObTenantArchivePieceAttr piece;
        create_piece_with_scns(piece, piece_id, 1, "file:///path", 100 * piece_id, 100 * piece_id + 50,
                               100 * (piece_id + 1));
        ASSERT_EQ(OB_SUCCESS, piece_list.push_back(piece));
    }
    // ls 1001 was created while piece#4 was being archived, its first block is split between piece#4 and
    // piece#5 and the restore starts from the beginning of that block.
    add_ls_start_lsn(ls_start_lsn_array, 1001, 0);
    add_ls_info_to_piece(piece_info_descs, 4, 1001, 0, BLOCK_SIZE + 100);
    add_ls_info_to_piece(piece_info_descs, 5, 1001, BLOCK_SIZE + 100, 2 * BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1, 2, 3}), piece_ids);
    ASSERT_EQ(2, mock_data_provider->get_load_piece_info_desc_count());
}

// The ls meta of the retained backup set can not be read, so the lsn the restore needs is unknown. Keep
// every piece in this round instead of failing the whole clean job: the obsolete backup sets figured out
// in the same round are still reclaimed and the next round retries.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, KeepAllPiecesWhenBackupSetLSMetaCanNotBeRead) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 150, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path", 200, 250, 300);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsn_ret(OB_OBJECT_NOT_EXIST);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));
    ASSERT_EQ(0, piece_list.count());
}

// The piece info file of a candidate piece can not be read, so whether it holds the needed log is
// unknown. Keep it(and the pieces of the dest after it), but do NOT fail the clean job with
// OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED, and still reclaim the older pieces which are known to be
// obsolete.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, KeepPieceWhosePieceInfoCanNotBeRead) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 150, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path", 200, 250, 300);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));

    add_ls_start_lsn(ls_start_lsn_array, 1001, 2 * BLOCK_SIZE);
    // no piece info of piece#2, the mock fails to load it
    add_ls_info_to_piece(piece_info_descs, 1, 1001, 0, BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1}), piece_ids);
}

// A "plus archivelog" backup set carries the log it needs inside the backup set itself, so its restore
// does not read the archive pieces and the check is skipped(no piece info is read at all).
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, SkipTheCheckWhenBackupSetIsPlusArchivelog) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path", 100, 150, 200);
    create_piece_with_scns(p2, 2, 1, "file:///path", 200, 250, 300);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsn_ret(OB_OBJECT_NOT_EXIST);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point, true/*plus_archivelog*/);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));

    std::set<int64_t> piece_ids;
    collect_piece_ids(piece_list, piece_ids);
    ASSERT_EQ(std::set<int64_t>({1, 2}), piece_ids);
    ASSERT_EQ(0, mock_data_provider->get_load_piece_info_desc_count());
}

// The candidates are always expected to belong to ONE archive dest: a delete obsolete job refuses to run
// when more than one archive dest is valid(get_obsolete_backup_set_infos_helper_ fails with
// OB_ERR_UNEXPECTED), and get_one_dest_deletable_backup_piece_infos_ drops every candidate whose dest_id
// does not match the dest it works on. Should the invariant be broken anyway - e.g. an "alter system set
// log_archive_dest_n" repointed a dest_no in the middle of the job, which is the very race the dest_id
// check in get_one_dest_deletable_backup_piece_infos_ guards against - the check must not compare the
// lsn of one dest with the pieces of another one, so it keeps every piece in this round.
TEST_F(TestBackupCleanPieceSelector_RestoreStartLSN, KeepAllPiecesWhenCandidatesSpanTwoDests) {
    ObArray<share::ObTenantArchivePieceAttr> piece_list;
    ObArray<ObLSRestoreStartLSN> ls_start_lsn_array;
    ObArray<share::ObPieceInfoDesc> piece_info_descs;
    share::ObTenantArchivePieceAttr p1, p2;
    create_piece_with_scns(p1, 1, 1, "file:///path_1", 100, 150, 200);
    create_piece_with_scns(p2, 2, 2, "file:///path_2", 200, 250, 300);
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p1));
    ASSERT_EQ(OB_SUCCESS, piece_list.push_back(p2));

    add_ls_start_lsn(ls_start_lsn_array, 1001, 2 * BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 1, 1001, 0, BLOCK_SIZE);
    add_ls_info_to_piece(piece_info_descs, 2, 1001, BLOCK_SIZE, 2 * BLOCK_SIZE);

    ObBackupDeleteSelector selector;
    MockBackupDataProvider *mock_data_provider = build_selector(selector);
    mock_data_provider->set_ls_restore_start_lsns(ls_start_lsn_array);
    mock_data_provider->set_piece_info_descs(piece_info_descs);

    share::ObBackupSetFileDesc clean_point;
    create_clean_point(clean_point);
    ASSERT_EQ(OB_SUCCESS, selector.filter_pieces_needed_by_restore_(clean_point, piece_list));
    ASSERT_EQ(0, piece_list.count());
    ASSERT_EQ(0, mock_data_provider->get_load_piece_info_desc_count());
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