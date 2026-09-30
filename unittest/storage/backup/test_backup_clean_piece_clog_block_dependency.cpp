/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <memory>

#define private public
#define protected public

#include "src/rootserver/backup/ob_backup_clean_selector.h"
#include "share/backup/ob_archive_store.h"
#include "share/ls/ob_ls_info.h"
#include "unittest/storage/backup/test_backup_clean_selector_include.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
using namespace testing;

namespace oceanbase {
namespace backup {

// 测试数据结构定义
struct LSFileInfo {
  ObLSID ls_id;
  std::vector<int64_t> file_ids;

  LSFileInfo(ObLSID id, std::vector<int64_t> files) : ls_id(id), file_ids(files) {}
};

struct PieceInfo {
  int64_t dest_id;
  int64_t round_id;
  int64_t piece_id;
  std::vector<LSFileInfo> ls_files;

  PieceInfo(int64_t dest, int64_t round, int64_t piece, std::vector<LSFileInfo> files)
    : dest_id(dest), round_id(round), piece_id(piece), ls_files(files) {}
};

class TestBackupCleanPieceClogBlockDependency : public ::testing::Test {
protected:
  // Must be a user tenant id, otherwise ObTenantArchivePieceAttr::Key::is_pkey_valid() is false.
  static const uint64_t TEST_TENANT_ID = 1002;

  void SetUp() override {
    ASSERT_EQ(OB_SUCCESS, mock_sql_proxy_.init(nullptr));
    mock_schema_service_ = std::make_unique<MockObMultiVersionSchemaService>();
    mock_rpc_proxy_ = std::make_unique<MockObSrvRpcProxy>();
    mock_delete_mgr_ = std::make_unique<MockObUserTenantBackupDeleteMgr>();
  }

  void TearDown() override {}

  // Helper function to create a piece info desc
  void create_piece_info_desc(ObPieceInfoDesc &piece_desc,
                              int64_t dest_id,
                              int64_t round_id,
                              int64_t piece_id) {
    piece_desc.dest_id_ = dest_id;
    piece_desc.round_id_ = round_id;
    piece_desc.piece_id_ = piece_id;
  }

  // The archive file id of a log stream is a pure function of the lsn("lsn / PALF_BLOCK_SIZE + 1",
  // archive::cal_archive_file_id with MAX_ARCHIVE_FILE_SIZE == palf::PALF_BLOCK_SIZE), so a file id is
  // just the id of the palf block the log lives in. The lsn range a piece holds is derived from its file
  // id list below: it starts at the beginning of the first block and ends in the middle of the last one,
  // which is what a piece switch in the middle of a block looks like.
  static uint64_t block_start_lsn(const int64_t file_id) {
    return static_cast<uint64_t>(file_id - 1) * static_cast<uint64_t>(palf::PALF_BLOCK_SIZE);
  }

  // Helper function to create a single LS info desc
  void create_single_ls_info_desc(ObSingleLSInfoDesc &ls_desc,
                                  int64_t dest_id,
                                  int64_t round_id,
                                  int64_t piece_id,
                                  const ObLSID &ls_id) {
    ls_desc.dest_id_ = dest_id;
    ls_desc.round_id_ = round_id;
    ls_desc.piece_id_ = piece_id;
    ls_desc.ls_id_ = ls_id;
    ls_desc.start_scn_ = SCN::min_scn();
    ls_desc.checkpoint_scn_ = SCN::min_scn();
    ls_desc.min_lsn_ = 0;
    ls_desc.max_lsn_ = 0;
    ls_desc.deleted_ = false;
  }

  // Helper function to add file to LS info desc
  void add_file_to_ls_desc(ObSingleLSInfoDesc &ls_desc,
                           int64_t file_id,
                           int64_t size_bytes = 1024) {
    ObSingleLSInfoDesc::OneFile file;
    file.file_id_ = file_id;
    file.size_bytes_ = size_bytes;
    ASSERT_EQ(OB_SUCCESS, ls_desc.filelist_.push_back(file));
    // Keep the archived lsn range consistent with the file list: [start of the first block, somewhere
    // inside the last block).
    if (1 == ls_desc.filelist_.count()) {
      ls_desc.min_lsn_ = block_start_lsn(file_id);
    }
    ls_desc.max_lsn_ = block_start_lsn(file_id) + static_cast<uint64_t>(palf::PALF_BLOCK_SIZE) / 2;
  }

  // Add one log stream entry to `piece` with an EXPLICIT archived lsn range, which is what the piece
  // info file really records(ObSingleLSInfoDesc::min_lsn_/max_lsn_). `file_ids` may be empty, which is
  // exactly how an IDLE log stream looks: it archived nothing into the piece, so it has no file at all
  // while min_lsn_ == max_lsn_ == the lsn it stands at(see record_piece_info and
  // ObLSArchiveTask::ArchiveDest::compensate_piece). The file id based helpers above always derive the
  // range from the file list, so they can not express that state.
  void add_ls_to_piece_with_range(ObPieceInfoDesc &piece,
                                  const ObLSID &ls_id,
                                  const uint64_t min_lsn,
                                  const uint64_t max_lsn,
                                  const std::vector<int64_t> &file_ids) {
    ObSingleLSInfoDesc ls_desc;
    create_single_ls_info_desc(ls_desc, piece.dest_id_, piece.round_id_, piece.piece_id_, ls_id);
    for (int64_t file_id : file_ids) {
      ObSingleLSInfoDesc::OneFile file;
      file.file_id_ = file_id;
      file.size_bytes_ = 1024;
      ASSERT_EQ(OB_SUCCESS, ls_desc.filelist_.push_back(file));
    }
    ls_desc.min_lsn_ = min_lsn;
    ls_desc.max_lsn_ = max_lsn;
    ASSERT_EQ(OB_SUCCESS, piece.filelist_.push_back(ls_desc));
  }

  // Build the candidate piece attr array which matches `pieces` one by one.
  void create_candidate_piece_attrs(const ObArray<ObPieceInfoDesc> &pieces,
                                    ObArray<ObTenantArchivePieceAttr> &candidate_piece_infos) {
    candidate_piece_infos.reset();
    for (int64_t i = 0; i < pieces.count(); ++i) {
      ObTenantArchivePieceAttr piece_attr;
      create_candidate_piece_attr(piece_attr, pieces.at(i).dest_id_, pieces.at(i).round_id_,
          pieces.at(i).piece_id_);
      ASSERT_EQ(OB_SUCCESS, candidate_piece_infos.push_back(piece_attr));
    }
  }

  // Create a piece and add it to pieces array
  void create_piece(ObArray<ObPieceInfoDesc> &pieces, const PieceInfo &piece_info) {
    ObPieceInfoDesc piece;
    create_piece_info_desc(piece, piece_info.dest_id, piece_info.round_id, piece_info.piece_id);

    for (const auto &ls_file : piece_info.ls_files) {
      ObSingleLSInfoDesc ls_desc;
      create_single_ls_info_desc(ls_desc, piece_info.dest_id, piece_info.round_id,
                                piece_info.piece_id, ls_file.ls_id);

      for (int64_t file_id : ls_file.file_ids) {
        add_file_to_ls_desc(ls_desc, file_id);
      }

      ASSERT_EQ(OB_SUCCESS, piece.filelist_.push_back(ls_desc));
    }

    ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece));
  }

  // Build a VALID candidate piece attr. A piece attr which is not is_valid() is rejected by
  // check_piece_log_needed_ with OB_INVALID_ARGUMENT, and such a piece is then conservatively treated
  // as "still needed", so the dependency walk would stop at the oldest piece no matter what the piece
  // info files say. Note that the default constructor leaves the fields uninitialized, reset() is a
  // must here.
  void create_candidate_piece_attr(ObTenantArchivePieceAttr &piece_attr,
                                   const int64_t dest_id,
                                   const int64_t round_id,
                                   const int64_t piece_id) {
    piece_attr.reset();
    piece_attr.key_.tenant_id_ = TEST_TENANT_ID;
    piece_attr.key_.dest_id_ = dest_id;
    piece_attr.key_.round_id_ = round_id;
    piece_attr.key_.piece_id_ = piece_id;
    piece_attr.incarnation_ = 1;
    piece_attr.dest_no_ = 0;
    piece_attr.file_count_ = 1;
    piece_attr.start_scn_ = SCN::base_scn();
    piece_attr.checkpoint_scn_ = SCN::base_scn();
    piece_attr.max_scn_ = SCN::base_scn();
    piece_attr.end_scn_ = SCN::base_scn();
    piece_attr.compatible_.version_ = ObArchiveCompatible::Compatible::COMPATIBLE_VERSION_1;
    piece_attr.input_bytes_ = 1024;
    piece_attr.output_bytes_ = 1024;
    piece_attr.status_.status_ = ObArchivePieceStatus::Status::FROZEN;
    piece_attr.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
    piece_attr.cp_file_id_ = 0;
    piece_attr.cp_file_offset_ = 0;
    ASSERT_EQ(OB_SUCCESS, piece_attr.path_.assign("file:///archive"));
  }

  // Set selector and run test
  int64_t run_dependency_test(const ObArray<ObPieceInfoDesc> &pieces,
                             const ObArray<ObTenantArchivePieceAttr> &candidate_piece_infos) {
    ObBackupDeleteSelector selector;
    // The selector MUST be really inited: get_min_depended_piece_idx_ and the functions it calls
    // check IS_NOT_INIT and fall back to "keep every candidate piece"(idx 0) when it fails, which
    // would make every expectation below trivially pass/fail.
    mock_job_attr_.reset();
    mock_job_attr_.job_id_ = 1001;
    mock_job_attr_.tenant_id_ = TEST_TENANT_ID;
    mock_job_attr_.incarnation_id_ = 1;
    EXPECT_EQ(OB_SUCCESS, selector.init(mock_sql_proxy_, *mock_schema_service_, mock_job_attr_,
                                        *mock_rpc_proxy_, *mock_delete_mgr_));

    MockBackupDataProvider *mock_data_provider = OB_NEW(MockBackupDataProvider, "BackupProvider");
    EXPECT_NE(nullptr, mock_data_provider);
    if (nullptr != selector.data_provider_) {
      OB_DELETE(IObBackupDataProvider, "BackupClean", selector.data_provider_);
      selector.data_provider_ = nullptr;
    }
    selector.data_provider_ = mock_data_provider;
    mock_data_provider->set_piece_info_descs(pieces);

    int64_t min_depended_pieces_idx = -1;
    int ret = selector.get_min_depended_piece_idx_(candidate_piece_infos, min_depended_pieces_idx);
    EXPECT_EQ(OB_SUCCESS, ret);

    return min_depended_pieces_idx;
  }

  // Convenient method: create test data from PieceInfo array and run test
  int64_t run_test_from_piece_infos(const std::vector<PieceInfo> &piece_infos) {
    ObArray<ObPieceInfoDesc> pieces;
    ObArray<ObTenantArchivePieceAttr> candidate_piece_infos;

    for (const auto &piece_info : piece_infos) {
      create_piece(pieces, piece_info);
    }

    for (int64_t i = 0; i < pieces.count(); ++i) {
      ObTenantArchivePieceAttr piece_attr;
      create_candidate_piece_attr(piece_attr, pieces.at(i).dest_id_, pieces.at(i).round_id_,
          pieces.at(i).piece_id_);
      candidate_piece_infos.push_back(piece_attr);
    }
    return run_dependency_test(pieces, candidate_piece_infos);
  }

  MockObMySQLProxy mock_sql_proxy_;
  std::unique_ptr<MockObMultiVersionSchemaService> mock_schema_service_;
  std::unique_ptr<MockObSrvRpcProxy> mock_rpc_proxy_;
  std::unique_ptr<MockObUserTenantBackupDeleteMgr> mock_delete_mgr_;
  ObBackupCleanJobAttr mock_job_attr_;
};

// Test case 1: Empty pieces array
TEST_F(TestBackupCleanPieceClogBlockDependency, test_empty_pieces) {
  ObArray<ObPieceInfoDesc> pieces;
  ObArray<ObTenantArchivePieceAttr> candidate_piece_infos;

  int64_t min_depended_pieces_idx = run_dependency_test(pieces, candidate_piece_infos);
  EXPECT_EQ(-1, min_depended_pieces_idx);
}

// Test case 2: Single piece, no dependency
TEST_F(TestBackupCleanPieceClogBlockDependency, test_single_piece_no_dependency) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {LSFileInfo(ObLSID(1001), {1})})
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  EXPECT_EQ(0, min_depended_pieces_idx);  // Only one piece, so index is 0
}

// Test case 3: Multiple pieces, LS with multiple files (crosses 64M boundary)
TEST_F(TestBackupCleanPieceClogBlockDependency, test_multiple_files_in_ls) {

  std::vector<PieceInfo> piece_infos = {
    // dest_id , round_id , piece_id , ls_files(ls_id, file_id)
    PieceInfo(1, 1, 1, {LSFileInfo(ObLSID(1001), {1, 2})}),  // Multiple files indicate crossing 64M boundary
    PieceInfo(1, 1, 2, {LSFileInfo(ObLSID(1001), {2})}),     // Single file
    PieceInfo(1, 1, 3, {LSFileInfo(ObLSID(1001), {2})}),     // Single file
    PieceInfo(1, 1, 4, {LSFileInfo(ObLSID(1001), {2, 3}), LSFileInfo(ObLSID(1002), {1})})  // Multiple LS
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  EXPECT_EQ(0, min_depended_pieces_idx);  // Should depend on piece1 (index 0) due to multiple files
}

// Test case 4: Dependency with same file_id across pieces
TEST_F(TestBackupCleanPieceClogBlockDependency, test_same_file_id_dependency) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {
      LSFileInfo(ObLSID(1001), {1, 2, 3}),
      LSFileInfo(ObLSID(1002), {1, 2, 3})
    }),
    PieceInfo(1, 1, 2, {
      LSFileInfo(ObLSID(1001), {3, 4}),
      LSFileInfo(ObLSID(1002), {3, 4})
    }),
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {4}),
      LSFileInfo(ObLSID(1002), {4})
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  EXPECT_EQ(1, min_depended_pieces_idx);
}

// Test case 5: No dependency chain, each piece starts a new file.
TEST_F(TestBackupCleanPieceClogBlockDependency, NoDependencyChain) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {
      LSFileInfo(ObLSID(1001), {1}),
      LSFileInfo(ObLSID(1002), {10})
    }),
    PieceInfo(1, 1, 2, {
      LSFileInfo(ObLSID(1001), {2}),
      LSFileInfo(ObLSID(1002), {11})
    }),
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {3}),
      LSFileInfo(ObLSID(1002), {12})
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  EXPECT_EQ(2, min_depended_pieces_idx); // Last piece depends only on itself
}

// Test case 6: A long dependency chain spanning all pieces.
TEST_F(TestBackupCleanPieceClogBlockDependency, LongDependencyChain) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {LSFileInfo(ObLSID(1001), {10})}),
    PieceInfo(1, 1, 2, {LSFileInfo(ObLSID(1001), {10})}),
    PieceInfo(1, 1, 3, {LSFileInfo(ObLSID(1001), {10})})
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  EXPECT_EQ(0, min_depended_pieces_idx); // Dependency chain goes back to the first piece
}

// Test case 7: Multiple log streams with different dependency lengths.
TEST_F(TestBackupCleanPieceClogBlockDependency, MultipleLSDependencies) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1002), {20, 21})  // Multi-file
    }),
    PieceInfo(1, 1, 2, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1002), {21})
    }),
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {11}),
      LSFileInfo(ObLSID(1002), {21})
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  // For ls1, dependency is on p3 itself (idx 2) because file_id changes at p2.
  // For ls2, dependency is on p1 (idx 0) because file_id 21 continues from p2, and p1 has multi-file.
  // The minimum of {2, 0} is 0.
  EXPECT_EQ(0, min_depended_pieces_idx);
}

// Test case 8: A dependency chain that stops midway.
TEST_F(TestBackupCleanPieceClogBlockDependency, DependencyEndingMidway) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {LSFileInfo(ObLSID(1001), {10})}),
    PieceInfo(1, 1, 2, {LSFileInfo(ObLSID(1001), {11})}),
    PieceInfo(1, 1, 3, {LSFileInfo(ObLSID(1001), {11})})
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  // p3 depends on p2 (same file_id 11). p2 does not depend on p1 (file_id changes from 10 to 11).
  // The chain starts at p2.
  EXPECT_EQ(1, min_depended_pieces_idx);
}

// Test case 9: An LS in the last piece does not exist in earlier pieces.
TEST_F(TestBackupCleanPieceClogBlockDependency, LSAppearsLater) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {LSFileInfo(ObLSID(1001), {10})}),
    PieceInfo(1, 1, 2, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1002), {20})
    }),
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1002), {20})
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  // For ls1, dependency goes back to p1 (idx 0).
  // For ls2, dependency goes back to p2 (idx 1), as it does not exist in p1.
  // The minimum of {0, 1} is 0.
  EXPECT_EQ(0, min_depended_pieces_idx);
}

// Test Case: A new log stream is added in an intermediate piece.
TEST_F(TestBackupCleanPieceClogBlockDependency, TestNewLogStreamAppearsMidway) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {LSFileInfo(ObLSID(1001), {10})}),
    PieceInfo(1, 1, 2, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1003), {30})
    }),
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1003), {30})
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  // Last piece is p3 (idx 2).
  // ls1001 in p3 depends on p2 and p1 (all have file_id 10). Min idx for ls1001 is 0.
  // ls1003 in p3 depends on p2. Search stops at p1 as ls1003 is missing. Min idx for ls1003 is 1.
  // The overall minimum is MIN(0, 1) = 0.
  EXPECT_EQ(0, min_depended_pieces_idx);
}

// Test Case: A log stream has a gap in its backup history.
TEST_F(TestBackupCleanPieceClogBlockDependency, TestLogStreamWithGap) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {
      LSFileInfo(ObLSID(1001), {10}),
      LSFileInfo(ObLSID(1002), {20})
    }),
    PieceInfo(1, 1, 2, {LSFileInfo(ObLSID(1001), {10})}),  // ls1002 has a GAP
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {11}),  // New file_id
      LSFileInfo(ObLSID(1002), {21})   // New file_id
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  // Last piece is p3 (idx 2).
  // ls1001 in p3 depends on p2 and p1. Min idx for ls1001 is 0.
  // ls1002 in p3 looks for dependency in p2, but it's missing. The search for ls1002 stops. Min idx for ls1002 is 2.
  // The overall minimum is MIN(0, 2) = 0.
  EXPECT_EQ(2, min_depended_pieces_idx);
}

// Test Case: Complex scenario with multiple streams, gaps, and new streams.
TEST_F(TestBackupCleanPieceClogBlockDependency, TestComplexScenarioWithMultipleStreamsAndGaps) {
  std::vector<PieceInfo> piece_infos = {
    PieceInfo(1, 1, 1, {
      LSFileInfo(ObLSID(1001), {10}),  // lsA
      LSFileInfo(ObLSID(1002), {20, 21}),  // lsB multi-file
      LSFileInfo(ObLSID(1004), {40})   // lsD
    }),
    PieceInfo(1, 1, 2, {
      LSFileInfo(ObLSID(1001), {10}),  // lsA continues
      LSFileInfo(ObLSID(1002), {21}),  // lsB continues
      LSFileInfo(ObLSID(1003), {30}),  // lsC is new
      LSFileInfo(ObLSID(1004), {40})   // lsD continues
    }),
    PieceInfo(1, 1, 3, {
      LSFileInfo(ObLSID(1001), {11}),  // lsA starts new file
      LSFileInfo(ObLSID(1002), {22}),  // lsB starts new file
      LSFileInfo(ObLSID(1003), {30})   // lsC continues
    })
  };

  int64_t min_depended_pieces_idx = run_test_from_piece_infos(piece_infos);
  EXPECT_EQ(1, min_depended_pieces_idx);
}

// Test Case: the log stream is IDLE in the anchor piece(it archived NOTHING into it, so its file list is
// empty and min_lsn_ == max_lsn_) while an OLDER piece holds the first half of the palf block it stands
// in. Being idle is not "no information": min_lsn_ is still the lsn the log stream stands at when the
// anchor begins, so reading the anchor still starts at the beginning of that block, and the piece holding
// the first half of it must be kept.
TEST_F(TestBackupCleanPieceClogBlockDependency, TestIdleLSInAnchorPieceDependsOnBlockPrefix) {
  const uint64_t block_11_start = block_start_lsn(11);
  const uint64_t mid_block_11 = block_11_start + static_cast<uint64_t>(palf::PALF_BLOCK_SIZE) / 2;
  const ObLSID ls_id(1001);
  ObArray<ObPieceInfoDesc> pieces;
  ObArray<ObTenantArchivePieceAttr> candidate_piece_infos;

  // piece 1(idx 0): the log stream archived the whole block 10 and nothing of block 11.
  ObPieceInfoDesc piece1;
  create_piece_info_desc(piece1, 1/*dest_id*/, 1/*round_id*/, 1/*piece_id*/);
  add_ls_to_piece_with_range(piece1, ls_id, block_start_lsn(10), block_11_start, {10});
  // piece 2(idx 1): the piece switch split block 11 in the middle, this piece holds its FIRST half, which
  // is not re-archived into any newer piece.
  ObPieceInfoDesc piece2;
  create_piece_info_desc(piece2, 1/*dest_id*/, 1/*round_id*/, 2/*piece_id*/);
  add_ls_to_piece_with_range(piece2, ls_id, block_11_start, mid_block_11, {11});
  // piece 3(idx 2): the anchor, i.e. the first not expired piece. The log stream is idle in it, so it
  // stands right where piece 2 left it, in the middle of block 11.
  ObPieceInfoDesc piece3;
  create_piece_info_desc(piece3, 1/*dest_id*/, 1/*round_id*/, 3/*piece_id*/);
  add_ls_to_piece_with_range(piece3, ls_id, mid_block_11, mid_block_11, {}/*no file at all*/);
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece1));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece2));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece3));
  create_candidate_piece_attrs(pieces, candidate_piece_infos);

  // Piece 2 has to be kept because it holds the first half of block 11, only piece 1 is reclaimable.
  EXPECT_EQ(1, run_dependency_test(pieces, candidate_piece_infos));
}

// Test Case: the log stream is idle in the anchor piece as well, but it stands exactly at a block
// boundary, so the block it is about to write has not been archived anywhere yet and no older piece has
// to be kept for it.
TEST_F(TestBackupCleanPieceClogBlockDependency, TestIdleLSAtBlockBoundaryInAnchorPiece) {
  const uint64_t block_11_start = block_start_lsn(11);
  const ObLSID ls_id(1001);
  ObArray<ObPieceInfoDesc> pieces;
  ObArray<ObTenantArchivePieceAttr> candidate_piece_infos;

  // piece 1(idx 0) and piece 2(idx 1): the log stream archived block 9 and block 10 respectively, both of
  // them end exactly at a block boundary.
  ObPieceInfoDesc piece1;
  create_piece_info_desc(piece1, 1/*dest_id*/, 1/*round_id*/, 1/*piece_id*/);
  add_ls_to_piece_with_range(piece1, ls_id, block_start_lsn(9), block_start_lsn(10), {9});
  ObPieceInfoDesc piece2;
  create_piece_info_desc(piece2, 1/*dest_id*/, 1/*round_id*/, 2/*piece_id*/);
  add_ls_to_piece_with_range(piece2, ls_id, block_start_lsn(10), block_11_start, {10});
  // piece 3(idx 2): the anchor. The log stream is idle in it and stands at the start of block 11.
  ObPieceInfoDesc piece3;
  create_piece_info_desc(piece3, 1/*dest_id*/, 1/*round_id*/, 3/*piece_id*/);
  add_ls_to_piece_with_range(piece3, ls_id, block_11_start, block_11_start, {}/*no file at all*/);
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece1));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece2));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(piece3));
  create_candidate_piece_attrs(pieces, candidate_piece_infos);

  // No older piece holds any log from block 11 on, so only the anchor itself is kept.
  EXPECT_EQ(2, run_dependency_test(pieces, candidate_piece_infos));
}

} // namespace backup
} // namespace oceanbase


int main(int argc, char **argv) {
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  system("rm -f test_backup_clean_piece_clog_block_dependency.log*");
  logger.set_file_name("test_backup_clean_piece_clog_block_dependency.log", true, true);
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}