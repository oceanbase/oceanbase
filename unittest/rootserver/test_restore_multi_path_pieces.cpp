/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gmock/gmock.h>
#include "lib/utility/ob_sort.h"

#define private public
#include "rootserver/restore/ob_restore_util.h"
#undef private

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

// Each piece i covers [i * PIECE_INTERVAL, (i + 1) * PIECE_INTERVAL).
class TestRestoreMultiPathPieces : public ::testing::Test
{
public:
  static constexpr uint64_t TENANT_ID = 1002;
  static constexpr int64_t DEST_ID = 1;
  static constexpr int64_t ROUND_ID = 1;
  static constexpr int64_t PIECE_INTERVAL = 100;
  static constexpr int64_t MAX_PIECE_ID = 10;

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, multi_path_map_.create(OB_MAX_BACKUP_PIECE_NUM, "TestMultiPath"));
    time_zone_wrap_.set_tz_info_offset(0);
    path_cnt_ = 0;
  }

  void TearDown() override
  {
    multi_path_map_.destroy();
  }

  static SCN make_scn(const int64_t val)
  {
    SCN scn;
    scn.convert_for_inner_table_field(static_cast<uint64_t>(val));
    return scn;
  }

  static ObTenantArchivePieceAttr make_frozen_piece(
      const int64_t piece_id,
      const int64_t dest_id = DEST_ID,
      const int64_t round_id = ROUND_ID)
  {
    ObTenantArchivePieceAttr piece;
    piece.key_.tenant_id_ = TENANT_ID;
    piece.key_.dest_id_ = dest_id;
    piece.key_.round_id_ = round_id;
    piece.key_.piece_id_ = piece_id;
    piece.start_scn_ = make_scn(piece_id * PIECE_INTERVAL);
    piece.end_scn_ = make_scn((piece_id + 1) * PIECE_INTERVAL);
    piece.checkpoint_scn_ = piece.end_scn_;
    piece.max_scn_ = piece.end_scn_;
    piece.status_.set_frozen();
    piece.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE;
    return piece;
  }

  static ObTenantArchivePieceAttr make_active_piece(const int64_t piece_id, const int64_t checkpoint)
  {
    ObTenantArchivePieceAttr piece = make_frozen_piece(piece_id);
    piece.checkpoint_scn_ = make_scn(checkpoint);
    piece.max_scn_ = piece.checkpoint_scn_;
    piece.status_.set_active();
    return piece;
  }

  // A normal frozen piece, whose checkpoint_scn may be smaller than end_scn, as logs are archived
  // by the max scn of log group.
  static ObTenantArchivePieceAttr make_frozen_piece_with_checkpoint(
      const int64_t piece_id,
      const int64_t checkpoint,
      const int64_t dest_id = DEST_ID,
      const int64_t round_id = ROUND_ID)
  {
    ObTenantArchivePieceAttr piece = make_frozen_piece(piece_id, dest_id, round_id);
    piece.checkpoint_scn_ = make_scn(checkpoint);
    piece.max_scn_ = piece.checkpoint_scn_;
    return piece;
  }

  // Register piece as if it was found in 'root'.
  void add_piece(const ObTenantArchivePieceAttr &piece, const char *root)
  {
    ASSERT_TRUE(path_cnt_ < MAX_PATHS);
    char *path = paths_[path_cnt_++];
    snprintf(path, OB_MAX_BACKUP_PATH_LENGTH, "file:///%s/d%ld_r%ld_p%ld",
        root, piece.key_.dest_id_, piece.key_.round_id_, piece.key_.piece_id_);
    ASSERT_EQ(OB_SUCCESS, ObRestoreUtil::add_piece_attr_(piece, ObString(path), piece_attrs_, multi_path_map_));
  }

  static void check_piece_root(const ObArray<ObRestoreLogPieceBriefInfo> &pieces,
      const int64_t piece_id, const char *expected_root)
  {
    bool found = false;
    for (int64_t i = 0; i < pieces.count(); ++i) {
      if (pieces.at(i).piece_id_ == piece_id) {
        found = true;
        ASSERT_EQ(0, STRNCMP(pieces.at(i).piece_path_.ptr(), expected_root, strlen(expected_root)));
      }
    }
    ASSERT_TRUE(found);
  }

  // The scenario:
  //   piece1~3 were copied to nfs1 and then deleted from dest, piece4~10 are in dest,
  //   piece10 is active.
  void build_scenario(const int64_t skipped_piece_id = -1)
  {
    for (int64_t i = 1; i <= MAX_PIECE_ID; ++i) {
      if (i == skipped_piece_id) {
      } else if (i < MAX_PIECE_ID) {
        add_piece(make_frozen_piece(i), i <= 3 ? "nfs1" : "dest");
      } else {
        add_piece(make_active_piece(i, 1050), "dest");
      }
    }
  }

  int get_pieces(const int64_t start, const int64_t end, ObArray<ObRestoreLogPieceBriefInfo> &pieces)
  {
    int ret = OB_SUCCESS;
    lib::ob_sort(piece_attrs_.begin(), piece_attrs_.end(), ObTenantArchivePieceAttrComparator());
    if (OB_FAIL(ObRestoreUtil::get_piece_paths_from_round_groups_(piece_attrs_,
        multi_path_map_, make_scn(start), make_scn(end), time_zone_wrap_, pieces))) {
      LOG_WARN("fail to get piece paths", K(ret));
    }
    return ret;
  }

  // Check pieces of <dest_id, round_id> only, and return max_restorable_scn of the round.
  int get_round_max_restorable_scn(const int64_t dest_id, const int64_t round_id,
      const int64_t start, const int64_t end, SCN &max_restorable_scn)
  {
    int ret = OB_SUCCESS;
    ObArray<ObTenantArchivePieceAttr> round_pieces;
    ObArray<ObRestoreLogPieceBriefInfo> pieces;
    lib::ob_sort(piece_attrs_.begin(), piece_attrs_.end(), ObTenantArchivePieceAttrComparator());
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_attrs_.count(); ++i) {
      if (piece_attrs_.at(i).key_.dest_id_ == dest_id && piece_attrs_.at(i).key_.round_id_ == round_id) {
        ret = round_pieces.push_back(piece_attrs_.at(i));
      }
    }
    if (OB_SUCC(ret)) {
      ret = ObRestoreUtil::get_piece_paths_in_range_from_multi_path_(round_pieces, multi_path_map_,
          make_scn(start), make_scn(end), pieces, max_restorable_scn);
    }
    return ret;
  }

  static void check_piece_ids(const ObArray<ObRestoreLogPieceBriefInfo> &pieces,
      const int64_t first_piece_id, const int64_t last_piece_id)
  {
    ASSERT_EQ(last_piece_id - first_piece_id + 1, pieces.count());
    for (int64_t i = 0; i < pieces.count(); ++i) {
      ASSERT_EQ(first_piece_id + i, pieces.at(i).piece_id_);
    }
  }

  static constexpr int64_t MAX_PATHS = 64;
  char paths_[MAX_PATHS][OB_MAX_BACKUP_PATH_LENGTH];
  int64_t path_cnt_;
  common::hash::ObHashMap<ObPieceKey, ObString> multi_path_map_;
  ObArray<ObTenantArchivePieceAttr> piece_attrs_;
  ObTimeZoneInfoWrap time_zone_wrap_;
};

TEST_F(TestRestoreMultiPathPieces, piece_attr_comparator)
{
  ObArray<ObTenantArchivePieceAttr> pieces;
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(make_frozen_piece(5, 2, 1)));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(make_frozen_piece(3, 1, 2)));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(make_frozen_piece(2, 1, 2)));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(make_frozen_piece(9, 1, 1)));
  ASSERT_EQ(OB_SUCCESS, pieces.push_back(make_frozen_piece(1, 1, 1)));
  lib::ob_sort(pieces.begin(), pieces.end(), ObTenantArchivePieceAttrComparator());
  const int64_t expected[][3] = {{1, 1, 1}, {1, 1, 9}, {1, 2, 2}, {1, 2, 3}, {2, 1, 5}};
  for (int64_t i = 0; i < pieces.count(); ++i) {
    ASSERT_EQ(expected[i][0], pieces.at(i).key_.dest_id_);
    ASSERT_EQ(expected[i][1], pieces.at(i).key_.round_id_);
    ASSERT_EQ(expected[i][2], pieces.at(i).key_.piece_id_);
  }
}

// Pieces deleted from dest but provided by other path can be used to restore.
TEST_F(TestRestoreMultiPathPieces, use_pieces_deleted_from_dest)
{
  build_scenario();
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_SUCCESS, get_pieces(250, 1020, pieces));
  check_piece_ids(pieces, 2, 10);
  for (int64_t i = 0; i < pieces.count(); ++i) {
    const char *expected_root = pieces.at(i).piece_id_ <= 3 ? "file:///nfs1/" : "file:///dest/";
    ASSERT_EQ(0, STRNCMP(pieces.at(i).piece_path_.ptr(), expected_root, strlen(expected_root)));
  }
}

// A missing piece in the restore range breaks continuity.
TEST_F(TestRestoreMultiPathPieces, missing_piece_needed)
{
  build_scenario(5 /*skipped_piece_id*/);
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
  ASSERT_TRUE(pieces.empty());

  // logs are continuous up to piece4.
  SCN max_restorable_scn;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH,
      get_round_max_restorable_scn(DEST_ID, ROUND_ID, 250, 1020, max_restorable_scn));
  ASSERT_EQ(make_scn(500), max_restorable_scn);
}

// The piece which contains restore_start_scn is missing.
TEST_F(TestRestoreMultiPathPieces, missing_start_piece)
{
  build_scenario(2 /*skipped_piece_id*/);
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
  ASSERT_TRUE(pieces.empty());
}

// A missing piece out of the restore range does not matter.
TEST_F(TestRestoreMultiPathPieces, missing_piece_not_needed)
{
  build_scenario(1 /*skipped_piece_id*/);
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_SUCCESS, get_pieces(250, 1020, pieces));
  check_piece_ids(pieces, 2, 10);
}

// Restore end scn is beyond the checkpoint scn of the latest piece.
TEST_F(TestRestoreMultiPathPieces, restore_end_scn_too_large)
{
  build_scenario();
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1060, pieces));
  ASSERT_TRUE(pieces.empty());
}

// Logs between checkpoint_scn and end_scn of a non-frozen piece may be missing,
// it must not be treated as continuous with the next piece.
TEST_F(TestRestoreMultiPathPieces, non_frozen_piece_not_continuous)
{
  build_scenario(3 /*skipped_piece_id*/);
  add_piece(make_active_piece(3, 350), "nfs1");
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
  ASSERT_TRUE(pieces.empty());

  // logs are continuous up to the checkpoint of active piece3.
  SCN max_restorable_scn;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH,
      get_round_max_restorable_scn(DEST_ID, ROUND_ID, 250, 1020, max_restorable_scn));
  ASSERT_EQ(make_scn(350), max_restorable_scn);

  // restore from piece4 is not affected.
  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(450, 1020, pieces));
  check_piece_ids(pieces, 4, 10);
}

// The last piece of a stopped round is frozen with checkpoint_scn < end_scn. Even if its end_scn
// equals to start_scn of the first piece of the next round, they are not continuous.
TEST_F(TestRestoreMultiPathPieces, pieces_of_different_rounds_not_continuous)
{
  const int64_t NEXT_ROUND_ID = ROUND_ID + 1;
  for (int64_t i = 1; i <= 3; ++i) {
    ObTenantArchivePieceAttr piece = make_frozen_piece(i);
    if (3 == i) {
      piece.checkpoint_scn_ = make_scn(350);
    }
    add_piece(piece, "nfs1");
  }
  for (int64_t i = 4; i <= MAX_PIECE_ID; ++i) {
    add_piece(make_frozen_piece(i, DEST_ID, NEXT_ROUND_ID), "dest");
  }
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
  ASSERT_TRUE(pieces.empty());

  // logs between 350 and 400 are missing.
  pieces.reset();
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(360, 1020, pieces));

  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(450, 1020, pieces));
  check_piece_ids(pieces, 4, 10);
}

// The round which covers restore_start_scn is selected, no matter it is the latest or not.
TEST_F(TestRestoreMultiPathPieces, select_round_by_restore_range)
{
  const int64_t NEXT_ROUND_ID = ROUND_ID + 1;
  for (int64_t i = 1; i <= 3; ++i) {
    add_piece(make_frozen_piece(i), "nfs1");
  }
  for (int64_t i = 6; i <= MAX_PIECE_ID; ++i) {
    add_piece(make_frozen_piece(i, DEST_ID, NEXT_ROUND_ID), "dest");
  }
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_SUCCESS, get_pieces(150, 350, pieces));
  check_piece_ids(pieces, 1, 3);

  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(650, 1020, pieces));
  check_piece_ids(pieces, 6, 10);

  // no round covers restore_start_scn.
  pieces.reset();
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(450, 1020, pieces));

  // restore range crosses rounds.
  pieces.reset();
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
}

// Pieces of other dest are not continuous with pieces of current dest.
TEST_F(TestRestoreMultiPathPieces, pieces_of_different_dests_not_continuous)
{
  for (int64_t i = 1; i <= 3; ++i) {
    add_piece(make_frozen_piece(i, DEST_ID + 1), "other");
  }
  for (int64_t i = 4; i <= MAX_PIECE_ID; ++i) {
    add_piece(make_frozen_piece(i), "dest");
  }
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(450, 1020, pieces));
  check_piece_ids(pieces, 4, 10);
}

// Logs between checkpoint_scn and end_scn of a normal frozen piece are archived into the next piece,
// restore_start_scn in [checkpoint_scn, end_scn) can be supported if the next piece exists.
TEST_F(TestRestoreMultiPathPieces, frozen_piece_checkpoint_less_than_end)
{
  for (int64_t i = 1; i < MAX_PIECE_ID; ++i) {
    add_piece(make_frozen_piece_with_checkpoint(i, (i + 1) * PIECE_INTERVAL - 30), i <= 3 ? "nfs1" : "dest");
  }
  add_piece(make_active_piece(MAX_PIECE_ID, 1050), "dest");
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  // checkpoint_scn < restore_start_scn < end_scn
  ASSERT_EQ(OB_SUCCESS, get_pieces(280, 1020, pieces));
  check_piece_ids(pieces, 2, 10);

  // restore_start_scn == checkpoint_scn
  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(270, 1020, pieces));
  check_piece_ids(pieces, 2, 10);

  // restore_end_scn is in [checkpoint_scn, end_scn) of piece2, piece3 is required.
  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(180, 290, pieces));
  check_piece_ids(pieces, 1, 3);

  // the next piece is missing.
  pieces.reset();
  piece_attrs_.reset();
  multi_path_map_.reuse();
  add_piece(make_frozen_piece_with_checkpoint(1, 170), "nfs1");
  add_piece(make_frozen_piece_with_checkpoint(2, 270), "nfs1");
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(280, 290, pieces));
  ASSERT_TRUE(pieces.empty());
}

// The stopped round of a larger dest id also covers restore_start_scn by [start_scn, end_scn),
// but logs are not enough. The round which can support restore should be selected.
TEST_F(TestRestoreMultiPathPieces, select_round_which_supports_restore)
{
  const int64_t OTHER_DEST_ID = DEST_ID + 1;
  for (int64_t i = 1; i <= 3; ++i) {
    ObTenantArchivePieceAttr piece = make_frozen_piece(i, OTHER_DEST_ID, ROUND_ID);
    if (3 == i) {
      piece.checkpoint_scn_ = make_scn(350);
    }
    add_piece(piece, "other");
  }
  // the next round starts at 355.
  for (int64_t i = 3; i <= MAX_PIECE_ID; ++i) {
    ObTenantArchivePieceAttr piece = make_frozen_piece(i, DEST_ID, ROUND_ID + 1);
    if (3 == i) {
      piece.start_scn_ = make_scn(355);
    }
    add_piece(piece, "dest");
  }
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_SUCCESS, get_pieces(360, 1020, pieces));
  check_piece_ids(pieces, 3, 10);
  check_piece_root(pieces, 3, "file:///dest/");

  pieces.reset();
  ASSERT_EQ(OB_SUCCESS, get_pieces(150, 320, pieces));
  check_piece_ids(pieces, 1, 3);
  check_piece_root(pieces, 3, "file:///other/");

  // logs between 350 and 355 are missing.
  pieces.reset();
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(250, 1020, pieces));
  ASSERT_TRUE(pieces.empty());
}

// If no round can support restore, max restorable scn is the max one among rounds which can restore
// from restore_start_scn.
TEST_F(TestRestoreMultiPathPieces, max_restorable_scn_of_rounds)
{
  const int64_t OTHER_DEST_ID = DEST_ID + 1;
  for (int64_t i = 1; i <= 3; ++i) {
    ObTenantArchivePieceAttr piece = make_frozen_piece(i, OTHER_DEST_ID, ROUND_ID);
    if (3 == i) {
      piece.checkpoint_scn_ = make_scn(350);
    }
    add_piece(piece, "other");
  }
  for (int64_t i = 3; i <= MAX_PIECE_ID; ++i) {
    ObTenantArchivePieceAttr piece = make_frozen_piece(i, DEST_ID, ROUND_ID + 1);
    if (3 == i) {
      piece.start_scn_ = make_scn(355);
    }
    add_piece(piece, "dest");
  }
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH, get_pieces(360, 1120, pieces));
  ASSERT_TRUE(pieces.empty());

  // Both rounds cover restore_start_scn by [start_scn, end_scn), but neither can support restore.
  SCN new_round_scn;
  SCN stopped_round_scn;
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH,
      get_round_max_restorable_scn(DEST_ID, ROUND_ID + 1, 360, 1120, new_round_scn));
  ASSERT_EQ(make_scn(1100), new_round_scn);
  ASSERT_EQ(OB_RESTORE_SOURCE_NOT_ENOUGH,
      get_round_max_restorable_scn(OTHER_DEST_ID, ROUND_ID, 360, 1120, stopped_round_scn));
  ASSERT_EQ(make_scn(350), stopped_round_scn);

  // The result does not depend on the order of rounds.
  const SCN restore_start_scn = make_scn(360);
  SCN max_restorable_scn;
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, new_round_scn, max_restorable_scn);
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, stopped_round_scn, max_restorable_scn);
  ASSERT_EQ(make_scn(1100), max_restorable_scn);

  max_restorable_scn.reset();
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, stopped_round_scn, max_restorable_scn);
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, new_round_scn, max_restorable_scn);
  ASSERT_EQ(make_scn(1100), max_restorable_scn);

  // Only the stopped round covers restore_start_scn, but its checkpoint_scn is smaller than
  // restore_start_scn, no max restorable scn.
  max_restorable_scn.reset();
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, stopped_round_scn, max_restorable_scn);
  ASSERT_FALSE(max_restorable_scn.is_valid());

  // A round without max restorable scn does not change the result.
  SCN invalid_scn;
  max_restorable_scn = make_scn(1100);
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, invalid_scn, max_restorable_scn);
  ASSERT_EQ(make_scn(1100), max_restorable_scn);

  // checkpoint_scn equals to restore_start_scn is acceptable.
  max_restorable_scn.reset();
  ObRestoreUtil::update_max_restorable_scn_(restore_start_scn, restore_start_scn, max_restorable_scn);
  ASSERT_EQ(restore_start_scn, max_restorable_scn);
}

// Frozen pieces may be copied to multiple paths, duplicate pieces are skipped and the first path is used.
TEST_F(TestRestoreMultiPathPieces, duplicate_pieces_in_multi_path)
{
  build_scenario();
  for (int64_t i = 3; i <= 5; ++i) {
    add_piece(make_frozen_piece(i), "nfs2");
  }
  ASSERT_EQ(MAX_PIECE_ID, piece_attrs_.count());
  ObArray<ObRestoreLogPieceBriefInfo> pieces;
  ASSERT_EQ(OB_SUCCESS, get_pieces(250, 1020, pieces));
  check_piece_ids(pieces, 2, 10);
  check_piece_root(pieces, 3, "file:///nfs1/");
  check_piece_root(pieces, 4, "file:///dest/");
  check_piece_root(pieces, 5, "file:///dest/");
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_restore_multi_path_pieces.log*");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_restore_multi_path_pieces.log", true);
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
