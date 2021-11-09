// Copyright 2010-2021 OceanBase Inc. All Rights Reserved.
// Author:
//   yangyi.yyy@antfin.com
//

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_multi_backup_dest_util.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

void mock_backup_set(const int64_t backup_set_id, const int64_t prev_full_backup_set_id,
    const int64_t prev_inc_backup_set_id, const int64_t snapshot_version, const int64_t start_replay_log_ts,
    const share::ObBackupType::BackupType type, share::ObBackupSetFileInfo& set_info)
{
  set_info.reset();
  set_info.backup_set_id_ = backup_set_id;
  set_info.prev_full_backup_set_id_ = prev_full_backup_set_id;
  set_info.prev_inc_backup_set_id_ = prev_inc_backup_set_id;
  set_info.snapshot_version_ = snapshot_version;
  set_info.start_replay_log_ts_ = start_replay_log_ts;
  set_info.backup_type_.type_ = type;
}

void mock_backup_piece(const int64_t round_id, const int64_t piece_id, const int64_t start_ts,
    const int64_t checkpoint_ts, const int64_t max_ts, const share::ObBackupPieceStatus::STATUS& status,
    share::ObBackupPieceInfo& piece)
{
  piece.reset();
  piece.key_.round_id_ = round_id;         // check all same round
  piece.key_.backup_piece_id_ = piece_id;  // check piece is continuous
  piece.start_ts_ = start_ts;
  piece.checkpoint_ts_ = checkpoint_ts;
  piece.max_ts_ = max_ts;
  piece.status_ = status;     // check all but last is frozen
  piece.start_piece_id_ = 1;  // check start piece id
}

int mock_only_full_backup_set(const int64_t full_count, common::ObArray<share::ObBackupSetFileInfo>& set_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= full_count; ++i) {
    ObBackupSetFileInfo info;
    mock_backup_set(i, 0, 0, 100, 100, ObBackupType::FULL_BACKUP, info);
    if (OB_FAIL(set_list.push_back(info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_only_inc_backup_set_list(const int64_t inc_count, common::ObArray<share::ObBackupSetFileInfo>& set_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  for (int64_t i = 2; OB_SUCC(ret) && i < 2 + inc_count; ++i) {
    ObBackupSetFileInfo info;
    mock_backup_set(i, 1, 1, 100, 100, ObBackupType::INCREMENTAL_BACKUP, info);
    if (OB_FAIL(set_list.push_back(info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_normal_full_and_inc_backup_set_list(common::ObArray<share::ObBackupSetFileInfo>& set_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupSetFileInfo info;
    if (1 == i) {
      mock_backup_set(1, 0, 0, 100, 100, ObBackupType::FULL_BACKUP, info);
    } else if (2 == i) {
      mock_backup_set(2, 1, 1, 200, 200, ObBackupType::INCREMENTAL_BACKUP, info);
    } else if (3 == i) {
      mock_backup_set(3, 1, 2, 300, 300, ObBackupType::INCREMENTAL_BACKUP, info);
    }
    if (OB_FAIL(set_list.push_back(info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_duplicate_inc_backup_set_list(common::ObArray<share::ObBackupSetFileInfo>& set_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupSetFileInfo info;
    if (1 == i) {
      mock_backup_set(1, 0, 0, 100, 100, ObBackupType::FULL_BACKUP, info);
    } else if (2 == i) {
      mock_backup_set(2, 1, 1, 200, 200, ObBackupType::INCREMENTAL_BACKUP, info);
    } else if (3 == i) {
      mock_backup_set(2, 1, 1, 300, 300, ObBackupType::INCREMENTAL_BACKUP, info);
    }
    if (OB_FAIL(set_list.push_back(info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_full_and_inc_discontinuous_backup_set_list_with_failed(common::ObArray<share::ObBackupSetFileInfo>& set_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupSetFileInfo info;
    if (1 == i) {
      mock_backup_set(1, 0, 0, 100, 100, ObBackupType::FULL_BACKUP, info);
    } else if (2 == i) {
      mock_backup_set(2, 1, 1, 200, 200, ObBackupType::INCREMENTAL_BACKUP, info);
      info.result_ = OB_IO_ERROR;
    } else if (3 == i) {
      mock_backup_set(3, 1, 1, 300, 300, ObBackupType::INCREMENTAL_BACKUP, info);
    }
    if (OB_FAIL(set_list.push_back(info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_full_and_inc_discontinuous_backup_set_list(common::ObArray<share::ObBackupSetFileInfo>& set_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupSetFileInfo info;
    if (1 == i) {
      mock_backup_set(1, 0, 0, 100, 100, ObBackupType::FULL_BACKUP, info);
    } else if (2 == i) {
      continue;
    } else if (3 == i) {
      mock_backup_set(3, 1, 2, 300, 300, ObBackupType::INCREMENTAL_BACKUP, info);
    }
    if (OB_FAIL(set_list.push_back(info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_normal_round_piece_list(common::ObArray<share::ObBackupPieceInfo>& piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupPieceInfo piece_info;
    if (1 == i) {
      mock_backup_piece(1, i, 100, 200, 300, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    } else if (2 == i) {
      mock_backup_piece(1, i, 300, 400, 500, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    } else if (3 == i) {
      mock_backup_piece(1, i, 500, 700, 1000, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    }
    if (OB_FAIL(piece_list.push_back(piece_info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_discontinuous_piece_list(common::ObArray<share::ObBackupPieceInfo>& piece_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupPieceInfo piece_info;
    piece_info.start_piece_id_ = 1;
    if (1 == i) {
      mock_backup_piece(1, i, 100, 200, 300, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    } else if (2 == i) {
      continue;
    } else if (3 == i) {
      mock_backup_piece(1, i, 600, 1000, 1100, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    }
    if (OB_FAIL(piece_list.push_back(piece_info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_duplicate_piece_list(common::ObArray<share::ObBackupPieceInfo>& piece_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupPieceInfo piece_info;
    mock_backup_piece(1, 1, 100, 200, 300, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    if (OB_FAIL(piece_list.push_back(piece_info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_mid_freezing_piece_list(common::ObArray<share::ObBackupPieceInfo>& piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupPieceInfo piece_info;
    if (1 == i) {
      mock_backup_piece(1, 1, 100, 200, 300, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    } else if (2 == i) {
      mock_backup_piece(1, 2, 300, 500, 600, ObBackupPieceStatus::BACKUP_PIECE_FREEZING, piece_info);
    } else if (3 == i) {
      mock_backup_piece(1, 3, 600, 1000, 1100, ObBackupPieceStatus::BACKUP_PIECE_ACTIVE, piece_info);
    }
    if (OB_FAIL(piece_list.push_back(piece_info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

int mock_different_round_piece_list(common::ObArray<share::ObBackupPieceInfo>& piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= 3; ++i) {
    ObBackupPieceInfo piece_info;
    if (1 == i) {
      mock_backup_piece(1, 1, 100, 200, 300, ObBackupPieceStatus::BACKUP_PIECE_FROZEN, piece_info);
    } else if (2 == i) {
      mock_backup_piece(2, 2, 300, 500, 600, ObBackupPieceStatus::BACKUP_PIECE_FREEZING, piece_info);
    } else if (3 == i) {
      mock_backup_piece(3, 3, 600, 1000, 1100, ObBackupPieceStatus::BACKUP_PIECE_ACTIVE, piece_info);
    }
    if (OB_FAIL(piece_list.push_back(piece_info))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  }
  return ret;
}

// case1:
// F I I
//    1 2 3
TEST(ObMultiBackupDestUtil, case1)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 600;  // change restore timestamp
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_normal_full_and_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// I I
//    1 2 3
TEST(ObMultiBackupDestUtil, case2)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 1000;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_only_inc_backup_set_list(2, set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 3: missing one incremental backup set
// F X I
//    1 2 3
TEST(ObMultiBackupDestUtil, case3)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 1000;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_full_and_inc_discontinuous_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 4: one incremntal backup set is failed
// F X I
//    1 2 3
TEST(ObMultiBackupDestUtil, case4)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 1000;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_full_and_inc_discontinuous_backup_set_list_with_failed(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 5: piece id not continuous
// F
//   1 3
TEST(ObMultiBackupDestUtil, case5)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 1000;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_only_full_backup_set(1, set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_discontinuous_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 6: duplicate inc backup set id
TEST(ObMultiBackupDestUtil, case6)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 700;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_duplicate_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 7: duplicate backup piece id
TEST(ObMultiBackupDestUtil, case7)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 700;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_normal_full_and_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_duplicate_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 8: has freezing backup piece
TEST(ObMultiBackupDestUtil, case8)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 700;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_normal_full_and_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_mid_freezing_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 9: piece from different round
TEST(ObMultiBackupDestUtil, case9)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 700;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_normal_full_and_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_different_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 10: restore timestamp too small
TEST(ObMultiBackupDestUtil, case10)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 299;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_normal_full_and_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// case 11: restore timestamp too large
TEST(ObMultiBackupDestUtil, case11)
{
  int ret = OB_SUCCESS;
  const int64_t restore_timestamp = 1000;
  bool is_complete = false;
  ObArray<ObBackupSetFileInfo> set_list;
  ObArray<ObBackupPieceInfo> piece_list;

  ret = mock_normal_full_and_inc_backup_set_list(set_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_normal_round_piece_list(piece_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObMultiBackupDestUtil::check_multi_path_is_complete(restore_timestamp, set_list, piece_list, is_complete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
