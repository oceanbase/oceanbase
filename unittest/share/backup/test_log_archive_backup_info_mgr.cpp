// Copyright 2010-2021 OceanBase Inc. All Rights Reserved.
// Author:
//   yongle.xh@antfin.com
//

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
using namespace oceanbase;
using namespace common;
using namespace share;

void init_piece(const int64_t round_id, const int64_t piece_id, const int64_t copy_id, ObBackupPieceInfo& piece)
{
  piece.reset();
  piece.key_.incarnation_ = OB_START_INCARNATION;
  piece.key_.tenant_id_ = 1;
  piece.key_.round_id_ = round_id;
  piece.key_.backup_piece_id_ = piece_id;
  piece.key_.copy_id_ = copy_id;
  piece.create_date_ = 1;
  piece.start_ts_ = 1;
  piece.checkpoint_ts_ = 2;
  piece.max_ts_ = 3;
  piece.status_ = ObBackupPieceStatus::BACKUP_PIECE_FROZEN;
  piece.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
  piece.backup_dest_.assign("file:///backup");
  piece.compatible_ = ObTenantLogArchiveStatus::COMPATIBLE_VERSION_2;
  piece.start_piece_id_ = 1;
}

int check_piece(ObArray<share::ObBackupPieceInfo>& result, ObArray<share::ObBackupPieceInfo>& expect)
{
  int ret = OB_SUCCESS;
  if (result.count() != expect.count()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("count not match", "result_count", result.count(), "expect_count", expect.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < result.count(); ++i) {
      if (result.at(i) != expect.at(i)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("piece not match", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < result.count(); ++i) {
      LOG_INFO("dump result piece", K(ret), K(i), "result", result.at(i));
    }
    for (int64_t i = 0; i < expect.count(); ++i) {
      LOG_INFO("dump expect piece", K(ret), K(i), "expect", expect.at(i));
    }
  } else {
    for (int64_t i = 0; i < result.count(); ++i) {
      LOG_INFO("dump result piece", K(ret), K(i), "result", result.at(i));
    }
  }
  return ret;
}

TEST(ObExternalBackupPieceInfo, update)
{
  ObExternalBackupPieceInfo external_info;
  share::ObBackupPieceInfo piece1_1;
  share::ObBackupPieceInfo piece1_2;
  share::ObBackupPieceInfo piece2_1;
  ObArray<share::ObBackupPieceInfo> expect_array;
  ObArray<share::ObBackupPieceInfo> result_array;

  // normal seq
  init_piece(1, 1, 0, piece1_1);
  init_piece(1, 2, 0, piece1_2);
  init_piece(2, 1, 0, piece2_1);
  ASSERT_EQ(OB_SUCCESS, external_info.update(piece1_1));
  ASSERT_EQ(OB_SUCCESS, external_info.update(piece1_2));
  ASSERT_EQ(OB_SUCCESS, external_info.update(piece2_1));
  ASSERT_EQ(OB_SUCCESS, external_info.get_piece_array(result_array));
  expect_array.reset();
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_1));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_2));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece2_1));
  ASSERT_EQ(OB_SUCCESS, check_piece(result_array, expect_array));

  // update same copy and piece
  init_piece(1, 2, 0, piece1_2);
  piece1_2.max_ts_ = 100;
  ASSERT_EQ(OB_SUCCESS, external_info.update(piece1_2));
  ASSERT_EQ(OB_SUCCESS, external_info.get_piece_array(result_array));
  expect_array.reset();
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_1));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_2));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece2_1));
  ASSERT_EQ(OB_SUCCESS, check_piece(result_array, expect_array));

  // insert 1_3
  share::ObBackupPieceInfo piece1_3;
  init_piece(1, 3, 0, piece1_3);
  ASSERT_EQ(OB_SUCCESS, external_info.update(piece1_3));
  ASSERT_EQ(OB_SUCCESS, external_info.get_piece_array(result_array));
  expect_array.reset();
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_1));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_2));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_3));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece2_1));
  ASSERT_EQ(OB_SUCCESS, check_piece(result_array, expect_array));

  // insert same piece with diff copy
  share::ObBackupPieceInfo piece1_2_1;
  init_piece(1, 2, 1, piece1_2_1);
  ASSERT_EQ(OB_SUCCESS, external_info.update(piece1_2_1));
  ASSERT_EQ(OB_SUCCESS, external_info.get_piece_array(result_array));
  expect_array.reset();
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_1));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_2));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_2_1));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece1_3));
  ASSERT_EQ(OB_SUCCESS, expect_array.push_back(piece2_1));
  ASSERT_EQ(OB_SUCCESS, check_piece(result_array, expect_array));
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
