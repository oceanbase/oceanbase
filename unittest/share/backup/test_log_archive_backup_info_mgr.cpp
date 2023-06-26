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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/backup/ob_backup_path.h"
using namespace oceanbase;
using namespace common;
using namespace share;

void init_piece(const int64_t round_id, const int64_t piece_id, const int64_t copy_id, ObBackupPieceInfo &piece)
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

int check_piece(ObArray<share::ObBackupPieceInfo> &result, ObArray<share::ObBackupPieceInfo> &expect)
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

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
