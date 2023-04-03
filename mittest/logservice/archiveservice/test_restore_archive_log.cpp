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

#include "lib/ob_errno.h"
#include "logservice/palf/log_define.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"
#include <cstdint>
#include <gtest/gtest.h>
#define private public
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#undef private
#include "fake_archive_piece_mgr.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace palf;
namespace unittest
{
TEST(FakeArchivePieceContext, get_piece)
{
  int ret = OB_SUCCESS;
  const int64_t ONE_SECOND = 1000 * 1000 * 1000L;
  const int64_t base_ts_1 = 1000 * ONE_SECOND;
  share::SCN base_scn_1;
  base_scn_1.convert_for_logservice(base_ts_1);
  const int64_t piece_switch_interval = 10 * ONE_SECOND;
  const int64_t ONE_MB = 1024 * 1024L;
  FakeRounds rounds;
  int64_t file_id = 1;
  FakeArchiveComponent round1;
  {
    round1.state_ = FakeRoundState::STOP;
    round1.round_id_ = 2;
    round1.start_scn_.convert_for_logservice(1005 * ONE_SECOND);
    round1.end_scn_.convert_for_logservice(1051 * ONE_SECOND);
    round1.base_piece_id_ = 1;
    round1.piece_switch_interval_ = piece_switch_interval;
    round1.base_piece_scn_ = base_scn_1;
    round1.min_piece_id_ = 1;
    round1.max_piece_id_ = 6;
    for (int64_t i = round1.min_piece_id_; OB_SUCC(ret) && i <= round1.max_piece_id_; i++) {
      FakePieceComponent piece;
      {
        if (i == 3) {
          piece.state_ = FakePieceState::EMPTY;
          piece.piece_id_ = i;
          piece.min_lsn_ = LSN((file_id - 1) *  64 * ONE_MB + 10240);
          piece.max_lsn_ = LSN((file_id - 1) *  64 * ONE_MB + 10240);
        } else {
          piece.state_ = FakePieceState::FRONZEN;
          piece.piece_id_ = i;
          piece.min_file_id_ = file_id;
          file_id = file_id + 10;
          piece.max_file_id_ = file_id;
          piece.min_lsn_ = LSN((piece.min_file_id_ - 1) * 64 * ONE_MB + 10240); // p1(1, 11), p2(11, 21), p3(EMPTY), p4(21, 31), p5(31, 41), p6(41, 51)
          piece.max_lsn_ = LSN((piece.max_file_id_ - 1) * 64 * ONE_MB + 10240);
        }
      }
      ret = round1.array_.push_back(piece);
      EXPECT_EQ(OB_SUCCESS, ret);
      CLOG_LOG(INFO, "COME HERE print", K(piece), K(round1.round_id_));
    }
  }
  ret = rounds.array_.push_back(round1);
  EXPECT_EQ(OB_SUCCESS, ret);

  FakeArchiveComponent round2;
  {
    round2.state_ = FakeRoundState::STOP;
    round2.round_id_ = 4;
    round2.start_scn_.convert_for_logservice(1051 * ONE_SECOND);
    round2.end_scn_.convert_for_logservice(1095 * ONE_SECOND);
    round2.base_piece_id_ = 2;
    round2.piece_switch_interval_ = piece_switch_interval;
    round2.base_piece_scn_ = base_scn_1;
    round2.min_piece_id_ = 7;
    round2.max_piece_id_ = 11;
    for (int64_t i = round2.min_piece_id_ ; OB_SUCC(ret) && i <= round2.max_piece_id_; i++) {
      FakePieceComponent piece;
      {
        piece.state_ = FakePieceState::FRONZEN;
        piece.piece_id_ = i;
        piece.min_file_id_ = file_id;
        file_id = file_id + 10;
        piece.max_file_id_ = file_id;
        piece.min_lsn_ = LSN((piece.min_file_id_ - 1) * 64 * ONE_MB + 10240); // p7(51, 61), p8(61, 71), p9(71, 81), p9(81, 91), p10(91, 101), p11(101, 111)
        piece.max_lsn_ = LSN((piece.max_file_id_ - 1) * 64 * ONE_MB + 10240);
      }
      ret = round2.array_.push_back(piece);
      EXPECT_EQ(OB_SUCCESS, ret);
      CLOG_LOG(INFO, "COME HERE print", K(piece), K(round2.round_id_));
    }
  }
  ret = rounds.array_.push_back(round2);
  EXPECT_EQ(OB_SUCCESS, ret);


  FakeArchiveComponent round3;
  {
    round3.state_ = FakeRoundState::STOP;
    round3.round_id_ = 5;
    round3.start_scn_.convert_for_logservice(1200 * ONE_SECOND);
    round3.end_scn_.convert_for_logservice(1295 * ONE_SECOND);
    round3.base_piece_id_ = 2;
    round3.piece_switch_interval_ = piece_switch_interval;
    round3.base_piece_scn_ = base_scn_1;
    round3.min_piece_id_ = 20;
    round3.max_piece_id_ = 25;
    for (int64_t i = round3.min_piece_id_; OB_SUCC(ret) && i < round3.max_piece_id_; i++) {
      FakePieceComponent piece;
      {
        piece.state_ = FakePieceState::FRONZEN;
        piece.piece_id_ = i;
        piece.min_file_id_ = file_id;
        file_id = file_id + 10;
        piece.max_file_id_ = file_id;
        piece.min_lsn_ = LSN((piece.min_file_id_ - 1) * 64 * ONE_MB + 10240);  // p20(111, 121), p22(121, 131), p23(131, 141), p24(141, 151), p25(NO file)
        piece.max_lsn_ = LSN((piece.max_file_id_ - 1) * 64 * ONE_MB + 10240);
      }
      ret = round3.array_.push_back(piece);
      EXPECT_EQ(OB_SUCCESS, ret);
      CLOG_LOG(INFO, "COME HERE print", K(piece), K(round3.round_id_));
    }
    FakePieceComponent piece;
    {
      piece.state_ = FakePieceState::ACTIVE;
      piece.piece_id_ = round3.max_piece_id_;
      piece.min_file_id_ = 0;
      piece.max_file_id_ = 0;
    }
    ret = round3.array_.push_back(piece);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  ret = rounds.array_.push_back(round3);
  EXPECT_EQ(OB_SUCCESS, ret);


  FakeArchivePieceContext archive_context;
  FakeArchivePieceContext *context = &archive_context;
  ret = archive_context.init(share::ObLSID(1001), &rounds);
  EXPECT_EQ(OB_SUCCESS, ret);

  int64_t log_ts = 1021 * ONE_SECOND;
  share::SCN scn;
  scn.convert_for_logservice(log_ts);
  palf::LSN lsn(64 * ONE_MB + ONE_MB);
  const int64_t ONE_PIECE_MB = 10 * 64 * ONE_MB;
  int64_t dest_id = 0;
  int64_t round_id = 0;
  int64_t piece_id = 0;
  int64_t cur_file_id = 0;
  int64_t offset = 0;
  bool to_newest = false;
  palf::LSN max_lsn;
  CLOG_LOG(INFO, "print get piece 1", K(lsn));
  ret = context->get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(round_id, 2);
  EXPECT_EQ(piece_id, 1);
  EXPECT_EQ(cur_file_id, 2);
  EXPECT_EQ(offset, 0);

  lsn = LSN(23 * 64 * ONE_MB);
  CLOG_LOG(INFO, "print get piece 2", K(lsn));
  scn.convert_for_logservice(log_ts + 3);
  ret = context->get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(round_id, 2);
  EXPECT_EQ(piece_id, 4);
  EXPECT_EQ(cur_file_id, 24);
  EXPECT_EQ(offset, 0);

  lsn = LSN(122 * 64 * ONE_MB);
  CLOG_LOG(INFO, "print get piece 3", K(lsn));
  scn.convert_for_logservice(log_ts+20);
  ret = archive_context.get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_ARCHIVE_ROUND_NOT_CONTINUOUS, ret);

  archive_context.reset_locate_info();
  log_ts = 1199 * ONE_SECOND;
  CLOG_LOG(INFO, "print get piece 4", K(lsn));
  scn.convert_for_logservice(log_ts);
  ret = archive_context.get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(round_id, 5);
  EXPECT_EQ(piece_id, 22);
  EXPECT_EQ(cur_file_id, 123);
  EXPECT_EQ(offset, 0);
  int64_t fake_offset = 102400;
  palf::LSN fake_lsn(102400);
  ret = archive_context.update_file_info(dest_id, round_id, piece_id, cur_file_id, fake_offset, fake_lsn);
  EXPECT_EQ(OB_SUCCESS, ret);
  lsn = lsn + 102400;
  CLOG_LOG(INFO, "print get piece 6", K(lsn));
  ret = archive_context.get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(round_id, 5);
  EXPECT_EQ(piece_id, 22);
  EXPECT_EQ(cur_file_id, 123);
  EXPECT_EQ(offset, fake_offset);
  EXPECT_EQ(max_lsn, fake_lsn);

  lsn = LSN(151 * 64 * ONE_MB + ONE_MB);
  CLOG_LOG(INFO, "print get piece 7", K(lsn));
  ret = archive_context.get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_ITER_END, ret);

  // bad case, current piece hang, can not backward piece
  lsn = LSN(10 * 64 * ONE_MB);
  CLOG_LOG(INFO, "print get piece 8", K(lsn));
  ret = archive_context.get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ret);

  archive_context.reset_locate_info();
  log_ts = 1010;
  scn.convert_for_logservice(log_ts);
  CLOG_LOG(INFO, "print get piece 9", K(lsn));
  ret = archive_context.get_piece(scn, lsn, dest_id, round_id, piece_id, cur_file_id, offset, max_lsn, to_newest);
  EXPECT_EQ(OB_SUCCESS, ret);
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_restore.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
