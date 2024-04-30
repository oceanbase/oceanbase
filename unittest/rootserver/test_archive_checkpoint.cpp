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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <time.h>
#include "share/backup/ob_tenant_archive_round.h"
#include "share/backup/ob_archive_checkpoint.h"
#include "share/backup/ob_tenant_archive_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace share;

class ArchiveCheckpointerTest : public testing::Test
{
public:
  ArchiveCheckpointerTest() {}
  virtual ~ArchiveCheckpointerTest(){}
  virtual void SetUp() {};
  virtual void TearDown() {}
  virtual void TestBody() {}

  const uint64_t TENANT_ID    = 1002UL;
  const int64_t DEST_ID       = 1001L;
  const int64_t DEST_NO       = 0L;
  const int64_t ROUND_ID      = 1L;
  const int64_t ONE_MINUTE    = 60 * 1000 * 1000L;

  // return unit: us
  int64_t convert_timestr_2_timestamp(const ObString &time_str)
  {
    const int64_t ONE_SECOND_US = 1000 * 1000;
    struct tm timeinfo;
    memset(&timeinfo, 0, sizeof(timeinfo));
    strptime(time_str.ptr(), "%Y-%m-%d %H:%M:%S", &timeinfo);
    time_t timestamp = mktime(&timeinfo);
    return timestamp * ONE_SECOND_US;
  }

  uint64_t convert_timestr_2_scn(const ObString &time_str)
  {
    int64_t ts = convert_timestr_2_timestamp(time_str);
    return static_cast<uint64_t>(ts) * 1000;
  }

  void fill_basic_round(ObTenantArchiveRoundAttr &round)
  {
    round.key_.tenant_id_ = TENANT_ID;
    round.key_.dest_no_ = DEST_NO;
    round.incarnation_ = OB_START_INCARNATION;
    round.dest_id_ = DEST_ID;
    round.round_id_ = ROUND_ID;
    // round.state_
    round.start_scn_ = share::SCN::min_scn();
    round.checkpoint_scn_ = share::SCN::min_scn();
    round.max_scn_ = share::SCN::min_scn();
    // round.compatible_
    round.base_piece_id_ = 1;
    round.used_piece_id_ = 1;
    round.piece_switch_interval_ = ONE_MINUTE;
    

    round.frozen_input_bytes_ = 0;
    round.frozen_output_bytes_ = 0;
    round.active_input_bytes_ = 0;
    round.active_output_bytes_ = 0;
    round.deleted_input_bytes_ = 0;
    round.deleted_output_bytes_ = 0;

    round.path_.assign("file:///obbackup/archive");
  }

  void fill_prepare_round(ObTenantArchiveRoundAttr &round)
  {
    fill_basic_round(round);
    round.state_.set_prepare();
  }

  void fill_beginning_round(ObTenantArchiveRoundAttr &round, const ObString &start_time)
  {
    fill_basic_round(round);

    uint64_t start_scn = convert_timestr_2_scn(start_time);

    round.start_scn_.convert_for_inner_table_field(start_scn);
    round.checkpoint_scn_.convert_for_inner_table_field(start_scn);
    round.max_scn_.convert_for_inner_table_field(start_scn);

    round.state_.set_beginning();
  }

  void fill_round(
      const ObArchiveRoundState& state,
      const ObString &start_time,
      const ObString &checkpoint_time,
      const ObString &max_time,
      const int64_t used_piece_id,
      const int64_t frozen_input_bytes,
      const int64_t frozen_output_bytes,
      const int64_t active_input_bytes,
      const int64_t active_output_bytes,
      const int64_t deleted_input_bytes,
      const int64_t deleted_output_bytes,
      ObTenantArchiveRoundAttr &round)
  {
    fill_beginning_round(round, start_time);

    round.state_ = state;
    round.checkpoint_scn_.convert_for_logservice(convert_timestr_2_scn(checkpoint_time));
    round.max_scn_.convert_for_logservice(convert_timestr_2_scn(max_time));
    round.used_piece_id_ = used_piece_id;

    round.frozen_input_bytes_ = frozen_input_bytes;
    round.frozen_output_bytes_ = frozen_output_bytes;
    round.active_input_bytes_ = active_input_bytes;
    round.active_output_bytes_ = active_output_bytes;
    round.deleted_input_bytes_ = deleted_input_bytes;
    round.deleted_output_bytes_ = deleted_output_bytes;
  }

  void fill_new_round(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObArchiveRoundState& state,
      const ObString &checkpoint_time,
      const ObString &max_time,
      const int64_t used_piece_id,
      const int64_t frozen_input_bytes,
      const int64_t frozen_output_bytes,
      const int64_t active_input_bytes,
      const int64_t active_output_bytes,
      const int64_t deleted_input_bytes,
      const int64_t deleted_output_bytes,
      ObTenantArchiveRoundAttr &new_round)
  {
    new_round.deep_copy_from(old_round);

    new_round.state_ = state;
    new_round.checkpoint_scn_.convert_for_logservice(convert_timestr_2_scn(checkpoint_time));
    new_round.max_scn_.convert_for_logservice(convert_timestr_2_scn(max_time));
    new_round.used_piece_id_ = used_piece_id;

    new_round.frozen_input_bytes_ = frozen_input_bytes;
    new_round.frozen_output_bytes_ = frozen_output_bytes;
    new_round.active_input_bytes_ = active_input_bytes;
    new_round.active_output_bytes_ = active_output_bytes;
    new_round.deleted_input_bytes_ = deleted_input_bytes;
    new_round.deleted_output_bytes_ = deleted_output_bytes;
  }

  void fill_piece(
      const ObTenantArchiveRoundAttr &old_round,
      const int64_t piece_id,
      const ObString &checkpoint_time,
      const ObString &max_time,
      const int64_t input_bytes,
      const int64_t output_bytes,
      const ObArchivePieceStatus &status,
      const ObBackupFileStatus::STATUS file_status,
      ObTenantArchivePieceAttr &piece)
  {
    piece.key_.tenant_id_ = old_round.key_.tenant_id_;
    piece.key_.dest_id_ = old_round.dest_id_;
    piece.key_.round_id_ = old_round.round_id_;
    piece.key_.piece_id_ = piece_id;
    piece.incarnation_ = old_round.incarnation_;
    piece.dest_no_ = old_round.key_.dest_no_;

    ObTenantArchiveMgr::decide_piece_start_scn(old_round.start_scn_, old_round.base_piece_id_, old_round.piece_switch_interval_, piece_id, piece.start_scn_);
    piece.checkpoint_scn_.convert_for_logservice(convert_timestr_2_scn(checkpoint_time));
    piece.max_scn_.convert_for_logservice(convert_timestr_2_scn(max_time));
    ObTenantArchiveMgr::decide_piece_end_scn(old_round.start_scn_, old_round.base_piece_id_, old_round.piece_switch_interval_, piece_id, piece.end_scn_);

    piece.input_bytes_ = input_bytes;
    piece.output_bytes_ = output_bytes;
    piece.status_ = status;
    piece.file_status_ = file_status;

    piece.path_ = old_round.path_;
  }

  void fill_not_archive_ls(ObLSDestRoundSummary &summary, const int64_t ls_id)
  {
    int ret = OB_SUCCESS;
    ObArchiveLSPieceSummary piece_summary;
    piece_summary.tenant_id_ = TENANT_ID;
    piece_summary.ls_id_ = ObLSID(ls_id);
    piece_summary.is_archiving_ = false;
    piece_summary.is_deleted_ = true;
    piece_summary.round_id_ = 0;

    ret = summary.add_one_piece(piece_summary);
  }

  void fill_archive_ls_piece(
      const int64_t ls_id,
      const bool is_deleted,
      const int64_t piece_id,
      const ObArchiveRoundState& state,
      const ObString &start_time,
      const ObString &checkpoint_time,
      const uint64_t min_lsn,
      const uint64_t max_lsn,
      const int64_t input_bytes,
      const int64_t output_bytes,
      ObArchiveLSPieceSummary &piece_summary)
  {
    piece_summary.tenant_id_ = TENANT_ID;
    piece_summary.ls_id_ = ObLSID(ls_id);
    piece_summary.is_archiving_ = true;
    piece_summary.is_deleted_ = is_deleted;

    piece_summary.dest_id_ = DEST_ID;
    piece_summary.round_id_ = ROUND_ID;
    piece_summary.piece_id_ = piece_id;
    piece_summary.incarnation_ = OB_START_INCARNATION;
    piece_summary.state_ = state;

    piece_summary.start_scn_.convert_for_logservice(convert_timestr_2_scn(start_time));
    piece_summary.checkpoint_scn_.convert_for_logservice(convert_timestr_2_scn(checkpoint_time));
    piece_summary.min_lsn_ = min_lsn;
    piece_summary.max_lsn_ = max_lsn;
    piece_summary.input_bytes_ = input_bytes;
    piece_summary.output_bytes_ = output_bytes;
  }

  ObDestRoundCheckpointer::GeneratedLSPiece gen_checkpoint_ls_piece(
      const int64_t ls_id,
      const ObString &start_time,
      const ObString &checkpoint_time,
      const uint64_t min_lsn,
      const uint64_t max_lsn,
      const int64_t input_bytes,
      const int64_t output_bytes)
  {
    ObDestRoundCheckpointer::GeneratedLSPiece piece;
    piece.ls_id_ = ObLSID(ls_id);
    piece.start_scn_.convert_for_logservice(convert_timestr_2_scn(start_time));
    piece.checkpoint_scn_.convert_for_logservice(convert_timestr_2_scn(checkpoint_time));
    piece.min_lsn_ = min_lsn;
    piece.max_lsn_ = max_lsn;
    piece.input_bytes_ = input_bytes;
    piece.output_bytes_ = output_bytes;
    return piece;
  }

  void fill_basic_round_summary(ObDestRoundSummary &summary)
  {
    summary.tenant_id_ = TENANT_ID;
    summary.dest_id_ = DEST_ID;
    summary.round_id_ = ROUND_ID;
  }


  void fill_empty_summary(ObDestRoundSummary &summary)
  {
    fill_basic_round_summary(summary);

    ObLSDestRoundSummary ls_summary1;
    fill_not_archive_ls(ls_summary1, 1001);

    ObLSDestRoundSummary ls_summary2;
    fill_not_archive_ls(ls_summary2, 1002);

    summary.add_ls_dest_round_summary(ls_summary1);
    summary.add_ls_dest_round_summary(ls_summary2);
  }


  int compare_two_rounds(const ObTenantArchiveRoundAttr &round1, const ObTenantArchiveRoundAttr &round2)
  {
    #define CMP_VALUE(field)  \
    if (OB_SUCC(ret)) { \
      if (round1.field##_ != round2.field##_) { \
        ret = OB_ERR_UNEXPECTED;  \
        LOG_WARN("not equal", K(ret), K_(round1.field), K_(round2.field)); \
      } \
    }

    int ret = OB_SUCCESS;
    CMP_VALUE(key);
    CMP_VALUE(incarnation);
    CMP_VALUE(dest_id);
    CMP_VALUE(round_id);
    CMP_VALUE(state);
    CMP_VALUE(start_scn);
    CMP_VALUE(checkpoint_scn);
    CMP_VALUE(max_scn);
    CMP_VALUE(compatible);
    CMP_VALUE(base_piece_id);
    CMP_VALUE(used_piece_id);
    CMP_VALUE(piece_switch_interval);
    CMP_VALUE(frozen_input_bytes);
    CMP_VALUE(frozen_output_bytes);
    CMP_VALUE(active_input_bytes);
    CMP_VALUE(active_output_bytes);
    CMP_VALUE(deleted_input_bytes);
    CMP_VALUE(deleted_output_bytes);
    CMP_VALUE(path);

    #undef CMP_VALUE
    return ret;
  }

  int compare_two_pieces(const ObTenantArchivePieceAttr &piece1, const ObTenantArchivePieceAttr &piece2)
  {
    #define CMP_VALUE(field)  \
    if (OB_SUCC(ret)) { \
      if (piece1.field##_ != piece2.field##_) { \
        ret = OB_ERR_UNEXPECTED;  \
        LOG_WARN("not equal", K(ret), K_(piece1.field), K_(piece2.field)); \
      } \
    }

    int ret = OB_SUCCESS;
    CMP_VALUE(key);
    CMP_VALUE(incarnation);
    CMP_VALUE(dest_no);
    CMP_VALUE(start_scn);
    CMP_VALUE(checkpoint_scn);
    CMP_VALUE(max_scn);
    CMP_VALUE(end_scn);
    CMP_VALUE(compatible);
    CMP_VALUE(input_bytes);
    CMP_VALUE(output_bytes);
    CMP_VALUE(status);
    CMP_VALUE(file_status);
    CMP_VALUE(path);

    #undef CMP_VALUE
    return ret;
  }

  int compare_two_checkpoint_pieces(const ObDestRoundCheckpointer::GeneratedPiece &piece1, const ObDestRoundCheckpointer::GeneratedPiece &piece2)
  {
    #define CMP_VALUE(field)  \
    if (OB_SUCC(ret)) { \
      if (ls_piece1.field##_ != ls_piece2.field##_) { \
        ret = OB_ERR_UNEXPECTED;  \
        LOG_WARN("not equal", K(ret), K_(ls_piece1.field), K_(ls_piece2.field)); \
      } \
    }

    int ret = OB_SUCCESS;
    if (OB_FAIL(compare_two_pieces(piece1.piece_info_, piece2.piece_info_))) {
      LOG_WARN("piece not equal", K(ret), K_(piece1.piece_info), K_(piece2.piece_info));
    }

    if (OB_SUCC(ret)) {
      if (piece1.ls_piece_list_.count() != piece2.ls_piece_list_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls piece list count", K(ret), K_(piece1.ls_piece_list), K_(piece2.ls_piece_list));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < piece1.ls_piece_list_.count(); i++) {
          const ObDestRoundCheckpointer::GeneratedLSPiece &ls_piece1 = piece1.ls_piece_list_.at(i);

          const ObDestRoundCheckpointer::GeneratedLSPiece *find_piece;
          int64_t j = 0;
          for (; j < piece2.ls_piece_list_.count(); j++) {
            const ObDestRoundCheckpointer::GeneratedLSPiece &ls_piece = piece2.ls_piece_list_.at(i);
            if (ls_piece1.ls_id_ == ls_piece.ls_id_) {
              find_piece = &ls_piece;
              break;
            }
          }
          if (j == piece2.ls_piece_list_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cannot find ls piece", K(ret), K(ls_piece1), K_(piece2.ls_piece_list));
          } else {
            const ObDestRoundCheckpointer::GeneratedLSPiece &ls_piece2 = *find_piece;
            CMP_VALUE(ls_id);
            CMP_VALUE(start_scn);
            CMP_VALUE(checkpoint_scn);
            CMP_VALUE(min_lsn);
            CMP_VALUE(max_lsn);
            CMP_VALUE(input_bytes);
            CMP_VALUE(output_bytes);
          }
        }
      }
    }

    #undef CMP_VALUE
    return ret;
  }

};


static int64_t g_call_cnt = 0;

TEST_F(ArchiveCheckpointerTest, in_prepare)
{
  class MockRoundHandler final : public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      // should not come here
      g_call_cnt++;
      return OB_ERR_UNEXPECTED; 
    }

  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        // should not come here
        g_call_cnt++;
        return OB_ERR_UNEXPECTED; 
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        // should not come here
        g_call_cnt++;
        return OB_ERR_UNEXPECTED; 
      };

  // old round's status is PREPARE，checkpoint is not allowed.
  ObTenantArchiveRoundAttr old_round;
  fill_prepare_round(old_round);

  ObDestRoundSummary summary;
  fill_empty_summary(summary);

  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN scn;
  (void)scn.convert_for_logservice(1000);
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(g_call_cnt, 0);
}


TEST_F(ArchiveCheckpointerTest, in_beginning_01)
{
  // old round's status is BEGINNING
  ObTenantArchiveRoundAttr old_round;
  fill_beginning_round(old_round, "2022-01-01 00:00:30");

  // 2 log streams, one is archiving, the other has not started archive.
  ObDestRoundSummary summary;
  // log stream 1001 has not started archive.
  ObLSDestRoundSummary ls_1001;
  fill_not_archive_ls(ls_1001, 1001);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // As long as one log stream is not archived, the status will not advance.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      // should not come here.
      g_call_cnt++;
      return OB_ERR_UNEXPECTED; 
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        // should not come here.
        g_call_cnt++;
        return OB_ERR_UNEXPECTED;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        // should not come here.
        g_call_cnt++;
        return OB_ERR_UNEXPECTED;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:50"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 0);
}


TEST_F(ArchiveCheckpointerTest, in_beginning_02)
{
  // old round's status is BEGINNING.
  ObTenantArchiveRoundAttr old_round;
  fill_beginning_round(old_round, "2022-01-01 00:00:30");

  // 2 log streams, one is archiving, the other is INTERRUPTED.
  ObDestRoundSummary summary;
  // log stream 1001 is INTERRUPTED.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::interrupted(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:30",
    0,
    0,
    0,
    0,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // As long as one log stream is interrupted, the round's status will turn to INTERRUPTED.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::interrupted(),
        "2022-01-01 00:00:30",
        "2022-01-01 00:00:40",
        1,
        0,
        0,
        100,
        10,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:30",
        "2022-01-01 00:00:40",
        100,
        10,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:40",
          100,
          10,
          ObArchivePieceStatus::active(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:30",
          0,
          0,
          0,
          0);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:40",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::interrupted(),
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:40",
          1,
          0,
          0,
          100,
          10,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:50"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}


TEST_F(ArchiveCheckpointerTest, in_beginning_03)
{
  // old round's status is BEGINNING.
  ObTenantArchiveRoundAttr old_round;
  fill_beginning_round(old_round, "2022-01-01 00:00:30");

  // 2 log streams are archiving.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // All log streams are archiving, status turn to DOING.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::doing(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:50",
        1,
        0,
        0,
        300,
        30,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:50",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:50",
          300,
          30,
          ObArchivePieceStatus::active(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:50",
          0,
          2000,
          200,
          20);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:40",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::doing(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:50",
          1,
          0,
          0,
          300,
          30,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:45"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}


TEST_F(ArchiveCheckpointerTest, in_doing_01)
{
  // old round's status is DOING
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:35",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams are archiving.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // All log streams are archiving, the next status is DOING.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::doing(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:50",
        1,
        0,
        0,
        300,
        30,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:50",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:50",
          300,
          30,
          ObArchivePieceStatus::active(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:50",
          0,
          2000,
          200,
          20);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:40",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::doing(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:50",
          1,
          0,
          0,
          300,
          30,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:45"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}


TEST_F(ArchiveCheckpointerTest, in_doing_02)
{
  // old round's status is DOING.
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:00:35",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams are archiving.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1/* piece id */,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00"/* start time */,
    "2022-01-01 00:01:00"/* checkpoint time*/,
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1/* piece id */,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00"/* start time */,
    "2022-01-01 00:01:00"/* checkpoint time*/,
    0,
    1000,
    100,
    10,
    piece_1002_1);
  
  ObArchiveLSPieceSummary piece_1002_2;
  fill_archive_ls_piece(
    1002, 
    false, 
    2/* piece id */,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:01:00"/* start time */,
    "2022-01-01 00:01:30"/* checkpoint time*/,
    1000,
    2000,
    100,
    10,
    piece_1002_2);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_2), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // All log streams are archiving, the next status is DOING.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::doing(),
        "2022-01-01 00:00:50",
        "2022-01-01 00:01:30",
        2,
        0,
        0,
        400,
        40,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece_1;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:50",
        "2022-01-01 00:01:00",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece_1);
      
      ObTenantArchivePieceAttr expect_piece_2;
      test.fill_piece(
        old_round, 
        2, 
        "2022-01-01 00:01:00",
        "2022-01-01 00:01:30",
        100,
        10,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
        expect_piece_2);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_1 = pieces.at(0);
        if (OB_FAIL(test.compare_two_pieces(piece_1, expect_piece_1))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_1), K(expect_piece_1));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_2 = pieces.at(1);
        if (OB_FAIL(test.compare_two_pieces(piece_2, expect_piece_2))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_2), K(expect_piece_2));
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        if (piece.piece_info_.key_.piece_id_ == 1) {
          test.fill_piece(
            old_round, 
            1, 
            "2022-01-01 00:00:50",
            "2022-01-01 00:01:00",
            300,
            30,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
            expect_piece.piece_info_);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
            1001,
            "2022-01-01 00:00:00",
            "2022-01-01 00:01:00",
            0,
            2000,
            200,
            20);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:00:00",
            "2022-01-01 00:01:00",
            0,
            1000,
            100,
            10);
          
          expect_piece.ls_piece_list_.push_back(ls_piece_1001);
          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        } else {
          test.fill_piece(
            old_round, 
            2, 
            "2022-01-01 00:01:00",
            "2022-01-01 00:01:30",
            100,
            10,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
            expect_piece.piece_info_);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:01:00",
            "2022-01-01 00:01:30",
            1000,
            2000,
            100,
            10);
           expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        }
        
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::doing(),
          "2022-01-01 00:00:50",
          "2022-01-01 00:01:30",
          2,
          0,
          0,
          400,
          40,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:50"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 4);
}


TEST_F(ArchiveCheckpointerTest, in_doing_03)
{
  // old round's status is DOING
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:00:35",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams are archiving.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:01:00",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  
  ObArchiveLSPieceSummary piece_1001_2;
  fill_archive_ls_piece(
    1001, 
    false, 
    2,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:01:00",
    "2022-01-01 00:01:30",
    2000,
    4000,
    200,
    20,
    piece_1001_2);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_2), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:01:00",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  
  ObArchiveLSPieceSummary piece_1002_2;
  fill_archive_ls_piece(
    1002, 
    false, 
    2,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:01:00",
    "2022-01-01 00:02:00",
    1000,
    2000,
    100,
    10,
    piece_1002_2);
  
  ObArchiveLSPieceSummary piece_1002_3;
  fill_archive_ls_piece(
    1002, 
    false, 
    3,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:02:00",
    "2022-01-01 00:02:30",
    2000,
    3000,
    100,
    10,
    piece_1002_3);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_2), OB_SUCCESS);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_3), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // All log streams are archiving, the next status is DOING.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::doing(),
        "2022-01-01 00:01:30",
        "2022-01-01 00:02:30",
        3,
        300,
        30,
        400,
        40,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece_1;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:01:00",
        "2022-01-01 00:01:00",
        300,
        30,
        ObArchivePieceStatus::frozen(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece_1);
      
      ObTenantArchivePieceAttr expect_piece_2;
      test.fill_piece(
        old_round, 
        2, 
        "2022-01-01 00:01:30",
        "2022-01-01 00:02:00",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece_2);
      
      ObTenantArchivePieceAttr expect_piece_3;
      test.fill_piece(
        old_round, 
        3, 
        "2022-01-01 00:02:00",
        "2022-01-01 00:02:30",
        100,
        10,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
        expect_piece_3);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 3) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_1 = pieces.at(0);
        if (OB_FAIL(test.compare_two_pieces(piece_1, expect_piece_1))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_1), K(expect_piece_1));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_2 = pieces.at(1);
        if (OB_FAIL(test.compare_two_pieces(piece_2, expect_piece_2))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_2), K(expect_piece_2));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_3 = pieces.at(2);
        if (OB_FAIL(test.compare_two_pieces(piece_3, expect_piece_3))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_3), K(expect_piece_3));
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        if (piece.piece_info_.key_.piece_id_ == 1) {
          test.fill_piece(
            old_round, 
            1, 
            "2022-01-01 00:01:00",
            "2022-01-01 00:01:00",
            300,
            30,
            ObArchivePieceStatus::frozen(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
            expect_piece.piece_info_);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
            1001,
            "2022-01-01 00:00:00",
            "2022-01-01 00:01:00",
            0,
            2000,
            200,
            20);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:00:00",
            "2022-01-01 00:01:00",
            0,
            1000,
            100,
            10);
          
          expect_piece.ls_piece_list_.push_back(ls_piece_1001);
          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        } else if (piece.piece_info_.key_.piece_id_ == 2) {
          test.fill_piece(
            old_round, 
            2, 
            "2022-01-01 00:01:30",
            "2022-01-01 00:02:00",
            300,
            30,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
            expect_piece.piece_info_);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
            1001,
            "2022-01-01 00:01:00",
            "2022-01-01 00:01:30",
            2000,
            4000,
            200,
            20);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:01:00",
            "2022-01-01 00:02:00",
            1000,
            2000,
            100,
            10);

          expect_piece.ls_piece_list_.push_back(ls_piece_1001);
          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        } else {
          test.fill_piece(
            old_round, 
            3, 
            "2022-01-01 00:02:00",
            "2022-01-01 00:02:30",
            100,
            10,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
            expect_piece.piece_info_);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:02:00",
            "2022-01-01 00:02:30",
            2000,
            3000,
            100,
            10);

          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        }
        
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::doing(),
          "2022-01-01 00:01:30",
          "2022-01-01 00:02:30",
          3,
          300,
          30,
          400,
          40,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:03:00"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 5);
}


TEST_F(ArchiveCheckpointerTest, in_doing_04)
{
  // old round's status is DOING.
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:35",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams, one is archiving, the other has not started archive.
  ObDestRoundSummary summary;
  // log stream is created after DOING status.
  ObLSDestRoundSummary ls_1001;
  fill_not_archive_ls(ls_1001, 1001);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // As long as one log stream is not archived, the status will retain.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      // should not come here.
      g_call_cnt++;
      return OB_ERR_UNEXPECTED; 
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        // should not come here.
        g_call_cnt++;
        return OB_ERR_UNEXPECTED;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        // should not come here.
        g_call_cnt++;
        return OB_ERR_UNEXPECTED;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:45"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 0);
}


TEST_F(ArchiveCheckpointerTest, in_doing_05)
{
  // old round's status is DOING
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:35",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams, one is archiving, the other is INTERRUPTED.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is INTERRUPTED.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::interrupted(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:55",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // As long as one log stream is INTERRUPTED, the next status is INTERRUPTED.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::interrupted(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        1,
        0,
        0,
        300,
        30,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          300,
          30,
          ObArchivePieceStatus::active(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:50",
          0,
          2000,
          200,
          20);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:55",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::interrupted(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          1,
          0,
          0,
          300,
          30,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:40"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}

// Some log stream is deleted during archive.
TEST_F(ArchiveCheckpointerTest, in_doing_06)
{
  // old round's status is DOING
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing()/* status */,
    "2022-01-01 00:00:30"/* start_time */,
    "2022-01-01 00:00:35"/* checkpoint_time */,
    "2022-01-01 00:00:40"/* max_time */,
    1/* used_piece_id */,
    0/* frozen_input_bytes */,
    0/* frozen_output_bytes */,
    100/* active_input_bytes */,
    10/* active_output_bytes */,
    0/* deleted_input_bytes */,
    0/* deleted_output_bytes */,
    old_round);

  // 3 log streams, one is deleted.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001/* ls_id */,
    false/* is_deleted */,
    1/* piece_id */,
    ObArchiveRoundState::doing()/* state */,
    "2022-01-01 00:00:30"/* start_time */,
    "2022-01-01 00:00:50"/* checkpoint_time */,
    0/* min_lsn */,
    2000/* max_lsn */,
    200/* input_bytes */,
    20/* output_bytes */,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002/* ls_id */,
    false/* is_deleted */,
    1/* piece_id */,
    ObArchiveRoundState::doing()/* state */,
    "2022-01-01 00:00:30"/* start_time */,
    "2022-01-01 00:00:55"/* checkpoint_time */,
    0/* min_lsn */,
    1000/* max_lsn */,
    100/* input_bytes */,
    10/* output_bytes */,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);

  // log stream 1003 is deleted.
  ObLSDestRoundSummary ls_1003;
  ObArchiveLSPieceSummary piece_1003_1;
  fill_archive_ls_piece(
    1003/* ls_id */,
    true/* is_deleted */,
    1/* piece_id */,
    ObArchiveRoundState::doing()/* state */,
    "2022-01-01 00:00:30"/* start_time */,
    "2022-01-01 00:00:40"/* checkpoint_time */,
    0/* min_lsn */,
    1000/* max_lsn */,
    100/* input_bytes */,
    10/* output_bytes */,
    piece_1003_1);
  ASSERT_EQ(ls_1003.add_one_piece(piece_1003_1), OB_SUCCESS);


  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1003), OB_SUCCESS);


  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round,
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round,
        ObArchiveRoundState::doing()/* state */,
        "2022-01-01 00:00:50"/* checkpoint_time */,
        "2022-01-01 00:00:55"/* max_time */,
        1/* used_piece_id */,
        0/* frozen_input_bytes */,
        0/* frozen_output_bytes */,
        400/* active_input_bytes */,
        40/* active_output_bytes */,
        0/* deleted_input_bytes */,
        0/* deleted_output_bytes */,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round,
        1/* piece_id */,
        "2022-01-01 00:00:50"/* checkpoint_time */,
        "2022-01-01 00:00:55"/* max_time */,
        400/* input_bytes */,
        40/* output_bytes */,
        ObArchivePieceStatus::active()/* status */,
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE/* file_status */,
        expect_piece);


      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb =
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece)
      {
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round,
          1/* piece_id */,
          "2022-01-01 00:00:50"/* checkpoint_time */,
          "2022-01-01 00:00:55"/* max_time */,
          400/* input_bytes */,
          40/* output_bytes */,
          ObArchivePieceStatus::active()/* status */,
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE/* file_status */,
          expect_piece.piece_info_);

        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001/* ls_id */,
          "2022-01-01 00:00:30"/* start_time */,
          "2022-01-01 00:00:50"/* checkpoint_time */,
          0/* min_lsn */,
          2000/* max_lsn */,
          200/* input_bytes */,
          20/* output_bytes */);

        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002/* ls_id */,
          "2022-01-01 00:00:30"/* start_time */,
          "2022-01-01 00:00:55"/* checkpoint_time */,
          0/* min_lsn */,
          1000/* max_lsn */,
          100/* input_bytes */,
          10/* output_bytes */);

        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1003 = test.gen_checkpoint_ls_piece(
          1003/* ls_id */,
          "2022-01-01 00:00:30"/* start_time */,
          "2022-01-01 00:00:40"/* checkpoint_time */,
          0/* min_lsn */,
          1000/* max_lsn */,
          100/* input_bytes */,
          10/* output_bytes */);

        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        expect_piece.ls_piece_list_.push_back(ls_piece_1003);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };

  ObDestRoundCheckpointer::RoundCheckpointCb round_cb =
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round)
      {
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round,
          ObArchiveRoundState::doing()/* state */,
          "2022-01-01 00:00:50"/* checkpoint_time */,
          "2022-01-01 00:00:55"/* max_time */,
          1/* used_piece_id */,
          0/* frozen_input_bytes */,
          0/* frozen_output_bytes */,
          400/* active_input_bytes */,
          40/* active_output_bytes */,
          0/* deleted_input_bytes */,
          0/* deleted_output_bytes */,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };

  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:59"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}

TEST_F(ArchiveCheckpointerTest, in_doing_07)
{
  // old round's status is DOING.
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",   /* start time */
    "2022-01-01 00:00:20",   /* checkpoint time */
    "2022-01-01 00:00:30",   /* max time */
    1,                       /* used piece id */
    0,                       /* frozen_input_bytes */
    0,                       /* frozen_output_bytes */
    100,                     /* active_input_bytes */
    10,                      /* active_output_bytes */
    0,                       /* deleted_input_bytes */
    0,                       /* deleted_output_bytes */
    old_round);

  // 2 log streams are archiving.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001,                           /* ls id */
    false,                          /* is deleted */
    1,                              /* piece id */
    ObArchiveRoundState::doing(),   /* state */
    "2022-01-01 00:00:00",          /* start time */
    "2022-01-01 00:00:50",          /* checkpoint time */
    0,                              /* min_lsn */
    2000,                           /* max_lsn */
    200,                            /* input_bytes */
    20,                             /* output_bytes */
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is archiving.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002,                           /* ls id */
    false,                          /* is deleted */
    1,                              /* piece id */
    ObArchiveRoundState::doing(),   /* state */
    "2022-01-01 00:00:00",          /* start time */
    "2022-01-01 00:00:40",          /* checkpoint time */
    0,                              /* min_lsn */
    1000,                           /* max_lsn */
    100,                            /* input_bytes */
    10,                             /* output_bytes */
    piece_1002_1);

  ObArchiveLSPieceSummary piece_1002_2;
  fill_archive_ls_piece(
    1002,                           /* ls id */
    false,                          /* is deleted */
    2,                              /* piece id */
    ObArchiveRoundState::doing(),   /* state */
    "2022-01-01 00:01:00",          /* start time */
    "2022-01-01 00:01:30",          /* checkpoint time */
    1000,                           /* min_lsn */
    2000,                           /* max_lsn */
    100,                            /* input_bytes */
    10,                             /* output_bytes */
    piece_1002_2);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_2), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // All log streams are archiving, the next status is DOING.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round,
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round,
        ObArchiveRoundState::doing(),   /* state */
        "2022-01-01 00:00:40",          /* checkpoint_time */
        "2022-01-01 00:01:30",          /* max_time */
        2,                              /* used_piece_id */
        0,                              /* frozen_input_bytes */
        0,                              /* frozen_output_bytes */
        400,                            /* active_input_bytes */
        40,                             /* active_output_bytes */
        0,                              /* deleted_input_bytes */
        0,                              /* deleted_output_bytes */
        expect_round);

      ObTenantArchivePieceAttr expect_piece_1;
      test.fill_piece(
        old_round,
        1,                                                    /* piece id */
        "2022-01-01 00:00:40",                                /* checkpoint time */
        "2022-01-01 00:00:50",                                /* max time */
        300,                                                  /* input_bytes */
        30,                                                   /* output_bytes */
        ObArchivePieceStatus::active(),                       /* piece status */
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,    /* file status */
        expect_piece_1);

      ObTenantArchivePieceAttr expect_piece_2;
      test.fill_piece(
        old_round,
        2,                                                    /* piece id */
        "2022-01-01 00:01:00",                                /* checkpoint time */
        "2022-01-01 00:01:30",                                /* max time */
        100,                                                  /* input_bytes */
        10,                                                   /* output_bytes */
        ObArchivePieceStatus::active(),                       /* piece status */
        ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,   /* file status */
        expect_piece_2);


      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_1 = pieces.at(0);
        if (OB_FAIL(test.compare_two_pieces(piece_1, expect_piece_1))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_1), K(expect_piece_1));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_2 = pieces.at(1);
        if (OB_FAIL(test.compare_two_pieces(piece_2, expect_piece_2))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_2), K(expect_piece_2));
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb =
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece)
      {
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        if (piece.piece_info_.key_.piece_id_ == 1) {
          test.fill_piece(
            old_round,
            1,
            "2022-01-01 00:00:40",
            "2022-01-01 00:00:50",
            300,
            30,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
            expect_piece.piece_info_);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
            1001,
            "2022-01-01 00:00:00",
            "2022-01-01 00:00:50",
            0,
            2000,
            200,
            20);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:00:00",
            "2022-01-01 00:00:40",
            0,
            1000,
            100,
            10);

          expect_piece.ls_piece_list_.push_back(ls_piece_1001);
          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        } else {
          test.fill_piece(
            old_round,
            2,
            "2022-01-01 00:01:00",
            "2022-01-01 00:01:30",
            100,
            10,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
            expect_piece.piece_info_);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:01:00",
            "2022-01-01 00:01:30",
            1000,
            2000,
            100,
            10);
           expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        }

        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };

  ObDestRoundCheckpointer::RoundCheckpointCb round_cb =
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round)
      {
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round,
          ObArchiveRoundState::doing(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:01:30",
          2,
          0,
          0,
          400,
          40,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };

  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:45"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 4);
}


TEST_F(ArchiveCheckpointerTest, in_stopping_01)
{
  // old round's status is STOPPING.
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::stopping(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams, one is archiving, the other is STOP.
  ObDestRoundSummary summary;
  // log stream 1001 is archiving.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is STOP.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::stop(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:55",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // checkpoint_scn will not advance after archive is STOPPPING. But, max_scn will always advance.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::stopping(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        1,
        0,
        0,
        300,
        30,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          300,
          30,
          ObArchivePieceStatus::active(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:50",
          0,
          2000,
          200,
          20);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:55",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::stopping(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          1,
          0,
          0,
          300,
          30,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:02:00"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}


TEST_F(ArchiveCheckpointerTest, in_stopping_02)
{
  // old round's status is STOPPING
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::stopping(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams are STOP.
  ObDestRoundSummary summary;
  // log stream is STOP.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::stop(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream is STOP.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::stop(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:55",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // All log streams are STOP, the next status is STOP.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::stop(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        1,
        300,
        30,
        0,
        0,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        300,
        30,
        ObArchivePieceStatus::frozen(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          300,
          30,
          ObArchivePieceStatus::frozen(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:50",
          0,
          2000,
          200,
          20);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:55",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::stop(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          1,
          300,
          30,
          0,
          0,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:02:00"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}


TEST_F(ArchiveCheckpointerTest, in_stopping_03)
{
  // old round's status is STOPPING.
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::stopping(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams, one is STOP, the other is created after STOPPING.
  ObDestRoundSummary summary;
  // log stream 1001 is created after STOPPING.
  ObLSDestRoundSummary ls_1001;
  fill_not_archive_ls(ls_1001, 1001);

  // log stream 1002 is STOP.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::stop(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:55",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // Although log stream 1001 has not archived, but is created after STOPPING. It will 
  // not be considered.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::stop(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        1,
        100,
        10,
        0,
        0,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        100,
        10,
        ObArchivePieceStatus::frozen(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          100,
          10,
          ObArchivePieceStatus::frozen(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:55",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::stop(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          1,
          100,
          10,
          0,
          0,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:02:00"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}


TEST_F(ArchiveCheckpointerTest, in_stopping_04)
{
  // old round's status is STOPPING.
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::stopping(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:40",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams, one is STOP, the other is INTERRUPED.
  ObDestRoundSummary summary;
  // log stream 1001 is STOP.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::stop(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is INTERRUPTED.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::interrupted(),
    "2022-01-01 00:00:30",
    "2022-01-01 00:00:55",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  // Only all log streams have stopped archiving, the next status is STOP.
  // Otherwise, it will be stuck in the STOPPING state.
  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::stopping(),
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        1,
        0,
        0,
        300,
        30,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:40",
        "2022-01-01 00:00:55",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        } else {
          const ObTenantArchivePieceAttr &piece = pieces.at(0);
          ret = test.compare_two_pieces(piece, expect_piece);
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        test.fill_piece(
          old_round, 
          1, 
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          300,
          30,
          ObArchivePieceStatus::active(),
          ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
          expect_piece.piece_info_);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
          1001,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:50",
          0,
          2000,
          200,
          20);
        
        ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
          1002,
          "2022-01-01 00:00:30",
          "2022-01-01 00:00:55",
          0,
          1000,
          100,
          10);
        
        expect_piece.ls_piece_list_.push_back(ls_piece_1001);
        expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::stopping(),
          "2022-01-01 00:00:40",
          "2022-01-01 00:00:55",
          1,
          0,
          0,
          300,
          30,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:00:40"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 3);
}

TEST_F(ArchiveCheckpointerTest, some_ls_interrupt_01)
{
  // old round's status is DOING
  ObTenantArchiveRoundAttr old_round;
  fill_round(
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:00:35",
    "2022-01-01 00:00:40",
    1,
    0,
    0,
    100,
    10,
    0,
    0,
    old_round);

  // 2 log streams, one is DOING, the other is INTERRUPED.
  ObDestRoundSummary summary;
  // log stream 1001 is DOING.
  ObLSDestRoundSummary ls_1001;
  ObArchiveLSPieceSummary piece_1001_1;
  fill_archive_ls_piece(
    1001, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:00:50",
    0,
    2000,
    200,
    20,
    piece_1001_1);
  
  ASSERT_EQ(ls_1001.add_one_piece(piece_1001_1), OB_SUCCESS);

  // log stream 1002 is INTERRUPED.
  ObLSDestRoundSummary ls_1002;
  ObArchiveLSPieceSummary piece_1002_1;
  fill_archive_ls_piece(
    1002, 
    false, 
    1,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:00:00",
    "2022-01-01 00:01:00",
    0,
    1000,
    100,
    10,
    piece_1002_1);
  
  ObArchiveLSPieceSummary piece_1002_2;
  fill_archive_ls_piece(
    1002, 
    false, 
    2,
    ObArchiveRoundState::doing(),
    "2022-01-01 00:01:00",
    "2022-01-01 00:02:00",
    1000,
    2000,
    100,
    10,
    piece_1002_2);
  
  ObArchiveLSPieceSummary piece_1002_3;
  fill_archive_ls_piece(
    1002, 
    false, 
    3,
    ObArchiveRoundState::interrupted(),
    "2022-01-01 00:02:00",
    "2022-01-01 00:02:30",
    2000,
    3000,
    100,
    10,
    piece_1002_3);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_1), OB_SUCCESS);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_2), OB_SUCCESS);
  ASSERT_EQ(ls_1002.add_one_piece(piece_1002_3), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1001), OB_SUCCESS);
  ASSERT_EQ(summary.add_ls_dest_round_summary(ls_1002), OB_SUCCESS);


  class MockRoundHandler final: public ObArchiveRoundHandler
  {
  public:
    int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces) override
    {
      int ret = OB_SUCCESS;
      g_call_cnt++;
      ArchiveCheckpointerTest test;
      ObTenantArchiveRoundAttr expect_round;
      test.fill_new_round(
        old_round, 
        ObArchiveRoundState::interrupted(),
        "2022-01-01 00:00:50",
        "2022-01-01 00:02:30",
        3,
        0,
        0,
        500,
        50,
        0,
        0,
        expect_round);

      ObTenantArchivePieceAttr expect_piece_1;
      test.fill_piece(
        old_round, 
        1, 
        "2022-01-01 00:00:50",
        "2022-01-01 00:01:00",
        300,
        30,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
        expect_piece_1);
      
      ObTenantArchivePieceAttr expect_piece_2;
      test.fill_piece(
        old_round, 
        2, 
        "2022-01-01 00:01:00",
        "2022-01-01 00:02:00",
        100,
        10,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
        expect_piece_2);
      
      ObTenantArchivePieceAttr expect_piece_3;
      test.fill_piece(
        old_round, 
        3, 
        "2022-01-01 00:02:00",
        "2022-01-01 00:02:30",
        100,
        10,
        ObArchivePieceStatus::active(),
        ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
        expect_piece_3);

      
      ret = test.compare_two_rounds(new_round, expect_round);
      if (OB_SUCC(ret)) {
        if (pieces.count() != 3) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pieces count", K(ret), K(pieces));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_1 = pieces.at(0);
        if (OB_FAIL(test.compare_two_pieces(piece_1, expect_piece_1))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_1), K(expect_piece_1));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_2 = pieces.at(1);
        if (OB_FAIL(test.compare_two_pieces(piece_2, expect_piece_2))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_2), K(expect_piece_2));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTenantArchivePieceAttr &piece_3 = pieces.at(2);
        if (OB_FAIL(test.compare_two_pieces(piece_3, expect_piece_3))) {
          LOG_WARN("not equal pieces", K(ret), K(piece_3), K(expect_piece_3));
        }
      }
      return ret;
    }
  };

  ObDestRoundCheckpointer::PieceGeneratedCb gen_piece_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObDestRoundCheckpointer::Result &result, const ObDestRoundCheckpointer::GeneratedPiece &piece) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObDestRoundCheckpointer::GeneratedPiece expect_piece;
        if (piece.piece_info_.key_.piece_id_ == 1) {
          test.fill_piece(
            old_round, 
            1, 
            "2022-01-01 00:00:50",
            "2022-01-01 00:01:00",
            300,
            30,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE,
            expect_piece.piece_info_);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1001 = test.gen_checkpoint_ls_piece(
            1001,
            "2022-01-01 00:00:00",
            "2022-01-01 00:00:50",
            0,
            2000,
            200,
            20);
          
          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:00:00",
            "2022-01-01 00:01:00",
            0,
            1000,
            100,
            10);
          
          expect_piece.ls_piece_list_.push_back(ls_piece_1001);
          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        } else if (piece.piece_info_.key_.piece_id_ == 2) {
          test.fill_piece(
            old_round, 
            2, 
            "2022-01-01 00:01:00",
            "2022-01-01 00:02:00",
            100,
            10,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
            expect_piece.piece_info_);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:01:00",
            "2022-01-01 00:02:00",
            1000,
            2000,
            100,
            10);

          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        } else {
          test.fill_piece(
            old_round, 
            3, 
            "2022-01-01 00:02:00",
            "2022-01-01 00:02:30",
            100,
            10,
            ObArchivePieceStatus::active(),
            ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE,
            expect_piece.piece_info_);

          ObDestRoundCheckpointer::GeneratedLSPiece ls_piece_1002 = test.gen_checkpoint_ls_piece(
            1002,
            "2022-01-01 00:02:00",
            "2022-01-01 00:02:30",
            2000,
            3000,
            100,
            10);

          expect_piece.ls_piece_list_.push_back(ls_piece_1002);
        }
        
        ret = test.compare_two_checkpoint_pieces(piece, expect_piece);
        return ret;
      };
  
  ObDestRoundCheckpointer::RoundCheckpointCb round_cb = 
    [](common::ObISQLClient *proxy, const ObTenantArchiveRoundAttr &old_round, const ObTenantArchiveRoundAttr &new_round) 
      { 
        int ret = OB_SUCCESS;
        g_call_cnt++;
        ArchiveCheckpointerTest test;
        ObTenantArchiveRoundAttr expect_round;
        test.fill_new_round(
          old_round, 
          ObArchiveRoundState::interrupted(),
          "2022-01-01 00:00:50",
          "2022-01-01 00:02:30",
          3,
          0,
          0,
          500,
          50,
          0,
          0,
          expect_round);

        ret = test.compare_two_rounds(new_round, expect_round);
        return ret;
      };
  
  int ret = OB_SUCCESS;
  g_call_cnt = 0;
  MockRoundHandler mock_handler;
  ObDestRoundCheckpointer checkpointer;
  share::SCN limit_scn;
  (void)limit_scn.convert_for_logservice(convert_timestr_2_scn("2022-01-01 00:03:00"));
  ret = checkpointer.init(&mock_handler, gen_piece_cb, round_cb, limit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = checkpointer.checkpoint(old_round, summary);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(g_call_cnt, 5);
}

int main(int argc, char **argv)
{
  system("rm -f test_archive_checkpoint.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_archive_checkpoint.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
