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

#ifndef FAKE_ARCHIVE_PIECE_CONTEXT_H_
#define FAKE_ARCHIVE_PIECE_CONTEXT_H_
#include "lib/container/ob_se_array.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include <cstdint>
#define private public
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#undef private
namespace oceanbase
{
namespace unittest
{

enum class FakeRoundState
{
  INVALID = 0,
  STOP = 1,
  ACTIVE = 2,
};
enum class FakePieceState
{
  INVALID = 0,
  FRONZEN = 1,
  ACTIVE = 2,
  EMPTY = 3,
};

struct FakePieceComponent
{
  FakePieceComponent();
  FakePieceState state_;
  int64_t piece_id_;
  int64_t min_file_id_;
  int64_t max_file_id_;
  palf::LSN min_lsn_;
  palf::LSN max_lsn_;
  int assgin(const FakePieceComponent &other);
  TO_STRING_KV(K(state_), K(piece_id_), K(min_file_id_), K(max_file_id_), K(min_lsn_), K(max_lsn_));
};

typedef ObSEArray<FakePieceComponent, 8> PieceArray;
struct FakeArchiveComponent
{
  FakeArchiveComponent();
  FakeRoundState state_;
  int64_t round_id_;
  share::SCN start_scn_;
  share::SCN end_scn_;
  int64_t base_piece_id_;
  share::SCN base_piece_scn_;
  int64_t piece_switch_interval_;
  int64_t min_piece_id_;
  int64_t max_piece_id_;
  PieceArray array_;
  int get_piece(const int64_t piece_id, FakePieceComponent *&piece);
  int assgin(const FakeArchiveComponent &other);
  TO_STRING_KV(K(round_id_), K(start_scn_), K(end_scn_), K(min_piece_id_), K(max_piece_id_));
};

typedef ObSEArray<FakeArchiveComponent, 8> RoundArray;
struct FakeRounds
{
  RoundArray array_;
  int get_round(const int64_t round_id, FakeArchiveComponent *&component);
};

class FakeArchivePieceContext : public logservice::ObLogArchivePieceContext
{
public:
  int init(const share::ObLSID &id, FakeRounds *rounds);
private:
  virtual int load_archive_meta_() override;
  virtual int get_round_(const share::SCN &start_scn) override;
  virtual int get_round_range_() override;
  virtual int load_round_(const int64_t round_id, RoundContext &round_context, bool &exist) override;
  virtual int get_round_piece_range_(const int64_t round_id, int64_t &min_piece_id, int64_t &max_piece_id) override;
  virtual int check_round_exist_(const int64_t round_id, bool &exist) override;
  virtual int get_piece_meta_info_(const int64_t piece_id) override;
  virtual int get_piece_file_range_() override;
  virtual int get_min_lsn_in_piece_() override;

  FakeRounds *rounds_;
};
}
}
#endif
