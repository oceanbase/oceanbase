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

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_CHECKPOINT_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_CHECKPOINT_H_

#include "lib/function/ob_function.h"
#include "share/backup/ob_archive_struct.h"

namespace oceanbase
{
namespace share
{


class ObArchiveRoundHandler;

class ObDestRoundCheckpointer
{
public:
  struct GeneratedLSPiece
  {
    ObLSID ls_id_;
    SCN start_scn_;
    SCN checkpoint_scn_;
    uint64_t min_lsn_;
    uint64_t max_lsn_;
    int64_t input_bytes_;
    int64_t output_bytes_;
    bool is_ls_deleted_;

    TO_STRING_KV(K_(ls_id), K_(start_scn), K_(checkpoint_scn), K_(min_lsn), K_(max_lsn), 
      K_(input_bytes), K_(output_bytes), K_(is_ls_deleted));
  };

  struct GeneratedPiece
  {
    ObTenantArchivePieceAttr piece_info_;
    common::ObArray<GeneratedLSPiece> ls_piece_list_;

    TO_STRING_KV(K_(piece_info), K_(ls_piece_list));
  };

  struct Result 
  {
    ObTenantArchiveRoundAttr new_round_info_;
    common::ObArray<GeneratedPiece> piece_list_;

    TO_STRING_KV(K_(new_round_info), K_(piece_list));
  };

  typedef common::ObFunction<int(common::ObISQLClient *, const ObTenantArchiveRoundAttr &, const Result &, const GeneratedPiece &)> PieceGeneratedCb;
  typedef common::ObFunction<int(common::ObISQLClient *, const ObTenantArchiveRoundAttr &, const ObTenantArchiveRoundAttr &)> RoundCheckpointCb;

  ObDestRoundCheckpointer() : is_inited_(false), allow_force_stop_(false), round_handler_(nullptr), max_checkpoint_scn_() {}

  int init(ObArchiveRoundHandler *round_handler, const PieceGeneratedCb &piece_generated_cb, 
      const RoundCheckpointCb &round_checkpoint_cb, const SCN &max_checkpoint_scn);
  void set_allow_force_stop();

  // This operation is allowed only if dest round is in BEGINNING/DOING/STOPPING state.
  int checkpoint(const ObTenantArchiveRoundAttr &round_info, const ObDestRoundSummary &summary);

private:
  struct Counter
  {
    int64_t ls_count_;
    int64_t deleted_ls_count_;
    // Count of log streams which has not started archive yet.
    int64_t not_start_cnt_;
    // Count of log streams whose archive state are in INTERRUPTED.
    int64_t interrupted_cnt_;
    // Count of log streams whose archive state are in DOING.
    int64_t doing_cnt_;
    // Count of log streams whose archive state are in STOP.
    int64_t stopped_cnt_;
    // Count of log streams whose archive state are in SUSPEND.
    int64_t suspended_cnt_;

    // First interrupt ls id.
    ObLSID interrupted_ls_id_;
    // The fastest log stream archived scn.
    SCN max_scn_;
    // The slowest log stream archived scn.
    SCN checkpoint_scn_;
    // Piece id which all log stream have generated. It is the max common piece id
    // for them.
    int64_t max_active_piece_id_;

    Counter();
    void reset();

    TO_STRING_KV(K_(ls_count), K_(deleted_ls_count), K_(not_start_cnt), K_(interrupted_cnt), 
      K_(doing_cnt), K_(stopped_cnt), K_(interrupted_ls_id) ,K_(max_scn), K_(checkpoint_scn), K_(max_active_piece_id));
  };

  bool can_do_checkpoint_(const ObTenantArchiveRoundAttr &round_info) const;

  int count_(const ObTenantArchiveRoundAttr &old_round_info, const ObDestRoundSummary &summary, Counter &counter) const;
  int calc_next_checkpoint_scn_(
      const ObTenantArchiveRoundAttr &old_round_info,
      const ObDestRoundSummary &summary,
      const Counter &counter,
      SCN &next_checkpoint_scn) const;
  int gen_new_round_info_(
      const ObTenantArchiveRoundAttr &old_round_info,
      const ObDestRoundSummary &summary,
      const Counter &counter,
      ObTenantArchiveRoundAttr &new_round_info,
      bool &need_checkpoint) const;

  // do checkpoint to checkpoint_ts.
  int checkpoint_(const ObTenantArchiveRoundAttr &round_info, const ObDestRoundSummary &summary,
      Result &result) const;

  int generate_pieces_(const ObTenantArchiveRoundAttr &old_round_info, const ObDestRoundSummary &summary, 
      Result &result) const;
  
  int generate_one_piece_(const ObTenantArchiveRoundAttr &old_round_info, const ObTenantArchiveRoundAttr &new_round_info, const ObDestRoundSummary &summary, 
      const int64_t piece_id, GeneratedPiece &piece) const;
  
  int fill_generated_pieces_(const Result &result, common::ObIArray<ObTenantArchivePieceAttr> &pieces) const;

  bool is_inited_;
  bool allow_force_stop_;
  ObArchiveRoundHandler *round_handler_;
  SCN max_checkpoint_scn_;
  PieceGeneratedCb piece_generated_cb_;
  RoundCheckpointCb round_checkpoint_cb_;
  DISALLOW_COPY_AND_ASSIGN(ObDestRoundCheckpointer);
};


}
}
#endif
