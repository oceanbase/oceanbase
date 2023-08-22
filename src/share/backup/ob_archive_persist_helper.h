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

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_PERSIST_HELPER_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_PERSIST_HELPER_H_

#include "share/ob_define.h"
#include "share/ob_inner_kv_table_operator.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_archive_mode.h"

namespace oceanbase
{
namespace share
{

// Define one archive dest parameter item.
class ObArchiveDestParaItem : public ObInnerKVItem
{
public:
  explicit ObArchiveDestParaItem(ObInnerKVItemValue *value);
  virtual ~ObArchiveDestParaItem() {}

  int set_dest_no(const int64_t dest_no);
  int64_t get_dest_no() const;

  // Return if primary key is valid.
  bool is_pkey_valid() const override;
  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  TO_STRING_KV(K_(dest_no), K_(name), K_(*value));

private:
  int64_t dest_no_;
};




class ObArchivePersistHelper final : public ObIExecTenantIdProvider
{
public:
  ObArchivePersistHelper();
  virtual ~ObArchivePersistHelper() {}

  uint64_t get_exec_tenant_id() const override;

  int init(const uint64_t tenant_id);

  int get_archive_mode(
      common::ObISQLClient &proxy, ObArchiveMode &mode) const;

  int open_archive_mode(common::ObISQLClient &proxy) const;

  int close_archive_mode(common::ObISQLClient &proxy) const;

  // lock LOG_ARCHIVE_DEST
  int lock_archive_dest(common::ObISQLClient &trans, const int64_t dest_no, bool &is_exist) const;

  // Get archive path.
  int get_archive_dest(
      common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no, 
      ObBackupPathString &path) const;

  int get_dest_id(
      common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no, 
      int64_t &dest_id) const;
  
  int get_piece_switch_interval(
      common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no, 
      int64_t &piece_switch_interval) const;

  int get_binding(common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no, 
      ObLogArchiveDestAtrr::Binding &binding) const;
    

  int get_dest_state(common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no,
      ObLogArchiveDestState &state) const;

  int set_kv_item(
      common::ObISQLClient &proxy, const int64_t dest_no, const common::ObSqlString &name, 
      const common::ObSqlString &value) const;

  int del_dest(common::ObISQLClient &proxy, const int64_t dest_no);
  
  int set_dest_state(common::ObISQLClient &proxy, const int64_t dest_no,
      const ObLogArchiveDestState &state);

  int get_string_value(common::ObISQLClient &proxy, const int64_t dest_no, const bool need_lock, 
      const common::ObString &name, common::ObSqlString &value) const;

  // Get all <dest_no, dest_id> dest pairs that archive dest channels are using no matter its archive state is STOP or not.
  int get_valid_dest_pairs(common::ObISQLClient &proxy, common::ObIArray<std::pair<int64_t, int64_t>> &pair_array) const;

  // Get all <dest_no, backup_dest> dest pairs that archive dest channels are using no matter its archive state is STOP or not.
  int get_valid_dest_pairs(common::ObISQLClient &proxy, common::ObIArray<std::pair<int64_t, ObBackupPathString>> &pair_array) const;

  // dest round operation
  int get_round(common::ObISQLClient &proxy, const int64_t dest_no,
      const bool need_lock, ObTenantArchiveRoundAttr &round) const;
  int clean_round_comment(common::ObISQLClient &proxy, const int64_t dest_no) const;
  int get_round_by_dest_id(common::ObISQLClient &proxy, const int64_t dest_id,
      const bool need_lock, ObTenantArchiveRoundAttr &round) const;
  int del_round(common::ObISQLClient &proxy, const int64_t dest_no) const;
  int start_new_round(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &new_round) const;
  int switch_round_state_to(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &round,
      const ObArchiveRoundState &new_state) const;
  int switch_round_state_to(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round) const;
  int get_all_active_rounds(common::ObISQLClient &proxy, common::ObIArray<ObTenantArchiveRoundAttr> &rounds) const;
  int stop_round(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &round) const;


  // his round operation
  int get_his_round(common::ObISQLClient &proxy, const int64_t dest_no,
      const int64_t round_id, ObTenantArchiveHisRoundAttr &his_round) const;
  int insert_his_round(common::ObISQLClient &proxy, const ObTenantArchiveHisRoundAttr &his_round) const;


  // piece operation
  int get_piece(common::ObISQLClient &proxy, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const bool need_lock,
      ObTenantArchivePieceAttr &piece) const;
  int get_piece(common::ObISQLClient &proxy, const int64_t dest_id,
      const int64_t piece_id, const bool need_lock, ObTenantArchivePieceAttr &piece) const;
  int get_pieces(common::ObISQLClient &proxy, const int64_t dest_id, common::ObIArray<ObTenantArchivePieceAttr> &piece_list) const;
  // Get all frozen pieces whose piece ids are smaller than `upper_piece_id`.
  int get_frozen_pieces(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t upper_piece_id, 
      common::ObIArray<ObTenantArchivePieceAttr> &piece_list) const;
  int get_candidate_obsolete_backup_pieces(common::ObISQLClient &proxy, const SCN &end_scn,
      const char *backup_dest_str, ObIArray<ObTenantArchivePieceAttr> &pieces) const;
  int insert_or_update_piece(common::ObISQLClient &proxy, const ObTenantArchivePieceAttr &piece) const;
  // Usually, we need do it in a transaction.
  int batch_update_pieces(common::ObISQLClient &proxy, const common::ObIArray<ObTenantArchivePieceAttr> &pieces_array) const;
  int mark_new_piece_file_status(common::ObISQLClient &proxy, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const ObBackupFileStatus::STATUS new_status) const;


  // ls round progress operation
  // get ls archive piece with max piece id for that ls
  int get_latest_ls_archive_progress(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
      const ObLSID &id, ObLSArchivePersistInfo &info, bool &record_exist) const;
  int insert_ls_archive_progress(common::ObISQLClient &proxy, const ObLSArchivePersistInfo &info, int64_t &affected_rows) const;
  // set all ls archive piece to 'STOP'
  int set_ls_archive_stop(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
      const ObLSID &id, int64_t &affected_rows) const;
  int set_ls_archive_suspend(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
      const ObLSID &id, int64_t &affected_rows) const;
  int set_ls_archive_doing(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
      const ObLSID &id, int64_t &affected_rows) const;


  // Returned log stream must with the same input round_id, or with 0 round_id which indicates
  // that it has not started archived yet and piece_id bigger or equal than input 'since_piece_id'.
  int get_dest_round_summary(common::ObISQLClient &proxy, const int64_t dest_id,
      const int64_t round_id, const int64_t since_piece_id, ObDestRoundSummary &summary) const;
  int get_piece_by_scn(common::ObISQLClient &proxy, const int64_t dest_id,
      const share::SCN &scn, ObTenantArchivePieceAttr &piece) const;
  int get_pieces_by_range(common::ObISQLClient &proxy, const int64_t dest_id,
      const int64_t start_piece_id, const int64_t end_piece_id, ObIArray<ObTenantArchivePieceAttr> &pieces) const;

private:
  int parse_round_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObTenantArchiveRoundAttr> &rounds) const;
  int parse_piece_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObTenantArchivePieceAttr> &pieces) const;
  int parse_dest_round_summary_result_(sqlclient::ObMySQLResult &result, ObDestRoundSummary &summary) const;
  int do_parse_ls_archive_piece_summary_result_(sqlclient::ObMySQLResult &result, ObArchiveLSPieceSummary &piece) const;

  int parse_dest_pair_result_(sqlclient::ObMySQLResult &result, common::ObIArray<std::pair<int64_t, int64_t>> &pair_array) const;
  int parse_dest_pair_result_(sqlclient::ObMySQLResult &result, common::ObIArray<std::pair<int64_t, ObBackupPathString>> &pair_array) const;
  int do_parse_dest_pair_(sqlclient::ObMySQLResult &result, std::pair<int64_t, int64_t> &pair) const;
  int do_parse_dest_pair_(sqlclient::ObMySQLResult &result, std::pair<int64_t, ObBackupPathString> &pair) const;

  bool is_inited_;
  uint64_t tenant_id_; // sys or user tenant id

  DISALLOW_COPY_AND_ASSIGN(ObArchivePersistHelper);
};

}
}
#endif
