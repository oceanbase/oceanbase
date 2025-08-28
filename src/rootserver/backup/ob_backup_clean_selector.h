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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_CLEAN_SELECTOR_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_CLEAN_SELECTOR_H_

#include "share/backup/ob_backup_clean_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/ob_srv_rpc_proxy.h"
#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_store.h"
#include "share/backup/ob_archive_store.h"

namespace oceanbase
{
namespace rootserver
{

class ObUserTenantBackupDeleteMgr;


// Abstracting static functions into the IObBackupDataProvider interface, allowing us to isolate the component
// under test by replacing real database calls with mock objects.
class IObBackupDataProvider {
public:
  virtual ~IObBackupDataProvider() {}
  // Method to get a single backup set file description
  virtual int get_one_backup_set_file(
      const int64_t backup_set_id,
      const uint64_t tenant_id,
      share::ObBackupSetFileDesc &backup_set_desc) = 0;

  // Method to get all backup set files for a specific destination
  virtual int get_backup_set_files_specified_dest(
      const uint64_t tenant_id,
      const int64_t dest_id,
      common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos) = 0;

  // Method to get the latest full backup set for a specific path
  virtual int get_latest_full_backup_set(
      const uint64_t tenant_id,
      const share::ObBackupPathString &backup_path,
      share::ObBackupSetFileDesc &latest_backup_desc) = 0;

  // Method to get the oldest full backup set for a specific path
  virtual int get_oldest_full_backup_set(
      const uint64_t tenant_id,
      const share::ObBackupPathString &backup_path,
      share::ObBackupSetFileDesc &oldest_backup_desc) = 0;

  // Method to get the current backup destination path
  virtual int get_backup_dest(
      const uint64_t tenant_id,
      share::ObBackupPathString &path) = 0;

  // Method to get dest_id by backup_dest
  virtual int get_dest_id(
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      int64_t &dest_id) = 0;

  // Method to get dest_type by backup_dest
  virtual int get_dest_type(
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      share::ObBackupDestType::TYPE &dest_type) = 0;

  virtual int is_delete_policy_exist(const uint64_t tenant_id, bool &is_delete_policy_exist) = 0;

  virtual int get_candidate_obsolete_backup_sets(
      const uint64_t tenant_id,
      const int64_t expired_time,
      const char *backup_path_str,
      common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos) = 0;

  virtual int load_piece_info_desc(
      const uint64_t tenant_id,
      const share::ObTenantArchivePieceAttr &piece_attr,
      share::ObPieceInfoDesc &piece_info_desc) = 0;
};

class ObBackupDataProvider final : public IObBackupDataProvider {
public:
  ObBackupDataProvider();
  virtual ~ObBackupDataProvider() = default;

  int init(common::ObMySQLProxy &sql_proxy);

  // Implement interface methods by calling the real static functions
  int get_one_backup_set_file(
      const int64_t backup_set_id,
      const uint64_t tenant_id,
      share::ObBackupSetFileDesc &backup_set_desc) override;

  int get_backup_set_files_specified_dest(
      const uint64_t tenant_id,
      const int64_t dest_id,
      common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos) override;

  int get_oldest_full_backup_set(
      const uint64_t tenant_id,
      const share::ObBackupPathString &backup_path,
      share::ObBackupSetFileDesc &oldest_backup_desc) override;

  int get_latest_full_backup_set(
      const uint64_t tenant_id,
      const share::ObBackupPathString &backup_path,
      share::ObBackupSetFileDesc &latest_backup_desc) override;

  int get_backup_dest(
      const uint64_t tenant_id,
      share::ObBackupPathString &path) override;

  int get_dest_id(
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      int64_t &dest_id) override;

  int get_dest_type(
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      share::ObBackupDestType::TYPE &dest_type) override;

  int is_delete_policy_exist(const uint64_t tenant_id, bool &is_delete_policy_exist) override;

  int get_candidate_obsolete_backup_sets(
      const uint64_t tenant_id,
      const int64_t expired_time,
      const char *backup_path_str,
      common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos) override;

  int load_piece_info_desc(
      const uint64_t tenant_id,
      const share::ObTenantArchivePieceAttr &piece_attr,
      share::ObPieceInfoDesc &piece_info_desc) override;

private:
  common::ObISQLClient *sql_proxy_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataProvider);
};


// Interface for connectivity checking, We use this interface to isolate the component under test by replacing real database calls with mock objects.

class IObConnectivityChecker {
public:
  virtual ~IObConnectivityChecker() = default;
  virtual int check_dest_connectivity(const share::ObBackupPathString &backup_dest_str,
                                      const share::ObBackupDestType::TYPE dest_type) = 0;
};

// Concrete implementation of connectivity checker
class ObConnectivityChecker final : public IObConnectivityChecker {
public:
  ObConnectivityChecker();
  virtual ~ObConnectivityChecker() = default;

  int init(common::ObMySQLProxy &sql_proxy,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           const uint64_t tenant_id);

  int check_dest_connectivity(const share::ObBackupPathString &backup_dest_str,
                              const share::ObBackupDestType::TYPE dest_type) override;

private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObConnectivityChecker);
};

class ObBackupDeleteSelector final {
public:
  static const int64_t INVALID_CLEAN_ID = -1;

  ObBackupDeleteSelector();
  ~ObBackupDeleteSelector();

  // Internal structure for grouping data by backup destination
  template<typename T>
  struct GenericBackupGroup {
    GenericBackupGroup() : path_(), candidates_() {}
    ~GenericBackupGroup() = default;

    void reset() {
      path_.reset();
      candidates_.reset();
    }

    TO_STRING_KV(K_(path), K_(candidates));

    share::ObBackupPathString path_;
    ObArray<T> candidates_;          // array of candidates (either sets or pieces)
  };

  // Structure to hold failure information for backup operations
  struct BackupCleanFailureInfo {
    BackupCleanFailureInfo() : failure_id(INVALID_CLEAN_ID), related_id(INVALID_CLEAN_ID) {}
    ~BackupCleanFailureInfo() = default;

    void reset() {
      failure_id = INVALID_CLEAN_ID;
      failure_reason.reset();
      related_id = INVALID_CLEAN_ID;
    }

    TO_STRING_KV(K(failure_id), K(failure_reason), K(related_id));

    int64_t failure_id;
    ObString failure_reason;
    int64_t related_id;
  };

  // Type aliases for specific, readable use cases.
  using BackupGroupedSet = GenericBackupGroup<share::ObBackupSetFileDesc>;
  using BackupGroupedPiece = GenericBackupGroup<share::ObTenantArchivePieceAttr>;

  int init(common::ObMySQLProxy &sql_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObBackupCleanJobAttr &job_attr,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           ObUserTenantBackupDeleteMgr &delete_mgr);

  int get_delete_backup_set_infos(ObIArray<share::ObBackupSetFileDesc> &set_list);
  // manually delete piece
  int get_delete_backup_piece_infos(ObIArray<share::ObTenantArchivePieceAttr> &piece_list);
  // auto delete obsolete OR manually launch a delete obsolete
  int get_delete_obsolete_infos(ObIArray<share::ObBackupSetFileDesc> &set_list,
                         ObIArray<share::ObTenantArchivePieceAttr> &piece_list);
  // clear all file in one dest
  int get_delete_backup_all_infos(ObIArray<share::ObBackupSetFileDesc> &set_list, ObIArray<share::ObTenantArchivePieceAttr> &piece_list);

private:
  // ----------------------------Set deletion related methods----------------------------
  int init_user_requested_ids_set_(
      common::hash::ObHashSet<int64_t> &all_user_requested_ids_set) const;

  int get_candidate_backups_(
      BackupGroupedSet &candidate_data,
      BackupCleanFailureInfo &failure_info);

  int apply_current_path_retention_policy_(
      const BackupGroupedSet &candidate_data,
      BackupCleanFailureInfo &failure_info);

  int perform_dependency_check_(
      const common::hash::ObHashSet<int64_t> &requested_deletion_ids,
      const BackupGroupedSet &candidate_data,
      ObIArray<share::ObBackupSetFileDesc> &set_list,
      BackupCleanFailureInfo &failure_info);

  int is_backup_set_depended_on_(
    const int64_t candidate_backup_set_id,
    const int64_t current_dest_id,
    const ObArray<share::ObBackupSetFileDesc> &sets_in_same_dest,
    const hash::ObHashSet<int64_t> &requested_deletion_ids,
    bool &is_depended_on,
    int64_t &dependent_id);

  int add_failure_reason_(const BackupCleanFailureInfo &failure_info);
  // ----------------------------Piece deletion related methods----------------------------
  int get_candidate_pieces_(
    BackupGroupedPiece &candidate_data,
    BackupCleanFailureInfo &failure_info);

  int check_piece_path_valid_(
    const BackupGroupedPiece &candidate_data);

  int apply_current_path_piece_retention_policy_(
    const BackupGroupedPiece &candidate_data,
    BackupCleanFailureInfo &failure_info);

  int get_oldest_full_backup_set_(
    share::ObBackupSetFileDesc &oldest_full_backup_desc,
    BackupCleanFailureInfo &failure_info);

  int perform_piece_sequential_check_(
    BackupGroupedPiece &candidate_data,
    ObIArray<share::ObTenantArchivePieceAttr> &piece_list,
    BackupCleanFailureInfo &failure_info);

  int check_previous_pieces_are_deleted_(
    const ObIArray<share::ObTenantArchivePieceAttr> &pieces_in_same_dest,
    const int64_t &first_candidate_piece_id, int64_t &idx_of_same_dest_piece,
    BackupCleanFailureInfo &failure_info);

  int check_candidate_pieces_are_continuous_(
    const ObIArray<share::ObTenantArchivePieceAttr> &pieces_in_same_dest,
    const ObIArray<share::ObTenantArchivePieceAttr> &candidate_pieces,
    int64_t &idx_of_same_dest_piece, BackupCleanFailureInfo &failure_info);

  // ----------------------------Delete backup all related methods----------------------------
  int check_current_backup_dest_(BackupCleanFailureInfo &failure_info);
  int check_current_archive_dest_(BackupCleanFailureInfo &failure_info);

  int get_all_pieces_or_sets_in_dest_(
    ObIArray<share::ObBackupSetFileDesc> &set_list,
    ObIArray<share::ObTenantArchivePieceAttr> &piece_list);

  // ----------------------------Delete obsolete related methods----------------------------
  int get_obsolete_backup_set_infos_helper_(share::ObBackupSetFileDesc &first_full_backup_set,
                                            const char *backup_path_str,
                                            ObIArray<share::ObBackupSetFileDesc> &set_list);
  int get_delete_obsolete_backup_set_infos_(share::ObBackupSetFileDesc &clog_data_clean_point,
                                            ObIArray<share::ObBackupSetFileDesc> &set_list);
  int get_delete_obsolete_backup_piece_infos_(const share::ObBackupSetFileDesc &clog_data_clean_point,
                                              ObIArray<share::ObTenantArchivePieceAttr> &piece_list);

  int get_min_depended_piece_idx_(const ObIArray<share::ObTenantArchivePieceAttr> &candidate_piece_infos, int64_t &min_depended_pieces_idx);
  int get_min_depended_piece_idx_ls_(const ObIArray<share::ObTenantArchivePieceAttr> &candidate_piece_infos,
                                    const share::ObSingleLSInfoDesc &ls_info,
                                    const int64_t file_id,
                                    int64_t &min_depended_pieces_idx);
  int check_piece_dependency_(ObIArray<share::ObTenantArchivePieceAttr> &candidate_piece_infos);
  int get_delete_obsolete_backup_piece_infos_log_only_(int64_t expired_time,
                                            ObIArray<share::ObTenantArchivePieceAttr> &piece_list);

  int get_all_dest_backup_piece_infos_(const share::SCN &clog_data_clean_point,
                                      ObIArray<share::ObTenantArchivePieceAttr> &backup_piece_infos,
                                      const bool is_log_only);

  bool can_backup_pieces_be_deleted_(const share::ObArchivePieceStatus &status);
#ifdef ERRSIM
  int get_errsim_expired_parameter_(int64_t &expired_param_from_errsim);
#endif

  struct CompareBackupSetInfo
  {
    bool operator()(const share::ObBackupSetFileDesc &lhs,
                    const share::ObBackupSetFileDesc &rhs) const
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };
  struct CompareBackupPieceInfo
  {
    bool operator()(const share::ObTenantArchivePieceAttr &lhs,
                    const share::ObTenantArchivePieceAttr &rhs) const
    {
      return lhs.key_.piece_id_ < rhs.key_.piece_id_;
    }
  };

private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObBackupCleanJobAttr *job_attr_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObUserTenantBackupDeleteMgr *delete_mgr_;
  IObBackupDataProvider *data_provider_;
  IObConnectivityChecker *connectivity_checker_;
  share::ObArchivePersistHelper *archive_helper_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDeleteSelector);
};

}
}

#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_CLEAN_SELECTOR_H_
