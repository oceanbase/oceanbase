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

#ifndef OCEANBASE_UNITTEST_STORAGE_BACKUP_TEST_BACKUP_CLEAN_SELECTOR_COMMON_H_
#define OCEANBASE_UNITTEST_STORAGE_BACKUP_TEST_BACKUP_CLEAN_SELECTOR_COMMON_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <set>

#define private public
#define protected public

#include "deps/oblib/src/lib/ob_errno.h"
#include "deps/oblib/src/lib/oblog/ob_log.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "src/observer/ob_service.h"
#include "src/rootserver/backup/ob_backup_clean_selector.h"
#include "src/rootserver/backup/ob_backup_clean_scheduler.h"
#include "src/rootserver/backup/ob_backup_clean_scheduler.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "src/observer/ob_service.h"
#include "src/share/backup/ob_backup_helper.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "src/rootserver/backup/ob_backup_clean_selector.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace testing;

namespace oceanbase {
namespace rootserver {

// =================================================================================
// Common Mock Classes
// =================================================================================

class MockConnectivityChecker final : public rootserver::IObConnectivityChecker {
public:
  MockConnectivityChecker() = default;
  virtual ~MockConnectivityChecker() = default;
  void set_connectivity_result(int result) { connectivity_result_ = result; }
  int check_dest_connectivity(
      const share::ObBackupPathString &backup_dest_str,
      const share::ObBackupDestType::TYPE dest_type) override {
    return connectivity_result_;
  }

private:
  int connectivity_result_ = OB_SUCCESS;
  DISALLOW_COPY_AND_ASSIGN(MockConnectivityChecker);
};

class MockBackupDataProvider : public rootserver::IObBackupDataProvider {
public:
  struct MockDestInfo {
    share::ObBackupPathString backup_dest_str_;
    int64_t dest_id_;
    share::ObBackupDestType::TYPE dest_type_;

    MockDestInfo() : dest_id_(-1), dest_type_(share::ObBackupDestType::TYPE::DEST_TYPE_MAX) {}

    MockDestInfo(const char* dest_str, int64_t id, share::ObBackupDestType::TYPE type)
        : dest_id_(id), dest_type_(type) {
        backup_dest_str_.assign(dest_str);
    }

    TO_STRING_KV(K_(backup_dest_str), K_(dest_id), K_(dest_type));
  };

  MockBackupDataProvider() = default;
  virtual ~MockBackupDataProvider() = default;

  int set_backup_sets(const ObIArray<share::ObBackupSetFileDesc> &sets);
  int set_current_path(const share::ObBackupPathString &path);
  int set_dest_infos(const ObIArray<MockDestInfo> &dest_infos);
  void set_policy_exist(bool exist) { policy_exist_ = exist; }

  // Interface implementations
  int get_one_backup_set_file(const int64_t id, const uint64_t tid,
                              share::ObBackupSetFileDesc &desc) override;
  int get_backup_set_files_specified_dest(
      const uint64_t tid, const int64_t did,
      ObIArray<share::ObBackupSetFileDesc> &infos) override;

  int get_latest_valid_full_backup_set(const uint64_t tid,
                                share::ObArchivePersistHelper &archive_helper,
                                 const share::ObBackupPathString &path,
                                 share::ObBackupSetFileDesc &desc) override;
  int get_oldest_full_backup_set(
      const uint64_t tenant_id,
      const share::ObBackupPathString &backup_path,
      share::ObBackupSetFileDesc &oldest_backup_desc) override;
  int get_backup_dest(const uint64_t tid,
                      share::ObBackupPathString &path) override;
  int get_dest_id(const uint64_t tenant_id,
                  const share::ObBackupDest &backup_dest,
                  int64_t &dest_id) override;
  int get_dest_type(const uint64_t tenant_id,
                    const share::ObBackupDest &backup_dest,
                    share::ObBackupDestType::TYPE &dest_type) override;
  int get_candidate_obsolete_backup_sets(
      const uint64_t tenant_id,
      const int64_t expired_time,
      const char *backup_path_str,
      common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos) override;
  int is_delete_policy_exist(const uint64_t tenant_id, bool &exist) override;

  void set_piece_info_descs(const ObIArray<share::ObPieceInfoDesc> &piece_info_descs) {
    piece_info_descs_.reset();
    for (int64_t i = 0; i < piece_info_descs.count(); ++i) {
      piece_info_descs_.push_back(piece_info_descs.at(i));
    }
  }

  int load_piece_info_desc(
      const uint64_t tenant_id,
      const share::ObTenantArchivePieceAttr &piece_attr,
      share::ObPieceInfoDesc &piece_info_desc) override;

private:
  bool policy_exist_ = false;
  ObArray<share::ObBackupSetFileDesc> backup_sets_;
  share::ObBackupPathString current_path_;
  ObArray<MockDestInfo> dest_infos_;
  ObArray<ObPieceInfoDesc> piece_info_descs_;
  DISALLOW_COPY_AND_ASSIGN(MockBackupDataProvider);
};

class MockArchivePersistHelper final : public share::ObArchivePersistHelper {
public:
    MockArchivePersistHelper() = default;
    virtual ~MockArchivePersistHelper() = default;

    int set_valid_dest_pairs(const ObIArray<std::pair<int64_t, int64_t>> &pairs) {
        valid_dest_pairs_.reset();
        for (int64_t i = 0; i < pairs.count(); ++i) {
            valid_dest_pairs_.push_back(pairs.at(i));
        }
        return OB_SUCCESS;
    }
    int set_all_pieces(const ObIArray<share::ObTenantArchivePieceAttr> &pieces) {
        all_pieces_.reset();
        for (int64_t i = 0; i < pieces.count(); ++i) {
            all_pieces_.push_back(pieces.at(i));
        }
        return OB_SUCCESS;
    }

    int init(const uint64_t tenant_id) {
        UNUSED(tenant_id);
        return OB_SUCCESS;
    }

    int get_valid_dest_pairs(common::ObISQLClient &proxy, common::ObIArray<std::pair<int64_t, int64_t>> &pair_array) const override {
        UNUSED(proxy);
        pair_array.reset();
        for (int64_t i = 0; i < valid_dest_pairs_.count(); ++i) {
            pair_array.push_back(valid_dest_pairs_.at(i));
        }
        return OB_SUCCESS;
    }

    int get_pieces(common::ObISQLClient &proxy, const int64_t dest_id, common::ObIArray<share::ObTenantArchivePieceAttr> &piece_list) const override {
        UNUSED(proxy);
        piece_list.reset();
        for (int64_t i = 0; i < all_pieces_.count(); ++i) {
            if (all_pieces_.at(i).key_.dest_id_ == dest_id) {
                piece_list.push_back(all_pieces_.at(i));
            }
        }
        return OB_SUCCESS;
    }

    int get_piece(common::ObISQLClient &proxy, const int64_t piece_id, const bool need_lock, share::ObTenantArchivePieceAttr &piece_desc) const override {
        UNUSED(proxy);
        for (int64_t i = 0; i < all_pieces_.count(); ++i) {
            if (all_pieces_.at(i).key_.piece_id_ == piece_id) {
                piece_desc = all_pieces_.at(i);
                return OB_SUCCESS;
            }
        }
        return OB_ENTRY_NOT_EXIST;
    }

    uint64_t get_exec_tenant_id() const override {
        return 1002; // Return test tenant_id
    }

    void reset() {
        valid_dest_pairs_.reset();
        all_pieces_.reset();
    }

private:
    ObArray<std::pair<int64_t, int64_t>> valid_dest_pairs_;
    ObArray<share::ObTenantArchivePieceAttr> all_pieces_;
    DISALLOW_COPY_AND_ASSIGN(MockArchivePersistHelper);
};

// =================================================================================
// Common Mock Dependency Classes
// =================================================================================

class MockObMySQLProxy : public ObMySQLProxy {
public:
  int init(sqlclient::ObISQLConnectionPool *pool) override { UNUSED(pool); return OB_SUCCESS; }
  void reset() {}
};

class MockObMultiVersionSchemaService : public share::schema::ObMultiVersionSchemaService {};
class MockObSrvRpcProxy : public obrpc::ObSrvRpcProxy {};
class MockObUserTenantBackupDeleteMgr : public ObUserTenantBackupDeleteMgr {};

// Mock class implementation
int MockBackupDataProvider::set_backup_sets(
    const ObIArray<share::ObBackupSetFileDesc> &sets) {
  backup_sets_.reset();
  for (int64_t i = 0; i < sets.count(); ++i) {
    backup_sets_.push_back(sets.at(i));
  }
  return OB_SUCCESS;
}

int MockBackupDataProvider::set_current_path(
    const share::ObBackupPathString &path) {
  current_path_.reset();
  current_path_.assign(path);
  return OB_SUCCESS;
}

int MockBackupDataProvider::set_dest_infos(const ObIArray<MockDestInfo> &dest_infos) {
    dest_infos_.reset();
    for (int64_t i = 0; i < dest_infos.count(); ++i) {
        dest_infos_.push_back(dest_infos.at(i));
    }
    return OB_SUCCESS;
}

int MockBackupDataProvider::get_one_backup_set_file(const int64_t id, const uint64_t tid, share::ObBackupSetFileDesc &desc) {
    UNUSED(tid);
    for (int64_t i = 0; i < backup_sets_.count(); ++i) {
        if (backup_sets_.at(i).backup_set_id_ == id) {
            desc = backup_sets_.at(i);
            return OB_SUCCESS;
        }
    }
    return OB_ENTRY_NOT_EXIST;
}


int MockBackupDataProvider::get_backup_set_files_specified_dest(
    const uint64_t tid, const int64_t did,
    ObIArray<share::ObBackupSetFileDesc> &infos) {
  int ret = OB_SUCCESS;
  infos.reset();

  for (int64_t i = 0; i < backup_sets_.count(); ++i) {
    if (backup_sets_.at(i).dest_id_ == did) {
      if (OB_FAIL(infos.push_back(backup_sets_.at(i)))) {
        break;
      }
    }
  }

  return ret;
}

int MockBackupDataProvider::get_oldest_full_backup_set(const uint64_t tenant_id, const share::ObBackupPathString &backup_path, share::ObBackupSetFileDesc &oldest_backup_desc) {
    UNUSED(tenant_id);
    UNUSED(backup_path);
    // Simple implementation: return the first full backup set
    for (int64_t i = 0; i < backup_sets_.count(); ++i) {
        if (backup_sets_.at(i).backup_type_.type_ == share::ObBackupType::BackupType::FULL_BACKUP) {
            oldest_backup_desc = backup_sets_.at(i);
            return OB_SUCCESS;
        }
    }
    return OB_ENTRY_NOT_EXIST;
}

int MockBackupDataProvider::get_latest_valid_full_backup_set(
    const uint64_t tid,
    share::ObArchivePersistHelper &archive_helper,
    const share::ObBackupPathString &path,
    share::ObBackupSetFileDesc &desc) {
  int ret = OB_SUCCESS;
  int64_t latest_id = -1;
  UNUSED(tid);
  UNUSED(archive_helper);

  for (int64_t i = 0; i < backup_sets_.count(); ++i) {
    const auto &set = backup_sets_.at(i);
    if (set.backup_path_ == path
        && ObBackupType::FULL_BACKUP == set.backup_type_.type_
        && set.backup_set_id_ > latest_id) {
      latest_id = set.backup_set_id_;
      desc = set;
    }
  }

  if (-1 == latest_id) {
    ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
  }

  return ret;
}

int MockBackupDataProvider::get_backup_dest(const uint64_t tid,
                                            share::ObBackupPathString &path) {
    UNUSED(tid);
    path = current_path_;
    return OB_SUCCESS;
}

int MockBackupDataProvider::get_dest_id(const uint64_t tenant_id,
                                        const share::ObBackupDest &backup_dest,
                                        int64_t &dest_id) {
    UNUSED(tenant_id);
    UNUSED(backup_dest);
    // Simple implementation: return the first dest_info's dest_id
    if (dest_infos_.count() > 0) {
        dest_id = dest_infos_.at(0).dest_id_;
        return OB_SUCCESS;
    }
    return OB_ENTRY_NOT_EXIST;
}

int MockBackupDataProvider::get_dest_type(const uint64_t tenant_id,
                                          const share::ObBackupDest &backup_dest,
                                          share::ObBackupDestType::TYPE &dest_type) {
    UNUSED(tenant_id);
    UNUSED(backup_dest);
    // Simple implementation: return the first dest_info's dest_type
    if (dest_infos_.count() > 0) {
        dest_type = dest_infos_.at(0).dest_type_;
        return OB_SUCCESS;
    }
    return OB_ENTRY_NOT_EXIST;
}

int MockBackupDataProvider::get_candidate_obsolete_backup_sets(
    const uint64_t tenant_id,
    const int64_t expired_time,
    const char *backup_path_str,
    common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos) {
  UNUSED(tenant_id);
  backup_set_infos.reset();
  for (int64_t i = 0; i < backup_sets_.count(); ++i) {
    if (backup_sets_.at(i).backup_path_ == backup_path_str
        && backup_sets_.at(i).end_time_ < expired_time) {
      backup_set_infos.push_back(backup_sets_.at(i));
    }
  }
  return OB_SUCCESS;
}

int MockBackupDataProvider::is_delete_policy_exist(const uint64_t tenant_id, bool &exist) {
  exist = policy_exist_;
  return OB_SUCCESS;
}

// Mock implementation of load_piece_info_desc
int MockBackupDataProvider::load_piece_info_desc(const uint64_t tenant_id,
                                      const share::ObTenantArchivePieceAttr &piece_attr,
                                      share::ObPieceInfoDesc &piece_info_desc) {
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  bool match_flag = false;
  for (int64_t i = 0; i < piece_info_descs_.count(); ++i) {
    printf("piece_id: %ld,: %ld\n", piece_info_descs_.at(i).piece_id_, piece_attr.key_.piece_id_);
    if (piece_info_descs_.at(i).piece_id_ == piece_attr.key_.piece_id_) {
      piece_info_desc = piece_info_descs_.at(i);
      match_flag = true;
    }
  }
  if (!match_flag) {
    return OB_ENTRY_NOT_EXIST;
  }
  return OB_SUCCESS;
}


} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_BACKUP_TEST_BACKUP_CLEAN_SELECTOR_COMMON_H_