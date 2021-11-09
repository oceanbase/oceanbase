// Copyright 2021 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#define USING_LOG_PREFIX SHARE

#include "share/backup/ob_multi_backup_dest_util.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_tenant_name_mgr.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "lib/restore/ob_storage.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace share {

bool ObCmpSetPathCopyIdSmaller::operator()(
    const share::ObSimpleBackupSetPath& lhs, const share::ObSimpleBackupSetPath& rhs)
{
  return lhs.copy_id_ < rhs.copy_id_;
}

bool ObCmpPiecePathCopyIdSmaller::operator()(
    const share::ObSimpleBackupPiecePath& lhs, const share::ObSimpleBackupPiecePath& rhs)
{
  return lhs.copy_id_ < rhs.copy_id_;
}

bool ObCmpBackupPieceInfoBackupPieceId::operator()(
    const share::ObBackupPieceInfo& lhs, const share::ObBackupPieceInfo& rhs)
{
  return lhs.key_.backup_piece_id_ < rhs.key_.backup_piece_id_;
}

bool ObCmpBackupSetInfoBackupSetId::operator()(
    const share::ObBackupSetFileInfo& lhs, const share::ObBackupSetFileInfo& rhs)
{
  return lhs.backup_set_id_ < rhs.backup_set_id_;
}

int ObMultiBackupDestUtil::parse_multi_uri(
    const common::ObString& multi_uri, common::ObArenaAllocator& allocator, common::ObArray<common::ObString>& uri_list)
{
  int ret = OB_SUCCESS;
  uri_list.reset();
  char *buf = NULL, *tok = NULL, *ptr = NULL;

  if (multi_uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(multi_uri));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(multi_uri.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", KR(ret));
  } else {
    MEMCPY(buf, multi_uri.ptr(), multi_uri.length());
    buf[multi_uri.length()] = '\0';
    tok = buf;
    for (char* str = tok; OB_SUCC(ret); str = NULL) {
      tok = ::strtok_r(str, ",", &ptr);
      if (OB_ISNULL(tok)) {
        break;
      } else if (OB_FAIL(uri_list.push_back(tok))) {
        LOG_WARN("failed to push back", KR(ret), K(tok));
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::check_all_path_is_same_type(
    const common::ObArray<common::ObString>& path_list, bool& is_same, ObMultiBackupPathType& type)
{
  int ret = OB_SUCCESS;
  is_same = true;
  type = BACKUP_PATH_MAX;
  ObMultiBackupPathType tmp_type = BACKUP_PATH_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); ++i) {
    ObBackupDest backup_dest;
    const ObString& path = path_list.at(i);
    if (OB_FAIL(backup_dest.set(path.ptr()))) {
      LOG_WARN("failed to set backup path", KR(ret));
    } else if (OB_FAIL(get_path_type(backup_dest.root_path_, backup_dest.storage_info_, type))) {
      LOG_WARN("failed to get path type", KR(ret), K(path));
    } else {
      if (BACKUP_PATH_MAX == tmp_type) {
        tmp_type = type;
      }
      if (type != tmp_type) {
        is_same = false;
        LOG_WARN("path type is not same", K(path), K(tmp_type), K(type), K(path_list));
        break;
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::get_backup_tenant_id(const common::ObArray<common::ObString>& url_list,
    const ObMultiBackupPathType& type, const common::ObString& cluster_name, const int64_t cluster_id,
    const common::ObString& tenant_name, const int64_t restore_timestamp, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  if (url_list.empty() || BACKUP_PATH_MAX == type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(type));
  } else if (BACKUP_PATH_CLUSTER_LEVEL == type) {
    if (1 != url_list.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support multi cluster level backup dest for now", KR(ret), K(url_list));
    } else {
      const common::ObString& url = url_list.at(0);
      if (OB_FAIL(inner_get_backup_tenant_id_from_tenant_name_info(
              cluster_name, cluster_id, url, tenant_name, restore_timestamp, tenant_id))) {
        LOG_WARN("failed to inner get backup tenant id", KR(ret), K(url), K(tenant_name), K(restore_timestamp));
      }
    }
  } else if (BACKUP_PATH_SINGLE_DIR == type) {
    if (OB_FAIL(inner_get_backup_tenant_id_from_set_or_piece(url_list, restore_timestamp, tenant_id))) {
      LOG_WARN("failed to inner get backup tenant id", KR(ret), K(url_list), K(tenant_name), K(restore_timestamp));
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::get_multi_backup_path_list(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t restore_timestamp,
    const common::ObArray<common::ObString>& list, common::ObArray<ObSimpleBackupSetPath>& set_list,
    common::ObArray<ObSimpleBackupPiecePath>& piece_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  piece_list.reset();
  ObStorageUtil util(false /*need_retry*/);
  if (OB_INVALID_ID == tenant_id || restore_timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(restore_timestamp));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      const ObString& backup_dest_str = list.at(i);
      int64_t snapshot_version = 0;
      int64_t start_replay_log_ts = 0;
      if (OB_FAIL(get_backup_set_list(is_preview,
              cluster_name,
              cluster_id,
              tenant_id,
              restore_timestamp,
              backup_dest_str,
              set_list,
              snapshot_version,
              start_replay_log_ts))) {
        LOG_WARN("failed to get backup set", KR(ret), K(tenant_id), K(restore_timestamp), K(list));
      } else if (OB_FAIL(get_backup_piece_list(is_preview,
                     cluster_name,
                     cluster_id,
                     tenant_id,
                     snapshot_version,
                     start_replay_log_ts,
                     restore_timestamp,
                     backup_dest_str,
                     piece_list))) {
        LOG_WARN("failed to get backup piece list", KR(ret), K(tenant_id), K(restore_timestamp), K(list));
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::filter_duplicate_path_list(
    common::ObArray<ObSimpleBackupSetPath>& set_list, common::ObArray<ObSimpleBackupPiecePath>& piece_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObSimpleBackupSetPath> tmp_set_list;
  ObArray<ObSimpleBackupPiecePath> tmp_piece_list;
  ObCmpSetPathCopyIdSmaller set_cmp;
  ObCmpPiecePathCopyIdSmaller piece_cmp;
  std::sort(set_list.begin(), set_list.end(), set_cmp);
  std::sort(piece_list.begin(), piece_list.end(), piece_cmp);

  for (int64_t i = 0; OB_SUCC(ret) && i < set_list.count(); ++i) {
    const ObSimpleBackupSetPath& path_i = set_list.at(i);
    bool exist = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < tmp_set_list.count(); ++j) {
      const ObSimpleBackupSetPath& path_j = set_list.at(j);
      if (path_i.backup_set_id_ == path_j.backup_set_id_) {
        exist = true;
        LOG_INFO("exist same path", K(path_i), K(path_j));
        break;
      }
    }
    if (OB_SUCC(ret) && !exist) {
      if (OB_FAIL(tmp_set_list.push_back(path_i))) {
        LOG_WARN("failed to push back simple path", KR(ret), K(path_i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_list.reset();
    if (OB_FAIL(set_list.assign(tmp_set_list))) {
      LOG_WARN("failed to assign list", KR(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); ++i) {
    const ObSimpleBackupPiecePath& path_i = piece_list.at(i);
    bool exist = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < tmp_piece_list.count(); ++j) {
      const ObSimpleBackupPiecePath& path_j = piece_list.at(j);
      if (path_i.backup_piece_id_ == path_j.backup_piece_id_) {
        exist = true;
        LOG_INFO("exist same path", K(path_i), K(path_j));
        break;
      }
    }
    if (OB_SUCC(ret) && !exist) {
      if (OB_FAIL(tmp_piece_list.push_back(path_i))) {
        LOG_WARN("failed to push back simple path", KR(ret), K(path_i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    piece_list.reset();
    if (OB_FAIL(piece_list.assign(tmp_piece_list))) {
      LOG_WARN("failed to assign list", KR(ret));
    }
  }

  return ret;
}

int ObMultiBackupDestUtil::check_multi_path_is_complete(const int64_t restore_timestamp,
    common::ObArray<share::ObBackupSetFileInfo>& set_info_list,
    common::ObArray<share::ObBackupPieceInfo>& piece_info_list, bool& is_complete)
{
  int ret = OB_SUCCESS;
  is_complete = false;
  bool last_backup_set_is_full = false;

  ObCmpBackupSetInfoBackupSetId cmp_set;
  ObCmpBackupPieceInfoBackupPieceId cmp_piece;
  std::sort(set_info_list.begin(), set_info_list.end(), cmp_set);
  std::sort(piece_info_list.begin(), piece_info_list.end(), cmp_piece);

  if (piece_info_list.empty() || set_info_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup set list or backup piece list is emtpy", KR(ret), K(piece_info_list), K(set_info_list));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "restore backup set list or backup piece list");
  }

  // check has duplicate backup set
  for (int64_t i = 0; OB_SUCC(ret) && i < set_info_list.count() - 1; ++i) {
    const ObBackupSetFileInfo& prev = set_info_list.at(i);
    const ObBackupSetFileInfo& next = set_info_list.at(i + 1);
    if (prev.backup_set_id_ == next.backup_set_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("has duplicate backup set id", KR(ret), K(set_info_list));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate backup set id");
    }
  }

  for (int64_t i = set_info_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupSetFileInfo& info = set_info_list.at(i);
    if (OB_SUCCESS != info.result_) {
      continue;
    } else if (ObBackupType::FULL_BACKUP == info.backup_type_.type_) {
      last_backup_set_is_full = true;
      break;
    } else {
      bool prev_inc_exist = false;
      const int64_t prev_inc_backup_set_id = info.prev_inc_backup_set_id_;
      for (int64_t j = i - 1; OB_SUCC(ret) && j >= 0; --j) {
        const ObBackupSetFileInfo& prev_info = set_info_list.at(j);
        if (prev_info.backup_set_id_ == prev_inc_backup_set_id) {
          prev_inc_exist = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !prev_inc_exist) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("backup set is not continuous", KR(ret), K(set_info_list));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "incomplete backup set list");
      }
    }
  }

  if (OB_SUCC(ret) && !last_backup_set_is_full) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("last backup set is not full backup set", KR(ret), K(set_info_list));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "incomplete backup set list");
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < piece_info_list.count() - 1; ++i) {
    const ObBackupPieceInfo& prev = piece_info_list.at(i);
    const ObBackupPieceInfo& next = piece_info_list.at(i + 1);
    if (prev.key_.round_id_ != next.key_.round_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup piece do not belong to same round", KR(ret), K(prev), K(next));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "not same round backup piece");
    } else if (prev.key_.backup_piece_id_ == next.key_.backup_piece_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("has duplicate backup piecen id", KR(ret), K(set_info_list));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate backup piece id");
    } else if (ObBackupPieceStatus::BACKUP_PIECE_FROZEN != prev.status_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup piece not frozen", KR(ret), K(prev));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "non frozen backup piece");
    } else if (next.key_.backup_piece_id_ - prev.key_.backup_piece_id_ != 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup piece is not continuous", KR(ret), K(prev), K(next), K(piece_info_list));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "incomplete backup piece list");
    }
  }

  if (OB_SUCC(ret)) {
    const ObBackupPieceInfo& smallest_piece = piece_info_list.at(0);
    const ObBackupPieceInfo& largest_piece = piece_info_list.at(piece_info_list.count() - 1);
    const ObBackupSetFileInfo& largest_set = set_info_list.at(set_info_list.count() - 1);

    if (restore_timestamp < largest_set.snapshot_version_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup set is too new", KR(ret), K(restore_timestamp), K(largest_set));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "backup set list, restore timestamp smaller than largest snapshot version");
    } else if (restore_timestamp > largest_piece.checkpoint_ts_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("piece not enough", KR(ret), K(restore_timestamp), K(largest_piece));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "backup piece list, restore timestamp larger than largest checkpoint ts");
    } else {
      if (smallest_piece.start_piece_id_ == smallest_piece.key_.backup_piece_id_) {
        // if smallest piece is the first piece in round
        // no need compare
      } else if (largest_set.start_replay_log_ts_ < smallest_piece.start_ts_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("cannot restore", KR(ret), K(largest_set), K(smallest_piece));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "backup set list, backup set start replay log ts less then piece start ts");
      }
      if (OB_SUCC(ret)) {
        is_complete = true;
      }
    }
  }

  return ret;
}

int ObMultiBackupDestUtil::get_backup_set_info_path(const common::ObString& user_path, ObBackupPath& backup_set_path)
{
  int ret = OB_SUCCESS;
  backup_set_path.reset();
  if (user_path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(user_path));
  } else if (OB_FAIL(backup_set_path.init(user_path))) {
    LOG_WARN("failed to init backup set path", KR(ret));
  } else if (OB_FAIL(backup_set_path.join(OB_STR_SINGLE_BACKUP_SET_INFO))) {
    LOG_WARN("failed to join backup set path", KR(ret));
  }
  return ret;
}

int ObMultiBackupDestUtil::get_backup_piece_info_path(
    const common::ObString& user_path, ObBackupPath& backup_piece_path)
{
  int ret = OB_SUCCESS;
  backup_piece_path.reset();
  if (user_path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(user_path));
  } else if (OB_FAIL(backup_piece_path.init(user_path))) {
    LOG_WARN("failed to init backup piece path", KR(ret));
  } else if (OB_FAIL(backup_piece_path.join(OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO))) {
    LOG_WARN("failed to join backup piece path", KR(ret));
  }
  return ret;
}

int ObMultiBackupDestUtil::get_path_type(
    const common::ObString& user_path, const common::ObString& storage_info, ObMultiBackupPathType& type)
{
  int ret = OB_SUCCESS;
  type = BACKUP_PATH_CLUSTER_LEVEL;
  bool set_exist = false;
  bool piece_exist = false;
  ObStorageUtil util(false /*need_retry*/);
  ObBackupPath backup_set_path, backup_piece_path;
  if (OB_FAIL(get_backup_set_info_path(user_path, backup_set_path))) {
    LOG_WARN("failed to get backup set info path", KR(ret), K(user_path));
  } else if (OB_FAIL(get_backup_piece_info_path(user_path, backup_piece_path))) {
    LOG_WARN("failed to get backup piece info path", KR(ret), K(user_path));
  } else if (OB_FAIL(util.is_exist(backup_set_path.get_obstr(), storage_info, set_exist))) {
    LOG_WARN("failed to check is exist", KR(ret), K(backup_set_path), K(storage_info));
  } else if (OB_FAIL(util.is_exist(backup_piece_path.get_obstr(), storage_info, piece_exist))) {
    LOG_WARN("failed to check is exist", KR(ret), K(backup_piece_path), K(storage_info));
  } else if (set_exist || piece_exist) {
    type = BACKUP_PATH_SINGLE_DIR;
  }
  return ret;
}

int ObMultiBackupDestUtil::inner_get_backup_tenant_id_from_set_or_piece(
    const common::ObArray<common::ObString>& path_list, const int64_t restore_timestamp, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t tmp_tenant_id = OB_INVALID_ID;
  ObStorageUtil util(false /*need retry*/);
  for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); ++i) {
    const ObString& path = path_list.at(i);
    bool is_set_path = false;
    bool is_piece_path = false;
    common::ObArenaAllocator allocator("MultiBackupDest");
    ObArray<ObString> file_names;
    ObSimpleBackupPiecePath simple_path;
    if (OB_FAIL(simple_path.set(path))) {
      LOG_WARN("failed to set simple path", KR(ret), K(path));
    } else if (OB_FAIL(util.list_files(
                   simple_path.get_simple_path(), simple_path.get_storage_info(), allocator, file_names))) {
      LOG_WARN("failed to list files", KR(ret), K(path));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < file_names.count(); ++j) {
        const ObString& file_name = file_names.at(j);
        if (file_name.prefix_match(OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO)) {
          is_piece_path = true;
        } else if (file_name.prefix_match(OB_STR_SINGLE_BACKUP_SET_INFO)) {
          is_set_path = true;
        }
      }
    }
    if (OB_SUCC(ret) && is_piece_path == is_set_path) {
      if (is_piece_path) {
        ret = OB_ERR_SYS;
        LOG_WARN("neither piece or set", KR(ret));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("do not exist info file", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_set_path) {
        if (FAILEDx(inner_get_backup_tenant_id_from_set_info(path, restore_timestamp, tenant_id))) {
          LOG_WARN("failed to inner get backup tenant id from set info", K(ret), K(path));
        }
      }
      if (is_piece_path) {
        if (FAILEDx(inner_get_backup_tenant_id_from_piece_info(path, restore_timestamp, tenant_id))) {
          LOG_WARN("failed to inner get backup tenant id from piece info", K(ret), K(path));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID == tmp_tenant_id) {
        tmp_tenant_id = tenant_id;
      }
      if (tmp_tenant_id != tenant_id) {
        ret = OB_ERR_SYS;
        LOG_ERROR("path is not belong to same tenant", K(ret), K(tmp_tenant_id), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::inner_get_backup_tenant_id_from_set_info(
    const common::ObString& url, const int64_t restore_timestamp, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ObSimpleBackupSetPath simple_path;
  ObFakeBackupLeaseService fake_lease;
  ObExternSingleBackupSetInfoMgr mgr;
  ObBackupSetFileInfo info;
  if (OB_FAIL(simple_path.set(url))) {
    LOG_WARN("failed to set simple path", KR(ret), K(url));
  } else if (OB_FAIL(mgr.init(simple_path, fake_lease))) {
    LOG_WARN("failed to init extern single backup set info mgr", KR(ret), K(simple_path));
  } else if (OB_FAIL(mgr.get_extern_backup_set_file_info(info))) {
    LOG_WARN("failed to get extern backup set file infos", KR(ret));
  } else {
    if (info.snapshot_version_ > restore_timestamp) {
      ret = OB_ERR_SYS;
      LOG_WARN("info is not correct", K(ret));
    } else {
      tenant_id = info.tenant_id_;
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::inner_get_backup_tenant_id_from_piece_info(
    const common::ObString& url, const int64_t restore_timestamp, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(restore_timestamp);
  tenant_id = OB_INVALID_ID;
  ObLogArchiveBackupInfoMgr mgr;
  ObBackupPath backup_path;
  ObSimpleBackupPiecePath simple_path;
  ObBackupPieceInfo piece_info;
  ObFakeBackupLeaseService fake_lease;
  if (OB_FAIL(simple_path.set(url))) {
    LOG_WARN("failed to init simple path", KR(ret), K(url));
  } else if (OB_FAIL(backup_path.init(simple_path.get_simple_path()))) {
    LOG_WARN("failed to init backup path", K(ret), K(url));
  } else if (OB_FAIL(backup_path.join(OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO))) {
    LOG_WARN("failed to join file name", KR(ret));
  } else if (OB_FAIL(mgr.read_external_single_backup_piece_info(
                 backup_path, simple_path.get_storage_info(), piece_info, fake_lease))) {
    LOG_WARN("failed to read external single backup piece info", K(ret));
  } else {
    tenant_id = piece_info.key_.tenant_id_;
  }
  return ret;
}

int ObMultiBackupDestUtil::inner_get_backup_tenant_id_from_tenant_name_info(const common::ObString& cluster_name,
    const int64_t cluster_id, const common::ObString& url, const common::ObString& tenant_name,
    const int64_t restore_timestamp, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ObClusterBackupDest cluster_backup_dest;
  ObTenantNameSimpleMgr mgr;
  if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init tenant name simple mgr", KR(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(url.ptr(), cluster_name.ptr(), cluster_id, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(url), K(cluster_name));
  } else if (OB_FAIL(mgr.read_backup_file(cluster_backup_dest))) {
    LOG_WARN("failed to read backup file", KR(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(mgr.get_tenant_id(tenant_name, restore_timestamp, tenant_id))) {
    LOG_WARN("failed to get tenant id", KR(ret), K(tenant_name), K(restore_timestamp));
  }
  return ret;
}

int ObMultiBackupDestUtil::get_cluster_backup_dest(const ObBackupDest& backup_dest, const char* cluster_name,
    const int64_t cluster_id, ObClusterBackupDest& cluster_backup_dest)
{
  int ret = OB_SUCCESS;
  cluster_backup_dest.reset();
  char backup_dest_ptr[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_ptr, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", K(ret), K(backup_dest));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest_ptr, cluster_name, cluster_id, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret));
  }
  return ret;
}

int ObMultiBackupDestUtil::get_backup_set_list(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t restore_timestamp,
    const common::ObString& backup_dest_str, common::ObArray<ObSimpleBackupSetPath>& list, int64_t& snapshot_version,
    int64_t& start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  snapshot_version = 0;
  start_replay_log_ts = 0;
  ObBackupDest backup_dest;
  ObArray<ObSimpleBackupSetPath> tmp_list;
  char backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || restore_timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(restore_timestamp));
  } else if (OB_FAIL(databuff_printf(backup_dest_buf,
                 OB_MAX_BACKUP_DEST_LENGTH,
                 "%.*s",
                 backup_dest_str.length(),
                 backup_dest_str.ptr()))) {
    LOG_WARN("failed to databuff printf", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_dest_buf))) {
    LOG_WARN("failed to set backup path", KR(ret), K(backup_dest));
  } else if (OB_FAIL(do_get_backup_set_list(is_preview,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 restore_timestamp,
                 backup_dest,
                 tmp_list,
                 snapshot_version,
                 start_replay_log_ts))) {
    LOG_WARN("failed to inner get backup set list", KR(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(append(list, tmp_list))) {
    LOG_WARN("failed to add array", KR(ret), K(tmp_list));
  }
  return ret;
}

int ObMultiBackupDestUtil::do_get_backup_set_list(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t restore_timestamp,
    const ObBackupDest& backup_dest, common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version,
    int64_t& start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  path_list.reset();
  start_replay_log_ts = -1;
  ObMultiBackupPathType type;
  const ObString path(backup_dest.root_path_);
  const ObString storage_info(backup_dest.storage_info_);

  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(get_path_type(path, storage_info, type))) {
    LOG_WARN("failed to get path type", KR(ret), K(path), K(storage_info));
  } else {
    if (BACKUP_PATH_CLUSTER_LEVEL == type) {
      if (OB_FAIL(do_get_backup_set_list_from_cluster_level(is_preview,
              cluster_name,
              cluster_id,
              tenant_id,
              restore_timestamp,
              backup_dest,
              path_list,
              snapshot_version,
              start_replay_log_ts))) {
        LOG_WARN("failed to do get cluster level backup set list",
            KR(ret),
            K(is_preview),
            K(cluster_name),
            K(cluster_id),
            K(restore_timestamp),
            K(backup_dest));
      }
    } else if (BACKUP_PATH_SINGLE_DIR == type) {
      ObSimpleBackupSetPath simple_path;
      ObFakeBackupLeaseService fake_lease;
      ObExternSingleBackupSetInfoMgr mgr;
      ObBackupSetFileInfo info;
      if (OB_FAIL(simple_path.set(path, storage_info))) {
        LOG_WARN("failed to set simple path", KR(ret), K(path), K(storage_info));
      } else if (OB_FAIL(mgr.init(simple_path, fake_lease))) {
        LOG_WARN("failed to init extern single backup set info mgr", KR(ret), K(simple_path));
      } else if (OB_FAIL(mgr.get_extern_backup_set_file_info(info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get extern backup set file infos", KR(ret), K(simple_path));
        }
      } else {
        simple_path.backup_set_id_ = info.backup_set_id_;
        simple_path.copy_id_ = info.copy_id_;
        simple_path.file_status_ = info.file_status_;
        if (OB_FAIL(path_list.push_back(simple_path))) {
          LOG_WARN("failed to push backup path", KR(ret), K(type));
        }
      }
    } else if (BACKUP_PATH_MAX == type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup path type is not valid", KR(ret), K(type));
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::do_get_backup_set_list_from_cluster_level(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t restore_timestamp,
    const ObBackupDest& backup_dest, common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version,
    int64_t& start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObExternBackupSetFileInfoMgr mgr;
  ObArray<ObBackupSetFileInfo> file_infos;
  ObFakeBackupLeaseService lease_service;
  const ObString path(backup_dest.root_path_);
  const ObString storage_info(backup_dest.storage_info_);
  bool is_backup_backup = true;
  ObCompareSimpleBackupSetPath cmp_set;
  if (OB_FAIL(check_backup_path_is_backup_backup(
          cluster_name, cluster_id, path, storage_info, tenant_id, is_backup_backup))) {
    LOG_WARN("failed to check backup path is backup backup",
        KR(ret),
        K(cluster_name),
        K(cluster_id),
        K(path),
        K(storage_info),
        K(tenant_id));
  } else if (OB_FAIL(get_cluster_backup_dest(backup_dest, cluster_name, cluster_id, cluster_backup_dest))) {
    LOG_WARN("failed to get cluster backup dest", KR(ret), K(path), K(storage_info));
  } else if (OB_FAIL(mgr.init(tenant_id, cluster_backup_dest, is_backup_backup, lease_service))) {
    LOG_WARN("failed to init extern backup set file info mgr", KR(ret), K(tenant_id), K(cluster_backup_dest));
  } else if (OB_FAIL(mgr.get_backup_set_file_infos(file_infos))) {
    LOG_WARN("failed to get backup set file infos", KR(ret));
  } else if (file_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no backup file infos exist", KR(ret), K(path));
  } else if (OB_FAIL(do_inner_get_backup_set_list(cluster_name,
                 cluster_id,
                 restore_timestamp,
                 file_infos,
                 path_list,
                 snapshot_version,
                 start_replay_log_ts))) {
    LOG_WARN("failed to do inner get backup set list", KR(ret), K(restore_timestamp));
  } else {
    std::sort(path_list.begin(), path_list.end(), cmp_set);
  }

  if (OB_SUCC(ret)) {
    if (is_preview && !is_backup_backup) {
      ObExternBackupSetFileInfoMgr mgr;
      ObArray<ObBackupSetFileInfo> tmp_file_infos;
      ObFakeBackupLeaseService lease_service;
      ObArray<ObSimpleBackupSetPath> tmp_path_list;
      if (OB_FAIL(mgr.init(tenant_id, cluster_backup_dest, true /*is_backup_backup*/, lease_service))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to init extern backup set file info mgr", KR(ret), K(tenant_id), K(cluster_backup_dest));
        }
      } else if (OB_FAIL(mgr.get_backup_set_file_infos(tmp_file_infos))) {
        LOG_WARN("failed to get backup set file infos", KR(ret));
      } else if (tmp_file_infos.empty()) {
        // do not return error code because it may be empty
        LOG_WARN("no backup file infos exist", K(path));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); ++i) {
          const ObSimpleBackupSetPath& simple_path = path_list.at(i);
          for (int64_t j = 0; OB_SUCC(ret) && j < tmp_file_infos.count(); ++j) {
            const ObBackupSetFileInfo& tmp_file_info = tmp_file_infos.at(j);
            if (OB_SUCCESS != tmp_file_info.result_) {
              // do nothing
            } else if (!ObBackupFileStatus::can_show_in_preview(tmp_file_info.file_status_)) {
              LOG_INFO("backup set info cannot list in preview", K(tmp_file_info));
            } else if (simple_path.backup_set_id_ == tmp_file_info.backup_set_id_) {
              ObSimpleBackupSetPath new_simple_path = simple_path;
              new_simple_path.backup_set_id_ = tmp_file_info.backup_set_id_;
              new_simple_path.copy_id_ = tmp_file_info.copy_id_;
              new_simple_path.file_status_ = tmp_file_info.file_status_;

              ObBackupBaseDataPathInfo base_data_path_info;
              ObBackupPath backup_path;
              const int64_t full_backup_set_id = tmp_file_info.backup_type_.is_full_backup()
                                                     ? tmp_file_info.backup_set_id_
                                                     : tmp_file_info.prev_full_backup_set_id_;
              ObClusterBackupDest cluster_backup_dest;
              ObBackupDest tmp_backup_dest;
              char tmp_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
              if (OB_FAIL(cluster_backup_dest.set(
                      tmp_file_info.backup_dest_.ptr(), cluster_name, cluster_id, OB_START_INCARNATION))) {
                LOG_WARN("failed to set cluster backup dest");
              } else if (OB_FAIL(base_data_path_info.set(cluster_backup_dest,
                             tmp_file_info.tenant_id_,
                             full_backup_set_id,
                             tmp_file_info.backup_set_id_,
                             tmp_file_info.date_,
                             tmp_file_info.compatible_))) {
                LOG_WARN("failed to set backup base data path", KR(ret), K(tmp_file_info));
              } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(
                             base_data_path_info, backup_path))) {
                LOG_WARN("failed to get tenant data inc backup set path", KR(ret), K(base_data_path_info));
              } else if (OB_FAIL(tmp_backup_dest.set(backup_path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
                LOG_WARN("failed to set backup dest", KR(ret), K(backup_path), K(cluster_backup_dest));
              } else if (OB_FAIL(tmp_backup_dest.get_backup_dest_str(tmp_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
                LOG_WARN("failed to get backup dest str", KR(ret));
              } else if (OB_FAIL(new_simple_path.backup_dest_.assign(tmp_backup_dest_str))) {
                LOG_WARN("failed to assign str", KR(ret), K(tmp_file_info));
              } else if (OB_FAIL(tmp_path_list.push_back(new_simple_path))) {
                LOG_WARN("failed to push back info", KR(ret), K(new_simple_path));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          std::sort(tmp_path_list.begin(), tmp_path_list.end(), cmp_set);
          if (OB_FAIL(append(path_list, tmp_path_list))) {
            LOG_WARN("failed to add array", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::do_inner_get_backup_set_list(const char* cluster_name, const int64_t cluster_id,
    const int64_t restore_timestamp, const ObArray<ObBackupSetFileInfo>& file_infos,
    common::ObArray<ObSimpleBackupSetPath>& path_list, int64_t& snapshot_version, int64_t& start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  snapshot_version = -1;
  start_replay_log_ts = -1;
  int64_t idx = -1;
  if (restore_timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(restore_timestamp));
  }
  for (int64_t i = file_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupSetFileInfo& info = file_infos.at(i);
    if (OB_SUCCESS != info.result_) {
      // do nothing
    } else if (info.snapshot_version_ <= restore_timestamp) {
      idx = i;
      snapshot_version = info.snapshot_version_;
      start_replay_log_ts = info.start_replay_log_ts_;
      break;
    }
  }

  if (OB_SUCC(ret) && idx < 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no backup info exist", KR(ret), K(file_infos), K(restore_timestamp));
  }

  for (int64_t i = idx; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupSetFileInfo& info = file_infos.at(i);
    if (OB_SUCCESS != info.result_) {
      // do nothing
    } else if (!ObBackupFileStatus::can_show_in_preview(info.file_status_)) {
      LOG_INFO("backup set info cannot list in preview", K(info));
    } else {
      ObSimpleBackupSetPath simple_path;
      simple_path.backup_set_id_ = info.backup_set_id_;
      simple_path.copy_id_ = info.copy_id_;
      simple_path.file_status_ = info.file_status_;
      ObBackupBaseDataPathInfo base_data_path_info;
      ObBackupPath backup_path;
      const int64_t full_backup_set_id =
          info.backup_type_.is_full_backup() ? info.backup_set_id_ : info.prev_full_backup_set_id_;
      ObClusterBackupDest cluster_backup_dest;
      ObBackupDest tmp_backup_dest;
      char tmp_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
      if (OB_FAIL(cluster_backup_dest.set(info.backup_dest_.ptr(), cluster_name, cluster_id, OB_START_INCARNATION))) {
        LOG_WARN("failed to set cluster backup dest");
      } else if (OB_FAIL(base_data_path_info.set(cluster_backup_dest,
                     info.tenant_id_,
                     full_backup_set_id,
                     info.backup_set_id_,
                     info.date_,
                     info.compatible_))) {
        LOG_WARN("failed to set backup base data path", KR(ret), K(info));
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(base_data_path_info, backup_path))) {
        LOG_WARN("failed to get tenant data inc backup set path", KR(ret), K(base_data_path_info));
      } else if (OB_FAIL(tmp_backup_dest.set(backup_path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(backup_path), K(cluster_backup_dest));
      } else if (OB_FAIL(tmp_backup_dest.get_backup_dest_str(tmp_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to get backup dest str", KR(ret));
      } else if (OB_FAIL(simple_path.backup_dest_.assign(tmp_backup_dest_str))) {
        LOG_WARN("failed to assign str", KR(ret), K(info));
      } else if (OB_FAIL(path_list.push_back(simple_path))) {
        LOG_WARN("failed to push back info", KR(ret), K(simple_path));
      } else if (info.backup_type_.is_full_backup()) {
        break;
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::get_backup_piece_list(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t snapshot_version,
    const int64_t start_replay_log_ts, const int64_t restore_timestamp, const common::ObString& backup_dest_str,
    common::ObArray<ObSimpleBackupPiecePath>& list)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObArray<ObSimpleBackupPiecePath> tmp_list;
  char backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH] = "";
  STRNCPY(backup_dest_buf, backup_dest_str.ptr(), backup_dest_str.length());
  backup_dest_buf[backup_dest_str.length()] = '\0';
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(backup_dest.set(backup_dest_buf))) {
    LOG_WARN("failed to set backup path", KR(ret), K(backup_dest));
  } else if (OB_FAIL(do_get_backup_piece_list(is_preview,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 snapshot_version,
                 start_replay_log_ts,
                 restore_timestamp,
                 backup_dest,
                 tmp_list))) {
    LOG_WARN("failed to inner get backup set list", KR(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(append(list, tmp_list))) {
    LOG_WARN("failed to add array", KR(ret), K(tmp_list));
  }
  return ret;
}

int ObMultiBackupDestUtil::do_get_backup_piece_list(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t snapshot_version,
    const int64_t start_replay_log_ts, const int64_t restore_timestamp, const ObBackupDest& backup_dest,
    common::ObArray<ObSimpleBackupPiecePath>& path_list)
{
  int ret = OB_SUCCESS;
  path_list.reset();
  ObMultiBackupPathType type;
  ObString path(backup_dest.root_path_);
  ObString storage_info(backup_dest.storage_info_);

  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(get_path_type(path, storage_info, type))) {
    LOG_WARN("failed to get path type", KR(ret), K(path), K(storage_info));
  } else {
    if (BACKUP_PATH_CLUSTER_LEVEL == type) {
      if (OB_FAIL(do_get_backup_piece_list_from_cluster_level(is_preview,
              cluster_name,
              cluster_id,
              tenant_id,
              snapshot_version,
              start_replay_log_ts,
              restore_timestamp,
              backup_dest,
              path_list))) {
        LOG_WARN("failed to do get backup piece list from cluster level",
            KR(ret),
            K(is_preview),
            K(cluster_name),
            K(cluster_id),
            K(tenant_id),
            K(snapshot_version),
            K(start_replay_log_ts),
            K(restore_timestamp),
            K(backup_dest));
      }
    } else if (BACKUP_PATH_SINGLE_DIR == type) {
      ObSimpleBackupPiecePath simple_path;
      ObLogArchiveBackupInfoMgr mgr;
      ObBackupPath backup_path;
      ObBackupPieceInfo piece_info;
      ObFakeBackupLeaseService fake_lease;
      if (OB_FAIL(backup_path.init(path.ptr()))) {
        LOG_WARN("failed to init backup path", K(ret), K(path));
      } else if (OB_FAIL(backup_path.join(OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO))) {
        LOG_WARN("failed to join file name", KR(ret));
      } else if (OB_FAIL(
                     mgr.read_external_single_backup_piece_info(backup_path, storage_info, piece_info, fake_lease))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to read external single backup piece info", K(ret));
        }
      } else if (OB_FAIL(simple_path.set(path, storage_info))) {
        LOG_WARN("failed to set simple path", KR(ret));
      } else {
        simple_path.backup_piece_id_ = piece_info.key_.backup_piece_id_;
        simple_path.copy_id_ = piece_info.key_.copy_id_;
        simple_path.file_status_ = piece_info.file_status_;
        if (OB_FAIL(path_list.push_back(simple_path))) {
          LOG_WARN("failed to push backup path", KR(ret), K(type));
        }
      }
    } else if (BACKUP_PATH_MAX == type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup path type is not valid", KR(ret), K(type));
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::do_get_backup_piece_list_from_cluster_level(const bool is_preview, const char* cluster_name,
    const int64_t cluster_id, const uint64_t tenant_id, const int64_t snapshot_version,
    const int64_t start_replay_log_ts, const int64_t restore_timestamp, const ObBackupDest& backup_dest,
    common::ObArray<ObSimpleBackupPiecePath>& path_list)
{
  int ret = OB_SUCCESS;
  ObString path(backup_dest.root_path_);
  ObString storage_info(backup_dest.storage_info_);
  ObClusterBackupDest cluster_backup_dest;
  ObBackupPath backup_path;
  ObBackupPath backup_path_2;
  bool is_backup_backup = true;  // if the at 'backup_dest' in restore sql is backup backup
  if (OB_FAIL(check_backup_path_is_backup_backup(
          cluster_name, cluster_id, path, storage_info, tenant_id, is_backup_backup))) {
    LOG_WARN("failed to check backup path is backup backup",
        KR(ret),
        K(cluster_name),
        K(cluster_id),
        K(path),
        K(storage_info),
        K(tenant_id));
  } else if (OB_FAIL(get_cluster_backup_dest(backup_dest, cluster_name, cluster_id, cluster_backup_dest))) {
    LOG_WARN("failed to get cluster backup dest", KR(ret), K(path), K(storage_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_info_path(
                 cluster_backup_dest, tenant_id, is_backup_backup, backup_path))) {
    LOG_WARN("failed to get tenant clog backup piece info path", KR(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (is_preview && !is_backup_backup &&
             OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_info_path(
                 cluster_backup_dest, tenant_id, true /*is_backup_backup*/, backup_path_2))) {
    LOG_WARN("failed to get tenant clog backup piece info path", KR(ret), K(cluster_backup_dest), K(tenant_id));
  } else {
    ObLogArchiveBackupInfoMgr mgr;
    ObExternalBackupPieceInfo extern_info;
    ObArray<ObBackupPieceInfo> piece_array;
    ObFakeBackupLeaseService fake_lease_service;
    ObCompareSimpleBackupPiecePath cmp_piece;
    if (OB_FAIL(mgr.get_external_backup_piece_info(backup_path, storage_info, extern_info, fake_lease_service))) {
      LOG_WARN("failed to get external backup piece info", KR(ret), K(backup_path), K(storage_info));
    } else if (OB_FAIL(extern_info.get_piece_array(piece_array))) {
      LOG_WARN("failed to get piece array", KR(ret), K(extern_info));
    } else if (piece_array.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no piece info exist", KR(ret));
    } else if (!is_backup_backup &&
               OB_FAIL(may_need_replace_active_piece_info(cluster_backup_dest, storage_info, piece_array))) {
      LOG_WARN("failed to need fill active piece info", KR(ret), K(backup_path), K(storage_info));
    } else if (OB_FAIL(do_inner_get_backup_piece_list(cluster_name,
                   cluster_id,
                   snapshot_version,
                   start_replay_log_ts,
                   restore_timestamp,
                   piece_array,
                   path_list))) {
      LOG_WARN("failed to do inner get backup piece list", KR(ret), K(restore_timestamp));
    } else {
      std::sort(path_list.begin(), path_list.end(), cmp_piece);
    }

    if (OB_SUCC(ret)) {
      if (is_preview && !is_backup_backup) {
        ObLogArchiveBackupInfoMgr mgr;
        ObExternalBackupPieceInfo extern_info;
        ObArray<ObBackupPieceInfo> tmp_piece_array;
        ObFakeBackupLeaseService fake_lease_service;
        ObArray<ObSimpleBackupPiecePath> tmp_path_list;
        if (OB_FAIL(mgr.get_external_backup_piece_info(backup_path_2, storage_info, extern_info, fake_lease_service))) {
          if (OB_BACKUP_FILE_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get external backup piece info", KR(ret), K(backup_path_2), K(storage_info));
          }
        } else if (OB_FAIL(extern_info.get_piece_array(tmp_piece_array))) {
          LOG_WARN("failed to get piece array", KR(ret), K(extern_info));
        } else if (tmp_piece_array.empty()) {
          // do not return error code because it may be empty
          LOG_WARN("no piece info exist");
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); ++i) {
            const ObSimpleBackupPiecePath& simple_path = path_list.at(i);
            for (int64_t j = 0; OB_SUCC(ret) && j < tmp_piece_array.count(); ++j) {
              const ObBackupPieceInfo& tmp_piece = tmp_piece_array.at(j);
              if (!ObBackupFileStatus::can_show_in_preview(tmp_piece.file_status_)) {
                LOG_INFO("backup piece info cannot list in preview", K(tmp_piece));
              } else if (simple_path.backup_piece_id_ == tmp_piece.key_.backup_piece_id_ &&
                         simple_path.round_id_ == tmp_piece.key_.round_id_ &&
                         tmp_piece.checkpoint_ts_ >= restore_timestamp) {
                ObSimpleBackupPiecePath new_simple_path = simple_path;
                new_simple_path.backup_piece_id_ = tmp_piece.key_.backup_piece_id_;
                new_simple_path.copy_id_ = tmp_piece.key_.copy_id_;
                new_simple_path.file_status_ = tmp_piece.file_status_;
                ObBackupPath backup_path;
                ObClusterBackupDest cluster_backup_dest;
                ObBackupDest tmp_backup_dest;
                char tmp_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
                if (OB_FAIL(cluster_backup_dest.set(
                        tmp_piece.backup_dest_.ptr(), cluster_name, cluster_id, OB_START_INCARNATION))) {
                  LOG_WARN("failed to set cluster backup dest", KR(ret));
                } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_dir_path(cluster_backup_dest,
                               tmp_piece.key_.tenant_id_,
                               tmp_piece.key_.round_id_,
                               tmp_piece.key_.backup_piece_id_,
                               tmp_piece.create_date_,
                               backup_path))) {
                  LOG_WARN("failed to get cluster clog prefix path", KR(ret));
                } else if (OB_FAIL(
                               tmp_backup_dest.set(backup_path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
                  LOG_WARN("failed to set backup dest", KR(ret), K(backup_path), K(cluster_backup_dest));
                } else if (OB_FAIL(
                               tmp_backup_dest.get_backup_dest_str(tmp_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
                  LOG_WARN("failed to get backup dest str", KR(ret));
                } else if (OB_FAIL(new_simple_path.backup_dest_.assign(tmp_backup_dest_str))) {
                  LOG_WARN("failed to assign str", KR(ret), K(tmp_piece));
                } else if (OB_FAIL(tmp_path_list.push_back(new_simple_path))) {
                  LOG_WARN("failed to push back new simple path", KR(ret), K(new_simple_path));
                }
              }
            }
          }

          if (OB_SUCC(ret)) {
            std::sort(tmp_path_list.begin(), tmp_path_list.end(), cmp_piece);
            if (OB_FAIL(append(path_list, tmp_path_list))) {
              LOG_WARN("failed to add array", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::do_inner_get_backup_piece_list(const char* cluster_name, const int64_t cluster_id,
    const int64_t snapshot_version, const int64_t start_replay_log_ts, const int64_t restore_timestamp,
    const common::ObArray<ObBackupPieceInfo>& piece_array, common::ObArray<ObSimpleBackupPiecePath>& path_list)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  int64_t round_id = -1;
  int64_t start_ts = -1;
  if (start_replay_log_ts < 0 || restore_timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(start_replay_log_ts), K(restore_timestamp));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < piece_array.count(); ++i) {
    const ObBackupPieceInfo& piece = piece_array.at(i);
    if (round_id != piece.key_.round_id_) {
      round_id = piece.key_.round_id_;
      start_ts = piece.start_ts_;  // the start_ts of first piece in one round
    }
    if (piece.checkpoint_ts_ >= restore_timestamp) {
      idx = i;
      break;
    }
  }

  if (OB_SUCC(ret) && (idx < 0 || snapshot_version < start_ts)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no backup info exist", KR(ret), K(start_ts), K(snapshot_version), K(restore_timestamp), K(piece_array));
  }

  for (int64_t i = idx; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupPieceInfo& piece = piece_array.at(i);
    if (round_id != piece.key_.round_id_ || piece.max_ts_ < start_replay_log_ts) {
      break;
    } else if (!ObBackupFileStatus::can_show_in_preview(piece.file_status_)) {
      LOG_INFO("backup piece info cannot list in preview", K(piece));
    } else {
      ObSimpleBackupPiecePath simple_path;
      simple_path.round_id_ = piece.key_.round_id_;
      simple_path.backup_piece_id_ = piece.key_.backup_piece_id_;
      simple_path.copy_id_ = piece.key_.copy_id_;
      simple_path.file_status_ = piece.file_status_;
      ObBackupPath backup_path;
      ObClusterBackupDest cluster_backup_dest;
      ObBackupDest tmp_backup_dest;
      char tmp_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
      if (OB_FAIL(cluster_backup_dest.set(piece.backup_dest_.ptr(), cluster_name, cluster_id, OB_START_INCARNATION))) {
        LOG_WARN("failed to set cluster backup dest", KR(ret));
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_dir_path(cluster_backup_dest,
                     piece.key_.tenant_id_,
                     piece.key_.round_id_,
                     piece.key_.backup_piece_id_,
                     piece.create_date_,
                     backup_path))) {
        LOG_WARN("failed to get cluster clog prefix path", KR(ret));
      } else if (OB_FAIL(tmp_backup_dest.set(backup_path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(backup_path), K(cluster_backup_dest));
      } else if (OB_FAIL(tmp_backup_dest.get_backup_dest_str(tmp_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to get backup dest str", KR(ret));
      } else if (OB_FAIL(simple_path.backup_dest_.assign(tmp_backup_dest_str))) {
        LOG_WARN("failed to assign str", KR(ret), K(piece));
      } else if (OB_FAIL(path_list.push_back(simple_path))) {
        LOG_WARN("failed to push back info", KR(ret), K(simple_path));
      }
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::may_need_replace_active_piece_info(const ObClusterBackupDest& dest,
    const common::ObString& storage_info, common::ObArray<ObBackupPieceInfo>& piece_array)
{
  int ret = OB_SUCCESS;
  ObBackupPieceInfo last_piece;
  ObLogArchiveBackupInfoMgr mgr;
  bool is_active_piece = false;
  if (piece_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece array is empty");
  } else if (!dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(dest));
  } else if (OB_FAIL(piece_array.pop_back(last_piece))) {
    LOG_WARN("failed to pop back", KR(ret), K(piece_array));
  } else if (FALSE_IT(is_active_piece = ObBackupPieceStatus::BACKUP_PIECE_ACTIVE == last_piece.status_)) {
  } else if (!is_active_piece) {
    if (OB_FAIL(piece_array.push_back(last_piece))) {
      LOG_WARN("failed to push back piece info", KR(ret), K(last_piece));
    }
  } else {
    ObBackupPath single_piece_backup_path;
    ObBackupPieceInfo active_piece;
    ObFakeBackupLeaseService fake_lease;
    if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_single_piece_info_path(dest,
            last_piece.key_.tenant_id_,
            last_piece.key_.round_id_,
            last_piece.key_.backup_piece_id_,
            last_piece.create_date_,
            single_piece_backup_path))) {
      LOG_WARN("failed to get tenant clog backup single piece info path", KR(ret), K(dest), K(last_piece));
    } else if (OB_FAIL(mgr.read_external_single_backup_piece_info(
                   single_piece_backup_path, storage_info, active_piece, fake_lease))) {
      LOG_WARN("failed to read external single backup piece info", KR(ret));
    } else if (OB_FAIL(piece_array.push_back(active_piece))) {
      LOG_WARN("failed to push back active piece", KR(ret), K(active_piece));
    } else {
      LOG_INFO("replace active piece info in array", K(last_piece), K(active_piece));
    }
  }
  return ret;
}

int ObMultiBackupDestUtil::check_backup_path_is_backup_backup(const char* cluster_name, const int64_t cluster_id,
    const common::ObString& root_path, const common::ObString& storage_info, const uint64_t tenant_id,
    bool& is_backup_backup)
{
  int ret = OB_SUCCESS;
  is_backup_backup = false;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObBackupPath backup_path;
  bool exist = false;
  ObStorageUtil util(false /*need_retry*/);
  if (OB_FAIL(backup_dest.set(root_path.ptr(), storage_info.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(root_path), K(storage_info));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest_str, cluster_name, cluster_id, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest_str), K(cluster_name), K(cluster_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_file_info_path(
                 cluster_backup_dest, tenant_id, false /*is_backup_backup*/, backup_path))) {
    LOG_WARN("failed to get tenant backup set file info path", KR(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(util.is_exist(backup_path.get_obstr(), storage_info, exist))) {
    LOG_WARN("failed to check is file exist", KR(ret), K(backup_path), K(storage_info));
  } else {
    is_backup_backup = !exist;
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
