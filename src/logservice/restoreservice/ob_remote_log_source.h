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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_SOURCE_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_SOURCE_H_

#include "lib/container/ob_se_array.h"     // SEArray
#include "lib/net/ob_addr.h"               // ObAddr
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "share/backup/ob_backup_struct.h"     // ObBackupPathString
#include "share/backup/ob_log_restore_struct.h"
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/restore/ob_log_restore_source.h"  // ObLogRestoreSourceType
#include "ob_log_archive_piece_mgr.h"           // ObLogArchivePieceContext
#include "ob_log_restore_define.h"
namespace oceanbase
{
namespace logservice
{
using oceanbase::share::ObLogRestoreSourceType;
//using oceanbase::share::DirArray;
typedef share::ObRestoreSourceServiceAttr RestoreServiceAttr;
// The management of remote log source, three types are supported, LOCATION/SERVICE/RAWPATH
class ObRemoteLogParent
{
public:
  explicit ObRemoteLogParent(const ObLogRestoreSourceType &type, const share::ObLSID &ls_id);
  virtual ~ObRemoteLogParent();

public:
  virtual int deep_copy_to(ObRemoteLogParent &other) = 0;
  virtual bool is_valid() const = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  virtual int update_locate_info(ObRemoteLogParent &source) = 0;
  bool to_end() const { return to_end_; }
  bool set_to_end(const share::SCN &scn);
  void get_end_scn(share::SCN &scn) const { scn = end_fetch_scn_;}
  void get_upper_limit_scn(share::SCN &scn) const { scn = upper_limit_scn_; }
  ObLogRestoreSourceType get_source_type() const { return type_; }
  const char *get_source_type_str(const ObLogRestoreSourceType &type) const;
  void mark_error(share::ObTaskId &trace_id, const int ret_code);
  void get_error_info(share::ObTaskId &trace_id, int &ret_code, bool &error_exist);

protected:
  void base_copy_to_(ObRemoteLogParent &other);
  bool is_valid_() const;

protected:
  share::ObLSID ls_id_;
  ObLogRestoreSourceType type_;
  share::SCN upper_limit_scn_;
  bool to_end_;
  share::SCN end_fetch_scn_;
  palf::LSN end_lsn_;

  ObLogRestoreErrorContext error_context_;    // 记录该source的错误信息, 仅leader有效
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogParent);
};

class ObRemoteSerivceParent : public ObRemoteLogParent
{
public:
  ObRemoteSerivceParent(const share::ObLSID &ls_id);
  virtual ~ObRemoteSerivceParent();

public:
  int set(const RestoreServiceAttr &attr, const share::SCN &end_scn);
  void get(RestoreServiceAttr *&attr, share::SCN &end_scn);
  int deep_copy_to(ObRemoteLogParent &other) override;
  bool is_valid() const override;
  int update_locate_info(ObRemoteLogParent &source) override { UNUSED(source); return OB_SUCCESS; }
  TO_STRING_KV("ObRemoteLogParent", get_source_type_str(type_), K_(ls_id), K_(attr),
      K_(upper_limit_scn), K_(to_end), K_(end_fetch_scn), K_(end_lsn));

private:
  RestoreServiceAttr attr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteSerivceParent);
};

class ObRemoteLocationParent : public ObRemoteLogParent
{
public:
  ObRemoteLocationParent(const share::ObLSID &ls_id);
  virtual ~ObRemoteLocationParent();

public:
  void get(share::ObBackupDest *&dest, ObLogArchivePieceContext *&piece_context, share::SCN &end_scn);
  int set(const share::ObBackupDest &dest, const share::SCN &end_scn);
  int deep_copy_to(ObRemoteLogParent &other) override;
  bool is_valid() const override;
  int update_locate_info(ObRemoteLogParent &source) override;

  TO_STRING_KV("ObRemoteLogParent", get_source_type_str(type_), K_(ls_id), K_(root_path), K_(piece_context),
      K_(upper_limit_scn), K_(to_end), K_(end_fetch_scn), K_(end_lsn));

private:
  share::ObBackupDest root_path_;   // uri & storage_info
  ObLogArchivePieceContext piece_context_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLocationParent);
};

class ObRemoteRawPathParent : public ObRemoteLogParent
{
public:
  ObRemoteRawPathParent(const share::ObLSID &ls_id);
  virtual ~ObRemoteRawPathParent();

public:
  void get(ObLogRawPathPieceContext *&raw_piece_ctx, share::SCN &end_scn);
  int deep_copy_to(ObRemoteLogParent &other) override;
  bool is_valid() const override;
  int set(DirArray &array, const share::SCN &end_scn);
  int update_locate_info(ObRemoteLogParent &source) override;

  TO_STRING_KV("ObRemoteLogParent", get_source_type_str(type_), K_(ls_id), K_(upper_limit_scn), K_(to_end),
      K_(end_fetch_scn), K_(end_lsn), K_(raw_piece_ctx));

private:
  ObLogRawPathPieceContext raw_piece_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteRawPathParent);
};

class ObRemoteSourceGuard
{
public:
  ObRemoteSourceGuard();
  ~ObRemoteSourceGuard();

  ObRemoteLogParent *get_source() const { return source_; }
  int set_source(ObRemoteLogParent *source);
  void reset();
private:
  ObRemoteLogParent *source_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteSourceGuard);
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_SOURCE_H_*/
