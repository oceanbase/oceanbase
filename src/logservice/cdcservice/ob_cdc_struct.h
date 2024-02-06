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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_STRUCT_H_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_STRUCT_H_

#include "logservice/restoreservice/ob_remote_log_iterator.h" // ObRemoteLogGroupEntryIterator
#include "share/backup/ob_backup_struct.h" // ObBackupPathString
#include "share/ob_ls_id.h" // ObLSID

namespace oceanbase
{
namespace logservice
{
class ObRemoteLogParent;
class ObLogArchivePieceContext;
class ObRemoteSourceGuard;
}
namespace cdc
{

class ClientLSCtx;

class ObCdcGetSourceFunctor {
public:
  explicit ObCdcGetSourceFunctor(ClientLSCtx &ctx): ctx_(ctx) {}
  int operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard);
private:
  ClientLSCtx &ctx_;
};

class ObCdcUpdateSourceFunctor {
public:
  explicit ObCdcUpdateSourceFunctor(ClientLSCtx &ctx): ctx_(ctx) {}
  int operator()(const share::ObLSID &id, logservice::ObRemoteLogParent *source);
private:
  ClientLSCtx &ctx_;
};

// Temporarily not supported
class ObCdcRefreshStorageInfoFunctor {
public:
  int operator()(share::ObBackupDest &dest) {
    UNUSED(dest);
    return OB_SUCCESS;
  }
};

class ClientLSKey
{
public:
  ClientLSKey():
      client_addr_(),
      client_pid_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_(share::ObLSID::INVALID_LS_ID)
      { }
  ClientLSKey(const common::ObAddr &client_addr,
              const uint64_t client_pid,
              const uint64_t tenant_id,
              const share::ObLSID &ls_id);
  ~ClientLSKey() { reset(); }
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ClientLSKey &that) const;
  bool operator!=(const ClientLSKey &that) const;
  ClientLSKey &operator=(const ClientLSKey &that);
  int compare(const ClientLSKey &key) const;
  void reset();

  TO_STRING_KV(K_(client_addr), K_(client_pid), K_(ls_id))

private:
  common::ObAddr client_addr_;
  uint64_t client_pid_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

enum class FetchMode {
  FETCHMODE_UNKNOWN = 0,

  FETCHMODE_ONLINE,
  FETCHMODE_ARCHIVE,

  FETCHMODE_MAX
};

class ClientLSCtx: public common::LinkHashValue<ClientLSKey>
{
public:
  ClientLSCtx();
  ~ClientLSCtx();

public:
  int init(const int64_t client_progress);
  void reset();
  void set_source(logservice::ObRemoteLogParent *source);
  logservice::ObRemoteLogParent *get_source() { return source_; }

  void set_fetch_mode(FetchMode mode, const char *reason) {
    FetchMode from = fetch_mode_, to = mode;
    fetch_mode_ = mode;
    EXTLOG_LOG(INFO, "set fetch mode ", K(from), K(to), K(reason));
  }
  FetchMode get_fetch_mode() const { return fetch_mode_; }

  void update_touch_ts() { last_touch_ts_ = ObTimeUtility::current_time(); }
  int64_t get_touch_ts() const { return last_touch_ts_; }

  void set_progress(int64_t progress) { client_progress_ = progress; }
  int64_t get_progress() const { return client_progress_; }

  TO_STRING_KV(K_(source),
               K_(fetch_mode),
               K_(last_touch_ts),
               K_(client_progress))

friend class ObCdcGetSourceFunctor;
friend class ObCdcUpdateSourceFunctor;

private:
  bool is_inited_;
  ObSpinLock source_lock_;
  logservice::ObRemoteLogParent *source_;
  FetchMode fetch_mode_;
  int64_t last_touch_ts_;
  int64_t client_progress_;
};

typedef common::ObSEArray<std::pair<int64_t, share::ObBackupPathString>, 1> ObArchiveDestInfo;
typedef ObLinkHashMap<ClientLSKey, ClientLSCtx> ClientLSCtxMap;
}
}

#endif