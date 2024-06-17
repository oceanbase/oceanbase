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
  explicit ObCdcGetSourceFunctor(ClientLSCtx &ctx, int64_t &version): ctx_(ctx), version_(version) {}
  int operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard);

private:
  ClientLSCtx &ctx_;
  int64_t &version_;
};

class ObCdcUpdateSourceFunctor {
public:
  explicit ObCdcUpdateSourceFunctor(ClientLSCtx &ctx, int64_t &version):
      ctx_(ctx),
      version_(version) {}
  int operator()(const share::ObLSID &id, logservice::ObRemoteLogParent *source);
private:
  ClientLSCtx &ctx_;
  int64_t &version_;
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

  const common::ObAddr &get_client_addr() const {
    return client_addr_;
  }

  uint64_t get_client_pid() const {
    return client_pid_;
  }

  uint64_t get_tenant_id() const {
    return tenant_id_;
  }

  const ObLSID &get_ls_id() const {
    return ls_id_;
  }


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

enum class FetchLogProtocolType
{
  Unknown = -1,
  LogGroupEntryProto = 0,
  RawLogDataProto = 1,
};

class ClientLSCtx: public common::LinkHashValue<ClientLSKey>
{
public:
  ClientLSCtx();
  ~ClientLSCtx();

public:
  int init(const int64_t client_progress, const FetchLogProtocolType proto);

  // thread safe method,
  // OB_INIT_TWICE: archive source has been inited;
  // OB_ALLOCATE_MEMORY_FAILED
  // OB_NO_NEED_UPDATE: there is a newer archive dest, which is unexpected
  // other
  int try_init_archive_source(const ObLSID &ls_id,
      const ObBackupDest &archive_dest,
      const int64_t dest_version);

  // thread safe method
  // deep copy source in ctx to target_src, would allocate memory for target_src.
  // caller should call logservice::ObResSrcAlloctor::free to free target_src
  int try_deep_copy_source(const ObLSID &ls_id,
      logservice::ObRemoteLogParent *&target_src,
      int64_t &version) const;

  // thread safe method
  // write the info of current source to source in ctx.
  int try_update_archive_source(logservice::ObRemoteLogParent *source,
      const int64_t source_ver);

  // thread safe method
  int try_change_archive_source(const ObLSID &ls_id,
      const ObBackupDest &dest,
      const int64_t source_ver);

  bool archive_source_inited() const {
    SpinRLockGuard guard(source_lock_);
    return nullptr != source_;
  }

  // make sure only one thread would call this method.
  void reset();

  void set_proto_type(const FetchLogProtocolType type) {
    FetchLogProtocolType from = proto_type_, to = type;
    proto_type_ = type;
    EXTLOG_LOG(INFO, "set fetch protocol ", K(from), K(to));
  }

  // non-thread safe method
  FetchLogProtocolType get_proto_type() const {
    return proto_type_;
  }

  // non-thread safe method
  void set_fetch_mode(FetchMode mode, const char *reason) {
    FetchMode from = fetch_mode_, to = mode;
    fetch_mode_ = mode;
    EXTLOG_LOG(INFO, "set fetch mode ", K(from), K(to), K(reason));
  }

  // non-thread safe method
  FetchMode get_fetch_mode() const { return fetch_mode_; }

  // non-thread safe method
  void update_touch_ts() { last_touch_ts_ = ObTimeUtility::current_time(); }

  // non-thread safe method
  int64_t get_touch_ts() const { return last_touch_ts_; }

  // non-thread safe method
  void set_progress(int64_t progress) { client_progress_ = progress; }

  // non-thread safe method
  int64_t get_progress() const { return client_progress_; }

  TO_STRING_KV(KP_(source),
               K_(source_version),
               K_(proto_type),
               K_(fetch_mode),
               K_(last_touch_ts),
               K_(client_progress))

private:
  // caller need to hold source_lock_ before invoke this method
  int set_source_version_(logservice::ObRemoteLogParent *source, const int64_t version);
  // caller need to hold source_lock_ before invoke this method
  void set_source_(logservice::ObRemoteLogParent *source);

private:
  SpinRWLock source_lock_;
  // GUARDED_BY(source_lock_)
  int64_t source_version_;
  // GUARDED_BY(source_lock_)
  logservice::ObRemoteLogParent *source_;
  // stat, it's ok even if it's not correct.
  // only set when init, can hardly be wrong
  FetchLogProtocolType proto_type_;
  // it concerns about the thread num of log_ext_storage_handler,
  // should be eventually correct.
  FetchMode fetch_mode_;
  // it's used for recycling context, would be updated when fetching log
  // though it may not be precise, it wouldn't deviate a lot, unless some
  // threads get stuck for a long time or clock drift occurs.
  int64_t last_touch_ts_;
  // for fetch_raw_log protocol, it's not used.
  int64_t client_progress_;
};

typedef common::ObSEArray<std::pair<int64_t, share::ObBackupPathString>, 1> ObArchiveDestInfo;
typedef ObLinkHashMap<ClientLSKey, ClientLSCtx> ClientLSCtxMap;
}
}

#endif
