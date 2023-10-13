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

#define USING_LOG_PREFIX EXTLOG

#include "ob_cdc_struct.h"
#include "logservice/restoreservice/ob_remote_log_source_allocator.h"

namespace oceanbase
{

namespace cdc
{
///////////////////////////////////////////ClientLSKey///////////////////////////////////////////

ClientLSKey::ClientLSKey(
    const common::ObAddr &client_addr,
    const uint64_t client_pid,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
    : client_addr_(client_addr),
      client_pid_(client_pid),
      tenant_id_(tenant_id),
      ls_id_(ls_id)
{
}

uint64_t ClientLSKey::hash() const
{
  uint64_t hash_val = client_pid_;
  hash_val = murmurhash(&hash_val , sizeof(hash_val), client_addr_.hash());
  hash_val = murmurhash(&hash_val, sizeof(hash_val), ls_id_.hash());
  hash_val = murmurhash(&hash_val, sizeof(hash_val), tenant_id_);
  return hash_val;
}

int ClientLSKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ClientLSKey::operator==(const ClientLSKey &that) const
{
  return client_addr_ == that.client_addr_ &&
         client_pid_ == that.client_pid_ &&
         tenant_id_ == that.tenant_id_ &&
         ls_id_ == that.ls_id_;
}

bool ClientLSKey::operator!=(const ClientLSKey &that) const
{
  return !(*this == that);
}

ClientLSKey &ClientLSKey::operator=(const ClientLSKey &that)
{
  client_addr_ = that.client_addr_;
  client_pid_ = that.client_pid_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  return *this;
}

int ClientLSKey::compare(const ClientLSKey &key) const
{
  int ret = ls_id_.compare(key.ls_id_);
  if (0 == ret) {
    ret = client_addr_.compare(key.client_addr_);
  }
  if (0 == ret) {
    if (client_pid_ > key.client_pid_) {
      ret = 1;
    } else if (client_pid_ == key.client_pid_) {
      ret = 0;
    } else {
      ret = -1;
    }
  }
  return ret;
}

void ClientLSKey::reset()
{
  client_addr_.reset();
  client_pid_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_ = ObLSID::INVALID_LS_ID;
}
///////////////////////////////////////////ClientLSCtx///////////////////////////////////////////

ClientLSCtx::ClientLSCtx()
  : is_inited_(false),
    source_lock_(ObLatchIds::CDC_SERVICE_LS_CTX_LOCK),
    source_(NULL),
    fetch_mode_(FetchMode::FETCHMODE_UNKNOWN),
    last_touch_ts_(OB_INVALID_TIMESTAMP),
    client_progress_(OB_INVALID_TIMESTAMP)
{
}

ClientLSCtx::~ClientLSCtx()
{
  reset();
}

int ClientLSCtx::init(int64_t client_progress)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TIMESTAMP != client_progress) {
    is_inited_ = true;
    set_progress(client_progress);
    set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "ClientLSCtxInit");
    update_touch_ts();
  } else {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "client progress is invalid", KR(ret), K(client_progress));
  }
  return ret;
}

void ClientLSCtx::reset()
{
  is_inited_ = false;
  if (NULL != source_) {
    logservice::ObResSrcAlloctor::free(source_);
    source_ = NULL;
  }
  fetch_mode_ = FetchMode::FETCHMODE_UNKNOWN;
  last_touch_ts_ = OB_INVALID_TIMESTAMP;
  client_progress_ = OB_INVALID_TIMESTAMP;
}

void ClientLSCtx::set_source(logservice::ObRemoteLogParent *source)
{
  if (NULL != source_) {
    logservice::ObResSrcAlloctor::free(source_);
    source_ = NULL;
  }
  source_ = source;
}

//////////////////////////////////////////////////////////////////////////
int ObCdcGetSourceFunctor::operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard) {
  int ret = OB_SUCCESS;
  ObSpinLockGuard ctx_source_guard(ctx_.source_lock_);
  logservice::ObRemoteLogParent *ctx_source = ctx_.get_source();
  if (OB_ISNULL(ctx_source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source in ClientLSCtx is null, cannot get source from ctx", KR(ret), K(id));
  } else {
    logservice::ObRemoteLogParent *source = logservice::ObResSrcAlloctor::alloc(ctx_source->get_source_type(), id);
    if (OB_ISNULL(source)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("source allocated is null, allocate failed", KR(ret), K(id));
    } else if (OB_FAIL(ctx_source->deep_copy_to(*source))) {
      LOG_WARN("deep copy from source in ctx failed", KR(ret), K(id));
    } else if (OB_FAIL(guard.set_source(source))) {
      LOG_WARN("RemoteSourceGuard set source failed", KR(ret));
    } else { }

    if (OB_FAIL(ret) && OB_NOT_NULL(source)) {
      logservice::ObResSrcAlloctor::free(source);
      source = nullptr;
    }
  }
  return ret;
}

int ObCdcUpdateSourceFunctor::operator()(const share::ObLSID &id, logservice::ObRemoteLogParent *source) {
  int ret = OB_SUCCESS;
  UNUSED(id);
  ObSpinLockGuard ctx_source_guard(ctx_.source_lock_);
  logservice::ObRemoteLogParent *ctx_source = ctx_.get_source();
  if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source is null when updating source", KR(ret), K(id));
  } else if (OB_ISNULL(ctx_source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source in ctx is null when updating source", KR(ret), K(id));
  } else if (ctx_source->get_source_type() != source->get_source_type()) {
    LOG_WARN("the type of source and ctx_source is not same", KR(ret), KPC(source), KPC(ctx_source));
  } else if (OB_FAIL(ctx_source->update_locate_info(*source))) {
    LOG_WARN("update locate info failed", KR(ret));
  }
  return ret;
}

} // cdc
} // oceanbase