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

#define USING_LOG_PREFIX SHARE

#include "share/tablet/ob_tablet_info.h"
#include "share/ob_errno.h" // KR(ret)
#include "share/ob_ls_id.h" // ls_id
#include "share/tablet/ob_tablet_filter.h" // ObTabletFilter

namespace oceanbase
{
namespace share
{
ObTabletReplica::ObTabletReplica()
    : tenant_id_(OB_INVALID_TENANT_ID),
      tablet_id_(),
      ls_id_(),
      server_(),
      snapshot_version_(0),
      data_size_(0),
      required_size_(0),
      report_scn_(0),
      status_(SCN_STATUS_MAX)
{
}

ObTabletReplica::~ObTabletReplica()
{
  reset();
}

void ObTabletReplica::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_id_.reset();
  ls_id_.reset();
  server_.reset();
  snapshot_version_ = 0;
  data_size_ = 0;
  required_size_ = 0;
  report_scn_ = 0;
  status_ = SCN_STATUS_MAX;
}

int ObTabletReplica::assign(const ObTabletReplica &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    server_ = other.server_;
    snapshot_version_ = other.snapshot_version_;
    data_size_ = other.data_size_;
    required_size_ = other.required_size_;
    report_scn_ = other.report_scn_;
    status_ = other.status_;
  }
  return ret;
}

int ObTabletReplica::init(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &server,
    const int64_t snapshot_version,
    const int64_t data_size,
    const int64_t required_size,
    const int64_t report_scn,
    const ScnStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      !tablet_id.is_valid_with_tenant(tenant_id)
      || !ls_id.is_valid_with_tenant(tenant_id)
      || !server.is_valid()
      || snapshot_version < 0
      || data_size < 0
      || required_size < 0
      || report_scn < 0
      || !is_status_valid(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init with invalid arguments", KR(ret), K(tenant_id), K(tablet_id), K(ls_id),
        K(server), K(snapshot_version), K(data_size), K(required_size), K(report_scn), K(status));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    ls_id_ = ls_id;
    server_ = server;
    snapshot_version_ = snapshot_version;
    data_size_ = data_size;
    required_size_ = required_size;
    report_scn_ = report_scn;
    status_ = status;
  }
  return ret;
}

bool ObTabletReplica::is_equal_for_report(const ObTabletReplica &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else if (tenant_id_ == other.tenant_id_
      && tablet_id_ == other.tablet_id_
      && ls_id_ == other.ls_id_
      && server_ == other.server_
      && snapshot_version_ == other.snapshot_version_
      && data_size_ == other.data_size_
      && required_size_ == other.required_size_) {
    is_equal = true;
  }
  return is_equal;
}

void ObTabletReplica::fake_for_diagnose(const uint64_t tenant_id,
                                       const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id)
{
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
}

ObTabletInfo::ObTabletInfo()
    : tenant_id_(OB_INVALID_TENANT_ID),
      tablet_id_(),
      ls_id_(),
      replicas_()
{
}

ObTabletInfo::ObTabletInfo(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const ObArray<ObTabletReplica> &replicas)
    : tenant_id_(tenant_id),
      tablet_id_(tablet_id),
      ls_id_(ls_id),
      replicas_(replicas)
{
}

ObTabletInfo::~ObTabletInfo()
{
  reset();
}

void ObTabletInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_id_.reset();
  ls_id_.reset();
  replicas_.reset();
}

int ObTabletInfo::assign(const ObTabletInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    if (OB_FAIL(replicas_.assign(other.replicas_))) {
      LOG_WARN("fail to assign replicas", KR(ret), K_(tenant_id), K_(tablet_id), K_(replicas));
    }
  }
  return ret;
}

int ObTabletInfo::init(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const common::ObIArray<ObTabletReplica> &replicas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id)
      || !ls_id.is_valid_with_tenant(tenant_id)
      || replicas.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init with invalid arguments", KR(ret),
        K(tenant_id), K(tablet_id), K(ls_id), K(replicas));
  } else if (OB_FAIL(replicas_.assign(replicas))) {
    LOG_WARN("fail to assign replicas", KR(ret), K(replicas));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    ls_id_ = ls_id;
  }
  return ret;
}

int ObTabletInfo::init_by_replica(const ObTabletReplica &replica)
{
  int ret = OB_SUCCESS;
  reset();
  ObSEArray<ObTabletReplica, 1> replicas;
  if (OB_UNLIKELY(!replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica));
  } else if (OB_FAIL(replicas.push_back(replica))) {
    LOG_WARN("fail to push back replica", KR(ret), K(replica));
  } else if (OB_FAIL(init(
      replica.get_tenant_id(),
      replica.get_tablet_id(),
      replica.get_ls_id(),
      replicas))) {
    LOG_WARN("fail to init tablet_info", KR(ret), K(replica), K(replicas));
  }
  return ret;
}

int ObTabletInfo::add_replica(const ObTabletReplica &replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this), K(replica));
  } else if (OB_UNLIKELY(tenant_id_ != replica.get_tenant_id()
      || tablet_id_ != replica.get_tablet_id())
      || ls_id_ != replica.get_ls_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replica not belong to this tablet",
        KR(ret), K_(tenant_id), K_(tablet_id), K_(ls_id), K(replica));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    ret = find_replica_idx_(replica, idx);
    if (OB_SUCC(ret)) { // repeated replica
      if (OB_UNLIKELY(idx < 0 || idx >= replicas_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index", KR(ret), K(idx), "replica_count", replicas_.count());
      } else if (OB_FAIL(replicas_.at(idx).assign(replica))) {
        LOG_WARN("fail to assign replicas_.at(idx)", KR(ret), K(idx), K(replica));
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(replicas_.push_back(replica))) {
        LOG_WARN("insert replica failed", KR(ret), K(replica));
      }
    } else {
      LOG_WARN("find replica index failed", KR(ret), K(replica));
    }
  }
  return ret;
}

bool ObTabletInfo::is_self_replica(const ObTabletReplica &replica) const
{
  return replica.get_tenant_id() == tenant_id_
      && replica.get_tablet_id() == tablet_id_
      && replica.get_ls_id() == ls_id_;
}

int ObTabletInfo::filter(const ObTabletReplicaFilter &filter)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "tablet_info", *this);
  }
  for (int64_t i = replicas_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    bool pass = true;
    if (OB_FAIL(filter.check(replicas_.at(i), pass))) {
      LOG_WARN("filter replica failed", K(ret), "replica", replicas_.at(i));
    } else {
      if (!pass) {
        if (OB_FAIL(replicas_.remove(i))) {
          LOG_WARN("remove replica failed", K(ret), "idx", i);
        }
      }
    }
  }
  return ret;
}

int ObTabletInfo::find_replica_idx_(const ObTabletReplica &replica, int64_t &idx) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!is_valid()
      || !replica.is_valid()
      || !is_self_replica(replica))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this), K(replica));
  } else {
    ARRAY_FOREACH_X(replicas_, i, cnt, OB_ENTRY_NOT_EXIST == ret) {
      if (replica.get_server() == replicas_.at(i).get_server()) {
        idx = i;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObTabletToLSInfo::init(
    const common::ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const uint64_t table_id,
    const int64_t transfer_seq) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      !tablet_id.is_valid()
      || !ls_id.is_valid()
      || OB_INVALID_ID == table_id
      || transfer_seq <= OB_INVALID_TRANSFER_SEQ)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init with invalid argument",
        KR(ret), K(tablet_id), K(ls_id), K(table_id), K(transfer_seq));
  } else {
    tablet_id_ = tablet_id;
    ls_id_ = ls_id;
    table_id_ = table_id;
    transfer_seq_ = transfer_seq;
  }
  return ret;
}

int ObTabletToLSInfo::assign(const ObTabletToLSInfo &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    table_id_ = other.table_id_;
    transfer_seq_ = other.transfer_seq_;
  }
  return ret;
}

ObTabletTablePair::ObTabletTablePair()
  : tablet_id_(), table_id_(OB_INVALID_ID)
{}

ObTabletTablePair::ObTabletTablePair(
  const common::ObTabletID &tablet_id,
  const uint64_t table_id)
  : tablet_id_(tablet_id), table_id_(table_id)
{}

ObTabletTablePair::~ObTabletTablePair()
{}

int ObTabletTablePair::init(
  const common::ObTabletID &tablet_id,
  const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  table_id_ = table_id;
  return ret;
}

void ObTabletTablePair::reset()
{
  tablet_id_.reset();
  table_id_ = OB_INVALID_ID;
}

bool ObTabletTablePair::is_valid() const
{
  return tablet_id_.is_valid() && OB_INVALID_ID != table_id_;
}

int ObTabletLSPair::init(
  const common::ObTabletID &tablet_id,
  const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arugments", KR(ret), K(tablet_id), K(ls_id));
  } else {
    tablet_id_ = tablet_id;
    ls_id_ = ls_id;
  }
  return ret;
}

int ObTabletLSPair::assign(const ObTabletLSPair &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

void ObTabletLSPair::reset()
{
  tablet_id_.reset();
  ls_id_.reset();
}

bool ObTabletLSPair::is_valid() const
{
  return tablet_id_.is_valid() && ls_id_.is_valid();
}

bool ObTabletLSPair::operator==(const ObTabletLSPair &other) const
{
  return tablet_id_ == other.tablet_id_
      && ls_id_ == other.ls_id_;
}

uint64_t ObTabletLSPair::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  return hash_val;
}

} // end namespace share
} // end namespace oceanbase
