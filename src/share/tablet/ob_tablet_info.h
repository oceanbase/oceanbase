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

#ifndef OCEANBASE_SHARE_OB_TABLET_INFO
#define OCEANBASE_SHARE_OB_TABLET_INFO

#include "common/ob_tablet_id.h" // ObTabletID
#include "lib/net/ob_addr.h" // ObAddr
#include "share/ob_ls_id.h" // ObLSID
#include "share/transfer/ob_transfer_info.h" // OB_INVALID_TRANSFER_SEQ

namespace oceanbase
{
namespace share
{
class ObTabletReplicaFilter;

class ObTabletReplica
{
public:
  enum ScnStatus
  {
    SCN_STATUS_IDLE = 0,
    SCN_STATUS_ERROR,
    SCN_STATUS_MAX
  };

  ObTabletReplica();
  virtual ~ObTabletReplica();
  void reset();
  inline bool is_valid() const
  {
    return tablet_id_.is_valid_with_tenant(tenant_id_)
        && ls_id_.is_valid_with_tenant(tenant_id_)
        && server_.is_valid()
        && snapshot_version_ >= 0
        && data_size_ >= 0
        && required_size_ >= 0
        && report_scn_ >= 0
        && is_status_valid(status_);
  }
  inline bool primary_keys_are_valid() const
  {
    return tablet_id_.is_valid_with_tenant(tenant_id_)
        && server_.is_valid()
        && ls_id_.is_valid_with_tenant(tenant_id_);
  }
  int assign(const ObTabletReplica &other);
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline const common::ObAddr &get_server() const { return server_; }
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline int64_t get_snapshot_version() const { return snapshot_version_; }
  inline int64_t get_data_size() const { return data_size_; }
  inline int64_t get_required_size() const { return required_size_; }
  inline int64_t get_report_scn() const { return report_scn_; }
  inline ScnStatus get_status() const { return status_; }
  int init(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const common::ObAddr &server,
      const int64_t snapshot_version,
      const int64_t data_size,
      const int64_t required_size,
      const int64_t report_scn,
      const ScnStatus status);
  void fake_for_diagnose(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id);
  bool is_equal_for_report(const ObTabletReplica &other) const;
  static bool is_status_valid(const ScnStatus status)
  {
    return status >= SCN_STATUS_IDLE && status < SCN_STATUS_MAX;
  }
  TO_STRING_KV(
      K_(tenant_id),
      K_(tablet_id),
      K_(ls_id),
      K_(server),
      K_(snapshot_version),
      K_(data_size),
      K_(required_size),
      K_(report_scn),
      K_(status));
private:
  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  common::ObAddr server_;
  int64_t snapshot_version_;
  int64_t data_size_; // load balancing releated
  int64_t required_size_; // load balancing releated
  // below: tablet level member for compaction
  int64_t report_scn_;
  ScnStatus status_;
};

class ObTabletInfo
{
public:
  ObTabletInfo();
  explicit ObTabletInfo(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const ObLSID &ls_id,
      const ObArray<ObTabletReplica> &replicas);
  virtual ~ObTabletInfo();
  void reset();
  inline bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && tablet_id_.is_valid_with_tenant(tenant_id_)
        && ls_id_.is_valid_with_tenant(tenant_id_)
        && replicas_.count() > 0;
  }
  int assign(const ObTabletInfo &other);
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline const ObLSID &get_ls_id() const { return ls_id_; }
  inline const common::ObArray<ObTabletReplica> &get_replicas() const { return replicas_; }
  int64_t replica_count() const { return replicas_.count(); }
  int init(const uint64_t tenant_id,
           const common::ObTabletID &tablet_id,
           const ObLSID &ls_id,
           const common::ObIArray<ObTabletReplica> &replicas);
  int init_by_replica(const ObTabletReplica &replica);
  int add_replica(const ObTabletReplica &replica);
  bool is_self_replica(const ObTabletReplica &replica) const;
  int filter(const ObTabletReplicaFilter &filter);
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(replicas));
private:
  int find_replica_idx_(const ObTabletReplica &replica, int64_t &idx) const;

  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;
  ObLSID ls_id_;
  common::ObArray<ObTabletReplica> replicas_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletInfo);
};

// ObTabletToLSInfo is used to store info for __all_tablet_to_ls.
// Structure: <tablet_id, ls_id, table_id, transfer_seq>
class ObTabletToLSInfo
{
public:
  ObTabletToLSInfo()
      : tablet_id_(), ls_id_(), table_id_(OB_INVALID_ID), transfer_seq_(OB_INVALID_TRANSFER_SEQ) {}
  explicit ObTabletToLSInfo(
      const common::ObTabletID &tablet_id,
      const ObLSID &ls_id,
      const uint64_t table_id,
      const int64_t transfer_seq)
      : tablet_id_(tablet_id), ls_id_(ls_id), table_id_(table_id), transfer_seq_(transfer_seq)
  {
  }
  ~ObTabletToLSInfo() { reset(); }
  inline void reset()
  {
    tablet_id_.reset();
    ls_id_.reset();
    table_id_ = OB_INVALID_ID;
    transfer_seq_ = OB_INVALID_TRANSFER_SEQ;
  }
  inline bool is_valid() const
  {
    return tablet_id_.is_valid()
        && ls_id_.is_valid()
        && OB_INVALID_ID != table_id_
        && transfer_seq_ > OB_INVALID_TRANSFER_SEQ;
  }
  inline bool operator==(const ObTabletToLSInfo &other) const
  {
    return other.tablet_id_ == tablet_id_
        && other.ls_id_ == ls_id_
        && other.table_id_ == table_id_
        && other.transfer_seq_ == transfer_seq_;
  }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline const ObLSID &get_ls_id() const { return ls_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline int64_t get_transfer_seq() const { return transfer_seq_; }
  int init(
      const common::ObTabletID &tablet_id,
      const ObLSID &ls_id,
      const uint64_t table_id,
      const int64_t transfer_seq);
  int assign(const ObTabletToLSInfo &other);
  TO_STRING_KV(K_(tablet_id), K_(ls_id), K_(table_id), K_(transfer_seq));
private:
  common::ObTabletID tablet_id_;
  ObLSID ls_id_;
  uint64_t table_id_;
  int64_t transfer_seq_;
};

class ObTabletTablePair
{
public:
  ObTabletTablePair();
  ObTabletTablePair(const common::ObTabletID &tablet_id, const uint64_t table_id);
  ~ObTabletTablePair();

  int init(const ObTabletID &tablet_id, const uint64_t table_id);
  void reset();
  bool is_valid() const;
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  uint64_t get_table_id() const { return table_id_; }
  TO_STRING_KV(K_(tablet_id), K_(table_id));
private:
  common::ObTabletID tablet_id_;
  uint64_t table_id_;
};

class ObTabletLSPair
{
public:
  ObTabletLSPair() {}
  explicit ObTabletLSPair(const common::ObTabletID &tablet_id, const ObLSID &ls_id)
      : tablet_id_(tablet_id), ls_id_(ls_id) {}
  explicit ObTabletLSPair(const int64_t tablet_id, const int64_t ls_id)
      : tablet_id_(tablet_id), ls_id_(ls_id) {}
  ~ObTabletLSPair() {}
  int init(const ObTabletID &tablet_id, const ObLSID &ls_id);
  int assign(const ObTabletLSPair &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  const ObLSID &get_ls_id() const { return ls_id_; }
  bool operator==(const ObTabletLSPair &other) const;
  TO_STRING_KV(K_(tablet_id), K_(ls_id));
private:
  common::ObTabletID tablet_id_;
  ObLSID ls_id_;
};

} // end namespace share
} // end namespace oceanbase
#endif
