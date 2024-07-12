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

#ifndef OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/ob_define.h"
#include "share/ob_ls_id.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"

namespace oceanbase
{
namespace obrpc
{
struct ObBatchCreateTabletArg;
struct ObBatchRemoveTabletArg;
struct ObCreateTabletInfo;
struct ObBatchGetTabletBindingArg;
struct ObBatchGetTabletBindingRes;
}

namespace share
{
class SCN;
}

namespace transaction
{
struct ObMulSourceDataNotifyArg;
class ObTransID;
}

namespace storage
{
namespace mds
{
struct BufferCtx;
class MdsCtx;
}

class ObLS;
class ObLSHandle;
class ObTabletHandle;
class ObTabletTxMultiSourceDataUnit;
class ObTabletMapKey;

// deprecated
class ObBatchUnbindTabletArg final
{
public:
  ObBatchUnbindTabletArg();
  ~ObBatchUnbindTabletArg() {}
  int assign(const ObBatchUnbindTabletArg &other);
  inline bool is_redefined() const { return schema_version_ != OB_INVALID_VERSION; }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(schema_version), K_(orig_tablet_ids), K_(hidden_tablet_ids));
  bool is_valid() { return true; }
  static int is_old_mds(const char *buf, const int64_t len, bool &is_old_mds);
  static int skip_array_len(const char *buf, int64_t data_len, int64_t &pos);
  OB_UNIS_VERSION_V(1);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t schema_version_;
  ObSArray<ObTabletID> orig_tablet_ids_;
  ObSArray<ObTabletID> hidden_tablet_ids_;
  bool is_old_mds_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchUnbindTabletArg);
};

// deprecated
class ObTabletBindingHelper final
{
public:
  ObTabletBindingHelper(const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags)
    : ls_(ls), trans_flags_(trans_flags) {}
  ~ObTabletBindingHelper() {}

  // create tablet by new mds
  static int modify_tablet_binding_for_new_mds_create(const obrpc::ObBatchCreateTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx);
  static int bind_hidden_tablet_to_orig_tablet(ObLS &ls, const obrpc::ObCreateTabletInfo &info, const share::SCN &replay_scn, mds::BufferCtx &ctx, const bool for_old_mds);
  static int bind_lob_tablet_to_data_tablet(ObLS &ls, const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info, const share::SCN &replay_scn, mds::BufferCtx &ctx);
  // TODO (lihongqin.lhq) delete get_tablet_for_new_mds
  static int get_tablet_for_new_mds(const ObLS &ls, const ObTabletID &tablet_id, const share::SCN &replay_scn, ObTabletHandle &handle);

  // common
  template<typename F>
  static int modify_tablet_binding_new_mds(ObLS &ls, const ObTabletID &tablet_id, const share::SCN &replay_scn, mds::BufferCtx &ctx, const bool for_old_mds, F &&op);
  static int has_lob_tablets(const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info, bool &has_lob);
  static int get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle);
private:
  const ObLS &ls_;
  const transaction::ObMulSourceDataNotifyArg &trans_flags_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletBindingHelper);
};

// deprecated
class ObTabletUnbindMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int register_process(ObBatchUnbindTabletArg &arg, mds::BufferCtx &ctx);
  static int on_commit_for_old_mds(const char* buf, const int64_t len, const transaction::ObMulSourceDataNotifyArg &notify_arg);
  static int replay_process(ObBatchUnbindTabletArg &arg, const share::SCN &scn, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
private:
  static int unbind_hidden_tablets_from_orig_tablets(ObLS &ls, const ObBatchUnbindTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx);
  static int set_redefined_versions_for_hidden_tablets(ObLS &ls, const ObBatchUnbindTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx);
  static int modify_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx);
};

class ObTabletBindingMdsArg final
{
public:
  OB_UNIS_VERSION(1);
public:
  // arg with such tablet cnt cannot be more than mds buffer limit (1.5M)
  const static int64_t BATCH_TABLET_CNT = 8192;
  ObTabletBindingMdsArg();
  ~ObTabletBindingMdsArg() {}
  bool is_valid() const;
  void reset();
  int assign(const ObTabletBindingMdsArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_ids), K_(binding_datas));
public:
  uint64_t tenant_id_;
  uint64_t tenant_data_version_;
  share::ObLSID ls_id_;
  ObSArray<ObTabletID> tablet_ids_;
  ObSArray<ObTabletBindingMdsUserData> binding_datas_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletBindingMdsArg);
};

class ObBindHiddenTabletToOrigTabletOp final
{
public:
  ObBindHiddenTabletToOrigTabletOp() : info_(nullptr) {}
  ObBindHiddenTabletToOrigTabletOp(const obrpc::ObCreateTabletInfo &info) : info_(&info) {}
  ~ObBindHiddenTabletToOrigTabletOp() = default;
  int assign(const ObBindHiddenTabletToOrigTabletOp &other);
  int operator()(ObTabletBindingMdsUserData &data);
  TO_STRING_KV(KPC_(info));
private:
  const obrpc::ObCreateTabletInfo *info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBindHiddenTabletToOrigTabletOp);
};

class ObBindLobTabletToDataTabletOp final
{
public:
  ObBindLobTabletToDataTabletOp() : arg_(nullptr), info_(nullptr) {}
  ObBindLobTabletToDataTabletOp(const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info)
    : arg_(&arg), info_(&info) {}
  int assign(const ObBindLobTabletToDataTabletOp &other);
  ~ObBindLobTabletToDataTabletOp() = default;
  int operator()(ObTabletBindingMdsUserData &data);
  TO_STRING_KV(KPC_(arg), KPC_(info));
private:
  const obrpc::ObBatchCreateTabletArg *arg_;
  const obrpc::ObCreateTabletInfo *info_;
};

class ObUnbindHiddenTabletFromOrigTabletOp final
{
public:
  ObUnbindHiddenTabletFromOrigTabletOp(const int64_t schema_version)
    : schema_version_(schema_version) {}
  ~ObUnbindHiddenTabletFromOrigTabletOp() = default;
  int operator()(ObTabletBindingMdsUserData &data);
private:
  int64_t schema_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUnbindHiddenTabletFromOrigTabletOp);
};

class ObSetRwDefensiveOp final
{
public:
  ObSetRwDefensiveOp(const int64_t schema_version)
    : schema_version_(schema_version) {}
  ~ObSetRwDefensiveOp() = default;
  int operator()(ObTabletBindingMdsUserData &data);
private:
  int64_t schema_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetRwDefensiveOp);
};

template<typename F>
struct ModifyBindingByOp final
{
  ModifyBindingByOp(F &&op) : op_(op) {}
  ~ModifyBindingByOp() = default;
  int operator()(const int64_t i, ObTabletBindingMdsUserData &data) { return op_(data); }
  F &&op_;
};

template<typename F>
struct ModifyBindingByOps final
{
  ModifyBindingByOps(ObIArray<F> &ops) : ops_(ops) {}
  ~ModifyBindingByOps() = default;
  int operator()(const int64_t i, ObTabletBindingMdsUserData &data) { return ops_.at(i)(data); }
  ObIArray<F> &ops_;
};

struct LSTabletCmp final
{
  bool operator()(const std::pair<share::ObLSID, ObTabletID> &lhs, const std::pair<share::ObLSID, ObTabletID> &rhs) {
    return lhs.first != rhs.first ? lhs.first < rhs.first : lhs.second < rhs.second;
  }
};

class ObTabletBindingMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);

public:
  static int get_sorted_ls_tablets(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    ObArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablets,
    ObMySQLTransaction &trans);
  static int batch_get_tablet_binding(
    const int64_t abs_timeout_us,
    const obrpc::ObBatchGetTabletBindingArg &arg,
    obrpc::ObBatchGetTabletBindingRes &res);
  static int get_tablet_binding_mds_by_rpc(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletBindingMdsUserData> &datas);
  static int modify_tablet_binding_for_create(
    const uint64_t tenant_id,
    const obrpc::ObBatchCreateTabletArg &arg,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);
  static int modify_tablet_binding_for_unbind(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &orig_tablet_ids,
    const ObIArray<ObTabletID> &hidden_tablet_ids,
    const int64_t redefined_schema_version,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);
  static int modify_tablet_binding_for_rw_defensive(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t schema_version,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);

private:
  template<typename F>
  static int modify_tablet_binding_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans);
  template<typename F>
  static int modify_tablet_binding_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans);
  static int register_mds_(
    const ObTabletBindingMdsArg &arg,
    ObMySQLTransaction &trans);

  static int modify_(const ObTabletBindingMdsArg &arg, const share::SCN &scn, mds::BufferCtx &ctx);
  static int set_tablet_binding_mds_(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    const ObTabletBindingMdsUserData &data,
    mds::BufferCtx &ctx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER
