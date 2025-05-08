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
#include "lib/ob_define.h"
#include "share/ob_ls_id.h"
#include "ob_tablet_binding_mds_user_data.h"

namespace oceanbase
{
namespace obrpc
{
struct ObBatchCreateTabletArg;
struct ObBatchRemoveTabletArg;
struct ObCreateTabletInfo;
}

namespace share
{
class SCN;
}

namespace rootserver
{
class ObDDLService;
class ObDDLSQLTransaction;
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

class ObBatchUnbindTabletArg final
{
public:
  ObBatchUnbindTabletArg();
  ~ObBatchUnbindTabletArg() {}
  int assign(const ObBatchUnbindTabletArg &other);
  inline bool is_redefined() const { return schema_version_ != OB_INVALID_VERSION; }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(schema_version), K_(orig_tablet_ids), K_(hidden_tablet_ids), K_(is_write_defensive));
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
  bool is_write_defensive_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchUnbindTabletArg);
};

class ObBatchUnbindLobTabletArg final
{
public:
  ObBatchUnbindLobTabletArg();
  ~ObBatchUnbindLobTabletArg() {}
  int assign(const ObBatchUnbindLobTabletArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(data_tablet_ids));
  bool is_valid() { return true; }
  OB_UNIS_VERSION_V(1);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSArray<ObTabletID> data_tablet_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchUnbindLobTabletArg);
};

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
  static int modify_tablet_binding_new_mds(ObLS &ls, const ObTabletID &tablet_id, const share::SCN &replay_scn, mds::BufferCtx &ctx, const bool for_old_mds, F op);
  static int has_lob_tablets(const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info, bool &has_lob);
  static int get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle);

  static int build_single_table_write_defensive(rootserver::ObDDLService &ddl_service,
                                                const ObTableSchema &table_schema,
                                                const int64_t schema_version,
                                                rootserver::ObDDLSQLTransaction &trans);
private:
  const ObLS &ls_;
  const transaction::ObMulSourceDataNotifyArg &trans_flags_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletBindingHelper);
};

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

class ObTabletUnbindLobMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int register_process(ObBatchUnbindTabletArg &arg, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
private:
  static int modify_tablet_binding_for_unbind_lob_(const ObBatchUnbindLobTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx);
};

struct ClearLobTabletId
{
public:
  ClearLobTabletId() {}
  ClearLobTabletId& operator=(const ClearLobTabletId&) = delete;
  int operator()(ObTabletBindingMdsUserData &data) const
  {
    // lob_meta_tablet and lob_piece_tablet_id need to be cleaned up at the same time
    data.lob_meta_tablet_id_.reset();
    data.lob_piece_tablet_id_.reset();
    return OB_SUCCESS;
  }
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER
