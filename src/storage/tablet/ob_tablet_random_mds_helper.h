/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_RANDOM_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_RANDOM_MDS_HELPER

#include <stdint.h>
#include "lib/worker.h"
#include "lib/container/ob_iarray.h"
#include "storage/tx/ob_trans_define.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_rpc_struct.h"
#include "src/storage/tablet/ob_tablet_random_mds_user_data.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
class SCN;
}

namespace common
{
class ObTabletID;
}


namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObTabletRandomMdsArg final
{
public:
  const static int64_t BATCH_TABLET_CNT = 8192;
  OB_UNIS_VERSION_V(1);

public:
  ObTabletRandomMdsArg()
    : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), random_info_datas_(), tablet_ids_(), autoinc_seq_arg_()
    {}
  ~ObTabletRandomMdsArg() {}
  bool is_valid() const;
  int assign(const ObTabletRandomMdsArg &other);
  void reset();
  int init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObTabletRandomMdsUserData> &random_info_datas,
    const obrpc::ObBatchSetTabletAutoincSeqArg &autoinc_seq_arg);
  TO_STRING_KV(K_(ls_id), K_(tenant_id), K_(random_info_datas), K_(tablet_ids), K_(autoinc_seq_arg));

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSEArray<ObTabletRandomMdsUserData, 3> random_info_datas_;
  ObSEArray<ObTabletID, 3> tablet_ids_;
  obrpc::ObBatchSetTabletAutoincSeqArg autoinc_seq_arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletRandomMdsArg);
};

struct ObModifyAutoRandomSizeOp final
{
public:
  ObModifyAutoRandomSizeOp(const int64_t auto_random_size) : auto_random_size_(auto_random_size) {}
  ~ObModifyAutoRandomSizeOp() = default;
  int operator()(ObTabletRandomMdsUserData &data);
public:
  int64_t auto_random_size_;
};

struct ObModifyInactiveOp final
{
public:
  ObModifyInactiveOp() {}
  ~ObModifyInactiveOp() = default;
  int operator()(ObTabletRandomMdsUserData &data);
};


class ObTabletRandomMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN scn, mds::BufferCtx &ctx);
  static int process(const char* buf, const int64_t len, const share::SCN &scn,
                     mds::BufferCtx &ctx, bool for_replay);

  static int register_mds(const ObTabletRandomMdsArg &arg,
                          ObMySQLTransaction &trans);

  static int set_tablet_random_mds(const share::ObLSID &ls_id,
                                  const ObTabletID &tablet_id,
                                  const share::SCN &replay_scn,
                                  const ObTabletRandomMdsUserData &data,
                                  mds::BufferCtx &ctx);
  static int modify(const ObTabletRandomMdsArg &arg,
                    const share::SCN &scn,
                    mds::BufferCtx &ctx);
  static int get_valid_timeout(const int64_t abs_timeout_us, int64_t &timeout_us);
  static int get_random_data_with_timeout(const ObTablet &tablet, ObTabletRandomMdsUserData &random_data, const int64_t abs_timeout_us);
  static int batch_get_tablet_random(
    const int64_t abs_timeout_us,
    const obrpc::ObBatchGetTabletRandomArg &arg,
    obrpc::ObBatchGetTabletRandomRes &res);
  static int get_tablet_random_mds_by_rpc(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletRandomMdsUserData> &datas);
  static int set_auto_random_size_for_create(
    const uint64_t tenant_id,
    const obrpc::ObBatchCreateTabletArg &create_arg,
    const ObIArray<int64_t> &auto_random_size_arr,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);
  static int set_autoinc_seq_for_create(
    const ObTableSchema &table_schema,
    const int64_t prev_high_bound_val,
    ObMySQLTransaction &trans);
  static int modify_auto_random_size(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t auto_random_size,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);
  static int modify_inactive(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);
  template<typename F>
  static int modify_tablet_random_mds_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_RANDOM_MDS_HELPER
