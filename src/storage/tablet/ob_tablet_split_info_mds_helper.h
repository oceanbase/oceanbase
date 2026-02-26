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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_INFO_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_INFO_MDS_HELPER

#include <stdint.h>
#include "lib/worker.h"
#include "lib/container/ob_iarray.h"
#include "storage/tx/ob_trans_define.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_rpc_struct.h"
#include "src/storage/tablet/ob_tablet_split_info_mds_user_data.h"

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

class ObTabletSplitInfoMdsArg final
{
public:
  OB_UNIS_VERSION_V(1);

public:
  ObTabletSplitInfoMdsArg()
    : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), split_info_datas_()
    {}
  ~ObTabletSplitInfoMdsArg() {}
  bool is_valid() const;
  int assign(const ObTabletSplitInfoMdsArg &other);
  void reset();
  int init(
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObTabletSplitInfoMdsUserData> &split_datas);
  static int set_tablet_split_mds(const share::ObLSID &ls_id, const ObTabletID &tablet_id, const share::SCN &replay_scn, const ObTabletSplitMdsUserData &data, mds::BufferCtx &ctx);

  TO_STRING_KV(K_(ls_id), K_(tenant_id), K_(split_info_datas));

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSEArray<ObTabletSplitInfoMdsUserData, 3> split_info_datas_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitInfoMdsArg);
};


class ObTabletSplitInfoMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN scn, mds::BufferCtx &ctx);
  static int process(const char* buf, const int64_t len, const share::SCN &scn,
                     mds::BufferCtx &ctx, bool for_replay);

  static int register_mds(const ObTabletSplitInfoMdsArg &arg,
                          ObMySQLTransaction &trans);

  static int set_tablet_split_mds(const share::ObLSID &ls_id,
                                  const ObTabletID &tablet_id,
                                  const share::SCN &replay_scn,
                                  const ObTabletSplitInfoMdsUserData &data,
                                  mds::BufferCtx &ctx);
  static int modify(const ObTabletSplitInfoMdsArg &arg,
                    const share::SCN &scn,
                    mds::BufferCtx &ctx);

};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_SPLIT_INFO_MDS_HELPER