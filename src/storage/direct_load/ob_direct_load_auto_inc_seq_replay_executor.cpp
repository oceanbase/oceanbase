/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/direct_load/ob_direct_load_auto_inc_seq_replay_executor.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::storage::mds;

namespace oceanbase
{
namespace storage
{

ObDirectLoadAutoIncSeqReplayExecutor::ObDirectLoadAutoIncSeqReplayExecutor()
 : logservice::ObTabletReplayExecutor(), user_ctx_(nullptr), seq_data_(nullptr)
{
}

int ObDirectLoadAutoIncSeqReplayExecutor::init(
    BufferCtx &user_ctx,
    const ObDirectLoadAutoIncSeqData &seq_data,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scn is invalid", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    seq_data_ = &seq_data;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}
int ObDirectLoadAutoIncSeqReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    MdsCtx &user_ctx = static_cast<MdsCtx&>(*user_ctx_);
    ObTablet *tablet = tablet_handle.get_obj();
    if (OB_ISNULL(tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is nullptr", KR(ret));
    } else if (OB_UNLIKELY(tablet->is_ls_inner_tablet())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("inner tablet has no mds table", KR(ret));
    } else {
      ObLSService *ls_service = MTL(ObLSService *);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      const ObLSID &ls_id = tablet->get_tablet_meta().ls_id_;
      const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
      if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
        LOG_WARN("fail to get ls", KR(ret));
      } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls handle is invalid", KR(ret));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is nullptr", KR(ret));
      } else if (OB_FAIL(ls->get_tablet_svr()->replay_set_direct_load_auto_inc_seq(
                           tablet_id, *seq_data_, user_ctx, scn_))) {
        LOG_WARN("fail to replay set inc major auto inc seq", KR(ret), K(tablet_id), K(scn_));
      }
    }
  }
  return ret;
}

bool ObDirectLoadAutoIncSeqReplayExecutor::is_replay_update_tablet_status_() const
{
  return true;
}

bool ObDirectLoadAutoIncSeqReplayExecutor::is_replay_update_mds_table_() const
{
  return true;
}

} // namespace storage
} // namespace oceanbase