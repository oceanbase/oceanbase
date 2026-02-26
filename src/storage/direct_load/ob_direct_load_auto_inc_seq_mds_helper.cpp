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

#define USING_LOG_PREFIX MDS
#include "storage/direct_load/ob_direct_load_auto_inc_seq_mds_helper.h"
#include "storage/direct_load/ob_direct_load_auto_inc_seq_data.h"
#include "storage/direct_load/ob_direct_load_auto_inc_seq_replay_executor.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::storage::mds;

namespace oceanbase
{
namespace storage
{

/*--------------- ObDirectLoadAutoIncSeqArg ---------------*/
ObDirectLoadAutoIncSeqArg::ObDirectLoadAutoIncSeqArg() {}

ObDirectLoadAutoIncSeqArg::~ObDirectLoadAutoIncSeqArg() {}

bool ObDirectLoadAutoIncSeqArg::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && seq_.is_valid();
}
OB_SERIALIZE_MEMBER(ObDirectLoadAutoIncSeqArg, ls_id_, tablet_id_, seq_);

/*--------------- ObDirectLoadAutoIncSeqMdsHelpler ---------------*/
int ObDirectLoadAutoIncSeqMdsHelpler::on_register(
    const char* buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  return process(buf, len, share::SCN(), ctx, false/*for_replay*/);
}

int ObDirectLoadAutoIncSeqMdsHelpler::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN scn,
    mds::BufferCtx &ctx)
{
  return process(buf, len, scn, ctx, true/*for_replay*/);
}

int ObDirectLoadAutoIncSeqMdsHelpler::process(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    bool for_replay)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObDirectLoadAutoIncSeqArg arg;
  if (OB_UNLIKELY(nullptr == buf || len <= 0 || (for_replay && !scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(len), K(for_replay), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), K(arg));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else {
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSTabletService *ls_tablet_service = nullptr;
    MdsCtx &user_ctx = static_cast<MdsCtx&>(ctx);
    ObDirectLoadAutoIncSeqData seq;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is nullptr", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret));
    } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls handle is invalid", KR(ret), K(ls_handle));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is nullptr", KR(ret), K(ls_handle));
    } else if (FALSE_IT(seq = arg.seq_)) {
    } else {
      if (!for_replay) {
        if (OB_FAIL(ls->get_tablet_svr()->set_direct_load_auto_inc_seq(arg.tablet_id_, seq, user_ctx, 0))) {
          if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
            ret = OB_EAGAIN;
          } else {
            LOG_WARN("fail to set inc major auto inc seq", KR(ret), K(arg));
          }
        }
      } else {
        ObDirectLoadAutoIncSeqReplayExecutor replay_executor;
        if (OB_FAIL(replay_executor.init(ctx, seq, scn))) {
          LOG_WARN("fail to init replay executor", KR(ret), K(seq), K(scn));
        } else if (OB_FAIL(replay_executor.execute(scn, arg.ls_id_, arg.tablet_id_))) {
          LOG_WARN("fail to execute replay inc major auto inc seq mds data", KR(ret), K(arg));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase