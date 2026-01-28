//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX MDS
#include "storage/compaction_ttl/ob_ttl_filter_info_mds_helper.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "rootserver/compaction_ttl/ob_compaction_ttl_service.h"
#include "share/compaction/ob_sync_mds_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{
using namespace rootserver;
namespace storage
{
using namespace mds;

class ObTTLFilterInfoClogReplayExecutor : public ObMdsClogReplayExecutor<ObTTLFilterInfoArg>
{
public:
  ObTTLFilterInfoClogReplayExecutor(ObTTLFilterInfoArg &arg)
    : ObMdsClogReplayExecutor<ObTTLFilterInfoArg>(arg)
  {}
protected:
  virtual int do_replay_(ObTabletHandle &tablet_handle) override;
};

int ObTTLFilterInfoClogReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);
  if (OB_FAIL(replay_to_mds_table_(tablet_handle, static_cast<const ObTTLFilterInfoArg&>(arg_), user_ctx, scn_))) {
    COMMON_LOG(WARN, "failed to replay to tablet", K(ret));
  }
  return ret;
}

int ObTTLFilterInfoMdsHelper::on_register(
  const char* buf,
  const int64_t len,
  BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTTLFilterInfoArg arg;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);

  if (OB_UNLIKELY(nullptr == buf || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_id_array_.count(); ++i) {
      if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->set_ttl_filter_info(
              arg.tablet_id_array_.at(i), arg, user_ctx, 0 /*lock_timeout_us*/))) {
        LOG_WARN("failed to get tablet", K(ret), K(arg.ls_id_), K(arg.tablet_id_array_.at(i)));
      } else {
        LOG_INFO("[COMPACTION TTL] on_register for ObTTLFilterInfoArg", K(ret), K(arg), K(user_ctx.get_writer()));
      }
    } // for
  }
  return ret;
}

int ObTTLFilterInfoMdsHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTTLFilterInfoArg arg;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = arg.tablet_id_array_.at(i);
      ObTTLFilterInfoClogReplayExecutor executor(arg);
      if (OB_FAIL(executor.init(ctx, scn))) {
        LOG_WARN("failed to init reply executor", K(ret), K(arg), K(ctx), K(scn));
      } else if (OB_FAIL(executor.execute(scn, arg.ls_id_, tablet_id))) {
        LOG_WARN("failed to executor", K(ret), K(arg), K(ctx), K(scn));
      } else {
        LOG_INFO("[COMPACTION TTL] on_replay for ObTTLFilterInfoArg", K(ret), K(arg), K(tablet_id));
      }
    } // for
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
