//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX MDS
#include "storage/truncate_info/ob_truncate_info_mds_helper.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "rootserver/truncate_info/ob_truncate_info_service.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{
using namespace rootserver;
namespace storage
{
using namespace mds;
class ObTruncateInfoClogReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTruncateInfoClogReplayExecutor(rootserver::ObTruncateTabletArg &truncate_arg);
  int init(mds::BufferCtx &user_ctx, const share::SCN &scn);
protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }
  int do_replay_(ObTabletHandle &tablet_handle) override;
  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }
private:
  mds::BufferCtx *user_ctx_;
  rootserver::ObTruncateTabletArg &truncate_arg_;
  share::SCN scn_;
};

int ObTruncateInfoMdsHelper::on_register(
  const char* buf,
  const int64_t len,
  BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObTruncateTabletArg arg;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);

  if (OB_UNLIKELY(nullptr == buf || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(tmp_allocator, buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(arg));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->set_truncate_info(arg, user_ctx, 0/*lock_timeout_us*/))) {
    LOG_WARN("failed to set truncate info", K(ret), K(arg.ls_id_), K(arg.index_tablet_id_));
  } else {
    LOG_INFO("[TRUNCATE INFO] on_register for ObTruncateTabletArg", K(ret), K(arg), K(user_ctx.get_writer()));
  }
  return ret;
}

int ObTruncateInfoMdsHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  rootserver::ObTruncateTabletArg arg;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(tmp_allocator, buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else {
    ObTruncateInfoClogReplayExecutor executor(arg);
    if (OB_FAIL(executor.init(ctx, scn))) {
      LOG_WARN("failed to init reply executor", K(ret), K(arg), K(ctx), K(scn));
    } else if (OB_FAIL(executor.execute(scn, arg.ls_id_, arg.index_tablet_id_))) {
      LOG_WARN("failed to executor", K(ret), K(arg), K(ctx), K(scn));
    } else {
      LOG_INFO("[TRUNCATE INFO] on_replay for ObTruncateTabletArg", K(ret), K(arg));
    }
  }
  return ret;
}

ObTruncateInfoClogReplayExecutor::ObTruncateInfoClogReplayExecutor(
    rootserver::ObTruncateTabletArg &truncate_arg)
    : user_ctx_(nullptr),
      truncate_arg_(truncate_arg),
      scn_()
{
}

int ObTruncateInfoClogReplayExecutor::init(
  mds::BufferCtx &user_ctx, 
  const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!truncate_arg_.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_arg_), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObTruncateInfoClogReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);
  if (OB_FAIL(replay_to_mds_table_(tablet_handle, static_cast<const rootserver::ObTruncateTabletArg &>(truncate_arg_), user_ctx, scn_))) {
    LOG_WARN("failed to replay to truncate info", K(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
