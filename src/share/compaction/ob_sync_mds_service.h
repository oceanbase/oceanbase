//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_TTL_OB_SYNC_MDS_SERVICE_H_
#define OB_SHARE_COMPACTION_TTL_OB_SYNC_MDS_SERVICE_H_
#include <stdint.h>
#include "common/ob_timeout_ctx.h"
#include "share/ob_share_util.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "observer/ob_inner_sql_connection.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
namespace oceanbase
{
namespace share
{

class ObSyncMDSService
{
public:
  template<typename T>
  static int register_mds(
    observer::ObInnerSQLConnection &conn,
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const T &arg,
    const transaction::ObTxDataSourceType &type);

  template<typename T>
  static int retry_register_mds(
    observer::ObInnerSQLConnection &conn,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const T &arg,
    const transaction::ObTxDataSourceType &type,
    const char *buf,
    const int64_t buf_len);
private:
  static const int64_t SLEEP_INTERVAL = 100 * 1000L; // 100ms
  static bool need_retry_errno(const int ret)
  {
    return is_location_service_renew_error(ret) || OB_NOT_MASTER == ret;
  }
};

template<typename T>
int ObSyncMDSService::register_mds(
  observer::ObInnerSQLConnection &conn,
  ObIAllocator &allocator,
  const uint64_t tenant_id,
  const T &arg,
  const transaction::ObTxDataSourceType &type)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = arg.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = (char *)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail alloc memory", KR(ret), K(buf_len));
  } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "fail to serialize", KR(ret), K(arg));
  } else if (OB_FAIL(ObSyncMDSService::retry_register_mds<T>(
      conn, tenant_id, arg.ls_id_, arg,
      type, buf, buf_len))) {
    COMMON_LOG(WARN, "fail to register mds", KR(ret), K(tenant_id), K(arg));
  }
  return ret;
}

template<typename T>
int ObSyncMDSService::retry_register_mds(
  observer::ObInnerSQLConnection &conn,
  const uint64_t tenant_id,
  const ObLSID &ls_id,
  const T &arg,
  const transaction::ObTxDataSourceType &type,
  const char *buf,
  const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t default_timeout_ts = GCONF.rpc_timeout;
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout_ts))) {
    COMMON_LOG(WARN, "fail to set timeout ctx", KR(ret), K(default_timeout_ts));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    do {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        COMMON_LOG(WARN, "already timeout", KR(ret), K(ctx));
      } else if (OB_FAIL(conn.register_multi_data_source(
                    tenant_id, ls_id, type,
                    buf, buf_len))) {
        if (need_retry_errno(ret)) {
          COMMON_LOG(INFO, "fail to register_tx_data, try again", KR(ret), K(tenant_id), K(type));
          ob_usleep(SLEEP_INTERVAL);
        } else {
          COMMON_LOG(WARN, "fail to register_tx_data", KR(ret), K(type), K(buf), K(buf_len));
        }
      }
    } while (need_retry_errno(ret));
    if (OB_SUCC(ret)) {
      COMMON_LOG(INFO, "[MDS] success to register mds", KR(ret), K(buf_len), K(type),
        "cost_ts", ObTimeUtility::current_time() - start_time);
    }
  }
  return ret;
}

template<typename T>
class ObMdsClogReplayExecutor : public logservice::ObTabletReplayExecutor
{
public:
  ObMdsClogReplayExecutor(T &arg)
    : user_ctx_(nullptr),
      arg_(arg),
      scn_()
  {}
  int init(mds::BufferCtx &user_ctx, const share::SCN &scn);
protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }
  virtual int do_replay_(ObTabletHandle &tablet_handle) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }
protected:
  mds::BufferCtx *user_ctx_;
  T &arg_;
  share::SCN scn_;
};

template<typename T>
int ObMdsClogReplayExecutor<T>::init(mds::BufferCtx &user_ctx, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!arg_.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", KR(ret), K(arg_), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_TTL_OB_SYNC_MDS_SERVICE_H_
