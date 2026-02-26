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

#include "storage/tablet/ob_tablet_create_mds_helper.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_helper.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tablet/ob_tablet_ddl_complete_replay_executor.h"
#include "storage/tablet/ob_tablet_inc_major_info_replay_executor.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#define USING_LOG_PREFIX MDS

using namespace oceanbase::observer;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
ObTabletDDLCompleteArg::ObTabletDDLCompleteArg():
  has_complete_(false), ls_id_(), tablet_id_(), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
  rec_scn_(share::SCN::min_scn()), start_scn_(share::SCN::min_scn()),data_format_version_(0), snapshot_version_(0), table_key_(),
  trans_id_(ObTabletDDLCompleteMdsUserDataKey::DDL_COMPLETE_TX_ID), storage_schema_(), allocator_()
{ }

ObTabletDDLCompleteArg::~ObTabletDDLCompleteArg()
{
  storage_schema_ = nullptr;
  allocator_.reset();
}

void ObTabletDDLCompleteArg::reset()
{
  has_complete_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  rec_scn_ = share::SCN::min_scn();
  start_scn_ = share::SCN::min_scn();
  data_format_version_ = 0;
  snapshot_version_ = 0;
  table_key_.reset();
  write_stat_.reset();
  trans_id_ = ObTabletDDLCompleteMdsUserDataKey::DDL_COMPLETE_TX_ID;
  storage_schema_ = nullptr;
  allocator_.reset();
}
bool ObTabletDDLCompleteArg::is_valid() const
{
  return (!has_complete_ && ls_id_.is_valid() && tablet_id_.is_valid()) ||
         (has_complete_ && ls_id_.is_valid() && tablet_id_.is_valid() && table_key_.is_valid()
                        && nullptr != storage_schema_ && storage_schema_->is_valid() && write_stat_.is_valid())
          || (is_incremental_major_direct_load(direct_load_type_)
              && OB_NOT_NULL(storage_schema_)
              && storage_schema_->is_valid());
}
int ObTabletDDLCompleteArg::set_storage_schema(const ObStorageSchema &other)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if ( nullptr != storage_schema_) {
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObStorageSchema))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed alloc buf", K(ret));
  } else {
    storage_schema_ = new (buf) ObStorageSchema();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(storage_schema_->assign(allocator_, other))) {
    LOG_WARN("failed to assign storage schema", K(ret));
  } else{
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_schema_->column_array_.count(); ++i) {
      ObStorageColumnSchema &cs = storage_schema_->column_array_.at(i);
      cs.orig_default_value_.reset();
    }
  }
  return ret;
}

int ObTabletDDLCompleteArg::assign(const ObTabletDDLCompleteArg &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (other.has_complete_ && OB_FAIL(set_storage_schema(*other.storage_schema_))) {
    LOG_WARN("failed to set storage_schema", K(ret));
  } else if (other.has_complete_ && OB_FAIL(write_stat_.assign(other.write_stat_))) {
    LOG_WARN("failed to set write_stat", K(ret));
  }else {
    has_complete_ = other.has_complete_;
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    direct_load_type_ = other.direct_load_type_;
    rec_scn_ = other.rec_scn_;
    start_scn_ = other.start_scn_;
    data_format_version_ = other.data_format_version_;
    snapshot_version_ = other.snapshot_version_;
    table_key_ = other.table_key_;
    trans_id_ = other.trans_id_;
  }
  return ret;
}

int64_t ObTabletDDLCompleteArg::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              has_complete_, ls_id_, tablet_id_,
              direct_load_type_, rec_scn_, start_scn_,
              data_format_version_, snapshot_version_,
              table_key_, write_stat_, trans_id_);
  if (nullptr != storage_schema_) {
    len += storage_schema_->get_serialize_size();
  }
  return len;
}

int ObTabletDDLCompleteArg::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              has_complete_, ls_id_, tablet_id_, direct_load_type_, rec_scn_, start_scn_,
              data_format_version_, snapshot_version_, table_key_, write_stat_, trans_id_);
  if (OB_FAIL(ret)) {
  } else if (has_complete_) {
    if (nullptr == storage_schema_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage schema should not be null", K(ret));
    } else if (OB_FAIL(storage_schema_->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize storage_schema", K(ret));
    }
  }
  return ret;
}

int ObTabletDDLCompleteArg::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, has_complete_, ls_id_, tablet_id_, direct_load_type_, rec_scn_, start_scn_,
              data_format_version_, snapshot_version_, table_key_, write_stat_, trans_id_);
  if (OB_FAIL(ret)) {
  } else if (has_complete_) {
    if (nullptr == storage_schema_) {
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObStorageSchema))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        storage_schema_ = new (buf) ObStorageSchema();
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_complete_) {
  } else if (OB_FAIL(storage_schema_->deserialize(allocator_, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize stroage_schema", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletDDLCompleteArg::from_mds_user_data(const ObTabletDDLCompleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  if (!user_data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(user_data));
  } else if (OB_FAIL(set_storage_schema(user_data.storage_schema_))) {
    LOG_WARN("failed to set storage schema", K(ret));
  } else if (OB_FAIL(write_stat_.assign(user_data.write_stat_))) {
    LOG_WARN("failed to set write stat", K(ret));
  } else {
    has_complete_ = user_data.has_complete_;
    direct_load_type_ = user_data.direct_load_type_;
    data_format_version_ = user_data.data_format_version_;
    snapshot_version_ = user_data.snapshot_version_;
    table_key_ = user_data.table_key_;
    trans_id_ = user_data.trans_id_;
    start_scn_ = user_data.start_scn_;
    rec_scn_ = user_data.inc_major_commit_scn_;
  }
  return ret;
}

int ObTabletDDLCompleteMdsHelper::process(const char* buf, const int64_t len, const share::SCN &scn,
                                          mds::BufferCtx &ctx, bool for_replay)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletDDLCompleteArg arg;
  if (nullptr == buf || len <= 0 || (for_replay && !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(len), K(for_replay), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf,len, pos))) {
    LOG_WARN("failed to deserialized from arg", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    ObLSHandle ls_handle;
    ObLSService *ls_service = MTL(ObLSService*);
    common::ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "Ddl_Com_MdsH"));
    ObTabletDDLCompleteMdsUserData data;

    /* set flag */
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_service is null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg));
    } else if (OB_FAIL(data.set_with_merge_arg(arg, allocator))) {
      LOG_WARN("failed to set with merge arg", K(ret));
    } else if (is_incremental_major_direct_load(arg.direct_load_type_)) {
      if (OB_FAIL(process_inc_major(ctx, ls_handle, arg.tablet_id_, data, scn, for_replay))) {
        LOG_WARN("failed to process inc major", KR(ret), K(arg), K(data), K(scn), K(for_replay));
      }
    } else {
      if (OB_FAIL(process_ddl(ctx, ls_handle, arg.tablet_id_, data, scn, for_replay))) {
        LOG_WARN("failed to process ddl", KR(ret), K(arg), K(data), K(scn), K(for_replay));
      }
    }
    FLOG_INFO("[DDL_REPLAY] schedule merge task on mds", K(ret), K(arg), K(for_replay));
  }
  return ret;
}

int ObTabletDDLCompleteMdsHelper::process_inc_major(
    mds::BufferCtx &ctx,
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const ObTabletDDLCompleteMdsUserData &data,
    const share::SCN &scn,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  MDS_TG(1_s);
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
  ObLS* ls = nullptr;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_id.is_valid() || !data.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid argument", KR(ret), K(ls_handle), K(tablet_id), K(data));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(ls_handle));
  } else if (!for_replay) {
    if (CLICK_FAIL(ls->get_tablet_svr()->set_ddl_complete(tablet_id,
                                                          ObTabletDDLCompleteMdsUserDataKey(data.trans_id_),
                                                          data,
                                                          user_ctx,
                                                          0/*lock_timeout_us*/))) {
      if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("failed to set ddl complete info", KR(ret), K(tablet_id), K(data));
      }
    }
  } else {
    ObTabletIncMajorInfoReplayExecutor replay_executor;
    if (CLICK_FAIL(replay_executor.init(ctx, scn, false/*for old mds*/, data))) {
      LOG_WARN("failed to inti inc major info replay executor", KR(ret), K(data), K(data));
    } else if (CLICK_FAIL(replay_executor.execute(scn, ls->get_ls_id(), tablet_id))) {
      LOG_WARN("failed to execute replay ddl complete mds data", KR(ret), K(scn), K(tablet_id));
    }
  }
  return ret;
}

int ObTabletDDLCompleteMdsHelper::process_ddl(
    mds::BufferCtx &ctx,
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const ObTabletDDLCompleteMdsUserData &data,
    const share::SCN &scn,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  MDS_TG(1_s);
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
  ObLS* ls = nullptr;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_id.is_valid() || !data.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid argument", KR(ret), K(ls_handle), K(tablet_id), K(data));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(ls_handle));
  } else if (!for_replay) {
    if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    } else if (OB_FAIL(ObTabletDDLCompleteReplayExecutor::freeze_ddl_kv(*tablet_handle.get_obj(), data))) {
      LOG_WARN("failed to freeze ddl kv", K(ret));
    } else if (OB_FAIL(ObTabletDDLCompleteReplayExecutor::update_tablet_table_store(*tablet_handle.get_obj(), data))) {
      LOG_WARN("failed to update tablet table store", K(ret));
    } else if (CLICK_FAIL(ls->get_tablet_svr()->set_ddl_complete(tablet_id, ObTabletDDLCompleteMdsUserDataKey(data.trans_id_), data, user_ctx, 0/*lock_timeout_us*/))) {
      if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("failed to set ddl complete info", K(ret), K(tablet_id), K(data));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObTabletDDLCompleteReplayExecutor::schedule_merge(*tablet_handle.get_obj(), data))) {
        LOG_WARN("failed to schedule merge", K(ret));
      }
    }
  } else {
    ObTabletDDLCompleteReplayExecutor replay_executor;
    if (CLICK_FAIL(replay_executor.init(ctx, scn, false /* for old mds */, data))) {
      LOG_WARN("failed to inti replay executor", K(ret));
    } else if (CLICK_FAIL(replay_executor.execute(scn, ls->get_ls_id(), tablet_id))) {
      LOG_WARN("failed to execute replay ddl complete mds data", K(ret));
    }
  }
  return ret;
}

int ObTabletDDLCompleteMdsHelper::on_replay(const char* buf, const int64_t len, share::SCN scn, mds::BufferCtx &ctx)
{
  return process(buf, len, scn, ctx, true /* for replay*/);
}

int ObTabletDDLCompleteMdsHelper::on_register(const char* buf, const int64_t len,  mds::BufferCtx &ctx)
{
  return process(buf, len, share::SCN(), ctx, false /* not for replay*/);
}

int ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(
  const ObTabletDDLCompleteArg &complete_arg,
  common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  const uint64_t tenant_id = MTL_ID();
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const int64_t SLEEP_INTERVAL = 100 * 1000L; // 100ms
  ObTimeoutCtx ctx;
  const int64_t default_timeout_ts = GCONF.rpc_timeout;

  if (OB_UNLIKELY(!complete_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg for completement arg", KR(ret), K(complete_arg));
  } else if (FALSE_IT(buf_len = complete_arg.get_serialize_size())) {
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buf", KR(ret), K(buf_len));
  } else if (OB_FAIL(complete_arg.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize complete_arg", KR(ret), K(complete_arg));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy", KR(ret), KP(sql_proxy));
  } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, default_timeout_ts))) {
    LOG_WARN("fail to set timeout ctx", KR(ret), K(default_timeout_ts));
  } else {
    ObMySQLTransaction trans;
    ObInnerSQLConnection *conn = nullptr;
    if (OB_FAIL(trans.start(sql_proxy, tenant_id))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null connection", KR(ret), KP(conn));
    } else {
      do {
        if (ctx.is_timeouted()) {
          ret = OB_TIMEOUT;
          LOG_WARN("already timeout", KR(ret), K(ctx));
        } else if (OB_FAIL(conn->register_multi_data_source(tenant_id, complete_arg.ls_id_,
                                transaction::ObTxDataSourceType::DDL_COMPLETE_MDS, buf, buf_len))) {
          LOG_WARN("fail to register_tx_data", KR(ret), K(complete_arg), K(buf), K(buf_len));
          if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NOT_MASTER == ret) {
            LOG_INFO("fail to find leader, try again", K(tenant_id), K(complete_arg));
            ob_usleep(SLEEP_INTERVAL);
          }
        }
      } while (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NOT_MASTER == ret);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
    } else {
      FLOG_INFO("succeed to record ddl complete arg to mds", K(complete_arg), K(common::lbt()));
      char extra_info[512];
      snprintf(extra_info, sizeof(extra_info), "has_complete:%s, start_scn:%ld, rec_scn:%ld, row_count:%ld, data_format_version:%ld, snapshot_version:%ld",
          complete_arg.has_complete_ ? "true" : "false",
          complete_arg.start_scn_.get_val_for_tx(),
          complete_arg.rec_scn_.get_val_for_tx(),
          complete_arg.write_stat_.row_count_,
          complete_arg.data_format_version_,
          complete_arg.snapshot_version_);
      SERVER_EVENT_ADD("ddl", "ddl write complete mds",
                       "tenant_id", tenant_id,
                       "ret", ret,
                       "trace_id", *ObCurTraceId::get_trace_id(),
                       "tablet_id", complete_arg.tablet_id_,
                       "trans_id", complete_arg.trans_id_,
                       "direct_load_type", complete_arg.direct_load_type_,
                       extra_info);
    }
  }
  return ret;
}

}
}
