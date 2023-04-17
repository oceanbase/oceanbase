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

#include "ob_multi_data_source.h"
#include "ob_trans_define.h"
#include "share/ob_errno.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ls/ob_ls_member_table.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "share/ls/ob_ls_operator.h"
#include "share/ob_standby_upgrade.h"  // ObStandbyUpgrade

namespace oceanbase
{

using namespace common;
using namespace memtable;
using namespace transaction::tablelock;

namespace transaction
{

//#####################################################
// ObMDSStr
//#####################################################3

OB_SERIALIZE_MEMBER(ObMDSStr, mds_str_, type_, ls_id_);

ObMDSStr::ObMDSStr() { reset(); }

ObMDSStr::~ObMDSStr() {}

void ObMDSStr::reset()
{
  mds_str_.reset();
  type_ = ObTxDataSourceType::UNKNOWN;
  ls_id_.reset();
}

int ObMDSStr::set(const char *msd_buf,
                  const int64_t msd_buf_len,
                  const ObTxDataSourceType &type,
                  const share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(msd_buf) || 0 == msd_buf_len || ObTxDataSourceType::UNKNOWN == type
      || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), KP(msd_buf), K(msd_buf_len), K(type), K(ls_id));
  } else if (!mds_str_.empty()) {
    TRANS_LOG(WARN, "MSD str is not empty", K(ret), K(*this));
  } else {
    mds_str_.assign_ptr(msd_buf, msd_buf_len);
    type_ = type;
    ls_id_ = ls_id;
  }
  return ret;
}

//#####################################################
// ObTxBufferNode
//#####################################################3

OB_SERIALIZE_MEMBER(ObTxBufferNode, type_, data_);

int ObTxBufferNode::init(const ObTxDataSourceType type, const ObString &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type <= ObTxDataSourceType::UNKNOWN || type >= ObTxDataSourceType::MAX_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(type));
  } else {
    reset();
    type_ = type;
    data_ = data;
  }
  return ret;
}

void ObTxBufferNode::replace_data(const common::ObString &data)
{
  if (nullptr != data_.ptr()) {
    ob_free(data_.ptr());
    data_.assign_ptr(nullptr, 0);
  }

  data_ = data;
  has_submitted_ = false;
  has_synced_ = false;
}
//#####################################################
// ObTxMDSRange
//#####################################################3


//#####################################################
// ObMulSourceTxDataNotifier
//#####################################################3

int ObMulSourceTxDataNotifier::notify_table_lock(const ObTxBufferNodeArray &array,
                                                 const ObMulSourceDataNotifyArg &arg,
                                                 ObPartTransCtx *part_ctx,
                                                 int64_t &total_time)
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mt_ctx = nullptr;
  ObMulSourceDataNotifyArg tmp_notify_arg = arg;
  const NotifyType notify_type = NotifyType::TX_END;
  if (OB_ISNULL(part_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(part_ctx));
  } else if (OB_ISNULL(mt_ctx = part_ctx->get_memtable_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "memtable ctx should not null", KR(ret), K(part_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      ObTimeGuard notify_time;
      const ObTxBufferNode &node = array.at(i);
      const char *buf = node.data_.ptr();
      const int64_t len = node.data_.length();

      tmp_notify_arg.redo_submitted_ = node.is_submitted();
      tmp_notify_arg.redo_synced_ = node.is_synced();

      if (ObTxDataSourceType::TABLE_LOCK == node.type_) {
        if (OB_FAIL(notify_table_lock(notify_type, buf, len, tmp_notify_arg, mt_ctx))) {
          TRANS_LOG(WARN, "notify table lock failed", KR(ret), K(node));
        }
      }
      if (notify_time.get_diff() > 1 * 1000 * 1000) {
        TRANS_LOG(INFO, "notify one data source with too much time", K(ret), K(notify_type), K(i), K(node),
                  K(arg), K(notify_time.get_diff()));
      }
      total_time += notify_time.get_diff();
    }
  }

  return ret;
}

int ObMulSourceTxDataNotifier::notify(const ObTxBufferNodeArray &array,
                                      const NotifyType notify_type,
                                      const ObMulSourceDataNotifyArg &arg,
                                      ObPartTransCtx *part_ctx,
                                      int64_t &total_time)
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mt_ctx = nullptr;
  ObMulSourceDataNotifyArg tmp_notify_arg = arg;
  if (OB_ISNULL(part_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(part_ctx));
  } else if (OB_ISNULL(mt_ctx = part_ctx->get_memtable_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "memtable ctx should not null", KR(ret), K(part_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      ObTimeGuard notify_time;
      const ObTxBufferNode &node = array.at(i);
      const char *buf = node.data_.ptr();
      const int64_t len = node.data_.length();

      tmp_notify_arg.redo_submitted_ = node.is_submitted();
      tmp_notify_arg.redo_synced_ = node.is_synced();

      switch (node.type_) {
      case ObTxDataSourceType::TABLE_LOCK: {
        ret = notify_table_lock(notify_type, buf, len, tmp_notify_arg, mt_ctx);
        break;
      }
      case ObTxDataSourceType::LS_TABLE: {
        ret = notify_ls_table(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      case ObTxDataSourceType::CREATE_TABLET: {
        ret = notify_create_tablet(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      case ObTxDataSourceType::REMOVE_TABLET: {
        ret = notify_remove_tablet(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      case ObTxDataSourceType::DDL_TRANS: {
        ret = notify_ddl_trans(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      case ObTxDataSourceType::DDL_BARRIER: {
        ret = notify_ddl_barrier(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      case ObTxDataSourceType::MODIFY_TABLET_BINDING: {
        ret = notify_modify_tablet_binding(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      case ObTxDataSourceType::STANDBY_UPGRADE: {
        ret = notify_standby_upgrade(notify_type, buf, len, tmp_notify_arg);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      }
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "notify data source failed", KR(ret), K(node));
      }

      if (notify_time.get_diff() > 1 * 1000 * 1000) {
        TRANS_LOG(INFO, "notify one data source with too much time", K(ret), K(notify_type), K(i), K(node),
                  K(arg), K(notify_time.get_diff()));
      }
      
      total_time += notify_time.get_diff();
    }
  }

  return ret;
}

void ObMulSourceTxDataNotifier::ob_abort_log_cb_notify_(const NotifyType type,
                                                        int err_code,
                                                        bool for_replay)
{
  bool need_core = false;

  if (NotifyType::ON_REDO == type || NotifyType::ON_PREPARE == type || NotifyType::ON_COMMIT == type
      || NotifyType::ON_ABORT == type) {
    if (for_replay) {
      if (OB_SUCCESS != err_code && OB_TENANT_OUT_OF_MEM != err_code
          && OB_ALLOCATE_MEMORY_FAILED != err_code && OB_EAGAIN != err_code) {
        need_core = true;
      }
    } else {
      if (OB_SUCCESS != err_code) {
        need_core = true;
      }
    }
  }

  if (need_core) {
    TRANS_LOG_RET(ERROR, OB_ERROR, "data source can not return error in log_cb on_success", K(type), K(err_code), K(for_replay));
    ob_usleep(1000 * 1000);
    ob_abort();

  }
}

int ObMulSourceTxDataNotifier::notify_table_lock(
    const NotifyType type,
    const char *buf,
    const int64_t len,
    const ObMulSourceDataNotifyArg &arg,
    ObMemtableCtx *mt_ctx)
{
  int ret = OB_SUCCESS;
  tablelock::ObTableLockOp lock_op;
  bool need_replay = false;
  const bool is_replay_multi_source = true;
  const bool is_committed = true;
  int64_t pos = 0;
  const int64_t curr_timestamp = ObTimeUtility::current_time();
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0) || OB_ISNULL(mt_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(buf), K(len), K(mt_ctx));
  } else if (NotifyType::TX_END != type) {
    // TABLELOCK only deal with tx_end type, but not redo/prepare/commit/abort.
  } else if (!arg.for_replay_) {
    // TABLELOCK only need deal with replay process, but not apply.
    // the replay process will produce a lock op and will be dealt at trans end.
  } else if (OB_FAIL(lock_op.deserialize(buf, len, pos))) {
    TRANS_LOG(WARN, "deserialize lock op failed", K(ret), K(len), KP(buf));
  } else if (FALSE_IT(lock_op.create_timestamp_ = OB_MIN(curr_timestamp,
                                                         lock_op.create_timestamp_))) {
  } else if (OB_FAIL(mt_ctx->replay_lock(lock_op,
                                         arg.scn_))) {
    TRANS_LOG(WARN, "replay lock failed", K(ret));
  } else {
    // do nothing
  }
  TABLELOCK_LOG(DEBUG, "ObMulSourceTxDataNotifier::notify_table_lock", K(ret), K(type), K(arg.for_replay_), K(lock_op));

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_ls_table(const NotifyType type,
                                               const char *buf, const int64_t len,
                                               const ObMulSourceDataNotifyArg &arg)
{
  int ret = OB_SUCCESS;
  //TODO by msy164651 just for check
  share::ObLSAttr ls_attr;
  int64_t pos = 0;
  if (OB_FAIL(ls_attr.deserialize(buf, len, pos))) {
    TRANS_LOG(WARN, "failed to deserialize ls attr", KR(ret), K(buf), K(len));
  } else if (pos > len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
  }
  TRANS_LOG(INFO, "ls table notify", KR(ret), K(ls_attr));

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_standby_upgrade(const NotifyType type,
                                               const char *buf, const int64_t len,
                                               const ObMulSourceDataNotifyArg &arg)
{
  int ret = OB_SUCCESS;
  share::ObStandbyUpgrade data_version;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(buf), K(len), K(type));
  } else if (OB_FAIL(data_version.deserialize(buf, len, pos))) {
    TRANS_LOG(WARN, "failed to deserialize data_version", KR(ret), K(buf), K(len));
  } else if (pos > len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
  }
  TRANS_LOG(INFO, "standby upgrade notify", KR(ret), K(data_version), K(len), K(type));

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_create_tablet(const NotifyType type,
                                                      const char *buf, const int64_t len,
                                                      const ObMulSourceDataNotifyArg & arg)
{
  int ret = ObLSMemberTable::handle_create_tablet_notify(type, buf, len, arg);

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_remove_tablet(const NotifyType type,
                                                      const char *buf, const int64_t len,
                                                      const ObMulSourceDataNotifyArg &arg)
{
  int ret = ObLSMemberTable::handle_remove_tablet_notify(type, buf, len, arg);

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_modify_tablet_binding(const NotifyType type,
                                                            const char *buf,
                                                            const int64_t len,
                                                            const ObMulSourceDataNotifyArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBatchUnbindTabletArg modify_arg;
  if (OB_FAIL(modify_arg.deserialize(buf, len, pos))) {
    TRANS_LOG(WARN, "failed to deserialize arg", K(ret));
  } else {
    switch (type) {
    case transaction::NotifyType::REGISTER_SUCC: {
      if (OB_FAIL(ObTabletBindingHelper::lock_tablet_binding_for_unbind(modify_arg, arg))) {
        TRANS_LOG(WARN, "failed to lock tablet binding", K(ret));
      }
      break;
    }
    case transaction::NotifyType::ON_REDO: {
      if (!arg.for_replay_ && OB_FAIL(ObTabletBindingHelper::set_scn_for_unbind(modify_arg, arg))) {
        TRANS_LOG(WARN, "failed to lock tablet binding, retry", K(ret));
      }
      break;
    }
    case transaction::NotifyType::TX_END: {
      if (arg.is_redo_submitted()) {
        if (OB_FAIL(ObTabletBindingHelper::on_tx_end_for_modify_tablet_binding(modify_arg, arg))) {
          TRANS_LOG(WARN, "failed to tx_end", K(ret));
        }
      }
      break;
    }
    case transaction::NotifyType::ON_COMMIT: {
      if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_for_unbind(modify_arg, arg))) {
        TRANS_LOG(WARN, "failed to modify tablet binding, retry", K(ret));
      } else if (OB_FAIL(ObTabletBindingHelper::unlock_tablet_binding_for_unbind(modify_arg, arg))) {
        TRANS_LOG(WARN, "failed to unlock tablet binding, retry", K(ret));
      }
      break;
    }
    case transaction::NotifyType::ON_ABORT: {
      if (OB_FAIL(ObTabletBindingHelper::fix_binding_info_for_modify_tablet_binding(modify_arg, arg))) {
        TRANS_LOG(WARN, "failed to fix_binding_info_for_modify_tablet_binding", K(ret), K(modify_arg), K(arg));
      } else if (OB_FAIL(ObTabletBindingHelper::unlock_tablet_binding_for_unbind(modify_arg, arg))) {
        TRANS_LOG(WARN, "failed to unlock tablet binding, retry", K(ret));
      }
      break;
    }
    default: {
      break;
    }
    }
  }

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_ddl_trans(const NotifyType type,
                                                const char *buf, const int64_t len,
                                                const ObMulSourceDataNotifyArg &arg)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(DEBUG, "ddl trans commit notify", KR(ret), K(type), KP(buf), K(len));

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

int ObMulSourceTxDataNotifier::notify_ddl_barrier(const NotifyType type,
                                                  const char *buf, const int64_t len,
                                                  const ObMulSourceDataNotifyArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObDDLBarrierLog log;
  if (OB_FAIL(log.deserialize(buf, len, pos))) {
    TRANS_LOG(WARN, "failed to deserialize buf", K(ret));
  } else if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid ddl barrier log", K(ret), K(type), K(arg));
  } else {
    TRANS_LOG(INFO, "notify ddl barrier", K(type), K(arg), K(log));
  }

  ob_abort_log_cb_notify_(type, ret, arg.for_replay_);

  return ret;
}

const char *
ObMulSourceTxDataDump::dump_buf(ObTxDataSourceType source_type, const char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  const char *dump_str = "Unkown Multi Data Source";
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(buf), K(len));
  } else {
    switch (source_type) {
    case ObTxDataSourceType::TABLE_LOCK: {
      tablelock::ObTableLockOp lock_op;
      if (OB_FAIL(lock_op.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "deserialize lock op failed", K(ret), K(len), KP(buf));
      } else if (pos > len) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
      } else {
        dump_str = to_cstring(lock_op);
      }
      break;
    }
    case ObTxDataSourceType::LS_TABLE: {
      share::ObLSAttr ls_attr;
      if (OB_FAIL(ls_attr.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "failed to deserialize ls attr", KR(ret), K(buf), K(len));
      } else if (pos > len) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
      } else {
        dump_str = to_cstring(ls_attr);
      }
      break;
    }
    case ObTxDataSourceType::CREATE_TABLET: {
      obrpc::ObBatchCreateTabletArg create_arg;
      if (OB_FAIL(create_arg.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "deserialize failed for ls_member trans", KR(ret));
      } else if (pos > len) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
      } else {
        dump_str = to_cstring(create_arg);
      }
      break;
    }
    case ObTxDataSourceType::REMOVE_TABLET: {
      obrpc::ObBatchRemoveTabletArg remove_arg;
      if (OB_FAIL(remove_arg.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "deserialize failed for ls_member trans", KR(ret));
      } else if (pos > len) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
      } else {
        dump_str = to_cstring(remove_arg);
      }
      break;
    }
    case ObTxDataSourceType::DDL_BARRIER: {
      ObDDLBarrierLog log;
      if (OB_FAIL(log.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "failed to deserialize buf", K(ret));
      } else if (pos > len) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
      } else {
        dump_str = to_cstring(log);
      }
      break;
    }
    case ObTxDataSourceType::MODIFY_TABLET_BINDING: {
      ObBatchUnbindTabletArg modify_arg;
      if (OB_FAIL(modify_arg.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "failed to deserialize arg", K(ret));
      } else if (pos > len) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "deserialize error", KR(ret), K(pos), K(len));
      } else {
        dump_str = to_cstring(modify_arg);
      }
      break;
    }
    default: {
      // TRANS_LOG(ERROR, "Unkown Multi Data Source", K(source_type), KP(buf), K(len));
    }
    }
  }

  return dump_str;
}

} // transaction

} // oceanbase
