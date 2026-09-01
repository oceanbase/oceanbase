/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_drop_gtt_v2_session_tablet_rpc.h"

#include "lib/utility/utility.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/tablet/ob_session_tablet_helper.h"
#include "storage/tablet/ob_session_tablet_info_map.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace storage
{

namespace
{

class IGTTV2SessionTabletFunctor
{
public:
  explicit IGTTV2SessionTabletFunctor(const ObDropGTTV2SessionTabletArg &arg)
    : is_batch_(false),
      arg_(&arg),
      batch_arg_(nullptr),
      map_err_(OB_SUCCESS)
  {}
  explicit IGTTV2SessionTabletFunctor(const ObBatchDropGTTV2SessionTabletArg &batch_arg)
    : is_batch_(true),
      arg_(nullptr),
      batch_arg_(&batch_arg),
      map_err_(OB_SUCCESS)
  {}
  virtual ~IGTTV2SessionTabletFunctor() = default;
  bool operator()(sql::ObSQLSessionMgr::Key /*key*/, sql::ObSQLSessionInfo *sess_info)
  {
    // ObTenantSwitchGuard only sets MTL_ID(); for_each_session iterates
    // globally, so filter by effective tenant here to avoid scanning across
    // tenants on sessid collisions.
    if (OB_NOT_NULL(sess_info)
        && is_matched(*sess_info)) {
      handle_matched_session(*sess_info);
    }
    return true;
  }
  int get_map_err() const { return map_err_; }

protected:
  virtual void process_one(
      sql::ObSQLSessionInfo &sess_info,
      const ObSessionTabletInfoKey &info_key) = 0;
  void record_local_err(const int err)
  {
    if (OB_SUCCESS == map_err_) {
      map_err_ = err;
    }
  }

private:
  void handle_matched_session(sql::ObSQLSessionInfo &sess_info)
  {
    if (!is_batch_) {
      OB_ASSERT(nullptr != arg_);
      const common::ObIArray<uint64_t> &table_ids = arg_->get_table_ids();
      for (int64_t i = 0; i < table_ids.count(); ++i) {
        const uint64_t table_id = table_ids.at(i);
        const ObSessionTabletInfoKey info_key(table_id, arg_->get_sequence(), arg_->get_session_id());
        process_one(sess_info, info_key);
      }
    } else {
      OB_ASSERT(nullptr != batch_arg_);
      const common::ObIArray<storage::ObSessionTabletInfo> &tablet_infos = batch_arg_->get_tablet_infos();
      for (int64_t i = 0; i < tablet_infos.count(); ++i) {
        const storage::ObSessionTabletInfo &info = tablet_infos.at(i);
        if (sess_info.get_sessid_for_table() == info.get_session_id()
            && (!batch_arg_->exclude_active_session_trx_tablet()
                || sess_info.get_trans_gtt_v2_sequence() != info.get_sequence())) {
          const ObSessionTabletInfoKey info_key(info.get_table_id(), info.get_sequence(), info.get_session_id());
          process_one(sess_info, info_key);
        }
      }
    }
  }
  bool is_matched(sql::ObSQLSessionInfo &sess_info) const
  {
    bool b_ret = false;
    if (!is_batch_) {
      OB_ASSERT(nullptr != arg_);
      b_ret = (sess_info.get_effective_tenant_id() == arg_->get_tenant_id()
               && sess_info.get_sessid_for_table() == arg_->get_session_id());
    } else {
      OB_ASSERT(nullptr != batch_arg_);
      b_ret = (sess_info.get_effective_tenant_id() == batch_arg_->get_tenant_id());
    }
    return b_ret;
  }

protected:
  bool is_batch_;
  const ObDropGTTV2SessionTabletArg *arg_;
  const ObBatchDropGTTV2SessionTabletArg *batch_arg_;
  int map_err_;
};

// Functor used with ObSQLSessionMgr::for_each_session to collect creator
// tablet infos for the storage delete. Local-failure return codes from
// get/push are tracked in map_err_ so the outer caller can decide whether to
// skip the storage delete.
class DropGTTV2SessionTabletFunctor : public IGTTV2SessionTabletFunctor
{
public:
  explicit DropGTTV2SessionTabletFunctor(const ObDropGTTV2SessionTabletArg &arg)
    : IGTTV2SessionTabletFunctor(arg),
      local_map_hit_(false),
      creator_infos_()
  {}
  explicit DropGTTV2SessionTabletFunctor(const ObBatchDropGTTV2SessionTabletArg &batch_arg)
    : IGTTV2SessionTabletFunctor(batch_arg),
      local_map_hit_(false),
      creator_infos_()
  {}
  bool local_map_hit() const { return local_map_hit_; }
  common::ObIArray<ObSessionTabletInfo> &get_creator_infos() { return creator_infos_; }

protected:
  void process_one(
      sql::ObSQLSessionInfo &sess_info,
      const ObSessionTabletInfoKey &info_key) override
  {
    ObSessionTabletInfo tablet_info;
    // local_only=true: only the owning session's local map has is_creator_=true;
    // do not point_get/push into unrelated sessions' maps during for_each_session.
    const int local_get_ret = sess_info.get_gtt_tablet_info_map().get_session_tablet(
        info_key, tablet_info, true/*local_only*/);
    if (OB_ENTRY_NOT_EXIST == local_get_ret) {
      // no entry on this session
    } else if (OB_SUCCESS != local_get_ret) {
      LOG_WARN_RET(local_get_ret, "fail to get session tablet from local map",
                    K_(is_batch), KPC_(arg), KPC_(batch_arg), K(info_key), KP(&sess_info));
      record_local_err(local_get_ret);
    } else {
      local_map_hit_ = true;
      if (tablet_info.is_creator_) {
        const int local_push_ret = creator_infos_.push_back(tablet_info);
        if (OB_SUCCESS != local_push_ret) {
          LOG_WARN_RET(local_push_ret, "fail to record creator tablet info",
                        K_(is_batch), KPC_(arg), KPC_(batch_arg), K(info_key), K(tablet_info));
          record_local_err(local_push_ret);
        }
      }
    }
  }

private:
  bool local_map_hit_;
  common::ObSEArray<ObSessionTabletInfo,
                    ObDropGTTV2SessionTabletArg::DEFAULT_TABLE_ID_COUNT> creator_infos_;
};

class RemoveGTTV2SessionTabletFunctor : public IGTTV2SessionTabletFunctor
{
public:
  RemoveGTTV2SessionTabletFunctor(const ObDropGTTV2SessionTabletArg &arg)
    : IGTTV2SessionTabletFunctor(arg)
  {
  }

  RemoveGTTV2SessionTabletFunctor(const ObBatchDropGTTV2SessionTabletArg &batch_arg)
    : IGTTV2SessionTabletFunctor(batch_arg)
  {
  }

protected:
  void process_one(
      sql::ObSQLSessionInfo &sess_info,
      const ObSessionTabletInfoKey &info_key) override
  {
    const int local_rm_ret = sess_info.get_gtt_tablet_info_map().remove_session_tablet(info_key);
    if (OB_ENTRY_NOT_EXIST == local_rm_ret) {
      // do nothing
    } else if (OB_SUCCESS != local_rm_ret) {
      LOG_WARN_RET(local_rm_ret, "failed to remove session tablet from gtt tablet info map", K(info_key));
      record_local_err(local_rm_ret);
    }
  }
};
} // anonymous namespace

template<class ArgType>
static int template_do_delete_as_creator(
    const ArgType &arg,
    common::ObIArray<ObSessionTabletInfo> &creator_tablet_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid()) || OB_UNLIKELY(creator_tablet_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg), K(creator_tablet_infos));
  } else {
    common::ObSEArray<ObSessionTabletInfo *,
                      ObDropGTTV2SessionTabletArg::DEFAULT_TABLE_ID_COUNT> tablet_info_ptrs;
    ARRAY_FOREACH(creator_tablet_infos, i) {
      ObSessionTabletInfo &info = creator_tablet_infos.at(i);
      if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet info", KR(ret), K(info));
      } else if (OB_UNLIKELY(!arg.is_matched(info))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("creator tablet info does not match arg", KR(ret), K(arg), K(info));
      } else if (OB_FAIL(tablet_info_ptrs.push_back(&info))) {
        LOG_WARN("failed to push back tablet info pointer", KR(ret), K(info));
      }
    }
    if (OB_SUCC(ret)) {
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, arg.get_tenant_id()))) {
        LOG_WARN("failed to start trans", KR(ret), K(arg));
      } else {
        ObSessionTabletDeleteHelper delete_helper(arg.get_tenant_id(), tablet_info_ptrs, trans);
        if (OB_FAIL(delete_helper.do_work())) {
          LOG_WARN("failed to delete session tablets on creator", KR(ret), K(arg), K(tablet_info_ptrs));
        } else {
          LOG_INFO("succeed to delete session tablets on creator", K(arg), K(tablet_info_ptrs));
        }
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        const bool is_commit = (OB_SUCCESS == ret);
        if (OB_TMP_FAIL(trans.end(is_commit))) {
          LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret), K(is_commit));
          ret = is_commit ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

template<class ArgType>
static int template_handle_in_tenant(
    const ArgType &arg,
    ObDropGTTV2SessionTabletRes &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  result.set_executed_on_creator(false);
  result.set_local_map_hit(false);
  result.set_ret(OB_SUCCESS);
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_ISNULL(session_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr is null", KR(ret));
  } else {
    DropGTTV2SessionTabletFunctor functor(arg);
    if (OB_FAIL(session_mgr->for_each_session(functor))) {
      LOG_WARN("fail to iterate sessions", KR(ret), K(arg));
    } else {
      // map_err_ records per-session get/push/remove failures encountered
      // while walking sessions. Do NOT use it to short-circuit the creator
      // delete: a failure on one matching session must not prevent storage
      // teardown of creator entries that were successfully captured from
      // other matching sessions on the same observer. The error is
      // propagated only via result.set_ret() so the originator can log it.
      const int map_err = functor.get_map_err();
      if (OB_SUCCESS != map_err) {
        LOG_WARN("session gtt map mutation failed during drop; proceeding with creator delete",
                 K(map_err), K(arg));
      }
      result.set_local_map_hit(functor.local_map_hit());
      common::ObIArray<ObSessionTabletInfo> &creator_infos = functor.get_creator_infos();
      if (!creator_infos.empty()) {
        const int delete_ret = template_do_delete_as_creator(arg, creator_infos);
        result.set_executed_on_creator(true);
        // delete_ret is authoritative; surface map_err only when the storage
        // delete itself succeeded, so the originator can still see the
        // stale-cache risk without losing the real failure.
        result.set_ret(OB_SUCCESS != delete_ret ? delete_ret : map_err);
        if (OB_SUCCESS != delete_ret) {
          LOG_WARN("creator failed to delete session tablets", K(delete_ret), K(arg), K(creator_infos));
        } else {
          LOG_INFO("creator finished session tablet drop", K(arg), K(creator_infos));
        }
      } else {
        result.set_ret(map_err);
        LOG_INFO("non-creator observer handled drop gtt v2 session tablet",
                 K(arg), K(map_err), "local_map_hit", functor.local_map_hit());
      }

      if (OB_SUCCESS == result.get_ret()) {
        // remove session tablets from local gtt maps only when del tablets is successful.
        RemoveGTTV2SessionTabletFunctor local_rm_functor(arg);
        int local_rm_ret = OB_SUCCESS;
        if (OB_SUCCESS != (local_rm_ret = session_mgr->for_each_session(local_rm_functor))) {
          LOG_WARN_RET(local_rm_ret, "failed to iterate sessions", K(arg));
        } else if (OB_SUCCESS != (local_rm_ret = local_rm_functor.get_map_err())) {
          LOG_WARN_RET(local_rm_ret, "some failure happened during "
            "removing local session tablet cache", K(arg));
        }
        result.set_ret(local_rm_ret);
      }
    }
  }
  return ret;
}

int ObDropGTTV2SessionTabletUtil::handle_in_tenant(
    const ObDropGTTV2SessionTabletArg &arg,
    ObDropGTTV2SessionTabletRes &result)
{
  return template_handle_in_tenant(arg, result);
}

int ObDropGTTV2SessionTabletUtil::handle_in_tenant(
    const ObBatchDropGTTV2SessionTabletArg &arg,
    ObDropGTTV2SessionTabletRes &result)
{
  return template_handle_in_tenant(arg, result);
}

int ObRpcDropGTTV2SessionTabletP::process()
{
  int ret = OB_SUCCESS;
  const ObDropGTTV2SessionTabletArg &arg = arg_;
  ObDropGTTV2SessionTabletRes &res = result_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drop gtt v2 session tablet arg", KR(ret), K(arg));
  } else {
    share::ObTenantSwitchGuard guard = share::_make_tenant_switch_guard();
    if (OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
      LOG_WARN("fail to switch tenant for drop gtt v2 session tablet rpc", KR(ret), K(arg));
    } else if (OB_FAIL(ObDropGTTV2SessionTabletUtil::handle_in_tenant(arg, res))) {
      LOG_WARN("fail to handle drop gtt v2 session tablet in tenant", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObRpcBatchDropGTTV2SessionTabletP::process()
{
  int ret = OB_SUCCESS;
  const ObBatchDropGTTV2SessionTabletArg &arg = arg_;
  ObDropGTTV2SessionTabletRes &res = result_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drop gtt v2 session tablet arg", KR(ret), K(arg));
  } else {
    share::ObTenantSwitchGuard guard = share::_make_tenant_switch_guard();
    if (OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
      LOG_WARN("fail to switch tenant for drop gtt v2 session tablet rpc", KR(ret), K(arg));
    } else if (OB_FAIL(ObDropGTTV2SessionTabletUtil::handle_in_tenant(arg, res))) {
      LOG_WARN("fail to handle drop gtt v2 session tablet in tenant", KR(ret), K(arg));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
