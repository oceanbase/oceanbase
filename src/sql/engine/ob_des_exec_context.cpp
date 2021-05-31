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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/ob_sql.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/stat/ob_session_stat.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObDesExecContext::ObDesExecContext(ObSQLSessionMgr* session_mgr) : ObExecContext()
{
  set_session_mgr(session_mgr);
  free_session_ctx_.sessid_ = ObSQLSessionInfo::INVALID_SESSID;
}

ObDesExecContext::ObDesExecContext(ObIAllocator& allocator, ObSQLSessionMgr* session_mgr) : ObExecContext(allocator)
{
  set_session_mgr(session_mgr);
  free_session_ctx_.sessid_ = ObSQLSessionInfo::INVALID_SESSID;
}

ObDesExecContext::~ObDesExecContext()
{
  cleanup_session();
  if (NULL != phy_plan_ctx_) {
    phy_plan_ctx_->~ObPhysicalPlanCtx();
    phy_plan_ctx_ = NULL;
  }
}

void ObDesExecContext::cleanup_session()
{
  if (NULL != my_session_) {
    if (ObSQLSessionInfo::INVALID_SESSID == free_session_ctx_.sessid_) {
      my_session_->~ObSQLSessionInfo();
      my_session_ = NULL;
    } else if (NULL != session_mgr_) {
      session_mgr_->revert_session(my_session_);
      session_mgr_->free_session(free_session_ctx_);
      my_session_ = NULL;
      session_mgr_->mark_sessid_unused(free_session_ctx_.sessid_);
    }
  }
}

void ObDesExecContext::show_session()
{
  if (NULL != my_session_) {
    my_session_->set_shadow(false);
  }
}

void ObDesExecContext::hide_session()
{
  if (NULL != my_session_) {
    my_session_->set_shadow(true);
  }
}

int ObDesExecContext::create_my_session(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* local_session = NULL;
  if (OB_UNLIKELY(my_session_ != NULL)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("my_session is not null.");
  } else if (NULL == session_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session manager is NULL", K(ret));
  } else {
    uint32_t sid = ObSQLSessionInfo::INVALID_SESSID;
    uint64_t proxy_sid = 0;
    uint64_t tenand_id = OB_INVALID_TENANT_ID;
    if (OB_FAIL(session_mgr_->create_sessid(sid))) {
      LOG_WARN("alloc session id failed", K(ret));
    } else if (OB_FAIL(session_mgr_->create_session(
                   tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), my_session_))) {
      LOG_WARN("create session failed", K(ret), K(sid));
      session_mgr_->mark_sessid_unused(sid);
      my_session_ = NULL;
    } else {
      free_session_ctx_.sessid_ = sid;
      free_session_ctx_.proxy_sessid_ = proxy_sid;
      free_session_ctx_.version_ = my_session_->get_version();
    }
    if (OB_FAIL(ret)) {
      // fail back to local session allocating, avoid remote/distribute executing fail
      // if server session overflow.
      ret = OB_SUCCESS;
      if (OB_UNLIKELY(
              NULL == (local_session = static_cast<ObSQLSessionInfo*>(allocator_.alloc(sizeof(ObSQLSessionInfo)))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no more memory to create sql session info");
      } else {
        local_session = new (local_session) ObSQLSessionInfo();
        uint32_t tmp_sid = 123456789;
        uint32_t tmp_version = 0;
        uint64_t tmp_proxy_sessid = 1234567890;
        if (OB_FAIL(local_session->init(tmp_version, tmp_sid, tmp_proxy_sessid, NULL))) {
          LOG_WARN("my session init failed", K(ret));
          local_session->~ObSQLSessionInfo();
        } else {
          my_session_ = local_session;
        }
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObDesExecContext)
{
  int ret = OB_SUCCESS;
  uint64_t ser_version = get_ser_version();
  int64_t index = 0;
  int32_t real_input_count = 0;
  ObPhyOperatorType phy_op_type;
  int64_t tmp_phy_op_type = 0;
  ObIPhyOperatorInput* input_param = NULL;
  uint64_t phy_op_size = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (ser_version == SER_VERSION_1) {
    OB_UNIS_DECODE(tenant_id);
  }
  OB_UNIS_DECODE(phy_op_size);
  // now to init ObExecContext container
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_physical_plan_ctx())) {
      LOG_WARN("create physical plan context failed", K(ret));
    } else if (OB_ISNULL(phy_plan_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to create phy plan ctx, but phy plan ctx is NULL", K(ret));
    } else if (OB_FAIL(create_my_session(tenant_id))) {
      LOG_WARN("create my session failed", K(ret));
    } else if (OB_ISNULL(my_session_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to create session, but session is NULL", K(ret));
    } else {
      OB_UNIS_DECODE(*phy_plan_ctx_);
      ObSQLSessionInfo::LockGuard query_guard(my_session_->get_query_lock());
      ObSQLSessionInfo::LockGuard data_guard(my_session_->get_thread_data_lock());
      OB_UNIS_DECODE(*my_session_);
      my_session_->set_is_remote(true);
      my_session_->set_session_type_with_flag();
      my_session_->set_mysql_cmd(obmysql::OB_MYSQL_COM_QUERY);
      if (OB_FAIL(ret)) {
        LOG_WARN("session deserialize failed", K(ret));
      } else if (OB_FAIL(my_session_->set_session_state(QUERY_ACTIVE))) {
        LOG_WARN("set session state failed", K(ret));
      } else if (OB_FAIL(my_session_->store_query_string(ObString::make_string("REMOTE/DISTRIBUTE PLAN EXECUTING")))) {
        LOG_WARN("store query string failed", K(ret));
      }

      // alloc from session manager, increase active session number
      if (OB_SUCC(ret) && free_session_ctx_.sessid_ != ObSQLSessionInfo::INVALID_SESSID) {
        free_session_ctx_.tenant_id_ = my_session_->get_effective_tenant_id();
        ObTenantStatEstGuard g(free_session_ctx_.tenant_id_);
        EVENT_INC(ACTIVE_SESSIONS);
        free_session_ctx_.has_inc_active_num_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // init operator context need session info, initialized after session deserialized.
    if (OB_FAIL(init_phy_op(phy_op_size))) {
      LOG_WARN("init exec context phy op failed", K(ret), K_(phy_op_size));
    }
  }
  /**
   * session will be used to generate sql plan even if in remote / distributed execution,
   * because nested session will generate and execute nested sql with inner connection.
   */
  if (OB_SUCC(ret) && !OB_ISNULL(my_session_) && !OB_ISNULL(GCTX.sql_engine_)) {
    ObPCMemPctConf pc_mem_conf;
    if (OB_FAIL(my_session_->get_pc_mem_conf(pc_mem_conf))) {
      if (OB_ENTRY_NOT_EXIST == ret && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100) {
        /**
         * ignore OB_ENTRY_NOT_EXIST if in upgrade process, this session must come from
         * 1470 or 2000, they can not generate remote / distributed plan with foreign
         * key operation.
         */
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get pc mem conf", K(ret));
      }
    } else {
      my_session_->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &real_input_count))) {
      LOG_WARN("decode int32_t", K(ret), K(data_len), K(pos));
    }
  }

  if (OB_SUCC(ret) && nullptr != phy_plan_ctx_ && !phy_plan_ctx_->is_new_engine()) {
    for (int32_t i = 0; OB_SUCC(ret) && i < real_input_count; ++i) {
      OB_UNIS_DECODE(index);
      OB_UNIS_DECODE(tmp_phy_op_type);
      phy_op_type = static_cast<ObPhyOperatorType>(tmp_phy_op_type);
      if (OB_FAIL(create_phy_op_input(index, phy_op_type, input_param))) {
        LOG_WARN("create physical operator input failed",
            K(ret),
            K(index),
            "phy_op_type",
            ob_phy_operator_type_str(phy_op_type));
      } else if (OB_ISNULL(input_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("succ to create phy op input, but op input is NULL",
            K(ret),
            K(index),
            "phy_op_type",
            ob_phy_operator_type_str(phy_op_type));
      } else {
        OB_UNIS_DECODE(*input_param);
      }
    }
  }
  OB_UNIS_DECODE(task_executor_ctx_);
  //  OB_UNIS_DECODE(execution_id_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_expr_op(phy_plan_ctx_->get_expr_op_size()))) {
      LOG_WARN("init exec context expr op failed", K(ret));
    }
  }
  return ret;
}

ObDistributedExecContext::ObDistributedExecContext(ObSQLSessionMgr* session_mgr)
    : ObDesExecContext(session_mgr),
      phy_plan_ctx_buf_(NULL),
      phy_plan_ctx_len_(0),
      my_session_buf_(NULL),
      my_session_len_(0),
      task_executor_ctx_buf_(NULL),
      task_executor_ctx_len_(0)
{}

ObDistributedExecContext::ObDistributedExecContext(ObIAllocator& allocator, ObSQLSessionMgr* session_mgr)
    : ObDesExecContext(allocator, session_mgr),
      phy_plan_ctx_buf_(NULL),
      phy_plan_ctx_len_(0),
      my_session_buf_(NULL),
      my_session_len_(0),
      task_executor_ctx_buf_(NULL),
      task_executor_ctx_len_(0)
{}

ObDistributedExecContext::~ObDistributedExecContext()
{}

DEFINE_SERIALIZE(ObDistributedExecContext)
{
  int ret = OB_SUCCESS;
  uint64_t ser_version = get_ser_version();
  int64_t input_start_pos = pos;  // the position of the starting serialization
  int32_t real_input_count = 0;   // real serialized input param count
  ObIPhyOperatorInput* input_param = NULL;
  if (ser_version == SER_VERSION_1) {
    OB_UNIS_ENCODE(my_session_->get_login_tenant_id());
  }
  OB_UNIS_ENCODE(phy_op_size_);
  if (OB_SUCC(ret)) {
    if (!row_id_list_array_.empty()) {
      OB_UNIS_ENCODE(*phy_plan_ctx_);
    } else if (phy_plan_ctx_len_ > buf_len - pos) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialize buffer overflow", K(ret), K_(phy_plan_ctx_len), K(buf_len), K(pos));
    } else {
      MEMCPY(buf + pos, phy_plan_ctx_buf_, phy_plan_ctx_len_);
      pos += phy_plan_ctx_len_;
    }
  }

  if (OB_SUCC(ret) && my_session_len_ > buf_len - pos) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize buffer overflow", K(ret), K_(my_session_len), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, my_session_buf_, my_session_len_);
    pos += my_session_len_;
  }

  input_start_pos = pos;
  // only after the serialization of physical operator input, we can know the real input count
  // so skip the length of int32_t to serialize real_input_count and serialize input param first
  pos += serialization::encoded_length_i32(real_input_count);
  for (int64_t index = 0; OB_SUCC(ret) && NULL != phy_op_input_store_ && index < phy_op_size_; ++index) {
    // if input parameter is NULL, it means that the operator has no input parameter
    // not need to handle it
    if (NULL != (input_param = static_cast<ObIPhyOperatorInput*>(phy_op_input_store_[index])) &&
        input_param->need_serialized()) {
      OB_UNIS_ENCODE(index);                                                 // serialize index
      OB_UNIS_ENCODE(static_cast<int64_t>(input_param->get_phy_op_type()));  // serialize operator type
      OB_UNIS_ENCODE(*input_param);                                          // serialize input parameter
      if (OB_SUCC(ret)) {
        ++real_input_count;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i32(buf, buf_len, input_start_pos, real_input_count))) {
      LOG_WARN("encode int32_t", K(buf_len), K(input_start_pos), K(real_input_count));
    }
  }

  if (OB_SUCC(ret) && task_executor_ctx_len_ > buf_len - pos) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize buffer overflow", K(ret), K_(task_executor_ctx_len), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, task_executor_ctx_buf_, task_executor_ctx_len_);
    pos += task_executor_ctx_len_;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObDistributedExecContext)
{
  int ret = OB_SUCCESS;
  uint64_t ser_version = get_ser_version();
  int64_t index = 0;
  int32_t real_input_count = 0;
  ObPhyOperatorType phy_op_type;
  int64_t tmp_phy_op_type = 0;
  ObIPhyOperatorInput* input_param = NULL;
  uint64_t phy_op_size = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t expr_op_size = 0;
  int64_t tmp_pos = 0;
  if (ser_version == SER_VERSION_1) {
    OB_UNIS_DECODE(tenant_id);
  }
  OB_UNIS_DECODE(phy_op_size);
  // now to init ObExecContext container
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_physical_plan_ctx())) {
      LOG_WARN("create physical plan context failed", K(ret));
    } else if (OB_ISNULL(phy_plan_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to create phy plan ctx, but phy plan ctx is NULL", K(ret));
    } else if (OB_FAIL(create_my_session(tenant_id))) {
      LOG_WARN("create my session failed", K(ret));
    } else if (OB_ISNULL(my_session_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to create session, but session is NULL", K(ret));
    } else {
      phy_plan_ctx_buf_ = buf + pos;
      tmp_pos = pos;
      OB_UNIS_DECODE(*phy_plan_ctx_);
      phy_plan_ctx_len_ = pos - tmp_pos;

      my_session_buf_ = buf + pos;
      tmp_pos = pos;
      ObSQLSessionInfo::LockGuard query_guard(my_session_->get_query_lock());
      ObSQLSessionInfo::LockGuard data_guard(my_session_->get_thread_data_lock());
      OB_UNIS_DECODE(*my_session_);
      my_session_len_ = pos - tmp_pos;
      my_session_->set_session_type_with_flag();
      my_session_->set_mysql_cmd(obmysql::OB_MYSQL_COM_QUERY);
      // ObDistributedExecContext only used in distribute scheduler, set peer to self address.
      my_session_->set_peer_addr(GCONF.self_addr_);
      if (OB_FAIL(ret)) {
        LOG_WARN("session deserialize failed", K(ret));
      } else if (OB_FAIL(my_session_->set_session_state(QUERY_ACTIVE))) {
        LOG_WARN("set session state failed", K(ret));
      } else if (OB_FAIL(my_session_->store_query_string(ObString::make_string("DISTRIBUTE PLAN SCHEDULING")))) {
        LOG_WARN("store query string failed", K(ret));
      }
      // alloc from session manager, increase active session number
      if (OB_SUCC(ret) && free_session_ctx_.sessid_ != ObSQLSessionInfo::INVALID_SESSID) {
        free_session_ctx_.tenant_id_ = my_session_->get_effective_tenant_id();
        ObTenantStatEstGuard g(free_session_ctx_.tenant_id_);
        EVENT_INC(ACTIVE_SESSIONS);
        free_session_ctx_.has_inc_active_num_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    // init operator context need session info, initialized after session deserialized.
    if (OB_FAIL(init_phy_op(phy_op_size))) {
      LOG_WARN("init exec context phy op failed", K(ret), K_(phy_op_size));
    }
  }

  /**
   * session will be used to generate sql plan even if in remote / distributed execution,
   * because nested session will generate and execute nested sql with inner connection.
   */
  if (OB_SUCC(ret) && !OB_ISNULL(my_session_) && !OB_ISNULL(GCTX.sql_engine_)) {
    ObPCMemPctConf pc_mem_conf;
    if (OB_FAIL(my_session_->get_pc_mem_conf(pc_mem_conf))) {
      if (OB_ENTRY_NOT_EXIST == ret && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100) {
        /**
         * ignore OB_ENTRY_NOT_EXIST if in upgrade process, this session must come from
         * 1470 or 2000, they can not generate remote / distributed plan with foreign
         * key operation.
         */
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get pc mem conf", K(ret));
      }
    } else {
      my_session_->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &real_input_count))) {
      LOG_WARN("decode int32_t", K(ret), K(data_len), K(pos));
    }
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < real_input_count; ++i) {
    OB_UNIS_DECODE(index);
    OB_UNIS_DECODE(tmp_phy_op_type);
    phy_op_type = static_cast<ObPhyOperatorType>(tmp_phy_op_type);
    if (OB_FAIL(create_phy_op_input(index, phy_op_type, input_param))) {
      LOG_WARN("create physical operator input failed",
          K(ret),
          K(index),
          "phy_op_type",
          ob_phy_operator_type_str(phy_op_type));
    } else if (OB_ISNULL(input_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("succ to create phy op input, but op input is NULL",
          K(ret),
          K(index),
          "phy_op_type",
          ob_phy_operator_type_str(phy_op_type));
    } else {
      OB_UNIS_DECODE(*input_param);
    }
  }

  task_executor_ctx_buf_ = buf + pos;
  tmp_pos = pos;
  OB_UNIS_DECODE(task_executor_ctx_);
  task_executor_ctx_len_ = pos - tmp_pos;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_expr_op(phy_plan_ctx_->get_expr_op_size()))) {
      LOG_WARN("init exec context expr op failed", K(ret), K(expr_op_size));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDistributedExecContext)
{
  int64_t len = 0;
  uint64_t ser_version = get_ser_version();
  int32_t real_input_count = 0;
  ObIPhyOperatorInput* input_param = NULL;
  if (ser_version == SER_VERSION_1) {
    OB_UNIS_ADD_LEN(my_session_->get_login_tenant_id());
  }
  OB_UNIS_ADD_LEN(phy_op_size_);
  if (!row_id_list_array_.empty()) {
    len += phy_plan_ctx_len_;
  } else if (phy_plan_ctx_ != NULL) {
    OB_UNIS_ADD_LEN(*phy_plan_ctx_);
  }
  len += my_session_len_;
  len += serialization::encoded_length_i32(real_input_count);
  for (int64_t index = 0; NULL != phy_op_input_store_ && index < phy_op_size_; ++index) {
    if (NULL != (input_param = phy_op_input_store_[index]) && input_param->need_serialized()) {
      int64_t op_type = static_cast<int64_t>(input_param->get_phy_op_type());
      OB_UNIS_ADD_LEN(index);
      OB_UNIS_ADD_LEN(op_type);
      OB_UNIS_ADD_LEN(*input_param);
    }
  }
  len += task_executor_ctx_len_;
  return len;
}

}  // namespace sql
}  // namespace oceanbase
