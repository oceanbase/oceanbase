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

#define USING_LOG_PREFIX SQL_MONITOR
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::json;
namespace oceanbase
{
namespace sql
{
bool ObFLTResetSessOp::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (false == sess_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is not valid", K(ret));
  } else {
    // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (OB_FAIL(sess_info->try_lock_thread_data())) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to try lock thread data", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (sess_info->get_effective_tenant_id() != tenant_id_) {
        // do nothing
        (void)sess_info->unlock_thread_data();
      } else if (sess_info->is_coninfo_set_by_sess()) {
        // already has, do nothing
        (void)sess_info->unlock_thread_data();
      } else {
        sess_info->set_send_control_info(false);
        sess_info->get_control_info().reset();
        (void)sess_info->unlock_thread_data();
      }
      (void)sess_info->unlock_query();
    }
  }
  return OB_SUCCESS == ret;
}

bool ObFLTApplyByClientIDOp::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (false == sess_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is not valid", K(ret));
  } else {
    // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (OB_FAIL(sess_info->try_lock_thread_data())) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to try lock thread data", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        if (sess_info->is_coninfo_set_by_sess()) {
          // already has, do nothing
        } else if (sess_info->get_effective_tenant_id() != tenant_id_) {
          // do nothing
        } else if (sess_info->get_client_identifier().case_compare(id_con_info_.identifier_name_) != 0) {
          // do nothing
        } else {
          sess_info->set_send_control_info(false);
          sess_info->set_flt_control_info(id_con_info_.control_info_);
        }
        (void)sess_info->unlock_thread_data();
      }
      (void)sess_info->unlock_query();
    }
  }
  return OB_SUCCESS == ret;
}

bool ObFLTApplyByModActOp::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (false == sess_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is not valid", K(ret));
  } else {
    // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (OB_FAIL(sess_info->try_lock_thread_data())) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to try lock thread data", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        if (sess_info->is_coninfo_set_by_sess()) {
          // already has, do nothing
        } else if (sess_info->get_effective_tenant_id() != tenant_id_) {
        // do nothing
        } else if (sess_info->get_module_name().case_compare(mod_act_info_.mod_name_) != 0) {
        // do nothing
        } else if (sess_info->get_action_name().case_compare(mod_act_info_.act_name_) != 0) {
        // do nothing
        } else {
          sess_info->set_send_control_info(false);
          sess_info->set_flt_control_info(mod_act_info_.control_info_);
        }
        (void)sess_info->unlock_thread_data();
      }
      (void)sess_info->unlock_query();
    }
  }
  return OB_SUCCESS == ret;
}

bool ObFLTApplyByTenantOp::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (false == sess_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is not valid", K(ret));
  } else {
    // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (OB_FAIL(sess_info->try_lock_thread_data())) {
          if (OB_UNLIKELY(OB_EAGAIN != ret)) {
            LOG_WARN("fail to try lock thread data", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
      } else {
        if (sess_info->is_coninfo_set_by_sess()) {
          // already has, do nothing
        } else if (sess_info->get_effective_tenant_id() != tenant_id_) {
          // do nothing
        } else {
          sess_info->set_send_control_info(false);
          sess_info->set_flt_control_info(tenant_info_);
        }
        (void)sess_info->unlock_thread_data();
      }
      (void)sess_info->unlock_query();
    }
  }
  return OB_SUCCESS == ret;
}

int ObFLTControlInfoManager::apply_control_info()
{
  int ret = OB_SUCCESS;

  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  ObFLTResetSessOp reset_op(tenant_id_);
  if (OB_ISNULL(session_mgr)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "sessionMgr is NULL", K(ret));
  } else if (OB_FAIL(session_mgr->for_each_session(reset_op))) {
    SERVER_LOG(WARN, "fill scanner fail", K(ret));
  } else {
    // do nothing
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!tenant_info_.is_valid()) {
    // do nothing
  } else {
    ObFLTApplyByTenantOp t_op(tenant_id_, tenant_info_);
    if (OB_FAIL(session_mgr->for_each_session(t_op))) {
      SERVER_LOG(WARN, "fill scanner fail", K(ret));
    } else {
      // do nothing
    }
  }
 
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (mod_infos_.count() <= 0) {
    // do nothing
  } else {
    for (int64_t i=0; OB_SUCC(ret) && i<mod_infos_.count(); i++) {
      ObFLTApplyByModActOp mod_op(tenant_id_, mod_infos_.at(i));
      if (OB_FAIL(session_mgr->for_each_session(mod_op))) {
        SERVER_LOG(WARN, "fill scanner fail", K(ret));
      } else {
        // do nothing
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (identifier_infos_.count() <= 0) {
    // do nothing
  } else {
    for (int64_t i=0; OB_SUCC(ret) && i<identifier_infos_.count(); i++) {
      ObFLTApplyByClientIDOp i_op(tenant_id_, identifier_infos_.at(i));
      if (OB_FAIL(session_mgr->for_each_session(i_op))) {
        SERVER_LOG(WARN, "fill scanner fail", K(ret), K(lbt()));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObFLTControlInfoManager::set_control_info(sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
  int64_t affected_rows = 0;
  ObSqlString sql;
  int pos = 0;
  Value *val = NULL;
  // generate json
  HEAP_VAR(char[OB_MAX_CONFIG_VALUE_LEN], buf) {
    if (OB_FAIL(to_json(alloc_, val))) {
      LOG_WARN("failed to generate json", K(ret));
    } else {
      Tidy tidy(val);
      // from json to string
      pos = tidy.to_string(buf, OB_MAX_CONFIG_VALUE_LEN);
      ObString trace_info(pos, buf);

      // write sql
      if (OB_ISNULL(sql_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_proxy is null", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("ALTER SYSTEM SET `_trace_control_info` = '%s'", trace_info.ptr()))) {
        LOG_WARN("failed to set trace control info", K(ret));
      } else if (OB_FAIL(sql_proxy->write(tenant_id_, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}


int ObFLTControlInfoManager::from_json_control_info(FLTControlInfo &control_info, json::Pair* it) {
  int ret = OB_SUCCESS;
  if (it->name_.case_compare(LEVEL) == 0) {
    if (NULL == it->value_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL value pointer", K(ret));
    } else if (json::JT_NUMBER != it->value_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected address type", K(ret), "type", it->value_->get_type());
    } else {
      control_info.level_ = it->value_->get_number();
    }
  } else if (it->name_.case_compare(SAM_PCT) == 0) {
    if (NULL == it->value_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL value pointer", K(ret));
    } else if (json::JT_STRING != it->value_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected address type", K(ret), "type", it->value_->get_type());
    } else {
      ObString sample_pct_val = it->value_->get_string();
      control_info.sample_pct_ = atof(sample_pct_val.ptr());
    }
  } else if (it->name_.case_compare(REC_POL) == 0) {
    if (NULL == it->value_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL value pointer", K(ret));
    } else if (json::JT_STRING != it->value_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected address type", K(ret), "type", it->value_->get_type());
    } else {
      ObString rp_val = it->value_->get_string();
      if (0 == rp_val.case_compare(RP_ALL)) {
        control_info.rp_ = FLTControlInfo::RecordPolicy::RP_ALL;
      } else if (0 == rp_val.case_compare(RP_ONLY_SLOW_QUERY)) {
        control_info.rp_ = FLTControlInfo::RecordPolicy::RP_ONLY_SLOW_QUERY;
      } else if (0 == rp_val.case_compare(RP_SAMPLE_AND_SLOW_QUERY)) {
        control_info.rp_ = FLTControlInfo::RecordPolicy::RP_SAMPLE_AND_SLOW_QUERY;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid record policy", K(ret));
      }
    }
  }
  return ret;
}

int ObFLTControlInfoManager::from_json_mod_act(json::Value *&con_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(con_val)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("con_val is null", K(ret));
  } else if (json::JT_OBJECT != con_val->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs list is not array", K(ret), "type", con_val->get_type());
  } else {
    // assmble ObIdentifierConInfo
    ObModActConInfo mod_con;
    DLIST_FOREACH(it, con_val->get_object()) {
      if (it->name_.case_compare(MOD_NAME) == 0) {
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_STRING != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected address type", K(ret), "type", it->value_->get_type());
        } else if (OB_FAIL(ob_write_string(alloc_, it->value_->get_string(),
                                        mod_con.mod_name_))) {
          LOG_WARN("fail to write string", K(it->value_->get_string()), K(ret));
        } else {
          // do nothing
        }
      } else if (it->name_.case_compare(ACT_NAME) == 0) {
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_STRING != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected address type", K(ret), "type", it->value_->get_type());
        } else if (OB_FAIL(ob_write_string(alloc_, it->value_->get_string(),
                                        mod_con.act_name_))) {
          LOG_WARN("fail to write string", K(it->value_->get_string()), K(ret));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(from_json_control_info(mod_con.control_info_, it))) {
        LOG_WARN("failed to resolve control_info", K(ret));
      }
    }
    if (OB_FAIL(mod_infos_.push_back(mod_con))) {
      LOG_WARN("failed to push back id info", K(ret));
    }
  }

  return ret;
}
int ObFLTControlInfoManager::from_json_identifier(json::Value *&con_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(con_val)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("con_val is null", K(ret));
  } else if (json::JT_OBJECT != con_val->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs list is not array", K(ret), "type", con_val->get_type());
  } else {
    // assmble ObIdentifierConInfo
    ObIdentifierConInfo client_id_con_info;
    DLIST_FOREACH(it, con_val->get_object()) {
      if (it->name_.case_compare(ID_NAME) == 0) {
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_STRING != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected address type", K(ret), "type", it->value_->get_type());
        } else if (OB_FAIL(ob_write_string(alloc_, it->value_->get_string(),
                                        client_id_con_info.identifier_name_))) {
          LOG_WARN("fail to write string", K(it->value_->get_string()), K(ret));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(from_json_control_info(client_id_con_info.control_info_, it))) {
        LOG_WARN("failed to resolve control_info", K(ret));
      }
    }
    if (OB_FAIL(identifier_infos_.push_back(client_id_con_info))) {
      LOG_WARN("failed to push back id info", K(ret));
    }
  }
  return ret;
}

int ObFLTControlInfoManager::from_json(ObArenaAllocator &allocator, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  Value* data = NULL;
  Parser parser;
  if (buf_len == 0) {
    // do nothing
  } else if (buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf len is not valid", K(ret), K(buf_len));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(buf, buf_len, data))) {
    LOG_WARN("parse json failed", K(ret));
  } else if (NULL == data) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret));
  } else {
    DLIST_FOREACH_X(it, data->get_object(), OB_SUCC(ret)) {
      //bool find = false;
      if (it->name_.case_compare(TYPE_I) == 0 ) {
        //find = true;
        if (json::JT_ARRAY != it->value_->get_type()) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config", K(ret));
          break;
        } else {
          DLIST_FOREACH(client_id, it->value_->get_array()) {
            if (json::JT_OBJECT != client_id->get_type()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("not object in array", K(ret), "type", client_id->get_type());
              break;
            } else if (OB_FAIL(from_json_identifier(client_id))) {
              LOG_WARN("failed to resolve control info from json", K(ret));
            } else {
              // do nothing
            }
          }
        }
      } else if (it->name_.case_compare(TYPE_MOD_ACT) == 0) {
        if (json::JT_ARRAY != it->value_->get_type()) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config", K(ret));
          break;
        } else {
          DLIST_FOREACH(mod_act, it->value_->get_array()) {
            if (json::JT_OBJECT != mod_act->get_type()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("not object in array", K(ret), "type", mod_act->get_type());
              break;
            } else if (OB_FAIL(from_json_mod_act(mod_act))) {
              LOG_WARN("failed to resolve control info from json", K(ret));
            } else {
              // do nothing
            }
          }
        }
      } else if (it->name_.case_compare(TYPE_TENANT) == 0) {
        if (json::JT_OBJECT != it->value_->get_type()) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config", K(ret));
          break;
        } else if (OB_ISNULL(it->value_->get_object().get_first())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config, get unexpected null", K(ret));
          break;
        } else {
          DLIST_FOREACH(x, it->value_->get_object()) {
            if (OB_FAIL(from_json_control_info(tenant_info_, x))){
              LOG_WARN("failed to resolve control info from json", K(ret));
            } else {
            }
          }
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObFLTControlInfoManager::to_json_contorl_info(ObIAllocator &allocator,
                                                  FLTControlInfo flt_con,
                                                  json::Value *&ret_val)
{
  int ret = OB_SUCCESS;
  Pair *level = NULL;
  Value *level_val = NULL;
  Pair *sample_pct = NULL;
  Value *sample_pct_val = NULL;
  Pair *rp = NULL;
  Value *rp_val = NULL;
  char* sample_pct_buf = NULL;
  int64_t sample_pct_buf_sz = 10;

  if (OB_ISNULL(sample_pct_buf = (char *)allocator.alloc(sample_pct_buf_sz))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate sample_pct buf");
  } else if (OB_ISNULL(level = (Pair*)allocator.alloc(sizeof(Pair)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate json value");
  } else if (OB_ISNULL(level = new(level) Pair())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json pair");
  } else if (OB_ISNULL(level_val = (Value*)allocator.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate json value");
  } else if (OB_ISNULL(level_val = new(level_val)Value())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json Value");
  } else if (OB_ISNULL(sample_pct = (Pair*)allocator.alloc(sizeof(Pair)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate json value");
  } else if (OB_ISNULL(sample_pct = new(sample_pct) Pair())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json pair");
  } else if (OB_ISNULL(sample_pct_val = (Value*)allocator.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate json value");
  } else if (OB_ISNULL(sample_pct_val = new(sample_pct_val)Value())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json Value");
  } else if (OB_ISNULL(rp = (Pair*)allocator.alloc(sizeof(Pair)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate json value");
  } else if (OB_ISNULL(rp = new(rp) Pair())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json pair");
  } else if (OB_ISNULL(rp_val = (Value*)allocator.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate json value");
  } else if (OB_ISNULL(rp_val = new(rp_val)Value())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json Value");
  } else {
    // level
    level_val->set_type(JT_NUMBER);
    level_val->set_int(flt_con.level_);
    level->name_ = LEVEL;
    level->value_ = level_val;

    // sample pct
    sample_pct_val->set_type(JT_STRING);
    int64_t len = sprintf(sample_pct_buf, "%.2f", flt_con.sample_pct_);
    sample_pct_val->set_string(sample_pct_buf, len);
    sample_pct->name_ = SAM_PCT;
    sample_pct->value_ = sample_pct_val;

    // record policy
    ObString rp_str;
    if (FLTControlInfo::RecordPolicy::RP_ALL == flt_con.rp_) {
      rp_str = RP_ALL;
    } else if (FLTControlInfo::RecordPolicy::RP_ONLY_SLOW_QUERY == flt_con.rp_) {
      rp_str = RP_ONLY_SLOW_QUERY;
    } else if (FLTControlInfo::RecordPolicy::RP_SAMPLE_AND_SLOW_QUERY == flt_con.rp_) {
      rp_str = RP_SAMPLE_AND_SLOW_QUERY;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected record policy type", K(flt_con.rp_));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      rp_val->set_type(JT_STRING);
      rp_val->set_string(rp_str.ptr(), rp_str.length());
      rp->name_ = REC_POL;
      rp->value_ = rp_val;

      // assamble control info
      ret_val->object_add(level);
      ret_val->object_add(sample_pct);
      ret_val->object_add(rp);
    }
  }

  return ret;
}

int ObFLTControlInfoManager::to_json_identifier(ObIAllocator &allocator, json::Value *&ret_val)
{
  int ret = OB_SUCCESS;
  Value *id_val = NULL;
  Pair *id_name = NULL;
  Value *id_name_val = NULL;


  for (int64_t i=0; OB_SUCC(ret) && i<identifier_infos_.count(); i++) {
    if (OB_ISNULL(id_val = (Value*)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(id_val = new(id_val)Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(id_name = (Pair*)allocator.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(id_name = new(id_name) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json pair");
    } else if (OB_ISNULL(id_name_val = (Value*)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(id_name_val = new(id_name_val)Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else  {
      id_val->set_type(JT_OBJECT);

      id_name_val->set_type(JT_STRING);
      id_name_val->set_string(identifier_infos_.at(i).identifier_name_.ptr(),
                              identifier_infos_.at(i).identifier_name_.length());
      id_name->name_ = ID_NAME;
      id_name->value_ = id_name_val;
      id_val->object_add(id_name);
      if (OB_FAIL(to_json_contorl_info(allocator, identifier_infos_.at(i).control_info_, id_val))) {
        LOG_WARN("failed to convert control info to json", K(ret));
      }
      ret_val->array_add(id_val);
    }
  }

  return ret;
}

int ObFLTControlInfoManager::to_json_mod_act(ObIAllocator &allocator, json::Value *&ret_val)
{
  int ret = OB_SUCCESS;
  Value *mod_act_val = NULL;
  Pair *mod_name = NULL;
  Value *mod_name_val = NULL;
  Pair *act_name = NULL;
  Value *act_name_val = NULL;

  for (int64_t i=0; OB_SUCC(ret) && i<mod_infos_.count(); i++) {
    if (OB_ISNULL(mod_act_val = (Value*)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(mod_act_val = new(mod_act_val)Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(mod_name = (Pair*)allocator.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(mod_name = new(mod_name) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json pair");
    } else if (OB_ISNULL(mod_name_val = (Value*)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(mod_name_val = new(mod_name_val)Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(act_name = (Pair*)allocator.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(act_name = new(act_name) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json pair");
    } else if (OB_ISNULL(act_name_val = (Value*)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate json value");
    } else if (OB_ISNULL(act_name_val = new(act_name_val)Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else  {
      mod_act_val->set_type(JT_OBJECT);

      mod_name_val->set_type(JT_STRING);
      mod_name_val->set_string(mod_infos_.at(i).mod_name_.ptr(),
                              mod_infos_.at(i).mod_name_.length());
      mod_name->name_ = MOD_NAME;
      mod_name->value_ = mod_name_val;
      act_name_val->set_type(JT_STRING);
      act_name_val->set_string(mod_infos_.at(i).act_name_.ptr(),
                              mod_infos_.at(i).act_name_.length());
      act_name->name_ = ACT_NAME;
      act_name->value_ = act_name_val;
      mod_act_val->object_add(mod_name);
      mod_act_val->object_add(act_name);
      if (OB_FAIL(to_json_contorl_info(allocator, mod_infos_.at(i).control_info_, mod_act_val))) {
        LOG_WARN("failed to convert control info to json", K(ret));
      }
      ret_val->array_add(mod_act_val);
    }
  }

  return ret;
}

int ObFLTControlInfoManager::to_json(ObIAllocator &allocator, json::Value *&ret_val)
{
  int ret = OB_SUCCESS;
  json::Pair *type_i = NULL;
  json::Pair *type_m = NULL;
  json::Pair *type_t = NULL;

  Value *type_i_value = NULL;
  Value *type_m_value = NULL;
  Value *type_t_value = NULL;
  if (OB_ISNULL(ret_val = (Value *)allocator.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  } else if (OB_ISNULL(ret_val = new(ret_val) Value())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json Value");

  } else if (OB_ISNULL(type_m = (Pair *)allocator.alloc(sizeof(Pair)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  } else if (OB_ISNULL(type_m = new(type_m) Pair())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json pair");
  } else if (OB_ISNULL(type_t = (Pair *)allocator.alloc(sizeof(Pair)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  } else if (OB_ISNULL(type_t = new(type_t) Pair())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json pair");
  } else if (OB_ISNULL(type_m_value = (Value *)allocator.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  } else if (OB_ISNULL(type_m_value = new(type_m_value) Value())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json Value");
  } else if (OB_ISNULL(type_t_value = (Value *)allocator.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  } else if (OB_ISNULL(type_t_value = new(type_t_value) Value())) {
    ret = OB_ERROR;
    LOG_WARN("failed to new json Value");
  } else {
    ret_val->set_type(JT_OBJECT);

    // assamble identifier
    int N = identifier_infos_.count();
    if (0 >= N) {
      // do nothing
    } else if (OB_ISNULL(type_i = (Pair *)allocator.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(type_i = new(type_i) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json pair");
    } else if (OB_ISNULL(type_i_value = (Value *)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(type_i_value = new(type_i_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else {
      type_i_value->set_type(JT_ARRAY);
      type_i->name_ = TYPE_I;
      if (OB_FAIL(to_json_identifier(allocator, type_i_value))) {
        LOG_WARN("failed to generate json fo identifier", K(ret));
      } else {
        type_i->value_ = type_i_value;
      }
      ret_val->object_add(type_i);
    }

    // assamble mod_infos_ 
    N = mod_infos_.count();
    if (0 >= N) {
      // do nothing
    } else if (OB_ISNULL(type_m = (Pair *)allocator.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(type_m = new(type_m) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json pair");
    } else if (OB_ISNULL(type_m_value = (Value *)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(type_m_value = new(type_m_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else {
      type_m_value->set_type(JT_ARRAY);
      type_m->name_ = TYPE_MOD_ACT;
      if (OB_FAIL(to_json_mod_act(allocator, type_m_value))) {
        LOG_WARN("failed to generate json fo identifier", K(ret));
      } else {
        type_m->value_ = type_m_value;
      }
      ret_val->object_add(type_m);
    }

    // assamble tenant
    if (!tenant_info_.is_valid()) {
      // do nothing
    } else if (OB_ISNULL(type_t = (Pair *)allocator.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(type_t = new(type_t) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json pair");
    } else if (OB_ISNULL(type_t_value = (Value *)allocator.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(type_t_value = new(type_t_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else {
      type_t_value->set_type(JT_OBJECT);
      type_t->name_ = TYPE_TENANT;
      if (OB_FAIL(to_json_contorl_info(allocator, tenant_info_, type_t_value))) {
        LOG_WARN("failed to generate json fo identifier", K(ret));
      } else {
        type_t->value_ = type_t_value;
      }
      ret_val->object_add(type_t);
    }
  }
  return ret;
}

int ObFLTControlInfoManager::add_identifier_con_info(sql::ObExecContext &ctx, ObIdentifierConInfo i_coninfo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(identifier_infos_.push_back(i_coninfo))) {
    LOG_WARN("failed to add identifier infos", K(ret));
  } else if (OB_FAIL(set_control_info(ctx))) {
    LOG_WARN("failed to set control info", K(ret));
  } else if (OB_FAIL(apply_control_info())) {
    LOG_WARN("failed to apply control info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::find_identifier_con_info(common::ObString client_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  bool find = false;
  for (int64_t i=0; !find && i<identifier_infos_.count(); i++) {
    if (identifier_infos_.at(i).identifier_name_.case_compare(client_id)==0) {
      idx = i;
      find = true;
    }
  }
  return ret;
}

int ObFLTControlInfoManager::remove_identifier_con_info(sql::ObExecContext &ctx, common::ObString client_id)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_FAIL(find_identifier_con_info(client_id, idx))) {
    LOG_WARN("failed to find module action control info", K(ret));
  } else if (idx >= 0 && idx < identifier_infos_.count() && OB_FAIL(identifier_infos_.remove(idx))) {
    LOG_WARN("failed to remove identifier infos ", K(ret), K(idx));
  } else if (OB_FAIL(set_control_info(ctx))) {
    LOG_WARN("failed to set control info", K(ret));
  } else if (OB_FAIL(apply_control_info())) {
    LOG_WARN("failed to apply control info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::add_mod_act_con_info(sql::ObExecContext &ctx, ObModActConInfo mod_coninfo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mod_infos_.push_back(mod_coninfo))) {
    LOG_WARN("failed to add identifier infos", K(ret));
  } else if (OB_FAIL(set_control_info(ctx))) {
    LOG_WARN("failed to set control info", K(ret));
  } else if (OB_FAIL(apply_control_info())) {
    LOG_WARN("failed to apply control info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::find_mod_act_con_info(common::ObString mod, common::ObString act, int64_t &idx)
{
  int ret = OB_SUCCESS;
  bool find = false;
  for (int64_t i=0; !find && i<mod_infos_.count(); i++) {
    if (mod_infos_.at(i).mod_name_.case_compare(mod)==0 &&
          mod_infos_.at(i).act_name_.case_compare(act) == 0) {
      idx = i;
      find = true;
    }
  }
  return ret;
}


int ObFLTControlInfoManager::remove_mod_act_con_info(sql::ObExecContext &ctx,
                                    common::ObString mod, common::ObString act)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_FAIL(find_mod_act_con_info(mod, act, idx))) {
    LOG_WARN("failed to find module action control info", K(ret));
  } else if (idx >= 0 && idx < mod_infos_.count() && OB_FAIL(mod_infos_.remove(idx))) {
    LOG_WARN("failed to remove mod_infos", K(ret), K(idx));
  } else if (OB_FAIL(set_control_info(ctx))) {
    LOG_WARN("failed to set control info", K(ret));
  } else if (OB_FAIL(apply_control_info())) {
    LOG_WARN("failed to apply control info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::add_tenant_con_info(sql::ObExecContext &ctx, FLTControlInfo coninfo)
{
  int ret = OB_SUCCESS;
  tenant_info_ = coninfo;
  if (OB_FAIL(set_control_info(ctx))) {
    LOG_WARN("failed to set control info", K(ret));
  } else if (OB_FAIL(apply_control_info())) {
    LOG_WARN("failed to apply control info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::remove_tenant_con_info(sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  tenant_info_.reset();
  if (OB_FAIL(set_control_info(ctx))) {
    LOG_WARN("failed to set control info", K(ret));
  } else if (OB_FAIL(apply_control_info())) {
    LOG_WARN("failed to apply control info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::init()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()){
    // do nothing
  } else {
    ObString trace_info(strlen(tenant_config->_trace_control_info.str()),
                                tenant_config->_trace_control_info.str());
    if (OB_FAIL(from_json(alloc_, trace_info.ptr(), trace_info.length()))) {
      LOG_WARN("failed to resolve json val", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObFLTControlInfoManager::get_mod_act_con_info(common::ObString mod,
                                                  common::ObString act,
                                                  FLTControlInfo &coninfo)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_FAIL(find_mod_act_con_info(mod, act, idx))) {
    LOG_WARN("failed to find module action control info", K(ret));
  } else if (idx >= 0 && idx < mod_infos_.count()) {
    coninfo = mod_infos_.at(idx).control_info_;
  } else {
    coninfo.reset();
    // no rule, do nothing
  }
  return ret;
}

int ObFLTControlInfoManager::get_client_id_con_info(common::ObString client_id,
                                                    FLTControlInfo &coninfo)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_FAIL(find_identifier_con_info(client_id, idx))) {
      LOG_WARN("failed to find module action control info", K(ret));
  } else if (idx >= 0 && idx < identifier_infos_.count()) {
    coninfo = identifier_infos_.at(idx).control_info_;
  } else {
    coninfo.reset();
    // no rule, do nothing
  }
  return ret;
}


int ObFLTControlInfoManager::find_appropriate_con_info(sql::ObSQLSessionInfo &sess) {
  int ret = OB_SUCCESS;

  FLTControlInfo con;
  FLTControlInfo con_info;
  con_info.reset();
  LOG_TRACE("flt control info manager get control info", K(sess.get_module_name()), K(sess.get_action_name()), K(sess.get_client_identifier()));
  if (sess.is_coninfo_set_by_sess()) {
    // already has, do nothing because sess level control info is the highest level
  } else {
    if (tenant_info_.is_valid()) {
      con_info = tenant_info_;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (sess.get_module_name().empty() 
              || sess.get_action_name().empty()) {
      // do nothing
    } else if (OB_FAIL(get_mod_act_con_info(sess.get_module_name(),
                                        sess.get_action_name(), con))) {
      LOG_WARN("failed to get control info for client info", K(ret));
    } else if (!con.is_valid()) {
      // not find
    } else {
      con_info = con;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (sess.get_client_identifier().empty()) {
      // do nothing
    } else if (OB_FAIL(get_client_id_con_info(sess.get_client_identifier(), con))) {
      LOG_WARN("failed to get control info for client info", K(ret));
    } else if (!con.is_valid()) {
      // not find
    } else {
      con_info = con;
    }

    sess.set_send_control_info(false);
    sess.set_flt_control_info(con_info);
  }
  LOG_TRACE("control info manager get constrol info end", K(con_info), K(con));
  return ret;
}

int ObFLTControlInfoManager::get_all_flt_config(common::ObIArray<ObFLTConfRec> &rec_list, ObIAllocator &alloc) {
  int ret = OB_SUCCESS;
  // teannt_level
  ObFLTConfRec rec;
  rec.tenant_id_ = tenant_id_;
  rec.type_ = FLT_TENANT_TYPE;
  rec.control_info_ = tenant_info_;
  if (OB_FAIL(rec_list.push_back(rec))) {
    LOG_WARN("failed to push back flt config rec", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < mod_infos_.count(); i++) {
    ObFLTConfRec rec;
    rec.tenant_id_ = tenant_id_;
    rec.type_ = FLT_MOD_ACT_TYPE;
    rec.control_info_ = mod_infos_.at(i).control_info_;
    if (OB_FAIL(ob_write_string(alloc, mod_infos_.at(i).mod_name_, rec.mod_name_))) {
      LOG_WARN("failed to write string", K(mod_infos_.at(i).mod_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(alloc, mod_infos_.at(i).act_name_, rec.act_name_))) {
      LOG_WARN("failed to write string", K(mod_infos_.at(i).act_name_), K(ret));
    } else if (OB_FAIL(rec_list.push_back(rec))) {
      LOG_WARN("failed to push back flt config rec", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < identifier_infos_.count(); i++) {
    ObFLTConfRec rec;
    rec.tenant_id_ = tenant_id_;
    rec.type_ = FLT_CLIENT_ID_TYPE;
    rec.control_info_ = identifier_infos_.at(i).control_info_;
    if (OB_FAIL(ob_write_string(alloc, identifier_infos_.at(i).identifier_name_, rec.identifier_name_))) {
      LOG_WARN("failed to write string", K(mod_infos_.at(i).mod_name_), K(ret));
    } else if (OB_FAIL(rec_list.push_back(rec))) {
      LOG_WARN("failed to push back flt config rec", K(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
