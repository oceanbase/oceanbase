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

#ifndef OCEANBASE_SQL_OB_FLT_CONTROL_INFO_MGR_H_
#define OCEANBASE_SQL_OB_FLT_CONTROL_INFO_MGR_H_
#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include "lib/signal/ob_signal_utils.h"
#include "sql/engine/ob_exec_context.h"


namespace oceanbase
{
namespace sql
{
  // for client identifier
  static const char TYPE_I[] = "type_i";
  // for mod_act
  static const char TYPE_MOD_ACT[] = "type_m";
  // for tenant
  static const char TYPE_TENANT[] = "type_t";
  // for control info
  static const char LEVEL[] = "level";
  static const char SAM_PCT[] = "sample_pct";
  static const char REC_POL[] = "record_policy";

  // for record policy
  static const char RP_ALL[] = "ALL";
  static const char RP_ONLY_SLOW_QUERY[] = "ONLY_SLOW_QUERY";
  static const char RP_SAMPLE_AND_SLOW_QUERY[] = "SAMPLE_AND_SLOW_QUERY";

  static const char ID_NAME[] = "identifier_name";
  static const char MOD_NAME[] = "mod_name";
  static const char ACT_NAME[] = "action_name";

  class ObIdentifierConInfo {
  public:
    ObIdentifierConInfo():
      identifier_name_(),
      control_info_() {}
    ObString identifier_name_;
    FLTControlInfo control_info_;
    TO_STRING_KV(K_(identifier_name), K_(control_info));
  };

  class ObModActConInfo {
  public:
    ObModActConInfo():
      mod_name_(),
      act_name_(),
      control_info_() {}
    ObString mod_name_;
    ObString act_name_;
    FLTControlInfo control_info_;
    TO_STRING_KV(K_(mod_name), K_(act_name), K_(control_info));
  };

  enum ObFLTConfigType {
    FLT_INVALID_TYPE = 0,
    FLT_TENANT_TYPE = 1,
    FLT_MOD_ACT_TYPE = 2,
    FLT_CLIENT_ID_TYPE = 3
  };
  
  class ObFLTConfRec {
  public:
    ObFLTConfRec():
      tenant_id_(OB_INVALID_ID),
      type_(FLT_INVALID_TYPE),
      mod_name_(),
      act_name_(),
      control_info_() {}
  public:
    uint64_t tenant_id_;
    ObFLTConfigType type_;
    ObString mod_name_;
    ObString act_name_;
    ObString identifier_name_;
    FLTControlInfo control_info_;

    TO_STRING_KV(K_(tenant_id), K_(type), K_(mod_name), K_(act_name), K_(identifier_name), K_(control_info));
  };


  class ObFLTControlInfoManager {
    public:
    ObFLTControlInfoManager(uint64_t t_id) :
      tenant_id_(t_id),
      identifier_infos_(),
      mod_infos_(),
      tenant_info_()
    {}
    int to_json(ObIAllocator &allocator, json::Value *&ret_val);
    int from_json(ObArenaAllocator &allocator, char *buf, const int64_t buf_len);
    int from_json_identifier(json::Value *&con_val);
    int from_json_control_info(FLTControlInfo &control_info, json::Pair* it);
    int from_json_mod_act(json::Value *&con_val);
    int set_control_info(sql::ObExecContext &ctx);
    int to_json_contorl_info(ObIAllocator &allocator, FLTControlInfo ftl_con, json::Value *&ret_val);
    int to_json_identifier(ObIAllocator &allocator, json::Value *&ret_val);
    int to_json_mod_act(ObIAllocator &allocator, json::Value *&ret_val);
    int add_identifier_con_info(sql::ObExecContext &ctx, ObIdentifierConInfo i_coninfo);
    int remove_identifier_con_info(sql::ObExecContext &ctx, common::ObString client_id);
    int add_mod_act_con_info(sql::ObExecContext &ctx, ObModActConInfo mod_coninfo);
    int remove_mod_act_con_info(sql::ObExecContext &ctx, common::ObString mod, common::ObString act);
    int add_tenant_con_info(sql::ObExecContext &ctx, FLTControlInfo coninfo);
    int remove_tenant_con_info(sql::ObExecContext &ctx);
    int find_identifier_con_info(common::ObString client_id, int64_t &idx);
    int find_mod_act_con_info(common::ObString mod, common::ObString act, int64_t &idx);
    int init();

    int get_mod_act_con_info(common::ObString mod, common::ObString act, FLTControlInfo &coninfo);
    int get_client_id_con_info(common::ObString client_id, FLTControlInfo &coninfo);
    int find_appropriate_con_info(sql::ObSQLSessionInfo &sess);
    int get_all_flt_config(common::ObIArray<ObFLTConfRec> &rec_list, ObIAllocator &allocator);

    bool is_valid_tenant_config() {
      return tenant_info_.is_valid();
    }

    void reset_tenant_config() {
      tenant_info_.reset();
    }

    FLTControlInfo get_control_info() {
      return tenant_info_;
    }

    int apply_control_info();

    private:
    uint64_t tenant_id_;
    // identifier trace
    common::ObSEArray<ObIdentifierConInfo, 1, common::ModulePageAllocator, true> identifier_infos_;
    // mod act trace
    common::ObSEArray<ObModActConInfo, 1, common::ModulePageAllocator, true> mod_infos_;
    // tenant trace
    FLTControlInfo tenant_info_;
    // allocator
    ObArenaAllocator alloc_;
  };

  struct ObFLTApplyByClientIDOp {
  public:
    explicit ObFLTApplyByClientIDOp(uint64_t tenant_id, ObIdentifierConInfo id_con_info)
                    : tenant_id_(tenant_id), id_con_info_(id_con_info) {}
    bool operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info);

  private:
    uint64_t tenant_id_;
    ObIdentifierConInfo id_con_info_;
  };
  
  struct ObFLTApplyByModActOp {
  public:
    explicit ObFLTApplyByModActOp(uint64_t tenant_id, ObModActConInfo mod_act_info)
                    : tenant_id_(tenant_id), mod_act_info_(mod_act_info) {}
    bool operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info);

  private:
    uint64_t tenant_id_;
    ObModActConInfo mod_act_info_;
  };
  
  struct ObFLTApplyByTenantOp {
  public:
    explicit ObFLTApplyByTenantOp(uint64_t tenant_id, FLTControlInfo tenant_info)
                    : tenant_id_(tenant_id), tenant_info_(tenant_info) {}
    bool operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info);

  private:
    uint64_t tenant_id_;
    FLTControlInfo tenant_info_;
  };

  struct ObFLTResetSessOp {
  public:
    explicit ObFLTResetSessOp(uint64_t tenant_id) : tenant_id_(tenant_id){}
    bool operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info);
  private:
    uint64_t tenant_id_;
  };
} // namespace sql
} // namespace oceanbase


#endif
