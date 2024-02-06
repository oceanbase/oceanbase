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

#ifndef OCEANBASE_SQL_SECURITY_AUDIT_H
#define OCEANBASE_SQL_SECURITY_AUDIT_H
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/net/ob_addr.h"
#include "lib/oblog/ob_log.h"
#include "sql/ob_sql_define.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace sql
{
#ifdef OB_BUILD_AUDIT_SECURITY
//Audit file /data/log1/oracle/oracle/admin/orcl/adump/orcl_ora_998570_20181225191358365255143795.aud
//Oracle Database 12c Enterprise Edition Release 12.2.0.1.0 - 64bit Production
//Build label:    RDBMS_12.2.0.1.0_LINUX.X64_170125
//ORACLE_HOME:    /data/log1/oracle/oracle/product/12.2.0
//System name:    Linux
//Node name:      OceanBase004065.sqa.ztt
//Release:        3.10.0-327.ali2000.alios7.x86_64
//Version:        #1 SMP Tue Dec 29 19:54:05 CST 2015
//Machine:        x86_64
//Instance name: orcl
//Redo thread mounted by this instance: 1
//Oracle process number: 165
//Unix process pid: 998570, image: oracle@OceanBase004065.sqa.ztt
//
//Tue Dec 25 19:13:58 2018 +08:00
//LENGTH : '336'
//ACTION :[7] 'CONNECT'
//DATABASE USER:[3] 'SYS'
//PRIVILEGE :[6] 'SYSDBA'
//CLIENT USER:[9] 'xiaoyi.xy'
//CLIENT TERMINAL:[5] 'pts/7'
//STATUS:[1] '0'
//DBID:[10] '1492669708'
//SESSIONID:[10] '4294967295'
//USERHOST:[23] 'OceanBase224012.sqa.bja'
//CLIENT ADDRESS:[56] '(ADDRESS=(PROTOCOL=tcp)(HOST=10.125.224.12)(PORT=53119))'
//ACTION NUMBER:[3] '100'
class ObSecurityAuditData : public common::ObBasebLogPrint
{
public:
  ObSecurityAuditData()
    : ObBasebLogPrint(), is_const_filled_(false), length_(0), svr_port_(0), tenant_id_(0),
      user_id_(0), effective_user_id_(0), proxy_session_id_(0), session_id_(0),
      entry_id_(0), statement_id_(0), commit_version_(0), trace_id_{0}, db_id_(0), cur_db_id_(0),
      sql_timestamp_us_(0), record_timestamp_us_(0), audit_id_(0), audit_type_(0),
      operation_type_(0), action_id_(0), return_code_(0),
      logoff_logical_read_(0), logoff_physical_read_(0), logoff_logical_write_(0),
      logoff_lock_count_(0), logoff_cpu_time_us_(0), logoff_exec_time_us_(0),
      logoff_alive_time_us_(0)
  {}

  virtual ~ObSecurityAuditData() {}
  void reset() { new (this) ObSecurityAuditData(); }

  TO_STRING_KV(K_(is_const_filled), K_(svr_ip), K_(svr_port), K_(tenant_id), K_(tenant_name),
               K_(user_id), K_(user_name), K_(effective_user_id), K_(effective_user_name),
               K_(client_ip), K_(user_client_ip), K_(proxy_session_id), K_(session_id),
               K_(entry_id), K_(statement_id), K_(trans_id), K_(commit_version),
               K(trace_id_[0]), K(trace_id_[1]), K_(cur_db_id), K_(cur_db_name),
               K_(db_id), K_(db_name), K_(sql_timestamp_us), K_(record_timestamp_us),
               K_(audit_id), K_(audit_type), K_(operation_type), K_(action_id),
               K_(return_code), K_(sql_text), K_(auth_privileges), K_(auth_grantee));

  void calc_total_length();
  virtual int64_t get_data_length() const { return length_; }
  virtual int64_t get_timestamp() const { return record_timestamp_us_; }
  virtual int print_data(char *buf, int64_t buf_len, int64_t &pos) const;

  inline static uint64_t get_next_entry_id()
  {
    static uint64_t next_entry_id = 1;
    return ATOMIC_FAA(&next_entry_id, 1);
  }

public:
  bool is_const_filled_;
  //refers to the total number of bytes used in this audit record.
  //This number includes the trailing newline bytes (\n), if any,
  //at the end of the audit record.
  int64_t length_;

  //SERVER ADDR
  common::ObString svr_ip_;
  int32_t svr_port_;

  //TENANTID
  uint64_t tenant_id_;

  //CLIENT TENANT
  common::ObString tenant_name_;

  //USERID
  uint64_t user_id_;
  common::ObString user_name_;
  uint64_t effective_user_id_;
  common::ObString effective_user_name_;

  //Client host machine name
  //CLIENT ADDRESS
  common::ObString client_ip_;

  //PROXY_CLIENT ADDRESS
  common::ObString user_client_ip_;

  //OB_PROXY_SESSIONID
  uint64_t proxy_session_id_;

  //Numeric ID for each Oracle session. Each user session gets a unique session ID.
  //SESSIONID
  uint64_t session_id_;


  //indicates the current audit entry number, assigned to each audit trail record.
  //The audit ENTRYID sequence number is shared between fine-grained audit records
  //and regular audit records
  //ENTRYID
  uint64_t entry_id_;

  //nth statement in the user session. The first SQL statement gets a value of 1 and
  //the value is incremented for each subsequent SQL statement.
  //Note that one SQL statement can create more than one audit trail entry
  //(for example, when more than one object is audited from the same SQL statement),
  //and in this case the statement ID remains the same for that statement
  //and the entry ID increases for each audit trail entry created by the statement.
  //STATEMENT
  uint64_t statement_id_;

  common::ObString trans_id_;

  int64_t commit_version_;

  uint64_t trace_id_[2];

  //is a database identifier calculated when the database is created.
  //It corresponds to the DBID column of the V$DATABASE data dictionary view.
  uint64_t db_id_;

  uint64_t cur_db_id_;

  //DATABASE USER
  common::ObString db_name_;
  common::ObString cur_db_name_;

  int64_t sql_timestamp_us_;

  //Date and time of the creation of the audit trail entry in the local database session time zone
  int64_t record_timestamp_us_;

  uint64_t audit_id_;
  uint64_t audit_type_;
  uint64_t operation_type_;

  //is a numeric value representing the action the user performed.
  //The corresponding name of the action type is in the AUDIT_ACTIONS table.
  //For example, action 100 refers to LOGON
  //ACTION_NUMBER
  uint64_t action_id_;

  //indicates if the audited action was successful. 0 indicates success.
  //If the action fails, the return code lists the Oracle Database error number.
  //For example, if you try to drop a non-existent table, the error number
  //is ORA-00903 invalid table name, which in turn translates to 903 in the RETURNCODE setting.
  //RETURNCODE
  int return_code_;

  common::ObString obj_owner_name_;
  common::ObString obj_name_;
  common::ObString new_obj_owner_name_;
  common::ObString new_obj_name_;

  common::ObString auth_privileges_;
  common::ObString auth_grantee_;

  uint64_t logoff_logical_read_;
  uint64_t logoff_physical_read_;
  uint64_t logoff_logical_write_;
  uint64_t logoff_lock_count_;
  common::ObString logoff_dead_lock_;
  uint64_t logoff_cpu_time_us_;
  uint64_t logoff_exec_time_us_;
  uint64_t logoff_alive_time_us_;


  common::ObString comment_text_;
  common::ObString sql_bind_;
  common::ObString sql_text_;
};
#endif
} //namespace sql
} //namespace oceanbase
#endif


