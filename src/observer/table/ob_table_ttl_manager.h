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

#ifndef _OB_TABLE_TTL_MANAGER_H
#define _OB_TABLE_TTL_MANAGER_H

#include "lib/task/ob_timer.h"
#include "share/table/ob_ttl_util.h"
#include "ob_table_ttl_common.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_i_partition_group.h"

namespace oceanbase
{
namespace observer
{

struct ObTTLTaskCtx
{
public :
  ObTTLTaskCtx() : task_info_(),
                   task_status_(common::ObTTLTaskStatus::OB_TTL_TASK_INVALID),
                   ttl_para_(),
                   task_start_time_(OB_INVALID_ID),
                   last_modify_time_(OB_INVALID_ID),
                   task_end_time_(OB_INVALID_ID),
                   failure_times_(0),
                   rsp_time_(OB_INVALID_ID),
                   is_dirty_(false),
                   is_moved_(false),
                   need_refresh_(true) {}
  bool is_valid()
  {
    return task_info_.is_valid() && ttl_para_.is_valid();
  }

  TO_STRING_KV(K_(task_info), K_(task_status), K_(ttl_para), K_(task_start_time),
               K_(last_modify_time), K_(task_end_time), K_(failure_times),
               K_(rsp_time), K_(is_dirty), K_(is_moved), K_(need_refresh));

public:
  ObTTLTaskInfo           task_info_;
  common::ObTTLTaskStatus task_status_;
  ObTTLPara               ttl_para_;
  int64_t                 task_start_time_;
  int64_t                 last_modify_time_;
  int64_t                 task_end_time_;
  int64_t                 failure_times_;

  int64_t                 rsp_time_;
  bool                    is_invalid_;
  bool                    is_dirty_; // should sync sys table for tasks
  bool                    is_moved_;
  bool                    need_refresh_; // should refresh task from task table
};

class OBTTLTimerPeriodicTask : public common::ObTimerTask {
public: 
  OBTTLTimerPeriodicTask() {}
  virtual ~OBTTLTimerPeriodicTask() {}
  virtual void runTimerTask() override;
};

class ObTTLManager
{
public: 
  friend ObTTLTaskCtx;
  int init();
  int start();
  static ObTTLManager &get_instance();

  /*interface*/
  int proc_rs_cmd(uint64_t tenant_id, uint64_t task_id, 
                  bool is_usr_trigger, obrpc::ObTTLRequestArg::TTLRequestType cmd);
  int report_task_status(ObTTLTaskInfo& task_info, 
                         ObTTLPara& task_para, bool& is_stop);
  void on_leader_active(const ObPartitionKey& pkey);
  void on_schema_changed(uint64_t schema_changed_tenant_id);
  
  /*timer handle function*/
  virtual int generate_tenant_tasks(uint64_t tenant_id);
  int scan_all_tenanat_handle_event(); 
  int check_all_tenant_mem();
private:
  typedef common::hash::ObHashMap<ObPartitionKey, ObTTLTaskCtx*> PartTasksMap;
  typedef PartTasksMap::iterator ttl_parts_iterator;

  struct ObTTLTenantInfo
  {
  public:
    ObTTLTenantInfo() : part_task_map_(),
                        allocator_(),
                        tenant_id_(OB_INVALID_ID),
                        task_id_(OB_INVALID_ID),
                        is_usr_trigger_(false),
                        need_check_(false),
                        is_dirty_(false),
                        ttl_continue_(true),
                        cmd_type_(obrpc::ObTTLRequestArg::TTL_INVALID_TYPE),
                        rsp_time_(OB_INVALID_ID),
                        state_(common::ObTTLTaskStatus::OB_TTL_TASK_INVALID),
                        is_droped_(false),
                        is_finished_(false)
    {}
    void destory()
    {
      part_task_map_.destroy();
      allocator_.reset();
    }
    TO_STRING_KV(K_(tenant_id),
                 K_(task_id),
                 K_(is_usr_trigger),
                 K_(need_check),
                 K_(is_dirty),
                 K_(ttl_continue),
                 K_(cmd_type),
                 K_(rsp_time),
                 K_(state),
                 K_(is_droped),
                 K_(is_finished));

  public:
      PartTasksMap                      part_task_map_;
      common::ObArenaAllocator          allocator_;                
      uint64_t                          tenant_id_;
      uint64_t                          task_id_;
      bool                              is_usr_trigger_;
      bool                              need_check_; /*need scan partition & check*/
      bool                              is_dirty_;  /*need check the current ctx task*/
      bool                              ttl_continue_;
      obrpc::ObTTLRequestArg::TTLRequestType         cmd_type_;
      int64_t                           rsp_time_; // OB_INVALID_ID means no need response
      common::ObTTLTaskStatus           state_;
      bool                              is_droped_;   // tenant is droped
      bool                              is_finished_; // all delete task is finished (or canceled)
  };

  typedef common::hash::ObHashMap<int64_t, ObTTLTenantInfo*> TenantPartsMap;
  typedef TenantPartsMap::iterator ttl_tenants_iterator;

  /*get single tenant or partition info*/
  ObTTLTenantInfo* get_tenant_info(uint64_t tenant_id, bool create_if_not_exist);
  ObTTLTaskCtx* get_one_partition_ctx(const ObPartitionKey& pkey);

  /*inner function*/
  int deep_copy_task(ObTTLTaskCtx* ctx, ObTTLTaskInfo& task_info);
  int inner_handle_single_tenant_event(uint64_t tenant_id, common::ObSArray<ObPartitionKey>& parts_array);
  int inner_handle_one_partition_event(ObTTLTenantInfo* tenant_info, ObTTLTaskCtx* ctx);
  void cancel_all_tenant_task();  //for stop
  int generate_one_partition_task(ObTTLTaskInfo& task_info, ObTTLPara& para);
  int get_ttl_para_from_schema(const share::schema::ObTableSchema *table_schema, 
                           ObTTLPara& para, bool& is_tableapi_schema);
  int check_partition_can_gen_ttl(const ObPartitionKey& pkey,
                                  ObTTLPara &para, bool& can_ttl);
  int check_and_do_rsp(uint64_t tenant_id);
  void mark_tenant_need_check(uint64_t tenant_id);
  void mark_tenant_rsp(uint64_t tenant_id, int64_t rsp_time);
  virtual int generate_ttl_dag(ObTTLTaskInfo& task_info, ObTTLPara& para);
  int response_ttl_cmd(const uint64_t& tenant_id, const uint64_t& task_id,
                       obrpc::ObTTLRequestArg::TTLRequestType cmd_type);
  // convert ttl task ctx to ttl task status record
  static int construct_task_record_filter(const uint64_t& task_id,
                                          const uint64_t& table_id,
                                          const uint64_t& partition_id,
                                          ObTTLStatusFieldArray& filter);
  // construct the where condition
  static int construct_task_record_filter(const common::ObTTLStatus& ttl_record, ObTTLStatusFieldArray& filter);
  // move record from __all_tenant_ttl_task_status to __all_ttl_task_status_history
  int move_record_to_history_table(uint64_t tenant_id);
  common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  int sync_sys_table(ObPartitionKey& pkey);
  int construct_sys_table_record(ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record);
  int try_schedule_task(ObTTLTenantInfo* tenant_info, ObTTLTaskCtx* ctx);
  int try_schedule_remaining_tasks(ObTTLTenantInfo* tenant_info, const ObTTLTaskCtx *current_ctx);
  bool can_schedule_tenant(const ObTTLTenantInfo &tenant_info);
  bool can_schedule_task(const ObTTLTaskCtx &ttl_task);
  int check_cmd_state_valid(const common::ObTTLTaskStatus current_state,
                            const common::ObTTLTaskStatus incoming_state);
  int copy_all_tenant_ctxs(common::ObSArray<ObTTLTaskCtx *>& ctx_array, uint64_t tenant_id);
  int from_ttl_record(ObPartitionKey& pkey, common::ObTTLStatus& record, bool with_status = true, bool with_err_code = true);
  void mark_ttl_ctx_dirty(ObTTLTenantInfo* tenant_info, ObTTLTaskCtx* ctx);
  void check_ttl_tenant_state(uint64_t tenant_id);
  int transform_cmd_to_state(const obrpc::ObTTLRequestArg::TTLRequestType& cmd, common::ObTTLTaskStatus& state);
  int try_schedule_prepare_task(ObPartitionKey& pkey);
  void mark_tenant_checked(uint64_t tenant_id);
  int mark_tenant_droped(const uint64_t& tenant_id);
  int check_and_reset_droped_tenant();
  obrpc::ObTTLRequestArg::TTLRequestType transform_state_to_cmd(const int64_t state);
  int refresh_partition_task(ObTTLTaskCtx &ttl_task, bool refresh_status, bool refresh_retcode = false);
  bool is_ttl_comment(const ObString &str);

private:
  static const int64_t DEFAULT_TTL_BUCKET_NUM = 100;
  static const int64_t TTL_PERIODIC_DELAY = 5*1000*1000; //5s

  TenantPartsMap ttl_tenant_parts_map_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator allocator_;
  bool is_init_;
  bool is_timer_start_;
  int64_t periodic_delay_;
  common::ObTimer ttl_timer_;
  OBTTLTimerPeriodicTask periodic_task_;
  common::ObSpinLock lock_;
  bool is_first_cmd_; // recovery tenant info after restart  
  
private:
  void stop();
  void destroy();
  ObTTLManager() : schema_service_(NULL),
                   allocator_(),
                   is_init_(false),
                   is_timer_start_(false),
                   periodic_delay_(TTL_PERIODIC_DELAY),
                   ttl_timer_(),
                   periodic_task_(),
                   lock_(),
                   is_first_cmd_(true)
  {}
  ~ObTTLManager() {}
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_TTL_MANAGER_H */
