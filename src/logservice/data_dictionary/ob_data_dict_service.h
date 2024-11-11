/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Define DataDictionaryService
*/

#ifndef OCEANBASE_DATA_DICTIONARY_SERVICE_
#define OCEANBASE_DATA_DICTIONARY_SERVICE_

#include "lib/thread/thread_mgr_interface.h" //TGTaskHandler

#include "ob_data_dict_sql_client.h"
#include "ob_data_dict_storager.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
  class ObSchemaGetterGuard;
  class ObMultiVersionSchemaService;
  class ObTenantSchema;
}
}
namespace storage
{
  class ObLSService;
}

namespace datadict
{
class ObDataDictService
  : public common::ObTimerTask,
    public logservice::ObIReplaySubHandler,
    public logservice::ObIRoleChangeSubHandler,
    public logservice::ObICheckpointSubHandler
{
public:
  ObDataDictService();
  ~ObDataDictService() { destroy(); }
public:
  // for MTL
  static int mtl_init(ObDataDictService *&datadict_service);
public:
  int init(
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService *schema_service,
      storage::ObLSService *ls_service);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  virtual void runTimerTask(); // for ObTimerTask
  virtual int replay(
      const void *buffer,
      const int64_t nbytes,
      const palf::LSN &lsn,
      const share::SCN &scn) override final { return OB_SUCCESS; } // for ReplaySubHandler
  virtual int flush(share::SCN &rec_scn) override final { return OB_SUCCESS; } // for CheckpointSubHandler
  virtual void switch_to_follower_forcedly() override final; // for RoleChangeSubHandler
  virtual int switch_to_leader() override final;
  virtual int switch_to_follower_gracefully() override final;
  virtual int resume_leader() override final;
  virtual share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
public:
  // is sys_ls leader or not;
  OB_INLINE bool is_leader() const { return ATOMIC_LOAD(&is_leader_); }
  OB_INLINE int64_t get_last_dump_succ_time() const { return last_dump_succ_time_; }
  // mark need force dump data_dict, will dump data_dict regardless dump_interval.
  void mark_force_dump_data_dict(const bool need_dump = true);
  void update_expected_dump_scn(const share::SCN &expected_dump_scn);
  // update retention to data_dict dump history
  // ignore if data_dict_dump_history_retention_sec < 0
  void update_data_dict_dump_history_retention(const int64_t data_dict_dump_history_retention_sec);
private:
  void refresh_config_();
  void switch_role_to_(bool is_leader);
  int do_dump_data_dict_();
  int check_cluster_status_normal_(bool &is_normal);
  int get_snapshot_scn_(share::SCN &snapshot_scn);
  int generate_dict_and_dump_(const share::SCN &snapshot_scn);
  int get_tenant_schema_guard_(
      const int64_t schema_version,
      share::schema::ObSchemaGetterGuard &tenant_guard,
      const bool is_force_fallback);
  int check_tenant_status_normal_(
      share::schema::ObSchemaGetterGuard &tenant_schema_guard,
      bool &is_normal);
  int handle_tenant_meta_(
      const share::SCN &snapshot_scn,
      const int64_t schema_version,
      ObIArray<uint64_t> &database_ids,
      ObIArray<uint64_t> &table_ids);
  int get_database_ids_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObIArray<uint64_t> &database_ids);
  int handle_database_metas_(
      const int64_t schema_version,
      const ObIArray<uint64_t> &database_ids);
  int handle_table_metas_(
      const int64_t schema_version,
      const ObIArray<uint64_t> &table_ids,
      int64_t &filter_table_count);
  int filter_table_(const share::schema::ObTableSchema &table_schema, bool &is_filtered);
  void try_recycle_dict_history_();
private:
  static const int64_t TIMER_TASK_INTERVAL;
  static const int64_t PRINT_DETAIL_INTERVAL;
  static const int64_t SCHEMA_OP_TIMEOUT;
  static const int64_t DEFAULT_REPORT_TIMEOUT;
private:
  bool is_inited_;
  bool is_leader_;
  volatile bool stop_flag_;
  uint64_t tenant_id_;
  ObArenaAllocator allocator_;
  ObDataDictSqlClient sql_client_;
  ObDataDictStorage storage_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  storage::ObLSService *ls_service_;
  int64_t dump_interval_;
  int timer_tg_id_;
  int64_t last_dump_succ_time_;
  share::SCN expected_dump_snapshot_scn_;
  share::SCN last_dump_succ_snapshot_scn_;
  // if data_dict_dump_history_retention_sec <= 0: won't recycle history;
  // if data_dict_dump_history_retention_sec > 0: try recycle after dump succ;
  int64_t data_dict_dump_history_retention_sec_;
  bool force_need_dump_;
};

} // namespace datadict
} // namespace oceanbase

#endif
