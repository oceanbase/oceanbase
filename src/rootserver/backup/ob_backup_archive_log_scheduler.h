// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_LOG_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_LOG_SCHEDULER_H_
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_server_manager.h"
#include "lib/queue/ob_link.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash_func/murmur_hash.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_backup_archive_log_round_stat.h"

namespace oceanbase {
namespace rootserver {

class ObBackupArchiveLogIdling : public ObThreadIdling {
public:
  explicit ObBackupArchiveLogIdling(volatile bool& stop) : ObThreadIdling(stop), idle_time_us_(0)
  {}
  virtual int64_t get_idle_interval_us() override;
  int64_t get_local_idle_time() const
  {
    return idle_time_us_;
  }
  void set_idle_interval_us(int64_t idle_time);

private:
  int64_t idle_time_us_;
};

class ObBackupArchiveLogScheduler : public ObRsReentrantThread {
  static const int64_t MAX_BUCKET_NUM = 1024;
  static const int64_t MAX_BATCH_SIZE = 1024;
  static const int64_t SERVER_LEASE_TIME = 2 * 60 * 1000 * 1000;  // 2min
  struct ServerStatus {
    ServerStatus() : status_(MAX), lease_time_(0)
    {}
    enum Status { STATUS_FREE, STATUS_BUSY, MAX };

    Status status_;
    int64_t lease_time_;
  };

  struct SimplePGWrapper : public common::ObLink {
    SimplePGWrapper() : archive_round_(-1), piece_id_(-1), create_date_(-1), pg_key_()
    {}
    ~SimplePGWrapper() = default;
    TO_STRING_KV(K_(pg_key), K_(piece_id), K_(create_date), K_(archive_round));
    int64_t archive_round_;
    int64_t piece_id_;
    int64_t create_date_;
    common::ObPGKey pg_key_;
  };

  struct ServerTraceStat {
    ServerTraceStat() : server_(), trace_id_()
    {}
    ~ServerTraceStat()
    {}

    bool is_valid() const
    {
      return server_.is_valid() && !trace_id_.is_invalid();
    }

    bool operator==(const ServerTraceStat& rhs) const
    {
      return server_ == rhs.server_ && trace_id_.equals(rhs.trace_id_);
    }

    inline uint64_t hash() const
    {
      uint64_t hash_ret = 0;
      hash_ret = common::murmurhash(&server_, sizeof(server_), 0);
      hash_ret = common::murmurhash(&trace_id_, sizeof(trace_id_), hash_ret);
      return hash_ret;
    }

    TO_STRING_KV(K_(server), K_(trace_id));
    common::ObAddr server_;     // which server has this task assigned to
    share::ObTaskId trace_id_;  // the trace id of the pg task
  };

  struct CurrentTurnPGStat {
    CurrentTurnPGStat() : pg_key_(), server_(), trace_id_(), finished_(false)
    {}
    ~CurrentTurnPGStat()
    {}

    bool operator==(const CurrentTurnPGStat& rhs) const
    {
      return pg_key_ == rhs.pg_key_ && server_ == rhs.server_ && trace_id_.equals(rhs.trace_id_);
    }

    inline uint64_t hash() const
    {
      uint64_t hash_ret = 0;
      hash_ret = common::murmurhash(&pg_key_, sizeof(pg_key_), 0);
      hash_ret = common::murmurhash(&server_, sizeof(server_), hash_ret);
      hash_ret = common::murmurhash(&trace_id_, sizeof(trace_id_), hash_ret);
      return hash_ret;
    }

    TO_STRING_KV(K_(pg_key), K_(server), K_(trace_id), K_(finished));
    common::ObPGKey pg_key_;
    common::ObAddr server_;     // which server has this task assigned to
    share::ObTaskId trace_id_;  // the trace id of the pg task
    bool finished_;             // current turn finished
  };

  struct TenantTaskQueue {
    TenantTaskQueue()
        : tenant_id_(OB_INVALID_ID),
          checkpoint_ts_(0),
          check_need_reschedule_ts_(0),
          total_pg_count_(0),
          finish_pg_count_(0),
          in_history_(false),
          piece_id_(0),
          job_id_(0),
          pushed_before_(false),
          need_fetch_new_(true),
          pg_map_()
    {}
    ~TenantTaskQueue()
    {}
    TO_STRING_KV(K_(tenant_id), K_(checkpoint_ts), K_(check_need_reschedule_ts), K_(total_pg_count),
        K_(finish_pg_count), K_(in_history), K_(piece_id), K_(job_id), K_(pushed_before));
    uint64_t tenant_id_;
    int64_t checkpoint_ts_;
    int64_t check_need_reschedule_ts_;
    int64_t total_pg_count_;
    int64_t finish_pg_count_;
    bool in_history_;
    int64_t piece_id_;
    int64_t job_id_;
    bool pushed_before_;  // default false
    bool need_fetch_new_;
    common::ObSpLinkQueue queue_;
    common::hash::ObHashMap<common::ObPGKey, CurrentTurnPGStat> pg_map_;
  };

public:
  ObBackupArchiveLogScheduler();
  virtual ~ObBackupArchiveLogScheduler();
  int init(ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy, common::ObMySQLProxy& sql_proxy,
      share::ObIBackupLeaseService& backup_lease_service, share::schema::ObMultiVersionSchemaService& schema_service);
  int start();
  void stop();
  int idle();
  void wakeup();
  void run3() override;
  int blocking_run() override
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int set_server_busy(const common::ObAddr& addr);
  int set_server_free(const common::ObAddr& addr);
  int set_pg_finish(const int64_t archive_round, const int64_t checkpoint_ts, const uint64_t tenant_id,
      const common::ObIArray<common::ObPGKey>& pg_list);
  int check_is_history_round(const int64_t round, bool& is_his_round);
  int get_log_archive_round_checkpoint_ts(
      const uint64_t tenant_id, const int64_t archive_round, int64_t& checkpoint_ts);
  int set_round_interrupted(const int64_t round);
  int mark_piece_pg_task_finished(const uint64_t tenant_id, const int64_t piece_id, const int64_t job_id,
      const common::ObIArray<common::ObPGKey>& pg_list);
  int redo_failed_pg_tasks(const uint64_t tenant_id, const int64_t piece_id, const int64_t job_id,
      const common::ObIArray<common::ObPGKey>& pg_list);
  int handle_enable_auto_backup(const bool is_enable);

private:
  int check_archive_beginning(const uint64_t tenant_id, bool& is_beginning);
  int do_if_pg_task_finished(
      const int64_t archive_round, const uint64_t tenant_id, const share::ObBackupDest& backup_dest);
  int check_current_turn_in_history(const uint64_t tenant_id, bool& in_history);
  int check_is_switch_piece(bool& is_switch_piece);
  int check_is_round_mode(const uint64_t tenant_id, const int64_t round_id, bool& is_round_mode);
  int do_backup_archivelog();
  int update_sys_tenant_checkpoint_ts();
  int check_cur_tenant_task_is_finished(const int64_t round, const uint64_t tenant_id, bool& finished);
  void clear_memory_resource_if_stopped();
  int get_copy_id(int64_t& copy_id);
  bool is_paused() const
  {
    return is_paused_;
  }
  int set_paused(const bool pause);
  int update_inner_table_log_archive_status(const share::ObLogArchiveStatus::STATUS& status);
  int set_checkpoint_ts(const uint64_t tenant_id, const int64_t checkpoint_ts);
  int get_checkpoint_ts(const uint64_t tenant_id, int64_t& checkpoint_ts);
  int set_checkpoint_ts_with_lock(const uint64_t tenant_id, const int64_t checkpoint_ts);
  int get_checkpoint_ts_with_lock(const uint64_t tenant_id, int64_t& checkpoint_ts);
  int do_schedule(const int64_t round, const share::ObBackupDest& src, const share::ObBackupDest& dst);
  int do_if_round_finished(const int64_t round, const share::ObBackupDest& dst);
  int ensure_all_tenant_finish(const int64_t round, const share::ObBackupDest& src, const share::ObBackupDest& dst,
      const common::ObIArray<uint64_t>& tenant_ids);
  int check_can_do_work();
  int check_is_enable_auto_backup(bool& is_enable);
  int get_status_line_count(const uint64_t tenant_id, int64_t& status_count);
  int check_backup_info(const uint64_t tenant_id, const share::ObBackupDest& backup_dest);
  int check_backup_infos(const common::ObArray<uint64_t>& tenant_list, const share::ObBackupDest& backup_dest);
  int insert_backup_archive_info(const uint64_t tenant_id, const share::ObBackupDest& backup_dest);
  int get_backup_dest_info(
      const uint64_t tenant_id, const int64_t round, share::ObBackupDest& src, share::ObBackupDest& dst);
  int update_extern_backup_info(const share::ObBackupDest& src, const share::ObBackupDest& dst);
  int update_tenant_info(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst);
  int update_tenant_name_info(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst);
  int update_clog_info_list(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst);
  int update_extern_tenant_clog_backup_info(const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id,
      const int64_t copy_id, const int64_t checkpoint_ts, const share::ObBackupDest& dst,
      const share::ObLogArchiveStatus::STATUS& status);
  int update_tenant_inner_table_cur_info(const uint64_t tenant_id, const int64_t archive_round,
      const int64_t checkpoint_ts, const int64_t copy_id, const int64_t backup_piece_id,
      const share::ObLogArchiveStatus::STATUS& status, const share::ObBackupDest& dst, common::ObISQLClient& proxy);
  int update_tenant_inner_table_his_info(const uint64_t tenant_id, const int64_t round_id, const int64_t copy_id,
      const share::ObBackupDest& dst, common::ObISQLClient& sql_proxy);
  int get_all_tenant_ids(const int64_t archive_round, common::ObIArray<uint64_t>& tenant_ids);
  int gc_unused_tenant_resources(const int64_t round_id);
  int check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped);
  int rebalance_all_tenants(const int64_t shift_num, common::ObArray<uint64_t>& tenant_ids);
  int get_src_backup_dest(const uint64_t tenant_id, const int64_t round, share::ObBackupDest& backup_dest);
  int get_current_log_archive_round_info(const uint64_t tenant_id, share::ObLogArchiveBackupInfo& info);
  int get_log_archive_history_info(const uint64_t tenant_id, const int64_t round, share::ObLogArchiveBackupInfo& info);
  int prepare_tenant_if_needed(const common::ObIArray<uint64_t>& tenant_ids, const bool need_fetch_new);
  int prepare_server_if_needed(const common::ObIArray<common::ObAddr>& servers);
  int get_next_round(const common::ObArray<int64_t>& round_list, int64_t& round);
  int check_round_finished(const int64_t round, const common::ObArray<uint64_t>& tenant_list);
  int check_tenant_task_stopped(const uint64_t tenant_id, bool& is_stopped);
  int update_inner_and_extern_tenant_info_to_stop(
      const share::ObBackupDest& backup_dest, const uint64_t tenant_id, const int64_t round);
  int update_inner_and_extern_tenant_info(const share::ObBackupDest& backup_dest, const uint64_t tenant_id,
      const int64_t round_id, const bool is_interrupted, common::ObISQLClient& sql_proxy);
  int update_inner_and_extern_tenant_infos_if_fast_forwarded(const int64_t first_round, const int64_t last_round);
  int do_tenant_schedule(const uint64_t tenant_id, const int64_t round, const int64_t piece_id,
      const int64_t create_date, const int64_t job_id, const share::ObBackupDest& src, const share::ObBackupDest& dst);
  int get_log_archive_info_list(const uint64_t tenant_id, common::ObIArray<share::ObLogArchiveBackupInfo>& round_list);
  int get_bb_sys_his_log_archive_info_list(common::ObArray<int64_t>& round_list);
  int get_delta_round_list(const common::ObArray<int64_t>& b_round_list, const common::ObArray<int64_t>& bb_round_list,
      common::ObArray<int64_t>& round_list);
  int get_log_archive_round_info(const uint64_t tenant_id, const int64_t round_id, share::ObLogArchiveBackupInfo& info);
  int get_log_archive_round_list(const uint64_t tenant_id, common::ObArray<share::ObLogArchiveBackupInfo>& info_list,
      common::ObArray<int64_t>& round_list);
  int check_archivelog_started(const common::ObIArray<share::ObLogArchiveBackupInfo>& info_list, bool& started);
  int get_batch_pg_list(const share::ObClusterBackupDest& backup_dest, const int64_t log_archive_round,
      const uint64_t tenant_id, common::ObArray<SimplePGWrapper*>& pg_list);
  int rollback_pg_info_if_failed(const int64_t round, TenantTaskQueue* queue_ptr);
  int check_current_tenant_task_turn_finished(TenantTaskQueue* queue, bool& finished);
  int push_pg_list(common::ObSpLinkQueue& queue, const int64_t round_id, const int64_t piece_id,
      const int64_t create_date, const common::ObIArray<common::ObPGKey>& pg_keys);
  int pop_pg_list(common::ObSpLinkQueue& queue, common::ObArray<SimplePGWrapper*>& node_list);
  int pop_all_pg_queue(TenantTaskQueue* queue_ptr);
  int alloc_tenant_queue(const uint64_t tenant_id, TenantTaskQueue*& queue);
  void free_tenant_queue(TenantTaskQueue*& queue_ptr);
  int alloc_pg_task(SimplePGWrapper*& pg, const int64_t round_id, const int64_t piece_id, const int64_t create_date,
      const common::ObPGKey& pg_key);
  void free_pg_task(SimplePGWrapper*& pg);
  void free_pg_task_list(common::ObArray<SimplePGWrapper*>& node_list);
  int get_pg_list_from_node_list(
      const common::ObArray<SimplePGWrapper*>& node_list, common::ObIArray<common::ObPGKey>& pg_list);
  int remove_duplicate_pg(common::ObIArray<common::ObPGKey>& pg_list);
  int check_self_dup_task(const common::ObIArray<common::ObPGKey>& pg_list);
  int get_clog_pg_list_v2(const share::ObClusterBackupDest& backup_dest, const int64_t round_id, const int64_t piece_id,
      const int64_t timestamp,  // create ts
      const uint64_t tenant_id, common::ObIArray<common::ObPGKey>& pg_keys);
  int generate_backup_archive_log_task(const uint64_t tenant_id, const int64_t archive_round,
      const int64_t piece_id,     // 备份拆分才会用
      const int64_t create_date,  // 备份拆分才会用
      const int64_t job_id,       // 备份拆分才会用
      const share::ObBackupDest& src, const share::ObBackupDest& dst, const common::ObAddr& server,
      common::ObArray<SimplePGWrapper*>& pg_keys);
  int do_backup_archive_log_rpc_failed(const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id,
      const int64_t create_date, const common::ObArray<SimplePGWrapper*>& node_list, TenantTaskQueue* queue_ptr);
  int build_backup_archive_log_arg(const uint64_t tenant_id, const int64_t archive_round, const int64_t piece_id,
      const int64_t create_date, const int64_t job_id, const share::ObBackupDest& src, const share::ObBackupDest& dst,
      const share::ObTaskId& trace_id, const common::ObArray<SimplePGWrapper*>& pg_keys,
      obrpc::ObBackupArchiveLogBatchArg& arg);
  int update_pg_stat(const common::ObAddr& server, const share::ObTaskId& trace_id,
      common::ObArray<SimplePGWrapper*>& pg_list, TenantTaskQueue* queue_ptr);
  int select_one_server(common::ObAddr& addr);
  int get_dst_backup_dest(const uint64_t tenant_id, char* backup_backup_dest, const int64_t len);
  int get_round_start_ts(const uint64_t tenant_id, const int64_t round, int64_t& start_ts);
  int check_response_checkpoint_ts_valid(const uint64_t tenant_id, const int64_t checkpoint_ts, bool& is_valid);
  int inner_get_next_round_with_lock(const common::ObArray<int64_t>& round_list, int64_t& next_round);
  int get_tenant_queue_ptr(const uint64_t tenant_id, TenantTaskQueue*& queue_ptr);
  int cancel_all_outgoing_tasks();
  int get_all_server_trace_list(common::ObIArray<ServerTraceStat>& stat_list);
  int get_tenant_server_trace_list(const TenantTaskQueue* queue_ptr, common::ObIArray<ServerTraceStat>& stat_list);
  int maybe_reschedule_timeout_pg_task(const uint64_t tenant_id);
  int check_need_reschedule(TenantTaskQueue* queue, bool& need_reschedule);
  int get_unfinished_pg_list_from_tenant_queue(
      const TenantTaskQueue* queue, common::ObIArray<common::ObPGKey>& pg_list);
  int get_lost_pg_list(const TenantTaskQueue* queue, const common::ObIArray<common::ObPGKey>& unfinished_list,
      common::ObIArray<common::ObPGKey>& lost_list);
  int check_task_in_progress(const common::ObAddr& server, const share::ObTaskId& trace_id, bool& in_progress);
  int get_stat_set_from_pg_list(const TenantTaskQueue* queue, const common::ObIArray<common::ObPGKey>& unfinished_list,
      common::hash::ObHashSet<ServerTraceStat>& trace_set);
  int rethrow_pg_back_to_queue(const common::ObIArray<common::ObPGKey>& lost_list, TenantTaskQueue*& queue);
  int mark_current_turn_pg_list(
      const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list, const bool finished);
  int mark_current_turn_in_history(const uint64_t tenant_id, const bool in_history);
  int check_current_turn_pg_finished(const uint64_t tenant_id, bool& finished);
  int get_same_trace_id_pg_list(const TenantTaskQueue* queue, const common::ObAddr& server,
      const share::ObTaskId& trace_id, common::ObIArray<common::ObPGKey>& pg_list);
  int do_clear_tenant_resource_if_dropped(const uint64_t tenant_id);
  // change backup backup dest
  int may_do_backup_backup_dest_changed();
  int check_backup_backup_dest_changed(const uint64_t tenant_id, bool& changed);
  int do_backup_backup_dest_changed(const uint64_t tenant_id);
  int get_current_round_task_info(const uint64_t tenant_id, share::ObLogArchiveBackupInfo& info);
  int move_current_task_to_history(const share::ObLogArchiveBackupInfo& info);
  int update_new_backup_backup_dest(const uint64_t tenant_id, common::ObISQLClient& proxy);
  int reset_round_stat_if_dest_changed(const uint64_t tenant_id);
  int get_current_round_copy_id(
      const share::ObBackupDest& backup_dest, const uint64_t tenant_id, const int64_t round, int64_t& copy_id);
  int check_has_same_backup_dest(const common::ObIArray<share::ObLogArchiveBackupInfo>& info_list,
      const share::ObBackupDest& backup_dest, bool& has_same);

private:
  int check_backup_dest_same_with_gconf(const share::ObBackupDest& backup_dest, bool& is_same);
  int check_bb_dest_same_with_gconf(const share::ObBackupBackupPieceJobInfo& job_info, bool& is_same);
  int get_job_copy_id_level(
      const share::ObBackupBackupPieceJobInfo& job_info, share::ObBackupBackupCopyIdLevel& copy_id_level);
  int get_smallest_job_info(share::ObBackupBackupPieceJobInfo& job_info);
  int do_backup_backuppiece();  // for backup piece
  int get_backup_piece_tenant_list(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t piece_id,
      const int64_t copy_id, common::ObArray<uint64_t>& tenant_list);
  int may_do_backup_backup_dest_changed_in_piece_mode(
      const share::ObBackupBackupCopyIdLevel& copy_id_level, const share::ObBackupDest& backup_dest);
  int backup_piece_do_with_status(const share::ObBackupBackupPieceJobInfo& job_info);
  int update_backup_piece_job(const share::ObBackupBackupPieceJobInfo& job_info);
  int do_schedule(const share::ObBackupBackupPieceJobInfo& job_info);
  int inner_do_schedule_all(const share::ObBackupBackupPieceJobInfo& job_info);
  int check_origin_backup_in_current_gconf(
      const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, bool& in_current_gconf);
  int check_backup_piece_id_continuous(
      const int64_t src_min_piece_id, const int64_t dst_max_piece_id, bool& is_continuous);
  int get_job_min_backup_piece_id(const share::ObBackupBackupPieceJobInfo& job_info, int64_t& min_backup_piece_id);
  int get_job_max_backup_piece_id(const share::ObBackupBackupPieceJobInfo& job_info, int64_t& max_backup_piece_id);
  int inner_do_schedule_single(const share::ObBackupBackupPieceJobInfo& job_info);
  int check_single_piece_is_valid(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id,
      const int64_t piece_id, bool& is_valid);
  int check_backup_backup_has_been_backed_up_before(const share::ObBackupBackupPieceJobInfo& job_info,
      const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, bool& deleted_before);
  int inner_do_schedule_tenant_piece_task(const share::ObBackupBackupPieceJobInfo& job_info,
      const common::ObIArray<uint64_t>& tenant_list, const int64_t piece_id);
  int calc_backup_piece_copy_id(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id,
      const int64_t piece_id, int64_t& copy_id);
  int get_backup_piece_copy_id(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id,
      const int64_t piece_id, int64_t& copy_id);
  int build_backup_piece_task_info(const share::ObBackupBackupPieceJobInfo& job_info, const uint64_t tenant_id,
      const int64_t round_id, const int64_t piece_id, const int64_t copy_id,
      share::ObBackupBackupPieceTaskInfo& task_info);
  int build_backup_piece_info(const share::ObBackupPieceInfo& src_info,
      const share::ObBackupFileStatus::STATUS file_status, const share::ObBackupDest& backup_dest,
      const int64_t copy_id, share::ObBackupPieceInfo& dst_info);
  int do_copy(const share::ObBackupBackupPieceJobInfo& job_info);
  int prepare_tenant_piece_task_if_needed(
      const common::ObIArray<uint64_t>& tenant_ids, const int64_t piece_id, const int64_t job_id);
  int get_smallest_doing_backup_piece_task(
      const int64_t job_id, const uint64_t tenant_id, share::ObBackupBackupPieceTaskInfo& task_info);
  int insert_backup_backuppiece_task(const share::ObBackupBackupPieceTaskInfo& task_info);
  int update_backup_piece_info(const share::ObBackupPieceInfo& files_info, common::ObISQLClient& sql_proxy);
  int update_backup_piece_task_finish(
      const uint64_t tenant_id, const int64_t job_id, const int64_t piece_id, common::ObISQLClient& proxy);
  int do_extern_backup_piece_info(const share::ObBackupBackupPieceJobInfo& job_info,
      const share::ObBackupFileStatus::STATUS file_status, const uint64_t tenant_id, const int64_t round_id,
      const int64_t piece_id, const int64_t copy_id);
  int do_cancel(const share::ObBackupBackupPieceJobInfo& job_info);
  int do_finish(const share::ObBackupBackupPieceJobInfo& job_info);
  int do_transform_job_tasks_when_finish(
      const share::ObBackupBackupPieceJobInfo& job_info, common::ObArray<share::ObBackupBackupPieceTaskInfo>& tasks);
  int update_extern_info_when_finish(const share::ObBackupBackupPieceJobInfo& job_info);
  int update_backup_piece_info(const share::ObBackupBackupPieceJobInfo& job_info,
      const common::ObIArray<share::ObBackupBackupPieceTaskInfo>& task_list);
  int get_piece_batch_pg_list(const int64_t job_id, const int64_t round_id, const int64_t piece_id,
      const int64_t create_date, const uint64_t tenant_id, const share::ObClusterBackupDest& backup_dest,
      common::ObArray<SimplePGWrapper*>& node_list);
  int get_piece_info_list(
      const uint64_t tenant_id, const int64_t copy_id, common::ObIArray<share::ObBackupPieceInfo>& info_list);
  int get_piece_round_id(
      common::ObIArray<share::ObBackupPieceInfo>& info_list, const int64_t piece_id, int64_t& round_id);
  int get_backup_piece_info(const int64_t incarnation, const uint64_t tenant_id, const int64_t round_id,
      const int64_t backup_piece_id, const int64_t copy_id, common::ObISQLClient& proxy,
      share::ObBackupPieceInfo& info);
  int check_all_backup_piece_task_finished(const share::ObBackupBackupPieceJobInfo& job_info, bool& all_finished);
  int check_src_data_available(
      const share::ObBackupBackupPieceJobInfo& job, const int64_t round_id, const int64_t piece_id, bool& is_available);
  int update_all_backup_piece_task_finished(const share::ObBackupBackupPieceJobInfo& job_info, const int result);
  int check_cur_piece_tenant_tasks_finished(const int64_t piece_id, const share::ObBackupBackupPieceJobInfo& job_info,
      const common::ObIArray<uint64_t>& tenant_ids, bool& all_finished);
  int do_cur_piece_tenant_tasks_finished(const int64_t round_id, const int64_t piece_id,
      const share::ObBackupBackupPieceJobInfo& job_info, const common::ObIArray<uint64_t>& tenant_ids);
  int inner_do_cur_piece_tenant_task_finished(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t round_id,
      const int64_t piece_id, const uint64_t tenant_id);
  int check_piece_task_finished(const share::ObBackupBackupPieceJobInfo& job_info, const uint64_t tenant_id,
      const int64_t piece_id, bool& finished);
  int check_backup_piece_pg_task_finished(const uint64_t tenant_id, const int64_t piece_id, bool& finished);
  int get_piece_src_and_dst_backup_dest(
      const share::ObBackupBackupPieceTaskInfo& task_info, share::ObBackupDest& src, share::ObBackupDest& dst);
  int sync_backup_backup_piece_info(
      const uint64_t tenant_id, const share::ObBackupDest& src_dest, const share::ObBackupDest& dst_dest);
  int create_mount_file(const share::ObBackupDest& backup_dest, const int64_t round_id);
  int check_is_last_piece(const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, bool& last_piece);
  int update_round_piece_info_for_clean(const uint64_t tenant_id, const int64_t round_id, const bool round_finished,
      const share::ObBackupDest& backup_dest, common::ObISQLClient& proxy);
  bool is_fatal_error(const int64_t result);
  int on_fatal_error(const share::ObBackupBackupPieceJobInfo& job_info, const int64_t result);
  int check_normal_tenant_passed_sys_tenant(
      const uint64_t tenant_id, const int64_t local_sys_checkpoint_ts, bool& passed);

private:
  static const int64_t CHECK_NEED_RESCHEDULE_TIME_INTERVAL = 1000 * 1000 * 1000;

  bool is_inited_;
  bool is_working_;
  bool is_paused_;
  int64_t schedule_round_;
  int64_t local_sys_tenant_checkpoint_ts_;
  mutable ObBackupArchiveLogIdling idling_;
  ObServerManager* server_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* sql_proxy_;
  share::ObIBackupLeaseService* backup_lease_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::hash::ObHashMap<uint64_t, TenantTaskQueue*> tenant_queue_map_;
  common::hash::ObHashMap<common::ObAddr, ServerStatus> server_status_map_;

  lib::ObMutex mutex_;
  share::ObPGBackupBackupsetStatTree round_stat_;
  share::ObBackupInnerTableVersion inner_table_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchiveLogScheduler);
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif
