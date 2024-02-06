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

#ifndef OCEANBASE_LOGSERVICE_LOG_ENGINE_
#define OCEANBASE_LOGSERVICE_LOG_ENGINE_

#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"                // TO_STRING_KV
#include "common/ob_member_list.h"                     // ObMemberList
#include "log_storage.h"                               // LogStorage
#include "log_net_service.h"                           // LogNetService
#include "log_meta.h"                                  // LogMeta
#include "log_define.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
class ObILogAllocator;
} // namespace common
namespace palf
{
class LogRpc;
class LogGroupEntry;
class LSN;
class LogIOWorker;
class PalfHandleImpl;
class LogIOTask;
class LogIOFlushLogTask;
class LogIOTruncateLogTask;
class LogIOFlushMetaTask;
class LogIOTruncatePrefixBlocksTask;
class FlushLogCbCtx;
class TruncateLogCbCtx;
class FlushMetaCbCtx;
class TruncatePrefixBlocksCbCtx;
class LogWriteBuf;
class LogGroupEntryHeader;
class TruncatePrefixBlocksCbCtx;
class LogIOTruncatePrefixBlocksTask;
class LogIOFlashbackTask;
class FlashbackCbCtx;
class LogIOPurgeThrottlingTask;
class PurgeThrottlingCbCtx;

#define OVERLOAD_SUBMIT_CHANGE_CONFIG_META_REQ(type)                                \
  virtual int submit_change_config_meta_req(const type &member_list,                \
                                            const int64_t &msg_proposal_id,         \
                                            const int64_t &prev_log_proposal_id,    \
                                            const LSN &prev_lsn,                    \
                                            const int64_t &prev_mode_pid,           \
                                            const LogConfigMeta &config_meta)       \
  {                                                                                 \
    return submit_change_config_meta_req_(                                          \
        member_list, msg_proposal_id, prev_log_proposal_id, prev_lsn,               \
        prev_mode_pid, config_meta);                                                \
  }

#define OVERLOAD_SUBMIT_PREPARE_META_REQ(type)                                \
  virtual int submit_prepare_meta_req(const type &member_list,                \
                                      const int64_t &log_proposal_id)         \
  {                                                                           \
    return submit_prepare_meta_req_(member_list, log_proposal_id);            \
  }

#define OVERLOAD_SUBMIT_CHANGE_MODE_META_REQ(type)                                  \
  virtual int submit_change_mode_meta_req(const type &member_list,                  \
                                          const int64_t &msg_proposal_id,           \
                                          const bool is_applied_mode_meta,          \
                                          const LogModeMeta &mode_meta)             \
  {                                                                                 \
    return submit_change_mode_meta_req_(member_list, msg_proposal_id,               \
        is_applied_mode_meta, mode_meta);                                           \
  }

class LogEngine
{
  friend class PalfHandleImpl; // need get net_service to init election
public:
  LogEngine();
  virtual ~LogEngine();

public:
  // int64_t is as the unique id for LogNetService
  //
  // NB: Except LogNetService, other components should just
  // only use int64_t to debug
  int init(const int64_t palf_id,
           const char *base_dir,
           const LogMeta &log_meta,
           common::ObILogAllocator *alloc_mgr,
           ILogBlockPool *log_block_pool,
           LogHotCache *hot_cache,
           LogRpc *log_rpc,
           LogIOWorker *log_io_worker,
           LogPlugins *plugins,
           const int64_t palf_epoch,
           const int64_t log_storage_block_size,
           const int64_t log_meta_storage_block_size);
  void destroy();

  int load(const int64_t palf_id,
           const char *base_dir,
           common::ObILogAllocator *alloc_mgr,
           ILogBlockPool *log_block_pool,
           LogHotCache *hot_cache,
           LogRpc *log_rpc,
           LogIOWorker *log_io_worker,
           LogPlugins *plugins,
           LogGroupEntryHeader &entry_header,
           const int64_t palf_epoch,
           bool &is_integrity,
           const int64_t log_storage_size,
           const int64_t log_meta_storage_size);

  // ==================== Submit async task start ================
  //
  int submit_flush_log_task(const FlushLogCbCtx &flush_log_cb_ctx,
                            const char *buf,
                            const int64_t buf_len);

  virtual int submit_flush_log_task(const FlushLogCbCtx &flush_log_cb_ctx, const LogWriteBuf &write_buf);

  int submit_flush_prepare_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                     const LogPrepareMeta &prepare_meta);

  virtual int submit_flush_change_config_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                                   const LogConfigMeta &config_meta);

  virtual int submit_flush_mode_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                          const LogModeMeta &mode_meta);

  int submit_flush_snapshot_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                      const LogSnapshotMeta &log_snapshot_meta);

  virtual int submit_truncate_log_task(const TruncateLogCbCtx &truncate_log_cb_ctx);
  virtual int submit_flush_replica_property_meta_task(
      const FlushMetaCbCtx &flush_meta_cb_ctx,
      const LogReplicaPropertyMeta &log_replica_property_meta);

  int submit_truncate_prefix_blocks_task(
      const TruncatePrefixBlocksCbCtx &truncate_prefix_blocks_ctx);
  int submit_flashback_task(const FlashbackCbCtx &flashback_ctx);
  int submit_purge_throttling_task(const PurgeThrottlingType purge_type);

  // ==================== Submit aysnc task end ==================

  // ====================== LogStorage start =====================
  int append_log(const LSN &lsn, const LogWriteBuf &write_buf, const share::SCN &scn);
  int append_log(const LSNArray &lsn, const LogWriteBufArray &write_buf, const SCNArray &scn_array);
  int read_log(const LSN &lsn,
               const int64_t in_read_size,
               ReadBuf &read_buf,
               int64_t &out_read_size);
  int read_group_entry_header(const LSN &lsn, LogGroupEntryHeader &log_group_entry_header);
  int truncate(const LSN &lsn);
  int truncate_prefix_blocks(const LSN &lsn);
  int begin_flashback(const LSN &start_lsn_of_block);
  int end_flashback(const LSN &start_lsn_of_block);
  int delete_block(const block_id_t &block_id);

  const LSN get_begin_lsn() const;
  int get_block_id_range(block_id_t &min_block_id, block_id_t &max_block_id) const;
  int get_block_min_scn(const block_id_t &block_id, share::SCN &scn) const;
  //
  // ====================== LogStorage end =======================

  // ===================== MetaStorage start =====================
  //
  int update_base_lsn_used_for_gc(const LSN &lsn);
  int update_manifest(const block_id_t block_id);
  int append_meta(const char *buf, const int64_t buf_len);
  int update_log_snapshot_meta_for_flashback(const LogInfo &prev_log_inf);
  //
  // ===================== MetaStorage end =======================

  // ===================== NetService start ======================
  LogNetService& get_net_service();
  // @brief: this function used to transfer log to remote node
  // @param[in] member_list: remote member list
  // @param[in] msg_proposal_id: the current proposal_id
  // @param[in] prev_lsn: the offset of prev log
  // @param[in] prev_log_proposal_id: the proposal_id of prev log
  // @param[in] curr_lsn: the offset of curr log
  //
  // NB: this function just only send log to the remote member list
  template <class List>
  int submit_push_log_req(const List &member_list,
                          const PushLogType &push_log_type,
                          const int64_t &msg_proposal_id,
                          const int64_t &prev_log_proposal_id,
                          const LSN &prev_lsn,
                          const LSN &curr_lsn,
                          const LogWriteBuf &write_buf)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(ERROR, "LogEngine not init", K(ret), KPC(this));
    } else if (OB_FAIL(log_net_service_.submit_push_log_req(member_list,
                                                            push_log_type,
                                                            msg_proposal_id,
                                                            prev_log_proposal_id,
                                                            prev_lsn,
                                                            curr_lsn,
                                                            write_buf))) {
      // PALF_LOG(ERROR,
      //          "LogNetService submit_group_entry_to_memberlist failed",
      //          K(ret),
      //          KPC(this),
      //          K(member_list),
      //          K(prev_log_proposal_id),
      //          K(prev_lsn),
      //          K(prev_log_proposal_id),
      //          K(curr_lsn),
      //          K(write_buf));
    } else {
      PALF_LOG(TRACE,
               "submit_group_entry_to_memberlist success",
               K(ret),
               KPC(this),
               K(member_list),
               K(msg_proposal_id),
               K(prev_log_proposal_id),
               K(prev_lsn),
               K(curr_lsn));
    }
    return ret;
  }
  virtual int submit_push_log_req(const common::ObAddr &addr,
                                  const PushLogType &push_log_type,
                                  const int64_t &msg_proposal_id,
                                  const int64_t &prev_log_proposal_id,
                                  const LSN &prev_lsn,
                                  const LSN &curr_lsn,
                                  const LogWriteBuf &write_buf);

  // @brief: this function used to submit acknowledgement to specified server
  // @param[in] server: the address of remote server(leader)
  // @param[in] proposal_id: the proposal id of this log, used to filter stale log
  // @param[in] lsn: the offset of log
  virtual int submit_push_log_resp(const common::ObAddr &server,
                                   const int64_t &msg_proposal_id,
                                   const LSN &lsn);

  template <class List>
  int submit_prepare_meta_req_(const List &member_list, const int64_t &log_proposal_id)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      ret = log_net_service_.submit_prepare_meta_req(member_list, log_proposal_id);
    }
    return ret;
  }

  virtual int submit_prepare_meta_resp(const common::ObAddr &server,
                                       const int64_t &msg_proposal_id,
                                       const bool vote_granted,
                                       const int64_t &log_proposal_id,
                                       const LSN &max_flushed_lsn,
                                       const LSN &committed_end_lsn,
                                       const LogModeMeta &mode_meta);

  template <class List>
  int submit_change_config_meta_req_(const List &member_list,
                                     const int64_t &msg_proposal_id,
                                     const int64_t &prev_log_proposal_id,
                                     const LSN &prev_lsn,
                                     const int64_t &prev_mode_pid,
                                     const LogConfigMeta &config_meta)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      ret = log_net_service_.submit_change_config_meta_req(
          member_list, msg_proposal_id, prev_log_proposal_id, prev_lsn, prev_mode_pid, config_meta);
      PALF_LOG(INFO, "submit_change_config_meta_req_ success", K(ret), K(member_list), K(msg_proposal_id),
          K(prev_log_proposal_id), K(prev_lsn), K(prev_mode_pid), K(config_meta));
    }
    return ret;
  }

  template <class List>
  int submit_change_mode_meta_req_(
        const List &member_list,
        const int64_t &msg_proposal_id,
        const bool is_applied_mode_meta,
        const LogModeMeta &mode_meta)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      ret = log_net_service_.submit_change_mode_meta_req(member_list, msg_proposal_id,
          is_applied_mode_meta, mode_meta);
    }
    return ret;
  }

  OVERLOAD_SUBMIT_CHANGE_CONFIG_META_REQ(common::ObMemberList);
  OVERLOAD_SUBMIT_CHANGE_CONFIG_META_REQ(common::GlobalLearnerList);
  OVERLOAD_SUBMIT_CHANGE_CONFIG_META_REQ(common::ResendConfigLogList);
  OVERLOAD_SUBMIT_PREPARE_META_REQ(common::ObMemberList);
  OVERLOAD_SUBMIT_PREPARE_META_REQ(common::GlobalLearnerList);
  OVERLOAD_SUBMIT_CHANGE_MODE_META_REQ(common::ObMemberList);
  OVERLOAD_SUBMIT_CHANGE_MODE_META_REQ(common::ResendConfigLogList);

  virtual int submit_change_config_meta_resp(const common::ObAddr &server,
                                             const int64_t msg_proposal_id,
                                             const LogConfigVersion &config_version);

  virtual int submit_change_mode_meta_resp(const common::ObAddr &server,
                                           const int64_t &msg_proposal_id);

  virtual int submit_config_change_pre_check_req(const common::ObAddr &server,
                                                 const LogConfigVersion &config_version,
                                                 const bool need_purge_throttling,
                                                 const int64_t timeout_us,
                                                 LogGetMCStResp &resp);
#ifdef OB_BUILD_ARBITRATION
  virtual int sync_get_arb_member_info(const common::ObAddr &server,
                                       const int64_t timeout_us,
                                       LogGetArbMemberInfoResp &resp);
#endif

  // @brief: this function used to submit fetch log request to sepcified server
  // @param[in] server: the address of remote server(data source)
  // @param[in] fetch_type: follower fetching or leader reconfirm fetching
  // @param[in] prev_lsn: the lsn of prev log, used to forward check
  // @param[in] lsn: the lsn of log
  // @param[in] fetch_log_size: the totoal size
  // @param[in] fetch_log_count: the totoal log count by log_id
  // @param[in] accepted_mode_pid: proposal_id of accepted_mode_meta
  virtual int submit_fetch_log_req(const common::ObAddr &server,
                                   const FetchLogType fetch_type,
                                   const int64_t msg_proposal_id,
                                   const LSN &prev_lsn,
                                   const LSN &lsn,
                                   const int64_t fetch_log_size,
                                   const int64_t fetch_log_count,
                                   const int64_t accepted_mode_pid);
  virtual int submit_batch_fetch_log_resp(const common::ObAddr &server,
                                          const int64_t msg_proposal_id,
                                          const int64_t prev_log_proposal_id,
                                          const LSN &prev_lsn,
                                          const LSN &curr_lsn,
                                          const LogWriteBuf &write_buf);
  virtual int submit_register_parent_req(const common::ObAddr &server,
                                         const LogLearner &child_itself,
                                         const bool is_to_leader);
  virtual int submit_register_parent_resp(const common::ObAddr &server,
                                          const LogLearner &parent_itself,
                                          const LogCandidateList &candidate_list,
                                          const RegisterReturn reg_ret);
  virtual int submit_retire_parent_req(const common::ObAddr &server,
                                       const LogLearner &child_itself);
  virtual int submit_retire_child_req(const common::ObAddr &server,
                                      const LogLearner &parent_itself);
  virtual int submit_learner_keepalive_req(const common::ObAddr &server,
                                           const LogLearner &sender_itself);
  virtual int submit_learner_keepalive_resp(const common::ObAddr &server,
                                            const LogLearner &sender_itself);

  virtual int submit_notify_rebuild_req(const ObAddr &server,
                                        const LSN &base_lsn,
                                        const LogInfo &base_prev_log_info);
  int submit_notify_fetch_log_req(const ObMemberList &dst_list);
  int submit_committed_info_req(
      const ObAddr &server,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn);
  // @brief: this function used to send committed_info to child replica
  // @param[in] member_list: current paxos member list
  // @param[in] msg_proposal_id: the current proposal_id
  // @param[in] prev_log_id: the log_id of prev log
  // @param[in] prev_log_proposal_id: the proposal_id of prev log
  // @param[in] committed_end_lsn: the current committed_end_lsn of leader
  //
  template <class List>
  int submit_committed_info_req(
      const List &member_list,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(ERROR, "LogEngine not init", K(ret), KPC(this));
    } else if (OB_FAIL(log_net_service_.submit_committed_info_req(
          member_list, msg_proposal_id,
          prev_log_id, prev_log_proposal_id, committed_end_lsn))) {
      PALF_LOG(ERROR, "LogNetService submit_committed_info_req failed", K(ret),
          KPC(this), K(member_list),
          K(prev_log_id), K(prev_log_proposal_id), K(committed_end_lsn));
    } else {
      PALF_LOG(TRACE, "submit_committed_info_req success", K(ret), KPC(this),
          K(member_list), K(msg_proposal_id), K(prev_log_id),
          K(prev_log_proposal_id), K(committed_end_lsn));
    }
    return ret;
  }

  int submit_get_stat_req(const common::ObAddr &server,
                          const int64_t timeout_us,
                          const LogGetStatReq &req,
                          LogGetStatResp &resp);

  LogMeta get_log_meta() const;
  const LSN &get_base_lsn_used_for_block_gc() const;
  // not thread safe
  int get_min_block_info_for_gc(block_id_t &block_id, share::SCN &max_scn);
  int get_min_block_info(block_id_t &block_id, share::SCN &min_scn) const;
  //
  // ===================== NetService end ========================
  LogStorage *get_log_storage() { return &log_storage_; }
  LogStorage *get_log_meta_storage() { return &log_meta_storage_; }
  int get_total_used_disk_space(int64_t &total_used_size_byte,
                                int64_t &unrecyclable_disk_space) const;
  virtual int64_t get_palf_epoch() const { return palf_epoch_; }
  TO_STRING_KV(K_(palf_id), K_(is_inited), K_(min_block_max_scn), K_(min_block_id), K_(base_lsn_for_block_gc),
      K_(log_meta), K_(log_meta_storage), K_(log_storage), K_(palf_epoch), K_(last_purge_throttling_ts), KP(this));
private:
  int submit_flush_meta_task_(const FlushMetaCbCtx &flush_meta_cb_ctx, const LogMeta &log_meta);
  int append_log_meta_(const LogMeta &log_meta);
  int construct_log_meta_(const LSN &lsn, block_id_t &expected_next_block_id);
  // =========== Async callback task generate and destroy ==============
  int generate_flush_log_task_(const FlushLogCbCtx &flush_log_cb_ctx,
                               const LogWriteBuf &write_buf,
                               LogIOFlushLogTask *&flush_log_task);

  int generate_truncate_log_task_(const TruncateLogCbCtx &truncate_log_cb_ctx,
                                  LogIOTruncateLogTask *&truncate_log_task);
  int generate_truncate_prefix_blocks_task_(
      const TruncatePrefixBlocksCbCtx &truncate_prefix_blocks_ctx,
      LogIOTruncatePrefixBlocksTask *&truncate_prefix_blocks_task);

  int generate_flush_meta_task_(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                const LogMeta &log_meta,
                                LogIOFlushMetaTask *&flush_meta_task);
  int generate_flashback_task_(const FlashbackCbCtx &flashback_cb_ctx,
                               LogIOFlashbackTask *&flashback_task);
  int generate_purge_throttling_task_(const PurgeThrottlingCbCtx &purge_cb_ctx,
                                      LogIOPurgeThrottlingTask *&purge_task);
  int update_config_meta_guarded_by_lock_(const LogConfigMeta &meta, LogMeta &log_meta);
  int try_clear_up_holes_and_check_storage_integrity_(
      const LSN &last_entry_begin_lsn,
      const block_id_t &expected_next_block_id,
      LogGroupEntryHeader &last_group_entry_header);
  bool check_last_block_whether_is_integrity_(const block_id_t expected_next_block_id,
                                              const block_id_t max_block_id,
                                              const LSN &log_storage_tail);

  int serialize_log_meta_(const LogMeta &log_meta, char *buf, int64_t buf_len);

  void reset_min_block_info_guarded_by_lock_(const block_id_t min_block_id,
                                             const share::SCN &min_block_max_scn);

  int integrity_verify_(const LSN &last_meta_entry_start_lsn,
                        const LSN &last_group_entry_header_lsn,
                        bool &is_integrity);
private:
  DISALLOW_COPY_AND_ASSIGN(LogEngine);

  const int64_t PURGE_THROTTLING_INTERVAL = 100 * 1000;//100ml
private:
  // used for GC
  mutable ObSpinLock block_gc_lock_;
  share::SCN min_block_max_scn_;
  mutable block_id_t min_block_id_;
  LSN base_lsn_for_block_gc_;

  mutable ObSpinLock log_meta_lock_;
  LogMeta log_meta_;
  LogStorage log_meta_storage_;
  LogStorage log_storage_;
  LogNetService log_net_service_;
  common::ObILogAllocator *alloc_mgr_;
  LogIOWorker *log_io_worker_;
  LogPlugins *plugins_;
  // Except for LogNetService, this field is just only used for debug
  int64_t palf_id_;
  // palf_epoch_ is used for identifying an uniq palf instance.
  int64_t palf_epoch_;
  //used to control frequency of purging throttling
  int64_t last_purge_throttling_ts_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
