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

#ifndef OCEABASE_STORAGE_LS_RESTORE_STAT_H
#define OCEABASE_STORAGE_LS_RESTORE_STAT_H

#include "share/restore/ob_restore_persist_helper.h"
#include "ob_ls_restore_task_mgr.h"
#include "storage/high_availability/ob_storage_restore_struct.h"
#ifdef OB_BUILD_SHARED_STOARGE
#include "close_modules/shared_storage/storage/high_availability/ob_ss_ls_restore_state.h"
#endif

namespace oceanbase
{
namespace storage
{
using namespace share;
class ObLSRestoreStat final
{
public:
  ObLSRestoreStat()
    : is_inited_(false),
      ls_key_(),
      total_tablet_cnt_(0),
      unfinished_tablet_cnt_(0),
      total_bytes_(0),
      unfinished_bytes_(0),
      last_report_ts_(0),
      mtx_(common::ObLatchIds::OB_LS_RESTORE_STAT_MUTEX) {}

  int init(const share::ObLSRestoreJobPersistKey &ls_key);
  int set_total_tablet_cnt(const int64_t cnt);
  int inc_total_tablet_cnt();
  int dec_total_tablet_cnt();
  int add_finished_tablet_cnt(const int64_t inc_finished_tablet_cnt);
  int report_unfinished_tablet_cnt(const int64_t unfinished_tablet_cnt);
  int load_restore_stat();
  int get_finished_tablet_cnt(int64_t &finished_tablet_cnt) const;
  int set_total_bytes(const int64_t bytes);
  int increase_total_bytes_by(const int64_t bytes);
  int decrease_total_bytes_by(const int64_t bytes);
  int add_finished_bytes(const int64_t bytes);
  int report_unfinished_bytes(const int64_t bytes);
  void reset();

  TO_STRING_KV(K_(is_inited),
                K_(ls_key),
                K_(total_tablet_cnt),
                K_(unfinished_tablet_cnt),
                K_(last_report_ts));

private:
  static const int64_t REPORT_INTERVAL = 30_s;
  int do_report_finished_tablet_cnt_(const int64_t finished_tablet_cnt);
  int do_report_finished_bytes_(const int64_t finished_bytes);
  int64_t get_finished_tablet_cnt_() const;
  int64_t get_finished_bytes() const;

private:
  bool is_inited_;
  share::ObLSRestoreJobPersistKey ls_key_;
  int64_t total_tablet_cnt_;
  int64_t unfinished_tablet_cnt_;
  int64_t total_bytes_;
  int64_t unfinished_bytes_;
  int64_t last_report_ts_;
  mutable lib::ObMutex mtx_;

  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreStat);
};

class ObLSRestoreResultMgr final
{
public:
  enum RestoreFailedType {
    DATA_RESTORE_FAILED_TYPE = 0,
    CLOG_RESTORE_FAILED_TYPE = 1,
    MAX_FAILED_TYPE
  };
  const static int64_t OB_MAX_LS_RESTORE_RETRY_TIME_INTERVAL = 10 * 1000 * 1000; // 10s
  const static int64_t OB_MAX_RESTORE_RETRY_TIMES = 64;
public:
  ObLSRestoreResultMgr();
  ~ObLSRestoreResultMgr() {}
  int get_result() const { return result_; }
  const share::ObTaskId &get_trace_id() const { return trace_id_; }
  bool can_retry() const;
  bool is_met_retry_time_interval();
  void set_result(const int result, const share::ObTaskId &trace_id, const RestoreFailedType &failed_type);
  int get_comment_str(const ObLSID &ls_id, const ObAddr &addr, ObHAResultInfo::Comment &comment) const;
  bool can_retrieable_err(const int err) const;
  void reset();

  TO_STRING_KV(K_(result), K_(retry_cnt), K_(trace_id), K_(failed_type));
private:
  lib::ObMutex mtx_;
  int result_;
  int64_t retry_cnt_;
  int64_t last_err_ts_;
  share::ObTaskId trace_id_;
  RestoreFailedType failed_type_;

  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreResultMgr);
};
}
}

#endif