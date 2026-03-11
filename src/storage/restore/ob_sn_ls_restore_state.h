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

#ifndef OCEABASE_STORAGE_SN_LS_RESTORE_STATE_H
#define OCEABASE_STORAGE_SN_LS_RESTORE_STATE_H

#include "ob_i_ls_restore_state.h"


namespace oceanbase
{
namespace storage
{
class ObLSRestoreStartState final : public ObILSRestoreState
{
public:
  ObLSRestoreStartState();
  virtual ~ObLSRestoreStartState();
  virtual int do_restore() override;
private:
  int check_ls_meta_exist_(bool &is_exist);
  int do_with_no_ls_meta_();
  int check_ls_created_(bool &is_created);
  int check_sys_ls_restore_finished_(bool &restore_finish);
  int do_with_uncreated_ls_();
  int inc_need_restore_ls_cnt_();
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreStartState);
};

class ObLSRestoreSysTabletState final : public ObILSRestoreState
{
public:
  ObLSRestoreSysTabletState();
  virtual ~ObLSRestoreSysTabletState();
  virtual int do_restore() override;
  virtual void set_retry_flag() { retry_flag_ = true; }
private:
  int leader_restore_sys_tablet_();
  int follower_restore_sys_tablet_();
  int do_restore_sys_tablet();
  bool is_need_retry_();
  int leader_fill_ls_restore_arg_(ObLSRestoreArg &arg);
  int follower_fill_ls_restore_arg_(ObLSRestoreArg &arg);
private:
  bool retry_flag_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreSysTabletState);
};

class ObLSRestoreCreateUserTabletState final : public ObILSRestoreState
{
public:
  ObLSRestoreCreateUserTabletState();
  virtual ~ObLSRestoreCreateUserTabletState();
  virtual int do_restore() override;
private:
  int leader_create_user_tablet_();
  int follower_create_user_tablet_();
  int do_create_user_tablet_(
      const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore);
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreCreateUserTabletState);
};


class ObLSRestoreConsistentScnState final : public ObILSRestoreState
{
public:
  ObLSRestoreConsistentScnState(): ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN), total_tablet_cnt_(0) {}
  virtual ~ObLSRestoreConsistentScnState() {}
  virtual int do_restore() override;

  // Check if log has recovered to consistent_scn.
  int check_recover_to_consistent_scn_finish(bool &is_finish) const;

private:
  // Set restore status to EMPTY for those committed tablets whose restore status is FULL,
  // but transfer table is not replaced.
  int set_empty_for_transfer_tablets_();
  int report_total_tablet_cnt_();

private:
  int64_t total_tablet_cnt_; // total to restore tablet count

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreConsistentScnState);
};


class ObLSQuickRestoreState final : public ObILSRestoreState
{
public:
  ObLSQuickRestoreState();
  virtual ~ObLSQuickRestoreState();
  virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override;

private:
  int leader_quick_restore_();
  int follower_quick_restore_();
  int do_quick_restore_(
      const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore);
  int check_tablet_checkpoint_();
  // Force reload all tablets and check is restored.
  bool has_rechecked_after_clog_recovered_;
  DISALLOW_COPY_AND_ASSIGN(ObLSQuickRestoreState);
};

class ObLSQuickRestoreFinishState final : public ObILSRestoreState
{
public:
  ObLSQuickRestoreFinishState();
  virtual ~ObLSQuickRestoreFinishState();
  virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
private:
  int leader_quick_restore_finish_();
  int follower_quick_restore_finish_();
  DISALLOW_COPY_AND_ASSIGN(ObLSQuickRestoreFinishState);
};

class ObLSRestoreMajorState final : public ObILSRestoreState
{
public:
  ObLSRestoreMajorState();
  virtual ~ObLSRestoreMajorState();
  virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
private:
  int leader_restore_major_data_();
  int follower_restore_major_data_();
  int do_restore_major_(
      const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore);
#ifdef ERRSIM
  int errsim_rebuild_before_restore_major_();
#endif
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreMajorState);
};

class ObLSRestoreFinishState final : public ObILSRestoreState
{
  public:
    ObLSRestoreFinishState();
    virtual ~ObLSRestoreFinishState();
    virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
  private:
    int restore_finish_();
    DISALLOW_COPY_AND_ASSIGN(ObLSRestoreFinishState);
};

class ObLSRestoreWaitState : public ObILSRestoreState
{
public:
  ObLSRestoreWaitState(const share::ObLSRestoreStatus::Status &status, const bool require_multi_replica_sync);
  virtual ~ObLSRestoreWaitState();
  virtual int do_restore() override;

protected:
  virtual int check_can_advance_status_(bool &can) const;
  virtual int report_restore_stat_();

private:
  int check_all_tablets_has_finished_(bool &all_finished);
  int leader_wait_follower_();
  int follower_wait_leader_();

private:
  // Indicate whether has checked all tablets has been restored.
  bool has_confirmed_;
  bool require_multi_replica_sync_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitState);
};

class ObLSRestoreWaitRestoreSysTabletState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitRestoreSysTabletState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS, false /* require multi replica sync */) {}
  virtual ~ObLSRestoreWaitRestoreSysTabletState() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitRestoreSysTabletState);
};

class ObLSRestoreWaitCreateUserTabletState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitCreateUserTabletState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META, true /* require multi replica sync */) {}
  virtual ~ObLSRestoreWaitCreateUserTabletState() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitCreateUserTabletState);
};


class ObLSWaitRestoreConsistentScnState final : public ObLSRestoreWaitState
{
public:
  ObLSWaitRestoreConsistentScnState()
    : ObLSRestoreWaitState(ObLSRestoreStatus::Status::WAIT_RESTORE_TO_CONSISTENT_SCN, false /* require multi replica sync */) {}
  virtual ~ObLSWaitRestoreConsistentScnState() {}

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = false;
    return OB_SUCCESS;
  }

protected:
  int check_can_advance_status_(bool &can) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSWaitRestoreConsistentScnState);
};


class ObLSRestoreWaitQuickRestoreState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitQuickRestoreState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE, false /* require multi replica sync */) {}
  virtual ~ObLSRestoreWaitQuickRestoreState() {}

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
protected:
  int check_can_advance_status_(bool &can) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitQuickRestoreState);
};

class ObLSRestoreWaitRestoreMajorDataState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitRestoreMajorDataState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA, false /* require multi replica sync */), has_reported_(false) {}
  virtual ~ObLSRestoreWaitRestoreMajorDataState() {}

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }

protected:
  virtual int report_restore_stat_() override;

private:
  bool has_reported_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitRestoreMajorDataState);
};

}
}
#endif