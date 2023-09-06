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

#ifndef OCEANBASE_PX_OB_DFO_SCHEDULER_H_
#define OCEANBASE_PX_OB_DFO_SCHEDULER_H_

#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
namespace sql
{
class ObPxCoordInfo;
class ObPxRootDfoAction;
class ObPxMsgProc;

class ObDfoSchedulerBasic
{
public:
  ObDfoSchedulerBasic(ObPxCoordInfo &coord_info,
                      ObPxRootDfoAction &root_dfo_action,
                      ObIPxCoordEventListener &listener);
public:
  virtual int dispatch_dtl_data_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const = 0;
  virtual int try_schedule_next_dfo(ObExecContext &ctx) const = 0;
  virtual int set_temp_table_ctx_for_sqc(ObExecContext &exec_ctx, ObDfo &child) const;
  virtual int dispatch_transmit_channel_info_via_sqc(ObExecContext &ctx,
                                                     ObDfo &child,
                                                     ObDfo &parent) const;
  virtual int dispatch_receive_channel_info_via_sqc(ObExecContext &ctx,
                                                    ObDfo &child,
                                                    ObDfo &parent,
                                                    bool is_parallel_scheduler = true) const;
  virtual void clean_dtl_interm_result(ObExecContext &ctx) = 0;
  int build_data_xchg_ch(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
  int build_data_mn_xchg_ch(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
  virtual int init_all_dfo_channel(ObExecContext &ctx) const;
  virtual int on_sqc_threads_inited(ObExecContext &ctx, ObDfo &dfo) const;
  virtual int dispatch_root_dfo_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
  int get_tenant_id(ObExecContext &ctx, uint64_t &tenant_id) const;
  int prepare_schedule_info(ObExecContext &ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDfoSchedulerBasic);
protected:
  ObPxCoordInfo &coord_info_;
  ObPxRootDfoAction &root_dfo_action_;
  ObIPxCoordEventListener &listener_;
};

class ObSerialDfoScheduler : public ObDfoSchedulerBasic
{
public:
  using ObDfoSchedulerBasic::ObDfoSchedulerBasic;

  virtual int init_all_dfo_channel(ObExecContext &ctx) const;
  virtual int dispatch_dtl_data_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
  virtual int try_schedule_next_dfo(ObExecContext &ctx) const;
  virtual void clean_dtl_interm_result(ObExecContext &ctx) override;

private:
  struct CleanDtlIntermRes
  {
    ObPxCoordInfo &coord_info_;
    uint64_t tenant_id_;
    CleanDtlIntermRes(ObPxCoordInfo &coord_info, const uint64_t &tenant_id) : coord_info_(coord_info), tenant_id_(tenant_id) {}
    bool operator()(const ObAddr &attr, ObPxCleanDtlIntermResArgs *arg);
  };
  int build_transmit_recieve_channel(ObExecContext &ctx, ObDfo *dfo) const;
  int init_dfo_channel(ObExecContext &ctx, ObDfo *child, ObDfo *parent) const;
  int init_data_xchg_ch(ObExecContext &ctx, ObDfo *dfo) const;
  int dispatch_sqcs(ObExecContext &exec_ctx, ObDfo &dfo, ObArray<ObPxSqcMeta *> &sqcs) const;
  int do_schedule_dfo(ObExecContext &ctx, ObDfo &dfo) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSerialDfoScheduler);
};


class ObParallelDfoScheduler : public ObDfoSchedulerBasic
{
public:
    ObParallelDfoScheduler(ObPxCoordInfo &coord_info,
                           ObPxRootDfoAction &root_dfo_action,
                           ObIPxCoordEventListener &listener,
                           ObPxMsgProc &proc)
        : ObDfoSchedulerBasic(coord_info, root_dfo_action, listener), proc_(proc)
    {}
    virtual int dispatch_dtl_data_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
    virtual int try_schedule_next_dfo(ObExecContext &ctx) const;
    virtual void clean_dtl_interm_result(ObExecContext &ctx) override { UNUSED(ctx); }
private:
    int dispatch_transmit_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
    int dispatch_receive_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const;
    int do_schedule_dfo(ObExecContext &exec_ctx, ObDfo &dfo) const;
    /* task 的 transmit channel 数据通过 sqc 捎带过去，避免一次 rpc
     * 用于和 root dfo 交互的场景
     */
    int check_if_can_prealloc_xchg_ch(ObDfo &child, ObDfo &parent, bool &bret) const;
    int do_fast_schedule(ObExecContext &exec_ctx,
                         ObDfo &child,
                         ObDfo &root_dfo) const;
    int mock_on_sqc_init_msg(ObExecContext &ctx, ObDfo &dfo) const;
    int schedule_dfo(ObExecContext &exec_ctx, ObDfo &dfo) const;
    int do_cleanup_dfo(ObDfo &dfo) const;
    int on_root_dfo_scheduled(ObExecContext &ctx, ObDfo &root_dfo) const;
    int dispatch_sqc(ObExecContext &exec_ctx,
                     ObDfo &dfo,
                     ObArray<ObPxSqcMeta *> &sqcs) const;
    int deal_with_init_sqc_error(ObExecContext &exec_ctx,
                                 const ObPxSqcMeta &sqc,
                                 int rc) const;
    int schedule_pair(ObExecContext &exec_ctx,
                      ObDfo &child,
                      ObDfo &parent) const;
    int wait_for_dfo_finish(ObDfoMgr &dfo_mgr) const;
  private:
    ObPxMsgProc &proc_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObParallelDfoScheduler);
};


}
}

#endif // OCEANBASE_PX_OB_DFO_SCHEDULER_H_
