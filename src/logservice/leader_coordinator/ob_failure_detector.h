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

#ifndef LOGSERVICE_COORDINATOR_FAILURE_DETECTOR_H
#define LOGSERVICE_COORDINATOR_FAILURE_DETECTOR_H

#include "failure_event.h"
#include "lib/function/ob_function.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/ob_occam_timer.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{

class ObLeaderCoordinator;

/**
 * @description: ObFailureDetector探测各模块的失败，记录并影响选举优先级
 * [FailureEvent的值语义]
 * FailureEvent由三个枚举字段和一个info_字段组成，info_字段的类型是ObStringHolder，它在堆上开辟内存空间，使用RAII管理所掌管的字符串的生命周期
 * 约定每个模块所注册的每个failure都是不同的，用户不应该多次重复注册同一个FailureEvent，ObFailureDetector将检查已注册的异常事件，
 * 并将拒绝用户注册一个已经存在的FailureEvent，用户需要在FailureEvent.info_字段的内容上作文章以区分同一个模块注册的两种不同的failure。
 * [Failure的不同等级]
 * 除非用户显示指定，FailureEvent具有默认的SERIOUS等级，该等级的Failure将触发切主。
 * 除此之外，可以显示指定NOTICE等级以及FATAL等级，前者不影响选举优先级仅作内部表展示，后者将比一般的Failure事件具备更高的切主优先级
 * [从异常中恢复]
 * 用户可以选择注册一个recover_detect_operation，该操作在后台以1次/s的频率探测失败是否恢复，
 * 约定recover_detect_operation返回true时表示异常恢复，用户应当保证recover_detect_operation的调用时间足够短（应在ms量级），以避免阻塞后台线程池
 * 用户也可以选择调用remove_failure_event(event)接口来主动取消异常事件，当event == failure的时候，取消对应failure的异常记录
 * @Date: 2022-01-10 10:21:28
 */
class ObFailureDetector
{
  friend class ObLeaderCoordinator;
public:
  ObFailureDetector();
  ~ObFailureDetector();
  void destroy();
  static int mtl_init(ObFailureDetector *&p_failure_detector);
  static int mtl_start(ObFailureDetector *&p_failure_detector);
  static void mtl_stop(ObFailureDetector *&p_failure_detector);
  static void mtl_wait(ObFailureDetector *&p_failure_detector);
  /**
   * @description: 设置一个不可自动恢复的failure，需要由注册的模块手动调用remove_failure_event()接口恢复failure，否则将持续存在
   * @param {FailureEvent} event failure事件，定义在failure_event.h中
   * @return {*}
   * @Date: 2021-12-29 11:13:25
   */
  int add_failure_event(const FailureEvent &event);
  /**
   * @description: 由外部设置的failure事件，并设置相应的恢复检测逻辑
   * @param {FailureEvent} event failure事件，定义在failure_event.h中
   * @param {ObFunction<bool()>} recover_detect_operation 检测failure恢复的操作，会被周期性调用
   * @return {*}
   * @Date: 2021-12-29 10:27:54
   */
  int add_failure_event(const FailureEvent &event, const ObFunction<bool()> &recover_detect_operation);
  /**
   * @description: 用户选择注销一个曾经注册过的failure事件
   * @param {FailureEvent} event 一个曾经注册过的failure事件
   * @return {*}
   * @Date: 2022-01-09 15:08:22
   */
  int remove_failure_event(const FailureEvent &event);
  int get_specified_level_event(FailureLevel level, ObIArray<FailureEvent> &results);
public:
  /**
   * @description: 定期探测异常是否恢复的定时任务
   * @param {*}
   * @return {*}
   * @Date: 2022-01-04 21:12:00
   */
  void detect_recover();
  /**
   * @description: detect whether failure has occured
   * @param {*}
   * @return {*}
   */
  void detect_failure();
  bool is_clog_disk_has_fatal_error();
  bool is_data_disk_has_fatal_error();
  bool is_schema_not_refreshed();
  bool is_data_disk_full() const
  {
    return has_add_disk_full_event_;
  }
private:
  bool check_is_running_() const { return is_running_; }
  int insert_event_to_table_(const FailureEvent &event, const ObFunction<bool()> &recover_operation, ObString info);
  void detect_palf_hang_failure_();
  void detect_data_disk_io_failure_();
  void detect_palf_disk_full_();
  void detect_schema_not_refreshed_();
  void detect_data_disk_full_();
private:
  struct FailureEventWithRecoverOp {
    int init(const FailureEvent &event, const ObFunction<bool()> &recover_detect_operation);
    int assign(const FailureEventWithRecoverOp &);
    FailureEvent event_;
    ObFunction<bool()> recover_detect_operation_;
    TO_STRING_KV(K_(event));
  };
  bool is_running_;
  common::ObArray<FailureEventWithRecoverOp> events_with_ops_;
  common::ObArray<common::ObAddr> tenant_server_list_;
  common::ObOccamTimerTaskRAIIHandle failure_task_handle_;
  common::ObOccamTimerTaskRAIIHandle recovery_task_handle_;
  ObLeaderCoordinator *coordinator_;
  bool has_add_clog_hang_event_;
  bool has_add_data_disk_hang_event_;
  bool has_add_clog_full_event_;
  bool has_schema_error_;
  bool has_add_disk_full_event_;
  ObSpinLock lock_;
};

}
}
}

#endif
