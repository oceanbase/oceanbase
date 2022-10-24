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

namespace oceanbase {
namespace lib {

////
// How to use Launcher?
//
// if (OB_SUCC(launcher.init())) {
//   if (OB_SUCC(launcher.start())) {
//     wait_stop_signal();
//     launcher.stop();
//     launcher.wait();
//   }
//   launcher.destroy();
// }
//
////
// How to combine launchers?
//
// int init()
// {
//   if (OB_SUCC(launcher1.init())) {
//     if (OB_SUCC(launcher2.init())) {
//       if (OB_SUCC(launcher3.init())) {
//       } else {
//         launcher1.destroy();
//         launcher2.destroy();
//       }
//     } else {
//       launcher1.destroy();
//     }
//   }
// }
// int start()
// {
//   if (OB_SUCC(launcher1.start())) {
//     if (OB_SUCC(launcher2.start())) {
//       if (OB_SUCC(launcher3.start())) {
//       } else {
//         launcher1.stop();
//         launcher2.stop();
//       }
//     } else {
//       launcher1.stop();
//     }
//   }
// }
////
// 整体原则
//
// 1. Launcher状态转换规则:
//
//     init/destroy        start     stop
// 未初始化 <===> 初始化未启动 ==> 已启动 ==> 正在停止
//                  /\                     ||
//                  ||         wait        ||
//                  \=======================/
//
// 2. init和destroy配对出现，start和stop配对出现。
// 3. init不要启动线程，也不要到destroy中再停线程，这会打乱外层的逻辑。
// 4. 实现init时，如果中途失败需要调用已经init成功成员的destroy函数，
//    不要出现多余的状态。start函数同理。
// 5. 错误的状态转移，比如“未初始化”时调用start方法，属于要修复的BUG。
//    推荐打印ERROR日志提早暴露。
// 6. 所有自包含线程的类都应该继承并遵循这套规则。
//
class ILauncher {
public:
  virtual ~ILauncher() {}

  // 初始化资源
  virtual int init() = 0;
  // 启动线程
  virtual int start() = 0;
  // 停止线程
  //
  // 注意该接口返回并不保证线程已经停止，只有调用wait返回了才表示线程已经退出。
  virtual void stop() = 0;
  // 和stop接口配合使用，wait返回表示所有线程都已经成功退出。
  virtual void wait() = 0;
  // 释放资源。
  virtual void destroy() = 0;
};


}  // lib
}  // oceanbase
