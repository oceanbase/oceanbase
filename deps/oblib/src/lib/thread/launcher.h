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
// Holistic principle
//
// 1. Launcher state transition rules:
//
//     init/destroy        start     stop
// Uninitialized <===> Initialization without start ==> started ==> stopping
//                                    /\                               ||
//                                    ||             wait              ||
//                                    \=================================/
//
// 2. Init and destroy appear in pairs, start and stop appear in pairs.
// 3. Do not start the thread in init, and do not stop the thread in destroy, which will disrupt the outer logic.
// 4. When implementing init, if you fail halfway, you need to call the destroy function of the member that has been
// init successfully, so there should be no redundant state. The start function is the same.
// 5. Wrong state transitions, such as calling the start method when "uninitialized", are bugs to be fixed. It is
// recommended to print the ERROR log for early exposure.
// 6. All self-contained thread classes should inherit and follow this set of rules.
//
class ILauncher {
public:
  virtual ~ILauncher()
  {}

  // Initialize resources
  virtual int init() = 0;
  // Start thread
  virtual int start() = 0;
  // Stop thread
  //
  // Note that the return of this interface does not guarantee that the thread has stopped, only the call to wait()
  // returns to indicate that the thread has exited.
  virtual void stop() = 0;
  // Used in conjunction with the stop interface, wait() returns to indicate that all threads have successfully exited.
  virtual void wait() = 0;
  // Free up resources.
  virtual void destroy() = 0;
};

}  // namespace lib
}  // namespace oceanbase
