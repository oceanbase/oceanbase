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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_signal_handle.h"

#include "io/easy_log.h"

#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/profile/ob_profile_log.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/profile/ob_atomic_event.h"
#include "lib/signal/ob_signal_struct.h"
#include "storage/memtable/ob_memtable.h"
#include "observer/ob_server.h"
#include "observer/ob_dump_task_generator.h"
#include "sql/ob_sql_init.h"
#include "lib/allocator/ob_pcounter.h"
#include "storage/tx_storage/ob_tenant_memory_printer.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace observer
{

void ObSignalHandle::run1()
{


  int ret = OB_SUCCESS;
  lib::set_thread_name("SignalHandle");
  sigset_t   waitset;
  if (OB_FAIL(add_signums_to_set(waitset))) {
    LOG_ERROR("Add signal set error", K(ret));
  } else {
    int signum = -1;
    //to check _stop every second
    struct timespec timeout = {1, 0};
    while (!has_set_stop()) {//need not to check ret
      {
        oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT);
        signum = sigtimedwait(&waitset, NULL, &timeout);
      }
      if (-1 == signum) {
        //do not log error, because timeout will also return -1.
      } else if (OB_FAIL(deal_signals(signum))) {
        LOG_WARN("Deal signal error", K(ret), K(signum));
      } else {
        //do nothing
      }
    }
  }
}

int ObSignalHandle::change_signal_mask()
{
  int ret = OB_SUCCESS;
  sigset_t block_set, old_set;
  if (OB_FAIL(add_signums_to_set(block_set))) {
    LOG_ERROR("Add signal set error", K(ret));
  } else if (0 != pthread_sigmask(SIG_BLOCK, &block_set, &old_set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Fail to change mask of blcked signals", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObSignalHandle::add_signums_to_set(sigset_t &sig_set)
{
  int ret = OB_SUCCESS;
  if (0 != sigemptyset(&sig_set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Empty signal set error", K(ret));
  } else {
    int rc = 0;
    int signals[] = {SIGPIPE, SIGTERM, SIGUSR1,
                     40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                     50, 51, 52, 53, 55, 56, 57, 59, 60, 62,
                     63, 64};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(signals); ++i) {
      if (0 != (rc = sigaddset(&sig_set, signals[i]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to add signal to block", "signum", signals[i], K(ret), K(rc));
      }
    }
  }
  return ret;
}

//signal http://blog.csdn.net/yiyeguzhou100/article/details/51316175
int ObSignalHandle::deal_signals(int signum)
{
  int ret = OB_SUCCESS;
  LOG_INFO("received signal", K(signum));
  switch (signum)
  {
    case SIGPIPE: {
      break;
    }
    case SIGTERM: {
      raise(SIGKILL);
      break;
    }
    case SIGUSR1: {
      ObServer::get_instance().prepare_stop();
      ObServer::get_instance().set_stop();
      break;
    }
    case 40: {
      OB_LOGGER.check_file();
      break;
    }
    case 41: {
      int64_t version = ::oceanbase::common::ObTimeUtility::current_time();
      OB_LOGGER.down_log_level(version);
      // ASYNC_LOG_LOGGER.down_log_level();
      //to echo info when change to warn level
      LOG_WARN("Signal 41 down ObLogger level", "current level", OB_LOGGER.get_level());
      break;
    }
    case 42: {
      int64_t version = ::oceanbase::common::ObTimeUtility::current_time();
      OB_LOGGER.up_log_level(version);
      // ASYNC_LOG_LOGGER.up_log_level();
      LOG_WARN("Signal 42 up ObLogger level", "current level", OB_LOGGER.get_level());
      break;
    }
    case 43: {  // toggle atomic operation profiler
      if (OB_ATOMIC_EVENT_IS_ENABLED()) {
        OB_DISABLE_ATOMIC_EVENT();
        LOG_WARN("atomic operation profiler disabled, see `atomic.pfda'");
      } else {
        OB_ENABLE_ATOMIC_EVENT();
        LOG_WARN("atomic operation profiler enabled");
      }
      break;
    }
    case 45: {
      if (OB_ISNULL(ObProfileLogger::getInstance())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ObProfileLogger instance should not be NULL", K(ret));
      } else {
        ObProfileLogger::getInstance()->setLogLevel("INFO");
      }
      break;
    }
    case 46: {
      if (OB_ISNULL(ObProfileLogger::getInstance())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ObProfileLogger instance should not be NULL", K(ret));
      } else {
        ObProfileLogger::getInstance()->setLogLevel("DEBUG");
      }
      break;
    }
    case 47: {
      easy_log_level = static_cast<easy_log_level_t>(static_cast<int>(easy_log_level) + 1);
      LOG_INFO("libeasy log level changed", K(easy_log_level));
      break;
    }
    case 48: {
      easy_log_level = static_cast<easy_log_level_t>(static_cast<int>(easy_log_level) - 1);
      LOG_INFO("libeasy log level changed", K(easy_log_level));
      break;
    }
    case 49: {
      ob_print_mod_memory_usage();
      //GARL_PRINT();
      PC_REPORT();
      ObTenantMemoryPrinter::get_instance().print_tenant_usage();
      break;
    }
    case 50: {
      ENABLE_PERF_EVENT();
      LOG_WARN("perf event enabled");
      break;
    }
    case 51: {
      DISABLE_PERF_EVENT();
      LOG_WARN("perf event disabled");
      break;
    }
    case 53: {
      // debug SQL modules
      if (OB_FAIL(
          ObLogger::get_logger().set_mod_log_levels("ALL.*:ERROR, SQL.*:DEBUG, RPC.*:WARN"))) {
        LOG_WARN("Set mod log level error", K(ret));
      }
      break;
    }
    case 55: {
      if (OB_ISNULL(ObServer::get_instance().get_gctx().omt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ObMultiTenant in global context should not be NULL", K(ret));
      } else {
        ObServer::get_instance().get_gctx().omt_->set_cpu_dump();
        LOG_INFO("CPU_DUMP: switch on");
      }
      break;
    }
    case 56: {
      if (OB_ISNULL(ObServer::get_instance().get_gctx().omt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ObMultiTenant in global context should not be NULL", K(ret));
      } else {
        ObServer::get_instance().get_gctx().omt_->unset_cpu_dump();
        LOG_INFO("CPU_DUMP: switch off");
      }
      break;
    }
    case 57: {
      // clog::ObBatchSubmitMockTest::handle_signal();
      break;
    }
    case 59: {               // similar to 49, verbose mode
      ob_print_mod_memory_usage(false, true);
      sql::print_sql_stat();
      break;
    }
    case 60: {
    #ifndef OB_USE_ASAN
      send_request_and_wait(VERB_LEVEL_1,
                            syscall(SYS_gettid)/*exclude_id*/);
    #endif
      break;
    }
    case 62: {
      //RESP_DUMP_TRACE_TO_FILE();
      ObDumpTaskGenerator::generate_task_from_file();
      break;
    }
    case 63: {
      // print tenant memstore consumption condition by wenduo.swd
      ObTenantMemoryPrinter::get_instance().print_tenant_usage();
      break;
    }
    default: {
      LOG_WARN("Ignore unknown signal", K(signum));
      break;
    }
  }
  return ret;
}

}
}
