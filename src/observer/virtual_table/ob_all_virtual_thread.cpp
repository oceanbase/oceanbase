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

#include "ob_all_virtual_thread.h"
#include "lib/thread/protected_stack_allocator.h"

#define GET_OTHER_TSI_ADDR(var_name, addr) \
const int64_t var_name##_offset = ((int64_t)addr - (int64_t)pthread_self()); \
decltype(*addr) var_name = *(decltype(addr))(thread_base + var_name##_offset);

namespace oceanbase
{
using namespace lib;
namespace observer
{
ObAllVirtualThread::ObAllVirtualThread() : is_inited_(false)
{
}

ObAllVirtualThread::~ObAllVirtualThread()
{
  reset();
}

int ObAllVirtualThread::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

void ObAllVirtualThread::reset()
{
  is_inited_ = false;
}

int ObAllVirtualThread::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    const int64_t col_count = output_column_ids_.count();
    pid_t pid = getpid();
    StackMgr::Guard guard(g_stack_mgr);
    for (auto* header = *guard; OB_NOT_NULL(header); header = guard.next()) {
      auto* thread_base = (char*)(header->pth_);
      if (OB_NOT_NULL(thread_base)) {
        GET_OTHER_TSI_ADDR(tid, &get_tid_cache());
        {
          char path[64];
          IGNORE_RETURN snprintf(path, 64, "/proc/self/task/%ld", tid);
          if (-1 == access(path, F_OK)) {
            // thread not exist, may have exited.
            continue;
          }
        }
        GET_OTHER_TSI_ADDR(tenant_id, &ob_get_tenant_id());
        if (!is_sys_tenant(effective_tenant_id_)
            && tenant_id != effective_tenant_id_) {
          continue;
        }
        GET_OTHER_TSI_ADDR(wait_addr, &ObLatch::current_wait);
        GET_OTHER_TSI_ADDR(join_addr, &Thread::thread_joined_);
        GET_OTHER_TSI_ADDR(sleep_us, &Thread::sleep_us_);
        GET_OTHER_TSI_ADDR(blocking_ts, &Thread::blocking_ts_);
        for (int64_t i = 0; i < col_count && OB_SUCC(ret); ++i) {
          const uint64_t col_id = output_column_ids_.at(i);
          ObObj *cells = cur_row_.cells_;
          switch (col_id) {
            case SVR_IP: {
              cells[i].set_varchar(ip_buf_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case SVR_PORT: {
              cells[i].set_int(GCONF.self_addr_.get_port());
              break;
            }
            case TENANT_ID: {
              cells[i].set_int(0 == tenant_id ? OB_SERVER_TENANT_ID : tenant_id);
              break;
            }
            case TID: {
              cells[i].set_int(tid);
              break;
            }
            case TNAME: {
              GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
              // PAY ATTENTION HERE
              MEMCPY(tname_, thread_base + tname_offset, sizeof(tname_));
              cells[i].set_varchar(tname_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case STATUS: {
              const char* status_str = nullptr;
              if (0 != join_addr) {
                status_str = "Join";
              } else if (0 != sleep_us) {
                status_str = "Sleep";
              } else if (0 != blocking_ts) {
                status_str = "Wait";
              } else {
                status_str = "Run";
              }
              cells[i].set_varchar(status_str);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case WAIT_EVENT: {
              GET_OTHER_TSI_ADDR(rpc_dest_addr, &Thread::rpc_dest_addr_);
              GET_OTHER_TSI_ADDR(event, &Thread::wait_event_);
              ObAddr addr;
              struct iovec local_iov = {&addr, sizeof(ObAddr)};
              struct iovec remote_iov = {thread_base + rpc_dest_addr_offset, sizeof(ObAddr)};
              wait_event_[0] = '\0';
              if (0 != join_addr) {
                IGNORE_RETURN snprintf(wait_event_, 64, "thread %u", *(uint32_t*)(join_addr + tid_offset));
              } else if (OB_NOT_NULL(wait_addr)) {
                uint32_t val = 0;
                struct iovec local_iov = {&val, sizeof(val)};
                struct iovec remote_iov = {wait_addr, sizeof(val)};
                ssize_t n = process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0);
                if (n != sizeof(val)) {
                } else if (0 != (val & (1<<30))) {
                  IGNORE_RETURN snprintf(wait_event_, 64, "wrlock on %u", val & 0x3fffffff);
                } else {
                  IGNORE_RETURN snprintf(wait_event_, 64, "%u rdlocks", val & 0x3fffffff);
                }
              } else if (sizeof(ObAddr) == process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0)
                         && addr.is_valid()) {
                GET_OTHER_TSI_ADDR(pcode, &Thread::pcode_);
                int64_t pos1 = 0;
                int64_t pos2 = 0;
                if (((pos1 = snprintf(wait_event_, 37, "rpc 0x%X(%s", pcode, obrpc::ObRpcPacketSet::instance().name_of_idx(obrpc::ObRpcPacketSet::instance().idx_of_pcode(pcode)) + 3)) > 0)
                    && ((pos2 = snprintf(wait_event_ + std::min(36L, pos1), 6, ") to ")) > 0)) {
                  int64_t pos = std::min(36L, pos1) + std::min(5L, pos2);
                  pos += addr.to_string(wait_event_ + pos, 64 - pos);
                }
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_IN_TENANT_QUEUE & event))) {
                IGNORE_RETURN snprintf(wait_event_, 64, "tenant worker requests");
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_IO_EVENT & event))) {
                IGNORE_RETURN snprintf(wait_event_, 64, "IO events");
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_LOCAL_RETRY & event))) {
                IGNORE_RETURN snprintf(wait_event_, 64, "local retry");
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_PX_MSG & event))) {
                IGNORE_RETURN snprintf(wait_event_, 64, "px message");
              } else if (0 != sleep_us) {
                IGNORE_RETURN snprintf(wait_event_, 64, "%ld us", sleep_us);
              } else if (0 != blocking_ts) {
                IGNORE_RETURN snprintf(wait_event_, 64, "%ld us", common::ObTimeUtility::fast_current_time() - blocking_ts);
              }
              cells[i].set_varchar(wait_event_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case LATCH_WAIT: {
              if (OB_ISNULL(wait_addr)) {
                cells[i].set_varchar("");
              } else {
                IGNORE_RETURN snprintf(wait_addr_, 16, "%p", wait_addr);
                cells[i].set_varchar(wait_addr_);
              }
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case LATCH_HOLD: {
              GET_OTHER_TSI_ADDR(locks_addr, &(ObLatch::current_locks[0]));
              GET_OTHER_TSI_ADDR(slot_cnt, &ObLatch::max_lock_slot_idx)
              const int64_t cnt = std::min(ARRAYSIZEOF(ObLatch::current_locks), (int64_t)slot_cnt);
              decltype(&locks_addr) locks = (decltype(&locks_addr))(thread_base + locks_addr_offset);
              locks_addr_[0] = 0;
              for (int64_t i = 0, j = 0; i < cnt; ++i) {
                int64_t idx = (slot_cnt + i) % ARRAYSIZEOF(ObLatch::current_locks);
                if (OB_NOT_NULL(locks[idx]) && j < 256) {
                  uint32_t val = 0;
                  struct iovec local_iov = {&val, sizeof(val)};
                  struct iovec remote_iov = {locks[idx], sizeof(val)};
                  ssize_t n = process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0);
                  if (n != sizeof(val)) {
                  } else if (0 != val) {
                    j += snprintf(locks_addr_ + j, 256 - j, "%p ", locks[idx]);
                  }
                }
              }
              cells[i].set_varchar(locks_addr_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case TRACE_ID: {
              GET_OTHER_TSI_ADDR(trace_id, ObCurTraceId::get_trace_id());
              IGNORE_RETURN trace_id.to_string(trace_id_buf_, sizeof(trace_id_buf_));
              cells[i].set_varchar(trace_id_buf_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case LOOP_TS: {
              GET_OTHER_TSI_ADDR(loop_ts, &oceanbase::lib::Thread::loop_ts_);
              cells[i].set_timestamp(loop_ts);
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
              break;
            }
          }
        }
        if (OB_SUCC(ret)) {
          // scanner最大支持64M，因此暂不考虑溢出的情况
          if (OB_FAIL(scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            if (OB_SIZE_OVERFLOW == ret) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      is_inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase