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

#include "mds_tenant_service.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/string/ob_string_holder.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
#include "ob_clock_generator.h"
#include "share/rc/ob_tenant_base.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet_iterator.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace mds
{
/********************FOR MEMORY LEAK DEBUG***************************/
thread_local char __thread_mds_tag__[TAG_SIZE] = {0};
TLOCAL(const char *, __thread_mds_alloc_type__) = nullptr;
TLOCAL(const char *, __thread_mds_alloc_file__) = nullptr;
TLOCAL(const char *, __thread_mds_alloc_func__) = nullptr;
TLOCAL(uint32_t, __thread_mds_alloc_line__) = 0;

void set_mds_mem_check_thread_local_info(const storage::mds::MdsWriter &writer,
                                         const char *alloc_ctx_type,
                                         const char *alloc_file,
                                         const char *alloc_func,
                                         const uint32_t alloc_line)
{
  int64_t pos = 0;
  databuff_printf(__thread_mds_tag__, TAG_SIZE, pos, "%s", to_cstring(writer));
  __thread_mds_alloc_type__ = alloc_ctx_type;
  __thread_mds_alloc_file__ = alloc_file;
  __thread_mds_alloc_func__ = alloc_func;
  __thread_mds_alloc_line__ = alloc_line;
}

void set_mds_mem_check_thread_local_info(const share::ObLSID &ls_id,
                                         const ObTabletID &tablet_id,
                                         const char *data_type,
                                         const char *alloc_file,
                                         const char *alloc_func,
                                         const uint32_t alloc_line)
{
  int64_t pos = 0;
  databuff_printf(__thread_mds_tag__, TAG_SIZE, pos, "%s, %s", to_cstring(ls_id), to_cstring(tablet_id));
  __thread_mds_alloc_type__ = data_type;
  __thread_mds_alloc_file__ = alloc_file;
  __thread_mds_alloc_func__ = alloc_func;
  __thread_mds_alloc_line__ = alloc_line;
}

void reset_mds_mem_check_thread_local_info()
{
  __thread_mds_tag__[0] = '\0';
  __thread_mds_alloc_type__ = nullptr;
  __thread_mds_alloc_file__ = nullptr;
  __thread_mds_alloc_func__ = nullptr;
  __thread_mds_alloc_line__ = 0;
}
/********************************************************************/

int ObTenantMdsService::mtl_init(ObTenantMdsService *&mds_service)
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (mds_service->is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG(ERROR, "init mds tenant service twice!", KR(ret), KPC(mds_service));
  } else if (MDS_FAIL(mds_service->memory_leak_debug_map_.init("MdsDebugMap", MTL_ID()))) {
    MDS_LOG(WARN, "init map failed", K(ret));
  } else if (MDS_FAIL(mds_service->mds_timer_.timer_.init_and_start(1/*worker number*/,
                                                                    100_ms/*precision*/,
                                                                    "MdsT"/*thread name*/))) {
    MDS_LOG(ERROR, "fail to init timer", KR(ret), KPC(mds_service));
  } else {
    mds_service->is_inited_ = true;
  }
  return ret;
}

int ObTenantMdsService::mtl_start(ObTenantMdsService *&mds_service)
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (MDS_FAIL(mds_service->mds_timer_.timer_.schedule_task_repeat(
    mds_service->mds_timer_.recycle_task_handle_,
    3_s,
    [mds_service]() -> bool {
      ObCurTraceId::init(GCONF.self_addr_);
      if (REACH_TIME_INTERVAL(30_s)) {
        observer::ObMdsEventBuffer::dump_statistics();
        mds_service->dump_map_holding_item(5_min);
      }
      mds_service->mds_timer_.try_recycle_mds_table_task();
      return false;// won't stop until tenant exit
    }
  ))) {
    MDS_LOG(ERROR, "fail to register recycle timer task to timer", KR(ret), KPC(mds_service));
  } else if (MDS_FAIL(mds_service->mds_timer_.timer_.schedule_task_repeat(
    mds_service->mds_timer_.dump_special_mds_table_status_task_handle_,
    15_s,
    [mds_service]() -> bool {
      ObCurTraceId::init(GCONF.self_addr_);
      mds_service->mds_timer_.dump_special_mds_table_status_task();
      return false;// won't stop until tenant exit
    }
  ))) {
    MDS_LOG(ERROR, "fail to register dump mds table status task to timer", KR(ret), KPC(mds_service));
  }
  return ret;
}

void ObTenantMdsService::mtl_stop(ObTenantMdsService *&mds_service)
{
  if (nullptr != mds_service) {
    mds_service->mds_timer_.recycle_task_handle_.stop();
    mds_service->mds_timer_.dump_special_mds_table_status_task_handle_.stop();
  }
}

void ObTenantMdsService::mtl_wait(ObTenantMdsService *&mds_service)
{
  if (nullptr != mds_service) {
    mds_service->mds_timer_.recycle_task_handle_.wait();
    mds_service->mds_timer_.dump_special_mds_table_status_task_handle_.wait();
  }
}

void ObTenantMdsTimer::try_recycle_mds_table_task()
{
  #define PRINT_WRAPPER KR(ret), KPC(this), K(tablet_oldest_scn), K(mds_table_handle)
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObTenantMdsService::for_each_ls_in_tenant([this](ObLS &ls) -> int {
    ObTenantMdsService::for_each_mds_table_in_ls(ls, [this](ObTablet &tablet) -> int {// FIXME: there is no need scan all tablets
      (void) this->process_with_tablet_(tablet);
      return OB_SUCCESS;// keep doing ignore error
    });
    return OB_SUCCESS;// keep doing ignore error
  });
  #undef PRINT_WRAPPER
}

void ObTenantMdsTimer::dump_special_mds_table_status_task()
{
  #define PRINT_WRAPPER KR(ret)
  MDS_TG(1_s);
  ObCurTraceId::init(GCONF.self_addr_);
  ObTenantMdsService::for_each_ls_in_tenant([](ObLS &ls) -> int {
    int ret = OB_SUCCESS;
    MDS_TG(1_s);
    MdsTableMgrHandle mds_table_mge_handle;
    share::SCN ls_mds_freezing_scn;
    if (MDS_FAIL(ls.get_mds_table_mgr(mds_table_mge_handle))) {
      MDS_LOG_NONE(WARN, "fail to get mds table mgr");
    } else if (FALSE_IT(ls_mds_freezing_scn = mds_table_mge_handle.get_mds_table_mgr()->get_freezing_scn())) {
    } else {
      (void)mds_table_mge_handle.get_mds_table_mgr()->for_each_in_t3m_mds_table([ls_mds_freezing_scn](MdsTableBase &mds_table) -> int {// with hash map bucket's lock protected
        (void) mds_table.operate([ls_mds_freezing_scn](MdsTableBase &mds_table)-> int {// with MdsTable's lock protected
          int ret = OB_SUCCESS;
          if (mds_table.get_rec_scn() <= ls_mds_freezing_scn) {
            // ignore ret
            MDS_LOG_NOTICE(WARN, "dump rec_scn lagging freeze_scn mds_table", K(ls_mds_freezing_scn), K(mds_table));
          }
          return OB_SUCCESS;// keep iterating
        });
        return OB_SUCCESS;// keep iterating
      });
      (void)mds_table_mge_handle.get_mds_table_mgr()->for_each_removed_mds_table([](MdsTableBase &mds_table) -> int {
        (void) mds_table.operate([](MdsTableBase &mds_table)-> int {// with MdsTable's lock protected
          int ret = OB_SUCCESS;
          if (ObClockGenerator::getClock() - mds_table.get_removed_from_t3m_ts() > 1_min) {
            // ignore ret
            MDS_LOG_NOTICE(WARN, "dump maybe leaked mds_table", K(mds_table));
          }
          return OB_SUCCESS;// keep iterating
        });
        return OB_SUCCESS;// keep iterating
      });
    };
    return OB_SUCCESS;// keep doing ignore error
  });
  #undef PRINT_WRAPPER
}

int ObTenantMdsService::for_each_ls_in_tenant(const ObFunction<int(ObLS &)> &op)
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(3_s);
  ObSharedGuard<ObLSIterator> iter;
  ObLS *ls = nullptr;
  int64_t succ_num = 0;
  if (!op.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(WARN, "invalid op");
  } else if (MDS_FAIL(MTL(ObLSService*)->get_ls_iter(iter, ObLSGetMod::MDS_TABLE_MOD))) {
    MDS_LOG_NONE(WARN, "fail to get ls iterator");
  } else {
    do {
      if (MDS_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          MDS_LOG_NONE(WARN, "get next iter failed");
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (MDS_FAIL(op(*ls))) {
        MDS_LOG_NONE(WARN, "fail to for each ls", K(succ_num));
      } else {
        MDS_LOG_NONE(DEBUG, "succeed to operate one ls", K(ret), "ls_id", ls->get_ls_id());
      }
    } while (++succ_num && OB_SUCC(ret));
  }
  MDS_LOG_NONE(INFO, "for each ls", K(succ_num));
  return ret;
  #undef PRINT_WRAPPER
}

int ObTenantMdsService::for_each_tablet_in_ls(ObLS &ls, const ObFunction<int(ObTablet &)> &op)
{
  #define PRINT_WRAPPER KR(ret), K(ls)
  int ret = OB_SUCCESS;
  int64_t succ_num = 0;
  ObLSTabletIterator tablet_iter(storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  MDS_TG(500_ms);
  if (!op.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(WARN, "invalid op");
  } else if (MDS_FAIL(ls.build_tablet_iter(tablet_iter))) {
    MDS_LOG_NONE(WARN, "failed to build ls tablet iter");
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    do {
      tablet_handle.reset();
      tablet = nullptr;
      if (MDS_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END != ret && OB_EMPTY_RESULT != ret) {
          MDS_LOG_NONE(WARN, "failed to get tablet");
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG_NONE(WARN, "tablet should not be NULL", KPC(tablet));
      } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
        // FIXME: there is no mds table on ls inner tablet yet, but there will be
      } else {
        op(*tablet);
      }
    } while (++succ_num && OB_SUCC(ret));
    MDS_LOG_NONE(INFO, "for each tablet", K(succ_num));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTenantMdsService::for_each_mds_table_in_ls(ObLS &ls, const ObFunction<int(ObTablet &)> &op)
{
  #define PRINT_WRAPPER KR(ret), K(ls), K(mds_table_total_num), K(ids_in_t3m_array.count())
  int ret = OB_SUCCESS;
  int64_t succ_num = 0;
  MDS_TG(10_s);

  int64_t mds_table_total_num = 0;
  MdsTableMgrHandle mgr_handle;
  ObArray<ObTabletID> ids_in_t3m_array;
  if (MDS_FAIL(ls.get_mds_table_mgr(mgr_handle))) {
    MDS_LOG_NONE(WARN, "fail to get mds table mgr");
  } else if (MDS_FAIL(mgr_handle.get_mds_table_mgr()->for_each_in_t3m_mds_table(
    [&mds_table_total_num, &ids_in_t3m_array, &ls](MdsTableBase &mds_table) -> int {// with map's bucket lock protected
      MDS_TG(1_s);
      int ret = OB_SUCCESS;
      if (MDS_FAIL(ids_in_t3m_array.push_back(mds_table.get_tablet_id()))) {
        MDS_LOG_NONE(WARN, "fail to push array");
      }
      return ret;
    }
  ))) {
    MDS_LOG_NONE(WARN, "fail to scan mds_table");
  } else {
    for (int64_t idx = 0; idx < ids_in_t3m_array.count(); ++idx) {// ignore ret
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls.get_tablet(ids_in_t3m_array[idx], tablet_handle, 1_s, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        MDS_LOG_NONE(WARN, "fail to get tablet_handle", K(ids_in_t3m_array[idx]));
      } else if (OB_FAIL(op(*tablet_handle.get_obj()))) {
        MDS_LOG_NONE(WARN, "fail to process with tablet", K(ids_in_t3m_array[idx]));
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTenantMdsTimer::process_with_tablet_(ObTablet &tablet)
{
  #define PRINT_WRAPPER KR(ret), KPC(this), K(tablet_oldest_scn), K(ls_id), K(tablet_id)
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const common::ObTabletID &tablet_id = tablet.get_tablet_id();
  share::SCN tablet_oldest_scn;
  MDS_TG(10_ms);
  if (MDS_FAIL(get_tablet_oldest_scn_(tablet, tablet_oldest_scn))) {
    MDS_LOG_GC(WARN, "fail to get tablet oldest scn");
  } else if (MDS_FAIL(try_recycle_mds_table_(tablet, tablet_oldest_scn))) {
    MDS_LOG_GC(WARN, "fail to recycle mds table");
  } else if (MDS_FAIL(try_gc_mds_table_(tablet))) {
    if (OB_EAGAIN != ret) {
      MDS_LOG_GC(WARN, "fail to gc mds table");
    } else {
      MDS_LOG_GC(TRACE, "try gc mds table need do again later");
    }
  } else {
    MDS_LOG_GC(INFO, "success do try gc mds table");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTenantMdsTimer::get_tablet_oldest_scn_(ObTablet &tablet, share::SCN &oldest_scn)
{
  #define PRINT_WRAPPER KR(ret), K(ls_id), K(tablet_id), K(oldest_scn), KPC(this)
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const common::ObTabletID &tablet_id = tablet.get_tablet_id();
  MDS_TG(5_ms);
  if (OB_ISNULL(MTL(ObTenantMetaMemMgr*))) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG_GC(ERROR, "MTL ObTenantMetaMemMgr is NULL");
  } else if (MDS_FAIL(MTL(ObTenantMetaMemMgr*)->get_min_mds_ckpt_scn(ObTabletMapKey(ls_id, tablet_id),
                                                                     oldest_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      MDS_LOG_GC(WARN, "get_min_mds_ckpt_scn meet OB_ENTRY_NOT_EXIST");
    } else {
      MDS_LOG_GC(WARN, "fail to get oldest tablet min_mds_ckpt_scn");
    }
    oldest_scn = SCN::min_scn();// means can not recycle any node
  } else if (oldest_scn.is_max() || !oldest_scn.is_valid()) {
    oldest_scn = SCN::min_scn();// means can not recycle any node
  }
  MDS_LOG_GC(DEBUG, "get tablet oldest scn");
  return ret;
  #undef PRINT_WRAPPER
}

int ObTenantMdsTimer::try_recycle_mds_table_(ObTablet &tablet,
                                             const share::SCN &tablet_oldest_scn)
{
  #define PRINT_WRAPPER KR(ret), K(tablet.get_tablet_meta().tablet_id_), K(tablet_oldest_scn), KPC(this)
  int ret = OB_SUCCESS;
  const ObTabletPointerHandle &pointer_handle = tablet.get_pointer_handle();
  ObTabletPointer *tablet_pointer = pointer_handle.get_resource_ptr();
  MDS_TG(5_ms);
  if (OB_ISNULL(tablet_pointer)) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG_GC(ERROR, "down cast to tablet pointer failed");
  } else if (MDS_FAIL(tablet_pointer->try_release_mds_nodes_below(tablet_oldest_scn))) {
    MDS_LOG_GC(WARN, "fail to release mds nodes");
  } else {
    MDS_LOG_GC(DEBUG, "success to release mds nodes");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTenantMdsTimer::try_gc_mds_table_(ObTablet &tablet)
{
  #define PRINT_WRAPPER KR(ret), K(tablet.get_tablet_meta().tablet_id_), KPC(this)
  int ret = OB_SUCCESS;
  const ObTabletPointerHandle &pointer_handle = tablet.get_pointer_handle();
  ObTabletPointer *tablet_pointer = pointer_handle.get_resource_ptr();
  MDS_TG(5_ms);
  if (OB_ISNULL(tablet_pointer)) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG_GC(ERROR, "down cast to tablet pointer failed");
  } else if (MDS_FAIL(tablet_pointer->try_gc_mds_table())) {
    if (OB_EAGAIN != ret) {
      MDS_LOG_GC(WARN, "try gc mds table failed");
    }
  } else {
    MDS_LOG_GC(DEBUG, "success to release mds nodes");
  }
  return ret;
  #undef PRINT_WRAPPER
}

void ObTenantMdsService::record_alloc_backtrace(void *obj,
                                                const char *tag,
                                                const char *data_type,
                                                const char *alloc_file,
                                                const char *alloc_func,
                                                const int64_t line)
{
#ifdef ENABLE_DEBUG_MDS_MEM_LEAK
  int ret = OB_SUCCESS;
  if (OB_FAIL(memory_leak_debug_map_.insert(ObIntWarp((int64_t)obj),
                                            ObMdsMemoryLeakDebugInfo(tag,
                                                                     TAG_SIZE,
                                                                     data_type,
                                                                     alloc_file,
                                                                     alloc_func,
                                                                     line)))) {
    MDS_LOG(WARN, "fail to insert lbt to map", KR(ret), KP(obj), K(data_type), K(alloc_file), K(alloc_func), K(line));
  }
#else
  UNUSED(obj);
  UNUSED(tag);
  UNUSED(data_type);
  UNUSED(alloc_file);
  UNUSED(alloc_func);
  UNUSED(line);
#endif
}

void ObTenantMdsService::erase_alloc_backtrace(void *obj)
{
#ifdef ENABLE_DEBUG_MDS_MEM_LEAK
  int ret = OB_SUCCESS;
  if (OB_FAIL(memory_leak_debug_map_.erase(ObIntWarp((int64_t)obj)))) {
    MDS_LOG(WARN, "fail to erase record from map", KR(ret), KP(obj));
  }
#else
  UNUSED(obj);
#endif
}

void ObTenantMdsService::dump_map_holding_item(int64_t check_alive_time_threshold)
{
  int ret = OB_SUCCESS;
  int64_t scan_cnt = 0;
  auto op = [&scan_cnt, check_alive_time_threshold](const ObIntWarp &obj_wrapper,
                                                    ObMdsMemoryLeakDebugInfo &debug_info) {
    void *obj = (void *)(int64_t)obj_wrapper.get_value();
    ++scan_cnt;
    if (ObTimeUtility::fast_current_time() - debug_info.alloc_ts_ >= check_alive_time_threshold) {
      MDS_LOG(INFO, "print item alloc backtrace",
                    KP(obj), K(debug_info), K(ObTimeLiteralPrettyPrinter(check_alive_time_threshold)));
    }
    return true;
  };
  if (OB_FAIL(memory_leak_debug_map_.for_each(op))) {
    MDS_LOG(WARN, "fail to do for_each", KR(ret));
  } else {
    MDS_LOG(INFO, "finish scan map holding items", K(scan_cnt));
  }
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase
