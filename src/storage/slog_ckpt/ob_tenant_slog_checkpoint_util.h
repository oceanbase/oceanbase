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

#ifndef OB_STORAGE_TENANT_SLOG_CHECKPOINT_UTIL_H_
#define OB_STORAGE_TENANT_SLOG_CHECKPOINT_UTIL_H_

#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_meta_obj_struct.h" // for ObMetaDiskAddr
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "observer/ob_startup_accel_task_handler.h"

namespace oceanbase
{

namespace omt
{
class ObTenant;
} // end namespace omt

namespace storage
{
class ObMetaBlockListHandle;
class ObLinkedMacroBlockItemWriter;

struct ObTabletStorageParam final
{
public:
  ObTabletStorageParam()
    : tablet_key_(),
      original_addr_()
  {
  }

  void reset()
  {
    tablet_key_.reset();
    original_addr_.reset();
  }

  bool is_valid() const
  {
    return tablet_key_.is_valid() && original_addr_.is_valid();
  }

  /// @brief set aligned to false if tablet is empty shell
  int64_t get_size(const bool aligned) const
  {
    int64_t size = original_addr_.size();
    return aligned ? common::ob_aligned_to2(size, DIO_READ_ALIGN_SIZE) : size;
  }

  TO_STRING_KV(K_(tablet_key), K_(original_addr));

public:
  ObTabletMapKey tablet_key_;
  ObMetaDiskAddr original_addr_;
};

struct ObTenantSlogCkptUtil
{
  static const int64_t DEFAULT_TABLET_ITEM_CNT = 16;

  /// @brief: comparator of floating members.
  /// @return: 1 if a > b; 0 if a == b; -1 if a < b
  static inline int precision_compare(const double &a, const double &b, const double &eps = 1e-9)
  {
    int cmp = -1;
    if (std::fabs(a - b) < eps) {
      cmp = 0;
    } else if (a > b) {
      cmp = 1;
    }
    return cmp;
  }

  /// @param total_tablet_size: sum of tablet size(aligned to 4K)
  static inline double cal_size_amplification(const int64_t &block_num, const int64_t &total_tablet_size)
  {
    /// check if @c total_tablet_size is aligned to 4K
    OB_ASSERT(total_tablet_size % DIO_READ_ALIGN_SIZE == 0);
    if (OB_UNLIKELY(0 == total_tablet_size || 0 == block_num))  {
      return 0;
    }
    double macro_block_size = block_num * OB_DEFAULT_MACRO_BLOCK_SIZE;
    return macro_block_size / total_tablet_size;
  }

  /// @brief: persist tablet and apply tablet into t3m.
  static int write_and_apply_tablet(
      const ObTabletStorageParam &storage_param,
      ObTenantMetaMemMgr &t3m,
      ObLSService &ls_service,
      ObTenantStorageMetaService &tsms, // for write slog
      ObArenaAllocator &allocator,
      bool &skipped);

  static int handle_old_version_tablet_for_compat(
      ObTenantMetaMemMgr &t3m,
      ObArenaAllocator &allocator,
      const ObTabletMapKey &tablet_key,
      const ObTablet &old_tablet,
      ObTabletHandle &new_tablet_handle);

  static int acquire_tmp_tablet_for_compat(
      ObTenantMetaMemMgr &t3m,
      const ObTabletMapKey &tablet_key,
      ObArenaAllocator &allocator,
      ObTabletHandle &handle);

  static int record_wait_gc_tablet(
        omt::ObTenant &tenant,
        ObTenantStorageMetaService &tsms,
        ObLinkedMacroBlockItemWriter &wait_gc_tablet_item_writer,
        blocksstable::MacroBlockId &wait_gc_tablet_entry,
        ObSlogCheckpointFdDispenser *fd_dispenser,
        const ObMemAttr &mem_attr);


  class DiskedTabletFilterOp final : public ObITabletFilterOp
  {
  public:
    int do_filter(const ObTabletResidentInfo &info, bool &is_skipped) override
    {
      int ret = OB_SUCCESS;
      is_skipped = false;
      if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet resident info is invalid", K(ret), K(info));
      } else if (info.addr_.is_none()) {
        ret = OB_NEED_RETRY; // (?)
        STORAGE_LOG(WARN,  "tablet addr is none", K(ret), K(info));
      } else if (info.addr_.is_memory()) {
        is_skipped = true;
        STORAGE_LOG(INFO, "skip in mem tablet", K(ret), K(info));
      }
      return ret;
    }
  };

  /// @brief not include empty shell tablet!!!
  class TabletDefragmentPicker final
  {
  public:
    struct MapValue final
    {
    public:
      MapValue(): total_occupied_(0) {}

      TO_STRING_KV(K_(tablet_storage_params), K_(total_occupied));

    public:
      ObSEArray<ObTabletStorageParam, DEFAULT_TABLET_ITEM_CNT> tablet_storage_params_;
      int64_t total_occupied_; // tablet size(aligned to 4KiB)
    };

    /**
    * NOTE: ObLinearMap will dynamically adjust its buckets, resulting in additional
    * memory allocation overhead. Moreover, the number of shared macro blocks can be
    * estimated based on the total number of tablets making the use of ObHashMap more
    * suitable here.
    */
    typedef common::hash::ObHashMap<MacroBlockId, MapValue*, common::hash::NoPthreadDefendMode> SharedMacroBlockMap;

    typedef common::hash::HashMapPair<MacroBlockId, MapValue*> EntryType;

  public:
    TabletDefragmentPicker(const ObMemAttr &mem_attr);
    ~TabletDefragmentPicker();
    int create(const int64_t &bucket_num)
    {
      return map_.create(bucket_num, mem_attr_);
    }
    int64_t block_num() const
    {
      return map_.size();
    }
    const int64_t &total_tablet_size() const
    {
      return total_tablet_size_;
    }

    /// @brief: pick the most suitable tablets for defragment.
    /// @param result[in/out]: the result of picking.
    /// @param size_amp_threshold: the minimum size amp for the picked shared macro block.
    /// @param tablet_size_threshold: the total size of tablets in @c result will not exceed this threshold;
    ///                               -1 indicates no limitation on the size of picked tablets.
    int pick_tablets_for_defragment(
        ObIArray<ObTabletStorageParam> &result,
        const double &size_amp_threshold,
        const int64_t &tablet_cnt_threshold,
        const int64_t tablet_size_threshold,
        int64_t &picked_tablet_size);

    int add_tablet(const ObTabletStorageParam &param);

  private:
    struct InnerPicker;

  private:
    DISALLOW_COPY_AND_ASSIGN(TabletDefragmentPicker);
    void reset_();
    int remove_(const MacroBlockId &block_id);

  private:
    ObArenaAllocator allocator_;
    const ObMemAttr mem_attr_;
    SharedMacroBlockMap map_;
    int64_t total_tablet_size_;
  };

  class MetaBlockListApplier final
  {
  public:
    MetaBlockListApplier(common::TCRWLock *lock,
                         ObMetaBlockListHandle *ls_block_handle,
                         ObMetaBlockListHandle *tablet_block_handle,
                         ObMetaBlockListHandle *wait_gc_tablet_block_handle);

    ~MetaBlockListApplier() = default;

    int is_valid() const;

    /// @brief: apply block lists into handles
    /// retry until allocate memory succeed
    int apply_from(const ObIArray<MacroBlockId> &ls_block_list,
                   const ObIArray<MacroBlockId> &tablet_block_list,
                   const ObIArray<MacroBlockId> &wait_gc_tablet_block_list);

    // disable copy and assign
    MetaBlockListApplier(const MetaBlockListApplier &) = delete;
    void operator=(const MetaBlockListApplier &) = delete;

    TO_STRING_KV(KP_(lock), KP_(ls_block_handle), KP_(tablet_block_handle), KP_(wait_gc_tablet_block_handle));
  private:
    common::TCRWLock *lock_; // protect all handles below
    ObMetaBlockListHandle *ls_block_handle_;
    ObMetaBlockListHandle *tablet_block_handle_;
    ObMetaBlockListHandle *wait_gc_tablet_block_handle_;
  };

  template<typename T>
  class OptionalVariable
  {
    static_assert(
      std::is_default_constructible_v<T>,
      "T must have a default constructor");
  public:
    explicit OptionalVariable(const T &null_ = T())
      : val_(null_),
        null_(null_)
    {
    }
    void set(const T &v)
    {
      val_ = v;
    }
    const T &get() const
    {
      return val_;
    }
    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      J_OBJ_START();
      if (val_ == null_) {
        databuff_printf(buf, buf_len, pos, "NULL");
      } else {
        J_KV(K_(val));
      }
      J_OBJ_END();
      return pos;
    }
    bool setted() const
    {
      return val_ != null_;
    }

  private:
    T val_;
    T null_;
  };

  class ParallelStartupTaskHandler final
  {
  public:
    class ITask : public observer::ObStartupAccelTask {
    public:
      ITask(ParallelStartupTaskHandler &hdl): is_inited_(false), hdl_(hdl) {}
      virtual ~ITask()
      {
        destroy_();
      }
      virtual int64_t to_string(char *buf, const int64_t buf_len) const override
      {
        return 0;
      }

    protected:
      void on_exec_error_(const int errcode)
      {
        hdl_.set_errcode_(errcode);
      }
      void on_exec_succeed_()
      {
        hdl_.inc_finished_task_();
      }
      void on_init_succeed_()
      {
        is_inited_ = true;
        hdl_.inc_inflight_task_cnt_();
      }

    private:
      void destroy_()
      {
        hdl_.dec_inflight_task_cnt_();
      }

    protected:
      bool is_inited_;

    private:
      /// By calling @c join(), ITask's lifecycle is ensured to be within @c hdl_.
      ParallelStartupTaskHandler &hdl_;
    };
  public:
    ParallelStartupTaskHandler();
    ~ParallelStartupTaskHandler() = default;

    /// @brief: add task into background task queue
    /// @param args...: arguments of TaskType::init()
    template<typename TaskType, typename ...TaskArgs>
    int add_task(TaskArgs&& ...args);

    /// @brief: wait all inflight tasks finish
    int wait();
    int64_t get_thread_cnt()
    {
      return startup_accel_task_hdl_->get_thread_cnt();
    }

  private:
    friend class ITask;

  private:
    DISALLOW_COPY_AND_ASSIGN(ParallelStartupTaskHandler); // disable copy and assign
    void set_errcode_(const int code)
    {
      ATOMIC_SET(&errcode_, code);
    }
    void inc_finished_task_()
    {
      ATOMIC_INC(&finished_task_cnt_);
    }
    void inc_inflight_task_cnt_()
    {
      ATOMIC_INC(&inflight_task_cnt_);
    }
    void dec_inflight_task_cnt_()
    {
      ATOMIC_DEC(&inflight_task_cnt_);
    }
    int get_errcode_()
    {
      return ATOMIC_LOAD(&errcode_);
    }
    int64_t get_inflight_task_cnt_()
    {
      return ATOMIC_LOAD(&inflight_task_cnt_);
    }
    int64_t get_finished_task_cnt_()
    {
      return ATOMIC_LOAD(&finished_task_cnt_);
    }

  private:
    observer::ObStartupAccelTaskHandler *startup_accel_task_hdl_;
    int errcode_; // ATOMIC_VAR
    int64_t inflight_task_cnt_; // ATOMIC_VAR
    int64_t finished_task_cnt_; // ATOMIC_VAR
    int64_t all_task_cnt_;
  };
};

template<typename TaskType, typename ...TaskArgs>
int ObTenantSlogCkptUtil::ParallelStartupTaskHandler::add_task(TaskArgs&& ...args)
{
  static_assert(std::is_base_of<ITask, TaskType>::value, "TaskType must be a subclass of ParallelStartupTaskHdl::ITask");
  /// @c startup_accel_task_hdl_ must be inited
  OB_ASSERT(nullptr != startup_accel_task_hdl_);
  OB_ASSERT(OB_INIT_TWICE == startup_accel_task_hdl_->init(observer::SERVER_ACCEL));

  int ret = OB_SUCCESS;
  TaskType *task = nullptr;
  if (OB_ISNULL(task = reinterpret_cast<TaskType*>(
      startup_accel_task_hdl_->get_task_allocator().alloc(sizeof(TaskType))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc task buf", K(ret));
  } else if (FALSE_IT(task = new(task) TaskType(*this))) {
  } else if (OB_FAIL(task->init(std::forward<TaskArgs>(args)...))) {
    STORAGE_LOG(WARN, "failed to init task", K(ret));
  }

  if (OB_SUCC(ret)) {
    OB_ASSERT(nullptr != task);
    STORAGE_LOG(INFO, "add task", KPC(task), K(get_inflight_task_cnt_()));
    bool need_retry = false;
    do {
      need_retry = false;
      if (OB_FAIL(get_errcode_())) {
        STORAGE_LOG(WARN, "some task has failed", K(ret), K(get_inflight_task_cnt_()));
      } else if (OB_FAIL(startup_accel_task_hdl_->push_task(task))) {
        if (OB_EAGAIN == ret) {
          STORAGE_LOG(INFO, "task queue is full, wait and retry", KPC(task), K(get_inflight_task_cnt_()));
          need_retry = true;
          ob_usleep(20_ms);
        } else {
          STORAGE_LOG(WARN, "failed to push task", K(ret), KPC(task), K(get_inflight_task_cnt_()));
        }
      } else {
        task = nullptr;
        ++all_task_cnt_;
      }
    } while (OB_FAIL(ret) && need_retry);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
    task->~TaskType();
    startup_accel_task_hdl_->get_task_allocator().free(task);
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
#endif // OB_STORAGE_TENANT_SLOG_CHECKPOINT_UTIL_H_
