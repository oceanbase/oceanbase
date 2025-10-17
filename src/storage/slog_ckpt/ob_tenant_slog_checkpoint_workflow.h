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

#ifndef OB_STORAGE_CKPT_TENANT_SLOG_CHECKPOINT_WORKFLOW_H_
#define OB_STORAGE_CKPT_TENANT_SLOG_CHECKPOINT_WORKFLOW_H_

#include "storage/slog/ob_storage_log.h" // for ObUpdateTabletLog
#include "storage/slog_ckpt/ob_tenant_slog_checkpoint_util.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"

namespace oceanbase
{
namespace omt
{
class ObTenant;
} // end namespace omt

namespace storage
{
struct ObLSCkptMember;
class ObTenantMetaMemMgr;
class ObLSService;
class ObServerStorageMetaService;
class ObTenantStorageMetaService;


struct ObTenantSlogCheckpointInfo final
{
public:
  ObTenantSlogCheckpointInfo()
      : last_defragment_time_us_(0),
        last_defragment_cost_us_(INT_MAX64),
        last_truncate_time_us_(0)
  {
  }

  bool is_valid() const
  {
    return last_defragment_time_us_ >= 0 &&
           last_defragment_cost_us_ >= 0 &&
           last_truncate_time_us_ >= 0;
  }

  TO_STRING_KV(KTIME(last_defragment_time_us_), K_(last_defragment_cost_us), KTIME(last_truncate_time_us_));

public:
  int64_t last_defragment_time_us_;

  int64_t last_defragment_cost_us_;

  int64_t last_truncate_time_us_;
};

class ObTenantSlogCheckpointWorkflow final
{
public:
  enum Type:uint8_t {
      NORMAL_SS = 0, // for Shared-Storage mode
      NORMAL_SN = 1, // for Shared-Nothing mode
      FORCE = 2, // for force triggered
      COMPAT_UPGRADE = 3, // for server compat upgrade
  };

public:
  /// @brief: execute ckpt workflow by specified type.
  /// @param type: type of workflow
  /// @param super_block_mutex: to make sure that supper block's write after read is atomic(provided by ObTenantCheckpointSlogHandler)
  /// @param mbl_applier: used for hold macro block ref cnt of macro block list(provided by ObTenantCheckpointSlogHandler)
  /// @param ckpt_info[optional]: should provided by ObTenantCheckpointSlogHandler if @c type is not COMPAT
  static int execute(const Type type, ObTenantCheckpointSlogHandler &ckpt_handler);

  static Type normal_type() {
      return GCTX.is_shared_storage_mode() ? Type::NORMAL_SS : Type::NORMAL_SN;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  struct Context
  {
  public:
    Context(lib::ObMutex &supper_block_mutex,
        ObTenantSlogCkptUtil::MetaBlockListApplier &mbl_applier,
        ObTenantSlogCheckpointInfo &ckpt_info);

    bool is_valid() const;

    DECLARE_TO_STRING;

    /// NOTE: To avoid too many redundant validity checks, all private methods
    /// assume that @c ctx_ is valid, the validity of @c ctx_ should be checked
    /// at outside.
    inline ObMemAttr get_mem_attr() const;
    inline bool ignore_this_block(const MacroBlockId &block_id) const;

  public:
    lib::ObMutex &supper_block_mutex_; // make sure that supper block's write after read is atomic
    ObTenantSlogCkptUtil::MetaBlockListApplier &mbl_applier_;
    ObTenantSlogCheckpointInfo &ckpt_info_;
    ObTenantMetaMemMgr &t3m_;
    omt::ObTenant &tenant_;
    ObLSService &ls_service_;
    ObServerStorageMetaService &server_smeta_svr_;
    ObTenantStorageMetaService &tenant_smeta_svr_;
  };

  // for inner trace log print(thread unsafe)
  class Tracer final
  {
  public:
    Tracer();
    ~Tracer() = default;
    // disable copy and assign
    Tracer(const Tracer&) = delete;
    void operator=(const Tracer&) = delete;

    void start()
    {
      start_time_.set(ObTimeUtility::fast_current_time());
    }
    void log_ckpt_finish(const ObTenantSlogCheckpointWorkflow &workflow) const;
    void record_size_amp_before_dfgt(const double size_amp)
    {
      size_amp_before_defragment_.set(size_amp);
    }
    void record_pick_tablets_cost(const int64_t time_cost_ms)
    {
      pick_tablets_cost_ms_.set(time_cost_ms);
    }
    void record_dfgt_tablets_size(const int64_t size)
    {
      defragment_tablets_size_.set(size);
    }
    void record_dfgt_tablets_cnt(const int64_t cnt)
    {
      defragment_tablets_cnt_.set(cnt);
    }
    void record_skipped_tablet_cnt(const int64_t cnt)
    {
      skipped_tablet_cnt_.set(cnt);
    }
    void record_defragment_end(const int ret)
    {
      defragment_ret_.set(ret);
      defragment_end_time_.set(ObTimeUtility::fast_current_time());
    }
    void record_super_block_before_truncate(const ObTenantSuperBlock &super_block)
    {
      SuperBlockAttr attr;
      attr.init(super_block);
      super_block_before_truncate_.set(attr);
    }
    void record_super_block_after_truncate(const ObTenantSuperBlock &super_block)
    {
      SuperBlockAttr attr;
      attr.init(super_block);
      super_block_after_truncate_.set(attr);
    }
    void record_truncate_end(const int ret)
    {
      truncate_ret_.set(ret);
      truncate_end_time_.set(ObTimeUtility::fast_current_time());
    }

  private:
    struct SuperBlockAttr final
    {
    public:
      SuperBlockAttr()
        : replay_start_point_(),
          ls_meta_entry_(),
          wait_gc_tablet_entry_(),
          min_file_id_(-1),
          max_file_id_(-1) {}
      ~SuperBlockAttr() = default;
      void init(const ObTenantSuperBlock &super_block)
      {
        replay_start_point_ = super_block.replay_start_point_;
        ls_meta_entry_ = super_block.ls_meta_entry_;
        if (super_block.wait_gc_tablet_entry_.is_valid()) {
            wait_gc_tablet_entry_ = super_block.wait_gc_tablet_entry_;
            min_file_id_ = super_block.min_file_id_;
            max_file_id_ = super_block.max_file_id_;
        }
      }
      bool operator==(const SuperBlockAttr &other) const
      {
        return replay_start_point_.file_id_ == other.replay_start_point_.file_id_ &&
               replay_start_point_.log_id_ == other.replay_start_point_.log_id_ &&
               replay_start_point_.offset_ == other.replay_start_point_.offset_ &&
               ls_meta_entry_ == other.ls_meta_entry_ &&
               wait_gc_tablet_entry_ == other.wait_gc_tablet_entry_ &&
               min_file_id_ == other.min_file_id_ &&
               max_file_id_ == other.max_file_id_;
      }

      TO_STRING_KV(
        K_(replay_start_point),
        K_(ls_meta_entry),
        K_(wait_gc_tablet_entry),
        K_(min_file_id),
        K_(max_file_id));

    public:
      common::ObLogCursor replay_start_point_;
      blocksstable::MacroBlockId ls_meta_entry_;
      blocksstable::MacroBlockId wait_gc_tablet_entry_;
      int64_t min_file_id_;
      int64_t max_file_id_;
    };

  private:
    typedef ObTenantSlogCkptUtil::OptionalVariable<int64_t> optional_int64_t;
    typedef ObTenantSlogCkptUtil::OptionalVariable<double> optional_double_t;
    typedef ObTenantSlogCkptUtil::OptionalVariable<int> optional_ret_t;
    typedef ObTenantSlogCkptUtil::OptionalVariable<SuperBlockAttr> optional_super_block_attr_t;

  private:
    void log_defragment_only_(const int64_t total_cost_time, const ObTenantSlogCheckpointWorkflow &workflow) const;

    void log_truncate_only_(const int64_t total_cost_time, const ObTenantSlogCheckpointWorkflow &workflow) const;

    void log_both_(const int64_t total_cost_time, const ObTenantSlogCheckpointWorkflow &workflow) const;

  private:
    optional_int64_t start_time_;

    /* --------- for defragment --------- */
    optional_int64_t defragment_end_time_;
    optional_ret_t defragment_ret_;
    optional_double_t size_amp_before_defragment_;
    optional_int64_t pick_tablets_cost_ms_;
    optional_int64_t defragment_tablets_size_;
    optional_int64_t defragment_tablets_cnt_;
    optional_int64_t skipped_tablet_cnt_;

    /* --------- for truncate --------- */
    optional_int64_t truncate_end_time_;
    optional_ret_t truncate_ret_;
    optional_super_block_attr_t super_block_before_truncate_;
    optional_super_block_attr_t super_block_after_truncate_;
  };

  class TabletDefragmentHelper final
  {
  public:
    TabletDefragmentHelper(Context &ctx, Tracer &tracer);

    /// @brief: do defragment work
    int do_defragment(const bool force_pick_all_tablets);
    /// @brief: do defragment work parallel(only used in COMPAT_UPGRADE)
    int do_defragment_parallel();

  private:
    static const int64_t MAX_DEFRAGMENT_INTERVAL = 30L * 60 * 1000 * 1000; // 30 mins by default
    static const int64_t MIN_DEFRAGMENT_INTERVAL = 10L * 60 * 1000 * 1000; // 10 mins by default
    static const int64_t INTERVAL_FACTOR = 30; // defragment interval = min(MAX_DEFRAGMENT_INTERVAL, last_cost_time * INTERVAL_FACTOR)
    /// @brief: indicate the maximum total size of tablets that can be picked during
    /// each defragmentation (tablets size is aligned to 4K). Only used in NORMAL mode;
    /// set to -1 if unlimited.
    static constexpr int64_t MAX_PICKED_TABLET_SIZE = 10 * OB_DEFAULT_MACRO_BLOCK_SIZE; // 20 MiB
    static constexpr int64_t MAX_PICKED_TABLET_CNT = 5000;
    static constexpr double SIZE_AMPLIFICATION_THRESHOLD = 2.15;
    static constexpr double MIN_SIZE_AMPLIFICATION = 1.15;

  private:
    typedef ObTenantSlogCkptUtil::ParallelStartupTaskHandler PSTHdl;
    typedef PSTHdl::ITask PITask;

  private:
    // for log print(thread unsafe)
    class ProgressPrinter
    {
    public:
      ProgressPrinter(
          const int64_t &_total,
          const int64_t &_print_interval = 1000)
          : total_(_total),
            print_interval_(_print_interval),
            current_(0)
      {
      }

      void increase();

      TO_STRING_KV(K_(total), K_(print_interval), K_(current));

    private:
      const int64_t total_;
      int64_t print_interval_;
      int64_t current_;
      char print_buf_[256];
    };

    class WriteTask final : public PITask
    {
    public:
      WriteTask(PSTHdl &hdl);
      ~WriteTask() = default;
      int init(const ObTabletStorageParam &param, const Context *ctx);
      int execute() override;
      int64_t to_string(char *buf, const int64_t buf_len) const override;

    private:
      ObTabletStorageParam storage_param_;
      const Context *ctx_;
      common::ObArenaAllocator allocator_;
    };

  private:
    int pick_tablets_(const bool force_pick_all_tablets);

    int check_if_required_(bool &need_defragment) const;

    /// @brief: only used in NORMAL mode. It will calculate the size amplification rate of
    /// each shared macro block token by tablet's 1st level meta.
    /// NOTE: skip ObSharedObjectReaderWriter::get_cur_shared_block
    int pick_tablets_by_size_amp_();

    int pick_all_tablets_();

    /// @brief: update checkpoint info
    int update_ckpt_info_() const;

  private:
    Context &ctx_;
    ObSEArray<ObTabletStorageParam, ObTenantSlogCkptUtil::DEFAULT_TABLET_ITEM_CNT> tablet_storage_params_;
    int64_t start_time_;
    Tracer &tracer_;
  };
  class SlogTruncateHelper final
  {
  public:
    SlogTruncateHelper(Context &ctx, Tracer &tracer);
    int do_truncate(const bool is_force);

  private:
    static const int64_t MAX_TRUNCATE_INTERVAL = 300L * 1000 * 1000; // 5 mins by default
    static const int64_t MIN_WRITE_CHECKPOINT_LOG_CNT = 1e5; // 100,000

  private:
    /// NOTE: To avoid too many redundant validity checks, all private methods
    /// assume that @c ctx_ is valid, the validity of @c ctx_ should be checked
    /// at outside.

    /// @brief update ckpt info
    int update_ckpt_info_() const;
    int get_slog_ckpt_cursor_(common::ObLogCursor &ckpt_cursor);
    int check_if_required_(
          const common::ObLogCursor &ckpt_cursor,
          const ObTenantSuperBlock &last_super_block,
          bool &need_truncate) const;
    int write_ls_item_(const ObLSCkptMember &ls_ckpt_member);
    int record_single_ls_meta_(
          ObLS &ls,
          ObSlogCheckpointFdDispenser *fd_dispenser);

    int record_ls_tablets_(
          ObLS &ls,
          MacroBlockId &tablet_meta_entry,
          ObSlogCheckpointFdDispenser *fd_dispenser);

    /// NOTE: @param tablet_handle will be reset if it's empty shell in SN mode.
    int record_single_tablet_(
          ObTabletHandle &tablet_handle,
          const int64_t ls_epoch,
          char (&slog_buf)[sizeof(ObUpdateTabletLog)]);

    /// @brief update tenant super block and remove useless slog(may abort if failed)
    int apply_truncate_result_(
          const common::ObLogCursor &ckpt_cursor,
          const MacroBlockId &ls_meta_entry,
          const MacroBlockId &wait_gc_tablet_entry,
          const ObSlogCheckpointFdDispenser &fd_dispenser);

  private:
    Context &ctx_;
    ObLinkedMacroBlockItemWriter ls_item_writer_;
    ObLinkedMacroBlockItemWriter tablet_item_writer_;
    ObLinkedMacroBlockItemWriter wait_gc_tablet_item_writer_;
    int64_t start_time_;
    Tracer &tracer_;
  };

private:
  ObTenantSlogCheckpointWorkflow(
      Type workflow_type,
      const Context &context)
      : type_(workflow_type),
        context_(context)
  {
  }

  ~ObTenantSlogCheckpointWorkflow() = default;

  int inner_execute_();

  // disable copy and assign
  ObTenantSlogCheckpointWorkflow(const ObTenantSlogCheckpointWorkflow&) = delete;
  void operator=(const ObTenantSlogCheckpointWorkflow&) = delete;

  bool is_valid_() const
  {
    return context_.is_valid();
  }
  bool pick_all_tablets_() const
  {
    return type_ == Type::FORCE ||
            type_ == Type::COMPAT_UPGRADE;
  }
  /// NOTE: In SS mode, slog checkpoint automatically triggered by the backend
  /// should skip tablet defragment.
  bool skip_defragment_() const;
  bool parallel_enabled_() const
  {
    return type_ == Type::COMPAT_UPGRADE;
  }
  bool force_truncate_slog_() const
  {
    return type_ == Type::COMPAT_UPGRADE ||
           type_ == Type::FORCE;
  }

  /// @brief: tablet defragment errcode can be ignored
  /// only errcode is OB_SERVER_OUTOF_DISK_SPACE or OB_ALLOCATE_MEMORY_FAILED and
  /// workflow type is not COMPAT_UPGRADE.
  bool can_ignore_errcode_(const int errcode)
  {
    OB_ASSERT(OB_SUCCESS != errcode);
    bool b_ret = false;
    if (COMPAT_UPGRADE != type_ &&
       (errcode == OB_SERVER_OUTOF_DISK_SPACE || errcode == OB_ALLOCATE_MEMORY_FAILED)) {
      b_ret = true;
    }
    return b_ret;
  }

private:
  const Type type_;
  Context context_;
};
} // end namespace storage
} // end namespace oceanbase



#endif // OB_STORAGE_CKPT_TENANT_SLOG_CHECKPOINT_WORKFLOW_H_