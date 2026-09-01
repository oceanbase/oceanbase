// owner: shouju.zyp
// owner group: log

/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// Integration tests for the PALF async-write path.
//
// The integrated runtime routes a user-tenant PALF through this pipeline:
//
//   LogAsyncIOWorker::dispatch_task_ -> IAsyncPalfIOCtx::enqueue_task
//     -> AsyncPalfIOCtx::drive_write -> LogStorage::async_pwrite
//     -> LogBlockMgr::aio_write
//     -> IOManager AIO -> LogAsyncIOCallback::inner_process
//     -> IAsyncPalfIOCtx::on_aio_complete
//     -> the next worker drive publishes the persisted prefix and callbacks
//
// These cases cover routing, block preparation, AIO completion and publication,
// unregister draining, and unaligned-tail prefix restoration on real storage.

#define private public
#include "env/ob_simple_log_cluster_env.h"
#include "logservice/palf/log_async_io_worker.h"
#include "logservice/palf/log_async_palf_ctx.h"
#include "logservice/palf/log_storage.h"
#include "logservice/palf/palf_env_impl.h"
#undef private

const std::string TEST_NAME = "async_write";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterAsyncWrite : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterAsyncWrite() : ObSimpleLogClusterTestEnv()
  {}
  void SetUp() override
  {
    ObSimpleLogClusterTestEnv::SetUp();
    if (!async_io_enabled_) {
      for (ObISimpleLogServer *base_server : get_cluster()) {
        ObSimpleLogServer *server = static_cast<ObSimpleLogServer *>(base_server);
        ASSERT_TRUE(NULL != server);
        server->set_enable_async_io(true);
      }
      ASSERT_EQ(OB_SUCCESS, restart_paxos_groups());
      async_io_enabled_ = true;
    }
  }

private:
  static bool async_io_enabled_;
};

class TestFrontBarrierTask : public LogIOTask
{
public:
  TestFrontBarrierTask(const int64_t palf_id,
                       const int64_t palf_epoch,
                       bool *executed,
                       bool *freed)
      : LogIOTask(palf_id, palf_epoch), executed_(executed), freed_(freed)
  {}
  ~TestFrontBarrierTask() override
  {}

private:
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final
  {
    UNUSED(tg_id);
    UNUSED(guard);
    if (NULL != executed_) {
      *executed_ = true;
    }
    return OB_SUCCESS;
  }
  int after_consume_(IPalfHandleImplGuard &guard) override final
  {
    UNUSED(guard);
    return OB_SUCCESS;
  }
  LogIOTaskType get_io_task_type_() const override final
  {
    return LogIOTaskType::PURGE_THROTTLING_TYPE;
  }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final
  {
    UNUSED(palf_env_impl);
    if (NULL != freed_) {
      *freed_ = true;
    }
    this->~TestFrontBarrierTask();
    ob_free(this);
  }
  int64_t get_io_size_() const override final
  {
    return 0;
  }
  bool need_purge_throttling_() const override final
  {
    return false;
  }

private:
  bool *executed_;
  bool *freed_;
  DISALLOW_COPY_AND_ASSIGN(TestFrontBarrierTask);
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;
bool TestObSimpleLogClusterAsyncWrite::async_io_enabled_ = false;

// Test helper for routing counters and per-PALF ctx lookup. Private runtime
// members are visible here through the private-to-public test wrapper.
static LogAsyncIOWorker *get_async_worker(PalfHandleImplGuard &leader)
{
  LogAsyncIOWorker *worker = NULL;
  if (NULL != leader.palf_handle_impl_) {
    LogIOWorkerBase *submitter =
        leader.palf_handle_impl_->log_engine_.io_task_submitter_;
    if (NULL != submitter) {
      worker = static_cast<LogAsyncIOWorker *>(submitter);
    }
  }
  return worker;
}

// Inject a fault into the serialized bytes of a group entry, then deserialize
// the corrupted bytes back into |entry| so it can be pwritten to disk. Mirrors
// the helper of the same name in test_ob_simple_log_data_intergrity.cpp.
typedef ObFunction<void(char *buf)> DataFaultInject;
static int make_log_group_entry_partial_error(LogGroupEntry &entry,
                                              char *&output_buf,
                                              DataFaultInject &inject)
{
  int ret = OB_SUCCESS;
  if (!entry.is_valid() || !inject.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    return ret;
  }
  int64_t pos = 0;
  char *serialize_buf = reinterpret_cast<char *>(ob_malloc(entry.get_serialize_size(), "MitTest"));
  if (NULL == serialize_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(entry.serialize(serialize_buf, entry.get_serialize_size(), pos))) {
    PALF_LOG(ERROR, "serialize failed", K(ret), K(entry));
  } else {
    inject(serialize_buf);
    pos = 0;
    entry.deserialize(serialize_buf, entry.get_serialize_size(), pos);
    entry.buf_ = serialize_buf + entry.header_.get_serialize_size();
    output_buf = serialize_buf;
  }
  if (OB_FAIL(ret) && NULL != serialize_buf) {
    ob_free(serialize_buf);
  }
  return ret;
}

// pwrite a (possibly corrupted) group entry straight to the on-disk block file
// that physically holds |lsn| -- NOT necessarily the currently writable block.
// Unlike pwrite_one_log_by_log_storage in the data-integrity test, this helper
// can target the block that physically contains the supplied LSN.
static int pwrite_entry_to_block_of_lsn(PalfHandleImplGuard &leader,
                                        const LogGroupEntry &entry,
                                        const LSN &lsn)
{
  int ret = OB_SUCCESS;
  LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
  const int dir_fd = log_storage->block_mgr_.dir_fd_;
  const block_id_t target_block_id = lsn_2_block(lsn, log_storage->logical_block_size_);
  const offset_t write_offset = log_storage->get_phy_offset_(lsn);
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  block_id_to_string(target_block_id, block_path, OB_MAX_FILE_NAME_LENGTH);
  int block_fd = -1;
  int64_t pos = 0;
  const int64_t ser_size = entry.get_serialize_size();
  char *serialize_buf = reinterpret_cast<char *>(ob_malloc(ser_size, "MitTest"));
  if (NULL == serialize_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (-1 == (block_fd = ::openat(dir_fd, block_path, O_WRONLY))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "openat failed", K(ret), K(block_path), K(target_block_id));
  } else if (OB_FAIL(entry.serialize(serialize_buf, ser_size, pos))) {
    PALF_LOG(ERROR, "serialize failed", K(ret), K(entry));
  } else if (0 >= pwrite(block_fd, serialize_buf, ser_size, write_offset)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "pwrite failed", K(ret), K(block_path), K(write_offset), K(entry));
  } else {
    PALF_LOG(INFO, "pwrite_entry_to_block_of_lsn done", K(target_block_id),
             K(write_offset), K(lsn), K(ser_size));
  }
  if (-1 != block_fd) {
    ::close(block_fd);
  }
  if (NULL != serialize_buf) {
    ob_free(serialize_buf);
  }
  return ret;
}

// Physically pread |size| bytes at |lsn|'s aligned-sector offset straight from
// the on-disk block file (bypassing the page/log cache), mirroring the openat
// approach of pwrite_entry_to_block_of_lsn. The tail-prefix test uses it to
// verify that the first unaligned async write preserves committed bytes before
// the logical tail.
static int pread_phys_from_block_of_lsn(PalfHandleImplGuard &leader,
                                        const LSN &sector_begin_lsn,
                                        char *out_buf,
                                        const int64_t size)
{
  int ret = OB_SUCCESS;
  LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
  const int dir_fd = log_storage->block_mgr_.dir_fd_;
  const block_id_t target_block_id = lsn_2_block(sector_begin_lsn, log_storage->logical_block_size_);
  const offset_t read_offset = log_storage->get_phy_offset_(sector_begin_lsn);
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  block_id_to_string(target_block_id, block_path, OB_MAX_FILE_NAME_LENGTH);
  int block_fd = -1;
  if (-1 == (block_fd = ::openat(dir_fd, block_path, O_RDONLY))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "openat failed", K(ret), K(block_path), K(target_block_id));
  } else if (size != pread(block_fd, out_buf, size, read_offset)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "pread failed", K(ret), K(block_path), K(read_offset), K(size));
  } else {
    PALF_LOG(INFO, "pread_phys_from_block_of_lsn done", K(target_block_id),
             K(read_offset), K(sector_begin_lsn), K(size));
  }
  if (-1 != block_fd) {
    ::close(block_fd);
  }
  return ret;
}

// Resolve the AsyncPalfIOCtx registered for palf_id and keep its entry alive
// through entry_guard while the test inspects planner and publication state.
static AsyncPalfIOCtx *get_async_ctx(PalfHandleImplGuard &leader,
                                     const int64_t palf_id,
                                     AsyncPalfIOCtxEntryGuard &entry_guard)
{
  AsyncPalfIOCtx *ctx = NULL;
  LogAsyncIOWorker *worker = get_async_worker(leader);
  if (NULL != worker
      && OB_SUCCESS == worker->get_entry_guard_(
                              palf_id, false /* allow_unregistering */, entry_guard)) {
    ctx = static_cast<AsyncPalfIOCtx *>(entry_guard.get_ctx());
  }
  return ctx;
}

// ===========================================================================
// front_control_barrier_before_planner_ready: migration/rebuild 可能在第一条
// flush task 前入队 barrier, 此时 planner 还没有有效 tail. barrier 必须先被
// 暂存并执行, 不能等待 flush task 刷新 planner, 否则 worker 会反复卡在同一队首.
// ===========================================================================
// TODO(shouju.zyp): The live async worker may drive this ctx concurrently with
// the direct drive_write() below, while plain bool flags also control task
// cleanup. Replace this with an isolated ctx or worker-only driving and a
// synchronized completion state before extending this case.
TEST_F(TestObSimpleLogClusterAsyncWrite, front_control_barrier_before_planner_ready)
{
  SET_CASE_LOG_FILE(TEST_NAME, "front_control_barrier_before_planner_ready");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t palf_epoch = -1;
  bool executed = false;
  bool freed = false;
  int64_t next_drive_interval_us = INT64_MAX;
  PalfHandleImplGuard leader;
  AsyncPalfIOCtxEntryGuard entry_guard;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_palf_epoch(palf_epoch));
  AsyncPalfIOCtx *ctx = get_async_ctx(leader, id, entry_guard);
  ASSERT_TRUE(NULL != ctx);

  void *buf = ob_malloc(sizeof(TestFrontBarrierTask), "MitTest");
  ASSERT_TRUE(NULL != buf);
  TestFrontBarrierTask *task = new (buf) TestFrontBarrierTask(id, palf_epoch, &executed, &freed);
  ctx->planner_.invalidate_plan_state();
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(task->get_io_task_type()));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_TRUE(executed);
  EXPECT_EQ(0, next_drive_interval_us);
  EXPECT_EQ(0, ctx->task_queue_.get_total());
  EXPECT_TRUE(NULL == ctx->control_barrier_task_);
  if (!freed) {
    task->free_this(NULL);
  }
  PALF_LOG(INFO, "end front_control_barrier_before_planner_ready", K(id));
}

// ===========================================================================
// async_main_path: a user-tenant non-SYS PALF must dispatch flush tasks to its
// registered async owner instead of falling back to a legacy worker.
// ===========================================================================
TEST_F(TestObSimpleLogClusterAsyncWrite, async_main_path)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_main_path");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ASSERT_NE(1, id) << "async path is skipped for SYS LS (palf_id == 1)";
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  LogAsyncIOWorker *worker = get_async_worker(leader);
  ASSERT_TRUE(NULL != worker);

  const int64_t task0 = worker->get_dispatched_task_count_();

  // A burst is sufficient because this case validates routing. Dedicated cases
  // below cover completion and publication.
  const int64_t kCount = 50;
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, kCount, leader_idx, lsn_array, scn_array));
  ASSERT_FALSE(lsn_array.empty());

  // Give the worker a moment to drain its queue into the async pipeline.
  const int64_t kWaitUs = 3 * 1000 * 1000;
  const int64_t start = ObTimeUtility::current_time();
  while (worker->get_dispatched_task_count_() <= task0
         && ObTimeUtility::current_time() - start < kWaitUs) {
    usleep(50 * 1000);
  }

  // The PALF write route is fixed when its async context is registered. A
  // routing regression leaves this counter unchanged.
  const int64_t task1 = worker->get_dispatched_task_count_();
  EXPECT_GT(task1, task0) << "no flush task dispatched into async pipeline "
                             "(worker-routing regression)";
  // Every accepted task must find its registered context.
  EXPECT_EQ(0, worker->get_dropped_task_count_());
  PALF_LOG(INFO, "end async_main_path", K(id), K(task0), K(task1));
}

// ===========================================================================
// delete_while_inflight: 大批量 submit 后立即删除 PALF. unregister 必须等
// task credit、planner fragment、AIO 以及 callback/dispatch pin 全部排空,
// 不能提前释放仍持有异步状态的 ctx.
// ===========================================================================
TEST_F(TestObSimpleLogClusterAsyncWrite, delete_while_inflight)
{
  SET_CASE_LOG_FILE(TEST_NAME, "delete_while_inflight");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  {
    PalfHandleImplGuard leader;
    ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    // 大批量提交后不等待 commit, 尽量让删除发生在 task 仍处于 queue、plan、
    // submit 或等待 publish 的阶段.
    std::vector<LSN> lsn_array;
    std::vector<SCN> scn_array;
    ASSERT_EQ(OB_SUCCESS, submit_log(leader, 500, leader_idx, lsn_array, scn_array));
  }
  // Releasing the leader guard lets delete enter unregister, which must drain
  // the complete async pipeline before freeing the ctx.
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  PALF_LOG(INFO, "end delete_while_inflight", K(id));
}

// ===========================================================================
// async_carry_recovery_preserves_lead_bytes:
// O_DIRECT 会按 4K 页写盘. 当持久化尾部没有 4K 对齐时, 重启后的首个异步写
// 会覆盖 [align_down(tail), align_up(end)) 区间. planner reset 必须先读取磁盘
// 尾页, 再把 [align_down(tail), tail) 前缀填回 group buffer; 否则补零会覆盖
// 已经持久化的日志数据.
//
// 验证步骤:
//   1) 写入多条非 4K 大小的 group log, 得到未对齐的持久化 tail;
//   2) 从磁盘保存 [align_down(tail), tail) 原始字节和已有 entry checksum;
//   3) 重启, 触发 planner reset 恢复尾页有效前缀;
//   4) 继续追加并完成跨页 DIO 写;
//   5) 重读同一尾页并校验前缀字节及全部旧 entry checksum 均未变化.
// ===========================================================================
TEST_F(TestObSimpleLogClusterAsyncWrite, async_carry_recovery_preserves_lead_bytes)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_carry_recovery_preserves_lead_bytes");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  ASSERT_NE(1, id) << "async path is skipped for SYS LS (palf_id == 1)";
  int64_t leader_idx = 0;
  LSN pre_tail;                 // 重启前未按 4K 对齐的持久化 tail
  LSN sector_begin;            // align_down(pre_tail, 4K), 尾页页首
  int64_t lead = 0;            // pre_tail - sector_begin, 尾页有效前缀长度
  char before_sector[LOG_DIO_ALIGN_SIZE];
  // 保存重启前所有 entry checksum, 用于证明跨页写没有修改 tail 之前的数据.
  std::vector<int64_t> data_checksums;
  std::vector<int64_t> accum_checksums;
  std::vector<LSN>     entry_lsns;
  {
    PalfHandleImplGuard leader;
    ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

    // 用多条小日志构造未按 4K 对齐的持久化 tail.
    ASSERT_EQ(OB_SUCCESS, submit_log(leader, 7, id, 1500));
    pre_tail = leader.get_palf_handle_impl()->get_max_lsn();
    ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, pre_tail));
    ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(pre_tail, leader));

    lead = static_cast<int64_t>(pre_tail.val_ % LOG_DIO_ALIGN_SIZE);
    // 若刚好落在 4K 边界, 再写一条小日志以构造真实的尾页有效前缀.
    if (0 == lead) {
      ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 777));
      pre_tail = leader.get_palf_handle_impl()->get_max_lsn();
      ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, pre_tail));
      ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(pre_tail, leader));
      lead = static_cast<int64_t>(pre_tail.val_ % LOG_DIO_ALIGN_SIZE);
    }
    ASSERT_GT(lead, 0) << "could not produce a non-4K-aligned tail to carry";
    sector_begin = LSN(pre_tail.val_ - lead);

    // 绕过 cache, 直接保存磁盘尾页原始字节.
    ASSERT_EQ(OB_SUCCESS,
        pread_phys_from_block_of_lsn(leader, sector_begin, before_sector, LOG_DIO_ALIGN_SIZE));

    // 保存 tail 前所有 entry checksum, 确认尾页前缀属于必须保留的有效日志.
    PalfGroupBufferIterator iterator;
    ASSERT_EQ(OB_SUCCESS,
        leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(LSN(0), iterator));
    LogGroupEntry entry;
    LSN curr_lsn;
    while (OB_SUCCESS == iterator.next()) {
      ASSERT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
      int64_t dchk = 0;
      ASSERT_TRUE(entry.check_integrity(dchk)) << "pre-restart entry invalid";
      data_checksums.push_back(dchk);
      accum_checksums.push_back(entry.get_header().get_accum_checksum());
      entry_lsns.push_back(curr_lsn);
    }
    ASSERT_FALSE(entry_lsns.empty());
    PALF_LOG(INFO, "captured pre-restart prefix", K(id), K(pre_tail),
             K(sector_begin), K(lead), "entry_cnt", static_cast<int64_t>(entry_lsns.size()));
  }

  // 重启后首个异步写会覆盖整个尾页, planner 必须在 drive 前从磁盘恢复
  // [sector_begin, pre_tail) 的有效前缀.
  ASSERT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard leader;
    ASSERT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    // The restart preserves the PALF tail and restores its valid page prefix.
    const LSN restart_tail = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(restart_tail, pre_tail) << "tail moved across a clean restart";

    // 从 pre_tail 继续追加, 触发覆盖 [sector_begin, ...) 的首个跨页 DIO 写.
    std::vector<LSN> lsn_array;
    std::vector<SCN> scn_array;
    ASSERT_EQ(OB_SUCCESS, submit_log(leader, 5, 1200, id, lsn_array, scn_array));
    const LSN new_max = leader.get_palf_handle_impl()->get_max_lsn();
    ASSERT_GT(new_max, pre_tail);
    ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, new_max));
    ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(new_max, leader));

    AsyncPalfIOCtxEntryGuard entry_guard;
    AsyncPalfIOCtx *ctx = get_async_ctx(leader, id, entry_guard);
    ASSERT_TRUE(NULL != ctx);
    PlannerStatus status;
    ctx->planner_.get_status(status);
    EXPECT_EQ(0, status.get_pending_task_count())
        << "pending task queue is not drained after a healthy restart write";
    EXPECT_EQ(0, status.get_active_fragment_count())
        << "active fragments remain after a healthy restart write";
    EXPECT_FALSE(status.has_pending_source())
        << "pending source remains after a healthy restart write";
    EXPECT_LE(new_max, status.get_persisted_lsn())
        << "async ctx did not persist the healthy restart write";

    // 再次直读磁盘尾页, 校验 pre_tail 之前的前缀字节完全不变.
    char after_sector[LOG_DIO_ALIGN_SIZE];
    ASSERT_EQ(OB_SUCCESS,
        pread_phys_from_block_of_lsn(leader, sector_begin, after_sector, LOG_DIO_ALIGN_SIZE));
    EXPECT_EQ(0, MEMCMP(before_sector, after_sector, lead))
        << "lead bytes [sector_begin, pre_tail) were clobbered by the first "
           "cross-sector async write (carry not primed -> data corruption)";

    // 重放整个旧前缀, 确认每条 entry 仍可解析且 checksum 一致.
    PalfGroupBufferIterator iterator;
    ASSERT_EQ(OB_SUCCESS,
        leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(LSN(0), iterator));
    LogGroupEntry entry;
    LSN curr_lsn;
    size_t idx = 0;
    while (idx < entry_lsns.size() && OB_SUCCESS == iterator.next()) {
      ASSERT_EQ(OB_SUCCESS, iterator.get_entry(entry, curr_lsn));
      EXPECT_EQ(entry_lsns[idx], curr_lsn) << "entry " << idx << " lsn moved";
      int64_t dchk = 0;
      ASSERT_TRUE(entry.check_integrity(dchk))
          << "entry " << idx << " no longer parses after cross-sector write";
      EXPECT_EQ(data_checksums[idx], dchk)
          << "entry " << idx << " data checksum changed (lead corruption)";
      EXPECT_EQ(accum_checksums[idx], entry.get_header().get_accum_checksum())
          << "entry " << idx << " accum checksum changed";
      ++idx;
    }
    EXPECT_EQ(idx, entry_lsns.size())
        << "fewer original entries survived than were written";
  }
  PALF_LOG(INFO, "end async_carry_recovery_preserves_lead_bytes", K(id),
           K(pre_tail), K(lead));
}

// ===========================================================================
// async_carry_recovery_keeps_other_ls_progress:
// 两个 PALF 共用一个 async worker. carry_id 使用未对齐 tail 验证重启后的
// 尾页前缀恢复和跨页写; good_id 同时继续追加, 验证 carry_id 的恢复过程不会
// 阻塞共享 worker 上的其他日志流.
// ===========================================================================
TEST_F(TestObSimpleLogClusterAsyncWrite, async_carry_recovery_keeps_other_ls_progress)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_carry_recovery_keeps_other_ls_progress");
  OB_LOGGER.set_log_level("INFO");
  const int64_t good_id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t carry_id = ATOMIC_AAF(&palf_id_, 1);
  ASSERT_NE(1, good_id);
  ASSERT_NE(1, carry_id);
  int64_t good_idx = 0;
  int64_t carry_idx = 0;
  LSN carry_pre_tail;
  {
    PalfHandleImplGuard good_leader;
    PalfHandleImplGuard carry_leader;
    ASSERT_EQ(OB_SUCCESS, create_paxos_group(good_id, good_idx, good_leader));
    ASSERT_EQ(OB_SUCCESS, create_paxos_group(carry_id, carry_idx, carry_leader));

    // Give carry_id a non-4K-aligned tail so reset must restore its page prefix.
    ASSERT_EQ(OB_SUCCESS, submit_log(carry_leader, 7, carry_id, 1500));
    carry_pre_tail = carry_leader.get_palf_handle_impl()->get_max_lsn();
    ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(carry_leader, carry_pre_tail));
    ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(carry_pre_tail, carry_leader));
    if (0 == carry_pre_tail.val_ % LOG_DIO_ALIGN_SIZE) {
      ASSERT_EQ(OB_SUCCESS, submit_log(carry_leader, 1, carry_id, 777));
      carry_pre_tail = carry_leader.get_palf_handle_impl()->get_max_lsn();
      ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(carry_leader, carry_pre_tail));
      ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(carry_pre_tail, carry_leader));
    }
    ASSERT_NE(0, carry_pre_tail.val_ % LOG_DIO_ALIGN_SIZE)
        << "could not produce a non-4K-aligned tail for the carry case";
  }

  ASSERT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard good_leader;
    PalfHandleImplGuard carry_leader;
    ASSERT_EQ(OB_SUCCESS, get_leader(good_id, good_leader, good_idx));
    ASSERT_EQ(OB_SUCCESS, get_leader(carry_id, carry_leader, carry_idx));

    AsyncPalfIOCtxEntryGuard carry_entry_guard;
    AsyncPalfIOCtx *carry_ctx = get_async_ctx(carry_leader, carry_id, carry_entry_guard);
    ASSERT_TRUE(NULL != carry_ctx);

    // Submit to carry_id first so its initial write exercises prefix restoration.
    std::vector<LSN> carry_lsn_array;
    std::vector<SCN> carry_scn_array;
    ASSERT_EQ(OB_SUCCESS,
        submit_log(carry_leader, 5, 1200, carry_id, carry_lsn_array, carry_scn_array));
    const LSN carry_new_max = carry_leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(carry_leader, carry_new_max));
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(carry_new_max, carry_leader));
    PlannerStatus carry_status;
    carry_ctx->planner_.get_status(carry_status);
    EXPECT_EQ(0, carry_status.get_pending_task_count())
        << "carry LS pending task queue is not drained after the cross-sector write";
    EXPECT_EQ(0, carry_status.get_active_fragment_count())
        << "carry LS active fragments remain after the cross-sector write";
    EXPECT_FALSE(carry_status.has_pending_source())
        << "carry LS pending source remains after the cross-sector write";
    EXPECT_LE(carry_new_max, carry_status.get_persisted_lsn())
        << "carry LS did not persist the post-restart cross-sector write";

    // The shared worker must still make progress on the GOOD LS: a fresh append
    // there flushes normally as well.
    AsyncPalfIOCtxEntryGuard good_entry_guard;
    AsyncPalfIOCtx *good_ctx = get_async_ctx(good_leader, good_id, good_entry_guard);
    ASSERT_TRUE(NULL != good_ctx);
    PlannerStatus good_status;
    good_ctx->planner_.get_status(good_status);
    EXPECT_EQ(0, good_status.get_active_fragment_count())
        << "good LS unexpectedly has active fragments before its fresh append";
    std::vector<LSN> good_lsn_array;
    std::vector<SCN> good_scn_array;
    ASSERT_EQ(OB_SUCCESS,
        submit_log(good_leader, 5, 1200, good_id, good_lsn_array, good_scn_array));
    const LSN good_new_max = good_leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(good_leader, good_new_max))
        << "shared async worker stalled -- good LS did not advance while the "
           "carry LS was active";
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(good_new_max, good_leader))
        << "good LS did not flush while carry LS was active";

  }
  PALF_LOG(INFO, "end async_carry_recovery_keeps_other_ls_progress",
           K(good_id), K(carry_id), K(carry_pre_tail));
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
