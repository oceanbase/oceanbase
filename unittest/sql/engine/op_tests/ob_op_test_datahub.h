/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_DATAHUB_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_DATAHUB_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_thread_cond.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace sql
{

// Use #define private public to access private members of PX classes
#define private public
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_sub_coord.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/px/ob_dfo.h"
#undef private

/**
 * @brief MockDtlChannel - Mock implementation of dtl::ObDtlChannel.
 *
 * This class inherits from dtl::ObDtlChannel and implements all pure virtual
 * methods to return OB_SUCCESS, effectively discarding all messages.
 * It is used in the op_test framework to mock the DTL channel for datahub
 * communication, allowing operators that use datahub (like Window Function,
 * Barrier, Join Filter) to run in a unit test environment without actual
 * PX infrastructure.
 */
class MockDtlChannel : public dtl::ObDtlChannel
{
public:
  MockDtlChannel()
      : dtl::ObDtlChannel(0,
                          common::ObAddr(common::ObAddr::IPV4, "127.0.0.1", 0),
                          dtl::ObDtlChannel::DtlChannelType::BASIC_CHANNEL)
  {}

  virtual ~MockDtlChannel() = default;

  /**
   * @brief Initialize the channel. Returns OB_SUCCESS.
   */
  virtual int init(dtl::ObDtlFlowControl *dfc = nullptr) override
  {
    UNUSED(dfc);
    return OB_SUCCESS;
  }

  /**
   * @brief Send a message. Discards the message and returns OB_SUCCESS.
   */
  virtual int send(const dtl::ObDtlMsg &msg, int64_t timeout_ts,
                   ObEvalCtx *eval_ctx = nullptr, bool is_eof = false) override
  {
    UNUSED(msg);
    UNUSED(timeout_ts);
    UNUSED(eval_ctx);
    UNUSED(is_eof);
    return OB_SUCCESS;
  }

  /**
   * @brief Flush the channel. Returns OB_SUCCESS.
   */
  virtual int flush(bool wait = true, bool wait_response = true) override
  {
    UNUSED(wait);
    UNUSED(wait_response);
    return OB_SUCCESS;
  }

  /**
   * @brief Feed up a buffer. Returns OB_SUCCESS.
   */
  virtual int feedup(dtl::ObDtlLinkedBuffer *&buffer) override
  {
    UNUSED(buffer);
    return OB_SUCCESS;
  }

  /**
   * @brief Attach a buffer. Returns OB_SUCCESS.
   */
  virtual int attach(dtl::ObDtlLinkedBuffer *&linked_buffer,
                     bool inc_recv_buf_cnt = true) override
  {
    UNUSED(linked_buffer);
    UNUSED(inc_recv_buf_cnt);
    return OB_SUCCESS;
  }

  /**
   * @brief Check if channel is empty. Always returns true.
   */
  virtual bool is_empty() const override
  {
    return true;
  }

  /**
   * @brief Process a message. Returns OB_SUCCESS.
   */
  virtual int process1(dtl::ObIDtlChannelProc *proc, int64_t timeout,
                       bool &last_row_in_buffer) override
  {
    UNUSED(proc);
    UNUSED(timeout);
    last_row_in_buffer = true;
    return OB_SUCCESS;
  }

  /**
   * @brief Send with a processor. Returns OB_SUCCESS.
   */
  virtual int send1(std::function<int(const dtl::ObDtlLinkedBuffer &buffer)> &proc,
                    int64_t timeout) override
  {
    UNUSED(proc);
    UNUSED(timeout);
    return OB_SUCCESS;
  }

  /**
   * @brief Set DFC index. No-op.
   */
  virtual void set_dfc_idx(int64_t idx) override
  {
    UNUSED(idx);
  }

  /**
   * @brief Clear response block. Returns OB_SUCCESS.
   */
  virtual int clear_response_block() override
  {
    return OB_SUCCESS;
  }

  /**
   * @brief Wait for response. Returns OB_SUCCESS.
   */
  virtual int wait_response() override
  {
    return OB_SUCCESS;
  }

  /**
   * @brief Clean receive list. Returns OB_SUCCESS.
   */
  virtual int clean_recv_list() override
  {
    return OB_SUCCESS;
  }

  /**
   * @brief Push buffer batch info. Returns OB_SUCCESS.
   */
  virtual int push_buffer_batch_info() override
  {
    return OB_SUCCESS;
  }
};

/**
 * @brief MockDatahubContext - Manages the mock environment for datahub testing.
 *
 * This class sets up the necessary mock infrastructure to enable operators
 * that use datahub (like Window Function with single_part_parallel mode,
 * Barrier synchronization) to run in unit tests without actual PX infrastructure.
 *
 * Key responsibilities:
 * 1. Create and initialize ObPxSqcHandler with ObPxSubCoord
 * 2. Set up MockDtlChannel for SQC-QC communication
 * 3. Pre-load whole message providers for datahub operations
 *
 * Usage:
 *   MockDatahubContext datahub_ctx;
 *   datahub_ctx.init(exec_ctx);
 *   datahub_ctx.register_barrier(op_id);
 *   // ... run operator tests ...
 *   datahub_ctx.destroy();
 */
class MockDatahubContext
{
public:
  MockDatahubContext()
      : allocator_("MockDatahub"),
        sqc_handler_(nullptr),
        sub_coord_(nullptr),
        mock_channel_(nullptr),
        inited_(false)
  {}

  ~MockDatahubContext()
  {
    destroy();
  }

  /**
   * @brief Initialize the mock datahub environment.
   *
   * This method:
   * 1. Creates ObPxSqcHandler and ObPxSubCoord with minimal configuration
   * 2. Sets up MockDtlChannel for SQC channel
   * 3. Initializes the sqc_proxy_'s msg_ready_cond_ to avoid blocking
   * 4. Registers the sqc_handler with exec_ctx
   *
   * @param exec_ctx The execution context to set up
   * @return OB_SUCCESS on success, error code otherwise
   */
  int init(ObExecContext &exec_ctx)
  {
    int ret = OB_SUCCESS;

    if (inited_) {
      ret = OB_INIT_TWICE;
      LOG_WARN("MockDatahubContext already initialized", K(ret));
    } else {
      // Step 1: Create MockDtlChannel
      mock_channel_ = static_cast<MockDtlChannel *>(
          allocator_.alloc(sizeof(MockDtlChannel)));
      if (OB_ISNULL(mock_channel_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate MockDtlChannel", K(ret));
      } else {
        new (mock_channel_) MockDtlChannel();
      }

      // Step 2: Configure ObPxSqcMeta with minimal settings
      if (OB_SUCC(ret)) {
        sqc_arg_.sqc_.set_execution_id(1);
        sqc_arg_.sqc_.set_qc_id(1);
        sqc_arg_.sqc_.set_sqc_id(0);
        sqc_arg_.sqc_.set_dfo_id(0);
        sqc_arg_.sqc_.set_task_count(1);

        common::ObAddr local_addr(common::ObAddr::IPV4, "127.0.0.1", 0);
        sqc_arg_.sqc_.set_exec_addr(local_addr);
        sqc_arg_.sqc_.set_qc_addr(local_addr);

        // Set MockDtlChannel as the SQC channel
        sqc_arg_.sqc_.sqc_channel_ = mock_channel_;
      }

      // Step 3: Create ObPxSubCoord using placement new
      // ObPxSubCoord constructor requires gctx_ and sqc_arg_
      if (OB_SUCC(ret)) {
        void *buf = allocator_.alloc(sizeof(ObPxSubCoord));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate ObPxSubCoord", K(ret));
        } else {
          // Use placement new with references to gctx_ and sqc_arg_
          sub_coord_ = new (buf) ObPxSubCoord(gctx_, sqc_arg_);
        }
      }

      // Step 4: Initialize sqc_proxy_'s msg_ready_cond_ to avoid blocking in wait_whole_msg
      // Note: We access private member via #define private public
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sub_coord_->sqc_ctx_.sqc_proxy_.msg_ready_cond_.init(
                common::ObWaitEventIds::DH_LOCAL_SYNC_COND_WAIT))) {
          LOG_WARN("failed to init msg_ready_cond", K(ret));
        }
      }

      // Step 5: Create ObPxSqcHandler
      if (OB_SUCC(ret)) {
        void *buf = allocator_.alloc(sizeof(ObPxSqcHandler));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate ObPxSqcHandler", K(ret));
        } else {
          sqc_handler_ = new (buf) ObPxSqcHandler();
          // Set sub_coord_ to sqc_handler_
          sqc_handler_->sub_coord_ = sub_coord_;
        }
      }

      // Step 6: Register sqc_handler with exec_ctx
      if (OB_SUCC(ret)) {
        exec_ctx.set_sqc_handler(sqc_handler_);
        inited_ = true;
      }
    }

    return ret;
  }

  /**
   * @brief Register a pre-filled whole message provider.
   *
   * This method creates a provider for the specified message type,
   * fills the whole message using the provided setup function,
   * and registers it with sqc_ctx.
   *
   * @tparam PieceMsg The piece message type
   * @tparam WholeMsg The whole message type
   * @param op_id The operator ID
   * @param msg_type The DTL message type
   * @param setup_fn Function to fill the whole message
   * @return OB_SUCCESS on success, error code otherwise
   */
  template <class PieceMsg, class WholeMsg>
  int register_whole_msg(uint64_t op_id,
                         dtl::ObDtlMsgType msg_type,
                         std::function<void(WholeMsg &)> setup_fn)
  {
    int ret = OB_SUCCESS;

    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("MockDatahubContext not initialized", K(ret));
    } else {
      // Create provider
      using ProviderType = ObWholeMsgProvider<PieceMsg, WholeMsg>;
      void *buf = allocator_.alloc(sizeof(ProviderType));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate provider", K(ret));
      } else {
        auto *provider = new (buf) ProviderType();

        // Initialize provider
        if (OB_FAIL(provider->init(op_id, msg_type))) {
          LOG_WARN("failed to init provider", K(ret), K(op_id), K(msg_type));
        } else {
          // Create and fill whole message
          WholeMsg whole_msg;
          setup_fn(whole_msg);

          // Pre-load the whole message
          if (OB_FAIL(provider->add_msg(whole_msg))) {
            LOG_WARN("failed to add whole message", K(ret));
          } else if (OB_FAIL(sub_coord_->sqc_ctx_.whole_msg_provider_list_.push_back(provider))) {
            LOG_WARN("failed to register provider", K(ret));
          }
        }
      }
    }

    return ret;
  }

  /**
   * @brief Convenience method to register a barrier whole message.
   *
   * This registers a pre-filled barrier whole message provider for
   * operators that use barrier synchronization.
   *
   * @param op_id The operator ID
   * @return OB_SUCCESS on success, error code otherwise
   */
  int register_barrier(uint64_t op_id)
  {
    return register_whole_msg<ObBarrierPieceMsg, ObBarrierWholeMsg>(
        op_id,
        dtl::ObDtlMsgType::DH_BARRIER_WHOLE_MSG,
        [](ObBarrierWholeMsg &msg) {
          // Barrier whole message doesn't need data
          UNUSED(msg);
        });
  }

  /**
   * @brief Clean up all resources.
   *
   * This method releases all allocated resources including
   * the sqc_handler, sub_coord, and mock_channel.
   */
  void destroy()
  {
    if (inited_) {
      // Reset provider list
      if (OB_NOT_NULL(sub_coord_)) {
        for (int i = 0; i < sub_coord_->sqc_ctx_.whole_msg_provider_list_.count(); ++i) {
          if (OB_NOT_NULL(sub_coord_->sqc_ctx_.whole_msg_provider_list_.at(i))) {
            sub_coord_->sqc_ctx_.whole_msg_provider_list_.at(i)->reset();
          }
        }
        sub_coord_->sqc_ctx_.whole_msg_provider_list_.reset();
      }

      // Destroy sub_coord_
      if (OB_NOT_NULL(sub_coord_)) {
        sub_coord_->~ObPxSubCoord();
        sub_coord_ = nullptr;
      }

      // Destroy sqc_handler_
      if (OB_NOT_NULL(sqc_handler_)) {
        sqc_handler_->~ObPxSqcHandler();
        sqc_handler_ = nullptr;
      }

      // Destroy mock_channel_
      if (OB_NOT_NULL(mock_channel_)) {
        mock_channel_->~MockDtlChannel();
        mock_channel_ = nullptr;
      }

      allocator_.reset();
      inited_ = false;
    }
  }

  /**
   * @brief Check if the context is initialized.
   */
  bool is_inited() const { return inited_; }

  /**
   * @brief Get the sqc_handler.
   */
  ObPxSqcHandler *get_sqc_handler() { return sqc_handler_; }

  /**
   * @brief Get the sqc_ctx.
   */
  ObSqcCtx *get_sqc_ctx()
  {
    return OB_NOT_NULL(sub_coord_) ? &sub_coord_->sqc_ctx_ : nullptr;
  }

private:
  common::ObArenaAllocator allocator_;
  ObPxSqcHandler *sqc_handler_;
  ObPxSubCoord *sub_coord_;
  MockDtlChannel *mock_channel_;
  bool inited_;

  // Minimal global context for ObPxSubCoord construction
  observer::ObGlobalContext gctx_;
  ObPxRpcInitSqcArgs sqc_arg_;

  DISALLOW_COPY_AND_ASSIGN(MockDatahubContext);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_DATAHUB_H_