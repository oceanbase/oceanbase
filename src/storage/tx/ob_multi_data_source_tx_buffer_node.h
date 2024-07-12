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

#ifndef OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_TX_BUFFER_NODE_H
#define OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_TX_BUFFER_NODE_H

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "share/scn.h"
#include "storage/tx/ob_tx_seq.h"
#include "storage/tx/ob_multi_data_source_printer.h"
#include "storage/multi_data_source/buffer_ctx.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class BufferCtx;
}
}

namespace transaction
{
enum class ObTxDataSourceType : int64_t;

class ObTxBufferNode
{
  friend class ObPartTransCtx;
  friend class ObTxExecInfo;
  friend class ObMulSourceTxDataNotifier;
  friend class ObTxMDSCache;
  friend class ObTxBufferNodeWrapper;
  OB_UNIS_VERSION(1);

public:
  ObTxBufferNode();
  ~ObTxBufferNode() = default;
  int init(const ObTxDataSourceType type,
           const common::ObString &data,
           const share::SCN &base_scn,
           const ObTxSEQ seq_no,
           storage::mds::BufferCtx *ctx);
  bool is_valid() const;
  void reset();

  static bool is_valid_register_no(const int64_t register_no) { return register_no > 0; }
  int set_mds_register_no(const uint64_t register_no);
  uint64_t get_register_no() const { return register_no_; }
  ObTxSEQ get_seq_no() const { return seq_no_; }

  // only for some mds types of CDC
  // can not be used by observer functions
  bool allow_to_use_mds_big_segment() const;

  void replace_data(const common::ObString &data);

  common::ObString &get_data() { return data_; }
  int64_t get_data_size() const { return data_.length(); }
  ObTxDataSourceType get_data_source_type() const { return type_; }
  const common::ObString &get_data_buf() const { return data_; }
  void *get_ptr() { return data_.ptr(); }

  void set_submitted() { has_submitted_ = true; };
  bool is_submitted() const { return has_submitted_; }

  void set_synced() { has_synced_ = true; }
  bool is_synced() const { return has_synced_; }

  const share::SCN &get_base_scn() { return mds_base_scn_; }

  bool operator==(const ObTxBufferNode &buffer_node) const;

  void log_sync_fail()
  {
    has_submitted_ = false;
    has_synced_ = false;
  }
  storage::mds::BufferCtxNode &get_buffer_ctx_node() const { return buffer_ctx_node_; }
  TO_STRING_KV(K(register_no_),
               K(seq_no_),
               K(has_submitted_),
               K(has_synced_),
               "type", ObMultiDataSourcePrinter::to_str_mds_type(type_),
               K(data_.length()));
private:
  uint64_t register_no_;
  ObTxSEQ seq_no_;
  bool has_submitted_;
  bool has_synced_;
  share::SCN mds_base_scn_;
  ObTxDataSourceType type_;
  common::ObString data_;
  mutable storage::mds::BufferCtxNode buffer_ctx_node_;
};

// manage mds_op contain (buffer_node, buffer, buffer_ctx)
class ObTxBufferNodeWrapper
{
  OB_UNIS_VERSION(1);
public:
  ObTxBufferNodeWrapper();
  ~ObTxBufferNodeWrapper();
  ObTxBufferNodeWrapper(const ObTxBufferNodeWrapper &) = delete;
  ObTxBufferNodeWrapper &operator=(const ObTxBufferNodeWrapper &) = delete;
  const ObTxBufferNode &get_node() const { return node_; }
  int64_t get_tx_id() const { return tx_id_; }
  int pre_alloc(int64_t tx_id, const ObTxBufferNode &node, common::ObIAllocator &allocator);
  // deep_copy by node
  int assign(int64_t tx_id, const ObTxBufferNode &node, common::ObIAllocator &allocator, bool has_pre_alloc);
  int assign(common::ObIAllocator &allocator, const ObTxBufferNodeWrapper &node_wrapper);

  TO_STRING_KV(K_(tx_id), K_(node));
private:
  int64_t tx_id_;
  ObTxBufferNode node_;
};

typedef common::ObSEArray<ObTxBufferNode, 1> ObTxBufferNodeArray;
typedef common::ObSEArray<storage::mds::BufferCtxNode , 1> ObTxBufferCtxArray;

}
}
#endif