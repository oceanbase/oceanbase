/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_local_channel.h"
#include "sql/dtl/ob_dtl_channel_watcher.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

////////////////////////////////////////////////////////////////////////////
// ObDtlHashTableCell
////////////////////////////////////////////////////////////////////////////
int ObDtlHashTableCell::insert_channel(uint64_t chid, ObDtlChannel *&chan)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard lock_guard(spin_lock_);
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH_X(node, chan_list_, OB_SUCC(ret)) {
      if (node->get_id() == chid) {
        ret = OB_HASH_EXIST;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!chan_list_.add_last(chan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set channel in map fail", K(ret), KP(chid));
  }
  return ret;
}

int ObDtlHashTableCell::remove_channel(uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  ObByteLockGuard lock_guard(spin_lock_);
  DLIST_FOREACH_REMOVESAFE_X(node, chan_list_, OB_SUCC(ret)) {
    if (node->get_id() == chid) {
      if (OB_ISNULL(chan_list_.remove(node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to remove channel", K(ret), KP(chid));
      } else {
        ch = node;
      }
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(ch)) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("channel not exist", K(ret), KP(chid));
  }
  return ret;
}

int ObDtlHashTableCell::get_channel(uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  ObByteLockGuard lock_guard(spin_lock_);
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH_X(node, chan_list_, OB_SUCC(ret)) {
      if (node->get_id() == chid) {
        ch = node;
        ch->pin();
        break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(ch)) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

int ObDtlHashTableCell::foreach_refactored(const std::function<int(ObDtlChannel *ch)> &op)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard lock_guard(spin_lock_);
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH_X(node, chan_list_, OB_SUCC(ret)) {
      if (OB_FAIL(op(node))) {
        LOG_WARN("foreach callback failed", K(ret));
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
// ObDtlHashTable
////////////////////////////////////////////////////////////////////////////
ObDtlHashTable::~ObDtlHashTable()
{
  if (nullptr != bucket_cells_) {
    for (int64_t i = 0; i < bucket_num_; ++i) {
      bucket_cells_[i].~ObDtlHashTableCell();
    }
    allocator_.free(bucket_cells_);
    bucket_cells_ = nullptr;
    bucket_num_ = 0;
  }
}

int ObDtlHashTable::init(int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (bucket_num <= 0 || (bucket_num & (bucket_num - 1)) != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket number", K(ret), K(bucket_num));
  } else {
    bucket_num_ = bucket_num;
    ObMemAttr attr(OB_SERVER_TENANT_ID, "SqlDtlMgr");
    if (OB_FAIL(allocator_.init(
        lib::ObMallocAllocator::get_instance(),
        OB_MALLOC_NORMAL_BLOCK_SIZE,
        attr))) {
      LOG_WARN("failed to init allocator", K(ret));
    } else {
      allocator_.set_label("SqlDtlMgr");
      bucket_cells_ = reinterpret_cast<ObDtlHashTableCell*>(allocator_.alloc(bucket_num * sizeof(ObDtlHashTableCell)));
      if (nullptr == bucket_cells_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hash table cells", K(ret));
      } else {
        char *buf = reinterpret_cast<char*>(bucket_cells_);
        for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); ++i) {
          ObDtlHashTableCell *cell = new (buf) ObDtlHashTableCell();
          buf += sizeof(ObDtlHashTableCell);
          UNUSED(cell);
        }
      }
    }
  }
  return ret;
}

int ObDtlHashTable::insert_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&chan)
{
  int ret = OB_SUCCESS;
  if (nullptr == bucket_cells_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket cells is null", K(ret));
  } else if (OB_FAIL(bucket_cells_[calc_bucket_idx(hash_val)].insert_channel(chid, chan))) {
    LOG_WARN("failed to insert channel", K(ret), K(hash_val), KP(chid));
  }
  return ret;
}

int ObDtlHashTable::remove_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  if (nullptr == bucket_cells_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket cells is null", K(ret));
  } else if (OB_FAIL(bucket_cells_[calc_bucket_idx(hash_val)].remove_channel(chid, ch))) {
    LOG_WARN("failed to remove channel", K(ret), K(hash_val), KP(chid));
  }
  return ret;
}

int ObDtlHashTable::get_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  if (nullptr == bucket_cells_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket cells is null", K(ret));
  } else if (OB_FAIL(bucket_cells_[calc_bucket_idx(hash_val)].get_channel(chid, ch))) {
  }
  return ret;
}

int ObDtlHashTable::foreach_refactored(const std::function<int(ObDtlChannel *ch)> &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bucket_cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket cells is null", K(ret));
  } else {
    for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(bucket_cells_[i].foreach_refactored(op))) {
        LOG_WARN("failed to foreach cell", K(ret), K(i));
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
// ObDtl
////////////////////////////////////////////////////////////////////////////
ObDtl::ObDtl()
    : is_inited_(false),
      allocator_("SqlDtlMgr"),
      rpc_proxy_(),
      dfc_server_(),
      hash_table_()
{
  rpc_proxy_.set_tenant(OB_DTL_TENANT_ID);
}

ObDtl::~ObDtl()
{
}

int ObDtl::init()
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RATIO = 256;
  int64_t ratio = common::ObServerConfig::get_instance()._px_chunklist_count_ratio;
  ratio = next_pow2(MAX(1LL, MIN(ratio, MAX_RATIO)));
  const int64_t bucket_num = ratio * DTL_CELL_BUCKET_BASE;
  if (OB_FAIL(dfc_server_.init())) {
    LOG_WARN("failed to init flow control server", K(ret));
  } else if (OB_FAIL(hash_table_.init(bucket_num))) {
    LOG_WARN("failed to init hash table", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDtl::create_channel(uint64_t tenant_id, uint64_t chid, const ObAddr &peer, ObDtlChannel *&chan, ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  // Create corresponding channel by peer address.
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (GCTX.self_addr() == peer) {
    // LOCAL CHANNEL
    ret = create_local_channel(tenant_id, chid, peer, chan, dfc);
  } else {
    // RPC CHANNEL
    ret = create_rpc_channel(tenant_id, chid, peer, chan, dfc);
  }
  return ret;
}

// 直接根据channel id将channel从hash_table中移除，并且析构掉
// 与remove channel不同的是，remove channel仅仅从hash_table中移除
// 目前主要用在rpc channel的处理方式
int ObDtl::destroy_channel(uint64_t chid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObDtlChannel *chan = nullptr;
    uint64_t hash_val = get_hash_value(chid);
    if (OB_FAIL(hash_table_.remove_channel(hash_val, chid, chan))) {
      LOG_WARN("failed to remove channel in destroy", K(ret), K(hash_val), KP(chid));
    } else if (nullptr != chan) {
      chan->unpin();
      // spin until there's no reference of this channel.
      while (chan->get_pins() != 0) {
        // 这里之所以添加一个sleep主要是为了让出cpu
        // 在sysbench px的join的场景这里占10%的cpu，让出cpu后，可以提高约10%
        // sql: select  /*+ use_px */t1.pad,t2.pad,t3.pad from sbtest1 t1,sbtest5 t2,sbtest4 t3
        //         where t1.id = 503100 and t1.id=t2.id and t2.id=t3.id
        // plan:
        // |0 |NESTED-LOOP JOIN     |        |1        |147 |
        // |1 | NESTED-LOOP JOIN    |        |1        |100 |
        // |2 |  EXCHANGE IN DISTR  |        |1        |53  |
        // |3 |   EXCHANGE OUT DISTR|:EX10000|1        |52  |
        // |4 |    TABLE GET        |t1      |1        |52  |
        // |5 |  TABLE GET          |t2      |1        |47  |
        // |6 | TABLE GET           |t3      |1        |47  |
        // sleep(100): cpu .0% // 看不到
        // sleep(50 ): cpu .87%
        // sleep(10 ): cpu .88%
        ob_usleep<ObWaitEventIds::DTL_DESTROY_CHANNEL_SLEEP>(100);
      }
      //LOG_WARN("DTL delete", K(chan), K(lbt()));
      if (nullptr != chan->get_msg_watcher()) {
        chan->get_msg_watcher()->remove_data_list(chan, true);
      }
      ob_delete(chan);
    }
  }
  return ret;
}

// 这里将channel释放逻辑拆成了2步
// 第一步：从hash_table中移除，避免后续还有rpc可以get到channel
// 第二步：等到rpc unpin后，对channel进行dfc(流控)的后续处理
// 最后才析构channel对象
// 主要用于data channel的析构处理，因为data channel需要进行dfc的一些特殊处理
int ObDtl::remove_channel(uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObDtlChannel *chan = nullptr;
    uint64_t hash_val = get_hash_value(chid);
    if (OB_FAIL(hash_table_.remove_channel(hash_val, chid, chan))) {
      LOG_WARN("failed to remove channel", K(ret), K(hash_val), KP(chid));
    } else if (nullptr != chan) {
      chan->unpin();
      // spin until there's no reference of this channel.
      while (chan->get_pins() != 0) {
      }
      // 表示data dtl都是等到rpc线程处理结束后，才开始进行清理操作，如dfc处理等
      ch = chan;
      if (nullptr != ch->get_msg_watcher()) {
        ch->get_msg_watcher()->remove_data_list(ch, true);
      }
    }
  }
  return ret;
}

//带有channel pin，封装在里面了，没有单独做一个interface来处理
int ObDtl::get_channel(uint64_t chid, ObDtlChannel *&chan)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    uint64_t hash_val = get_hash_value(chid);
    if (OB_FAIL(hash_table_.get_channel(hash_val, chid, chan))) {
    }
  }
  return ret;
}

// 仅仅用于对channel进行unpin操作，即不再引用
int ObDtl::release_channel(ObDtlChannel *chan)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    chan->unpin();
  }
  return ret;
}

int ObDtl::create_rpc_channel(uint64_t tenant_id, uint64_t chid, const ObAddr &peer,
    ObDtlChannel *&chan, ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  // if nullptr != chan, batch free chans until link_ch_sets
  const bool need_free_chan = (nullptr == chan);
  if (nullptr == chan
      && OB_FAIL(new_channel(tenant_id, chid, peer, chan, false))) {
    LOG_WARN("create rpc channel fail", K(tenant_id), KP(chid), K(ret));
  } else if (nullptr == chan) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel is null", K(tenant_id), KP(chid), K(ret));
  } else if (OB_FAIL(init_channel(tenant_id, chid, peer, chan, dfc, need_free_chan))) {
    LOG_WARN("failed to init channel", K(tenant_id), KP(chid), K(ret), KP(chan));
  }
  return ret;
}

int ObDtl::create_local_channel(uint64_t tenant_id, uint64_t chid, const ObAddr &peer,
    ObDtlChannel *&chan, ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  // if nullptr != chan, batch free chans until link_ch_sets
  const bool need_free_chan = (nullptr == chan);
  if (nullptr == chan
      && OB_FAIL(new_channel(tenant_id, chid, peer, chan, true))) {
    LOG_WARN("create rpc channel fail", K(tenant_id), KP(chid), K(ret));
  } else if (nullptr == chan) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel is null", K(tenant_id), KP(chid), K(ret));
  } else if (OB_FAIL(init_channel(tenant_id, chid, peer, chan, dfc, need_free_chan))) {
    LOG_WARN("failed to init channel", K(ret), K(tenant_id), KP(chid), K(chan));
  }
  return ret;
}

int ObDtl::register_data_channel(ObDtlChannel *chan, ObDtlFlowControl *dfc,
                                 ObTenantDfc *tenant_dfc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel is null", K(ret));
  } else if (OB_ISNULL(dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dfc is null", K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_dfc is null", K(ret));
  } else if (OB_FAIL(chan->init(dfc))) {
    LOG_WARN("init channel fail", KP(chan->get_id()), K(ret));
  } else if (OB_FAIL(dfc_server_.register_dfc_channel(*dfc, chan, tenant_dfc))) {
    LOG_WARN("failed to register channel to dfc", KP(chan->get_id()), K(ret));
  } else {
    IGNORE_RETURN chan->pin();
    uint64_t hash_val = get_hash_value(chan->get_id());
    if (OB_FAIL(hash_table_.insert_channel(hash_val, chan->get_id(), chan))) {
      LOG_WARN("failed to insert channel into hash table", K(ret), K(hash_val), KP(chan->get_id()));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = dfc_server_.unregister_dfc_channel(*dfc, chan))) {
        LOG_WARN("failed to unregister channel from dfc", K(tmp_ret), KP(chan->get_id()));
      }
    }
  }
  return ret;
}

int ObDtl::new_channel(uint64_t tenant_id, uint64_t chid, const ObAddr &peer,
    ObDtlChannel *&chan, bool is_local)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (is_local) {
      chan = static_cast<ObDtlChannel *> (ob_malloc(sizeof(ObDtlLocalChannel), ObMemAttr(tenant_id, "SqlDtlChan")));
      if (nullptr != chan) {
        new (chan) ObDtlLocalChannel(tenant_id, chid, peer, ObDtlChannel::DtlChannelType::LOCAL_CHANNEL);
      }
    } else {
      chan = static_cast<ObDtlChannel *> (ob_malloc(sizeof(ObDtlRpcChannel), ObMemAttr(tenant_id, "SqlDtlChan")));
      if (nullptr != chan) {
        new (chan) ObDtlRpcChannel(tenant_id, chid, peer, ObDtlChannel::DtlChannelType::RPC_CHANNEL);
      }
    }
    if (nullptr == chan) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create local channel fail", K(tenant_id), KP(chid), K(ret));
    }
  }
  return ret;
}

int ObDtl::init_channel(uint64_t tenant_id, uint64_t chid, const ObAddr &peer,
    ObDtlChannel *&chan, ObDtlFlowControl *dfc, const bool need_free_chan)
{
  int ret = OB_SUCCESS;
  UNUSED(peer);
  if (nullptr == chan) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel is null", K(tenant_id), KP(chid), K(ret));
  } else if (OB_FAIL(chan->init(dfc))) {
    LOG_WARN("init channel fail", K(tenant_id), KP(chid), K(ret));
  } else {
    if (nullptr != dfc) {
      // 如果有dfc，必须和channel一起建立，否则channel创建后，有rpc processor线程处理
      // 这样channel的dfc设置滞后后，会导致这行的处理没有dfc
      if (OB_FAIL(dfc_server_.register_dfc_channel(*dfc, chan))) {
        LOG_WARN("failed to register channel to dfc", K(tenant_id), KP(chid), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      IGNORE_RETURN chan->pin();
      uint64_t hash_val = get_hash_value(chid);
      if (OB_FAIL(hash_table_.insert_channel(hash_val, chid, chan))) {
        LOG_WARN("failed to insert channel into hash table", K(ret), K(hash_val), KP(chid));
      }
    }
  }
  if (OB_FAIL(ret) && nullptr != chan) {
    LOG_WARN("failed to create channel", K(tenant_id), KP(chid), K(ret), K(chan), KP(chan->get_id()));
    if (nullptr != dfc) {
      //注意错误码不要被覆盖掉
      int tmp_ret = OB_SUCCESS;
      // 之前如果注册到dfc了，必须unregister掉，否则dfc中的channel就是无效的地址
      if (OB_SUCCESS != (tmp_ret = dfc_server_.unregister_dfc_channel(*dfc, chan))) {
        ret = tmp_ret;
        LOG_WARN("failed to register channel to dfc", K(ret), K(tenant_id), KP(chid), KP(chan->get_id()));
      }
    }
    if (need_free_chan) {
      ob_delete(chan);
    }
    chan = nullptr;
  }
  return ret;
}

int ObDtl::get_tenant_dfc(uint64_t tenant_id, ObTenantDfc *&tenant_dfc)
{
  return dfc_server_.get_current_tenant_dfc(tenant_id, tenant_dfc);
}

int ObDtl::foreach_refactored(const std::function<int(ObDtlChannel *ch)> &op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hash_table_.foreach_refactored(op))) {
    LOG_WARN("failed to refactor hash table", K(ret));
  }
  return ret;
}

ObDtl *ObDtl::instance()
{
  static ObDtl *instance_ = nullptr;
  if (nullptr == instance_) {
    instance_ = static_cast<ObDtl *> (ob_malloc(sizeof(ObDtl), ObMemAttr(OB_SERVER_TENANT_ID, "SqlDtlMgr")));
    if (nullptr != instance_) {
      new (instance_) ObDtl();
    }
  }
  return instance_;
}

}  // dtl
}  // sql
}  // oceanbase
