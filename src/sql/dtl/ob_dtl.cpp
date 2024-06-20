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

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl.h"
#include "lib/oblog/ob_log.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_local_channel.h"
#include "observer/ob_server_struct.h"
#include "ob_dtl_interm_result_manager.h"
#include "sql/dtl/ob_dtl_channel_watcher.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

////////////////////////////////////////////////////////////////////////////
int ObDtlChannelManager::insert_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&chan)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
  if (OB_FAIL(hash_table_.insert_channel(hash_val, chid, chan))) {
  }
  return ret;
}

int ObDtlChannelManager::remove_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
  if (OB_FAIL(hash_table_.remove_channel(hash_val, chid, ch))) {
  }
  return ret;
}

int ObDtlChannelManager::get_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
  if (OB_FAIL(hash_table_.get_channel(hash_val, chid, ch))) {
  }
  return ret;
}

int ObDtlChannelManager::foreach_refactored(int64_t interval, std::function<int(ObDtlChannel *ch)> op)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = hash_table_.get_bucket_num();
  ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
  for (int64_t i = idx_; i < bucket_num && OB_SUCC(ret); i += interval) {
    if (OB_FAIL(hash_table_.foreach_refactored(i, op))) {
      LOG_WARN("failed to refactor all channels", K(ret), K(i), K(interval));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
ObDtlHashTable::~ObDtlHashTable()
{
  if (nullptr != bucket_cells_) {
    for (int64_t i = 0; i < bucket_num_; ++i) {
      ObDtlHashTableCell &cell = bucket_cells_[i];
      cell.~ObDtlHashTableCell();
    }
    allocator_.free(bucket_cells_);
    bucket_cells_ = nullptr;
  }
}

int ObDtlHashTable::init(int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  bucket_num_ = bucket_num;
  if (bucket_num <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect bukcet number", K(bucket_num));
  } else {
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
  } else {
    int64_t nth_cell = hash_val % bucket_num_;
    if (OB_FAIL(bucket_cells_[nth_cell].insert_channel(chid, chan))) {
    }
  }
  return ret;
}

int ObDtlHashTable::remove_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  if (nullptr == bucket_cells_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket cells is null", K(ret));
  } else {
    int64_t nth_cell = hash_val % bucket_num_;
    if (OB_FAIL(bucket_cells_[nth_cell].remove_channel(chid, ch))) {
    }
  }
  return ret;
}

int ObDtlHashTable::get_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  if (nullptr == bucket_cells_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket cells is null", K(ret));
  } else {
    int64_t nth_cell = hash_val % bucket_num_;
    if (OB_FAIL(bucket_cells_[nth_cell].get_channel(chid, ch))) {
    }
  }
  return ret;
}

int ObDtlHashTable::foreach_refactored(int64_t nth_cell, std::function<int(ObDtlChannel *ch)> op)
{
  int ret = OB_SUCCESS;
  if (0 > nth_cell || bucket_num_ <= nth_cell) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cell idx", K(ret), K(nth_cell));
  } else {
    if (OB_FAIL(bucket_cells_[nth_cell].foreach_refactored(op))) {
      LOG_WARN("failed to refactor all channels", K(ret));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
int ObDtlHashTableCell::foreach_refactored(std::function<int(ObDtlChannel *ch)> op)
{
  int ret = OB_SUCCESS;
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH_X(node, chan_list_, OB_SUCC(ret)) {
      if (OB_FAIL(op(node))) {
        LOG_WARN("failed to refactor channel", K(ret));
      }
    }
  }
  return ret;
}

int ObDtlHashTableCell::insert_channel(uint64_t chid, ObDtlChannel *&chan)
{
  int ret = OB_SUCCESS;
  // first find channel by chid
  ObDtlChannel *ch = nullptr;
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH(node, chan_list_) {
      if (node->get_id() == chid) {
        ch = node;
        break;
      }
    }
  }
  if (OB_NOT_NULL(ch)) {
    ret = OB_HASH_EXIST;
  } else if (!chan_list_.add_last(chan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set channel in map fail", KP(chid), K(ret), KP(chan->get_id()));
  }
  return ret;
}

int ObDtlHashTableCell::remove_channel(uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH_REMOVESAFE_X(node, chan_list_, OB_SUCC(ret)) {
      if (node->get_id() == chid) {
        ObDtlChannel *tmp = chan_list_.remove(node);
        if (nullptr == tmp) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to remove channel", K(ret), KP(chid));
        } else {
          ch = node;
        }
        break;
      }
    }
  }
  if (OB_ISNULL(ch)) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

int ObDtlHashTableCell::get_channel(uint64_t chid, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  if (0 < chan_list_.get_size()) {
    DLIST_FOREACH_X(node, chan_list_, OB_SUCC(ret)) {
      if (node->get_id() == chid) {
        ch = node;
        ch->pin();
        break;
      }
    }
  }
  if (nullptr == ch) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////
ObDtl::ObDtl()
    : is_inited_(false),
      allocator_("SqlDtlMgr"),
      rpc_proxy_(),
      dfc_server_(),
      hash_table_(),
      ch_mgrs_(nullptr)
{
  rpc_proxy_.set_tenant(OB_DTL_TENANT_ID);
}

ObDtl::~ObDtl()
{
  if (OB_NOT_NULL(ch_mgrs_)) {
    allocator_.free(ch_mgrs_);
    ch_mgrs_ = nullptr;
  }
}

int ObDtl::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfc_server_.init())) {
    LOG_WARN("failed to init flow control server", K(ret));
  } else {
    ch_mgrs_ = reinterpret_cast<ObDtlChannelManager*>(allocator_.alloc(sizeof(ObDtlChannelManager) * HASH_CNT));
    if (OB_ISNULL(ch_mgrs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel manager is null", K(ret));
    } else if (OB_FAIL(hash_table_.init(BUCKET_NUM))) {
      LOG_WARN("failed init hash table", K(ret));
    } else {
      char *buf = reinterpret_cast<char*>(ch_mgrs_);
      for (int64_t i = 0; i < HASH_CNT && OB_SUCC(ret); ++i) {
        ObDtlChannelManager *ch_mgr = new (buf) ObDtlChannelManager(i, hash_table_);
        UNUSED(ch_mgr);
        buf += sizeof(ObDtlChannelManager);
      }
      is_inited_ = true;
    }
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
    ObDtlChannelManager *ch_mgr = nullptr;
    if (OB_FAIL(get_dtl_channel_manager(hash_val, ch_mgr))) {
      LOG_WARN("failed to get dtl channel manager", K(hash_val), KP(chid), K(ret));
    } else if (nullptr == ch_mgr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel manager is null", K(ret));
    } else if (OB_FAIL(ch_mgr->remove_channel(hash_val, chid, chan))) {
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
    ObDtlChannelManager *ch_mgr = nullptr;
    if (OB_FAIL(get_dtl_channel_manager(hash_val, ch_mgr))) {
      LOG_WARN("failed to get dtl channel manager", K(hash_val), KP(chid), K(ret));
    } else if (nullptr == ch_mgr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel manager is null", K(ret));
    } else if (OB_FAIL(ch_mgr->remove_channel(hash_val, chid, chan))) {
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
    ObDtlChannelManager *ch_mgr = nullptr;
    if (OB_FAIL(get_dtl_channel_manager(hash_val, ch_mgr))) {
      LOG_WARN("failed to get dtl channel manager", K(hash_val), KP(chid), K(ret));
    } else if (nullptr == ch_mgr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel manager is null", K(ret));
    } else if (OB_FAIL(ch_mgr->get_channel(hash_val, chid, chan))) {
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
    LOG_WARN("failed to init channel", K(tenant_id), KP(chid), K(ret), K(chan));
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

int ObDtl::get_dtl_channel_manager(uint64_t hash_val, ObDtlChannelManager *&ch_mgr)
{
  int ret = OB_SUCCESS;
  int64_t nth_mgr = hash_val & (HASH_CNT - 1);
  if (nth_mgr < 0 || nth_mgr > HASH_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect nth channel manager", K(nth_mgr), K(ret));
  } else {
    ch_mgr = &ch_mgrs_[nth_mgr];
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
  } else if (OB_FAIL(chan->init())) {
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
      ObDtlChannelManager *ch_mgr = nullptr;
      if (OB_FAIL(get_dtl_channel_manager(hash_val, ch_mgr))) {
        LOG_WARN("failed to get dtl channel manager", K(hash_val), KP(chid), K(ret));
      } else if (nullptr == ch_mgr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("channel manager is null", K(ret));
      } else if (OB_FAIL(ch_mgr->insert_channel(hash_val, chid, chan))) {
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
        LOG_WARN("failed to register channel to dfc", K(tenant_id), KP(chid), K(ret), KP(chan->get_id()));
      }
    }
    if (need_free_chan) {
      ob_delete(chan);
    }
    chan = nullptr;
  }
  return ret;
}

int ObDtl::foreach_refactored(std::function<int(ObDtlChannel *ch)> op)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < HASH_CNT && OB_SUCC(ret); ++i) {
    if (OB_FAIL(ch_mgrs_[i].foreach_refactored(HASH_CNT, op))) {
      LOG_WARN("failed to refactor all channels", K(i));
    }
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
