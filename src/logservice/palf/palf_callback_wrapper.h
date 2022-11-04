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

#ifndef OCEANBASE_LOGSERVICE_PALF_CALLBACK_WRAPPER_
#define OCEANBASE_LOGSERVICE_PALF_CALLBACK_WRAPPER_
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/lock/ob_spin_lock.h"
#include "palf_callback.h"
namespace oceanbase
{
namespace palf
{

struct PalfFSCbNode : public common::ObDLinkBase<PalfFSCbNode>
{
  PalfFSCbNode(PalfFSCb *fs_cb) : fs_cb_(fs_cb) {}
  PalfFSCb *fs_cb_;
  TO_STRING_KV(KP(fs_cb_), KP(prev_), KP(next_));
};

class PalfFSCbWrapper
{
public:
  PalfFSCbWrapper();
  ~PalfFSCbWrapper();
  virtual int add_cb_impl(PalfFSCbNode *cb_impl);
  virtual void del_cb_impl(PalfFSCbNode *cb_impl);
  virtual int update_end_lsn(int64_t id, const LSN &end_lsn, const int64_t proposal_id);
private:
  // The head of list
  ObDList<PalfFSCbNode> list_;
  ObSpinLock lock_;
};

struct PalfRoleChangeCbNode : public common::ObDLinkBase<PalfRoleChangeCbNode>
{
  PalfRoleChangeCbNode(PalfRoleChangeCb *rc_cb) : rc_cb_(rc_cb) {}
  PalfRoleChangeCb *rc_cb_;
  TO_STRING_KV(KP(rc_cb_), KP(prev_), KP(next_));
};

class PalfRoleChangeCbWrapper
{
public:
  PalfRoleChangeCbWrapper();
  ~PalfRoleChangeCbWrapper();
  int add_cb_impl(PalfRoleChangeCbNode *cb_impl);
  void del_cb_impl(PalfRoleChangeCbNode *cb_impl);
  int on_role_change(int64_t id);
  int on_need_change_leader(const int64_t id, const ObAddr &new_leader);
private:
  ObDList<PalfRoleChangeCbNode> list_;
  ObSpinLock lock_;
};

struct PalfRebuildCbNode : public common::ObDLinkBase<PalfRebuildCbNode>
{
  PalfRebuildCbNode(PalfRebuildCb *rebuild_cb) : rebuild_cb_(rebuild_cb) {}
  PalfRebuildCb *rebuild_cb_;
  TO_STRING_KV(KP(rebuild_cb_), KP(prev_), KP(next_));
};

class PalfRebuildCbWrapper
{
public:
  PalfRebuildCbWrapper();
  ~PalfRebuildCbWrapper();
  int add_cb_impl(PalfRebuildCbNode *cb_impl);
  void del_cb_impl(PalfRebuildCbNode *cb_impl);
  int on_rebuild(const int64_t id, const LSN &lsn);
private:
  ObDList<PalfRebuildCbNode> list_;
  ObSpinLock lock_;
};
} // end namespace palf
} // end namespace oceanbase
#endif

