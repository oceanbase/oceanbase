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
#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_BUFFER_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_BUFFER_CTX_H

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "runtime_utility/common_define.h"
#include "mds_writer.h"

namespace oceanbase
{
namespace share
{
class SCN;
}

namespace storage
{
namespace mds
{

class BufferCtx
{
public:
  BufferCtx() : binding_type_id_(INVALID_VALUE) {}
  virtual ~BufferCtx() {}
  void set_binding_type_id(const int64_t type_id) { binding_type_id_ = type_id; }
  int64_t get_binding_type_id() const { return binding_type_id_; }
  // 允许用户重写的方法
  virtual const MdsWriter get_writer() const = 0;
  virtual void on_redo(const share::SCN &redo_scn) {}
  virtual void before_prepare() {}
  virtual void on_prepare(const share::SCN &prepare_version) {}
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) {}
  virtual void on_abort(const share::SCN &abort_scn) {}
  // 事务上下文恢复的时候需要从事务状态表将BufferCtx深拷贝至事务上下文
  virtual int deep_copy(BufferCtx &) const { return common::OB_SUCCESS; }
  virtual int64_t get_impl_binging_type_id() { return 0; }// 在事务层反序列化和深拷贝时需要得知子类对象的类型

  virtual int64_t to_string(char*, const int64_t buf_len) const = 0;
  // 同事务状态一起持久化以及恢复
  virtual int serialize(char*, const int64_t, int64_t&) const = 0;
  virtual int deserialize(const char*, const int64_t, int64_t&) = 0;
  virtual int64_t get_serialize_size(void) const = 0;
private:
  int64_t binding_type_id_;
};

// 该结构嵌入事务上下文中，与多数据源的BufferNode一一对应，同事务状态一起持久化以及恢复
// 多数据源框架负责维护该结构内的状态
// 事务层需要在指定的事件节点调用该结构对应的接口
class BufferCtxNode
{
public:
  BufferCtxNode() : ctx_(nullptr) {}
  void set_ctx(BufferCtx *ctx) { MDS_ASSERT(ctx_ == nullptr); ctx_ = ctx; }// 预期不应该出现覆盖的情况
  const BufferCtx *get_ctx() const { return ctx_; }
  void destroy_ctx();
  void on_redo(const share::SCN &redo_scn) { ctx_->on_redo(redo_scn); }
  void before_prepare() { ctx_->before_prepare(); }
  void on_prepare(const share::SCN &prepare_version) { ctx_->on_prepare(prepare_version); }
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) {
    MDS_LOG(INFO, "buffer ctx on commit", KP(this), K(*this));
    ctx_->on_commit(commit_version, commit_scn);
  }
  void on_abort(const share::SCN &abort_scn) {
    MDS_LOG(INFO, "buffer ctx on abort", KP(this), K(*this));
    ctx_->on_abort(abort_scn);
  }
  // 同事务状态一起持久化以及恢复
  int serialize(char*, const int64_t, int64_t&) const;// 要把实际的ctx类型编码进二进制中
  int deserialize(const char*, const int64_t, int64_t&);// 要根据实际的ctx的类型，在编译期反射子类类型
  int64_t get_serialize_size(void) const;
  TO_STRING_KV(KP(this), KP_(ctx), KPC_(ctx));
private:
  BufferCtx *ctx_;
};

}
}
}
#endif