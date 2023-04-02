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

#ifndef OB_CONTEXT_H_
#define OB_CONTEXT_H_

#include "lib/worker.h"
#include "lib/lock/ob_latch.h"
#include "lib/rc/context.h"

namespace oceanbase
{
namespace omt
{
class ObTenant;
}
namespace share
{
class ObTenantSpace;
class ObResourceOwner;
class ObTenantSpaceFetcher;
}

#define CREATE_ENTITY(entity, ...) share::create_entity(entity, __VA_ARGS__)
#define DESTROY_ENTITY(entity) destroy_entity(entity)
#define WITH_ENTITY_P(condition, entity)                                                              \
  ENTITY_P(condition, share::Entity2Enum<typename std::remove_pointer<decltype(entity)>::type>::type, \
           share::EntitySource::WITH, entity)
#define FETCH_ENTITY_P(condition, entity_type, ...)                                                   \
  ENTITY_P(condition, share::ObEntityType::entity_type, share::EntitySource::FETCH, __VA_ARGS__)
#define CREATE_WITH_TEMP_ENTITY_P(condition, entity_type, ...)                                        \
  ENTITY_P(condition, share::ObEntityType::entity_type, share::EntitySource::CREATE, __VA_ARGS__)
#define WITH_ENTITY(entity) WITH_ENTITY_P(true, entity)
#define FETCH_ENTITY(entity_type, ...) FETCH_ENTITY_P(true, entity_type, __VA_ARGS__)
#define CREATE_WITH_TEMP_ENTITY(entity_type, ...) CREATE_WITH_TEMP_ENTITY_P(true, entity_type, __VA_ARGS__)
// The following are auxiliary macros
#define ENTITY_P(condition, entity_type, entity_source, ...)                                                   \
  for (share::_S<entity_type, entity_source> _s{condition, __VA_ARGS__}; OB_SUCC(ret) && _s.i_-- > 0; _s.i_--) \
    if (OB_SUCC(_s.get_ret()))

#define CURRENT_ENTITY(TYPE) share::Guard<share::ObEntityType::TYPE>::current_guard()

#define BIND_ENTITY(ENTITY_TYPE, CLS)             \
  template<>                                      \
  class Enum2Entity<ENTITY_TYPE>                  \
  {                                               \
  public:                                         \
    using type = CLS;                             \
  };                                              \
  template<>                                      \
  class Entity2Enum<CLS>                          \
  {                                               \
  public:                                         \
    const static ObEntityType type = ENTITY_TYPE; \
  };

#define BIND_FETCHER(ENTITY_TYPE, CLS)        \
  template<>                                  \
  class Type2Fetcher<ENTITY_TYPE>             \
  {                                           \
  public:                                     \
    using type = CLS;                         \
  };

namespace share
{

enum class ObEntityType
{
  TENANT_SPACE,
  RESOURCE_OWNER
};

template<ObEntityType et>
class Enum2Entity;
template<typename T_Entity>
class Entity2Enum;
template<ObEntityType et>
class Type2Fetcher;
class ObTenantBase;

class EntityBase
{
  template<typename T_Entity, typename ... Args>
  friend int create_entity(T_Entity *&entity,
                           Args && ... args);
  template<typename T_Entity, typename ... Args>
  friend int create_entity(T_Entity &entity,
                           Args && ... args);
  template<typename T_Entity>
  friend void destroy_entity(T_Entity *entity);
public:
  EntityBase()
    : need_free_(true) {}
private:
  bool need_free_;
};

class ObTenantSpace : public EntityBase
{
public:
  ObTenantSpace(ObTenantBase *tenant)
    : tenant_(tenant)
  {}
  int init() { return common::OB_SUCCESS; }
  void deinit() {}
  uint64_t get_tenant_id() const;
  ObTenantBase *get_tenant() const { return tenant_; }
  static int guard_init_cb(const ObTenantSpace &tenant_space, char *buf, bool &is_inited);
  static void guard_deinit_cb(const ObTenantSpace &tenant_space, char *buf);
  static ObTenantSpace &root();
private:
  ObTenantBase *tenant_;
};

class ObResourceOwner : public EntityBase
{
public:
  ObResourceOwner(const uint64_t owner_id)
    : owner_id_(owner_id)
  {}
  int init() { return common::OB_SUCCESS; }
  void deinit() {}
  uint64_t get_owner_id() const { return owner_id_; }
  static int guard_init_cb(const ObResourceOwner &, char *, bool &)
  { return common::OB_SUCCESS; }
  static void guard_deinit_cb(const ObResourceOwner &, char *) {}
  static ObResourceOwner &root();
private:
  uint64_t owner_id_;
};

// For example, if you want to switch to TenantSpace, the most convenient interface for the upper layer is to only provide tenant_id.
// The TenantSpace is obtained by the framework, if the tenant_id is used as the parameter of the TenantGuard, the TenantGuard will do the search,
// Will destroy the Guard's positioning, we keep the Guard simple (Guard only does the switching function), so the logic of obtaining TenantSpace is placed on the outer layer
class ObTenantSpaceFetcher
{
public:
  ObTenantSpaceFetcher(uint64_t tenant_id);
  ~ObTenantSpaceFetcher();
  int get_ret() const { return ret_; }
  ObTenantSpace &entity()
  {
    abort_unless(entity_ != nullptr);
    return *entity_;
  }
private:
  int ret_;
  common::ObLDHandle handle_;
  ObTenantSpace *entity_;
};

template<ObEntityType et>
class Guard;
enum class EntitySource
{
  WITH,     // The parameter is already the target Entity, switch directly
  FETCH,    // Find by parameter
  CREATE    // Created by parameters
};

BIND_ENTITY(ObEntityType::TENANT_SPACE, share::ObTenantSpace);
BIND_ENTITY(ObEntityType::RESOURCE_OWNER, share::ObResourceOwner);
BIND_FETCHER(ObEntityType::TENANT_SPACE, share::ObTenantSpaceFetcher);

template<ObEntityType et>
class Guard final
{
  using T_Entity = typename Enum2Entity<et>::type;
public:
  Guard(T_Entity &ref_entity)
    : ref_entity_(ref_entity),
      prev_(nullptr),
      next_(nullptr),
      is_inited_(false),
      is_inited_of_cb_(false)
  {}
  T_Entity *operator -> () { return &ref_entity_; }
  T_Entity &entity() { return ref_entity_; }
  int init()
  {
    int ret = common::OB_SUCCESS;
    Guard *&cur = g_guard();
    if (nullptr == cur) {
      cur = this;
    } else {
      abort_unless(cur != this);
      cur->next_ = this;
      this->prev_ = cur;
      cur = this;
    }
    is_inited_ = true;
    ret = T_Entity::guard_init_cb(ref_entity_, buf_, is_inited_of_cb_);
    return ret;
  }
  void deinit()
  {
    if (is_inited_of_cb_) {
      T_Entity::guard_deinit_cb(ref_entity_, buf_);
    }
    if (is_inited_) {
      Guard *&cur = g_guard();
      abort_unless(cur == this);
      Guard *parent = cur->prev_;
      if (nullptr == parent) {
        cur = nullptr;
      } else {
        parent->next_ = nullptr;
        cur->prev_ = nullptr;
        cur = parent;
      }
    }
  }
  static Guard &current_guard()
  {
    Guard *&cur = g_guard();
    if (OB_UNLIKELY(nullptr == cur)) {
      struct GuardBuf {
        char v_[sizeof(Guard)];
      };
      RLOCAL(GuardBuf, buf);
      Guard *guard = new ((&buf)->v_) Guard(T_Entity::root());
      abort_unless(guard != nullptr);
      int ret = guard->init();
      abort_unless(common::OB_SUCCESS == ret);
      cur = guard;
    }
    return *cur;
  }
  Guard &parent()
  {
    abort_unless(prev_ != nullptr);
    return *prev_;
  }
private:
  static Guard *&g_guard()
  {
    RLOCAL(Guard*, g_guard);
    return g_guard;
  }
private:
  T_Entity &ref_entity_;
  Guard *prev_;
  Guard *next_;
  bool is_inited_;
  bool is_inited_of_cb_;
  // Buf for forced conversion, temporary code
  char buf_[32];
};

template<typename T_Entity, typename ... Args>
inline int create_entity(T_Entity *&entity,
                         Args && ... args)
{
  int ret = common::OB_SUCCESS;

  lib::ObMemAttr attr;
  attr.label_ = "CreateEntity";
  void *ptr = ROOT_CONTEXT->allocf(sizeof(T_Entity), attr);
  if (OB_ISNULL(ptr)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    entity = new (ptr) T_Entity(args...);
    entity->need_free_ = true;
    if (OB_FAIL(entity->init())) {
    }
  }
  if (OB_FAIL(ret)) {
    destroy_entity(entity);
  }
  return ret;
}

template<typename T_Entity, typename ... Args>
inline int create_entity(T_Entity &entity,
                         Args && ... args)
{
  int ret = common::OB_SUCCESS;

  new (&entity) T_Entity(args...);
  entity.need_free_ = false;
  ret = entity.init();
  if (OB_FAIL(ret)) {
    destroy_entity(&entity);
  }
  return ret;
}

template<typename T_Entity>
inline void destroy_entity(T_Entity *entity)
{
  if (OB_LIKELY(entity != nullptr)) {
    const bool need_free = entity->need_free_;
    entity->deinit();
    entity->~T_Entity();
    if (need_free) {
      ROOT_CONTEXT->free(entity);
    }
  }
}

class _SBase
{
public:
  int get_ret() const
  {
    return ret_;
  }
  _SBase()
    : i_(1),
      ret_(common::OB_SUCCESS)
  {}
  ~_SBase()
  {
    if (OB_UNLIKELY(0 == i_)) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "has break statement!!!");
    }
  }
  int i_;
  int ret_;
};

template<ObEntityType ct, EntitySource es> class _S {};

template<ObEntityType ct>
class _S<ct, EntitySource::WITH> : public _SBase
{
  using T_Guard =  Guard<ct>;
  using T_Entity = typename Enum2Entity<ct>::type;
public:
  _S(const bool condition, T_Entity *entity)
    : _SBase(), guard_(nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(entity)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (condition) {
      T_Guard *tmp_guard = new (buf_) T_Guard(*entity);
      if (OB_FAIL(tmp_guard->init())) {
      } else {
        guard_ = tmp_guard;
      }
    }
    ret_ = ret;
  }
  ~_S()
  {
    if (guard_ != nullptr) {
      guard_->deinit();
      guard_->~T_Guard();
    }
  }
  char buf_[sizeof(T_Guard)] __attribute__ ((aligned (16)));
  T_Guard *guard_;
};

template<ObEntityType ct>
class _S<ct, EntitySource::FETCH> : public _SBase
{
  using T_Guard =  Guard<ct>;
  using T_Entity = typename Enum2Entity<ct>::type;
  using T_EntityFetcher = typename Type2Fetcher<ct>::type;
public:
  template<typename ... Args>
  _S(const bool condition, Args && ... args)
    : _SBase(), fetcher_(nullptr), guard_(nullptr)
  {
    int ret = common::OB_SUCCESS;                                                                                                                                                                                                                                                                                               if (condition) {
      fetcher_ = new (buf0_) T_EntityFetcher(args...);
      if (OB_FAIL(fetcher_->get_ret())) {
      } else {
        T_Guard *tmp_guard = new (buf1_) T_Guard(fetcher_->entity());
        if (OB_FAIL(tmp_guard->init())) {
        } else {
          guard_ = tmp_guard;
        }
      }
    }
    ret_ = ret;
  }
  ~_S()
  {
    if (guard_ != nullptr) {
      guard_->deinit();
      guard_->~T_Guard();
    }
    if (fetcher_ != nullptr) {
      fetcher_->~T_EntityFetcher();
    }
  }
private:
  char buf0_[sizeof(T_EntityFetcher)] __attribute__ ((aligned (16)));
  char buf1_[sizeof(T_Guard)] __attribute__ ((aligned (16)));
  T_EntityFetcher *fetcher_;
  T_Guard *guard_;
};

template<ObEntityType ct>
class _S<ct, EntitySource::CREATE> : public _SBase
{
  using T_Guard =  Guard<ct>;
  using T_Entity = typename Enum2Entity<ct>::type;
public:
  template<typename ... Args>
  _S(const bool condition, Args && ... args)
    : _SBase(), entity_(nullptr), guard_(nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(condition)) {
      T_Entity *tmp_entity = reinterpret_cast<T_Entity*>(buf0_);
      if (OB_FAIL(create_entity(*tmp_entity, args...))) {
        OB_LOG(WARN, "create entity failed", K(ret));
      } else {
        entity_ = tmp_entity;
        T_Guard *tmp_guard = new (buf1_) T_Guard(*entity_);
        if (OB_FAIL(tmp_guard->init())) {
        } else {
          guard_ = tmp_guard;
        }
      }
    }
    ret_ = ret;
  }
  ~_S()
  {
    if (guard_ != nullptr) {
      guard_->deinit();
      guard_->~T_Guard();
    }
    if (entity_ != nullptr) {
      destroy_entity(entity_);
    }
  }
  char buf0_[sizeof(T_Entity)] __attribute__ ((aligned (16)));
  char buf1_[sizeof(T_Guard)] __attribute__ ((aligned (16)));
  T_Entity *entity_;
  T_Guard *guard_;
};

extern int get_tenant_ctx_with_tenant_lock(const uint64_t tenant_id,
                                           common::ObLDHandle &handle,
                                           ObTenantSpace *&tenant_ctx);

} // end of namespace lib
} // end of namespace oceanbase

#endif // OB_CONTEXT_H_
