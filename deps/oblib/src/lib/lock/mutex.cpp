#include "lib/lock/mutex.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_diagnose_info.h"
using namespace oceanbase::common;
namespace obutil
{
ObUtilMutex::ObUtilMutex(long long latch_id) : latch_id_(latch_id)
{
  const int rt = pthread_mutex_init(&_mutex, NULL);
#ifdef _NO_EXCEPTION
  assert( rt == 0 );
  if ( rt != 0 ) {
    _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
  }
#else
  if ( rt != 0 ) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
}

ObUtilMutex::~ObUtilMutex()
{
  const int rt = pthread_mutex_destroy(&_mutex);
  assert(rt == 0);
  if ( rt != 0 ) {
    _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
  }
}

bool ObUtilMutex::trylock() const
{
  const int rt = pthread_mutex_trylock(&_mutex);
#ifdef _NO_EXCEPTION
  if ( rt != 0 && rt !=EBUSY ) {
    if ( rt == EDEADLK ) {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadLockedException ");
    } else {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
    }
    return false;
  }
#else
  if(rt != 0 && rt != EBUSY) {
    if(rt == EDEADLK) {
      throw ThreadLockedException(__FILE__, __LINE__);
    } else {
      throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
  }
#endif
  return (rt == 0);
}

void ObUtilMutex::lock() const
{
  ObWaitEventGuard wait_guard(
    ObLatchDesc::wait_event_idx(latch_id_),
    0,
    reinterpret_cast<uint64_t>(this),
    0,
    0);
  const int rt = pthread_mutex_lock(&_mutex);
#ifdef _NO_EXCEPTION
  assert( rt == 0 );
  if ( rt != 0 ) {
    if ( rt == EDEADLK ) {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadLockedException ");
    } else {
      _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
    }
  }
#else
  if( rt != 0 ) {
    if(rt == EDEADLK) {
      throw ThreadLockedException(__FILE__, __LINE__);
    } else {
      throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
  }
#endif
}

void ObUtilMutex::lock(LockState&) const
{
}

void ObUtilMutex::unlock() const
{
  const int rt = pthread_mutex_unlock(&_mutex);
#ifdef _NO_EXCEPTION
  assert( rt == 0 );
  if ( rt != 0 ) {
    _OB_LOG_RET(ERROR,OB_ERR_SYS, "%s","ThreadSyscallException");
  }
#else
  if ( rt != 0 ) {
    throw ThreadSyscallException(__FILE__, __LINE__, rt);
  }
#endif
}

void ObUtilMutex::unlock(LockState& state) const
{
  state.mutex = &_mutex;
}

bool ObUtilMutex::will_unlock() const
{
  return true;
}
}//end namespace obutil
