#pragma once
#include <new>
class IMemPool
{
public:
  IMemPool() {}
  virtual ~IMemPool() {}
  virtual void* alloc(int64_t sz) = 0;
  virtual void destroy()  = 0;
};
