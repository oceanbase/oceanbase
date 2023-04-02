typedef struct fixed_stack_t
{
  int top_;
  void* array_[4];
} fixed_stack_t;

extern void fixed_stack_init(fixed_stack_t* stk);

int inc_bounded(int* addr, int limit)
{
  int nv = LOAD(addr);
  int ov = 0;
  while((ov = nv) < limit && ov != (nv = VCAS(addr, ov, ov + 1))) {
    SPIN_PAUSE();
  }
  return ov;
}

int dec_bounded(int* addr, int limit)
{
  int nv = LOAD(addr);
  int ov = 0;
  while((ov = nv) > limit && ov != (nv = VCAS(addr, ov, ov - 1))) {
    SPIN_PAUSE();
  }
  return ov;
}

inline int fixed_stack_push(fixed_stack_t* stk, void* p)
{
  int limit = arrlen(stk->array_);
  int top = inc_bounded(&stk->top_, limit);
  if (top < limit) {
    void** pdata = stk->array_ + top;
    while(!BCAS(pdata, NULL, p)) {
      SPIN_PAUSE();
    }
  }
  return top < limit? 0: -EAGAIN;
}

inline void* fixed_stack_pop(fixed_stack_t* stk)
{
  void* p = NULL;
  int top = dec_bounded(&stk->top_, 0);
  if (top > 0) {
    void** pdata = stk->array_ + top - 1;
    while(NULL == LOAD(pdata) || NULL == (p = TAS(pdata, NULL))) {
      SPIN_PAUSE();
    }
  }
  return p;
}
