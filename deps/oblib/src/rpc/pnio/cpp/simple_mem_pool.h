class RpcMemPool: public IMemPool
{
public:
  struct Link { Link* next_; };
  RpcMemPool(): tail_(NULL) {}
  virtual ~RpcMemPool() { destroy(); }
  static RpcMemPool* create(int64_t sz) {
    RpcMemPool* pool = NULL;
    Link* link = (Link*)direct_alloc(sizeof(Link) + sizeof(RpcMemPool) + sz);
    if (NULL != link) {
      pool = (RpcMemPool*)(link + 1);
      new(pool)RpcMemPool();
      pool->add_link(link);
    }
    return pool;
  }
  void* alloc(int64_t sz) {
    void* ret = NULL;
    Link* link = (Link*)direct_alloc(sizeof(Link) + sz);
    if (NULL != link) {
      add_link(link);
      ret = (void*)(link + 1);
    }
    return ret;
  }
  void destroy() {
    Link* cur = tail_;
    while(NULL != cur) {
      Link* next = cur->next_;
      direct_free(cur);
      cur = next;
    }
  }
private:
  static void* direct_alloc(int64_t sz) { return ::malloc(sz); }
  static void direct_free(void* p) { ::free(p); }
  void add_link(Link* link) {
    link->next_ = tail_;
    tail_ = link;
  }
private:
  Link* tail_;
};
