class RespReadyFlag
{
public:
  RespReadyFlag(): ready_(0) {}
  ~RespReadyFlag() {}
  void set_ready() {
    STORE(&ready_, 1);
    rk_futex_wake(&ready_, UINT32_MAX);
  }
  void wait_ready() {
    while(!LOAD(&ready_)) {
      rk_futex_wait(&ready_, 0, NULL);
    }
  }
private:
  int32_t ready_;
};
