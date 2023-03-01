static int my_sk_do_decode(my_sk_t* s, my_msg_t* msg) {
  int err = 0;
  void* b = NULL;
  int64_t sz = sizeof(easy_head_t);
  int64_t req_sz = sz;
  while(0 == (err = my_sk_read(&b, s, sz))
        && NULL != b && (req_sz = my_decode((char*)b, sz)) > 0 && req_sz > sz) {
    sz = req_sz;
  }
  if (req_sz <= 0) {
    err = EINVAL;
  }
  if (0 == err) {
    *msg = (my_msg_t) { .sz = req_sz, .payload = (char*)b };
  }
  return err;
}
