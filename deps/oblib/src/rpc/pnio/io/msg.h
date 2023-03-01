enum { MSG_LIMIT = 64 * 1024};
typedef struct msg_t
{
  int64_t s;
  char b[];
} msg_t;
inline msg_t* msg_init(msg_t* m, const char* b, int64_t s) {
  m->s = s + sizeof(msg_t);
  memcpy(m->b, b, s);
  return m;
}
