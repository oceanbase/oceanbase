#define BUCKET_SIZE    1024
typedef struct write_queue_t {
  queue_t queue;
  int64_t pos;
  int64_t cnt;
  int64_t sz;
  int16_t categ_count_bucket[BUCKET_SIZE];
} write_queue_t;

extern void wq_init(write_queue_t* wq);
extern void wq_push(write_queue_t* wq, link_t* l);
extern int wq_flush(sock_t* s, write_queue_t* wq, link_t** old_head);
