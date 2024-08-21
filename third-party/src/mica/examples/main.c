#include <getopt.h>
#include <pthread.h>
#include <stdbool.h>
#include "mica.h"

#define TEST_VAL_LEN 32
#define TEST_BATCH_SIZE 16
#define TEST_VERIFY_VAL 0

#define TEST_NUM_BKTS M_2
#define TEST_LOG_CAP M_1024
#define TEST_NUM_KEYS (TEST_NUM_BKTS * 4) /* 50% load */

/* Create a random permutation of @key_arr for thread @tid */
void permute_for(uint128* key_arr, uint64_t* seed) {
  int i, j;
  uint128 temp;

  /* Shuffle */
  for (i = TEST_NUM_KEYS - 1; i >= 1; i--) {
    j = hrd_fastrand(seed) % (i + 1);
    temp = key_arr[i];
    key_arr[i] = key_arr[j];
    key_arr[j] = temp;
  }
}

#if 0
/* Test MICA's batched mode */
void* test_batch_op(void* arg) {
  int i, j;
  int tid = *((int*)arg);
  uint64_t seed = 0xdeadbeef;
  printf("test: Starting thread %d\n", tid);

  assert(TEST_NUM_KEYS % TEST_BATCH_SIZE == 0);
  struct mica_op* op_ptr_arr[TEST_BATCH_SIZE];
  struct mica_resp resp_arr[TEST_BATCH_SIZE];

  struct timespec start, end;
  long long sum = 0;
  long long iter = 0;

  /* Initialize an empty table */
  struct mica_kv kv;
  mica_init(&kv, tid, TEST_NUM_BKTS, TEST_LOG_CAP); /* Instance = tid */

  /* Get a randomized array of keys */
  uint128* key_arr = mica_gen_keys(TEST_NUM_KEYS);

  struct mica_op* op_arr = memalign(4096, TEST_NUM_KEYS * sizeof(*op_arr));
  assert(op_arr != NULL);

  while (1) {
    /* We do either only PUTs or only GETs for a TEST_NUM_KEYS iteration */
    int do_puts = (iter & 1) == 0;

    /* Permute the keys at every iteration to avoid doing GETs in the same
     * sequence as the previous PUTs (which leads to better log caching). */
    hrd_red_printf("Re-permuting keys\n");
    permute_for(key_arr, tid, &seed);
    for (i = 0; i < TEST_NUM_KEYS; i++) {
      unsigned long long* op_key = (unsigned long long*)&op_arr[i].key;
      op_key[0] = key_arr[i].first;
      op_key[1] = key_arr[i].second;

      op_arr[i].val_len = TEST_VAL_LEN;
      memset(op_arr[i].value, (uint8_t)op_arr[i].key.tag, op_arr[i].val_len);
    }

    clock_gettime(CLOCK_REALTIME, &start);

    for (i = 0; i < TEST_NUM_KEYS; i += TEST_BATCH_SIZE) {
      /* Initialize a batch of requests */
      for (j = 0; j < TEST_BATCH_SIZE; j++) {
        if (do_puts) {
          op_arr[i + j].opcode = MICA_OP_PUT;
        } else {
          op_arr[i + j].opcode = MICA_OP_GET;
        }

        op_ptr_arr[j] = &op_arr[i + j];
      }

      mica_batch_op(&kv, TEST_BATCH_SIZE, op_ptr_arr, resp_arr);

      /* Verify responses */
      for (j = 0; j < TEST_BATCH_SIZE; j++) {
        if (do_puts) {
          assert(resp_arr[j].type == MICA_RESP_PUT_SUCCESS);
          continue;
        }

        if (resp_arr[j].type == MICA_RESP_GET_SUCCESS) {
          int exp_len = TEST_VAL_LEN;
          assert(resp_arr[j].val_len == exp_len);

          int k;
          for (k = 0; k < resp_arr[j].val_len; k++) {
            sum += resp_arr[j].val_ptr[k];
#if TEST_VERIFY_VAL == 1
            uint8_t exp_val = op_ptr_arr[j]->key.val;
            assert(resp_arr[j].val_ptr[k] == exp_val);
#endif
          }
        } else {
          assert(resp_arr[j].type == MICA_RESP_GET_FAIL);
        }
      }
    }

    clock_gettime(CLOCK_REALTIME, &end);
    double seconds = (double)(end.tv_nsec - start.tv_nsec) / 1000000000 +
                     (end.tv_sec - start.tv_sec);

    printf(
        "Thread %d: Throughput = %.2f M/s, fail fraction = %.4f. "
        "sum = %llu, seed = %llu\n",
        tid, TEST_NUM_KEYS / (seconds * 1000000),
        (double)kv.num_get_fail / kv.num_get_op, sum, (unsigned long long)seed);

    iter++;
  }
}
#endif

int main(int argc, char* argv[]) {
  int instance_id = 0;
  uint64_t seed = 0xdeadbeef;

  // Initialize an empty table
  struct mica_kv kv;
  mica_init(&kv, instance_id, TEST_NUM_BKTS, TEST_LOG_CAP);

  // Prepare the request/response structs
  struct mica_op op;
  struct mica_resp resp;

  // Get a randomized array of keys
  uint128* key_arr = mica_gen_keys(TEST_NUM_KEYS);

  hrd_red_printf("Re-permuting keys\n");
  permute_for(key_arr, &seed);

  // Prepare the values for PUT
  unsigned long long* op_key_put = (unsigned long long*)&op.key;
  op_key_put[0] = key_arr[0].first;
  op_key_put[1] = key_arr[0].second;

  op.opcode = MICA_OP_PUT;

  op.val_len = TEST_VAL_LEN;
  uint8_t val_byte = 0x10;
  memset(op.value, val_byte, op.val_len);

  // mica_print_op(&op);

  // Insert value
  mica_single_op(&kv, &op, &resp);

  // Check response
  assert(resp.type == MICA_RESP_PUT_SUCCESS);

  // // Prepare the values for GET
  // unsigned long long* op_key_get = (unsigned long long*)&op.key;
  // op_key_get[0] = key_arr[0].first;
  // op_key_get[1] = key_arr[0].second;

  // op.opcode = MICA_OP_GET;
  // memset(op.value, 0, TEST_VAL_LEN);

  // mica_print_op(&op);

  // // Insert value
  // mica_single_op(&kv, &op, &resp);

  // // Check response
  // assert(resp.type == MICA_RESP_GET_SUCCESS);

  // for (int i = 0; i < resp.val_len; i++) {
  //   printf("%d ", resp.val_ptr[i]);
  // }
  // printf("\n");

  free(key_arr);

  mica_free(&kv);

  return 0;
}
