/*
 *  Citrusleaf test
 *  loop.c - A key-value oriented, looping test that allows stress testing
 * of inserts, deletes, etc
 *
 *   Brian Bulkowski
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <pthread.h>

#include <event2/event.h>
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf/cf_clock.h"

#include "loop.h"


// #define DEBUG_VERBOSE 1

//
// Convert a uint to a number sz wide using front-zero padding
// to fill the space
//


void
my_itoa(char *s, uint64_t v, uint sz)
{
  s[sz] = 0;
  int i;
  for (i = sz-1; i >= 0; i--) {
    s[i] = (v % 10) + '0';
    if (v) v = v / 10;
  }

}

//
// State diagram
//
// NO_KEY - starting point. No key has been allocated
//   VALUE_UNINIT ---
//     VALUE_UNINIT_WRITE - key not known to exist in store. A delete has been started.
//     VALUE_UNINIT_GET    - key just delete, get to make sure it doesn't exist
//   VALUE_DELETED ---
//     VALUE_DELETED_GET_ALL - key should not exist. Get_all to make sure it doesn't.
//     VALUE_DELETED_WRITE - create a new random value and write it
//   VALUE_KNOWN ---
//     VALUE_KNOWN_GET - get the value and see if it's still the same
//     VALUE_KNOWN_DELETE - delete the value and set state to deleted
//     VALUE_KNOWN_WRITE - write a new value



enum transaction_state { 	START 						= 0,
                                VALUE_UNINIT_WRITE  		= 1,
                                VALUE_UNINIT_GET			= 2,
                                VALUE_UNINIT_RESPONSE		= 3,
                                VALUE_DELETED_GET_ALL		= 4,
                                VALUE_DELETED_PUT			= 5,
                                VALUE_DELETED_GET			= 6,
                                VALUE_DELETED_RESPONSE		= 7,
                                VALUE_KNOWN_GET				= 8,
                                VALUE_KNOWN_GET_RESPONSE	= 9,
                                VALUE_KNOWN_WRITE_NEW		= 10,
                                VALUE_KNOWN_WRITE_NEW_RESPONSE = 11,
                                VALUE_KNOWN_DELETE			= 12,
                                VALUE_KNOWN_DELETE_RESPONSE = 13
};


char *state_string[] = {
  "start",
  "value uninit delete",
  "value uninit get",
  "value_uninit_response",
  "value deleted get all",
  "value deleted put",
  "value deleted get",
  "value deleted response",
  "value known get",
  "value known get response",
  "value known write new",
  "value known write new response",
  "value known delete",
  "value known delete response"
};

typedef struct {
  int transaction_id;
  enum transaction_state state;
  uint32_t key;
  ev2citrusleaf_object key_o;
  uint64_t            start_ms; // last start time
  char key_s[64];  // object points into here, ugly
} transaction;

pthread_mutex_t		start_ms_lock =  PTHREAD_MUTEX_INITIALIZER;

static inline void
update_start_ms(transaction *t) {
  pthread_mutex_lock(&start_ms_lock);
  t->start_ms = cf_getms();
  pthread_mutex_unlock(&start_ms_lock);
}

static inline void
clear_start_ms(transaction *t) {
  pthread_mutex_lock(&start_ms_lock);
  t->start_ms = 0;
  pthread_mutex_unlock(&start_ms_lock);
}

// forward reference
void do_transaction(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, uint32_t expiration, void *udata);

//
// Good for debugging
//
uint64_t
get_digest(transaction *t)
{
  cf_digest d;
  memset(&d, 0, sizeof(d));
  ev2citrusleaf_calculate_digest(g_config.set, &t->key_o, &d);
  return( * (uint64_t *) &d );
}


// #define HALT_ON_ERROR

//
// Get a new key
// Sometimes you just need a new key - sets the key and key object in the
// transaction

void
get_new_key(transaction *t)
{
  int rv;
  uint32_t key;
  do {
    // Pick a key to use. Look it up to see if anyone else is using it.
    key = rand_64() % g_config.n_keys;

    rv =  shash_put_unique(g_config.in_progress_hash, &key, 0);
  } while (rv != SHASH_OK);

  t->key = key;
  my_itoa(t->key_s, key, g_config.key_len);
  ev2citrusleaf_object_init_str(&t->key_o, t->key_s);

}

int
write_new_value(transaction *t)
{
  char new_value_str[g_config.value_len+1];
  uint64_t new_value_int;
  do {
    new_value_int = rand_64();
  } while (new_value_int == VALUE_UNINIT || new_value_int == VALUE_DELETED);

  g_config.values[t->key] = new_value_int;

  ev2citrusleaf_bin values[1];
  strcpy(values[0].bin_name, g_config.bin);

  if (g_config.integer == false) {
    my_itoa(new_value_str, new_value_int, g_config.value_len);
    ev2citrusleaf_object_init_str(&values[0].object, new_value_str);
  }
  else {
    ev2citrusleaf_object_init_int(&values[0].object, new_value_int);
  }

  update_start_ms(t);

#ifdef DEBUG_VERBOSE
  fprintf(stderr, "write new value: setting key %u to value %"PRIx64"\n",
          t->key, g_config.values[t->key]);
#endif

  int rv;
  rv = ev2citrusleaf_put(g_config.asc, g_config.ns, g_config.set, &t->key_o, values, 1, 0,
                         g_config.timeout_ms, do_transaction, t, g_config.base);
  if (rv != 0) {
    fprintf(stderr, "aerospike put returned error %d, fail. digest %"PRIu64"\n",rv,get_digest(t) );
    return(-1);
  }
  return(0);
}

int
validate_value(transaction *t, ev2citrusleaf_bin *bins, int n_bins)
{

  // validate value
  if (n_bins != 1) {
    fprintf(stderr, "probe for correct value: wrong number bins, expect 1 got %d, digest %"PRIx64"\n",n_bins, get_digest(t) );
    return(-1);
  }

  if (g_config.integer == false) {
    if (bins[0].object.type != CL_STR) {
      fprintf(stderr, "probe for correct value: wrong type, expected %d got %d, digest %"PRIx64"\n",CL_STR,bins[0].object.type, get_digest(t));
      return(-1);
    }
    char new_value_str[g_config.value_len+1];
    my_itoa(new_value_str, g_config.values[t->key], g_config.value_len);
    if (strcmp(new_value_str, bins[0].object.u.str)) {
      fprintf(stderr, "probe for correct value at key failed, is %s should be %s, digest %"PRIx64"\n",
              new_value_str, bins[0].object.u.str, get_digest(t) );
      return(-1);
    }
  }
  else {
    if (bins[0].object.type != CL_INT) {
      fprintf(stderr, "probe for correct value: wrong type, expected %d got %d, digest %"PRIx64"\n",CL_INT,bins[0].object.type, get_digest(t) );
      return(-1);
    }
    if (g_config.values[t->key] !=  bins[0].object.u.i64) {
      fprintf(stderr, "probe for correct value at key failed, should be %"PRIx64" is %"PRIx64", digest %"PRIx64"\n",
              g_config.values[t->key], bins[0].object.u.i64, get_digest(t) );
      return(-1);
    }
  }
  return(0);
}



//
// Main processing loop. Called by libevent through the tranaction processing system
//

void
do_transaction(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, uint32_t expiration, void *udata)
{
  transaction *t = (transaction *) udata;
  int rv;
  uint64_t key_value;

#ifdef DEBUG_VERBOSE
  fprintf(stderr, "do transaction: id %d state %s\n",t->transaction_id,state_string[t->state]);
#endif

  switch(t->state) {
    case START:
      get_new_key(t);
      key_value = g_config.values[t->key];
      if (key_value == VALUE_UNINIT)
        t->state = VALUE_UNINIT_WRITE;
      else if (key_value == VALUE_DELETED)
        t->state = VALUE_DELETED_GET_ALL;
      else // value is well known, validate it's OK
        t->state = VALUE_KNOWN_GET;
      update_start_ms(t);

      do_transaction(0, 0, 0, 0, 0, (void *) t);
      break;

      // The value is known to be uninitialized, do a delete to make sure it's gone
    case VALUE_UNINIT_WRITE: {

      t->state = VALUE_UNINIT_GET; // next state

      if (0 != write_new_value(t)) {
        goto Fail;
      }

      atomic_int_add(g_config.write_counter, 1);
      atomic_int_add(g_config.key_counter, 1);

    } break;

      // start a get to make sure the value has really been deleted
    case VALUE_UNINIT_GET: {

      if (return_value != EV2CITRUSLEAF_OK) {
        if (return_value != -2) {
          fprintf(stderr, "state VALUE_UNINIT_GET: previous request returned bad rv %d, digest %"PRIx64"\n",return_value, get_digest(t) );
        }
        goto Fail;
      }

      t->state = VALUE_UNINIT_RESPONSE; // next state
      update_start_ms(t);

      const char *bins[1] = { g_config.bin };
      rv = ev2citrusleaf_get(g_config.asc, g_config.ns, g_config.set, &t->key_o,
                             bins, 1, g_config.timeout_ms, do_transaction, (void *) t, g_config.base);
      if (rv != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "could not dispatch get %d, fail, digest %"PRIx64"\n",rv, get_digest(t) );
        goto Fail;
      }

      atomic_int_add(g_config.read_counter, 1);

    } break;

      // validate the response from the uninit_get state
    case VALUE_UNINIT_RESPONSE: {

      if (return_value != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "state VALUE_UNINIT_RESPONSE: get request failed %d, digest %"PRIx64"\n",return_value, get_digest(t) );
        goto Fail;
      }

      if (0 != validate_value(t, bins, n_bins) ) {
        goto Fail;
      }

      t->state = START;		// next state
      clear_start_ms(t);

      // cleanup
      shash_delete(g_config.in_progress_hash, &t->key); // release key

      // set on path for next transaction
      do_transaction(0, 0, 0, 0, 0, (void *) t);

    } break;

      // trigger a get_all to make sure it's really not there
    case VALUE_DELETED_GET_ALL:

      t->state = VALUE_DELETED_PUT;  // next state
      update_start_ms(t);

      rv = ev2citrusleaf_get_all(g_config.asc, g_config.ns, g_config.set, &t->key_o,
                                 g_config.timeout_ms, do_transaction, (void *) t, g_config.base);
      if (rv != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "could not dispatch getall in deleted %d, fail, digest %"PRIx64"\n",rv, get_digest(t) );
        goto Fail;
      }

      atomic_int_add(g_config.read_counter, 1);

      break;

    case VALUE_DELETED_PUT:
      if (return_value != EV2CITRUSLEAF_FAIL_NOTFOUND) {
        fprintf(stderr, "state VALUE_DELETED_PUT: previous request returned wrong return value %d, digest %"PRIx64"\n",return_value, get_digest(t) );
        goto Fail;
      }

      t->state = VALUE_DELETED_GET;

      if (0 != write_new_value(t)) {
        goto Fail;
      }

      atomic_int_add(g_config.write_counter, 1);
      atomic_int_add(g_config.key_counter, 1);

      break;

    case VALUE_DELETED_GET:
      {
        if (return_value != EV2CITRUSLEAF_OK) {
          fprintf(stderr, "state VALUE_DELETED_GET: previous request returned wrong return value %d, digest %"PRIx64"\n",return_value, get_digest(t) );
          goto Fail;
        }

        t->state = VALUE_DELETED_RESPONSE;
        update_start_ms(t);

        const char *bins[1] = { g_config.bin };
        rv = ev2citrusleaf_get(g_config.asc, g_config.ns, g_config.set, &t->key_o,
                               bins, 1, g_config.timeout_ms, do_transaction, (void *) t, g_config.base);
        if (rv != EV2CITRUSLEAF_OK) {
          fprintf(stderr, "could not dispatch get %d, fail, digest %"PRIx64"\n",rv, get_digest(t) );
          goto Fail;
        }

        atomic_int_add(g_config.read_counter, 1);

      } break;

    case VALUE_DELETED_RESPONSE:

      if (return_value != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "state VALUE_DELETED_RESPONSE: previous request returned wrong return value %d, digest %"PRIx64"\n",return_value, get_digest(t));
        goto Fail;
      }

      t->state = START;
      clear_start_ms(t);

      // cleanup
      shash_delete(g_config.in_progress_hash, &t->key); // release key

      // set on path for next transaction
      do_transaction(0, 0, 0, 0, 0, (void *) t);

      break;

    case VALUE_KNOWN_GET: {

      t->state = VALUE_KNOWN_GET_RESPONSE;
      update_start_ms(t);

#ifdef DEBUG_VERBOSE
      fprintf(stderr, "known get: checking key %u has value %"PRIu64"\n",
              t->key, g_config.values[t->key]);
#endif

      const char *bins[1] = { g_config.bin };
      rv = ev2citrusleaf_get(g_config.asc, g_config.ns, g_config.set, &t->key_o,
                             bins, 1, g_config.timeout_ms, do_transaction, (void *) t, g_config.base);
      if (rv != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "could not dispatch get %d, fail, digest %"PRIx64"\n",rv, get_digest(t));
        goto Fail;
      }

      atomic_int_add(g_config.read_counter, 1);

    } break;

    case VALUE_KNOWN_GET_RESPONSE: {
      if (return_value != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "state VALUE_KNOWN_GET_RESPONSE: previous request returned wrong return value %d, digest %"PRIx64"\n",return_value, get_digest(t) );
        goto Fail;
      }

      if (0 != validate_value(t, bins, n_bins) ) {
        goto Fail;
      }


      // roll die: next state could be:
      //    write new value
      //    delete this value
      //    leave it alone and try a different key
      uint32_t    die = rand_64() & 0x03;
      if (die == 0) // 25% chance
        t->state = VALUE_KNOWN_WRITE_NEW;
      else if (die == 1) // 25% chance
        t->state = VALUE_KNOWN_DELETE;
      else { // 50% chance
        shash_delete(g_config.in_progress_hash, &t->key); // release key
        t->state = START;
      }
      clear_start_ms(t);

      do_transaction(0,0,0,0,0,(void *)t);
    } break;

    case VALUE_KNOWN_WRITE_NEW:

      t->state = VALUE_KNOWN_WRITE_NEW_RESPONSE;

      if (0 != write_new_value(t)) goto Fail;

      atomic_int_add(g_config.write_counter, 1);

      break;

    case VALUE_KNOWN_WRITE_NEW_RESPONSE:

      if (return_value != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "state %s: previous request returned wrong return value %d, digest %"PRIx64"\n",
                state_string[t->state],return_value, get_digest(t) );
        goto Fail;
      }

      shash_delete(g_config.in_progress_hash, &t->key); // release key

      t->state = START;
      clear_start_ms(t);

      do_transaction(0,0,0,0,0,(void *)t);
      break;

    case VALUE_KNOWN_DELETE: {

      if (EV2CITRUSLEAF_OK !=
          ev2citrusleaf_delete(g_config.asc, g_config.ns, g_config.set, &t->key_o, 0, g_config.timeout_ms,
                               do_transaction, (void *) t , g_config.base)) {
        fprintf(stderr, "state VALUE_INIT_DELETE: delete dispatch failed tid %d, digest %"PRIx64"\n",t->transaction_id, get_digest(t) );
        goto Fail;
      }

      atomic_int_add(g_config.delete_counter, 1);
      atomic_int_add(g_config.key_counter, -1);

      t->state = VALUE_KNOWN_DELETE_RESPONSE;
      clear_start_ms(t);

    } break;

    case VALUE_KNOWN_DELETE_RESPONSE:

      if (return_value != EV2CITRUSLEAF_OK) {
        fprintf(stderr, "state VALUE_UNINIT_RESPONSE: previous request returned wrong return value %d, digest %"PRIx64"\n",return_value, get_digest(t));
        goto Fail;
      }

      g_config.values[t->key] = VALUE_DELETED;
      shash_delete(g_config.in_progress_hash, &t->key); // release key

      t->state = START;
      clear_start_ms(t);

      do_transaction(0,0,0,0,0,(void *)t);
      break;

    default:

      fprintf(stderr, "big fail: why at unknown transaction state %d, digest %"PRIx64"\n",(int)t->state, get_digest(t));
      goto Fail;
  }

#ifdef DEBUG_VERBOSE
  fprintf(stderr, "do transaction complete: id %d state %s\n",t->transaction_id,state_string[t->state]);
#endif


  if (bins)	ev2citrusleaf_bins_free(bins, n_bins);

  return;

Fail:
  //fprintf(stderr, "FAIL FAIL FAIL FAIL id %d state %s, digest %"PRIx64"\n", t->transaction_id,state_string[t->state], get_digest(t));
#ifdef HALT_ON_ERROR
  abort();
#endif

  g_config.values[t->key] = VALUE_UNINIT;
  t->state = START;
  shash_delete(g_config.in_progress_hash, &t->key); // release key
  do_transaction(0, 0, 0, 0, 0, t);

  return;

}




uint32_t progress_hash_fn(void *key)
{
  uint32_t	value = *(uint32_t *)key;
  return(value);
}


//
// The transaction watcher

pthread_t trans_watcher_th;

#define TIMEOUT_ALERT_MS 500

void *
trans_watcher_fn(void *arg)
{
  int n_trans = g_config.n_threads;
  transaction *t = (transaction *) arg;

  do {
    sleep(1);

    for (int i=0;i<n_trans;i++) {

      pthread_mutex_lock(&start_ms_lock);
      uint64_t delta = 0;
      if (t[i].start_ms) delta = cf_getms() - t[i].start_ms;
      pthread_mutex_unlock(&start_ms_lock);

      if (delta > TIMEOUT_ALERT_MS)
        fprintf(stderr, "warning: transaction %d delayed %"PRIu64"\n",i,delta);
      // else
      // fprintf(stderr, " transaction watcher: id %d delta %"PRIu64"\n",i,delta);

    }
  } while (1);
  return(0);

}



int
do_loop_test ( )
{
  fprintf(stderr, "starting test\n");

  // this hash is the rendez-vous for all the worker threads, to make sure they're not working
  // on the same keys at the same time. It starts out empty. When a worker wants to use a key, it
  // drops an element in the hash unique.
#ifdef MULTITHREAD
  shash_create(&g_config.in_progress_hash, progress_hash_fn, sizeof(uint32_t), 0, g_config.n_threads * 2, SHASH_CR_MT_BIGLOCK);
#else // SINGLETHREAD
  shash_create(&g_config.in_progress_hash, progress_hash_fn, sizeof(uint32_t), 0, g_config.n_threads * 2, 0);
#endif

  // Create the aerospike cluster
  g_config.asc = ev2citrusleaf_cluster_create(0, 0);
  ev2citrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port);

  if (g_config.follow == false)
    ev2citrusleaf_cluster_follow(g_config.asc, false);

  // This array is the current value of a given key. Starts out as uninitialized.
  g_config.values = malloc( sizeof(uint64_t) * g_config.n_keys);
  if (g_config.values == 0) {
    fprintf(stderr, " could not malloc %d, use fewer keys",(int)(sizeof(uint64_t)*g_config.n_keys));
    return(-1);
  }
  for (uint i=0;i<g_config.n_keys;i++)
    g_config.values[i] = VALUE_UNINIT;

  g_config.read_counter = atomic_int_create(0);
  g_config.write_counter = atomic_int_create(0);
  g_config.delete_counter = atomic_int_create(0);
  g_config.key_counter = atomic_int_create(0);

  void *counter_control = start_counter_thread( g_config.read_counter, g_config.write_counter, g_config.delete_counter, g_config.key_counter);

  // start a certain number of simultaneous requests
  // create an empty transaction structure in the starting state, let er rip
  transaction *t_array = calloc(g_config.n_threads, sizeof(transaction));

  fprintf(stderr, "starting events for test: creating %d events\n",g_config.n_threads);
  for (uint i=0;i<g_config.n_threads;i++) {
    transaction *t = &t_array[i];
    t->transaction_id = i;
    t->state = START;
    clear_start_ms(t);
    do_transaction(0, 0, 0, 0, 0, t);
  }

  pthread_create(&trans_watcher_th, 0, trans_watcher_fn, t_array);

  // Event loop sinks in here - not sure the best way to signal out???
  fprintf(stderr, "event dispatch sink\n");
  event_base_dispatch(g_config.base);

  free(t_array);

  free(g_config.values);

  ev2citrusleaf_cluster_destroy(g_config.asc);

  stop_counter_thread(counter_control);

  return(-1);
}
