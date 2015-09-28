/*
 *  Citrusleaf Key Test
 *  An example program using the C interface
 *  include/key.h
 *
 *  Copyright 2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 * Brian Bulkowski
 */
#pragma once

#include <pthread.h>
#include <stdint.h>

#include "citrusleaf_event2/ev2citrusleaf.h"
#include "shash.h"

extern uint64_t rand_64();

extern int do_loop_test (void);

#define VALUE_UNINIT 	0xFFFFFFFFFFFFFFFF
#define VALUE_DELETED 	0xFFFFFFFFFFFFFFFE

// The libevent system right now is single threaded, so
// take out the thread safety locks and hide them uder this
// #define MULTITHREAD



typedef struct atomic_int_s {
	uint32_t		val;
	pthread_mutex_t	lock;
} atomic_int;

extern atomic_int	*atomic_int_create(uint64_t val);
extern void			atomic_int_destroy(atomic_int *ai);
extern uint64_t		atomic_int_add(atomic_int *ai, int val);
extern uint64_t		atomic_int_get(atomic_int *ai);
extern void		atomic_int_set(atomic_int *ai, uint64_t val);


extern void *start_counter_thread(atomic_int *reads, atomic_int *writes, atomic_int *deletes, atomic_int *keys);
extern void stop_counter_thread(void *id);

typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	char *bin;
	
	bool verbose;

	bool follow;

	bool integer;
	
	uint n_threads;
	uint n_keys;
	
	uint key_len;
	uint value_len;

	uint64_t	*values;  // array of uint64_t, size is the number of keys
	shash		*in_progress_hash;
	
	atomic_int	*read_counter;
	atomic_int	*write_counter;
	atomic_int	*delete_counter;
	atomic_int  *key_counter;
	
	uint		pseudo_seed;
	uint		timeout_ms;
	
	struct event_base *base;
	
	ev2citrusleaf_cluster	*asc;
	
} config;

extern config g_config;


