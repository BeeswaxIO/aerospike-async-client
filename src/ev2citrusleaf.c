/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * First attempt is a very simple non-threaded blocking interface
 * currently coded to C99 - in our tree, GCC 4.2 and 4.3 are used
 *
 * Brian Bulkowski, 2009
 * All rights reserved
 */

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <event2/dns.h>
#include <event2/event.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_base_types.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_errno.h"
#include "citrusleaf/cf_hooks.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_log_internal.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_vector.h"
#include "citrusleaf/proto.h"

#include "citrusleaf_event2/cl_cluster.h"
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"


//
// Default mutex lock functions:
//

static void* mutex_alloc() {
	pthread_mutex_t* p_lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));

	if (p_lock) {
		if (pthread_mutex_init(p_lock, NULL) == 0) {
			return (void*)p_lock;
		}

		free((void*)p_lock);
	}

	return NULL;
}

static void mutex_free(void* pv_lock) {
	pthread_mutex_destroy((pthread_mutex_t*)pv_lock);
	free(pv_lock);
}

static inline int mutex_lock(void* pv_lock) {
	return pthread_mutex_lock((pthread_mutex_t*)pv_lock);
}

static inline int mutex_unlock(void* pv_lock) {
	return pthread_mutex_unlock((pthread_mutex_t*)pv_lock);
}

// Container struct for default mutex lock functions:
ev2citrusleaf_lock_callbacks g_default_lock_callbacks;

// Pointer to app-implemented or default mutex lock functions:
ev2citrusleaf_lock_callbacks *g_lock_cb = 0;

//
// Citrusleaf Object calls
//

void
ev2citrusleaf_object_init(ev2citrusleaf_object *o)
{
	o->type = CL_NULL;
	o->size = 0;
	o->free = 0;
}

void
ev2citrusleaf_object_set_null(ev2citrusleaf_object *o)
{
	o->type = CL_NULL;
	o->size = 0;
	o->free = 0;
}


void
ev2citrusleaf_object_init_str(ev2citrusleaf_object *o, char *str)
{
	o->type = CL_STR;
	o->size = strlen(str);
	o->u.str = str;
	o->free = 0;
}

void
ev2citrusleaf_object_init_str2(ev2citrusleaf_object *o, char *str, size_t buf_len)
{
	o->type = CL_STR;
	o->size = buf_len;
	o->u.str = str;
	o->free = 0;
}

void
ev2citrusleaf_object_dup_str(ev2citrusleaf_object *o, char *str)
{
	o->type = CL_STR;
	o->size = strlen(str);
	o->free = o->u.str = strdup(str);
}


void
ev2citrusleaf_object_init_int(ev2citrusleaf_object *o, int64_t i)
{
	o->type = CL_INT;
	o->size = 8;
	o->u.i64 = i;
	o->free = 0;
}

void
ev2citrusleaf_object_init_float(ev2citrusleaf_object *o, double f)
{
	o->type = CL_FLOAT;
	o->size = 8;
	o->u.f64 = f;
	o->free = 0;
}

void
ev2citrusleaf_object_init_blob(ev2citrusleaf_object *o, void *blob, size_t len)
{
	o->type = CL_BLOB;
	o->size = len;
	o->u.blob = blob;
	o->free = 0;
}

void
ev2citrusleaf_object_init_blob2(enum ev2citrusleaf_type btype, ev2citrusleaf_object *o, void *blob, size_t len)
{
	o->type = btype;
	o->size = len;
	o->u.blob = blob;
	o->free = 0;
}

void
ev2citrusleaf_object_dup_blob(ev2citrusleaf_object *o, void *blob, size_t len)
{
	o->type = CL_BLOB;
	o->size = len;
	o->free = o->u.blob = malloc(len);
	memcpy(o->u.blob, blob, len);
}

void
ev2citrusleaf_object_dup_blob2(enum ev2citrusleaf_type btype, ev2citrusleaf_object *o, void *blob, size_t len)
{
	o->type = btype;
	o->size = len;
	o->u.blob = blob;
	o->free = 0;
}


void
ev2citrusleaf_object_free(ev2citrusleaf_object *o) {
	if (o->free)	free(o->free);
}


void
ev2citrusleaf_bins_free(ev2citrusleaf_bin *bins, int n_bins) {

	for (int i=0;i<n_bins;i++) {
		if (bins[i].object.free) free(bins[i].object.free);
	}
	return;
}


//
// Debug calls for printing the buffers. Very useful for debugging....
//
#if 0
static void
dump_buf(char *info, uint8_t *buf, size_t buf_len)
{
	if (cf_debug_enabled()) {
		char msg[buf_len * 4 + 2];
		char* p = msg;

		strcpy(p, "dump_buf: ");
		p += 10;
		strcpy(p, info);
		p += strlen(info);

		for (uint i = 0; i < buf_len; i++) {
			if (i % 16 == 8) {
				*p++ = ' ';
				*p++ = ':';
			}
			if (i && (i % 16 == 0)) {
				*p++ = '\n';
			}
			sprintf(p, "%02x ", buf[i]);
			p += 3;
		}

		*p = 0;
		cf_debug(msg);
	}
}
#endif


//
// Forward reference
//
bool ev2citrusleaf_restart(cl_request* req, bool may_throttle);


//
// Buffer formatting calls
//

uint8_t*
cl_write_header(uint8_t* buf, size_t msg_size, int info1, int info2,
		uint32_t generation, uint32_t expiration, uint32_t timeout,
		uint32_t n_fields, uint32_t n_ops)
{
	as_msg *msg = (as_msg *) buf;
	msg->proto.version = CL_PROTO_VERSION;
	msg->proto.type = CL_PROTO_TYPE_CL_MSG;
	msg->proto.sz = msg_size - sizeof(cl_proto);
	cl_proto_swap(&msg->proto);
	msg->m.header_sz = sizeof(cl_msg);
	msg->m.info1 = info1;
	msg->m.info2 = info2;
	msg->m.info3 = 0;  // info3 never currently written
	msg->m.unused = 0;
	msg->m.result_code = 0;
	msg->m.generation = generation;
	msg->m.record_ttl = expiration;
	msg->m.transaction_ttl = timeout;
	msg->m.n_fields = n_fields;
	msg->m.n_ops = n_ops;
	cl_msg_swap_header(&msg->m);
	return (buf + sizeof(as_msg));
}


//
// cl_request
//

cl_request*
cl_request_create(ev2citrusleaf_cluster* asc, struct event_base* base,
		int timeout_ms, ev2citrusleaf_write_parameters* wparam,
		ev2citrusleaf_callback cb, void* udata)
{
	size_t size = sizeof(cl_request) + (2 * event_get_struct_event_size());
	cl_request* r = (cl_request*)malloc(size);

	if (! r) {
		cf_error("request allocation failed");
		return NULL;
	}

	memset((void*)r, 0, size);

	r->MAGIC = CL_REQUEST_MAGIC;
	r->fd = -1;

	r->asc = asc;
	r->base = base;
	r->timeout_ms = timeout_ms;
	r->wpol = wparam ? wparam->wpol : CL_WRITE_RETRY;
	r->user_cb = cb;
	r->user_data = udata;

	return r;
}

void
cl_request_destroy(cl_request* r)
{
	if (r->wr_buf_size && r->wr_buf != r->wr_tmp) {
		free(r->wr_buf);
	}

	if (r->rd_buf_size && r->rd_buf != r->rd_tmp) {
		free(r->rd_buf);
	}

	if (r->cross_thread_lock) {
		if (r->cross_thread_locked) {
			MUTEX_UNLOCK(r->cross_thread_lock);
		}

		MUTEX_FREE(r->cross_thread_lock);
	}

	free(r);
}

struct event *
cl_request_get_network_event(cl_request *r)
{
	return( (struct event *) &r->event_space[0] );
}

struct event *
cl_request_get_timeout_event(cl_request *r)
{
	return( (struct event *) &r->event_space[ event_get_struct_event_size() ] );
}



//
// lay out a request into a buffer
// Caller is encouraged to allocate some stack space for something like this
// buf if the space isn't big enough we'll malloc
//
// FIELDS WILL BE SWAPED INTO NETWORK ORDER

static uint8_t*
write_fields(uint8_t* buf, const char* ns, int ns_len, const char* set,
		int set_len, const ev2citrusleaf_object* key, const cf_digest* d,
		cf_digest* d_ret)
{

	// lay out the fields
	cl_msg_field *mf = (cl_msg_field *) buf;
	cl_msg_field *mf_tmp;

	mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
	mf->field_sz = ns_len + 1;
	memcpy(mf->data, ns, ns_len);
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);
	mf = mf_tmp;

	if (set) {
		mf->type = CL_MSG_FIELD_TYPE_SET;
		mf->field_sz = set_len + 1;
		memcpy(mf->data, set, set_len);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}

	if (key) {
		mf->type = CL_MSG_FIELD_TYPE_KEY;
		// make a function call here, similar to our prototype code in the server
		if (key->type == CL_STR) {
			mf->field_sz = (uint32_t)key->size + 2;
			uint8_t *fd = (uint8_t *) &mf->data;
			fd[0] = CL_PARTICLE_TYPE_STRING;
			memcpy(&fd[1], key->u.str, key->size);
		}
		else if (key->type == CL_BLOB) {
			mf->field_sz = (uint32_t)key->size + 2;
			uint8_t *fd = (uint8_t *) &mf->data;
			fd[0] = CL_PARTICLE_TYPE_BLOB;
			memcpy(&fd[1], key->u.blob, key->size);
		}
		else if (key->type == CL_INT) {
			mf->field_sz = sizeof(int64_t) + 2;
			uint8_t *fd = (uint8_t *) &mf->data;
			fd[0] = CL_PARTICLE_TYPE_INTEGER;
			uint64_t swapped = htonll((uint64_t)key->u.i64);
			memcpy(&fd[1], &swapped, sizeof(swapped));
		}
		else {
			cf_warn("unknown citrusleaf type %d", key->type);
			return(0);
		}
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
	}

	if (d_ret && key)
		cf_digest_compute2( set, set_len, mf->data, key->size + 1, d_ret);

	if (d) {
		mf->type = CL_MSG_FIELD_TYPE_DIGEST_RIPE;
		mf->field_sz = sizeof(cf_digest) + 1;
		memcpy(mf->data, d, sizeof(cf_digest));
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		if (d_ret)
			memcpy(d_ret, d, sizeof(cf_digest));

		mf = mf_tmp;

	}


	return ( (uint8_t *) mf_tmp );
}

// Convert the int value to the wire protocol

int
value_to_op_int(int64_t value, uint8_t *data)
{
	if ((value < 0) || (value >= 0x7FFFFFFF)) {
		*(uint64_t*)data = htonll((uint64_t)value);  // swap in place
		return(8);
	}

	if (value <= 0x7F) {
		*data = (uint8_t)value;
		return(1);
	}

	if (value <= 0x7FFF) {
		*(uint16_t *)data = htons((uint16_t)value);
		return(2);
	}

	// what remains is 4 byte representation
	*(uint32_t *)data = htonl((uint32_t)value);
	return(4);
}

int
value_to_op_float(double value, uint8_t *data)
{
	*(uint64_t *)data = htonll(*(uint64_t *)&value);
	return 8;
}


extern int
ev2citrusleaf_calculate_digest(const char *set, const ev2citrusleaf_object *key, cf_digest *digest)
{
	int set_len = set ? (int)strlen(set) : 0;

	// make the key as it's laid out for digesting
	// THIS IS A STRIPPED DOWN VERSION OF THE CODE IN write_fields ABOVE
	// MUST STAY IN SYNC!!!
	uint8_t* k = (uint8_t*)alloca(key->size + 1);
	switch (key->type) {
		case CL_STR:
			k[0] = key->type;
			memcpy(&k[1], key->u.str, key->size);
			break;
		case CL_INT:
			{
			uint64_t swapped;
			k[0] = key->type;
			swapped = htonll((uint64_t)key->u.i64);
			memcpy(&k[1], &swapped, sizeof(swapped)); // THIS MUST LEAD TO A WRONG LENGTH CALCULATION BELOW
			}
			break;
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
			k[0] = key->type;
			memcpy(&k[1], key->u.blob, key->size);
			break;
		default:
			cf_warn("transmit key: unknown citrusleaf type %d", key->type);
			return(-1);
	}

	cf_digest_compute2((char *)set, set_len, k, key->size + 1, digest);

	return(0);
}

// Get the size of the wire protocol value
// Must match previous function EXACTLY

int
value_to_op_int_size(int64_t i)
{
	if (i < 0)	return(8);
	if (i <= 0x7F)  return(1);
	if (i < 0x7FFF) return(2);
	if (i < 0x7FFFFFFF) return(4);
	return(8);
}


// convert a wire protocol integer value to a local int64
int
op_to_value_int(const uint8_t *buf, int size, int64_t *value)
{
	if (size > 8)	return(-1);
	if (size == 8) {
		// no need to worry about sign extension - blast it
		*value = (int64_t)ntohll(*(uint64_t*)buf);
		return(0);
	}
	if (size == 0) {
		*value = 0;
		return(0);
	}
	if (size == 1 && *buf < 0x7f) {
		*value = *buf;
		return(0);
	}

	// negative numbers must be sign extended; yuck
	if (*buf & 0x80) {
		uint8_t	lg_buf[8];
		int i;
		for (i=0;i<8-size;i++)	lg_buf[i]=0xff;
		memcpy(&lg_buf[i],buf,size);
		*value = (int64_t)ntohll((uint64_t)*buf);
		return(0);
	}
	// positive numbers don't
	else {
		int64_t	v = 0;
		for (int i=0;i<size;i++,buf++) {
			v <<= 8;
			v |= *buf;
		}
		*value = v;
		return(0);
	}


	return(0);
}

int
op_to_value_float(const uint8_t *buf, int size, double *value)
{
	if (size != 8) {
		return -1;
	}

	uint64_t i = ntohll(*(uint64_t *)buf);
	*value = *(double *)&i;

	return 0;
}

int
value_to_op_get_size(const ev2citrusleaf_object *v, size_t *sz)
{

	switch(v->type) {
		case CL_NULL:
			break;
		case CL_INT:
			*sz += value_to_op_int_size(v->u.i64);
			break;
		case CL_FLOAT:
			*sz += 8;
			break;
		case CL_STR:
			*sz += v->size;
			break;
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_BLOB:
			*sz += v->size;
			break;
		default:
			cf_warn("internal error value_to_op get size has unknown value type %d", v->type);
			return(-1);
	}
	return(0);
}



void
bin_to_op(int operation, const ev2citrusleaf_bin *v, cl_msg_op *op)
{
	int	bin_len = (int)strlen(v->bin_name);
	op->op_sz = sizeof(cl_msg_op) + bin_len - sizeof(uint32_t);
	op->op = operation;
	op->version = 0;
	op->name_sz = bin_len;
	memcpy(op->name, v->bin_name, bin_len);

	// read operations are very simple because you don't have to copy the body
	if (operation == CL_MSG_OP_READ) {
		op->particle_type = 0; // reading - it's unknown
	}
	// write operation - must copy the value
	else {
		uint8_t *data = cl_msg_op_get_value_p(op);
		switch(v->object.type) {
			case CL_NULL:
				op->particle_type = CL_PARTICLE_TYPE_NULL;
				break;
			case CL_INT:
				op->particle_type = CL_PARTICLE_TYPE_INTEGER;
				op->op_sz += value_to_op_int(v->object.u.i64, data);
				break;
			case CL_FLOAT:
				op->particle_type = CL_PARTICLE_TYPE_FLOAT;
				op->op_sz += value_to_op_float(v->object.u.f64, data);
				break;
			case CL_STR:
				op->op_sz += (uint32_t)v->object.size;
				op->particle_type = CL_PARTICLE_TYPE_STRING;
				memcpy(data, v->object.u.str, v->object.size);
				break;
			case CL_BLOB:
				op->op_sz += (uint32_t)v->object.size;
				op->particle_type = CL_PARTICLE_TYPE_BLOB;
				memcpy(data, v->object.u.blob, v->object.size);
				break;
			default:
				cf_warn("internal error value_to_op has unknown value type");
				return;
		}
	}

}

void
operation_to_op(const ev2citrusleaf_operation *v, cl_msg_op *op)
{
	int	bin_len = (int)strlen(v->bin_name);
	op->op_sz = sizeof(cl_msg_op) + bin_len - sizeof(uint32_t);
	op->name_sz = bin_len;
	memcpy(op->name, v->bin_name, bin_len);

	// convert. would be better to use a table or something.
	switch (v->op) {
		case CL_OP_WRITE:
			op->op = CL_MSG_OP_WRITE;
			break;
		case CL_OP_READ:
			op->op = CL_MSG_OP_READ;
			break;
		case CL_OP_ADD:
			op->op = CL_MSG_OP_INCR;
			break;
	}


	// read operations are very simple because you don't have to copy the body
	if (v->op == CL_OP_READ) {
		op->particle_type = 0; // reading - it's unknown
	}
	// write operation - must copy the value
	else {
		uint8_t *data = cl_msg_op_get_value_p(op);
		switch(v->object.type) {
			case CL_NULL:
				op->particle_type = CL_PARTICLE_TYPE_NULL;
				break;
			case CL_INT:
				op->particle_type = CL_PARTICLE_TYPE_INTEGER;
				op->op_sz += value_to_op_int(v->object.u.i64, data);
				break;
			case CL_FLOAT:
				op->particle_type = CL_PARTICLE_TYPE_FLOAT;
				op->op_sz += value_to_op_float(v->object.u.f64, data);
				break;
			case CL_STR:
				op->op_sz += (uint32_t)v->object.size;
				op->particle_type = CL_PARTICLE_TYPE_STRING;
				memcpy(data, v->object.u.str, v->object.size);
				break;
			case CL_BLOB:
				op->op_sz += (uint32_t)v->object.size;
				op->particle_type = CL_PARTICLE_TYPE_BLOB;
				memcpy(data, v->object.u.blob, v->object.size);
				break;
			default:
				cf_warn("internal error value_to_op has unknown value type");
				return;
		}
	}

}


//
// n_values can be passed in 0, and then values is undefined / probably 0.
//
static int
compile(int info1, int info2, const char* ns, const char* set,
		const ev2citrusleaf_object* key, const cf_digest* digest,
		const ev2citrusleaf_write_parameters* wparam, uint32_t timeout,
		const ev2citrusleaf_bin* values, int n_values, uint8_t** buf_r,
		size_t* buf_size_r, cf_digest* digest_r)
{
	// I hate strlen
	int		ns_len = (int)strlen(ns);
	int		set_len = set ? (int)strlen(set) : 0;
	int		i;

	// determine the size
	size_t	msg_size = sizeof(as_msg); // header
	// fields
	if (ns) msg_size += ns_len + sizeof(cl_msg_field);
	if (set) msg_size += set_len + sizeof(cl_msg_field);
	if (key) msg_size += sizeof(cl_msg_field) + 1 + key->size;
	if (digest) msg_size += sizeof(cl_msg_field) + 1 + sizeof(cf_digest);
	// ops
	for (i=0;i<n_values;i++) {
		msg_size += sizeof(cl_msg_op) + strlen(values[i].bin_name);
		if (info2 & CL_MSG_INFO2_WRITE) {
			if (0 != value_to_op_get_size(&values[i].object, &msg_size)) {
				cf_warn("bad operation, writing with unknown type");
				return(-1);
			}
		}
	}

	// size too small? malloc!
	uint8_t	*buf;
	uint8_t *mbuf = 0;
	if ((*buf_r) && (msg_size > *buf_size_r)) {
		mbuf = buf = (uint8_t*)malloc(msg_size);
		if (!buf) 			return(-1);
		*buf_r = buf;
	}
	else
		buf = *buf_r;
	*buf_size_r = msg_size;

	// lay out the header
	uint32_t generation;
	uint32_t expiration;
	if (wparam) {
		if (wparam->use_generation) {
			info2 |= CL_MSG_INFO2_GENERATION;
			generation = wparam->generation;
		}
		else generation = 0;
		expiration = wparam->expiration;
	} else {
		generation = expiration = 0;
	}

	int n_fields = ( ns ? 1 : 0 ) + (set ? 1 : 0) + (key ? 1 : 0) + (digest ? 1 : 0);
	buf = cl_write_header(buf, msg_size, info1, info2, generation,expiration, timeout, n_fields, n_values);

	// now the fields
	buf = write_fields(buf, ns, ns_len, set, set_len, key, digest, digest_r);
	if (!buf) {
		if (mbuf)	free(mbuf);
		return(-1);
	}

	// lay out the ops
	if (n_values) {
		int operation = (info2 & CL_MSG_INFO2_WRITE) ? CL_MSG_OP_WRITE : CL_MSG_OP_READ;

		cl_msg_op *op = (cl_msg_op *) buf;
		cl_msg_op *op_tmp;
		for (i = 0; i< n_values;i++) {

			bin_to_op(operation, &values[i], op);

			op_tmp = cl_msg_op_get_next(op);
			cl_msg_swap_op(op);
			op = op_tmp;
		}
	}
	return(0);
}

//
// A different version of the compile function which takes operations, not values
// The operation is compiled by looking at the internal ops
//
static int
compile_ops(const char* ns, const char* set, const ev2citrusleaf_object* key,
		const cf_digest* digest, const ev2citrusleaf_operation* ops, int n_ops,
		const ev2citrusleaf_write_parameters* wparam, uint8_t** buf_r,
		size_t* buf_size_r, cf_digest* digest_r, bool* write)
{
	int info1 = 0;
	int info2 = 0;

	// I hate strlen
	int		ns_len = (int)strlen(ns);
	int		set_len = (int)strlen(set);
	int		i;

	// determine the size
	size_t	msg_size = sizeof(as_msg); // header
	// fields
	if (ns) msg_size += ns_len + sizeof(cl_msg_field);
	if (set) msg_size += set_len + sizeof(cl_msg_field);
	if (key) msg_size += sizeof(cl_msg_field) + 1 + key->size;
	if (digest) msg_size += sizeof(cl_msg_field) + 1 + sizeof(cf_digest);

	// ops
	for (i=0;i<n_ops;i++) {
		msg_size += sizeof(cl_msg_op) + strlen(ops[i].bin_name);
		if ((ops[i].op == CL_OP_WRITE) || (ops[i].op == CL_OP_ADD)) {
			value_to_op_get_size(&ops[i].object, &msg_size);
			info2 |= CL_MSG_INFO2_WRITE;
		}
		if (ops[i].op == CL_OP_READ) {
			info1 |= CL_MSG_INFO1_READ;
		}
	}
	if (write) { *write = info2 & CL_MSG_INFO2_WRITE ? true : false; }

	// size too small? malloc!
	uint8_t	*buf;
	uint8_t *mbuf = 0;
	if ((*buf_r) && (msg_size > *buf_size_r)) {
		mbuf = buf = (uint8_t*)malloc(msg_size);
		if (!buf) 			return(-1);
		*buf_r = buf;
	}
	else
		buf = *buf_r;
	*buf_size_r = msg_size;

	// lay out the header
	uint32_t generation;
	uint32_t expiration;
	if (wparam) {
		if (wparam->use_generation) {
			info2 |= CL_MSG_INFO2_GENERATION;
			generation = wparam->generation;
		}
		else generation = 0;
		expiration = wparam->expiration;
	} else {
		generation = expiration = 0;
	}

	int n_fields = ( ns ? 1 : 0 ) + (set ? 1 : 0) + (key ? 1 : 0) + (digest ? 1 : 0);
	buf = cl_write_header(buf, msg_size, info1, info2, generation, expiration, expiration, n_fields, n_ops);

	// now the fields
	buf = write_fields(buf, ns, ns_len, set, set_len, key, digest,digest_r);
	if (!buf) {
		if (mbuf)	free(mbuf);
		return(-1);
	}

	// lay out the ops
	if (n_ops) {

		cl_msg_op *op = (cl_msg_op *) buf;
		cl_msg_op *op_tmp;
		for (i = 0; i< n_ops;i++) {

			operation_to_op(&ops[i], op);

			op_tmp = cl_msg_op_get_next(op);
			cl_msg_swap_op(op);
			op = op_tmp;
		}
	}
	return(0);
}



// 0 if OK, -1 if fail

int
set_object(cl_msg_op *op, ev2citrusleaf_object *obj)
{
	obj->type = (ev2citrusleaf_type)op->particle_type;

	switch (op->particle_type) {
		case CL_PARTICLE_TYPE_NULL:
			obj->size = 0;
			obj->free = 0;
			break;

		case CL_PARTICLE_TYPE_INTEGER:
			obj->size = 8;
			obj->free = 0;
			return( op_to_value_int(cl_msg_op_get_value_p(op), cl_msg_op_get_value_sz(op),&(obj->u.i64)) );

		case CL_PARTICLE_TYPE_FLOAT:
			obj->size = 8;
			obj->free = 0;
			return op_to_value_float(cl_msg_op_get_value_p(op), cl_msg_op_get_value_sz(op), &obj->u.f64);

		// regrettably, we have to add the null. I hate null termination.
		case CL_PARTICLE_TYPE_STRING:
			obj->size = cl_msg_op_get_value_sz(op);
			obj->free = obj->u.str = (char*)malloc(obj->size+1);
			if (obj->free == 0) return(-1);
			memcpy(obj->u.str, cl_msg_op_get_value_p(op), obj->size);
			obj->u.str[obj->size] = 0;
			break;

		//
		case CL_PARTICLE_TYPE_BLOB:
		case CL_PARTICLE_TYPE_JAVA_BLOB:
		case CL_PARTICLE_TYPE_CSHARP_BLOB:
		case CL_PARTICLE_TYPE_PYTHON_BLOB:
		case CL_PARTICLE_TYPE_RUBY_BLOB:

			obj->size = cl_msg_op_get_value_sz(op);
			obj->u.blob = cl_msg_op_get_value_p(op);
			obj->free = 0;
			break;

		default:
			cf_warn("parse: internal error: received unknown object type %d",op->particle_type);
			return(-1);
	}
	return(0);
}

//
// Search through the value list and set the pre-existing correct one
// Leads ot n-squared in this section of code
// See other comment....
int
set_value_search(cl_msg_op *op, ev2citrusleaf_bin *values, int n_values)
{
	// currently have to loop through the values to find the right one
	// how that sucks! it's easy to fix eventuallythough
	int i;
	for (i=0;i<n_values;i++)
	{
		if (memcmp(values[i].bin_name, op->name, op->name_sz) == 0)
			break;
	}
	if (i == n_values) {
		cf_warn("set value: but value wasn't there to begin with. Don't understand.");
		return(-1);
	}

	// copy
	set_object(op, &values[i].object);
	return(0);
}


//
// Copy this particular operation to that particular value
void
cl_set_value_particular(cl_msg_op *op, ev2citrusleaf_bin *value)
{
	if (op->name_sz > sizeof(value->bin_name)) {
		cf_warn("Set Value Particular: bad response from server");
		return;
	}

	memcpy(value->bin_name, op->name, op->name_sz);
	value->bin_name[op->name_sz] = 0;
	set_object(op, &value->object);
}


int
parse_get_maxbins(uint8_t *buf, size_t buf_len)
{
	cl_msg	*msg = (cl_msg *)buf;
	return ( ntohs(msg->n_ops) );
}

//
// parse the incoming response buffer, copy the incoming ops into the values array passed in
// which has been pre-allocated on the stack by the caller and will be passed to the
// callback routine then auto-freed stack style
//
// The caller is allows to pass values_r and n_values_r as NULL if it doesn't want those bits
// parsed out.
//
// Unlike some of the read calls, the msg contains all of its data, contiguous
// And has been swapped?

int
parse(uint8_t *buf, size_t buf_len, ev2citrusleaf_bin *values, int n_values,
		int *result_code, uint32_t *generation, uint32_t *p_expiration)
{
	int i;
	cl_msg	*msg = (cl_msg *)buf;
	uint8_t *limit = buf + buf_len;
	buf += sizeof(cl_msg);

	cl_msg_swap_header(msg);

	*result_code = msg->result_code;
	*generation = msg->generation;
	*p_expiration = cf_server_void_time_to_ttl(msg->record_ttl);

	if (msg->n_fields) {
		cl_msg_field *mf = (cl_msg_field *)buf;
		for (i=0;i<msg->n_fields;i++) {

			if ((uint8_t *)mf >= limit) {
				cf_warn("poorly formatted response: fail");
				return(-1);
			}

			cl_msg_swap_field(mf);
			mf = cl_msg_field_get_next(mf);
		}
		buf = (uint8_t *) mf;
	}

	cl_msg_op *op = (cl_msg_op *)buf;

	// if you're interested in the values at all
	if ((0 == values) || (0 == n_values))
		return(0);

	// copy all incoming values into the newly allocated structure
	for (i=0;i<msg->n_ops;i++) {

		if ((uint8_t *)op >= limit) {
			cf_warn("poorly formatted response2");
			return(-1);
		}

		cl_msg_swap_op(op);

		cl_set_value_particular(op, &values[i]);

		op = cl_msg_op_get_next(op);
	}

	return(0);
}


void
ev2citrusleaf_request_complete(cl_request *req, bool timedout)
{
//	dump_buf("request complete :", req->rd_buf, req->rd_buf_size);

	if (req->timeout_set) {
		evtimer_del(cl_request_get_timeout_event(req));
	}

	// critical to close this before the file descriptor associated, for some
	// reason
	if (req->network_set) {
		event_del(cl_request_get_network_event(req));
	}

	// Reuse or close the socket, if it's open.
	if (req->fd > -1) {
		if (req->node) {
			if (! timedout) {
				cl_cluster_node_fd_put(req->node, req->fd);
			}
			else {
				cf_close(req->fd);
				cf_atomic32_decr(&req->node->n_fds_open);
			}

			req->fd = -1;
		}
		else {
			// Since we can't assert:
			cf_error("request has open fd but null node");
		}
	}

	if (timedout == false) {

		// Allocate on the stack for the bins
		int n_bins = parse_get_maxbins(req->rd_buf, req->rd_buf_size);
		ev2citrusleaf_bin   	*bins = 0;
		if (n_bins) bins = (ev2citrusleaf_bin*)alloca(n_bins * sizeof(ev2citrusleaf_bin));

		// parse up into the response
		int			return_code;
		uint32_t	generation;
		uint32_t	expiration;

		parse(req->rd_buf, req->rd_buf_size, bins, n_bins, &return_code, &generation, &expiration);

		// For simplicity & backwards-compatibility, convert server-side
		// timeouts to the usual timeout return-code:
		if (return_code == EV2CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT) {
			return_code = EV2CITRUSLEAF_FAIL_TIMEOUT;
			cf_debug("server-side timeout");
		}

		// Call the callback
		(req->user_cb) (return_code ,bins, n_bins, generation, expiration, req->user_data);

		if (req->node) {
			switch (return_code) {
			// TODO - any other server return codes to consider as failures?
			case EV2CITRUSLEAF_FAIL_TIMEOUT:
				cl_cluster_node_had_failure(req->node);
				cf_atomic_int_incr(&req->asc->n_req_timeouts);
				cf_atomic_int_incr(&req->asc->n_req_failures);
				break;
			default:
				cl_cluster_node_had_success(req->node);
				cf_atomic_int_incr(&req->asc->n_req_successes);
				break;
			}
		}
		else {
			// Since we can't assert:
			cf_error("request succeeded but has null node");
		}
	}

	else {
		// timedout

		// could still be in the cluster's pending queue. Scrub it out.
		MUTEX_LOCK(req->asc->request_q_lock);
		cf_queue_delete(req->asc->request_q ,&req , true /*onlyone*/ );
		MUTEX_UNLOCK(req->asc->request_q_lock);

		// If the request had been popped from the queue, base-hopped, and
		// activated (so it's about to be processed after this event) we need to
		// delete it. Note - using network event slot for base-hop event.
		if (req->base_hop_set) {
			event_del(cl_request_get_network_event(req));
		}

		// call with a timeout specifier
		(req->user_cb) (EV2CITRUSLEAF_FAIL_TIMEOUT , 0, 0, 0, 0, req->user_data);

		if (req->node) {
			cl_cluster_node_had_failure(req->node);
		}

		// The timeout will be counted in the timer callback - we also get here
		// on transaction failures that don't do an internal retry.
		cf_atomic_int_incr(&req->asc->n_req_failures);
	}

	// Release the node.
	if (req->node) {
		cl_cluster_node_put(req->node);
		req->node = 0;
	}

	cf_atomic_int_decr(&req->asc->requests_in_progress);

	cl_request_destroy(req);
}

//
// A quick non-blocking check to see if a server is connected. It may have
// dropped my connection while I'm queued, so don't use those connections
//
// if the fd is connected, we actually expect an error - ewouldblock or similar
//
int
ev2citrusleaf_is_connected(int fd)
{
	uint8_t buf[8];
	int rv = recv(fd, (cf_socket_data_t*)buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT | MSG_NOSIGNAL);
	if (rv == 0) {
		cf_debug("connected check: found disconnected fd %d", fd);
		return(CONNECTED_NOT);
	}

	if (rv < 0) {
		if (errno == EBADF) {
			cf_warn("connected check: EBADF fd %d", fd);
			return(CONNECTED_BADFD);
		}
		else if ((errno == EWOULDBLOCK) || (errno == EAGAIN)) {
			return(CONNECTED);
		}
		else {
			cf_info("connected check: fd %d error %d", fd, errno);
			return(CONNECTED_ERROR);
		}
	}

	return(CONNECTED);
}


static inline void
req_cross_thread_init_and_lock(cl_request* req)
{
	if (req->asc->static_options.cross_threaded) {
		MUTEX_ALLOC(req->cross_thread_lock);
		MUTEX_LOCK(req->cross_thread_lock);
		req->cross_thread_locked = true;
	}
}

static inline void
req_cross_thread_unlock(cl_request* req)
{
	if (req->cross_thread_lock) {
		req->cross_thread_locked = false;
		MUTEX_UNLOCK(req->cross_thread_lock);
	}
}

static inline void
event_cross_thread_check(cl_request* req)
{
	// In cross-threaded transaction models, events firing in the callback
	// thread need to be sure the original non-blocking call is complete.
	if (req->cross_thread_lock) {
		MUTEX_LOCK(req->cross_thread_lock);
		MUTEX_UNLOCK(req->cross_thread_lock);
	}
}


//
// Got an event on one of our file descriptors. DTRT.
// NETWORK EVENTS ONLY
void
ev2citrusleaf_event(evutil_socket_t fd, short event, void *udata)
{
	cl_request *req = (cl_request*)udata;

	if (req->MAGIC != CL_REQUEST_MAGIC)	{
		cf_error("network event: BAD MAGIC");
		return;
	}

	int rv;

	uint64_t _s = cf_getms();

	event_cross_thread_check(req);

	req->network_set = false;

	if (event & EV_WRITE) {
		if (req->wr_buf_pos < req->wr_buf_size) {
			rv = send(fd, (cf_socket_data_t*)&req->wr_buf[req->wr_buf_pos], (cf_socket_size_t)(req->wr_buf_size - req->wr_buf_pos), MSG_DONTWAIT | MSG_NOSIGNAL);

			if (rv > 0) {
				req->wr_buf_pos += rv;
				if (req->wr_buf_pos == req->wr_buf_size) {
					event_assign(cl_request_get_network_event(req),req->base ,fd, EV_READ, ev2citrusleaf_event, req);
				}
			}
			// according to man, send never returns 0. But do we trust it?
			else if (rv == 0) {
				cf_debug("ev2citrusleaf_write failed with 0, posix not followed: fd %d rv %d errno %d", fd, rv, errno);
				goto Fail;
			}
			else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
				cf_debug("ev2citrusleaf_write failed: fd %d rv %d errno %d", fd, rv, errno);
				goto Fail;
			}

		}
	}

	if (event & EV_READ) {
		if (req->rd_header_pos < sizeof(cl_proto) ) {
			rv = recv(fd, (cf_socket_data_t*)&req->rd_header_buf[req->rd_header_pos], (cf_socket_size_t)(sizeof(cl_proto) - req->rd_header_pos), MSG_DONTWAIT | MSG_NOSIGNAL);

			if (rv > 0) {
				req->rd_header_pos += rv;
			}
			else if (rv == 0) {
				// connection has been closed by the server. A normal occurrance, perhaps.
				cf_debug("ev2citrusleaf read2: connection closed: fd %d rv %d errno %d", fd, rv, errno);
				goto Fail;
			}
			else {
				if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
					cf_debug("read failed: rv %d errno %d", rv, errno);
					goto Fail;
				}
			}
		}

		if (req->rd_header_pos == sizeof(cl_proto)) {
			// initialize the read buffer
			if (req->rd_buf_size == 0) {
				// calculate msg size
				cl_proto *proto = (cl_proto *) req->rd_header_buf;
				cl_proto_swap(proto);

				// set up the read buffer
				if (proto->sz <= sizeof(req->rd_tmp))
					req->rd_buf = req->rd_tmp;
				else {
					req->rd_buf = (uint8_t*)malloc(proto->sz);
					if (!req->rd_buf) {
						cf_error("malloc fail");
						goto Fail;
					}
				}
				req->rd_buf_pos = 0;
				req->rd_buf_size = proto->sz;
			}
			if (req->rd_buf_pos < req->rd_buf_size) {
				rv = recv(fd, (cf_socket_data_t*)&req->rd_buf[req->rd_buf_pos], (cf_socket_size_t)(req->rd_buf_size - req->rd_buf_pos), MSG_DONTWAIT | MSG_NOSIGNAL);

				if (rv > 0) {
					req->rd_buf_pos += rv;
					if (req->rd_buf_pos == req->rd_buf_size) {
						ev2citrusleaf_request_complete(req, false); // frees the req
						req = 0;
						return;
					}
				}
				else if (rv == 0) {
					// connection has been closed by the server. Errno is invalid. A normal occurrance, perhaps.
					cf_debug("ev2citrusleaf read2: connection closed: fd %d rv %d errno %d", fd, rv, errno);
					goto Fail;
				}
				else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
					cf_debug("ev2citrusleaf read2: fail: fd %d rv %d errno %d", fd, rv, errno);
					goto Fail;
				}
			}
		}
		else {
			cf_debug("ev2citrusleaf event: received read while not expecting fd %d", fd);
		}

	}

	if (req) {
		if (0 == event_add(cl_request_get_network_event(req), 0 /*timeout*/)) {
			req->network_set = true;
		}
		else req->network_set = false;
	}

	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info(" *** event took %lu", delta);

	return;

Fail:
	cf_close(fd);
	req->fd = -1;

	if (req->node) {
		cf_atomic32_decr(&req->node->n_fds_open);
	}
	else {
		// Since we can't assert:
		cf_error("request network event has null node");
	}

	if (req->wpol == CL_WRITE_ONESHOT) {
		cf_info("ev2citrusleaf: write oneshot with network error, terminating now");
		// So far we're not distinguishing whether the failure was a local or
		// remote problem. It will be treated as remote and counted against the
		// node for throttle-control purposes.
		ev2citrusleaf_request_complete(req, true);
	}
	else {
		cf_debug("ev2citrusleaf failed a request, calling restart");

		if (req->node) {
			cl_cluster_node_put(req->node);
			req->node = 0;
		}
		// else - already "asserted".

		cf_atomic_int_incr(&req->asc->n_internal_retries);
		ev2citrusleaf_restart(req, false);
	}

	delta =  cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info(" *** event fail took %lu", delta);
}

//
// A timer has gone off on a request
// fd is not set

void
ev2citrusleaf_timer_expired(evutil_socket_t fd, short event, void *udata)
{
	cl_request *req = (cl_request*)udata;

	if (req->MAGIC != CL_REQUEST_MAGIC)	{
		cf_error("timer expired: BAD MAGIC");
		return;
	}

	uint64_t _s = cf_getms();

	event_cross_thread_check(req);

	if (req->cross_thread_lock && ! req->timeout_set) {
		// In the cross-threaded model, if the non-blocking call fails we use
		// the timeout_set flag (double-purposed!) to tell this event to just
		// destroy the cl_request and stop.
		cl_request_destroy(req);
		return;
	}

	req->timeout_set = false;

	cf_atomic_int_incr(&req->asc->n_req_timeouts);
	ev2citrusleaf_request_complete(req, true /*timedout*/); // frees the req

	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: timer expired took %lu", delta);
}


static void
ev2citrusleaf_base_hop_event(evutil_socket_t fd, short event, void *udata)
{
	cl_request* req = (cl_request*)udata;

	if (req->MAGIC != CL_REQUEST_MAGIC)	{
		cf_error("base hop event: BAD MAGIC");
		return;
	}

	event_cross_thread_check(req);

	req->base_hop_set = false;

	cf_debug("have node now, restart request %p", req);

	cf_atomic_int_incr(&req->asc->n_internal_retries_off_q);
	ev2citrusleaf_restart(req, false);
}


void
ev2citrusleaf_base_hop(cl_request *req)
{
	// We'll use the unused network event slot.
	event_assign(cl_request_get_network_event(req), req->base, -1, 0, ev2citrusleaf_base_hop_event, req);

	if (0 != event_add(cl_request_get_network_event(req), 0)) {
		cf_warn("unable to add base-hop event for request %p: will time out", req);
		return;
	}

	req->base_hop_set = true;

	// Tell the event to fire on the appropriate base ASAP.
	event_active(cl_request_get_network_event(req), 0, 0);
}


// Return values:
// true  - success, or will time out, or queued for internal retry
// false - throttled
bool
ev2citrusleaf_restart(cl_request* req, bool may_throttle)
{
	// If we've already timed out, don't bother adding the network event, just
	// let the timeout event (which no doubt is about to fire) clean up.
	if (req->timeout_ms > 0 && req->start_time + req->timeout_ms < cf_getms()) {
		return true;
	}

	// Set/reset state to beginning of transaction.
	req->wr_buf_pos = 0;
	req->rd_buf_pos = 0;
	req->rd_header_pos = 0;

	// Sanity checks.
	if (req->node) {
		cf_error("req has node %s on restart", req->node->name);
	}

	if (req->fd != -1) {
		cf_error("req has fd %d on restart", req->fd);
	}

	req->node = 0;
	req->fd = -1;

	cl_cluster_node* node;
	int fd;
	int i;

	for (i = 0; i < 5; i++) {
		node = cl_cluster_node_get(req->asc, req->ns, &req->d, req->write);

		if (! node) {
			cf_queue_push(req->asc->request_q, &req);
			return true;
		}

		// Throttle before bothering to get the socket.
		if (may_throttle && cl_cluster_node_throttle_drop(node)) {
			// Randomly dropping this transaction in order to throttle.
			cf_atomic_int_incr(&req->asc->n_req_throttles);
			cl_cluster_node_put(node);
			return false;
		}

		fd = -1;

		while (fd == -1) {
			fd = cl_cluster_node_fd_get(node);
		}

		if (fd > -1) {
			// Got a good socket.
			break;
		}

		// Couldn't get a socket, try again from scratch. Probably we'll get the
		// same node, but for normal reads or if we got a random node we could
		// get a different node.
		cl_cluster_node_put(node);
	}

	// Safety - don't retry from scratch forever.
	if (i == 5) {
		cf_info("request restart loop quit after 5 tries");
		cf_queue_push(req->asc->request_q, &req);
		return true;
	}

	// Go ahead, using the good node and socket.
	req->node = node;
	req->fd = fd;

	event_assign(cl_request_get_network_event(req), req->base, fd, EV_WRITE,
			ev2citrusleaf_event, req);

	req->network_set = true;

	if (0 != event_add(cl_request_get_network_event(req), 0 /*timeout*/)) {
		cf_warn("unable to add event for request %p: will time out", req);
		req->network_set = false;
	}

	return true;
}


void
start_failed(cl_request* req)
{
	if (! req->timeout_set) {
		cl_request_destroy(req);
		return;
	}

	if (req->cross_thread_lock) {
		// Unfortunately, in the cross-threaded model we have no idea whether
		// the timer has fired (and is waiting at the lock) or not, so we can't
		// just unlock and destroy - the destroy would race the fired timer. The
		// only way to safely destroy is to let the timer event do it.

		// Tell the timer event that the non-blocking call failed.
		req->timeout_set = false;
		req_cross_thread_unlock(req);
		// ... and don't destroy - the timer event will do it.
	}
	else {
		event_del(cl_request_get_timeout_event(req));
		cl_request_destroy(req);
	}
}


//
// Omnibus internal function used by public transactions API.
//
int
ev2citrusleaf_start(cl_request* req, int info1, int info2, const char* ns,
		const char* set, const ev2citrusleaf_object* key,
		const cf_digest* digest, const ev2citrusleaf_write_parameters* wparam,
		const ev2citrusleaf_bin* bins, int n_bins)
{
	if (! req) {
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	req_cross_thread_init_and_lock(req);

	// To implement timeout, add timer event in parallel to network event chain.
	if (req->timeout_ms) {
		if (req->timeout_ms < 0) {
			cf_warn("timeout < 0");
			cl_request_destroy(req);
			return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
		}

		if (req->timeout_ms > 1000 * 60) {
			cf_info("timeout > 60 seconds");
		}

		evtimer_assign(cl_request_get_timeout_event(req), req->base,
				ev2citrusleaf_timer_expired, req);

		struct timeval tv;
		tv.tv_sec = req->timeout_ms / 1000;
		tv.tv_usec = (req->timeout_ms % 1000) * 1000;

		if (0 != evtimer_add(cl_request_get_timeout_event(req), &tv)) {
			cf_warn("request add timer failed");
			cl_request_destroy(req);
			return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
		}

		req->timeout_set = true;
	}
	// else there's no timeout - supported, but a bit dangerous.

    req->start_time = cf_getms();
	req->wr_buf = req->wr_tmp;
	req->wr_buf_size = sizeof(req->wr_tmp);
	req->write = (info2 & CL_MSG_INFO2_WRITE) ? true : false;
	strcpy(req->ns, ns);

	// Fill out the request write buffer.
	if (0 != compile(info1, info2, ns, set, key, digest, wparam,
			req->timeout_ms, bins, n_bins, &req->wr_buf, &req->wr_buf_size,
			&req->d)) {
		start_failed(req);
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

//	dump_buf("sending request to cluster:", req->wr_buf, req->wr_buf_size);

	// Determine whether we may throttle.
	bool may_throttle = req->write ?
			cf_atomic32_get(req->asc->runtime_options.throttle_writes) != 0 :
			cf_atomic32_get(req->asc->runtime_options.throttle_reads) != 0;

	// Initial restart - get node and socket and initiate network event chain.
	if (! ev2citrusleaf_restart(req, may_throttle)) {
		start_failed(req);
		return EV2CITRUSLEAF_FAIL_THROTTLED;
	}

	cf_atomic_int_incr(&req->asc->requests_in_progress);
	req_cross_thread_unlock(req);

	return EV2CITRUSLEAF_OK;
}


//
// Internal function used by public operate transaction API.
//
int
ev2citrusleaf_start_op(cl_request* req, const char* ns, const char* set,
		const ev2citrusleaf_object* key, const cf_digest* digest,
		const ev2citrusleaf_operation* ops, int n_ops,
		const ev2citrusleaf_write_parameters* wparam)
{
	if (! req) {
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	req_cross_thread_init_and_lock(req);

	// To implement timeout, add timer event in parallel to network event chain.
	if (req->timeout_ms) {
		if (req->timeout_ms < 0) {
			cf_warn("timeout < 0");
			cl_request_destroy(req);
			return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
		}

		if (req->timeout_ms > 1000 * 60) {
			cf_info("timeout > 60 seconds");
		}

		evtimer_assign(cl_request_get_timeout_event(req), req->base,
				ev2citrusleaf_timer_expired, req);

		struct timeval tv;
		tv.tv_sec = req->timeout_ms / 1000;
		tv.tv_usec = (req->timeout_ms % 1000) * 1000;

		if (0 != evtimer_add(cl_request_get_timeout_event(req), &tv)) {
			cf_warn("request add timer failed");
			cl_request_destroy(req);
			return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
		}

		req->timeout_set = true;
	}
	// else there's no timeout - supported, but a bit dangerous.

    req->start_time = cf_getms();
	req->wr_buf = req->wr_tmp;
	req->wr_buf_size = sizeof(req->wr_tmp);
	strcpy(req->ns, ns);

	// Fill out the request write buffer.
	if (0 != compile_ops(ns, set, key, digest, ops, n_ops, wparam, &req->wr_buf,
			&req->wr_buf_size, &req->d, &req->write)) {
		start_failed(req);
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

//	dump_buf("sending request to cluster:", req->wr_buf, req->wr_buf_size);

	// Initial restart - get node and socket and initiate network event chain.
	if (! ev2citrusleaf_restart(req, false)) {
		start_failed(req);
		return EV2CITRUSLEAF_FAIL_THROTTLED;
	}

	cf_atomic_int_incr(&req->asc->requests_in_progress);
	req_cross_thread_unlock(req);

	return EV2CITRUSLEAF_OK;
}



//
// head functions
//

int
ev2citrusleaf_get_all(ev2citrusleaf_cluster *cl, char *ns, char *set, ev2citrusleaf_object *key,
	int timeout_ms, ev2citrusleaf_callback cb, void *udata, struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, NULL, cb, udata);

	return ev2citrusleaf_start(req, CL_MSG_INFO1_READ | CL_MSG_INFO1_GET_ALL, 0, ns, set, key, 0/*digest*/, 0, 0, 0);
}

int
ev2citrusleaf_get_all_digest(ev2citrusleaf_cluster *cl, char *ns, cf_digest *digest,
	int timeout_ms, ev2citrusleaf_callback cb, void *udata, struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, NULL, cb, udata);

	return ev2citrusleaf_start(req, CL_MSG_INFO1_READ | CL_MSG_INFO1_GET_ALL, 0, ns, 0/*set*/, 0/*key*/, digest, 0, 0, 0);
}

int
ev2citrusleaf_put(ev2citrusleaf_cluster *cl, char *ns, char *set, ev2citrusleaf_object *key,
	ev2citrusleaf_bin *bins, int n_bins, ev2citrusleaf_write_parameters *wparam, int timeout_ms,
	ev2citrusleaf_callback cb, void *udata, struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, wparam, cb, udata);

	return ev2citrusleaf_start(req, 0, CL_MSG_INFO2_WRITE, ns, set, key, 0/*digest*/, wparam, bins, n_bins);
}

int
ev2citrusleaf_put_digest(ev2citrusleaf_cluster *cl, char *ns, cf_digest *digest,
	ev2citrusleaf_bin *bins, int n_bins, ev2citrusleaf_write_parameters *wparam, int timeout_ms,
	ev2citrusleaf_callback cb, void *udata, struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, wparam, cb, udata);

	return ev2citrusleaf_start(req, 0, CL_MSG_INFO2_WRITE, ns, 0/*set*/, 0/*key*/, digest, wparam, bins, n_bins);
}

int
ev2citrusleaf_get(ev2citrusleaf_cluster *cl, char *ns, char *set, ev2citrusleaf_object *key,
	const char **bin_names, int n_bin_names, int timeout_ms, ev2citrusleaf_callback cb, void *udata,
	struct event_base *base)
{
	ev2citrusleaf_bin* bins = (ev2citrusleaf_bin*)alloca(n_bin_names * sizeof(ev2citrusleaf_bin));

	for (int i = 0; i < n_bin_names; i++) {
		strcpy(bins[i].bin_name, bin_names[i]);
		bins[i].object.type = CL_NULL;
	}

	cl_request* req = cl_request_create(cl, base, timeout_ms, NULL, cb, udata);

	return ev2citrusleaf_start(req, CL_MSG_INFO1_READ, 0, ns, set, key, 0/*digest*/, 0, bins, n_bin_names);
}

int
ev2citrusleaf_get_digest(ev2citrusleaf_cluster *cl, char *ns, cf_digest *digest,
	const char **bin_names, int n_bin_names, int timeout_ms, ev2citrusleaf_callback cb, void *udata,
	struct event_base *base)
{
	ev2citrusleaf_bin* bins = (ev2citrusleaf_bin*)alloca(n_bin_names * sizeof(ev2citrusleaf_bin));

	for (int i = 0; i < n_bin_names; i++) {
		strcpy(bins[i].bin_name, bin_names[i]);
		bins[i].object.type = CL_NULL;
	}

	cl_request* req = cl_request_create(cl, base, timeout_ms, NULL, cb, udata);

	return ev2citrusleaf_start(req, CL_MSG_INFO1_READ, 0, ns, 0/*set*/, 0/*key*/, digest, 0, bins, n_bin_names);
}

int
ev2citrusleaf_delete(ev2citrusleaf_cluster *cl, char *ns, char *set, ev2citrusleaf_object *key,
	ev2citrusleaf_write_parameters *wparam, int timeout_ms, ev2citrusleaf_callback cb, void *udata,
	struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, wparam, cb, udata);

	return ev2citrusleaf_start(req, 0, CL_MSG_INFO2_WRITE | CL_MSG_INFO2_DELETE, ns, set, key, 0/*digest*/, wparam, 0, 0);
}

int
ev2citrusleaf_delete_digest(ev2citrusleaf_cluster *cl, char *ns, cf_digest *digest,
	ev2citrusleaf_write_parameters *wparam, int timeout_ms, ev2citrusleaf_callback cb, void *udata,
	struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, wparam, cb, udata);

	return ev2citrusleaf_start(req, 0, CL_MSG_INFO2_WRITE | CL_MSG_INFO2_DELETE, ns, 0/*set*/, 0/*key*/, digest, wparam, 0, 0);
}

int
ev2citrusleaf_operate(ev2citrusleaf_cluster *cl, char *ns, char *set, ev2citrusleaf_object *key,
	ev2citrusleaf_operation *ops, int n_ops, ev2citrusleaf_write_parameters *wparam,
	int timeout_ms, ev2citrusleaf_callback cb, void *udata, struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, wparam, cb, udata);

	return ev2citrusleaf_start_op(req, ns, set, key, 0/*digest*/, ops, n_ops, wparam);
}

int
ev2citrusleaf_operate_digest(ev2citrusleaf_cluster *cl, char *ns, cf_digest *digest,
	ev2citrusleaf_operation *ops, int n_ops, ev2citrusleaf_write_parameters *wparam,
	int timeout_ms, ev2citrusleaf_callback cb, void *udata, struct event_base *base)
{
	cl_request* req = cl_request_create(cl, base, timeout_ms, wparam, cb, udata);

	return ev2citrusleaf_start_op(req, ns, 0/*set*/, 0/*key*/, digest, ops, n_ops, wparam);
}


bool g_ev2citrusleaf_initialized = false;

int ev2citrusleaf_init(ev2citrusleaf_lock_callbacks *lock_cb)
{
	if (g_ev2citrusleaf_initialized) {
		cf_info("citrusleaf: init called twice, benign");
		return(0);
	}

	g_ev2citrusleaf_initialized = true;

	extern char *citrusleaf_build_string;
	cf_info("Aerospike client version %s", citrusleaf_build_string);

	// TODO - add extra API to specify no locking (for single-threaded use).
	if (lock_cb) {
		g_lock_cb = lock_cb;
	}
	else {
		g_lock_cb = &g_default_lock_callbacks;
		g_lock_cb->alloc = mutex_alloc;
		g_lock_cb->free = mutex_free;
		g_lock_cb->lock = mutex_lock;
		g_lock_cb->unlock = mutex_unlock;
	}

	// Tell cf_base code to use the same locking calls as we'll use here:
	cf_hook_mutex(g_lock_cb);

	memset((void*)&g_cl_stats, 0, sizeof(g_cl_stats));

	citrusleaf_cluster_init();

	srand(cf_clepoch_seconds());

	return(0);
}

// TODO - get rid of unused param at next API change.
void
ev2citrusleaf_shutdown(bool fail_requests)
{
	citrusleaf_cluster_shutdown();
	g_ev2citrusleaf_initialized = false;
}


//==========================================================
// Statistics
//

cl_statistics g_cl_stats;

void
cluster_print_stats(ev2citrusleaf_cluster* asc)
{
	// Match with the log level below.
	if (! cf_info_enabled()) {
		return;
	}

	// Collect per-node info.
	MUTEX_LOCK(asc->node_v_lock);

	uint32_t n_nodes = cf_vector_size(&asc->node_v);
	uint32_t n_fds_open = 0;
	uint32_t n_fds_pooled = 0;

	for (uint32_t i = 0; i < n_nodes; i++) {
		cl_cluster_node* cn = (cl_cluster_node*)
				cf_vector_pointer_get(&asc->node_v, i);

		n_fds_open += cf_atomic32_get(cn->n_fds_open);
		n_fds_pooled += cf_queue_sz(cn->conn_q);
	}

	MUTEX_UNLOCK(asc->node_v_lock);

	// Most of the stats below are cf_atomic_int, and should be accessed with
	// cf_atomic_int_get(), but since I know that's a no-op wrapper I'm being
	// lazy and leaving the code below as-is -- AKG.

	// Global (non cluster-related) stats first.
	cf_info("stats :: global ::");
	cf_info("      :: app-info %lu", g_cl_stats.app_info_requests);

	// Cluster stats.
	cf_info("stats :: cluster %p ::", asc);
	cf_info("      :: nodes : created %lu destroyed %lu current %u", asc->n_nodes_created, asc->n_nodes_destroyed, n_nodes);
	cf_info("      :: tend-pings : success %lu fail %lu", asc->n_ping_successes, asc->n_ping_failures);
	cf_info("      :: node-info-reqs : success %lu fail %lu timeout %lu", asc->n_node_info_successes, asc->n_node_info_failures, asc->n_node_info_timeouts);
	cf_info("      :: reqs : success %lu fail %lu timeout %lu throttle %lu in-progress %lu", asc->n_req_successes, asc->n_req_failures, asc->n_req_timeouts, asc->n_req_throttles, asc->requests_in_progress);
	cf_info("      :: req-retries : direct %lu off-q %lu : on-q %d", asc->n_internal_retries, asc->n_internal_retries_off_q, cf_queue_sz(asc->request_q));
	cf_info("      :: batch-node-reqs : success %lu fail %lu timeout %lu", asc->n_batch_node_successes, asc->n_batch_node_failures, asc->n_batch_node_timeouts);
	cf_info("      :: fds : open %u pooled %u", n_fds_open, n_fds_pooled);
}

// TODO - deprecate cluster list and add cluster param to this API call?
void
ev2citrusleaf_print_stats(void)
{
	// Loop over all clusters.
	for (cf_ll_element* e = cf_ll_get_head(&cluster_ll); e; e = cf_ll_get_next(e)) {
		ev2citrusleaf_cluster* asc = (ev2citrusleaf_cluster*)e;

		cluster_print_stats(asc);
	}
}
