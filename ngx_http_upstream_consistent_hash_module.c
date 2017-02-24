/*
 * Hash a variable to choose an upstream server.
 *
 *
 * This module can be distributed under the same terms as Nginx itself.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#if (NGX_HTTP_HEALTHCHECK)
#include <ngx_http_healthcheck_module.h>
#endif

#if (NGX_UPSTREAM_CHECK_MODULE)
#include "ngx_http_upstream_check_handler.h"
#endif

#include "conhash.h"
#include "configure.h"

//每个实节点虚节点数= MMC_CONSISTENT_POINTS*weight 
#define MMC_CONSISTENT_POINTS 1024
//实节点数组，静态分配,实际占用的是server个数
struct node_s g_nodes[64];
struct conhash_s                  *conhash;

#define MAX_PEER_FAILED 5

#define ngx_bitvector_index(index) (index / (8 * sizeof(uintptr_t)))
#define ngx_bitvector_bit(index) ((uintptr_t) 1 << (index % (8 * sizeof(uintptr_t))))

static ngx_array_t * ngx_http_upstream_consistent_hash_key_lengths;
static ngx_array_t * ngx_http_upstream_consistent_hash_key_values;

typedef struct {
    ngx_array_t  *values;
    ngx_array_t  *lengths;
    //重试次数
	ngx_uint_t    retries;
} ngx_http_upstream_consistent_hash_conf_t;


typedef struct {
    struct sockaddr                *sockaddr;
    socklen_t                       socklen;
    ngx_str_t                       name;
    ngx_uint_t                      down;
    ngx_int_t                       weight;
	//consistent hash real node
	struct node_s 			       *node;
	ngx_uint_t 				       del_flag; 
    //each peer call times
    ngx_uint_t                     count;
#if (NGX_HTTP_HEALTHCHECK)
    ngx_int_t                       health_index;
#endif

#if (NGX_UPSTREAM_CHECK_MODULE)
	 ngx_uint_t check_index;
#endif

#if (NGX_HTTP_SSL)
    ngx_ssl_session_t              *ssl_session;   /* local to a process */
#endif
} ngx_http_upstream_consistent_hash_peer_t;

//虚拟节点只有key值，可在rb tree中找到

typedef struct {
    //ip address 总数
	ngx_uint_t                        number;
    ngx_uint_t 						  nreal;
	//ngx_uint_t                        total_weight;
    //虚拟节点总数
	ngx_uint_t                        total_point;
    //unsigned                          weighted:1;
    ngx_http_upstream_consistent_hash_peer_t     peer[0];
	//struct conhash_s                  *conhash;
} ngx_http_upstream_consistent_hash_peers_t;

typedef struct {
    ngx_http_upstream_consistent_hash_peers_t   *peers;
    //uint32_t                          hash;
    ngx_str_t                         current_key;
    ngx_uint_t                        current;
    uintptr_t                         tried[1];
} ngx_http_upstream_consistent_hash_peer_data_t;


static ngx_uint_t ngx_http_upstream_get_consistent_hash_peer_index(
        ngx_http_upstream_consistent_hash_peer_data_t *uhpd, ngx_log_t *log);
static void ngx_http_upstream_consistent_hash_next_peer(ngx_http_upstream_consistent_hash_peer_data_t *uhpd,
        ngx_uint_t *tries, ngx_log_t *log);
static ngx_int_t ngx_http_upstream_init_consistent_hash_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_consistent_hash_peer(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_free_consistent_hash_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
#if (NGX_HTTP_SSL)
static ngx_int_t ngx_http_upstream_set_consistent_hash_peer_session(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_save_consistent_hash_peer_session(ngx_peer_connection_t *pc,
    void *data);
#endif
static char *ngx_http_upstream_consistent_hash(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_upstream_init_consistent_hash(ngx_conf_t *cf,
    ngx_http_upstream_srv_conf_t *us);


static ngx_command_t  ngx_http_upstream_consistent_hash_commands[] = {
    { ngx_string("hash"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_http_upstream_consistent_hash,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_upstream_consistent_hash_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    //ngx_http_upstream_consistent_hash_create_conf,    /* create server configuration */
    NULL,
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};


ngx_module_t  ngx_http_upstream_consistent_hash_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_consistent_hash_module_ctx,    /* module context */
    ngx_http_upstream_consistent_hash_commands,       /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

//uhpd 请求的url 返回uhpd中peers的index
static ngx_uint_t
ngx_http_upstream_get_consistent_hash_peer_index(ngx_http_upstream_consistent_hash_peer_data_t *uhpd, ngx_log_t *log)
{
	ngx_uint_t i;
	const struct node_s *node;
	ngx_str_t key = uhpd->current_key;
	u_char *str = (u_char*)malloc( key.len+1);
	ngx_snprintf(str, key.len+1, "%V", &key);
	//找到请求最近的实节点返回
	node = conhash_lookup(conhash, str);
	//返回peer索引
	for(i = 0; i < uhpd->peers->number; i++) {
		if(uhpd->peers->peer[i].del_flag)
		  continue;
		if(uhpd->peers->peer[i].node == node) { 
			ngx_log_debug2(NGX_LOG_DEBUG_HTTP, log, 0,
                           "peer[%ui]: count=%ui",
                           i, 
                           ++(uhpd->peers->peer[i].count));
           
           return i;
		}
	}

	/* unreachable */
    return 0;
}


static ngx_int_t
ngx_http_upstream_init_consistent_hash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    u_char                          *hash_data;
	ngx_uint_t                       i, j, n, w;
    ngx_http_upstream_server_t      *server;
    ngx_http_upstream_consistent_hash_peers_t  *peers;
	//ngx_uint_t point;
#if (NGX_HTTP_HEALTHCHECK)
    ngx_int_t                        health_index;
#endif

#if (NGX_UPSTREAM_CHECK_MODULE)
    //ngx_int_t                        check_index;
#endif
    us->peer.init = ngx_http_upstream_init_consistent_hash_peer;

    if (!us->servers) {
        return NGX_ERROR;
    }

    server = us->servers->elts;

	hash_data = ngx_pcalloc(cf->pool, sizeof("255.255.255.255:65535-65535"));
	if (hash_data == NULL) {
	         return NGX_ERROR;
	}


    n = 0;
    w = 0;
    for (i = 0; i < us->servers->nelts; i++) {
        if (server[i].down)
            continue;

        n += server[i].naddrs;
        w += server[i].naddrs * server[i].weight * MMC_CONSISTENT_POINTS;
    }

    if (n == 0) {
        return NGX_ERROR;
    }

    peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_hash_peers_t)
            + sizeof(ngx_http_upstream_consistent_hash_peer_t) * n);

    if (peers == NULL) {
        return NGX_ERROR;
    }
	
	conhash = conhash_init(NULL);
    peers->number = n;
    peers->total_point = w;

    n = 0;
    /* one hostname can have multiple IP addresses in DNS */
    for (i = 0; i < us->servers->nelts; i++) {
        for (j = 0; j < server[i].naddrs; j++) {
            //down do not add into real 
			if (server[i].down)
                continue;
			// 初始化实节点
            peers->peer[n].sockaddr = server[i].addrs[j].sockaddr;
            peers->peer[n].socklen = server[i].addrs[j].socklen;
            peers->peer[n].name = server[i].addrs[j].name;
            peers->peer[n].down = server[i].down;
            peers->peer[n].weight = server[i].weight;
            peers->peer[n].del_flag = 0;
            peers->peer[n].count = 0;

			//set real node
			peers->peer[n].node = &g_nodes[n];
			//set virtual node
			ngx_snprintf(hash_data, sizeof("255.255.255.255:65535-65535"), "%V", &server[i].addrs[j].name);
			if(NULL == conhash) 
				return NGX_ERROR;
			conhash_set_node(peers->peer[n].node, (char *)hash_data, peers->peer[n].weight * MMC_CONSISTENT_POINTS);
			ngx_log_debug2(NGX_LOG_DEBUG_HTTP, cf->log, 0,
						                    "upstream_hash:init_conf:current_keyconhash_set_node: hash_data: %V, server[i].down:%ui",
						                    hash_data, server[i].down);
			/*  add nodes */
		    conhash_add_node(conhash, peers->peer[n].node);
			peers->nreal++;
#if (NGX_HTTP_HEALTHCHECK)
            if (!server[i].down) {
                health_index =
                    ngx_http_healthcheck_add_peer(us,
                    &server[i].addrs[j], cf->pool);
                if (health_index == NGX_ERROR) {
                    return NGX_ERROR;
                }
                peers->peer[n].health_index = health_index;
            }
#endif
//标记为down的机器不加入peers
#if (NGX_UPSTREAM_CHECK_MODULE)
			if (!server[i].down) {
				peers->peer[n].check_index =
					ngx_http_check_add_peer(cf, us, &server[i].addrs[j]);
				/*  add nodes */
			}
			else {
				peers->peer[n].check_index = (ngx_uint_t) NGX_ERROR;
			}
#endif
            n++;
        }
    }

    us->peer.data = peers;

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_init_consistent_hash_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_consistent_hash_peer_data_t     *uhpd;

    ngx_str_t val;

    if (ngx_http_script_run(r, &val, 
                            ngx_http_upstream_consistent_hash_key_lengths->elts, 0,
                            ngx_http_upstream_consistent_hash_key_values->elts) == NULL) {
        return NGX_ERROR;    
    }

    uhpd = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_consistent_hash_peer_data_t)
            + sizeof(uintptr_t) *
                ((ngx_http_upstream_consistent_hash_peers_t *)us->peer.data)->number /
                    (8 * sizeof(uintptr_t)));
    if (uhpd == NULL) {
        return NGX_ERROR;
    }

	r->upstream->peer.data = uhpd;    
    uhpd->peers = us->peer.data;

	ngx_uint_t i;
	//每次请求到来时检查被踢掉的peer是否连上，是将其加入到哈希环中
	for(i = 0; i < uhpd->peers->number; i++) {
		ngx_log_debug4(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
            "upstream_hash:request:add peer i:%ui,check_index:%ui, del_flag:%ui,isdowm:%ui", 
            i,
            uhpd->peers->peer[i].check_index,
            uhpd->peers->peer[i].del_flag,
            ngx_http_check_peer_down(uhpd->peers->peer[i].check_index) 
            );
		
        if(!uhpd->peers->peer[i].del_flag)
		  continue;
 #if (NGX_UPSTREAM_CHECK_MODULE)
		 if(!ngx_http_check_peer_down(uhpd->peers->peer[i].check_index)) {
			 /*   add nodes */
    	     conhash_add_node(conhash, uhpd->peers->peer[i].node);
			 uhpd->peers->nreal++;
			 uhpd->peers->peer[i].del_flag = !(uhpd->peers->peer[i].del_flag);
			 ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
	                    "upstream_hash:request:down peer %ui is reconnect", i);
		 }
 #endif		
	}

    r->upstream->peer.free = ngx_http_upstream_free_consistent_hash_peer;
    r->upstream->peer.get = ngx_http_upstream_get_consistent_hash_peer;

	if (uhpd->peers->number < MAX_PEER_FAILED)
	    r->upstream->peer.tries = uhpd->peers->number;
	else
		r->upstream->peer.tries = MAX_PEER_FAILED;
	
#if (NGX_HTTP_SSL)
    r->upstream->peer.set_session = ngx_http_upstream_set_hash_peer_session;
    r->upstream->peer.save_session = ngx_http_upstream_save_hash_peer_session;
#endif
 
    /* must be big enough for the retry keys */
    if ((uhpd->current_key.data = ngx_pcalloc(r->pool, NGX_ATOMIC_T_LEN + val.len)) == NULL) {
        return NGX_ERROR;
    }

	//request uri key
    ngx_memcpy(uhpd->current_key.data, val.data, val.len);
    uhpd->current_key.len = val.len;
    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "upstream_hash current_key：request: \"%V\" to %ui", &uhpd->current_key,
                   ngx_http_upstream_get_consistent_hash_peer_index(uhpd, r->connection->log));

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_get_consistent_hash_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_consistent_hash_peer_data_t  *uhpd = data;
    ngx_http_upstream_consistent_hash_peer_t       *peer;
    ngx_uint_t                           peer_index;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "upstream_hash: get upstream request hash peer try %ui", pc->tries);

    pc->cached = 0;
    pc->connection = NULL;

    peer_index = ngx_http_upstream_get_consistent_hash_peer_index(uhpd, pc->log);
	if(pc->tries == 0)
	  return NGX_BUSY;
    peer = &uhpd->peers->peer[peer_index];

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "upstream_hash:get: chose peer %ui w/ current_key:%V for tries %ui", peer_index, &uhpd->current_key, pc->tries);

    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    return NGX_OK;
}

/* retry implementation is PECL memcache compatible */
static void
ngx_http_upstream_free_consistent_hash_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_consistent_hash_peer_data_t  *uhpd = data;
    ngx_uint_t                           current;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
            "upstream_hash: free upstream hash peer try %ui", pc->tries);

	if (state & (NGX_PEER_FAILED|NGX_PEER_NEXT)
            && (ngx_int_t) pc->tries > 0) {
    	//请求失败
		if (state & (NGX_PEER_FAILED|NGX_PEER_NEXT)){    
			current = ngx_http_upstream_get_consistent_hash_peer_index(uhpd, pc->log);
	        uhpd->tried[ngx_bitvector_index(current)] |= ngx_bitvector_bit(current);
        	ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                       "upstream_hash:free: Using %ui because %ui failed",
                       ngx_http_upstream_get_consistent_hash_peer_index(uhpd, pc->log), current);
			//current fail, tick it out
			conhash_del_node(conhash, uhpd->peers->peer[current].node);
			uhpd->peers->nreal--;
			ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
			           "upstream_hash:free:delete node i:  %ui, tries:%ui", current, pc->tries);
			uhpd->peers->peer[current].del_flag = !(uhpd->peers->peer[current].del_flag);
			pc->tries--;
   		 }
	}
}

#if (NGX_HTTP_SSL)
static ngx_int_t
ngx_http_upstream_set_consistent_hash_peer_session(ngx_peer_connection_t *pc, void *data) {
    ngx_http_upstream_hash_peer_data_t  *uhpd = data;

    ngx_int_t                       rc;
    ngx_ssl_session_t              *ssl_session;
    ngx_http_upstream_hash_peer_t  *peer;
    ngx_uint_t                           current;

    current = ngx_http_upstream_get_hash_peer_index(uhpd, pc->log);

    peer = &uhpd->peers->peer[current];
    ssl_session = peer->ssl_session;
    rc = ngx_ssl_set_session(pc->connection, ssl_session);
    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "set session: %p:%d",
                   ssl_session, ssl_session ? ssl_session->references : 0);

    return rc;
}

static void
ngx_http_upstream_save_consistent_hash_peer_session(ngx_peer_connection_t *pc, void *data) {
    ngx_http_upstream_hash_peer_data_t *uhpd = data;
    ngx_ssl_session_t            *old_ssl_session, *ssl_session;
    ngx_http_upstream_hash_peer_t  *peer;
    ngx_uint_t                           current;

    ssl_session = ngx_ssl_get_session(pc->connection);

    if (ssl_session == NULL) {
        return;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "save session: %p:%d", ssl_session, ssl_session->references);

    current = ngx_http_upstream_get_hash_peer_index(uhpd, pc->log);

    peer = &uhpd->peers->peer[current];

    old_ssl_session = peer->ssl_session;
    peer->ssl_session = ssl_session;

    if (old_ssl_session) {

        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                       "old session: %p:%d",
                       old_ssl_session, old_ssl_session->references);

        /* TODO: may block */

        ngx_ssl_free_session(old_ssl_session);
    }
}
#endif

static void ngx_http_upstream_consistent_hash_next_peer(ngx_http_upstream_consistent_hash_peer_data_t *uhpd,
        ngx_uint_t *tries, ngx_log_t *log) {

    ngx_uint_t current;
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, log, 0,
	                 "free consistent hash peer, pc->tries: %ui", tries);
	
	current = ngx_http_upstream_get_consistent_hash_peer_index(uhpd, log);
    if ((*tries)-- && (
       (uhpd->tried[ngx_bitvector_index(current)] & ngx_bitvector_bit(current))
        || uhpd->peers->peer[current].down
#if (NGX_HTTP_HEALTHCHECK)
        || ngx_http_healthcheck_is_down(uhpd->peers->peer[current].health_index, log)
#endif
#if (NGX_UPSTREAM_CHECK_MODULE)
		|| ngx_http_check_peer_down(uhpd->peers->peer[current].check_index) 
#endif
        )) {
	   current = ngx_http_upstream_get_consistent_hash_peer_index(uhpd, log);
	   //请求失败，从环中踢掉
	   conhash_del_node(conhash, uhpd->peers->peer[current].node);
	   uhpd->peers->nreal--;
	   ngx_log_debug2(NGX_LOG_DEBUG_HTTP, log, 0,
           "upstream_hash:delete node i:  %ui, tries:%ui", current, tries);
	   uhpd->peers->peer[current].del_flag = !(uhpd->peers->peer[current].del_flag);
   } 
}


static char *
ngx_http_upstream_consistent_hash(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t   *uscf;
    ngx_http_script_compile_t       sc;
    ngx_str_t                      *value;
    ngx_http_upstream_consistent_hash_key_lengths = NULL;
    ngx_http_upstream_consistent_hash_key_values = NULL;

    value = cf->args->elts;

    ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

    sc.cf = cf;
    sc.source = &value[1];
    sc.lengths = &ngx_http_upstream_consistent_hash_key_lengths;
    sc.values = &ngx_http_upstream_consistent_hash_key_values;
    sc.complete_lengths = 1;
    sc.complete_values = 1;

    if (ngx_http_script_compile(&sc) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    uscf->peer.init_upstream = ngx_http_upstream_init_consistent_hash;

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
                  |NGX_HTTP_UPSTREAM_WEIGHT
                  |NGX_HTTP_UPSTREAM_DOWN;
    return NGX_CONF_OK;
}

