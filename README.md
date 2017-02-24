# nginx_upstream_consistent_hash
nginx 一致性哈希模块，支持虚节点，可动态剔除不健康节点
依赖libconhash
依赖ngx_http_healthcheck_module做健康检查

Installation:

    cd nginx # or whatever
    ./configure --add-module=/path/to/this/directory
    make
    make install

Usage:

    upstream backend {
        ... 
        hash        $request_uri;
        hash_again  10;          # default 0
    }   

Works the same on 32-bit and 64-bit systems.
