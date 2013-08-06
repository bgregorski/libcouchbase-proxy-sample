/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#define _XOPEN_SOURCE

#include <unistd.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <libcouchbase/couchbase.h>
#include <netinet/in.h>
#include "ringbuffer.h"

#define BUFFER_SIZE 1024

struct lcb_create_st opts;
int port;

void
info(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
}

void
fail(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
    exit(EXIT_FAILURE);
}

void
fail_e(lcb_error_t err, const char *fmt, ...)
{
    int en = errno;
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    if (en) {
        fprintf(stderr, " (0x%02x: %s)", en, strerror(en));
    }
    if (err != 0) {
        fprintf(stderr, " (0x%02x: %s)", err, lcb_strerror(NULL, err));
    }
    fprintf(stderr, "\n");
    exit(EXIT_FAILURE);
}

void usage(void)
{
    fprintf(stderr,
            "proxy [-?] [-p port] [-h endpoint] [-b bucket] [-u user] [-P password]\n"
            "\t-?\tshow this help\n"
            "\t-p\tport to listen at (default: 1987)\n"
            "\t-h\tcluster endpoint (default: 'example.com:8091')\n"
            "\t-b\tbucket name (default: 'default')\n"
            "\t-u\tuser name (default: none)\n"
            "\t-P\tpassword (default: none)\n");

}

void
scan_options(int argc, char *argv[])
{
    struct lcb_create_io_ops_st io_opts;
    lcb_error_t err;
    int c;

    io_opts.version = 0;
    io_opts.v.v0.type = LCB_IO_OPS_DEFAULT;
    io_opts.v.v0.cookie = NULL;
    port = 1987;
    opts.version = 0;
    opts.v.v0.host = "localhost:8091";
    opts.v.v0.bucket = "default";
    opts.v.v0.user = NULL;
    opts.v.v0.passwd = NULL;

    err = lcb_create_io_ops(&opts.v.v0.io, &io_opts);
    if (err != LCB_SUCCESS) {
        fail_e(err, "lcb_create_io_ops()");
    }
    if (opts.v.v0.io->version != 0) {
        fail_e(err, "this application designed to use v0 IO layout");
    }

    while ((c = getopt(argc, argv, "?p:h:b:u:P:")) != -1) {
        switch (c) {
        case 'p':
            port = atoi(optarg);
            break;
        case 'h':
            opts.v.v0.host = optarg;
            break;
        case 'b':
            opts.v.v0.bucket = optarg;
            break;
        case 'u':
            opts.v.v0.user = optarg;
            break;
        case 'P':
            opts.v.v0.passwd = optarg;
            break;
        default:
            usage();
            if (optopt) {
                exit(EXIT_FAILURE);
            } else {
                exit(EXIT_SUCCESS);
            }
        }
    }
    if (optind < argc) {
        usage();
        fail("unexpected arguments");
    }
}

void run_proxy(lcb_t conn);

int
main(int argc, char *argv[])
{
    lcb_error_t err;
    lcb_t conn = NULL;

    scan_options(argc, argv);
    err = lcb_create(&conn, &opts);
    if (err != LCB_SUCCESS) {
        fail_e(err, "lcb_create()");
    }
    info("connecting to bucket \"%s\" at \"%s\"",
         opts.v.v0.bucket, opts.v.v0.host);
    if (opts.v.v0.user) {
        info("\tas user \"%s\"", opts.v.v0.user);
    }
    if (opts.v.v0.passwd) {
        info("\twith password \"%s\"", opts.v.v0.passwd);
    }
    err = lcb_connect(conn);
    if (err != LCB_SUCCESS) {
        fail_e(err, "lcb_connect()");
    }
    err = lcb_wait(conn);
    if (err != LCB_SUCCESS) {
        fail_e(err, "lcb_connect()");
    }

    run_proxy(conn);

    return EXIT_SUCCESS;
}

void
error_callback(lcb_t conn, lcb_error_t err, const char *info)
{
    fail_e(err, info);
    (void)conn;
}

typedef struct server_st server_t;
struct server_st {
    lcb_t conn;
    lcb_io_opt_t io;
    int sock;
    void *event;
};

typedef struct client_st client_t;
struct client_st {
    server_t *server;
    int sock;
    ringbuffer_t buf;
    void *event;
};

void
proxy_client_callback(lcb_socket_t sock, short which, void *data)
{
    struct lcb_iovec_st iov[2];
    ssize_t nbytes;
    lcb_io_opt_t io;
    client_t *cl = data;

    io = cl->server->io;
    if (which & LCB_READ_EVENT) {
        ringbuffer_ensure_capacity(&cl->buf, BUFFER_SIZE);
        ringbuffer_get_iov(&cl->buf, RINGBUFFER_WRITE, iov);
        nbytes = io->v.v0.recvv(io, cl->sock, iov, 2);
        if (nbytes < 0) {
            fail("read error");
        } else if (nbytes == 0) {
            io->v.v0.destroy_event(io, cl->event);
            ringbuffer_destruct(&cl->buf);
            free(cl);
            info("peer disconnected");
            return;
        } else {
            ringbuffer_produced(&cl->buf, nbytes);
            info("received %zu bytes", nbytes);
            io->v.v0.update_event(io, cl->sock, cl->event,
                                  LCB_WRITE_EVENT, cl,
                                  proxy_client_callback);
        }
    } else if (which & LCB_WRITE_EVENT) {
        ringbuffer_get_iov(&cl->buf, RINGBUFFER_READ, iov);
        nbytes = io->v.v0.sendv(io, cl->sock, iov, 2);
        if (nbytes < 0) {
            fail("write error");
        } else if (nbytes == 0) {
            io->v.v0.destroy_event(io, cl->event);
            ringbuffer_destruct(&cl->buf);
            free(cl);
            info("peer disconnected");
            return;
        } else {
            ringbuffer_consumed(&cl->buf, nbytes);
            info("sent %zu bytes", nbytes);
            io->v.v0.update_event(io, cl->sock, cl->event,
                                  LCB_READ_EVENT, cl,
                                  proxy_client_callback);
        }
    } else  {
        fail("got invalid event");
    }
    (void)sock;
}

void
proxy_accept_callback(lcb_socket_t sock, short which, void *data)
{
    server_t *sv = data;
    client_t *cl;
    lcb_io_opt_t io;
    struct sockaddr_in addr;
    socklen_t naddr;

    if (!(which & LCB_READ_EVENT)) {
        fail("got invalid event");
    }
    io = sv->io;
    cl = malloc(sizeof(client_t));
    if (cl == NULL) {
        fail("failed to allocate memory for client");
    }
    if (ringbuffer_initialize(&cl->buf, BUFFER_SIZE) == 0) {
        fail("failed to allocate buffer for client");
    }
    naddr = sizeof(addr);
    cl->server = sv;
    cl->sock = accept((int)sock, (struct sockaddr *)&addr, &naddr);
    if (cl->sock == -1) {
        fail("accept()");
    }
    cl->event = io->v.v0.create_event(io);
    if (cl->event == NULL) {
        fail("failed to create event for client");
    }
    io->v.v0.update_event(io, cl->sock, cl->event, LCB_READ_EVENT, cl, proxy_client_callback);
}

void
run_proxy(lcb_t conn)
{
    lcb_io_opt_t io;
    struct sockaddr_in addr;
    int sock, rv;
    server_t server;

    io = opts.v.v0.io;
    info("starting proxy on port %d", port);
    sock = socket(PF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        fail("socket()", strerror(errno));
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    rv = bind(sock, (struct sockaddr *)&addr, sizeof(addr));
    if (rv == -1) {
        fail("bind()");
    }
    rv = listen(sock, 10);
    if (rv == -1) {
        fail("listen()");
    }
    server.conn = conn;
    server.io = io;
    server.event = io->v.v0.create_event(io);
    if (server.event == NULL) {
        fail("failed to create event for proxy");
    }
    io->v.v0.update_event(io, sock, server.event, LCB_READ_EVENT,
                          &server, proxy_accept_callback);

    lcb_set_error_callback(conn, error_callback);
    io->v.v0.run_event_loop(io);
}
