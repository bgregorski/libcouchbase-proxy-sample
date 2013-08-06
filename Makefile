CFLAGS = -Wall -Wextra -Werror -pedantic -std=c89
LDFLAGS = -lcouchbase

all: proxy

proxy: proxy.o ringbuffer.o
