CFLAGS = -Wall -Wextra -Werror -pedantic -std=c89 -ggdb3 -O0
LDFLAGS = -lcouchbase

all: proxy

proxy: proxy.o ringbuffer.o
