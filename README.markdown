libcouchbase-proxy-sample
=========================

This application is built to demostrate how to build asynchronous
application and integrate it with libcouchbase. It is simple proxy for
legacy memcached clients, i.e. you can use your favorite memcached
client to play with it. The proxy implements only two commands: GET
and SET, for demontration purposes it is more than enough.

The most interesting part located in `proxy.c`. To build just run
`make` in the project directory:

    $ make
    cc -Wall -Wextra -Werror -pedantic -std=c89   -c -o proxy.o proxy.c
    cc -lcouchbase  proxy.o ringbuffer.o   -o proxy

Obviously you need libcouchbase installed, if you don't have it, read
here how to get it: http://www.couchbase.com/communities/c

To get help about how to run the proxy, run:

    $ ./proxy -?
    proxy [-?] [-p port] [-h endpoint] [-b bucket] [-u user] [-P password]
            -?      show this help
            -p      port to listen at (default: 1987)
            -h      cluster endpoint (default: 'example.com:8091')
            -b      bucket name (default: 'default')
            -u      user name (default: none)
            -P      password (default: none)

To run it on port `1908` bind to bucket named `example`, use:

    $ ./proxy -p 1908 -b example
    connecting to bucket "example" at "localhost:8091"
    starting proxy on port 1908
    use ctrl-c to stop

Now you can run your favorite memcached client and see that you've got
the working mecached proxy on this port. Make sure that your client
can speak binary protocol.

    $ gem install dalli
    Successfully installed dalli-2.6.4
    1 gem installed
    $ irb -rrubygems -rdalli
    2.0.0p247 (main):001:0> x = Dalli::Client.new  "localhost:1908"
    #<Dalli::Client:0x007f3db72ac870 @servers=["localhost:1908"], @options={}, @ring=nil>
    2.0.0p247 (main):002:0> x.set("foo", "bar")
    true
    2.0.0p247 (main):003:0> x.get("foo")
    "bar"

Meanwhile server output be the following:

    $ ./proxy -p 1908 -b example
    connecting to bucket "example" at "localhost:8091"
    starting proxy on port 1908
    use ctrl-c to stop
    [1] connected
    [1] version
    [1] set "foo"
    [1] get "foo"
    [1] disconnected
