lc = require "luacassandra"

local repl = require "luanode.repl"

cluster = lc.cass_cluster_new()

cluster:set_contact_points("192.168.4.11")
--cluster:set_port(9042) --the native transport, not thrift

session = lc.cass_session_new()

assert(cluster:connect_session(session))


repl.start("> ")

process:loop()
