lc = require "luacassandra"

local repl = require "luanode.repl"

cluster = lc.cass_cluster_new()

cluster:set_protocol_version(1)	-- needed for Cassandra 1.2.19
cluster:set_contact_points("192.168.4.11")
--cluster:set_port(9042) --the native transport, not thrift

session = lc.cass_session_new()

--assert(cluster:connect_session(session))
assert(cluster:connect_session_keyspace(session, "Agent"))

schema = session:get_schema()
meta = schema:get_keyspace("Agent")
print("Agent", meta:type())
print("Agent:Campaigns", meta:get_entry("Campaigns"):type())

meta_camp = meta:get_entry("Campaigns")
print("keyspace_name ->", meta_camp:get_field_value("keyspace_name"))
print("columnfamily_name ->", meta_camp:get_field_value("columnfamily_name"))
--print("columnfamily_name ->", meta_camp:get_field_value("max_compaction_threshold"))

--meta:test()

--meta_camp:test()

statement = lc.cass_statement_new([[SELECT * FROM "Users"]])
res = assert(session:execute(statement))

--session:close()


repl.start("> ")

process:loop()
