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
schema:get_keyspace("examples"):get_entry("songs"):test()

--meta_camp:test()

--statement = lc.cass_statement_new([[SELECT * FROM "Campaigns"]])
--res = assert(session:execute(statement))

--statement = lc.cass_statement_new([[CREATE KEYSPACE examples WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };]])
--res = assert(session:execute(statement))

--statement = lc.cass_statement_new([[SELECT * FROM "Campaigns"]])
--res = assert(session:execute(statement))
--session:close()

--[=[
statement = lc.cass_statement_new([[CREATE TABLE examples.songs (key text PRIMARY KEY, title text, album text, artist text );]])-- WITH COMPACT STORAGE
res = assert(session:execute(statement))
--]=]

--[=[
statement = lc.cass_statement_new([[INSERT INTO examples.songs (key, title, album, artist) 
VALUES ('a3e64f8f', 'La Grange', 'ZZ Top', 'Tres Hombres');]], 4)
res = assert(session:execute(statement))

statement = lc.cass_statement_new([[INSERT INTO examples.songs (key, title, album, artist) 
VALUES ('8a172618', 'We Must Obey', 'Fu Manchu', 'Moving In Stereo');]], 4)
res = assert(session:execute(statement))

statement = lc.cass_statement_new([[INSERT INTO examples.songs (key, title, album, artist) 
VALUES ('2b09185b', 'Outside Woman Blues', 'Back Door Slam', 'Roll Away');]], 4)
res = assert(session:execute(statement))
--]=]

statement = lc.cass_statement_new([[SELECT * FROM examples.songs]])
res = assert(session:execute(statement))

repl.start("> ")

process:loop()
