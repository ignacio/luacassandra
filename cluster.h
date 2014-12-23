#ifndef _LUACASSANDRA_CLUSTER_H
#define _LUACASSANDRA_CLUSTER_H

#include <cassandra.h>

CassCluster* lua_cluster_get_ptr(lua_State* L, int index);

int lua_cass_cluster_new(lua_State* L);
int lua_cass_cluster_set_contact_points(lua_State* L);
int lua_cass_cluster_set_port(lua_State* L);
int lua_cass_cluster_connect_session(lua_State* L);
int lua_cass_cluster_tostring(lua_State* L);
int lua_cass_cluster_gc(lua_State* L);
int lua_cass_cluster_set_protocol_version(lua_State* L);

#endif
