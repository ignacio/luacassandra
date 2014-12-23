#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "cluster.h"
#include "session.h"
#include "utils.h"



/*
Userdata on given position of the stack.
*/
CassCluster* lua_cluster_get_ptr(lua_State* L, int index)
{
	CassCluster* cluster = *(CassCluster**)luaL_checkudata(L, index, "CassCluster");
	return cluster;
}

int lua_cass_cluster_new (lua_State* L)
{
	fprintf(stderr, "lua_cass_cluster_new\n");
	CassCluster* cluster = cass_cluster_new();
	if (!cluster) {
		return luaL_error(L, "cass_cluster_new call failed");
	}
	void* ptr = lua_newuserdata(L, sizeof(CassCluster*));
	luaL_getmetatable(L, "CassCluster");
	lua_setmetatable(L, -2);
	memcpy(ptr, &((void*)cluster), sizeof(CassCluster*));

	return 1;
}

int lua_cass_cluster_set_contact_points(lua_State* L)
{
	fprintf(stderr, "lua_cass_cluster_set_contact_points\n");
	CassCluster* cluster = lua_cluster_get_ptr(L, 1);
	const char* contact_points = luaL_checkstring(L, 2);
	CassError err = cass_cluster_set_contact_points(cluster, contact_points);
	if (err == CASS_OK) {
		lua_pushboolean(L, 1);
	}
	return lua_cass_push_error(L, err);
}

int lua_cass_cluster_set_port(lua_State* L)
{
	fprintf(stderr, "lua_cass_cluster_set_port\n");
	CassCluster* cluster = lua_cluster_get_ptr(L, 1);
	lua_Integer port = luaL_checkinteger(L, 2);
	CassError err = cass_cluster_set_port(cluster, port);
	if (err == CASS_OK) {
		lua_pushboolean(L, 1);
	}
	return lua_cass_push_error(L, err);
}

int lua_cass_cluster_connect_session(lua_State* L)
{
	fprintf(stderr, "lua_cass_cluster_connect_session\n");
	CassCluster* cluster = lua_cluster_get_ptr(L, 1);
	CassSession* session = lua_session_get_ptr(L, 2);

	CassFuture* future = cass_session_connect(session, cluster);

	cass_future_wait(future);
	CassError rc = cass_future_error_code(future);
	if (rc == CASS_OK) {
		lua_pushboolean(L, 1);
		return 1;
	}
	int res = lua_cass_push_future_error(L, future);
	cass_future_free(future);
	return res;
}

int lua_cass_cluster_connect_session_keyspace(lua_State* L)
{
	fprintf(stderr, "lua_cass_cluster_connect_session_keyspace\n");
	CassCluster* cluster = lua_cluster_get_ptr(L, 1);
	CassSession* session = lua_session_get_ptr(L, 2);
	const char* keyspace = luaL_checkstring(L, 3);

	CassFuture* future = cass_session_connect_keyspace(session, cluster, keyspace);

	cass_future_wait(future);
	CassError rc = cass_future_error_code(future);
	if (rc == CASS_OK) {
		lua_pushboolean(L, 1);
		return 1;
	}
	int res = lua_cass_push_future_error(L, future);
	cass_future_free(future);
	return res;
}

int lua_cass_cluster_tostring(lua_State* L)
{
	lua_pushstring(L, "a cluster!");
	return 1;
}

int lua_cass_cluster_gc (lua_State* L)
{
	fprintf(stderr, "lua_cass_cluster_gc\n");
	CassCluster* cluster = lua_cluster_get_ptr(L, 1);
	cass_cluster_free(cluster);
	return 0;
}

int lua_cass_cluster_set_protocol_version(lua_State* L)
{
	CassCluster* cluster = lua_cluster_get_ptr(L, 1);
	lua_Integer protocol_ver = luaL_checkinteger(L, 2);
	CassError err = cass_cluster_set_protocol_version(cluster, protocol_ver);
	if (err == CASS_OK) {
		lua_pushboolean(L, 1);
		return 1;
	}
	return lua_cass_push_error(L, err);
}




struct luaL_reg* get_cluster_exported_methods()
{
	static struct luaL_reg methods[] = {
		{ "__tostring", lua_cass_cluster_tostring },
		{ "__gc", lua_cass_cluster_gc },
		{ "set_contact_points", lua_cass_cluster_set_contact_points },
		{ "set_port", lua_cass_cluster_set_port },
		{ "connect_session", lua_cass_cluster_connect_session },
		{ "connect_session_keyspace", lua_cass_cluster_connect_session_keyspace },
		{ "set_protocol_version", lua_cass_cluster_set_protocol_version },
		{ NULL, NULL }
	};

	return methods;
}
