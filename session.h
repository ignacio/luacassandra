#ifndef _LUACASSANDRA_SESSION_H
#define _LUACASSANDRA_SESSION_H

#include <lua.h>
#include <cassandra.h>

CassSession* lua_session_get_ptr (lua_State* L, int index);

int lua_cass_session_new(lua_State* L);
int lua_cass_session_tostring(lua_State* L);
int lua_cass_session_gc(lua_State* L);
//int lua_cass_session_execute_query(lua_State* L);


#endif
