#ifndef _LUACASSANDRA_SCHEMA_H
#define _LUACASSANDRA_SCHEMA_H

#include <lua.h>
#include <cassandra.h>

CassSchema* lua_schema_get_ptr (lua_State* L, int index);

int lua_cass_push_schema(lua_State* L, const CassSchema* schema);

int lua_cass_schema_new(lua_State* L);
int lua_cass_schema_tostring(lua_State* L);
int lua_cass_schema_gc(lua_State* L);



struct luaL_reg* get_schema_exported_methods();

#endif
