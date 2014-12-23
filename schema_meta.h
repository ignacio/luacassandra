#ifndef _LUACASSANDRA_SCHEMA_META_H_
#define _LUACASSANDRA_SCHEMA_META_H_

#include <lua.h>
#include <cassandra.h>

CassSchemaMeta* lua_schema_meta_get_ptr (lua_State* L, int index);

int lua_cass_push_schema_meta(lua_State* L, const CassSchemaMeta* schema_meta);

int lua_cass_schema_meta_tostring(lua_State* L);
int lua_cass_schema_meta_gc(lua_State* L);



struct luaL_reg* get_schema_meta_exported_methods();

#endif
