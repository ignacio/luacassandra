#ifndef _LUACASSANDRA_STATEMENT_H_
#define _LUACASSANDRA_STATEMENT_H_

#include <lua.h>
#include <cassandra.h>

CassStatement* lua_statement_get_ptr (lua_State* L, int index);

int lua_cass_statement_new(lua_State* L);
int lua_cass_statement_tostring(lua_State* L);
int lua_cass_statement_gc(lua_State* L);



struct luaL_reg* get_statement_exported_methods();

#endif
