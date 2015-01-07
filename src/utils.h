#ifndef _LUACASSANDRA_UTILS_H
#define _LUACASSANDRA_UTILS_H

#include <lua.h>
#include <cassandra.h>

int lua_cass_push_error(lua_State* L, CassError e);
int lua_cass_push_future_error(lua_State* L, CassFuture* future);
int lua_cass_value_to_lua(lua_State* L, const CassValue* value);


#endif
