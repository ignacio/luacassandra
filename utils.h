#ifndef _LUACASSANDRA_UTILS_H
#define _LUACASSANDRA_UTILS_H

#include <lua.h>
#include <cassandra.h>

int lua_handle_error(lua_State* L, CassError e);
int lua_push_future_error(lua_State* L, CassFuture* future);


#endif
