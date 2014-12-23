#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "utils.h"



int lua_cass_push_error(lua_State* L, CassError e)
{
	const char* err_str = cass_error_desc(e);
	lua_pushnil(L);
	lua_pushstring(L, err_str);
	return 2;
}

int lua_cass_push_future_error (lua_State* L, CassFuture* future)
{
	CassString message = cass_future_error_message(future);
	lua_pushnil(L);
	lua_pushlstring(L, message.data, message.length);
	return 2;
}
