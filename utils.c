#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "utils.h"



int lua_handle_error(lua_State* L, CassError e)
{
	/*CASS_ERROR_MAP()
	switch (e) {
	case
	}*/
	return 0;
}

int lua_push_future_error(lua_State* L, CassFuture* future)
{
	CassString message = cass_future_error_message(future);
	lua_pushnil(L);
	lua_pushlstring(L, message.data, message.length);
	return 2;
}
