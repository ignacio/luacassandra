#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "session.h"



/*
Userdata on given position of the stack.
*/
CassSession* lua_session_get_ptr(lua_State* L, int index)
{
	CassSession* session = *(CassSession**)luaL_checkudata(L, index, "CassSession");
	return session;
}

int lua_cass_session_new(lua_State* L)
{
	fprintf(stderr, "lua_cass_session_new\n");
	CassSession* session = cass_session_new();
	if (!session) {
		return luaL_error(L, "cass_session_new call failed");
	}
	void* ptr = lua_newuserdata(L, sizeof(CassSession*));
	luaL_getmetatable(L, "CassSession");
	lua_setmetatable(L, -2);
	memcpy(ptr, &((void*)session), sizeof(CassSession*));

	return 1;
}

int lua_cass_session_tostring(lua_State* L)
{
	lua_pushstring(L, "a session!");
	return 1;
}

int lua_cass_session_gc(lua_State* L)
{
	fprintf(stderr, "lua_cass_session_gc\n");
	CassSession* session = lua_session_get_ptr(L, 1);
	cass_session_free(session);
	return 0;
}

/**
* Closes the session instance and waits for in-flight requests to finish.
*/
int lua_cass_session_close (lua_State* L)
{
	fprintf(stderr, "lua_cass_session_close\n");
	CassSession* session = lua_session_get_ptr(L, 1);
	CassFuture* future = cass_session_close(session);
	cass_future_wait(future);
	cass_future_free(future);
	return 0;
}

/*int lua_cass_session_execute_query(lua_State* L)
{
	CassSession* session = lua_session_get_ptr(L, 1);
	const char* query = luaL_checkstring(L, 2);
	CassStatement* statement = cass_statement_new(cass_string_init(query), 0);

	CassFuture* future = cass_session_execute(session, statement);
	cass_future_wait(future);

	CassError rc = cass_future_error_code(future);
	if (rc == CASS_OK) {

	}
}*/

