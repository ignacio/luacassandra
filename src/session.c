#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "session.h"
#include "schema.h"
#include "statement.h"
#include "utils.h"
#include <malloc.h>



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

int lua_cass_session_get_schema(lua_State* L)
{
	CassSession* session = lua_session_get_ptr(L, 1);
	const CassSchema* schema = cass_session_get_schema(session);

	return lua_cass_push_schema(L, schema);
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

/**
* Executes a statement on a given session. Currently builds an array of rows.
* That will hopefully change.
* Also, I may expose the iterator instead, so you can go crazy in Lua-land.
*/
int lua_cass_session_execute(lua_State* L)
{
	CassSession* session = lua_session_get_ptr(L, 1);
	CassStatement* statement = lua_statement_get_ptr(L, 2);

	CassFuture* future = cass_session_execute(session, statement);
	cass_future_wait(future);

	CassError rc = cass_future_error_code(future);
	if (rc != CASS_OK) {
		int num = lua_cass_push_future_error(L, future);
		cass_future_free(future);
		return num;
	}

	const CassResult* result = cass_future_get_result(future);
	cass_future_free(future);
	CassIterator* iterator = cass_iterator_from_result(result);

	// TODO: devolver un iterador ?
	int row_count = cass_result_row_count(result);
	int column_count = cass_result_column_count(result);

	int i = 0;
	lua_newtable(L);
	int rows_table = lua_gettop(L);

	/* iterate over rows */
	while (cass_iterator_next(iterator)) {
		const CassRow* row = cass_iterator_get_row(iterator);
		
		lua_newtable(L);
		int columns_table = lua_gettop(L);
		/* loop over columns */
		for (int j = 0; j < column_count; ++j) {
			CassString name = cass_result_column_name(result, j);
			lua_pushlstring(L, name.data, name.length);
			lua_cass_value_to_lua(L, cass_row_get_column(row, j));
			
			lua_settable(L, columns_table);
			//lua_rawseti(L, columns_table, j + 1);
		}
		
		/*int j = 0;
		CassIterator* it_columns = cass_iterator_from_row(row);
		while (cass_iterator_next(it_columns)) {
			const CassValue* column = cass_iterator_get_column(it_columns);
			
			CassString name = cass_result_column_name(result, j);
			lua_pushlstring(L, name.data, name.length);
			lua_cass_value_to_lua(L, column);
			lua_settable(L, columns_table);
			j++;
		}*/
		lua_rawseti(L, rows_table, ++i);
	}
	
	cass_result_free(result);
	cass_iterator_free(iterator);

	return 1;
}




struct luaL_reg* get_session_exported_methods()
{
	static struct luaL_reg methods[] = {
		{ "__tostring", lua_cass_session_tostring },
		{ "__gc", lua_cass_session_gc },
		{ "get_schema", lua_cass_session_get_schema },
		{ "close", lua_cass_session_close },
		{ "execute", lua_cass_session_execute },
		{ NULL, NULL }
	};

	return methods;
}
