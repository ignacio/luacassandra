#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "statement.h"



/*
Userdata on given position of the stack.
*/
CassStatement* lua_statement_get_ptr(lua_State* L, int index)
{
	CassStatement* statement = *(CassStatement**)luaL_checkudata(L, index, "CassStatement");
	return statement;
}

int lua_cass_statement_new(lua_State* L)
{
	fprintf(stderr, "lua_cass_statement_new\n");
	CassString query = cass_string_init(luaL_checkstring(L, 1));
	cass_size_t param_count = luaL_optint(L, 2, 0);
	
	CassStatement* statement = cass_statement_new(query, param_count);
	if (!statement) {
		return luaL_error(L, "cass_statement_new call failed");
	}
	void* ptr = lua_newuserdata(L, sizeof(CassStatement*));
	luaL_getmetatable(L, "CassStatement");
	lua_setmetatable(L, -2);
	memcpy(ptr, &((void*)statement), sizeof(CassStatement*));

	return 1;
}

int lua_cass_statement_tostring(lua_State* L)
{
	lua_pushstring(L, "a statement!");
	return 1;
}

int lua_cass_statement_gc(lua_State* L)
{
	fprintf(stderr, "lua_cass_statement_gc\n");
	CassStatement* statement = lua_statement_get_ptr(L, 1);
	cass_statement_free(statement);
	return 0;
}






struct luaL_reg* get_statement_exported_methods ()
{
	static struct luaL_reg methods[] = {
		{ "__tostring", lua_cass_statement_tostring },
		{ "__gc", lua_cass_statement_gc },
		//{ "get_keyspace", lua_cass_schema_get_keyspace },



		{ NULL, NULL }
	};

	return methods;
}
