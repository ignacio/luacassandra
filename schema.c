#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "schema.h"
#include "schema_meta.h"



/*
Userdata on given position of the stack.
*/
CassSchema* lua_schema_get_ptr(lua_State* L, int index)
{
	CassSchema* schema = *(CassSchema**)luaL_checkudata(L, index, "CassSchema");
	return schema;
}

// TODO: Beware pushing the same Schema instance more than once
int lua_cass_push_schema(lua_State* L, const CassSchema* schema)
{
	fprintf(stderr, "lua_cass_push_schema\n");
	//CassSchema* schema = cass_schema_new();
	//if (!schema) {
//		return luaL_error(L, "lua_cass_schema_new call failed");
//	}
	void* ptr = lua_newuserdata(L, sizeof(CassSchema*));
	luaL_getmetatable(L, "CassSchema");
	lua_setmetatable(L, -2);
	memcpy(ptr, &((void*)schema), sizeof(CassSchema*));

	return 1;
}

int lua_cass_schema_tostring(lua_State* L)
{
	lua_pushstring(L, "a schema!");
	return 1;
}

int lua_cass_schema_gc(lua_State* L)
{
	fprintf(stderr, "lua_cass_schema_gc\n");
	CassSchema* schema = lua_schema_get_ptr(L, 1);
	cass_schema_free(schema);
	return 0;
}

int lua_cass_schema_get_keyspace(lua_State* L)
{
	CassSchema* schema = lua_schema_get_ptr(L, 1);
	const char* keyspace_name = luaL_checkstring(L, 2);
	const CassSchemaMeta* meta = cass_schema_get_keyspace(schema, keyspace_name);
	if (meta == NULL) {
		lua_pushnil(L);
		return 1;
	}

	return lua_cass_push_schema_meta(L, meta);
}




struct luaL_reg* get_schema_exported_methods ()
{
	static struct luaL_reg methods[] = {
		{ "__tostring", lua_cass_schema_tostring },
		{ "__gc", lua_cass_schema_gc },
		{ "get_keyspace", lua_cass_schema_get_keyspace },



		{ NULL, NULL }
	};

	return methods;
}
