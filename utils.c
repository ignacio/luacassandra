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


int lua_cass_value_to_lua (lua_State* L, const CassValue* value)
{
	cass_int32_t i;
	cass_bool_t b;
	cass_double_t d;
	CassString s;
	CassUuid u;
	char us[CASS_UUID_STRING_LENGTH];

	CassValueType type = cass_value_type(value);
	switch (type) {
	case CASS_VALUE_TYPE_INT:
		cass_value_get_int32(value, &i);
		lua_pushinteger(L, i);
		//printf("%d", i);
		break;

	case CASS_VALUE_TYPE_BOOLEAN:
		cass_value_get_bool(value, &b);
		lua_pushboolean(L, b);
		//printf("%s", b ? "true" : "false");
		break;

	case CASS_VALUE_TYPE_DOUBLE:
		cass_value_get_double(value, &d);
		lua_pushnumber(L, d);
		//printf("%f", d);
		break;

	case CASS_VALUE_TYPE_TEXT:
	case CASS_VALUE_TYPE_ASCII:
	case CASS_VALUE_TYPE_VARCHAR:
		cass_value_get_string(value, &s);
		lua_pushlstring(L, s.data, s.length);
		//printf("\"%.*s\"", (int)s.length, s.data);
		break;

	case CASS_VALUE_TYPE_UUID:
		cass_value_get_uuid(value, &u);
		cass_uuid_string(u, us);
		lua_pushstring(L, us);
		//printf("%s", us);
		break;

	case CASS_VALUE_TYPE_LIST:
		//print_schema_list(value);
		luaL_error(L, "Handled CASS_VALUE_TYPE_LIST");
		break;

	case CASS_VALUE_TYPE_MAP:
		//print_schema_map(value);
		luaL_error(L, "Handled CASS_VALUE_TYPE_MAP");
		break;

	default:
		if (cass_value_is_null(value)) {
			lua_pushnil(L);
			//printf("null");
		}
		else {
			//printf("<unhandled type>");
			luaL_error(L, "unhandled type: %d", type);
		}
		break;
	}
	return 1;
}