#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include <cassandra.h>
#include "cluster.h"
#include "session.h"
#include "schema.h"
#include "schema_meta.h"



/*
** ===============================================================
**   Declarations
** ===============================================================
*/

// TODO: add declarations

/*
** ===============================================================
**   Generic internal code
** ===============================================================
*/

// TODO: add generic code

#ifndef NDEBUG
//can be found here  http://www.lua.org/pil/24.2.3.html
static void stackDump (lua_State *L, const char *text) {
	  int i;
	  int top = lua_gettop(L);
	  if (text == NULL)
		printf("--------Start Dump------------\n");
	  else
		printf("--------Start %s------------\n", text);
	  for (i = 1; i <= top; i++) {  /* repeat for each level */
		int t = lua_type(L, i);
		switch (t) {
	
		  case LUA_TSTRING:  /* strings */
			printf("`%s'", lua_tostring(L, i));
			break;
	
		  case LUA_TBOOLEAN:  /* booleans */
			printf(lua_toboolean(L, i) ? "true" : "false");
			break;
	
		  case LUA_TNUMBER:  /* numbers */
			printf("%g", lua_tonumber(L, i));
			break;
	
		  default:  /* other values */
			printf("%s", lua_typename(L, t));
			break;
	
		}
		printf("  ");  /* put a separator */
	  }
	  printf("\n");  /* end the listing */
	  printf("--------End Dump------------\n");
	}

static void tableDump(lua_State *L, int idx, const char* text)
{
	lua_pushvalue(L, idx);		// copy target table
	lua_pushnil(L);
	  if (text == NULL)
		printf("--------Table Dump------------\n");
	  else
		printf("--------Table dump: %s------------\n", text);
	while (lua_next(L,-2) != 0) {
		printf("%s - %s\n",
			lua_typename(L, lua_type(L, -2)),
			lua_typename(L, lua_type(L, -1)));
		lua_pop(L, 1);
	}
	lua_pop(L, 1);	// remove table copy
	printf("--------End Table dump------------\n");
}
#endif


static int create_metatable (lua_State* L, const char* name, const luaL_reg* methods)
{
	if (!luaL_newmetatable(L, name)) {
		return 0;
	}

	/* define methods */
	luaL_openlib(L, NULL, methods, 0);

	/* define metamethods */
	lua_pushliteral(L, "__index");
	lua_pushvalue(L, -2);
	lua_settable(L, -3);

	return 1;
}




/*
** ===============================================================
** Library initialization and shutdown
** ===============================================================
*/

// Structure with all functions made available to Lua
static const struct luaL_Reg LuaExportFunctions[] = {

	{ "cass_cluster_new", lua_cass_cluster_new },
	{ "cass_session_new", lua_cass_session_new },

	{NULL, NULL}  // last entry; list terminator
};

// Open method called when Lua opens the library
// On success; return 1
// On error; push errorstring on stack and return 0
static int L_openLib(lua_State *L) {



	// TODO: add startup/initialization code
	//lua_getglobal(L, "print");
	//lua_pushstring(L, "Now initializing module 'required' as:");
	//lua_pushvalue(L, 1); // pos 1 on the stack contains the module name
	//lua_call(L, 2, 0);





	return 1;	// report success
}


// Close method called when Lua shutsdown the library
// Note: check Lua os.exit() function for exceptions,
// it will not always be called! Changed from Lua 5.1 to 5.2.
static int L_closeLib(lua_State *L) {


	// TODO: add shutdown/cleanup code
	//lua_getglobal(L, "print");
	//lua_pushstring(L, "Now closing the Lua template library");
	//lua_call(L, 1, 0);




	return 0;
}


/*
** ===============================================================
** Core startup functionality, no need to change anything below
** ===============================================================
*/


// Setup a userdata to provide a close method
static void L_setupClose(lua_State *L) {
	// create library tracking userdata
	if (lua_newuserdata(L, sizeof(void*)) == NULL) {
		luaL_error(L, "Cannot allocate userdata, out of memory?");
	};
	// Create a new metatable for the tracking userdata
	luaL_newmetatable(L, LTLIB_UDATAMT);
	// Add GC metamethod
	lua_pushstring(L, "__gc");
	lua_pushcfunction(L, L_closeLib);
	lua_settable(L, -3);
	// Attach metatable to userdata
	lua_setmetatable(L, -2);
	// store userdata in registry
	lua_setfield(L, LUA_REGISTRYINDEX, LTLIB_UDATANAME);
}

// When initialization fails, removes the userdata and metatable
// again, throws the error stored by L_openLib(), will not return
// Top of stack must hold error string
static void L_openFailed(lua_State *L) {
	// get userdata and remove its metatable
	lua_getfield(L, LUA_REGISTRYINDEX, LTLIB_UDATANAME);
	lua_pushnil(L);
	lua_setmetatable(L, -2);
	// remove userdata
	lua_pushnil(L);
	lua_setfield(L, LUA_REGISTRYINDEX, LTLIB_UDATANAME);
	// throw the error (won't return)
	luaL_error(L, lua_tostring(L, -1));
}


LTLIB_EXPORTAPI	int LTLIB_OPENFUNC (lua_State *L){

	// Setup a userdata with metatable to create a close method
	L_setupClose(L);

	if (L_openLib(L) == 0) {  // call initialization code
		L_openFailed(L);    // Init failed, so cleanup, will not return
	}


	create_metatable(L, "CassCluster", get_cluster_exported_methods());
	lua_pop(L, 1);

	create_metatable(L, "CassSession", get_session_exported_methods());
	lua_pop(L, 1);

	create_metatable(L, "CassSchema", get_schema_exported_methods());
	lua_pop(L, 1);

	create_metatable(L, "CassSchemaMeta", get_schema_meta_exported_methods());
	lua_pop(L, 1);

	// Export Lua API
	lua_newtable(L);
#if LUA_VERSION_NUM < 502
	luaL_register(L, LTLIB_GLOBALNAME, LuaExportFunctions);
#else
	luaL_setfuncs (L, LuaExportFunctions, 0);
#endif

	//cass_log_set_level(CASS_LOG_DEBUG);
	
	return 1;
};


