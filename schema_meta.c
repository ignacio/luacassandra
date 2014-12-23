#include <lua.h>
#include <lauxlib.h>
#include "luacassandra.h"	// TODO: rename

#include "schema_meta.h"
#include "utils.h"



/*
Userdata on given position of the stack.
*/
CassSchemaMeta* lua_schema_meta_get_ptr (lua_State* L, int index)
{
	CassSchemaMeta* meta = *(CassSchemaMeta**)luaL_checkudata(L, index, "CassSchemaMeta");
	return meta;
}

// TODO: Beware pushing the same Schema instance more than once
int lua_cass_push_schema_meta(lua_State* L, const CassSchemaMeta* meta)
{
	fprintf(stderr, "lua_cass_push_schema_meta\n");

	void* ptr = lua_newuserdata(L, sizeof(CassSchemaMeta*));
	luaL_getmetatable(L, "CassSchemaMeta");
	lua_setmetatable(L, -2);
	memcpy(ptr, &((void*)meta), sizeof(CassSchemaMeta*));

	return 1;
}

int lua_cass_schema_meta_tostring(lua_State* L)
{
	lua_pushstring(L, "a schema meta!");
	return 1;
}

/*int lua_cass_schema_meta_gc(lua_State* L)
{
	fprintf(stderr, "lua_cass_schema_meta_gc\n");
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	cass_schema_meta_free(meta);
	return 0;
}*/

int lua_cass_schema_meta_get_type(lua_State* L)
{
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	CassSchemaMetaType type = cass_schema_meta_type(meta);
	switch (type) {
		case CASS_SCHEMA_META_TYPE_KEYSPACE:
			lua_pushliteral(L, "keyspace");
		break;

	case CASS_SCHEMA_META_TYPE_TABLE:
		lua_pushliteral(L, "table");
		break;

	case CASS_SCHEMA_META_TYPE_COLUMN:
		lua_pushliteral(L, "column");
		break;
	}
	return 1;
}

int lua_cass_schema_meta_get_entry(lua_State* L)
{
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	const char* name = luaL_checkstring(L, 2);
	const CassSchemaMeta* entry_meta = cass_schema_meta_get_entry(meta, name);
	if (!entry_meta) {
		lua_pushnil(L);
		return 1;
	}
	return lua_cass_push_schema_meta(L, entry_meta);
}

/*int lua_cass_schema_meta_get_field(lua_State* L)
{
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	const char* name = luaL_checkstring(L, 2);
	const CassSchemaMetaField* metafield = cass_schema_meta_get_field(meta, name);
	
}*/

int lua_cass_schema_meta_get_field_name (lua_State* L)
{
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	const char* name = luaL_checkstring(L, 2);
	const CassSchemaMetaField* metafield = cass_schema_meta_get_field(meta, name);
	if (!metafield) {
		lua_pushnil(L);
		return 1;
	}
	CassString cstring = cass_schema_meta_field_name(metafield);
	lua_pushlstring(L, cstring.data, cstring.length);
	return 1;
}

int lua_cass_schema_meta_get_field_value (lua_State* L)
{
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	const char* name = luaL_checkstring(L, 2);
	const CassSchemaMetaField* metafield = cass_schema_meta_get_field(meta, name);
	if (!metafield) {
		lua_pushnil(L);
		return 1;
	}
	const CassValue* value = cass_schema_meta_field_value(metafield);
	return lua_cass_value_to_lua(L, value);
}





void print_schema_meta(const CassSchemaMeta* meta, int indent);

void print_keyspace(CassSession* session, const char* keyspace) {
	const CassSchema* schema = cass_session_get_schema(session);
	const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(schema, keyspace);

	if (keyspace_meta != NULL) {
		print_schema_meta(keyspace_meta, 0);
	}
	else {
		fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
	}

	cass_schema_free(schema);
}

void print_table(CassSession* session, const char* keyspace, const char* table) {
	const CassSchema* schema = cass_session_get_schema(session);
	const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(schema, keyspace);

	if (keyspace_meta != NULL) {
		const CassSchemaMeta* table_meta = cass_schema_meta_get_entry(keyspace_meta, table);
		if (table_meta != NULL) {
			print_schema_meta(table_meta, 0);
		}
		else {
			fprintf(stderr, "Unable to find \"%s\" table in the schema metadata\n", table);
		}
	}
	else {
		fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
	}

	cass_schema_free(schema);
}

void print_error(CassFuture* future) {
	CassString message = cass_future_error_message(future);
	fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
}

void print_schema_value(const CassValue* value);
void print_schema_list(const CassValue* value);
void print_schema_map(const CassValue* value);
void print_schema_meta_field(const CassSchemaMetaField* field, int indent);
void print_schema_meta_fields(const CassSchemaMeta* meta, int indent);
void print_schema_meta_entries(const CassSchemaMeta* meta, int indent);

void print_indent(int indent) {
	int i;
	for (i = 0; i < indent; ++i) {
		printf("\t");
	}
}

void print_schema_value(const CassValue* value) {
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
		printf("%d", i);
		break;

	case CASS_VALUE_TYPE_BOOLEAN:
		cass_value_get_bool(value, &b);
		printf("%s", b ? "true" : "false");
		break;

	case CASS_VALUE_TYPE_DOUBLE:
		cass_value_get_double(value, &d);
		printf("%f", d);
		break;

	case CASS_VALUE_TYPE_TEXT:
	case CASS_VALUE_TYPE_ASCII:
	case CASS_VALUE_TYPE_VARCHAR:
		cass_value_get_string(value, &s);
		printf("\"%.*s\"", (int)s.length, s.data);
		break;

	case CASS_VALUE_TYPE_UUID:
		cass_value_get_uuid(value, &u);
		cass_uuid_string(u, us);
		printf("%s", us);
		break;

	case CASS_VALUE_TYPE_LIST:
		print_schema_list(value);
		break;

	case CASS_VALUE_TYPE_MAP:
		print_schema_map(value);
		break;

	default:
		if (cass_value_is_null(value)) {
			printf("null");
		}
		else {
			printf("<unhandled type>");
		}
		break;
	}
}

void print_schema_list(const CassValue* value) {
	CassIterator* iterator = cass_iterator_from_collection(value);
	cass_bool_t is_first = cass_true;

	printf("[ ");
	while (cass_iterator_next(iterator)) {
		if (!is_first) printf(", ");
		print_schema_value(cass_iterator_get_value(iterator));
		is_first = cass_false;
	}
	printf(" ]");
	cass_iterator_free(iterator);
}

void print_schema_map(const CassValue* value) {
	CassIterator* iterator = cass_iterator_from_map(value);
	cass_bool_t is_first = cass_true;

	printf("{ ");
	while (cass_iterator_next(iterator)) {
		if (!is_first) printf(", ");
		print_schema_value(cass_iterator_get_map_key(iterator));
		printf(" : ");
		print_schema_value(cass_iterator_get_map_value(iterator));
		is_first = cass_false;
	}
	printf(" }");
	cass_iterator_free(iterator);
}

void print_schema_meta_field(const CassSchemaMetaField* field, int indent) {
	CassString name = cass_schema_meta_field_name(field);
	const CassValue* value = cass_schema_meta_field_value(field);

	print_indent(indent);
	printf("%.*s: ", (int)name.length, name.data);
	print_schema_value(value);
	printf("\n");
}

void print_schema_meta_fields(const CassSchemaMeta* meta, int indent) {
	CassIterator* fields = cass_iterator_fields_from_schema_meta(meta);

	while (cass_iterator_next(fields)) {
		print_schema_meta_field(cass_iterator_get_schema_meta_field(fields), indent);
	}
	cass_iterator_free(fields);
}

void print_schema_meta_entries(const CassSchemaMeta* meta, int indent) {
	CassIterator* entries = cass_iterator_from_schema_meta(meta);

	while (cass_iterator_next(entries)) {
		print_schema_meta(cass_iterator_get_schema_meta(entries), indent);
	}
	cass_iterator_free(entries);
}

void print_schema_meta(const CassSchemaMeta* meta, int indent) {
	const CassSchemaMetaField* field = NULL;
	CassString name;
	print_indent(indent);

	switch (cass_schema_meta_type(meta)) {
	case CASS_SCHEMA_META_TYPE_KEYSPACE:
		field = cass_schema_meta_get_field(meta, "keyspace_name");
		cass_value_get_string(cass_schema_meta_field_value(field), &name);
		printf("Keyspace \"%.*s\":\n", (int)name.length, name.data);
		print_schema_meta_fields(meta, indent + 1);
		printf("\n");
		print_schema_meta_entries(meta, indent + 1);
		break;

	case CASS_SCHEMA_META_TYPE_TABLE:
		field = cass_schema_meta_get_field(meta, "columnfamily_name");
		cass_value_get_string(cass_schema_meta_field_value(field), &name);
		printf("Table \"%.*s\":\n", (int)name.length, name.data);
		print_schema_meta_fields(meta, indent + 1);
		printf("\n");
		print_schema_meta_entries(meta, indent + 1);
		break;

	case CASS_SCHEMA_META_TYPE_COLUMN:
		field = cass_schema_meta_get_field(meta, "column_name");
		cass_value_get_string(cass_schema_meta_field_value(field), &name);
		printf("Column \"%.*s\":\n", (int)name.length, name.data);
		print_schema_meta_fields(meta, indent + 1);
		printf("\n");
		break;
	}
}


int lua_cass_test(lua_State* L)
{
	CassSchemaMeta* meta = lua_schema_meta_get_ptr(L, 1);
	print_schema_meta(meta, 0);
	/*//CassIterator* entries = cass_iterator_from_schema_meta(meta);
	CassIterator* entries = cass_iterator_fields_from_schema_meta(meta);
	while (cass_iterator_next(entries)) {
		const CassSchemaMeta* entry = cass_iterator_get_schema_meta(entries);

		//print_schema_meta(, indent);
		CassSchemaMetaType type = cass_schema_meta_type(entry);
		switch (type) {
		case CASS_SCHEMA_META_TYPE_KEYSPACE:
			printf("keyspace\n");
			break;

		case CASS_SCHEMA_META_TYPE_TABLE:
			printf("table\n");
			break;

		case CASS_SCHEMA_META_TYPE_COLUMN:
			printf("column\n");
			break;
		}
	}
	cass_iterator_free(entries);*/
	return 0;
}

struct luaL_reg* get_schema_meta_exported_methods ()
{
	static struct luaL_reg methods[] = {
		{ "__tostring", lua_cass_schema_meta_tostring },
		{ "type", lua_cass_schema_meta_get_type },
		{ "get_entry", lua_cass_schema_meta_get_entry },
		//{ "get_field", lua_cass_schema_meta_get_field },
		{ "get_field_name", lua_cass_schema_meta_get_field_name },
		{ "get_field_value", lua_cass_schema_meta_get_field_value },
		//{ "__gc", lua_cass_schema_meta_gc },
		{ "test", lua_cass_test },



		{ NULL, NULL }
	};



	return methods;
}
