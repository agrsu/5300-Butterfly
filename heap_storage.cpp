#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include "heap_storage.h";

using namespace std;



#pragma region SlottedPage

/*
	PUBLIC
*/

typedef u_int16_t u16;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
	if (is_new) {
		this->num_records = 0;
		this->end_free = DbBlock::BLOCK_SZ - 1;
		put_header();
	}
	else {
		get_header(this->num_records, this->end_free);
	}
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
	if (!has_room(data->get_size()))
		throw DbBlockNoRoomError("not enough room for new record");
	u16 id = ++this->num_records;
	u16 size = (u16)data->get_size();
	this->end_free -= size;
	u16 loc = this->end_free + 1;
	put_header();
	put_header(id, size, loc);
	memcpy(this->address(loc), data->get_data(), size);
	return id;
}

Dbt* SlottedPage::get(RecordID record_id) {
	u16 size, loc;
	get_header(size, loc, record_id);
	if (loc == NULL)
		return NULL;
	Dbt* temp = new Dbt(this->address(loc), size);
	return temp;
}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
	u16 size, loc;
	get_header(size, loc, record_id);
	u16 new_size = (u16)data.get_size();
	if (new_size > size) {
		u16 extra = new_size - size;
		if (!has_room(extra))
			throw DbBlockNoRoomError("not enough room for enlarged record");
		slide(loc, loc - extra);
		memcpy(this->address(loc - extra), data.get_data(), new_size);
	}
	else {
		memcpy(this->address(loc), data.get_data(), new_size);
		slide(loc + new_size, loc + size);
	}
	get_header(size, loc, record_id);
	put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id) {
	u16 size, loc;
	get_header(size, loc, record_id);
	put_header(record_id, 0, 0);
	slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) {
	RecordIDs* recID = new RecordIDs();
	for (int i = 1; i <= this->num_records; i++) {
		u16 size, loc;
		get_header(size, loc, i);
		if (loc != 0)
			recID->push_back(i);
	}
	return recID;
}


/*
	PROTECTED
*/

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
	size = get_n(4 * id);
	loc = get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
	if (id == 0) { // called the put_header() version and using the default params
		size = this->num_records;
		loc = this->end_free;
	}
	put_n(4 * id, size);
	put_n(4 * id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
	u16 free = this->end_free - ((this->num_records + 1) * 4);
	return (size <= free);
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
	u16 shift = end - start;

	if (shift == 0)
		return;

	memcpy(this->address(end_free + 1), this->address(end_free + 1 + shift), shift);

	u16 size, loc;
	RecordIDs* recID = ids();
	for (unsigned int i = 0; i < recID->size(); i++) {
		RecordID id = recID->at(id);
		get_header(size, loc, id);
		if (loc <= start) {
			loc += shift;
			put_header(id, size, loc);
		}
	}

	end_free += shift;
	put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
	return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
	*(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
	return (void*)((char*)this->block.get_data() + offset);
}

#pragma endregion



#pragma region HeapFile

/*
	PUBLIC
*/

//HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
//	this->dbfilename = this->name + ".db";
//}

void HeapFile::create(void) {
	//create physical file
	db_open(DB_CREATE | DB_EXCL);
	get_new();
}

void HeapFile::drop(void) {
	//delete physial file
	close();
	Db db(_DB_ENV, 0);
	db.remove(this->dbfilename.c_str(), nullptr, 0);
}

void HeapFile::open(void) {
	//Open physical file
	db_open();
}

void HeapFile::close(void) {
	//Close the  physical file
	this->db.close(0);
	this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
	char block[DbBlock::BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));

	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));

	// write out an empty block and read it back in so Berkeley DB is managing the memory
	SlottedPage* page = new SlottedPage(data, this->last, true);
	this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
	this->db.get(nullptr, &key, &data, 0);
	return page;
}

SlottedPage* HeapFile::get(BlockID block_id) {
	Dbt key(&block_id, sizeof(block_id));
	Dbt data;
	this->db.get(nullptr, &key, &data, 0);
	return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock* block) {

	int block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids() {

	BlockIDs* vec = new BlockIDs();
	for (BlockID block_id = 1; block_id <= this->last; block_id++)
		vec->push_back(block_id);
	return vec;
}


/*
	PROTECTED
*/

void HeapFile::db_open(uint flags) {
	if (!this->closed)
		return;
	this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists
	this->dbfilename = this->name + ".db";
	this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);
	//this->last = flags ? 0 : get_block_count();
	this->closed = false;
}

#pragma endregion



#pragma region HeapTable

/*
	PUBLIC
*/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) :
	DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

void HeapTable::create() {
	file.create();
}

void HeapTable::create_if_not_exists() {
	try {
		open();
	}
	catch (DbException& e) {
		create();
	}
}

void HeapTable::drop() {
	file.drop();
}

void HeapTable::open() {
	file.open();
}

void HeapTable::close() {
	file.close();
}

Handle HeapTable::insert(const ValueDict* row) {
	open();
	ValueDict* full_row = validate(row);
	Handle handle = append(full_row);
	delete full_row;
	return handle;
}

void HeapTable::update(const Handle handle, const ValueDict* new_values) {
	throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
	open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file.get(block_id);
	block->del(record_id);
	this->file.put(block);
	delete block;
}

Handles* HeapTable::select() {
	return select(nullptr);
}

Handles* HeapTable::select(const ValueDict* where) {
	Handles* handles = new Handles();
	BlockIDs* block_ids = file.block_ids();
	for (auto const& block_id : *block_ids) {
		SlottedPage* block = file.get(block_id);
		RecordIDs* record_ids = block->ids();
		for (auto const& record_id : *record_ids)
			handles->push_back(Handle(block_id, record_id));
		delete record_ids;
		delete block;
	}
	delete block_ids;
	return handles;
}

ValueDict* HeapTable::project(Handle handle) {
	return project(handle, &this->column_names);
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = file.get(block_id);
	Dbt* data = block->get(record_id);
	ValueDict* row = unmarshal(data);
	delete data;
	delete block;
	if (column_names->empty())
		return row;
	ValueDict* result = new ValueDict();
	for (auto const& column_name : *column_names) {
		if (row->find(column_name) == row->end())
			throw DbRelationError("table does not have column named '" + column_name + "'");
		(*result)[column_name] = (*row)[column_name];
	}
	return result;
}


/*
	PROTECTED
*/

ValueDict* HeapTable::validate(const ValueDict* row) {
	ValueDict* full_row = new ValueDict();
	for (auto const& column_name : this->column_names) {
		Value value;
		ValueDict::const_iterator column = row->find(column_name);
		if (column == row->end())
			throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
		else
			value = column->second;
		(*full_row)[column_name] = value;
	}
	return full_row;
}

Handle HeapTable::append(const ValueDict* row) {
	Dbt* data = marshal(row);
	SlottedPage* block = this->file.get(this->file.get_last_block_id());
	RecordID record_id;
	try {
		record_id = block->add(data);
	}
	catch (DbBlockNoRoomError& e) {
		// need a new block
		block = this->file.get_new();
		record_id = block->add(data);
	}
	this->file.put(block);
	delete[](char*)data->get_data();
	delete data;
	return Handle(this->file.get_last_block_id(), record_id);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
	char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name : this->column_names) {
		ColumnAttribute ca = this->column_attributes[col_num++];
		ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;
		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			*(int32_t*)(bytes + offset) = value.n;
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
			uint size = value.s.length();
			*(u16*)(bytes + offset) = size;
			offset += sizeof(u16);
			memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
		}
		else {
			throw DbRelationError("Only know how to marshal INT and TEXT");
		}
	}
	char *right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) {
	ValueDict *row = new ValueDict();
	Value value;
	char *bytes = (char*)data->get_data();
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name : this->column_names) {
		ColumnAttribute ca = this->column_attributes[col_num++];
		value.data_type = ca.get_data_type();
		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			value.n = *(int32_t*)(bytes + offset);
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
			u16 size = *(u16*)(bytes + offset);
			offset += sizeof(u16);
			char buffer[DbBlock::BLOCK_SZ];
			memcpy(buffer, bytes + offset, size);
			buffer[size] = '\0';
			value.s = std::string(buffer);  // assume ascii for now
			offset += size;
		}
		else {
			throw DbRelationError("Only know how to unmarshal INT and TEXT");
		}
		(*row)[column_name] = value;
	}
	return row;
}

#pragma endregion



// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}
