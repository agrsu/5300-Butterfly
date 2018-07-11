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
	//implement
}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
	//implement
}

void SlottedPage::del(RecordID record_id) {

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

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0) {
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
	//implement
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
	//implement
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

void HeapFile::create(void) {
	//implement
}

void HeapFile::drop(void) {
	//implement
}

void HeapFile::open(void) {
	//implement
}

void HeapFile::close(void) {
	//implement
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
	char block[DB_BLOCK_SZ];
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
	//implement
}

void HeapFile::put(DbBlock* block) {
	//implement
}

BlockIDs* HeapFile::block_ids() {
	//implement
}


/*
	PROTECTED
*/

void HeapFile::db_open(uint flags = 0) {
	//implement
}

#pragma endregion



#pragma region HeapTable

/*
	PUBLIC
*/

void HeapTable::create() {
	//implement
}

void HeapTable::create_if_not_exists() {
	//implement
}

void HeapTable::drop() {
	//implement
}

void HeapTable::open() {
	//implement
}

void HeapTable::close() {
	//implement
}

Handle HeapTable::insert(const ValueDict* row) {
	//implement
}

void HeapTable::update(const Handle handle, const ValueDict* new_values) {
	//implement
}

void HeapTable::del(const Handle handle) {
	//implement
}

Handles* HeapTable::select() {
	//implement
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
	//implement
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
	//implement
}


/*
	PROTECTED
*/

ValueDict* HeapTable::validate(const ValueDict* row) {
	//implement
}

Handle HeapTable::append(const ValueDict* row) {
	//implement
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
	//implement
}

#pragma endregion