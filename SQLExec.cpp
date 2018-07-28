/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
	if(column_names)
		delete column_names;
	if(column_attributes)
		delete column_attributes;
	if(rows){
		for (auto row : *rows)
			delete row;
		delete rows;
	}
}


QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
	if (!SQLExec::tables)
		SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
	colName = col->name;
	if(col->type == ColumnDefinition::INT)
		column_attribute.set_data_type(ColumnAttribute::INT);
	else if(col->type == ColumnDefinition::TEXT)
		column_attribute.set_data_type(ColumnAttribute::TEXT);
	else
		throw SQLExecError("Unsupported data type, supported data types are: INT and TEXT");

}

QueryResult *SQLExec::create(const CreateStatement *statement) {
	if (statement->type != CreateStatement::kTable)
		return new QueryResult("Create table called with other statment type");//Change this text to something more professional

	//Add new table to _tables in schema
	Identifier tableName = statement->tableName;
	ValueDict row;
	row["table_name"] = tableName;
	Handle tableHandle = WQLExec::tables->insert(&row);
	
	//get new columns
	Identifier colName;
	ColumnNames colNames;
	ColumnAttribute colAttrib;
	ColumnAttributes colAttribs;

	for(ColumnDefinition* col : *statement->colummns) {
		column_definition(col, colName, colAttrib);
		colNames.push_back(colName);
		colAttribspush_back(colAttrib);
	}
	
	//update _columns in schema
	try {
		DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
		Handles colHandles;
		
		try {
			for(unsigned int i = 0; i < colNames.size(); i++) {
				row["column_name"] = colNames[i];
				row["data_type"] = Value(colAttribs[i].get_data_type == ColumnAttribute::INT ? "INT" : "TEXT");
				colHandles.push_back(columns.insert(&row));
			}

			//Create the relation
			DbRelation& newTable = SQLExec::tables->get_table(tableName);
			if (statement->ifNotExists){
				table.create_if_not_exists();
			}
			else {
				table.create();
			}
		}
		catch (exception& e) {
			//remove any new columns from _columns
			try {
				for(unsigned int i = 0; i < colHandles.size(); i++){
					columns.del(colHandles.at(i));
				}
			}
			catch (...) {} //TODO
			throw; //create table exception
		}
	}
	catch (exception& e) {
		//remove the new table from _tables
		try {
			SQLExec::tables->del(tableHandle);
		}
		catch (...) {} //TODO
		throw;
	}
	
	return new QueryResult("Created: " + tableName);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
  switch(statement->type) {
  case DropStatement::kTable:
    return drop_table(statement);
  case DropStatement::kIndex:
    return drop_index(statement);
  default:
    return new QueryResult("unsupported DROP type");
  }
}
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
  if (statement->type != DropStatment::kTable)
    return new QueryResult("Drop table called with other statement type");

  Identifier tableName = statement->name;

  //Check if table is schema table (not allowed to be dropped)
  if (tableName == Tables::TABLE_NAME || tableName == Columns::TABLE_NAMES)
    throw SQLExecError("Cannot drop a schema table");

  //Get table information
  DbRelation& table = SQLExec::tables->get_table(tableName);
  ValueDict dropTarget;
	 locationValDict["table_name"] = Value(tableName);
  DbRelation& cols = SQLExec:tables->get_table(Columns::TABLE_NAME);
  Handles* handles = cols.select(&locationValDict);

  /*TODO M4: Remove indices
  DbRelation& indices = SQLExec::tables->get_table(Indices::TABLE_NAME);
  Handles* index_handles = indices.select(&locationValDict);
  vector<Value> dropList;
  */
  //for(unsigned int i = 0; i < index_handles->size(); i++) {
  //ValueDict*

  delete handles;

  //delete the table
  table.drop();

  //delete the table from _tables
  SQLExec::tables->del(*SQLExec::tables->select(&dropTarget)->begin());
  return new QueryResult("Dropped: " + tableName);
}

/* @purpose - implement appropriate show function based on the type of
 * statement
 * @return - a QueryResult or error if statement type not supported
 */
QueryResult *SQLExec::show(const hsql::ShowStatement *statement)
{
  switch(statement->type){
  case ShowStatement::kTables:
    return show_tables();
  case ShowStatement::kColumns:
    return show_columns(statement);
    //  case ShowStatement::kIndex:
    //return show_index(statement);
  default:
    throw SQLExecError("SHOW type not recognized/supported");
  }
}

/*
QueryResult *SQLExec::show_index(const ShowStatement *statement) {

  ColumnNames* colNames = new ColumnNames;
  ColumnAttributes* colAttri = new ColumnAttributes;
  ValueDict tableVal;

  tableVal["table_name"] = Value(statement->tableName);
  colAttri->push_back(ColumnAttribute(ColumnAttribute::TEXT));
  colAttri->push_back(ColumnAttribute(ColumnAttribute::INT));
  colAttri->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));
  colNames->push_back("table_name");
  colNames->push_back("index_name");
  colNames->push_back("seq_in_index");
  colNames->push_back("index_type");
  colNames->push_back("is_unique");

  tableVal["table_name"] = Value(statement->tableName);
  Handles* handles = SQLExec::indices->select(&tableVal);
  u_int rowCount = handesl->size();

  for(auto const& currHandle: *handles){
    ValueDict* currRow = SQLExec::indices->project(currHandle, colNames);
    rows->push_back(currRow);
  }

  delete handles;
  return new QueryResult(coNames, colAttri, rows, "successfully returned " +
                         to_string(rowCount) + " rows");
}


/* @purpose - execute the SQL SHOW TABLES
 * @return - a QueryResult variable containting
 */
QueryResult *SQLExec::show_tables() {
	 ColumnNames* colNames = new ColumnNames;
  ColumnAttributes* colAttr = new ColumnAttributes;
  ValueDicts* rows = new ValuesDict;

  //set variable values
  Handles* handles = SQLExec::tables->select();
  u_int rowCount = handles->size()-2;
  colNames->push_back("table_name");
  colAttributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  for(auto const& currHandle: *handles){
    ValueDict* currRow = SQLExec::tables->project(currHandle, colNames);
    Identifier table_name = currRow->at("table_name").s;
    if((table_name != Columns::TABLE_NAME) &&
       (table_name != Tables::TABLE_NAME)){
      rows->push_back(currRow);
    }
  }

  delete handles;
  return QueryResult(colName, colAttr, row, "successfully returned " +
                     to_string(rowCount) + " rows");
}
/* @purpose - execute SHOW COLUMNS FROM sql statement
 * @param - a statement pertaining what columns to show from
 * @return - a queryresult of the requested show data
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
	 DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

  ColumnNames* colNames = new ColumnNames;
  ColumnAttributes* colAttr = new ColumnAttributes;
  ValueDicts* rows = new ValuesDict;
  ValueDict tableVal;

  tableVal["table_name"] = Value(statement->tableName);
  Handles* handles = columns.select(&tableVal);
  u_int rowCount = handle->size();

  colNames->push_back("table_name");
  colNames->push_back("column_name");
  colNames->push_back("data_type");
  colAttributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  for(auto const& currHandle: *handles){
    ValueDict* currRow = SQLExec::tables->project(currHandle, colNames);
    Identifier table_name = currRow->at("table_name").s;
  }

  delete handles;
  return QueryResult(colName, colAttr, row, "successfully returned " +
                     to_string(rowCount) + " rows");
}

