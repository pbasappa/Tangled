#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "Defs.h"
#include "BigQ.h"

//#include "genericDBFile.h"

typedef enum {heap, sorted, tree} fType;

class genericDBFile
{	
//	private:
//		virtual genericDBFile genInst;
	public:
		genericDBFile();
		
		virtual int Create (char *fpath, fType file_type, void *startup) = 0;
		virtual int Open (char *fpath) = 0;
		virtual int Close () = 0;
		virtual void Load (Schema &myschema, char *loadpath) = 0;
		virtual void MoveFirst () = 0;
		virtual void Add (Record &addme) = 0;
		virtual int GetNext (Record &fetchme) = 0;
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal,Schema *schema) = 0;
		
};

class DBFile {
private:
	
	genericDBFile *genInstance;
	
public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal, Schema *schema);

};

class HeapFile : public genericDBFile
{
	private:
	// currentPage: Holds the reference of the Page from which next Record is read.
	Page *currentPage;
	
	// lastPage: Holds the reference to the Page where new record is added.
	Page *lastPage;
	
	// file: Holds the reference to the Internal file.
	File file;

	// numOfPages : This value contains the number of Pages currently in the file.
	off_t numOfPages; 
	
	//ftype: This indicates the type of the Page. 
	fType ftype; 

	//pageRead: This hold the last read page from the file
	off_t pageRead;
	
	//fileOpened: Acts like a boolean value to indicate whether the file is opened or not. 
	//			  Used to handle error conditions.
	// 0 = File Not Opened
	// 1 = File Opened
	int fileOpened;

	//DirtyPage handler
	int dirtyPage;
	
	//nextCount: This variable is used to count the number of GetNext calls on currentPage 
	int nextCount;
	
	public:
		HeapFile();
		
		int Create (char *fpath, fType file_type, void *startup);
		int Open (char *fpath);
		int Close ();	
		void Load (Schema &myschema, char *loadpath);	
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	
};

class SortedFile : public genericDBFile
{
	private:

	Pipe *inputPipe;
	Pipe *output;   	
	int mode;	// 0 - Read Mode	1 - Write Mode
	BigQ *bigq;
	File file;
	Page *currentPage;
	Schema *schema;
	
	char* fileName;
	off_t offset;
	
	struct SortInfo{
		OrderMaker *myOrder;
		int runLength;
	}si;

	int *attNum;
	Type *attType;
	int numOfAtts;
	
	OrderMaker *query;
	OrderMaker  *litQuery;
	
	//Binary Search
	off_t numOfPages;
	int binarySearchPerformed;
	int querySetCalled;
	int querySet;
	
	public:
		SortedFile();
		
		int Create (char *fpath, fType file_type, void *startup);
		int Open (char *fpath);
		int Close ();	
		void Load (Schema &myschema, char *loadpath);	
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal,Schema *schema);		
	
		void Merge();
		void LoadFromPipe();
		
		int setQueryOrderMaker(CNF &cnf);
		
};
#endif
