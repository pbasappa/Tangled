#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

#include <pthread.h>

using namespace std;

//create a structure for each operator to hold all the parameters for a thread
struct WriteOutThreadParameter{
	Pipe *inputPipe;
	Schema *mySchema;
	FILE  *outFile;
};
struct DuplicateRemovalThreadParameter
{
Pipe *inputPipe;
Pipe *outputPipe;
Schema *mySchema;
int runLength;
};

struct GroupByThreadParameter
{
    Pipe *inputPipe;
    Pipe *outputPipe;
    OrderMaker *groupAtts;
    Function *computeMe;
	int runLength;
};


struct JoinThreadParameter
{
	Pipe *leftPipe;
	Pipe *rightPipe;
	Pipe *outputPipe;
	CNF *cnfExpression;
	Record *litRecord;
	Schema *lschema;
	Schema *rschema;
	int pageUsed;
};

struct SumThreadParameter
{
	Pipe *inputPipe;
	Pipe *outputPipe;
	Function *func;
};

struct ProjectThreadParameter
{

	Pipe *inputPipe;
	Pipe *outputPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
};


struct spThreadParameter
{
	Pipe *inputPipe;
	Pipe *outputPipe;
	CNF *selOperator;
	Record *litRecord;
};

struct sfThreadParameter
{
	DBFile *dbFile;
	Pipe *outputPipe;
	CNF *selOperator;
	Record *litRecord;
	Schema *schema;
};

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

private:
	pthread_t thread;
	//Record *buffer;

public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class SelectPipe : public RelationalOp {

private:
	pthread_t thread;

public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) ;
	void WaitUntilDone ();
	void Use_n_Pages (int n) ;

/* Original Code
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
*/
	
};



class Project : public RelationalOp { 
	
	private:

	pthread_t thread;
	
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class Sum : public RelationalOp {

	private:
	
	pthread_t thread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n) ;
};


class Join : public RelationalOp { 
	
	private:
	
	pthread_t thread;
	int pageUsed;

	public:
	Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class DuplicateRemoval : public RelationalOp {
private:
	pthread_t thread;
	int runLength;

	public:
	DuplicateRemoval();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};


class WriteOut : public RelationalOp {
private:
	pthread_t thread;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone () ;
	void Use_n_Pages (int n);
};


class GroupBy : public RelationalOp {
private:
    pthread_t thread;
    int runLength;
    public:
    GroupBy();
    void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif
