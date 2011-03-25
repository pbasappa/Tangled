#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

struct ThreadParameter
{
	Pipe *inputPipe;
	Pipe *outputPipe;
	OrderMaker *sortOrder;
	int runLength;
};

class QEntry
{
	private:
	
	Record *record;
	int runNumber;	
	OrderMaker *sortOrder;
	
	
	public:
	
	
	void setRecord(Record *rec)
	{
		record = rec;
	}
	
	void setRunNumber(int num)
	{
		runNumber = num;
	}
	
	void setOrderMaker(OrderMaker *so)
	{
		sortOrder = so;
	}
	
	Record* getRecord()
	{
		return record;
	}
	
	int getRunNumber()
	{
		return runNumber;
	}
	
	OrderMaker* getSortOrder()
	{
		return sortOrder;
	}
	
	int operator()(QEntry *x,QEntry *y)
    {  
		Record *r1;
        Record *r2;
		r1=x->getRecord();
		r2=y->getRecord();
		ComparisonEngine comparator;
		int result = comparator.Compare(r1,r2,x->getSortOrder()); 
		if(result >= 0)
			return 1;   
		else
			return 0;
	}
};

class PQueue{

private:
		OrderMaker *sortOrder;
		struct QEntry **queue;
		int numOfRuns;
		int currentNum;
public:
		PQueue(int numOfRuns,OrderMaker *so);
		~PQueue();
		void buildHeap(int index, QEntry *entry);
		void printHeap();
		void minHeapify(int index);
		int leftChild(int index);
		int rightChild(int index);
		struct QEntry* extractMin();
		void insert(QEntry *entry);
		int fixHeap();
};


class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runs);
	~BigQ ();
	

};

#endif
