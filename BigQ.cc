#include "Record.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "TwoWayList.h"
#include "BigQ.h"
#include "Pipe.h"
#include <cmath>
#include <iostream>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <vector>
#include <algorithm>
#include <string.h>

static char c ='a';

class Comp { 
 private:
		ComparisonEngine comparator;
		OrderMaker *mark; 
 public:
     Comp(OrderMaker *s)
     {
        mark=s;
     }       
     bool operator()(Record *x,Record *y)
     { 
		if(comparator.Compare(x,y,mark) < 0)
			return true;   
		else
			return false;
	}
};

int loadEntry(Page *page,QEntry *entry,int runNum,OrderMaker *so)
{
	Record record;
	Record *rec;
	if(! page->GetFirst(&record))
	{
		return 0;
	}
	else
	{
		rec = new Record;
		rec->Consume(&record);
		entry->setRecord(rec);
		entry->setRunNumber(runNum);
		entry->setOrderMaker(so);
		return 1;
	}
	
}

void printOffset(off_t *offt,int num)
{
	for(int i = 0; i < num; i++)
	{
		cout<<offt[i]<<"  ";
	}
	cout<<endl;
}

void performMerge(struct ThreadParameter *param,off_t *startOffsets,off_t *endOffsets,int numRuns,char *tempFileName)
{
	int runLength = param->runLength;
	File file;
	file.Open(1,tempFileName);
	int pageLength = file.GetLength();
	pageLength--;
	int numRuns1 = ceil((double)pageLength / runLength);
	//cout<<"Number of Runs : "<<numRuns<<endl;
	int lastRunPages = pageLength - ((numRuns - 1)* runLength);
	Page *pages = new Page[numRuns];
	off_t *offset = new off_t[numRuns];
	Pipe *outputPipe = param->outputPipe;
	int i,retRun;
	struct QEntry *entry,*minEntry;
	int count = 0;
	//mark1 = param->sortOrder;
	int pagesRead = 0;
	
	for(i = 0; i < numRuns; i++)
	{
		//offset[i] =  i * runLength;
		file.GetPage(&pages[i], startOffsets[i]);
		pagesRead++;
		//offset[i] = offset[i] + 1;
	}
//	printOffset(offset,numRuns);
//	printOffset(startOffsets,size);
//	printOffset(endOffsets,size);
	priority_queue<QEntry *, vector<QEntry *>, QEntry> mypq;
//	mark1 = param->sortOrder;
	
	for(i=0; i< numRuns; i++)
	{
		entry = new QEntry;
		loadEntry(&pages[i], entry, i,param->sortOrder);
		//entry->record->Print(param->schema);
		mypq.push(entry);
	}
	
	while(true)
	{
		minEntry = mypq.top();
		mypq.pop();
		
		retRun = minEntry->getRunNumber();
		outputPipe->Insert(minEntry->getRecord());
		count++;
		entry = new QEntry;
		
		int success = loadEntry(&pages[retRun],entry,retRun,param->sortOrder);
		
		if(!success)
		{
			startOffsets[retRun] = startOffsets[retRun] + 1;
			if(startOffsets[retRun] <= endOffsets[retRun])
			{
				file.GetPage(&pages[retRun], startOffsets[retRun]);
				//cout<<" "<<startOffsets[retRun];
				pagesRead++;
				loadEntry(&pages[retRun],entry,retRun,param->sortOrder);
				mypq.push(entry);
			}
			else
			{
				if(mypq.empty())
				{
					break;
				}
				else
				{
					continue;
				}
			}
		}
		else
		{
			mypq.push(entry);
		}
	}
	file.Close();
	remove(tempFileName);
	cout<<"Entered : "<<count<<endl;
	//cout<<"Value of PrintCounter : "<<printCounter<<endl;
}

void* performSort(void *parameter)
{
	cout<<"Control entered sorting"<<endl;
	struct ThreadParameter *param = (struct ThreadParameter *) parameter;
	
	vector<Record* > RecordVector;
	int appendResult = -1;
	int runLength = param->runLength;
	int i;
	off_t *startOffsets = new off_t[3000];
	off_t *endOffsets = new off_t[3000];
	int *pagesCount = new int[3000];
	int offsetCounter = 0;
	Pipe *in = param->inputPipe; 
	Record removeMe;
	int pageCounter = 0,recordCounter = 0, runCounter = 0;
	Record temp;
	Record *rec;
	Page TempPage;
    Record TempRecord;
    Comp comp(param->sortOrder);
	Page *page = new Page;
	File file;
	int recordCounter1 = 0;
	int addCounter1 = 0;
	char *tempFileName = new char[13];
	int index = 10;
	strcpy(tempFileName,"SortedRuns");
	tempFileName[index++] = c++;
	tempFileName[index] = '\0';
	
	file.Open(0,tempFileName);
	
	off_t fileOffset = 0;
	int totalPageCounter = 0;
	int pagesAdded = 0;
	while(in->Remove(&removeMe))
	{
		recordCounter1++;
		//cout<<"Removed : "<<recordCounter1<<endl;
		if(!page->Append(&removeMe))
		{
			while(page->GetFirst(&temp))
			{
				rec = new Record;
				rec->Consume(&temp);
				RecordVector.push_back(rec);
			}
			pageCounter++;
			page->Append(&removeMe);
			if(pageCounter == runLength)
			{
				//cout<<"Run Build Started"<<endl;
				pageCounter = 0;
				runCounter++;
				sort(RecordVector.begin(),RecordVector.end(),comp);
				//cout<<"Sort Completed"<<endl;
				//cout<<"Run Started : PagesAdded Value : "<<fileOffset<<endl;
				startOffsets[offsetCounter] = fileOffset;

				for(int i = 0; i < RecordVector.size();i++)
				{

					TempRecord.Consume(RecordVector[i]);
					appendResult = TempPage.Append(&TempRecord);
					addCounter1++;
					//cout<<"Add Counter "<<addCounter1<<endl;
					if(!appendResult)
					{     
						
						file.AddPage(&TempPage,fileOffset);
						pagesCount[fileOffset] = 1;
						pagesAdded++;
						fileOffset = fileOffset + 1;   
						TempPage.EmptyItOut();
						TempPage.Append(&TempRecord);
					}
				}

				file.AddPage(&TempPage,fileOffset);
				pagesAdded++;
				fileOffset = fileOffset + 1;   
				TempPage.EmptyItOut();
				RecordVector.clear();
				endOffsets[offsetCounter++] = fileOffset - 1;
				//cout<<"Run Ended Pages Added Value : "<<fileOffset<<endl;
				//cout<<"Run Build ended"<<endl;
			}
		}
	}
	while(page->GetFirst(&temp))
	{
		rec = new Record;
		rec->Consume(&temp);
		RecordVector.push_back(rec);
	}
	pageCounter++;
	runCounter++;
	sort(RecordVector.begin(),RecordVector.end(),comp);
	startOffsets[offsetCounter] = fileOffset;
	//cout<<"Run Started : PagesAdded Value : "<<pagesAdded<<endl;
	pagesAdded = 0;
	for(int i = 0; i < RecordVector.size();i++)
	{
		TempRecord.Consume(RecordVector[i]);
		if(!TempPage.Append(&TempRecord))
		{     
			file.AddPage(&TempPage,fileOffset);
			pagesCount[fileOffset] = 1;
			pagesAdded++;
			fileOffset = fileOffset + 1;   
			TempPage.EmptyItOut();
			TempPage.Append(&TempRecord);
		}
	}
	file.AddPage(&TempPage,fileOffset++);
	pagesAdded++;
	TempPage.EmptyItOut();
	RecordVector.erase(RecordVector.begin(),RecordVector.end());
	endOffsets[offsetCounter++] = fileOffset - 1;
//	cout<<"Run Ended Pages Added Value : "<<pagesAdded<<endl;
//	cout<<"Final Value of Pages Added :"<<endl;
	file.Close();
	performMerge(param,startOffsets,endOffsets,offsetCounter,tempFileName);
	param->outputPipe->ShutDown ();
}




BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
	pthread_t thread;
	struct ThreadParameter* param = new ThreadParameter();
	
	param->inputPipe = &in;
	param->outputPipe = &out;
	param->sortOrder = &sortorder;
	param->runLength = runlen;


	if(pthread_create(&thread, NULL,&performSort, (void *)param))
	{
		//Exception for Pthread: exit in case of error	
		cout<<"\nError: Thread couldn't be created\n";
		
		exit(-1);
	}
	//pthread_join(thread,NULL);
	//out.ShutDown ();
}

BigQ::~BigQ () {
}
