#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Pipe.h"
#include "BigQ.h"

#include <iostream>
#include <cstring>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <cmath>

using namespace std;

static int countme = 0;
genericDBFile::genericDBFile()
{
	
}

DBFile::DBFile ()
{

}

int DBFile::Create (char *fpath, fType file_type, void *startup)
{
	if(file_type == 0)
	{
		//genInstance = new HeapFile();
		if(genInstance->Create(fpath, file_type, startup) == 1)
		{
			cout<<"Create Heap Success\n";
		}
	}
	
	if(file_type == 1)
	{
		genInstance = new SortedFile();
		if(genInstance->Create(fpath, file_type, startup) == 1)
		{
			cout<<"Create Sorted Success\n";
		}
	}
}

int DBFile::Open (char *fpath)
{
	char *header = ".mdata";	
	char *concatString;	
	
	int fpathLen = strlen(fpath);
	int concatStringLen = fpathLen + strlen(header);
	concatString = new char[concatStringLen + 1];
	
	for(int i = 0;i < fpathLen;i++)
	{
		concatString[i] = fpath[i];
	}
	
	for(int i = fpathLen,j = 0; i < concatStringLen; i++)
	{
		concatString[i] = header[j++];
	}
	
	concatString[concatStringLen] = '\0';
		
	ifstream reader;	
	reader.open(concatString, ifstream::in);
	
//	if(reader.good())
//	{
		string line;		
		getline(reader, line);
		reader.close();
		
		if(!line.compare("heap"))
		{
			//genInstance = new HeapFile();
			genInstance->Open(fpath);
		}
		else
		{
			genInstance = new SortedFile();
			genInstance->Open(fpath);
		}
}

int DBFile::Close ()
{
	
	genInstance->Close();
	
}

void DBFile::Load (Schema &myschema, char *loadpath)
{

}

void DBFile::MoveFirst ()
{
	genInstance->MoveFirst();
}

void DBFile::Add (Record &addme)
{
	genInstance->Add(addme);
}

int DBFile::GetNext (Record &fetchme)
{
	genInstance->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal, Schema *schema)
{
	genInstance->GetNext(fetchme,cnf,literal,schema);
}

//HeapFile class member functions below

HeapFile::HeapFile () 
{
	lastPage = NULL;
	currentPage = NULL;
	dirtyPage = 0;
}

int HeapFile::Create (char *f_path, fType f_type, void *startup) 
{
	file.Open(0,f_path);
	lastPage = new Page;
	ftype = f_type;
	numOfPages = 0;
	pageRead = 0;
	fileOpened = 1;
	nextCount = 0;

	//creating the metadata file
	char *header = ".mdata";	
	char *concatString;	
	
	int fpathLen = strlen(f_path);
	int concatStringLen = fpathLen + strlen(header);
	concatString = new char[concatStringLen + 1];
	
	for(int i = 0;i < fpathLen;i++)
	{
		concatString[i] = f_path[i];
	}
	
	for(int i = fpathLen,j = 0; i < concatStringLen; i++)
	{
		concatString[i] = header[j++];
	}
	
	concatString[concatStringLen + 1] = '\0';
	
	ofstream writer;
	writer.open(concatString);
	writer<<"heap";
	writer.close();
	return 1;
}

int HeapFile::Open (char *f_path) {
	
	file.Open(1,f_path);
	
	currentPage = new Page;
	file.GetPage(currentPage,0);
	pageRead = 1;
	
	nextCount = 0;
	
	lastPage = new Page;
	numOfPages = file.GetLength();

	if(numOfPages != 0)
	{
		numOfPages--;
		file.GetPage(lastPage,numOfPages -1);
	}
	fileOpened = 1;
}

void HeapFile::Load (Schema &f_schema, char *loadpath) 
{
	FILE *fp = fopen(loadpath, "r");
	Record temp;
	
	if(!fileOpened)
	{
		cout<< " DBFile needs to be opened first";
		return;
	}
		
	int count = 0;
	while(temp.SuckNextRecord(&f_schema,fp))
	{
		if(!lastPage->Append(&temp))
		{
				if(numOfPages == 0)
				{
					file.AddPage(lastPage,numOfPages++);
				}
				else
				{
					file.AddPage(lastPage,numOfPages - 1);
				}
				numOfPages++;
				lastPage->EmptyItOut();
				lastPage->Append(&temp);
		}
		//dirtyPage = 1;	
	}
	//This is to add the final page into the file
	if(numOfPages == 0)
	{
		file.AddPage(lastPage,numOfPages++);
	}
	else
	{
		file.AddPage(lastPage,numOfPages - 1);
	}
	numOfPages++;
	lastPage->EmptyItOut();
	
	fclose(fp);
}

void HeapFile::MoveFirst () 
{	
	if(!fileOpened)
	{
		cout<<"DBFile needs to be opened first";
		return;
	}

	if(dirtyPage)
	{
		if(numOfPages == 0)
		{
			file.AddPage(lastPage,numOfPages);
		}
		else
		{
			file.AddPage(lastPage,numOfPages - 1);
		}
		dirtyPage = 0;
	}

	if(currentPage != NULL)
		currentPage->EmptyItOut();
	else
		currentPage = new Page;
	
	file.GetPage(currentPage,0);
	pageRead = 1;
	
	//Reset nextCount since we moved to the first record of the file
	nextCount = 0;
}

int HeapFile::Close () 
{	
	if(lastPage != NULL)
	{
		if(numOfPages == 0)
		{
			file.AddPage(lastPage,numOfPages);
		}
		else
		{
			file.AddPage(lastPage,numOfPages - 1);
		}
			
		lastPage->EmptyItOut();
		delete lastPage;
	}
	
	if(currentPage != NULL)
	{
		currentPage->EmptyItOut();
		delete currentPage;
	}
	
	file.Close();
	
	fileOpened = 0;
}

void HeapFile::Add (Record &rec) 
{
	if(!fileOpened)
	{
		cout<<"DBFile needs to be opened first";
		return;
	}
	
	dirtyPage = 1;
	if(!lastPage->Append(&rec))
	{
		if(numOfPages == 0)
		{
			file.AddPage(lastPage,numOfPages++);
		}
		else
		{
			file.AddPage(lastPage,numOfPages - 1);
		}
		numOfPages++;
		lastPage->EmptyItOut();
		lastPage->Append(&rec);
	}
}

int HeapFile::GetNext (Record &fetchme) 
{	
	if(dirtyPage)
	{
		if(numOfPages == 0)
		{
			file.AddPage(lastPage,numOfPages);
			
			//If there was a current page then it was definitely the last page
			if(currentPage != NULL)
			{
				file.GetPage(currentPage,numOfPages);
				for(int i=0; i<=nextCount; i++)
				{
					Record fetch;
					currentPage->GetFirst(&fetch);
				}
			}
		}
		else
		{
			file.AddPage(lastPage,numOfPages - 1);
			
			//If the current page was the last page before write then
			if(pageRead == numOfPages)
			{
				if(currentPage != NULL)
				{
					file.GetPage(currentPage,numOfPages - 1);
					for(int i=0; i<=nextCount; i++)
					{
						Record fetch;
						currentPage->GetFirst(&fetch);
					}
				}
			}
		}
		dirtyPage = 0;
	}
	else
	{
		//Everytime GetNext is called this count is incremented
		nextCount++;
	}
	
	if(currentPage == NULL)
	{
		currentPage = new Page;
		file.GetPage(currentPage,0);
		//When a new currentPage is loaded the nextCount has to be reset
		nextCount = 0;
		pageRead = 1;
	}
	
	
	if(!currentPage->GetFirst(&fetchme))
	{
		if(pageRead == numOfPages)
		{
			return 0;
		}
		else
		{
			file.GetPage(currentPage,pageRead++);
			currentPage->GetFirst(&fetchme);
			//Here the value of nextCount is set to 1 after a new currentPage is loaded
			//and the first record is fetched from it
			nextCount = 1;
			return 1;
		}
			
	}
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{	
	ComparisonEngine engine;
	
	while(true)
	{
		if(GetNext(fetchme))
		{
			if(engine.Compare(&fetchme,&literal,&cnf) == 1)
				return 1;
			else 
				continue;
		}
		else
		{
			return 0;
		}
	}
}

//SortedFile class member functions here

SortedFile::SortedFile ()
{
	//Page *currentPage = new Page;
	currentPage = NULL;
}

int SortedFile::Create (char *f_path, fType f_type, void *startup)
{
	file.Open(0,f_path);
	
	fileName = f_path;
	struct exp 
	{
		OrderMaker *o; int l;
	};
	
	struct exp *newStartUp = (struct exp *) startup;
	
	//creating the metadata file
	char *header = ".mdata";	
	char *concatString;	
	
	int fpathLen = strlen(f_path);
	int concatStringLen = fpathLen + strlen(header);
	concatString = new char[concatStringLen + 1];
	
	for(int i = 0;i < fpathLen;i++)
	{
		concatString[i] = f_path[i];
	}
	
	for(int i = fpathLen,j = 0; i < concatStringLen; i++)
	{
		concatString[i] = header[j++];
	}
	
	concatString[concatStringLen] = '\0';

	newStartUp->o->Write(concatString, newStartUp->l);
	
	numOfPages = 0;
}

int SortedFile::Open (char *fpath)
{
	fileName = fpath;
	
	//Creating the name of the metadata file
	char *header = ".mdata";	
	char *concatString;	
	
	int fpathLen = strlen(fpath);
	int concatStringLen = fpathLen + strlen(header);
	concatString = new char[concatStringLen + 1];
	
	for(int i = 0;i < fpathLen;i++)
	{
		concatString[i] = fpath[i];
	}
	
	for(int i = fpathLen,j = 0; i < concatStringLen; i++)
	{
		concatString[i] = header[j++];
	}
	
	concatString[concatStringLen] = '\0';
	
	ifstream reader;
	reader.open(concatString, ifstream::in);
	
	string line;
	int lineCount = 1;
	int numAtts = 0;
	int endOfFile = 0;
	OrderMaker *sortOrder = new OrderMaker;
	int index = 0;
	int runLen;
	while(reader.good())
	{
		getline(reader,line);
		
		if(line.empty())
			break;
			
		if(lineCount == 2)
		{
			const char *cLine;
			cLine = line.c_str();
			runLen = atoi(cLine);
			
		}

		if(lineCount == 3)
		{
			const char *cLine;
			cLine = line.c_str();
			int numAtts = atoi(cLine);
			sortOrder->setNumAtts(numAtts);
			attNum = new int[numAtts];
			attType = new Type[numAtts];
			numOfAtts = numAtts;
		}
	
		if(lineCount > 3)
		{
				const char *cLine;
				cLine = line.c_str();
				int attIndex = atoi(cLine);
				sortOrder->addAtt(index, attIndex);				
				getline(reader,line);
				attNum[index] = attIndex;
				if(line.compare("String") == 0)
				{
					sortOrder->addType(index, String);
					attType[index] = String;
				}else if(line.compare("Int") == 0)
				{
					sortOrder->addType(index, Int);
					attType[index] = Int;
				}
				else
				{
					attType[index] = Double;
					sortOrder->addType(index, Double);
				}
				
				index++;
		}
		lineCount++;	
	}
	reader.close();
	file.Open(1,fpath);
	
	numOfPages = file.GetLength();
	
	si.myOrder = sortOrder;
	si.runLength = runLen;
	mode = 0;
	binarySearchPerformed = 0;
	querySetCalled = 0;
	querySet = 0;
}

int SortedFile::Close ()
{	
	if(numOfPages > 0)
	{
		if(currentPage == NULL)	
		{
			currentPage = new Page;
			offset = 0;
			file.GetPage(currentPage,offset++);
		}
		if(mode == 1)
		{
			inputPipe->ShutDown();
			Merge();
			delete inputPipe;
			delete output;
			delete bigq;
		}
		currentPage->EmptyItOut();
		delete currentPage;
		file.Close();
	}
	else
	{
		if(mode == 1)
			LoadFromPipe();
			
	}	
}

void SortedFile::Load (Schema &myschema, char *loadpath)
{
	FILE *fp = fopen(loadpath, "r");
	Record temp;

	if(mode == 0)
	{
		int buffsz = 100;
		inputPipe = new Pipe(buffsz);
		output = new Pipe(buffsz);
		bigq = new BigQ(*inputPipe,*output,*(si.myOrder),si.runLength);
		mode = 1;
	}
	
	int count = 0;
	while(temp.SuckNextRecord(&myschema,fp))
	{
		inputPipe->Insert(&temp);	
	}
	
	fclose(fp);


}

void SortedFile::MoveFirst ()
{
	if(mode == 1)
	{
		inputPipe->ShutDown();
		Merge();
		delete inputPipe;
		delete output;
		delete bigq;
	}
	else
	{
		if(currentPage != NULL)
			currentPage->EmptyItOut();
		else
		{
			currentPage = new Page;
			
		}
	}
	
	file.GetPage(currentPage,0);
	offset = 1;	
}

void SortedFile::Add (Record &addme)
{
	if(mode == 0)
	{
		int buffsz = 100;
		inputPipe = new Pipe(buffsz);
		output = new Pipe(buffsz);
		bigq = new BigQ(*inputPipe,*output,*(si.myOrder),si.runLength);
		inputPipe->Insert (&addme);
		mode = 1;
	}
	else
	{
		inputPipe->Insert(&addme);	
	}

}

int SortedFile::GetNext (Record &fetchme)
{
	if(mode == 1)
	{
		inputPipe->ShutDown();
		Merge();
		delete inputPipe;
		delete output;
		delete bigq;
		if(currentPage != NULL)
			currentPage->EmptyItOut();
		else
			currentPage = new Page;
		
		offset = 0;
		file.GetPage(currentPage,offset++);
	}
		
	off_t pageLength = file.GetLength();
	
	if(!currentPage->GetFirst(&fetchme))
	{
		off_t pageLength = file.GetLength();
		if(offset == (pageLength - 1))
		{
			return 0;
		}
		else
		{
			file.GetPage(currentPage,offset++);
			currentPage->GetFirst(&fetchme);
			countme++;
			return 1;
		}
			
	}
	else
	{
		countme++;
		return 1;
	}
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal,Schema *schema)
{
	ComparisonEngine ce;
	if(!querySetCalled)
	{
		querySet = setQueryOrderMaker(cnf);
	}
	if(querySet)
	{
		if(binarySearchPerformed == 0)
		{
			off_t max = numOfPages - 2;
			off_t min = offset -1;
			off_t current;
	
			int result;
			int lessThanFlag = 0;
			int equalsFound = 0;
			int count = 0;
			off_t lastOffset = -1;
			while(max >= min)
			{
				lessThanFlag = 0;
				equalsFound = 0;
				current = ceil(double(max + min)/2);		
				if(lastOffset != -1 && current == lastOffset)
					break;
				
				lastOffset = current;
							
				file.GetPage(currentPage,current);
				while(currentPage->GetFirst(&fetchme))
				{
					count++;
					
					result = ce.Compare(&literal, litQuery,&fetchme, query);
					if(result == 0)
					{
						equalsFound = 1;
						offset = current;
						offset++;
						binarySearchPerformed = 1;
						break;
					}
					else
					{
						if(result < 0)
						{
							lessThanFlag = 1;
							break;
						}
					}
				}
				
				if(equalsFound)
				{
					break;
				}
				else
				{
					if(lessThanFlag)
					{
						max = current -1;
					}
					else
					{
						min = current;
					}
				}	
			}
			binarySearchPerformed = 1;
		}
		else
		{
			if(!currentPage->GetFirst(&fetchme))
			{
				off_t pageLength = file.GetLength();
				if(offset == (pageLength - 1))
				{
					return 0;
				}
				else
				{
					file.GetPage(currentPage,offset++);
					currentPage->GetFirst(&fetchme);
				}
			}
		
		}
		while(true)
		{
			if(ce.Compare(&literal, litQuery,&fetchme, query) == 0)
			{
				if(ce.Compare(&fetchme,&literal,&cnf) == 1)
				{
					return 1;
				}
				if(!currentPage->GetFirst(&fetchme))
				{
					off_t pageLength = file.GetLength();
					if(offset == (pageLength - 1))
					{
						return 0;
					}
					else
					{
						file.GetPage(currentPage,offset++);
						currentPage->GetFirst(&fetchme);
					}
				}
			}
			else
			{
				return 0;
			}
		}
	}
	else
	{
		while(GetNext(fetchme))
		{
			if(ce.Compare(&fetchme,&literal,&cnf) == 1)
			{
				return 1;
			}
		}
		return 0;
	
	}
}

int SortedFile :: setQueryOrderMaker(CNF &cnf)
{
	querySetCalled = 1;
	int *cnfAttr;
	int *num;
	int *litAttr;
	
	cnf.getAttributes(&cnfAttr,&litAttr,&num);

	query = new OrderMaker;
	litQuery = new OrderMaker;
	
	int querySet =0,queryAttCount = 0,found = 0;
	for(int i = 0; i < numOfAtts; i++)
	{
		found = 0;
		for(int j = 0; j < (*num); j++)
		{
			if(attNum[i] == cnfAttr[j])
			{
				found = 1;
				querySet = 1;
				query->addAtt(queryAttCount,attNum[i]);
				query->addType(queryAttCount,attType[i]);
				litQuery->addAtt(queryAttCount,litAttr[j]);
				litQuery->addType(queryAttCount++,attType[i]);
				break;
			}
					
		}
		if(!found)
		{
			break;
		}
	}
	if(querySet)
	{
		query->setNumAtts(queryAttCount);
		litQuery->setNumAtts(queryAttCount);
	}
	
	return querySet;
}

void SortedFile :: LoadFromPipe()
{
	
	inputPipe->ShutDown();
	Page *outputPage = new Page;
	File tempFile;
	tempFile.Open(0,"OutTemp.txt");
	Page *page = new Page; 
	Record queueRec;
	off_t opFileOffset = 0;
	while(output->Remove(&queueRec))
	{
		if(!outputPage->Append(&queueRec))
		{
			tempFile.AddPage(outputPage,opFileOffset++);			
			outputPage->EmptyItOut();
			outputPage->Append(&queueRec);
		}
	}
	tempFile.AddPage(outputPage,opFileOffset++);			
	tempFile.Close();
	
	if(remove(fileName) != 0)
		cout<<"Error Deleting File\n";	
	
	//Renaming file
	if(rename("OutTemp.txt",fileName) == 1)
		cout<<"Error Renaming file\n";
		
	file.Open(1,fileName);
	currentPage = new Page;
	offset = 0;
	file.GetPage(currentPage,offset++);
}

void SortedFile::Merge()
{
	Page *page = new Page; 
	Page *outputPage = new Page;
	File tempFile;
	tempFile.Open(0,"OutTemp.txt");
	off_t ipFileOffset = 0,opFileOffset = 0;
	file.GetPage(page,ipFileOffset++);
	off_t pageLength = file.GetLength();
	int fileNonEmpty = 1,queueRecSelected = 1,fileRecSelected = 1;
	Record fileRec,queueRec;
	ComparisonEngine comparator;
	
	
	while(true)
	{
		if(queueRecSelected)
		{
			if(!output->Remove(&queueRec))
			{
				break;
			}
			queueRecSelected = 0;
		}
		if(fileRecSelected)
		{
			if(!page->GetFirst(&fileRec))
			{
				if((ipFileOffset + 1) == pageLength)
				{
					ipFileOffset++;
					fileNonEmpty = 0;
					break;
				}
				else
				{
					file.GetPage(page,ipFileOffset++);
					page->GetFirst(&fileRec);
				}
			}
			fileRecSelected = 0;
		}
		
		if(comparator.Compare(&queueRec,&fileRec,si.myOrder) > 0)
		{
			fileRecSelected = 1;
			if(!outputPage->Append(&fileRec))
			{
				tempFile.AddPage(outputPage,opFileOffset++);			
				outputPage->EmptyItOut();
				outputPage->Append(&fileRec);
			}
		}
		else
		{
			queueRecSelected = 1;
			if(!outputPage->Append(&queueRec))
			{
				tempFile.AddPage(outputPage,opFileOffset++);			
				outputPage->EmptyItOut();
				outputPage->Append(&queueRec);
			}
		}
	}	
	if(fileNonEmpty)
	{
		if(!outputPage->Append(&fileRec))
		{
			tempFile.AddPage(outputPage,opFileOffset++);			
			outputPage->EmptyItOut();
			outputPage->Append(&fileRec);
		}
		while(page->GetFirst(&fileRec))
		{
			if(!outputPage->Append(&fileRec))
			{
				tempFile.AddPage(outputPage,opFileOffset++);			
				outputPage->EmptyItOut();
				outputPage->Append(&fileRec);
			}
		}
		while(ipFileOffset < pageLength)
		{
			file.GetPage(currentPage,ipFileOffset++);
			while(page->GetFirst(&fileRec))
			{
				if(!outputPage->Append(&fileRec))
				{	
					tempFile.AddPage(outputPage,opFileOffset++);			
					outputPage->EmptyItOut();
					outputPage->Append(&fileRec);
				}
			}
		}
	
	}
	else
	{
		if(!outputPage->Append(&queueRec))
		{
			tempFile.AddPage(outputPage,opFileOffset++);			
			outputPage->EmptyItOut();
			outputPage->Append(&queueRec);
		}
		while(output->Remove(&queueRec))
		{
			if(!outputPage->Append(&queueRec))
			{
				tempFile.AddPage(outputPage,opFileOffset++);			
				outputPage->EmptyItOut();
				outputPage->Append(&queueRec);
			}
		}
	}
	
	tempFile.AddPage(outputPage,opFileOffset++);			
	tempFile.Close();
	
	if(remove(fileName) != 0)
		cout<<"Error Deleting File\n";	
	
	//Renaming file
	if(rename("OutTemp.txt",fileName) == 1)
		cout<<"Error Renaming file\n";
		
	file.Open(1,fileName);
	cout<<"File length : "<<file.GetLength()<<endl;
	
	
	
	
	
}
