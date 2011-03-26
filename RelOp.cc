#include "RelOp.h"
#include <pthread.h>
#include <cstdlib>
#include <string.h>
#include <vector>

using namespace std;

//Select File

void* sfRun (void *parameter)
{

	//created parameter object
	struct sfThreadParameter *param = (struct sfThreadParameter *) parameter;
	
	Record *literal = param->litRecord;
	DBFile *inputFile = param->dbFile;
	Pipe *outputPipe = param->outputPipe;
	Record pipeRecord;
	
	ComparisonEngine ce;
	
	inputFile->MoveFirst();
	
	int count = 0;
	//while input pipe is not empty
	
	while(inputFile->GetNext(pipeRecord))
	{
		if(ce.Compare(&pipeRecord, literal, param->selOperator))
		{
			//Insert into output Pipe
			//pipeRecord.Print(param->schema);

			outputPipe->Insert(&pipeRecord);
			count++;
		}
	}	
	
	//cout<<"Added "<<count<<" Records into the Pipe : "<<endl;
	//Shuting down output pipe after writing to it
	param->outputPipe->ShutDown ();

}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) 
{
	//create a structure to hold all the variables
	
	sfThreadParameter *param = new sfThreadParameter;
	
	param->dbFile = &inFile;
	param->outputPipe = &outPipe;
	param->selOperator = &selOp;
	param->litRecord = &literal;
//	param->schema = schema;
	
	//call the worker function for the thread
	if(pthread_create(&thread, NULL,&sfRun, (void *)param))
	{
		//Exception for Pthread: exit in case of error	
		cout<<"\nError: Select File Thread couldn't be created\n";		
		exit(-1);
	}
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}

//Select Pipe

void* spRun(void *parameter)
{
	//created parameter object
	struct spThreadParameter *param = (struct spThreadParameter *) parameter;
	
	Record *literal = param->litRecord;
	Pipe *inputPipe = param->inputPipe;
	Pipe *outputPipe = param->outputPipe;
	Record pipeRecord;
	
	ComparisonEngine ce;
	
	//while input pipe is not empty
	while(inputPipe->Remove(&pipeRecord))
	{
		if(ce.Compare(&pipeRecord, literal, param->selOperator))
		{
			//Insert into output Pipe
			outputPipe->Insert(&pipeRecord);
		}
	}
	
//	param->inputPipe->ShutDown ();
	param->outputPipe->ShutDown ();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) 
{
    
	//create a structure to hold all the variables
	spThreadParameter *param = new spThreadParameter;
	
	param->inputPipe = &inPipe;
	param->outputPipe = &outPipe;
	param->selOperator = &selOp;
	param->litRecord = &literal;
	
	//call the worker function for the thread
	if(pthread_create(&thread, NULL,&spRun, (void *)param))
	{
		//Exception for Pthread: exit in case of error	
		cout<<"\nError: Select Pipe Thread couldn't be created\n";
		
		exit(-1);
	}
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

}

void* ProjectRun(void *parameter)
{
	//created parameter object
	struct ProjectThreadParameter *param = (struct ProjectThreadParameter *) parameter;
	
	Pipe *inputPipe = param->inputPipe;
	Pipe *outputPipe = param->outputPipe;
	Record rec;
	
	while(inputPipe->Remove(&rec))
	{
		rec.Project(param->keepMe,param->numAttsOutput,param->numAttsInput);
		outputPipe->Insert(&rec);
	}
	
	outputPipe->ShutDown();
}

void Project:: Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) 
{ 
	ProjectThreadParameter *pt = new ProjectThreadParameter;
	pt->inputPipe = &inPipe;
	pt->outputPipe = &outPipe;
	pt->keepMe = keepMe;
	pt->numAttsInput = numAttsInput;
	pt->numAttsOutput = numAttsOutput;
	
	if(pthread_create(&thread, NULL,&ProjectRun, (void *)pt))
	{
		//Exception for Pthread: exit in case of error	
		cout<<"\nError: Project Thread couldn't be created\n";
		
		exit(-1);
	}

}


void Project::WaitUntilDone () 
{ 
	pthread_join (thread, NULL);
}
	
void Project::Use_n_Pages (int n) 
{ 


}


void* SumRun(void *parameter)
{
		struct SumThreadParameter *param = (struct SumThreadParameter *) parameter;

		Pipe *inputPipe = param->inputPipe;
		Pipe *outputPipe = param->outputPipe;
		Function *func = param->func;
		Record rec;
		int totalIntResult = 0;
		double totalDoubleResult = 0.0;
		int intResult = 0;
		double doubleResult = 0.0;
		Type returnType;
		int count = 0;
		while(inputPipe->Remove(&rec))
		{
			count++;
			intResult = 0;
			doubleResult = 0.0;
			returnType = func->Apply(rec,intResult,doubleResult);
			totalIntResult += intResult;
			totalDoubleResult += doubleResult;
		}
		cout<<"Double Result :"<<totalDoubleResult<<endl;
		Attribute *attr = new Attribute;
		attr->myType = returnType;
		attr->name = "Sum";
		
		Attribute *atts = new Attribute[1];
		atts[0] = *attr;
		
		Schema sch("AggregateSchema",1,atts);
		Record sumRec;
		char *str = new char[20];
		sprintf(str,"%f",doubleResult);
		int strLen = strlen(str);
		str[strLen] = '|';
		str[strLen + 1] = '\0';
		sumRec.ComposeRecord(&sch,str);
		outputPipe->Insert(&sumRec);
		outputPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{

	SumThreadParameter *pt = new SumThreadParameter;
	pt->inputPipe = &inPipe;
	pt->outputPipe = &outPipe;
	pt->func = &computeMe;

	if(pthread_create(&thread, NULL,&SumRun, (void *)pt))
	{
		//Exception for Pthread: exit in case of error	
		cout<<"\nError: Sum Thread couldn't be created\n";
		
		exit(-1);
	}

}
void Sum::WaitUntilDone ()
{
	pthread_join (thread, NULL);
}
void Sum::Use_n_Pages (int n) 
{

}


void* JoinRun(void *parameter)
{
	cout<<"Join started working"<<endl;
	struct JoinThreadParameter *param = (struct JoinThreadParameter *) parameter;

	int buffsize = 100;
	Pipe *leftPipe = new Pipe(buffsize);
	Pipe *rightPipe = new Pipe(buffsize);
	OrderMaker leftOrder,rightOrder;
	int count = 0;
	
	if(param->cnfExpression->GetSortOrders(leftOrder,rightOrder) > 0)
	{	
		leftOrder.Print();
		rightOrder.Print();
		BigQ leftQ(*(param->leftPipe),*leftPipe,leftOrder,param->pageUsed);
		BigQ rightQ(*(param->rightPipe),*rightPipe,rightOrder,param->pageUsed);
	}
	cout<<"control after call to BigQ"<<endl;
	int printCount = 0;
	ComparisonEngine ce;
	Record leftCurrentRecord;
	Record leftNextRecord;
	Record rightCurrentRecord;
	Record rightNextRecord;
	Record *rec;
	vector<Record *> leftBuffer;
	vector<Record *> rightBuffer;
	int leftPipeExhausted = 0;
	int rightPipeExhausted = 0;
	leftPipe->Remove(&leftCurrentRecord);
	//leftCurrentRecord.Print(param->lschema);
	rightPipe->Remove(&rightCurrentRecord);
	//rightCurrentRecord.Print(param->rschema);
	int numAttsLeft = leftCurrentRecord.getAttNum();
	int numAttsRight = rightCurrentRecord.getAttNum();
	
	int *attsToKeep = new int[numAttsLeft + numAttsRight];
	
	for(int i = 0; i < numAttsLeft; i++)
	{
			attsToKeep[i] = i;
	}
	
	for(int i = 0; i < numAttsRight; i++)
	{
		attsToKeep[numAttsLeft + i] = i;
	}

	
	int counter = 0;
	int rightFlushed = 1,leftFlushed = 1;
	int result;
	int whileExecutionCount = 0;
	cout<<"Control before While loop"<<endl;
	while(true)
	{

		whileExecutionCount++;
		if(leftFlushed)
		{

			/*if(whileExecutionCount == 103 || whileExecutionCount == 102 || whileExecutionCount == 101)
			{
				cout<<"While Execution Count : "<<whileExecutionCount<<endl;
				rightCurrentRecord.Print(param->rschema);
				cout<<endl;
				leftCurrentRecord.Print(param->lschema);
			}*/

			leftFlushed = 0;
			while(true)
			{
				
				rec = new Record;
				rec->Consume(&leftCurrentRecord);
				leftBuffer.push_back(rec);
				
				if(leftPipe->Remove(&leftNextRecord))
				{
					//leftNextRecord.Print(param->lschema);
					if(ce.Compare(rec,&leftOrder,&leftNextRecord,&leftOrder) ==0)
					{
						leftCurrentRecord.Consume(&leftNextRecord);
					}
					else
					{
						break;
					}
				}
				else
				{
					leftPipeExhausted = 1;
					break;
				}
			}
		}
		if(rightFlushed)
		{
			rightFlushed = 0;
			while(true)
			{
				rec = new Record;
				rec->Consume(&rightCurrentRecord);

				rightBuffer.push_back(rec);
				if(rightPipe->Remove(&rightNextRecord))
				{
					//rightNextRecord.Print(param->rschema);
					if(ce.Compare(rec,&rightOrder,&rightNextRecord,&rightOrder) ==0)
					{
						rightCurrentRecord.Consume(&rightNextRecord);
					}
					else
					{
						break;
					}
				}
				else
				{
					rightPipeExhausted = 1;
					break;
				}
			}
		}
		result = ce.Compare(leftBuffer[0],&leftOrder,rightBuffer[0],&rightOrder);
		Record *mergeRecord;
		if(result == 0)
		{
			rightFlushed = 1;
			leftFlushed = 1;
			for(int i = 0; i < leftBuffer.size();i++)
			{

				for(int j = 0; j < rightBuffer.size(); j++)
				{
					mergeRecord = new Record;
					mergeRecord->MergeRecords(leftBuffer[i],rightBuffer[j],numAttsLeft,numAttsRight,attsToKeep,(numAttsLeft		
					+numAttsRight),numAttsLeft);
					param->outputPipe->Insert(mergeRecord);
					count++;
				}
			}
			for(int i =0; i < rightBuffer.size(); i++)
			{
				delete rightBuffer[i];
			}
			for(int i =0; i < leftBuffer.size(); i++)
			{
				delete leftBuffer[i];
			}
			leftBuffer.clear();
			rightBuffer.clear();
		}
		else if(result > 0)
		{
			/*if(printCount < 3)
			{
				cout<<"While Execution Count : "<<whileExecutionCount<<endl;
				rightBuffer[0]->Print(param->rschema);
				cout<<endl;
				leftBuffer[0]->Print(param->lschema);
				printCount++;
			}*/
			rightFlushed = 1;
			for(int i =0; i < rightBuffer.size(); i++)
			{
				delete rightBuffer[i];
			}
			rightBuffer.clear();
		}
		else
		{
			leftFlushed = 1;
			//cout<<"Flushing Right"<<endl;
			for(int i =0; i < leftBuffer.size(); i++)
			{
				delete leftBuffer[i];
			}
			leftBuffer.clear();
		}
		if(rightPipeExhausted)
		{
			for(int i =0; i < leftBuffer.size(); i++)
			{
				delete leftBuffer[i];
			}

			leftBuffer.clear();
			break;
		}
		else
		{
			if(rightFlushed)
				rightCurrentRecord.Consume(&rightNextRecord);
		}

		if(leftPipeExhausted)
		{
			for(int i =0; i < rightBuffer.size(); i++)
			{
				delete rightBuffer[i];
			}

			rightBuffer.clear();
			break;
		}
		else
		{
			if(leftFlushed)
				leftCurrentRecord.Consume(&leftNextRecord);
		}

	}
	cout<<"Records Inserted : "<<count<<endl;
	param->outputPipe->ShutDown();
}
	

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	JoinThreadParameter *pt = new JoinThreadParameter;
	pt->leftPipe = &inPipeL;
	pt->rightPipe = &inPipeR;
	pt->outputPipe = &outPipe;
	pt->cnfExpression = &selOp;
	pt->litRecord = &literal;
	pt->pageUsed = pageUsed;

	if(pthread_create(&thread, NULL,&JoinRun, (void *)pt))
	{
		//Exception for Pthread: exit in case of error	
		cout<<"\nError: Join Thread couldn't be created\n";
		
		exit(-1);
	}
}	
	
void Join::WaitUntilDone ()
{
	pthread_join (thread, NULL);
}
	
void Join::Use_n_Pages (int n)
{
	pageUsed = n;
}

Join::Join()
{
	pageUsed = 8;
}
//Duplicate Removal
void* DuplicateRemovalRun(void *parameter)
		{
	struct DuplicateRemovalThreadParameter *param = (struct DuplicateRemovalThreadParameter *) parameter;
	Pipe *inputPipe = param->inputPipe;
	Pipe *outputPipe = param->outputPipe;
	Schema *mySchema = param->mySchema;
	int runLength = param->runLength;
	//Create an instance of a pipe which will behave as the intermediate output pipe
	Pipe *intermediatePipe = new Pipe(100);
	//Construct the Ordermaker
	//Can we use the overridden constructor of ordermaker in comparison
	//to obtain the ordermaker object?
	OrderMaker *sortOrder = new OrderMaker(mySchema);
	//How to obtain the runlength? Can we set a runLength? I don't see how that could matter.
	//use BigQ to load into intermediate ouput pipe
	BigQ *bigQ = new BigQ(*inputPipe, *intermediatePipe, *sortOrder, runLength);
	//pull out from intermediate output pipe and load distinct records into output pipe
	ComparisonEngine ce;
	Record fetched, fetched1, fetched2;
	intermediatePipe->Remove(&fetched1);
	fetched2.Copy(&fetched1);
	// fetched1.Print(mySchema);
	outputPipe->Insert(&fetched1);
	//cout<<"Inserted\n";
	 int count = 1;
	 int count1= 0;
	while(intermediatePipe->Remove(&fetched))
	{
		 count1++;
		//cout<<"Inside While\n";
		//fetched.Print(mySchema);
		//cout<<count++<<endl;
		//When the records are not equal, the fetched record is inserted into the output Pipe
		if((ce.Compare(&fetched, &fetched2, sortOrder)) != 0)
		{
			//cout<<"Inside if\n";
			//fetched.Print(mySchema);
			fetched2.Copy(&fetched);
			//fetched.Print(mySchema);
			//fetched.Print(mySchema);
			outputPipe->Insert(&fetched);
			count++;
		}
	}
	/*ComparisonEngine ce;
	Record currentRecord,nextRecord;
	int count=0;
	Record *rec;
	intermediatePipe->Remove(&nextRecord);
	int pipeExhausted = 0;
	while(true)
	{
		currentRecord.Consume(&nextRecord);
		while(true)
		{
			if(intermediatePipe->Remove(&nextRecord))
			{
				if((ce.Compare(&currentRecord, &nextRecord, sortOrder)) != 0)
				{
					break;
				}
			}
			else
			{
				pipeExhausted = 1;
			}
		}
		//rec =new Record;
		Record Temp;
		Temp.Consume(&currentRecord);
		outputPipe->Insert(&Temp);
		count++;
		if(pipeExhausted)
			break;
	}*/
	/*while(intermediatePipe->Remove(&fetch1)){
		outputPipe->Insert(&fetch1);
		count++;
		while(intermediatePipe->Remove(&fetch2)){
			if((ce.Compare(&fetch1, &fetch2, sortOrder)) != 0)
					{



						outputPipe->Insert(&fetch2);
						count=count++;
						break;
					}
		}
	}*/
	cout<<"Count = "<<count<<endl;
	//cout<<"Count1 = "<<count1<<endl;
	cout<<"Out of Duplicate Removal"<<endl;
	outputPipe->ShutDown();

		}

DuplicateRemoval:: DuplicateRemoval()
{
runLength = 8;
}



void DuplicateRemoval:: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	DuplicateRemovalThreadParameter *param = new DuplicateRemovalThreadParameter;
	param->inputPipe = &inPipe;
	param->outputPipe = &outPipe;
	param->mySchema = &mySchema;
	param->runLength = runLength;
	if(pthread_create(&thread, NULL,&DuplicateRemovalRun, (void *)param))
	{
		//Exception for Pthread: exit in case of error
		cout<<"\nError: Thread couldn't be created\n";
		exit(-1);
	}
}

void DuplicateRemoval::WaitUntilDone ()
{
	cout<<"Control inside Wait Untill of Project :"<<endl;
	pthread_join (thread, NULL);
}
void DuplicateRemoval::Use_n_Pages (int n)
{
	runLength = n;
}




//writeout
void* woRun(void *parameter){
	cout<<"Entered writeout thread function"<<endl;
	struct WriteOutThreadParameter *param = (struct WriteOutThreadParameter *) parameter;
	Pipe *inputPipe=param->inputPipe;
	Schema *mySchema=param->mySchema;
	FILE *outFile=param->outFile;
	Record pipeRecord;
	//cout<<"Outside While loop of woRun"<<endl;

	while(inputPipe->Remove(&pipeRecord))
	{  // cout<<"Inside While loop of woRun"<<endl;


	//pipeRecord.Print(mySchema);
    pipeRecord.Print(outFile,mySchema);

	//cout<<"Printed Record to File"<<endl;

	}

   // inputPipe->ShutDown();
	fclose(outFile);
	cout<<"Exiting writeout thread function"<<endl;
}
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	cout<<"Entered Writeout run method"<<endl;
	WriteOutThreadParameter *param = new WriteOutThreadParameter;
	param->inputPipe = &inPipe;
	param->outFile = outFile;
	param->mySchema = &mySchema;

	if(pthread_create(&thread, NULL,&woRun, (void *)param))
	{
		//Exception for Pthread: exit in case of error
		cout<<"\nError: Thread couldn't be created\n";
		exit(-1);
	}
}
void WriteOut::Use_n_Pages(int n){

}
void WriteOut::WaitUntilDone ()
{
	pthread_join (thread, NULL);
}



// GROUP BY
GroupBy::GroupBy()
{
    runLength = 10;
}

void GroupBy:: Use_n_Pages (int n)
{
    runLength = n;
}

void* GroupByRun(void *parameter)
{
    struct GroupByThreadParameter *param = (struct GroupByThreadParameter *) parameter;
    Pipe *inputPipe = param->inputPipe;
    Pipe *outputPipe = param->outputPipe;
    OrderMaker *groupAtts = param->groupAtts;
    Function *computeMe = param ->computeMe;
	int runLength = param->runLength;
   
	Pipe *intermediatePipe1 = new Pipe(100);
	Pipe *intermediatePipe2 = new Pipe(100);
   
	cout<<"\ninside group by run\n";

	BigQ *bigQ = new BigQ(*inputPipe, *intermediatePipe1, *groupAtts, runLength);
	
	cout<<"\nafter bigQ call\n";
	//pull out from intermediate output pipe and load distinct records into output pipe
	ComparisonEngine ce;
	Record fetched, fetched1, fetched2;
	intermediatePipe1->Remove(&fetched1);
	fetched2.Copy(&fetched1);
	// fetched1.Print(mySchema);
	intermediatePipe2->Insert(&fetched1);
	//cout<<"Inserted\n";
	int count = 1;
	int count1= 0;
	while(intermediatePipe1->Remove(&fetched))
	{
		count1++;
		//When the records are equal, the fetched record is inserted into the other intermediate Pipe
		if((ce.Compare(&fetched, &fetched2, groupAtts)) == 0)
		{
			//cout<<"Inside if\n";
			intermediatePipe2->Insert(&fetched);
			//fetched.Print(mySchema);
			//fetched.Print(mySchema);
			//outputPipe->Insert(&fetched);
			count++;
		}
		else
		{
			cout<<"\ncount = "<<count<<endl;  
			count = 1;
			fetched2.Copy(&fetched);
			//call sum on the intermediate pipe 2 for the set of sorted records
//			Sum *sumObject = new Sum();
//			sumObject->Run(*intermediatePipe2, *outputPipe, *computeMe);
		}
	}
}

void GroupBy:: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
    GroupByThreadParameter *param = new GroupByThreadParameter;
   
    param->inputPipe = &inPipe;
    param->outputPipe = &outPipe;
    param->groupAtts = &groupAtts;
    param->computeMe = &computeMe;
	param->runLength = runLength;
   
    if(pthread_create(&thread, NULL,&GroupByRun, (void *)param))
    {
        //Exception for Pthread: exit in case of error
        cout<<"\nError: GroupBy Thread couldn't be created\n";
        exit(-1);
    }
}


void GroupBy::WaitUntilDone ()
{
    cout<<"Control inside Wait Untill of Project :"<<endl;
    pthread_join (thread, NULL);
}

