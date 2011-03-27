#include "RelOp.h"
#include <pthread.h>
#include <cstdlib>
#include <string.h>
#include <vector>

using namespace std;

static char ch = 'a';

// SELECT FILE
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

// SELECT PIPE
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


// PROJECT
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


// SUM
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


// JOIN
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

	rightPipe->Remove(&rightCurrentRecord);

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

	while(true)
	{
		whileExecutionCount++;
		if(leftFlushed)
		{
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



// DUPLICATE REOMOVAL
void* DuplicateRemovalRun(void *parameter)
{
	struct DuplicateRemovalThreadParameter *param = (struct DuplicateRemovalThreadParameter *) parameter;
	Pipe *inputPipe = param->inputPipe;
	Pipe *outputPipe = param->outputPipe;
	Schema *mySchema = param->mySchema;
	int runLength = param->runLength;

	//Create an instance of a pipe which will behave as the intermediate output pipe
	Pipe *intermediatePipe = new Pipe(100);

	OrderMaker *sortOrder = new OrderMaker(mySchema);

	//How to obtain the runlength? Can we set a runLength? I don't see how that could matter.
	//use BigQ to load into intermediate ouput pipe
	BigQ *bigQ = new BigQ(*inputPipe, *intermediatePipe, *sortOrder, runLength);

	//pull out from intermediate output pipe and load distinct records into output pipe
	ComparisonEngine ce;
	Record fetched, fetched1, fetched2;
	intermediatePipe->Remove(&fetched1);
	fetched2.Copy(&fetched1);

	outputPipe->Insert(&fetched1);

	int count = 1;
	int count1= 0;
	while(intermediatePipe->Remove(&fetched))
	{
		count1++;
		//When the records are not equal, the fetched record is inserted into the output Pipe
		if((ce.Compare(&fetched, &fetched2, sortOrder)) != 0)
		{
			fetched2.Copy(&fetched);
			outputPipe->Insert(&fetched);
			count++;
		}
	}

	cout<<"Count = "<<count<<endl;
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


// WRITEOUT
void* woRun(void *parameter){
	cout<<"Entered writeout thread function"<<endl;
	struct WriteOutThreadParameter *param = (struct WriteOutThreadParameter *) parameter;
	Pipe *inputPipe=param->inputPipe;
	Schema *mySchema=param->mySchema;
	FILE *outFile=param->outFile;
	Record pipeRecord;

	while(inputPipe->Remove(&pipeRecord))
	{
		pipeRecord.Print(outFile,mySchema);
	}

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


// GROUPBY
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

	Schema *schema = param->schema;
	Pipe *intermediatePipe1 = new Pipe(100);

	cout<<"\ninside group by run\n";

	BigQ *bigQ = new BigQ(*inputPipe, *intermediatePipe1, *groupAtts, runLength);

	cout<<"\nordermaker = ";
	groupAtts->Print();

	//pull out from intermediate output pipe and load distinct records into output pipe
	ComparisonEngine ce;
	Record fetched, fetched1, fetched2;
	intermediatePipe1->Remove(&fetched1);
	fetched2.Copy(&fetched1);
	int count1= 1;

	int totalIntResult = 0;
	double totalDoubleResult = 0.0;
	int intResult = 0;
	double doubleResult = 0.0;
	Type returnType;
	//int count = 0;

	Pipe *projectInput = new Pipe(1);
	Pipe *projectOutput = new Pipe(1);

	char *attValue;
	Attribute *attributeArray;

	int flag = 0;
	while(intermediatePipe1->Remove(&fetched2))
	{
		//When the records are equal, the fetched record is inserted into the other intermediate Pipe
		if((ce.Compare(&fetched1, &fetched2, groupAtts)) == 0)
		{
			returnType = computeMe->Apply(fetched1,intResult,doubleResult);
			fetched1.Consume(&fetched2);
			totalIntResult += intResult;
			totalDoubleResult += doubleResult;

		}
		else
		{
			projectInput->Insert(&fetched1);

			//Number of group by attributes
			int numOfAtts = groupAtts->getNumAtts();
			//			cout<<"num of atts = "<<numOfAtts<<endl;

			//You don't really need this but we are getting the numbers of
			//group by attribute here
			int *attributes = groupAtts->getAtts();

			//Obtaining the number of atts in the table from "schema"
			int numAtts = schema->GetNumAtts();

			//Obtaining the attributes in the table from "schema"
			Attribute *getSchemaAtts = new Attribute[numAtts];
			getSchemaAtts = schema->GetAtts();

			Project project;
			projectInput->ShutDown();

			project.Run(*projectInput, *projectOutput, attributes, numAtts, numOfAtts);
			project.WaitUntilDone();

			Attribute IA = {"int", Int};
			Attribute SA = {"string", String};
			Attribute DA = {"double", Double};
			Attribute att3[] = {IA, SA, DA};
			Schema out_sch ("out_sch", numOfAtts, att3);

			Record projectOutRecord;
			while(projectOutput->Remove(&projectOutRecord))
			{}
			projectOutput->ShutDown();

			attValue = projectOutRecord.GetValue(&out_sch, 0);

			//Build the new Attribute to hold the grouping attributes
			attributeArray = new Attribute[numOfAtts+1];
			//Attribute *attri

			//Setting the First attribute to sum
			attributeArray[0].myType = returnType;
			attributeArray[0].name = "Sum";

			//match the attribute number and get the name of the attribute
			int k=1;
			for(int j=0; j<numOfAtts; j++)
			{
				for(int i=0; i<numAtts; i++)
				{
					if(i == attributes[j])
					{
						attributeArray[k].myType = getSchemaAtts[i].myType;
						attributeArray[k++].name= getSchemaAtts[i].name;
					}
				}
			}

			Schema sch1("GroupBySchema1",2,attributeArray);
			Record newRec;
			char *str1 = new char[20];
			sprintf(str1,"%f",doubleResult);
			int strLen1 = strlen(str1);
			str1[strLen1] = '|';
			str1[strLen1 + 1] = '\0';

			char *str2 = new char[20];
			sprintf(str2, "%s", attValue);
			int strLen2 = strlen(str2);
			str2[strLen2] = '|';
			str2[strLen2 + 1] = '\0';

			strcat(str1, str2);
			newRec.ComposeRecord(&sch1,str1);
			newRec.Print(&sch1);
			//fetched1.Print(schema);
			outputPipe->Insert(&newRec);

			fetched1.Consume(&fetched2);

			intResult = 0;
			doubleResult = 0.0;
			totalIntResult = 0;
			totalDoubleResult = 0.0;
		}
	}

	projectInput->Insert(&fetched1);

	//Number of group by attributes
	int numOfAtts = groupAtts->getNumAtts();
	//			cout<<"num of atts = "<<numOfAtts<<endl;

	//You don't really need this but we are getting the numbers of
	//group by attribute here
	int *attributes = groupAtts->getAtts();

	//Obtaining the number of atts in the table from "schema"
	int numAtts = schema->GetNumAtts();

	//Obtaining the attributes in the table from "schema"
	Attribute *getSchemaAtts = new Attribute[numAtts];
	getSchemaAtts = schema->GetAtts();

	Project project;
	projectInput->ShutDown();

	project.Run(*projectInput, *projectOutput, attributes, numAtts, numOfAtts);
	project.WaitUntilDone();

	Attribute IA = {"int", Int};
	Attribute SA = {"string", String};
	Attribute DA = {"double", Double};
	Attribute att3[] = {IA, SA, DA};
	Schema out_sch ("out_sch", numOfAtts, att3);

	Record projectOutRecord;
	while(projectOutput->Remove(&projectOutRecord))
	{}
	projectOutput->ShutDown();

	attValue = projectOutRecord.GetValue(&out_sch, 0);

	//Build the new Attribute to hold the grouping attributes
	attributeArray = new Attribute[numOfAtts+1];
	//Attribute *attri

	//Setting the First attribute to sum
	attributeArray[0].myType = returnType;
	attributeArray[0].name = "Sum";

	//match the attribute number and get the name of the attribute
	int k=1;
	for(int j=0; j<numOfAtts; j++)
	{
		for(int i=0; i<numAtts; i++)
		{
			if(i == attributes[j])
			{
				attributeArray[k].myType = getSchemaAtts[i].myType;
				attributeArray[k++].name= getSchemaAtts[i].name;
			}
		}
	}

	Schema sch1("GroupBySchema1",2,attributeArray);
	Record newRec;
	char *str1 = new char[20];
	sprintf(str1,"%f",doubleResult);
	int strLen1 = strlen(str1);
	str1[strLen1] = '|';
	str1[strLen1 + 1] = '\0';

	char *str2 = new char[20];
	sprintf(str2, "%s", attValue);
	int strLen2 = strlen(str2);
	str2[strLen2] = '|';
	str2[strLen2 + 1] = '\0';

	strcat(str1, str2);
	newRec.ComposeRecord(&sch1,str1);
	newRec.Print(&sch1);
	//fetched1.Print(schema);
	outputPipe->Insert(&newRec);

	fetched1.Consume(&fetched2);


	intResult = 0;
	doubleResult = 0.0;
	totalIntResult = 0;
	totalDoubleResult = 0.0;

	outputPipe->ShutDown();
}

void GroupBy:: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, Schema &schema)
{
	GroupByThreadParameter *param = new GroupByThreadParameter;

	param->inputPipe = &inPipe;
	param->outputPipe = &outPipe;
	param->groupAtts = &groupAtts;
	param->computeMe = &computeMe;
	param->runLength = runLength;

	param->schema = &schema;

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

