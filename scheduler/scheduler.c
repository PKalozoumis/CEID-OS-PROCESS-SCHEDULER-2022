/* header files */
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <ctype.h>
#include <time.h>
#include <signal.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>

/* global definitions */

#define PRIORITY_VALUES 16

/* definition and implementation of process descriptor and queue(s) */

typedef enum
{
	READY, RUNNING, STOPPED, EXITED
}
ProcessState;

typedef enum
{
	FCFS, SJF, RR, PRIO
}
SchedulingPolicy;

typedef struct
{
	char* name;
	pid_t pid;
	uint8_t priority;
	uint32_t estimatedTime;
	double burstTime;
	double completionTime;
	double schedulerTime;
	ProcessState state;
}
ProcessDescriptor;

typedef struct ListNode
{
	struct ListNode* prev;
	struct ListNode* next;
	ProcessDescriptor data;
}
ListNode;

typedef ListNode* List;

char* readLine(FILE* file);
ProcessDescriptor makeProcessDescriptor(char* str, SchedulingPolicy policy);
void printProcessDescriptor(ProcessDescriptor* pd);

ListNode* makeList(char* filename, SchedulingPolicy policy);
void push(ListNode** listRef, ProcessDescriptor* pd);
void insert(ListNode** listRef, ProcessDescriptor* pd);
ProcessDescriptor pop(ListNode** listRef);
void freeList(ListNode** listRef);
void printList(ListNode* list);
void concat(ListNode** list1, ListNode* list2);

List* makePriorityQueue(char* filename);
void freePriorityQueue(List** prQueue);
void printPriorityQueue(List* prQueue);

void fcfsPolicy(ListNode** listRef);
ListNode* priorityPolicy(List** queueRef, uint32_t quantum);
ListNode* rrPolicy(ListNode** listRef, uint32_t quantum);
void runProcessWithQuantum(ListNode** listRef, ProcessDescriptor* pd, uint32_t quantum);

char* stringToLower(char* str);
void printSchedulerInformation(FILE* target, ListNode* list, char* filename, SchedulingPolicy policy);
double getTime(void);

/* signal handler(s) */
void signalHandler(int sigid);

/* global variables and data structures */

double schedulerTime = 0;
int childFinished = 0;

int main(int argc,char **argv)
{
	/* local variables */

	SchedulingPolicy policy;
	uint32_t quantum = 0;

	/* parse input arguments (policy, quantum (if required), input filename */

	if (argc == 1)
	{
		printf("Insert a scheduling policy.\n");
		exit(EXIT_FAILURE);
	}
	else
	{
		if ((strcmp(stringToLower(argv[1]), "fcfs") == 0)||(strcmp(stringToLower(argv[1]), "batch") == 0))
		{
			if (argc == 2)
			{
				printf("Insert a filename.\n");
				exit(EXIT_FAILURE);
			}
			else if (argc > 3)
			{
				printf("FCFS policy only needs the filename\n");
				exit(EXIT_FAILURE);
			}
			else
			{
				policy = FCFS;
			}
		}
		else if (strcmp(stringToLower(argv[1]), "sjf") == 0)
		{
			if (argc == 2)
			{
				printf("Insert a filename.\n");
				exit(EXIT_FAILURE);
			}
			else if (argc > 3)
			{
				printf("SJF policy only needs the filename\n");
				exit(EXIT_FAILURE);
			}
			else
			{
				policy = SJF;
			}
		}
		else if (strcmp(stringToLower(argv[1]), "rr") == 0)
		{
			if (argc <= 3)
			{
				printf("RR policy needs a quantum and the filename\n");
				exit(EXIT_FAILURE);
			}
			else if (argc > 4)
			{
				printf("RR policy only needs a quantum and the filename\n");
				exit(EXIT_FAILURE);
			}
			else
			{
				quantum = atoi(argv[2]);
				policy = RR;
			}
		}
		else if (strcmp(stringToLower(argv[1]), "prio") == 0)
		{
			if (argc <= 3)
			{
				printf("PRIO policy needs a quantum and the filename\n");
				exit(EXIT_FAILURE);
			}
			else if (argc > 4)
			{
				printf("PRIO policy only needs a quantum and the filename\n");
				exit(EXIT_FAILURE);
			}
			else
			{
				quantum = atoi(argv[2]);
				policy = PRIO;
			}
		}
		else
		{
			printf("Not a valid policy!\n");
			exit(EXIT_FAILURE);
		}
	}

	FILE* output = fopen("scheduler_information.txt", "w");
	schedulerTime = getTime();

	switch (policy)
	{
		case FCFS:
		case SJF:
		{
			ListNode* list = makeList(argv[2], policy);
			fcfsPolicy(&list);
			printf("\n");

			printSchedulerInformation(stdout, list, argv[2], policy);
			printSchedulerInformation(output, list, argv[2], policy);

			freeList(&list);
		}
		break;

		case RR:
		{
			ListNode* list = makeList(argv[3], RR);
			ListNode* executedPrograms = rrPolicy(&list, quantum);

			printSchedulerInformation(stdout, executedPrograms, argv[3], RR);
			printSchedulerInformation(output, executedPrograms, argv[3], policy);

			freeList(&list);
			freeList(&executedPrograms);
		}
		break;

		case PRIO:
		{
			List* prQueue = makePriorityQueue(argv[3]);
			ListNode* executedPrograms = priorityPolicy(&prQueue, quantum);

			printSchedulerInformation(stdout, executedPrograms, argv[3], PRIO);
			printSchedulerInformation(output, executedPrograms, argv[3], policy);

			freePriorityQueue(&prQueue);
			freeList(&executedPrograms);
		}
		break;
	}

	fclose(output);

	printf("Scheduler exits\n");

	exit(EXIT_SUCCESS);
}

void push(ListNode** listRef, ProcessDescriptor* pd)
{
	ListNode* newNode = (ListNode*)malloc(sizeof(ListNode));
	newNode->data = *pd;
	pd->name = NULL;

	newNode->next = NULL;

	if ((*listRef) == NULL)
	{
		(*listRef) = newNode;
		newNode->prev = NULL;
	}
	else
	{
		ListNode* p = *listRef;

		while(p->next != NULL)
			p = p->next;

		p->next = newNode;
		newNode->prev = p;
	}
}

void insert(ListNode** listRef, ProcessDescriptor* pd)
{
	ListNode* newNode = (ListNode*)malloc(sizeof(ListNode));
	newNode->data = *pd;
	pd->name = NULL; //Transfer ownership

	if ((*listRef) == NULL)
	{
		(*listRef) = newNode;
		newNode->next = NULL;
		newNode->prev = NULL;
	}
	else
	{
		ListNode* p = *listRef;

		while((p->next != NULL)&&(pd->estimatedTime >= p->next->data.estimatedTime))
			p = p->next;

		if ((p->prev == NULL)&&(pd->estimatedTime < p->data.estimatedTime))
		{
			newNode->next = p;
			newNode->prev = NULL;
			p->prev = newNode;

			*listRef = newNode;
		}
		else
		{
			newNode->next = p->next;
			newNode->prev = p;
			p->next = newNode;

			if (newNode->next != NULL)
				newNode->next->prev = newNode;
		}
	}
}

ProcessDescriptor pop(ListNode** listRef)
{
	ProcessDescriptor pd = {.name = NULL};

	if (*listRef != NULL)
	{
		ListNode* rem = *listRef;
		pd = rem->data;

		*listRef = (*listRef)->next;

		if (*listRef != NULL)
			(*listRef)->prev = NULL;

		free(rem);
	}

	return pd;
}

void freeList(ListNode** listRef)
{
	while (*listRef != NULL)
		free(pop(listRef).name);
}

void printList(ListNode* list)
{
	while(list != NULL)
	{
		printProcessDescriptor(&(list->data));
		printf("\n");
		list = list->next;
	}
}

void concat(ListNode** list1, ListNode* list2)
{
	while(list2 != NULL)
	{
		push(list1, &(list2->data));
		list2 = list2->next;
	}
}

List* makePriorityQueue(char* filename)
{
    ListNode** prQueue = NULL;
	if ((prQueue = (ListNode**)malloc(PRIORITY_VALUES*sizeof(ListNode*))) == NULL)
	{
		perror("MALLOC ERROR");
		exit(EXIT_FAILURE);
	}

	for (int i = 0; i < PRIORITY_VALUES; ++i)
		prQueue[i] = NULL;

    FILE* file;

    if ((file = fopen(filename, "r")) == NULL)
    {
        perror("FILE ERROR");
		free(prQueue);
        exit(EXIT_FAILURE);
    }

	char* str;

    while ((str = readLine(file)) != NULL)
	{
		ProcessDescriptor pd = makeProcessDescriptor(str, PRIO);

		push(&(prQueue[pd.priority]), &pd);
		free(str);
	}

    fclose(file);

    return prQueue;
}

void freePriorityQueue(List** prQueueRef)
{
	for (int i = 0; i < PRIORITY_VALUES; ++i)
		freeList(&((*prQueueRef)[i]));

	free(*prQueueRef);
	*prQueueRef = NULL;
}

void printPriorityQueue(List* prQueue)
{
	for (int i = 0; i < PRIORITY_VALUES; ++i)
	{
		printf("PRIORITY: %d =================================================\n", i);
		printList(prQueue[i]);
	}
}

char* readLine(FILE* file)
{
	int bufferSize = 32; //All characters in a line + newline + null
	int strSize = bufferSize;
	int endOfLine = 0;

    char* str;
	char* tempBuffer;

	if ((str = (char*)malloc(strSize*sizeof(char))) == NULL)
	{
		perror("STR MALLOC ERROR");
		exit(EXIT_FAILURE);
	}

	if ((tempBuffer = (char*)malloc(bufferSize*sizeof(char))) == NULL)
	{
		perror("BUFFER MALLOC ERROR");
		free(str);
		exit(EXIT_FAILURE);
	}

	strcpy(str, "");

	while(!endOfLine)
	{
		if (fgets(tempBuffer, bufferSize, file) != NULL) //bufferSize - 1 must include all characters in a line + a newline
		{
			if (tempBuffer[strlen(tempBuffer)-1] == '\n')
			{
				tempBuffer[strlen(tempBuffer)-1] = '\0';
				endOfLine = 1;
			}
			else if (feof(file)) //Reached last line
			{
				endOfLine = 1;
			}
			else if (!feof(file)) //Buffer was not big enough for the line
			{
				strcat(str, tempBuffer);

				bufferSize *= 2;
				strSize += bufferSize;

				//printf("\nBuffer: %s\nString: %s\nReallocating... Buffer size is now: %d. String size is now: %d\n\n", tempBuffer, str, bufferSize, strSize);

				char* temp;

				if ((temp = (char*)realloc(str, strSize*sizeof(char))) == NULL)
				{
					perror("STR REALLOC ERROR");
					free(str);
					free(tempBuffer);
					exit(EXIT_FAILURE);
				}

				str = temp;

				if ((temp = (char*)realloc(tempBuffer, bufferSize*sizeof(char))) == NULL)
				{
					perror("BUFFER REALLOC ERROR");
					free(str);
					free(tempBuffer);
					exit(EXIT_FAILURE);
				}

				tempBuffer = temp;

			}
		}
		else
		{
			free(str);
			free(tempBuffer);

			if (ferror(file))
			{
				perror("FILE ERROR");
				exit(EXIT_FAILURE);
			}

			return NULL;
		}
	}

	strcat(str, tempBuffer);
	free(tempBuffer);

	str  = (char*)realloc(str, (strlen(str)+1)*sizeof(char*));

	//printf("%s SIZE: %d\n", str, strlen(str));

	return str;
}

ListNode* makeList(char* filename, SchedulingPolicy policy)
{
	if (policy == PRIO)
		return NULL;
	
    ListNode* list = NULL;
    FILE* file;

    if ((file = fopen(filename, "r")) == NULL)
    {
        perror("FILE ERROR");
        exit(EXIT_FAILURE);
    }

	char* str;

    while ((str = readLine(file)) != NULL)
	{
		ProcessDescriptor pd = makeProcessDescriptor(str, policy);

		if ((policy == RR)||(policy == FCFS))
			push(&list, &pd);
		else if (policy == SJF)
			insert(&list, &pd);

		free(str);
	}

    fclose(file);

    return list;
}

ProcessDescriptor makeProcessDescriptor(char* str, SchedulingPolicy policy)
{
	ProcessDescriptor pd;

	int field = 0;
	int secondFieldValue = 0;

	char* token = strtok(str, "\t");
    while(token)
    {
		if (field == 0)
		{
			if ((pd.name = (char*)malloc((strlen(token)+1)*sizeof(char))) == NULL)
			{
				perror("MALLOC ERROR");
				exit(EXIT_FAILURE);
			}

			strcpy(pd.name, token);
		}
		else if (field == 1)
		{
			secondFieldValue = atoi(token);
		}
		else if (field == 2) //If there are 3 fields, second one is priority, third is time
		{
			pd.priority = secondFieldValue;

			if (pd.priority > PRIORITY_VALUES - 1)
				pd.priority = PRIORITY_VALUES - 1;

			if (pd.priority < 0)
				pd.priority = 0;

			pd.estimatedTime = atoi(token);
		}

		field++;
        token = strtok(NULL, "\t");
    }

	if (field < 3) //If there are only 2 fields, second one depends on the policy
	{
		if (policy == PRIO)
		{
			pd.estimatedTime = 0;
			pd.priority = secondFieldValue;

			if (pd.priority > PRIORITY_VALUES - 1)
				pd.priority = PRIORITY_VALUES - 1;

			if (pd.priority < 0)
				pd.priority = 0;
		}
		else
		{
			pd.priority = 0;
			pd.estimatedTime = secondFieldValue;
		}
	}

	pd.pid = 0;
	pd.state = READY;
	pd.burstTime = 0;
	pd.completionTime = getTime();
	pd.schedulerTime = 0;

	return pd;
}

void printProcessDescriptor(ProcessDescriptor* pd)
{
	printf("name: %s\npid: %ld\npriority: %" PRIu8 "\nestimated_time: %" PRIu32 "\nburst_time: %lf\ncompletion_time: %lf\nscheduler_time: %lf\nstate: %d\n",
		pd->name, (long)pd->pid, pd->priority, pd->estimatedTime, pd->burstTime, pd->completionTime, pd->schedulerTime, pd->state);
}

char* stringToLower(char* str)
{
	for (char* p = str; *p; ++p)
		*p = tolower(*p);

	return str;
}

void fcfsPolicy(ListNode** listRef)
{	
	while(1)
	{
		ProcessDescriptor pd;
		pd = pop(listRef);

		pd.pid = fork();

		if (pd.pid > 0)
		{
			pd.burstTime = getTime();
			
			pd.state = RUNNING;

			waitpid(pd.pid, NULL, 0);
			pd.state = EXITED;

			double t = getTime();
			pd.burstTime = t - pd.burstTime;
			pd.completionTime = t - pd.completionTime;
			pd.schedulerTime = t - schedulerTime;

			push(listRef, &pd);

			if ((*listRef)->data.state == EXITED)
				break;
		}
		else if (pd.pid == 0)
		{
			execl(pd.name, pd.name, NULL);
		}
		else
		{
			free(pd.name);
			perror("FORK ERROR");
			exit(EXIT_FAILURE);
		}
	}
}

ListNode*  priorityPolicy(List** queueRef, uint32_t quantum)
{
	ListNode* executedPrograms = NULL;

	for (int i = 0; i < PRIORITY_VALUES; ++i)
	{
		ListNode* temp = rrPolicy(&((*queueRef)[i]), quantum);
		concat(&executedPrograms, temp);
	}

	return executedPrograms;
}

ListNode* rrPolicy(ListNode** listRef, uint32_t quantum)
{
	//Create signal handler
    //====================================================================
	struct sigaction sig;

	sig.sa_handler = signalHandler;
	sigemptyset(&sig.sa_mask);

	//SIGCHLD does not get sent when stopping child with SIGSTOP. Only when it exits.
	sig.sa_flags = SA_NOCLDSTOP;

	if (sigaction(SIGCHLD, &sig, NULL) < 0)
	{
		perror("SIGACTION ERROR");
		freeList(listRef);
		exit(EXIT_FAILURE);
	}

	//Scheduling
    //====================================================================
	ListNode* executedPrograms = NULL;

	while(1)
	{
		ProcessDescriptor pd = pop(listRef);

		if (pd.name != NULL)
		{
			if (pd.state == READY)
			{
				pd.pid = fork();

				if (pd.pid > 0)
				{
					runProcessWithQuantum(listRef, &pd, quantum);

					if (pd.state == EXITED)
						push(&executedPrograms, &pd);
				}
				else if (pd.pid == 0)
				{
					execl(pd.name, pd.name, NULL);
				}
				else if (pd.pid < 0)
				{
					perror("FORK ERROR");
					freeList(listRef);
					exit(EXIT_FAILURE);
				}
			}
			else if (pd.state == STOPPED)
			{
				kill(pd.pid, SIGCONT);
				runProcessWithQuantum(listRef, &pd, quantum);

				if (pd.state == EXITED)
					push(&executedPrograms, &pd);
			}
		}
		else break;
	}

	//Remove signal handler
    //====================================================================
	sig.sa_handler = SIG_DFL;
	sigemptyset(&sig.sa_mask);
	sig.sa_flags = 0;

	if (sigaction(SIGCHLD, &sig, NULL) < 0)
	{
		perror("SIGACTION ERROR");
		freeList(listRef);
		exit(EXIT_FAILURE);
	}

	return executedPrograms;
}

void signalHandler(int sigid)
{
	waitpid(-1, NULL, 0);
	childFinished = 1;
}

void runProcessWithQuantum(ListNode** listRef, ProcessDescriptor* pd, uint32_t quantum)
{
	pd->state = RUNNING;

	printf("Currently running: %s\n", pd->name);

	double t1 = getTime();

	struct timespec t = {.tv_sec = quantum/1000, .tv_nsec = (quantum%1000)*1000000};

	//nanosleep gets interrupted automatically as soon as it receives a signal (e.g. SIGCHLD)
	if (nanosleep(&t, NULL) < 0)
	{
		//Continue only if the scheduler was interrupted by SIGCHLD
		if (childFinished)
		{
			double t2 = getTime();
			printf("Ran %s for %lf\n\n", pd->name, t2-t1);
			pd->burstTime += t2-t1;
			pd->completionTime = t2 - pd->completionTime;
			pd->schedulerTime = t2 - schedulerTime;

			childFinished = 0;
			pd->state = EXITED;

			return;
		}
	}

	t1 = getTime()-t1;
	printf("Ran %s for %lf\n\n", pd->name, t1);
	pd->burstTime += t1;

	if (kill(pd->pid, SIGSTOP) < 0)
	{
		perror("KILL ERROR");
		exit(EXIT_FAILURE);
	}

	pd->state = STOPPED;
	push(listRef, pd);
}

void printSchedulerInformation(FILE* target, ListNode* list, char* filename, SchedulingPolicy policy)
{
	fprintf(target, "FILE: %s\nPOLICY: %s\n\n\n", filename, policy==FCFS?"FCFS":policy==SJF?"SJF":policy==RR?"RR":"PRIORITY");
	
	ListNode* s = list;
	double completionTime = 0;
	
	while (s != NULL)
	{
		completionTime = s->data.completionTime;
		fprintf(target, "PID %" PRIu8 " - CMD: %s\n--------------------------------\n", s->data.pid, s->data.name);
		fprintf(target, "Burst time: %.3lf secs\nCompletion time: %.3lf secs\nScheduler time: %.3lf secs\n\n", s->data.burstTime, s->data.completionTime, s->data.schedulerTime);
		s = s->next;
	}

	fprintf(target, "SCHEDULER TIME: %.3lf secs\nTOTAL PROCESS COMPLETION TIME: %.3lf secs\n\n", getTime() - schedulerTime, completionTime);
}

double getTime(void)
{
  struct timeval t;
  gettimeofday(&t, NULL);
  return (double)t.tv_sec + (double)t.tv_usec*1.0e-6;
}