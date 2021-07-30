#include <iostream>
#include <vector>
#include <fstream>
#include <chrono>
#include <thread>            
#include <mutex>              
#include <condition_variable>
#include <atomic>
#include "build_graph.hpp"
#include "myqueue.cpp"
using namespace std;
#include <queue>
//#define PRINT
#define PRINTMINIMAL

void emitterJob(Graph &graph, vector<myqueue<int>> &e2w, myqueue<int> &c2e, int nw, int sourceNode) { 

	int turn=0; //value in range [0, ,nw] used for Round-Robin sending
    int nodeID; // unique value identifying a node

    c2e.push(sourceNode); //initialization of the queue with the source node

    if(graph.adjList[sourceNode].size()>0){ // if false, the source node has no children, end computation
        while(true){ 
            nodeID= c2e.pop(); 
            if(nodeID==EOS) //end computation condition
                break;
            e2w[turn].push(nodeID);
            turn = (turn + 1) % nw; //Round-Robin        
        }
    }


        for(int i=0; i<nw; i++) //propagate the EOS
        e2w[i].push(EOS);
    
    #ifdef PRINT
        cout << "EMITTER: EOS EMITTED \n ";
    #endif

    return;

}



void workerJob(int thID, myqueue<int> &e2w, myqueue<int> &w2c, Graph &graph, vector <int> &visitedNodes, vector<mutex> &globallock) {

	int nodeID;
    long Tw;
    nodeID=e2w.pop(); 

    while(nodeID!=EOS){   
        for(int v: graph.adjList[nodeID]){ // explore all the children   
                globallock[v].lock(); // lock on the mutual exclusion control structure   
                if(visitedNodes[v]==0){
                    visitedNodes[v]=1; // set to visited
                    globallock[v].unlock();
                    w2c.push(v); // send child to collector
                    #ifdef PRINT
                        std::cout << "WORKER" <<thID << " PUSHED: " << v << "\n";
                    #endif
                }
                else{
                    globallock[v].unlock();
                    w2c.push(-2); //node already explored, send special value in order to decrease the collector counter
                    #ifdef PRINT
                        std::cout << "WORKER" <<thID << " PUSHED: " << -2 << "\n";
                    #endif
                }
                
        }
    nodeID=e2w.pop();
    }
    w2c.push(EOS); // propagate the EOS for the termination
    return;

}

void collectorJob(Graph &graph,vector<myqueue<int>> &w2c, myqueue<int> &c2e,  int nw, int sourceNode, int X) {

    int totalOccourences=0; //counter for the occurences of X
	int countEOS=0; //counter for the #EOS received
	int turn=0;
	long Tcoll;
    int nodeID;
    vector <int> receivedNodes; //frontier, the collector insert in it the node received from the workers
    int expectedOutput; // counter used in order to understand when a whole level is explored (all workers finished)

    expectedOutput=graph.adjList[sourceNode].size(); //compute the initial counter with respect to the children of the source node

    if(graph.nodeValues[sourceNode]==X) // check the occurence of X
        totalOccourences++;

    while(true){
        if(!w2c[turn].empty()){  //verify if a node produced or not an output          
            nodeID=w2c[turn].pop();
 
            turn = (turn+1) %nw;

            if(nodeID==EOS){
                countEOS++;
                if(countEOS==nw)
                    break;
                continue;
            }

            #ifdef PRINT
                std::cout<< "COLLECTOR RECEIVED " << nodeID << "\n";
            #endif
                
            expectedOutput--; // for each output received, decrease the counter

            if(nodeID!=-2){ //if received a child to be explored
                if(graph.nodeValues[nodeID]==X) //check for occurences of X
                    totalOccourences++;
                if(graph.adjList[nodeID].size()>0) //if it has children is inserted in the frontier
                    receivedNodes.push_back(nodeID);
            }
            if(expectedOutput==0){ // if this child was the last explored in the frontier

                if(receivedNodes.size()==0){ // if the frontier is empty, terminates
                    c2e.push(EOS);
                    break;
                }
                        

                for(int v: receivedNodes) // re-compute the expectedOutput
                    expectedOutput+= graph.adjList[v].size();

                if(expectedOutput==0){ // if all the nodes in the frontier have no children, terminate
                    c2e.push(EOS);
                    break;
                }
            
                for(int v: receivedNodes){ //send the frontier out to the Emitter
                    c2e.push(v);
                }
                
                receivedNodes.clear(); //empty the frontier
                
            }
        }
        else{
            turn = (turn+1) %nw;
            continue;
        }
    }
    #ifdef PRINTMINIMAL
        cout << "THE OCCOURENCES FOUND: " << totalOccourences << endl;
    #endif
    return;
}


long farm(int numNodesGraph, int nw, Graph &graph, int X, vector <int> &visitedNodes, int sourceNode, vector<mutex> &globallock) {
    
    vector<myqueue<int>> e2w (nw);   // queues between Emitter and Workers
    vector<myqueue<int>> w2c (nw);   // queues between Workers and Collector
    myqueue<int> c2e;   // queue between Collector and Emitter

	long Tc;
		
	{
		utimer tt("TIME COMPLETION: ", &Tc);
        thread emitter(emitterJob, ref(graph), ref(e2w), ref(c2e), nw, sourceNode);      // this is the emitter, source of the stream of tasks

		vector<thread*> tids(nw);
		for(int i=0; i<nw; i++)
            tids[i]= new thread(workerJob, i, ref(e2w[i]), ref(w2c[i]), ref(graph), ref(visitedNodes), ref(globallock));
        
        thread collector(collectorJob, ref(graph), ref(w2c), ref(c2e), nw, sourceNode, X);
	
		emitter.join(); 
		for(int i=0; i<nw; i++) 
			tids[i]->join();
		collector.join();
	}
	return Tc ;
}


void sequential_execution(Graph &graph, vector<int> visitedNodes, int X, int sourceNode ){
    queue<int> frontier;
    int Occourences=0; //counter for the occurences of X
    int nodeID=sourceNode;
    long Tf, Tg;

        if(sourceNode==X) //check occurence of X for the source node
            Occourences++;
    
        frontier.push(nodeID); // initialization of the frontier

        while(frontier.size()>0){
            nodeID=frontier.front();// retrieve elem from the beginning of the queue
            frontier.pop();
            for(int v: graph.adjList[nodeID] ){ //explore all the children
                if(visitedNodes[v]==0){
                    visitedNodes[v]=1; //set to visited
                    if(graph.nodeValues[v]==X)
                        Occourences++;
                    frontier.push(v);// push the node at the end of the queue
                }
                    
            }
    }
    #ifdef PRINTMINIMAL
        cout << "THE OCCOURENCES FOUND: " << Occourences << endl;
    #endif

}


int main(int argc, char * argv[]){
  
	int nw;
	int X; // the value on which count the occurences
	int numNodes;// total number of nodes in the graph
    int sourceNode;
    long Tc;
	if(argc == 1)  {
		nw=2;
		X = 0;
		numNodes=10;
        sourceNode=0;
  	}
	else {
		nw = (atoi(argv[1]));
		X = (atoi(argv[2]));
		numNodes= (atoi(argv[3]));
        sourceNode= (atoi(argv[4]));
    }

    if(X >= 20){
        cout << "THE X VALUE IS OUT OF RANGE, THE RANGE IS (0, , 19) " << endl;
        return -1;
    }

    if(sourceNode >= numNodes){
        cout << "THE SOURCE NODE IS OUT OF RANGE, THE RANGE IS (0, ,"<< numNodes-1 << ") "<< endl;
        return -1;
    }

	string nameFileGraph="graph_stored_one_"+to_string(numNodes)+".txt";
    string nameFileNodeValues="graph_stored_two_"+to_string(numNodes)+".txt";

    // graph initialization
    Graph graph(nameFileGraph, nameFileNodeValues, numNodes, X);

    #ifdef PRINTMINIMAL
        cout << "THE TOTAL OCCOURENCES OF " << X << " ARE:  " << graph.Occurences << endl;
    #endif

    //initialize binary vec
	std::vector <int> visitedNodes (graph.numNodes);

    for( int i=0; i< graph.numNodes ; i+=5){
        visitedNodes[i]=0; 
        visitedNodes[i+1]=0;
        visitedNodes[i+2]=0;
        visitedNodes[i+3]=0;
        visitedNodes[i+4]=0;
    }

    if(nw==0){
        {
            utimer tt("TIME COMPLETION: ", &Tc);
            sequential_execution(graph, visitedNodes, X, sourceNode );
        }
        /*
        ofstream outfile("exec_time_seq.txt", ios::app);
	    outfile  << " NumNodes: " << graph.numNodes << " TC: "<< Tc <<  endl;
        outfile  << "----------------------------------"<<endl;
	    outfile.close(); 
        cout << " NumNodes: " << graph.numNodes << " TC: "<< Tc <<  endl;
        */
        return 0;
    }


    //graph.printGraph();
    vector<mutex> globallock(graph.numNodes);


	Tc = farm(graph.numNodes, nw, graph, X, visitedNodes, sourceNode, globallock);

    /*
	ofstream outfile("exec_time_par.txt", ios::app);
	outfile  << " NumNodes: " << graph.numNodes << " nw: "<< nw << " TC: "<< Tc <<  endl;
    outfile  <<"----------------------------------"<<endl;
	outfile.close();
	*/
    return 0;

}