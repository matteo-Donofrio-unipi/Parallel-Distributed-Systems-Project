#include <iostream>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <iostream>
#include <vector>
#include <fstream>
#include <chrono>
#include <thread>            
#include <mutex>              
#include <condition_variable>
#include <atomic>
#include "build_graph.hpp"
using namespace ff;
//#define PRINT
#define PRINTMINIMAL

struct emitter: ff_monode_t<int> {
    emitter(Graph &graph, int sourceNode): graph(graph), sourceNode(sourceNode) {}
    int* svc(int *task) {
        if (task == nullptr) { //init code
            if(graph.adjList[sourceNode].size()==0) // if the source node has no children, end computation
                return EOS;

            ff_send_out(new int(sourceNode)); // start computation sending the source node
            #ifdef PRINT
            std::cout<< "EMITTER PUSH: " << 0 << "\n";
            #endif
            return GO_ON;
        }    

        int &t = *task;
        if(t == -3){ // value that the Collector sends to the Emitter in order to notify an end of stream
            #ifdef PRINT
                std::cout<< "EMITTER SENT EOS: \n";
            #endif
            return EOS; //propagate the EOS
        }
        #ifdef PRINT
            std::cout<< "EMITTER PUSH: " << t << "\n";
        #endif
        
        return task; //normal case, push the node to be explored to the emitter
    }
    int sourceNode;
    Graph graph;

};

struct worker: ff_node_t<int> {
    worker(Graph &graph, vector <int> &visitedNodes, vector<mutex> &globallock):graph(graph), visitedNodes(visitedNodes), globallock(globallock) {}
    int* svc(int * task) { 
        int &nodeID = *task;
        #ifdef PRINT
            std::cout << "WORKER" << get_my_id() << " RECEIVED: " << nodeID << "\n";
        #endif         
            for(int v: graph.adjList[nodeID]){ //explore all the node children
                globallock[v].lock();
                if(visitedNodes[v]==0){
                    visitedNodes[v]=1; // set to visited
                    globallock[v].unlock();
                    ff_send_out(new int(v)); //send the child to the Collector
                    #ifdef PRINT
                        std::cout << "WORKER" << get_my_id() << " PUSHED: " << v << "\n";
                    #endif
                }
                else{
                    globallock[v].unlock();
                    ff_send_out(new int(-2)); //node already explored, send special value in order to decrease the collector counter
                    #ifdef PRINT
                        std::cout << "WORKER" << get_my_id() << " PUSHED: " << -2 << "\n";
                    #endif
                }
                
            }
            delete task;         
        return GO_ON; 
    }
    Graph graph;
    std::vector <int> &visitedNodes;
    std::vector<mutex> &globallock;
};

struct collector: ff_minode_t<int> {
    collector(Graph &graph, int expectedOutput, int X, int sourceNode):graph(graph), expectedOutput(expectedOutput), X(X), sourceNode(sourceNode) {}
    // in the constructor is initialized the expectedOutput
    int svc_init() { 
        if(graph.nodeValues[sourceNode]==X)  // check the occurence of X
            totalOccourences++;
        return 0;
    }

    int* svc(int * task) { 
        int &nodeID = *task;
        #ifdef PRINT
            std::cout<< "COLLECTOR RECEIVED " << nodeID << "\n";
        #endif
        
        expectedOutput--; // for each output received, decrease the counter
        if(nodeID!=-2){ //if received a child to be explored
            if(graph.nodeValues[nodeID]==X)
                totalOccourences++;
                if(graph.adjList[nodeID].size()>0) //if it has children is inserted in the frontier
                    receivedNodes.push_back(nodeID);
        }
        if(expectedOutput==0){ // if this child was the last explored in the frontier
           
            if(receivedNodes.size()==0){ // if the frontier is empty, terminates
                ff_send_out(new int(-3));
                return GO_ON;
            }
                

            for(int v: receivedNodes)  // re compute the expectedOutput
                expectedOutput+= graph.adjList[v].size();

            if(expectedOutput==0){ // if all the nodes in the frontier have no children, terminate
                ff_send_out(new int(-3));
                return GO_ON;
            }
      
            for(int v: receivedNodes) //send the frontier out to the Emitter
                ff_send_out(new int(v));
                

            receivedNodes.clear(); //empty the frontier
        }

        return GO_ON;

    }


    void svc_end() { 
        #ifdef PRINTMINIMAL
        std::cout << "THE OCCOURENCES FOUND: " << totalOccourences << "\n";
        #endif
    }

    Graph graph;
    int totalOccourences=0; //counter for the occurences of X
    int expectedOutput; // counter used in order to understand when a whole level is explored (all workers finished)
    int X;
    int sourceNode;
    std::vector <int> receivedNodes; //frontier, the collector insert in it the node received from the workers
};



int main(int argc, char *argv[]) {    
      
	int nw;
	int X; // the value on which count the occurences
	int numNodes;// total number of nodes in the graph
    int sourceNode;

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

    if(nw <= 0){
        cout << "THE NW MUST BE >= 1 " << endl;
        return -1;
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

	//initialize binary vec
	std::vector <int> visitedNodes (graph.numNodes);

    for( int i=0; i< graph.numNodes ; i+=5){
        visitedNodes[i]=0; 
        visitedNodes[i+1]=0;
        visitedNodes[i+2]=0;
        visitedNodes[i+3]=0;
        visitedNodes[i+4]=0;
    }

    //graph.printGraph();
    vector<mutex> globallock(graph.numNodes);

    #ifdef PRINTMINIMAL
        cout << "THE TOTAL OCCOURENCES OF " << X << " ARE:  " << graph.Occurences << endl;
    #endif

    int initialExpectedOutput = graph.adjList[sourceNode].size();
    #ifdef PRINT 
    std::cout << "INIT LEN: " << initialExpectedOutput<<endl;
    #endif
    emitter  Emitter(ref(graph), sourceNode);
    collector Collector(ref(graph), initialExpectedOutput, X, sourceNode); 

    std::vector<std::unique_ptr<ff_node> > W;
    for(size_t i=0;i<nw;++i) W.push_back(make_unique<worker>(ref(graph), ref(visitedNodes), ref(globallock) ));

    
    
    ff_Farm<int> farm(std::move(W), Emitter, Collector);
    farm.wrap_around();
    

    long Tc;
    {

    utimer tt("TIME COMPLETION: ", &Tc);
    if (farm.run_and_wait_end()<0) {
        error("running farm");
        return -1;
    }
    }
    ffTime(STOP_TIME);

    /*
    ofstream outfile("exec_time_ff.txt", ios::app);
	outfile  << " NumNodes: " << graph.numNodes << " nw: "<< nw << " TC: "<< Tc <<  endl;
    outfile  <<"----------------------------------"<<endl;
    */

    return 0;
}
