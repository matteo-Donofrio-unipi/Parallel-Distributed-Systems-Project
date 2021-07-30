#include <iostream>
#include <vector>
#include <atomic>
#include <cstdlib>
#include <string>
#include <thread> 
#include "utimer.cpp"
#include <algorithm>
using namespace std;




// A class to represent a graph object
class Graph{
public:
    int numNodes; // total number of nodes in the graph
    vector<vector<int>> adjList; // a vector of vectors to represent an adjacency list
    vector <int> nodeValues ; //vector containing the node values x
    int Occurences=0; // count the total occurences of the X value

    
 
    // Graph Constructor
    Graph(const string &nameFileGraph, const string &nameFileNodeValues, int numNodes , int X){
        long timeRead;
        long timeFill;
        //fill the graph
        ifstream infile1(nameFileGraph); //file containing the graph structure
        ifstream infile2(nameFileNodeValues); // file containing the node values
        int NodeSourceID, NodeDestID;

        this->numNodes=numNodes;
        nodeValues.resize(numNodes);
        adjList.resize(numNodes);
        //reading from file the input data
        {
            //utimer tt("Time reading the graph: ", &timeRead);
            while (infile1 >> NodeSourceID >> NodeDestID){
                adjList[NodeSourceID].push_back(NodeDestID);
            }
            infile1.close(); 
        }    
            

        //assign node values random
        int i=0;
        int nodeValue;
        
        {
            //utimer tt("Time filling the graph: ", &timeRead);
            for(int i=0 ; i<numNodes;i++){
                infile2 >> nodeValue ;;
                nodeValues[i]=nodeValue;
                if(nodeValue==X)
                    Occurences++;
            }
        }
        infile2.close(); 
        return; 
    }

        void printGraph(){
            for (int i = 0; i < numNodes; i++){
                // print the current vertex number
                cout << "nodeID: " << i << " nodeValue: "<<  nodeValues[i] << " Fronteer ——> " <<endl;
        
                // print all neighboring vertices of a vertex `i`
                for (int v: adjList[i]) {
                    cout << v << " ";
                }
                cout << endl;
                
            }
        }

};

