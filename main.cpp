#include "db.h"
#include <unistd.h>
#include <unordered_set>
#include <random>
#include <thread>
#include <chrono>

unordered_set<int> BobFloydAlgo(int sampleSize, int rangeUpperBound, default_random_engine* generator);
vector<vector<int>> genSeries(int totSeries, int numSeries, int numSelections, default_random_engine* generator);
void writer(DB* db, int numWrites, vector<int> seriesIDs, int tid);
void reader(DB* db, vector<int> seriesIDs, int mint, int maxt, int tid);

map<int, int> Wtimings;
map<int, int> Rtimings;

int main() {
  DB* db = new DB();
  int totSeries = 10000;
  int totSamples = 100;
  int numSeries = 1000;
  int numWriters = 40;
  int numReaders = 5;

  cout << "Enter number of readers:" << endl;
  cin >> numReaders;

  default_random_engine generator;

  auto writeSelections = genSeries(totSeries, numSeries, numWriters, &generator);
  auto readSelections = genSeries(totSeries, numSeries, numReaders, &generator);
  
  vector<thread> writeThreads;
  vector<thread> readThreads;

  cout << "CREATING WRITERS" << endl;
  int idx = 0;
  for (auto sids :  writeSelections)
    writeThreads.push_back(thread(writer, db, totSamples, sids, idx++));

  cout << "CREATING READERS" << endl;
  idx = 0;
  for (auto sids :  readSelections) 
    readThreads.push_back(thread(reader, db, sids, 0, totSeries, idx++));

  cout << "JOINING WRITERS" << endl;
  for (auto& wth :  writeThreads)
    wth.join();

  cout << "JOINING READERS" << endl;
  for (auto& rth :  readThreads)
    rth.join();

  int Wi = 0;
  int Wtimes = 0;
  for (auto& elem : Wtimings) {
    Wi++;
    Wtimes += elem.second;
  }
  int Ri = 0;
  int Rtimes = 0;
  for (auto& elem : Rtimings) {
    Ri++;
    Rtimes += elem.second;
  }

  cout << "Write avg: " << Wtimes/Wi << endl;
  cout << "Read avg: " << Rtimes/Ri << endl;
}

void writer(DB* db, int numWrites, vector<int> seriesIDs, int tid) {
  auto started = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < numWrites; i++) {
    auto app = db->NewAppender();
    for (int sid : seriesIDs) {
      app->AddRef(sid, i, i*sid);
    }
    app->Commit();
  }
  auto done = std::chrono::high_resolution_clock::now();
  Wtimings[tid] = std::chrono::duration_cast<std::chrono::milliseconds>(done-started).count();
}

void reader(DB* db, vector<int> seriesIDs, int mint, int maxt, int tid) {
  auto started = std::chrono::high_resolution_clock::now();
  auto q = db->NewQuerier(mint, maxt);
  for (int sid : seriesIDs) {
    auto si = q->Select(seriesIDs);
    while (si.Next()) {
      auto smplIt = si.At();
      int ref = smplIt.Ref();
      while (smplIt.Next()) {
        sample smpl = smplIt.At();
        // Do something with ref and sample.
      }
    }
  }
  auto done = std::chrono::high_resolution_clock::now();
  Rtimings[tid] = std::chrono::duration_cast<std::chrono::milliseconds>(done-started).count();
}

vector<vector<int>> genSeries(int totSeries, int numSeries, int numSelections, default_random_engine* generator) {
  vector<vector<int>> result;
  for (int i = 0; i < numSelections; i++) {
    auto set = BobFloydAlgo(numSeries, totSeries, generator);
    vector<int> sids;
    sids.insert(sids.end(), set.begin(), set.end());

    result.push_back(sids);
  }

  return result;
}

unordered_set<int> BobFloydAlgo(int sampleSize, int rangeUpperBound, default_random_engine* generator) {
   unordered_set<int> sample;

   for(int d = rangeUpperBound - sampleSize; d < rangeUpperBound; d++) {
     int t = uniform_int_distribution<>(0, d)(*generator);
     if (sample.find(t) == sample.end() )
       sample.insert(t);
     else
       sample.insert(d);
   }
   return sample;
}

