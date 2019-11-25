#include <mutex>
#include <map>

using namespace std;

class IsolationState;

class Isolation {
  public:
    // Mutex for accessing writeLastId and writesOpen.
    mutex writeMtx;
    // Each write is given an internal id.
    int lastWriteID;
    // Which writes are currently in progress.
    map<int, bool> writesOpen;

    // Mutex for accessing readsOpen.
    // If taking both writeMtx and readMtx, take writeMtx first.
    mutex readMtx;
    // All current in use isolationStates. This is a doubly-linked list.
    IsolationState* readsOpen;

    Isolation();

    int lowWaterMark();
    IsolationState* State();

    int newWriteID();
    void closeWrite(int writeID);
};

class IsolationState {
  public:
    int maxWriteID;
    map<int, bool> incompleteWrites;
    int lowWaterMark;

    Isolation* isolation;

    // Doubly linked list of active reads.
    IsolationState* next;
    IsolationState* prev;

    IsolationState(Isolation* isolation) {
      this->isolation = isolation;
      this->maxWriteID = isolation->lastWriteID;
      this->lowWaterMark = isolation->lowWaterMark();
      readIsoDetails();
    }

    IsolationState(int maxWriteID, int lowWaterMark, Isolation* isolation) {
      this->maxWriteID = maxWriteID;
      this->lowWaterMark = lowWaterMark;
      this->isolation = isolation;
      
      readIsoDetails();
    }

    void readIsoDetails() {
      isolation->writeMtx.lock();
      for (auto const& x : isolation->writesOpen) {
        this->incompleteWrites[x.first] = x.second;

        if (x.first < this->lowWaterMark) {
          this->lowWaterMark = x.first;
        }
      }
      isolation->writeMtx.unlock();


      isolation->readMtx.lock();
      if (isolation->readsOpen == NULL) {
        isolation->readMtx.unlock();
        return;
      }

      this->prev = isolation->readsOpen;
      this->next = isolation->readsOpen->next;
      isolation->readsOpen->next->prev = this;
      isolation->readsOpen->next = this;
      isolation->readMtx.unlock();
    }

    void Close() {
      isolation->readMtx.lock();
      next->prev = prev;
      prev->next = next;
      isolation->readMtx.unlock();
    }
};

Isolation::Isolation() {
  auto isoState = new IsolationState(this);
  isoState->next = isoState;
  isoState->prev = isoState;

  this->readsOpen = isoState;
}


int Isolation::lowWaterMark() {
  // Init period.
  if (this->readsOpen == NULL)
    return 0;

  this->writeMtx.lock();
  this->readMtx.lock();

  if (this->readsOpen->prev == this->readsOpen) {
    this->readMtx.unlock();
    this->writeMtx.unlock();

    return this->lastWriteID;
  }
  this->readMtx.unlock();
  this->writeMtx.unlock();
  
  return this->readsOpen->prev->lowWaterMark;
}

IsolationState* Isolation::State() {
  return new IsolationState(this);
}


int Isolation::newWriteID() {
  this->writeMtx.lock();
  this->lastWriteID++;
  int writeID = this->lastWriteID;
  this->writesOpen[writeID] = true;
  this->writeMtx.unlock();

  return writeID;
}

void Isolation::closeWrite(int writeID) {
  this->writeMtx.lock();
  this->writesOpen.erase(writeID);
  this->writeMtx.unlock();
}
