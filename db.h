#include <iostream>
#include <map>
#include <shared_mutex>


#include "series.h"

using namespace std;

class DB;
class Querier;

class Appender {
  private:
    vector<refSample> samples;
    DB* db;

  public:
    Appender(DB* db) {
      this->db = db;
    }

    void AddRef(int ref, int t, int v);

    void Commit();
};

class DB {
  private:
    shared_timed_mutex mtx;
  public:
    DB() {
    }

    map<int, Series*> series;

    Appender* NewAppender() {
      return new Appender(this);
    }

    Querier* NewQuerier(int mint, int maxt);


    void Lock() {
      mtx.lock();
    }

    void Unlock() {
      mtx.unlock();
    }

    void RLock() {
      mtx.lock_shared();
    }

    void RUnlock() {
      mtx.unlock_shared();
    }
};

class Querier {
  private:
    DB* db;
    int mint;
    int maxt;

  public:
    Querier(DB* db, int mint, int maxt) {
      this->db = db;
      this->mint = mint;
      this->maxt = maxt;
    }

    SeriesIterator Select(vector<int> refs) {
      vector<Series*> series;
      series.reserve(refs.size());

      db->RLock();
      for (int ref : refs) {
        auto it = db->series.find(ref);
        if (it == db->series.end())
          continue;
        
        series.push_back(it->second);
      }
      db->RUnlock();

      return SeriesIterator(series, mint, maxt);
    }
};


void Appender::AddRef(int ref, int t, int v) {
  struct refSample rs = {
    .ref=ref,
    .s={
      .t=t,
      .v=v
    }
  };

  db->RLock();
  Series* s = db->series[ref];
  db->RUnlock();
  if (s == NULL) {
    s = new Series(ref);
    db->Lock();
    db->series[ref] = s;
    db->Unlock();
  }

  rs.series = s;

  samples.push_back(rs);
};

void Appender::Commit() {
  for (refSample rs : samples) {
    rs.series->Lock();
    rs.series->append(rs.s.t, rs.s.v);
    rs.series->Unlock();
  }
};

Querier* DB::NewQuerier(int mint, int maxt) {
  return new Querier(this, mint, maxt);
}
