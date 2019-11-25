#include <climits>

#include <vector>
#include <mutex>

using namespace std;

struct sample {
  int t;
  int v;
};

class Chunk {
  public:
    Chunk(int mint) {
      this->start = mint;
      this->end = INT_MIN;
    }

    int start;
    int end;
  
    vector<sample> samples;

    int numSamples() {
      return samples.size();
    }

    void add(int t, int v) {
      if (!samples.empty()) {
        int lastT = samples.back().t;
        if (lastT >= t)
          return;
      }

      sample s = {
        .t=t,
        .v=v,
      };

      samples.push_back(s);
      if (end < t) {
        end = t;
      }
    }

    vector<sample> Samples() {
      return samples;
    }
};

class Series {
  private:
    int ref;
    mutex mtx;
    vector<Chunk*> chunks;

    Chunk* head() {
      if (chunks.size() == 0) {
        return NULL;
      }

      return chunks.back();
    }

    Chunk* cut(int t) {
      Chunk* c = new Chunk(t);
      chunks.push_back(c);

      return c;
    }

  public:
    Series(int ref) {
      this->ref = ref;
    }

    int Ref() {
      return ref;
    }

    void append(int t, int v) {
      auto c = head();

      if (c == NULL) {
        c = cut(t);
      }

      if (c->numSamples() >= 120) {
        c = cut(t);
      }

      c->add(t, v);
    }

    vector<Chunk*> Chunks() {
      return chunks;
    }

    void Lock() {
      mtx.lock();
    }
    void Unlock() {
      mtx.unlock();
    }
};

struct refSample {
  int ref;
  sample s;
  Series* series;
};


class SampleIterator {
  private:
    int ref;
    vector<Chunk*> chunks;
    int mint;
    int maxt;

    bool initialised;


    vector<Chunk*>::iterator chkIt;
    vector<sample>::iterator smplIt;
    sample cur;
  public:
    SampleIterator(int ref, vector<Chunk*> chunks, int mint, int maxt) {
      this->ref = ref;
      this->chunks = chunks;
      this->mint = mint;

      int maxMaxT = 0;
      for (Chunk* chk : chunks) {
        if (maxMaxT < chk->end)
          maxMaxT = chk->end;
      }

      if (maxt > maxMaxT) {
        maxt = maxMaxT;
      }
      this->maxt = maxt;

      this->initialised = false;

      // Also make a copy of the last chunk because it is being written to!
      if (this->chunks.empty()) {
        return;
      }
      auto lastChunk = this->chunks.back();
      this->chunks.pop_back();
      auto newLastChunk = new Chunk(lastChunk->start);
      newLastChunk->end = lastChunk->end;
      copy(lastChunk->samples.begin(), lastChunk->samples.end(), back_inserter(newLastChunk->samples));
      this->chunks.push_back(newLastChunk);
    }

    int Ref() {
      return ref;
    }

    bool Next() {
      if (!initialised) {
        chkIt = chunks.begin();
        if (chkIt == chunks.end()) {
          return false;
        }
        smplIt = (*chkIt)->Samples().begin();

        initialised = true;
      }

      while(true) {
        if (chkIt == chunks.end()) {
          return false;
        }

        if (smplIt == (*chkIt)->Samples().end()) {
          chkIt++;
          if (chkIt == chunks.end()) {
            return false;
          }
          smplIt = (*chkIt)->Samples().begin();
        }

        cur = *smplIt;
        smplIt++;

        if (cur.t >= maxt)
          return false;

        if (cur.t >= mint)
          break;
      }

      return true;
    }

    sample At() {
      return cur;
    }
};

class SeriesIterator {
  private:
    vector<Series*> series;
    int mint;
    int maxt;

    bool initialised;
    vector<Series*>::iterator it;

    Series* cur;

  public:
    SeriesIterator(vector<Series*> series, int mint, int maxt) {
      this->series = series;
      this->mint = mint;
      this->maxt = maxt;

      this->initialised = false;
    }

    bool Next() {
      if (!initialised) {
        it = series.begin();

        initialised = true;
      } 

      if (it == series.end()) {
        return false;
      }
      cur = *it;
      it++;

      if (cur == NULL)
        return false;

      return true;
    }

    SampleIterator At() {
      cur->Lock();
      auto si = SampleIterator(cur->Ref(), cur->Chunks(), mint, maxt);
      cur->Unlock();

      return si;
    }
};
