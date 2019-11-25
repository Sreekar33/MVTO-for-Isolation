#include "db.h"
#include <unistd.h>


int main() {
  DB db;

  for (int i = 0; i < 1000; i++) {
    Appender* app = db.NewAppender();
    for (int seriesID = 0; seriesID < 100; seriesID++) {
      app->AddRef(seriesID, i*seriesID, i*100);
    }
    app->Commit();
  }

  cout << "Appending done, waiting 10secs" << endl;
  sleep(10);
  cout << "Continuing to querying" << endl;


  auto q = db.NewQuerier(1, 100);
  vector<int> sids = {1, 2, 3, 4};

  auto si = q->Select(sids);
  while (si.Next()) {
    auto s = si.At();
    cout << "Ref: " << s.Ref() << endl;
    while (s.Next()) {
      cout << s.At().t << ", " << s.At().v << endl;
    }
  }
}
