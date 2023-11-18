//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#ifndef _DB_PERF_H_
#define _DB_PERF_H_

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <utility>
#include <thread>
#include "machine/cluster_config.h"


enum STATSYMBOLS {INSERTER_THROUGHPUT, INSERTER_BACKLOG, INSERTER_TIME, SEQUENCER_THROUGHPUT};
#define PERF_CONF {{"Inserter throughput", 4}, {"Inserter backlog", 4}, {"Inserter time", 4}, {"Sequencer throughput", 1}}


class StatContainer;
class PerThreadContainer;

class PerfTracker {
public:
    PerfTracker(uint64_t machine_id, std::vector<std::pair<std::string, uint64_t>> config = PERF_CONF);
    bool addData(uint64_t stat_id, uint64_t id, double data);
    void dumpToCSV(std::string fname);
    void dump(std::ostream & out);
    void pp(uint64_t stat_id, uint64_t thead_id, std::ostream & out);

private:
    uint64_t machine_id;
    std::vector<StatContainer *> containers_;
};

class StatContainer
{
public:
    StatContainer(uint64_t id, std::string name, uint64_t size);
    bool addData(uint64_t thread_id, double data);
    void dumpToFile(std::ofstream & file);
    void pp(uint64_t thread_id, std::ostream & out);
    void dump(std::ostream & out);
private:
    std::string name_;
    uint64_t id_;
    std::vector<PerThreadContainer *> per_thread_containers_;
};

class PerThreadContainer
{
public:
    PerThreadContainer(uint64_t id, uint64_t thread_id);
    bool addData(double d);
    double getAverage();
    double getMax();
    double getMin();
    uint64_t getCount();
private:
    uint64_t id_;
    uint64_t thread_id_;
    uint64_t count_;
    double average_;
    double max_;
    double min_;
};

#endif
