//
// Author: Joshua Hildred jt<lastname>@gmail.com>
//

#include "perf_tracker.h"
#include <float.h>

PerThreadContainer::PerThreadContainer(uint64_t id, uint64_t thread_id) : id_(id), thread_id_(thread_id), count_(0),
average_(0), max_(DBL_MIN), min_(DBL_MAX){}

bool PerThreadContainer::addData(double d) {
    if(d > max_)
    {
        max_ = d;
    }

    if(d < min_)
    {
        min_ = d;
    }
    average_ = ((average_ * count_) + d)/(count_ + 1);
    count_++;
    return true;
}

double PerThreadContainer::getAverage() {
    return average_;
}

double PerThreadContainer::getMax() {
    return max_;
}

double PerThreadContainer::getMin() {
    return min_;
}

uint64_t PerThreadContainer::getCount() {
    return count_;
}

StatContainer::StatContainer(uint64_t id, std::string name, uint64_t size) : id_(id), name_(name) {
    for(int i = 0; i < size; i++)
    {
        per_thread_containers_.emplace_back(new PerThreadContainer(id, i));
    }
}

bool StatContainer::addData(uint64_t thread_id, double data) {
    return per_thread_containers_[thread_id]->addData(data);
}

void StatContainer::dumpToFile(std::ofstream &file) {
    file << "Stat: " << name_ << "\n";
    uint64_t id = 0;
    for ( auto cont : per_thread_containers_) {
        file << "thread id: " << id << ", " << cont->getAverage() << ", " << cont->getMax() << ", " << cont->getMin() << ", " << cont->getCount() << "\n";
        id++;
    }
}

void StatContainer::pp(uint64_t thread_id, std::ostream &out) {
    auto cont = per_thread_containers_[thread_id];
    out << "Stat: " << name_ << " thread id: " << thread_id << ", Average = " << cont->getAverage() << ", Max = " << cont->getMax() << ", Min = " << cont->getMin() << ", Count = " << cont->getCount() << std::endl;
}

void StatContainer::dump(std::ostream &out) {
    out << "Stat: " << name_ << "\n";
    for ( int id = 0; id < per_thread_containers_.size(); id++) {
        pp(id, out);
    }
}

PerfTracker::PerfTracker(uint64_t machine_id, std::vector<std::pair<std::string, uint64_t>> config) : machine_id(machine_id){
    uint64_t id = 0;
    for (auto conf_pair : config)
    {
        containers_.emplace_back(new StatContainer(id, conf_pair.first, conf_pair.second));
        id++;
    }
}

bool PerfTracker::addData(uint64_t stat_id, uint64_t id, double data) {
    return containers_[stat_id]->addData(id, data);
}

void PerfTracker::dumpToCSV(std::string fname) {
    std::ofstream file;
    file.open(fname);
    for (auto cont : containers_) {
        cont->dumpToFile(file);
    }
    file.close();
}

void PerfTracker::dump(std::ostream & out) {
    for (auto cont : containers_) {
        cont->dump(out);
    }
}

void PerfTracker::pp(uint64_t stat_id, uint64_t thead_id, std::ostream &out) {
    return containers_[stat_id]->pp(thead_id, out);
}
