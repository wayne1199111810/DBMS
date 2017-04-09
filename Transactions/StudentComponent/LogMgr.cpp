#include <stdexcept>
#include <climits>
#include <sstream>
#include "LogMgr.h"

typedef unsigned int zplus;

int LogMgr::getLastLSN(int txnum){
	if (tx_table.find(txnum) != tx_table.end()) {
		return tx_table[txnum].lastLSN;
	} else {
		return NULL_LSN;
	}
}

void LogMgr::setLastLSN(int txnum, int lsn){
	if (tx_table.find(txnum) != tx_table.end()) {
		tx_table[txnum].lastLSN = lsn;
	} else {
		tx_table[txnum] = txTableEntry(lsn, U);
	}
}

void LogMgr::flushLogTail(int maxLSN){
	zplus i = 0;
	while (logtail.size() > i && logtail[i]->getLSN() <= maxLSN ) {
		se->updateLog(logtail[i]->toString());
		i++;
	}
	logtail.erase(logtail.begin(), logtail.begin() + i);
}

void LogMgr::analyze(vector <LogRecord*> log){
	int master = get_master();
	int start_log = 0;
	for (auto i = log.end() - 1; i >= log.begin(); i--){
		if (i->getLSN() != master) continue;
		else start_log = (int)(i - log.begin());
	}
	for(auto it = log.begin() + start_log + 1; it < log.end(); it++){
		if(it->getType() == END_CKPT) {
			tx_table = log[++start_log]->getTxTable()
			dirty_page_table = log[start_log]->getDirtyPageTable();
		} else if(it->getType() == END) {
			tx_table.erase(it->getTxID());
		}
		if(tx_table.find(it->getTxID()) == tx_table.end()) {
			tx_table[it->getTxID()] = txTableEntry(it->getLSN(), U);
		} else {
			tx_table[it->getTxID()].lastLSN = it->getLSN();
		}
		if(it->getType() == UPDATE) {
			if(dirty_page_table.find(it->getPageID()) == dirty_page_table.end()) {
				dirty_page_table[it->getPageID()] = it->getLSN();
			}
		} else if(it->getType() == COMMIT) {
			tx_table[it->getTxID()].status = C;
		}
	}
}

// TODO
bool LogMgr::redo(vector <LogRecord*> log){
	int start_log = smallestRecLSN();
	for(auto it = log.begin() + start_log; it < log.end(); it++){
		if(it->getType() == UPDATE) {
			redoUpdate(log[i]);
		} else if(it->getType() == CLR) {
			redoCLR();
		}
	}
}

// TODO
void LogMgr::undo(vector <LogRecord*> log, int txnum = NULL_TX){
	
}

vector<LogRecord*> stringToLRVector(string logstring){
	vector<LogRecord*> result;
	istringstream stream(logstring);
	string line;
	while(getline(stream, line)) {
		LogRecord* lr = LogRecord::stringToRecordPtr(line);
		result.push_back(lr);
	}
	return result;
}

// TODO
void LogMgr::abort(int txid){
	
}

void LogMgr::checkpoint(){
	// begin check point
	int lsn = se->nextLSN();
	logtail.push_back(new LogRecord(lsn, NULL_LSN, NULL_TX, BEGIN_CKPT));
	if(!se->store_master(lsn))
		 throw runtime_error("Cannot store master on begin check point.");
	// end check point
	lsn = se->nextLSN();
	logtail.push_back(new ChkptLogRecord(lsn, lsn - 1, NULL_TX, tx_table, dirty_page_table));
}

void LogMgr::commit(int txid){
	int lsn = se->nextLSN();
	if (tx_table.find(txid) != tx_table.end()) {
		tx_table[txid].status = C;
	} else {
		tx_table[txid] = txTableEntry(lsn, C);
	}
	setLastLSN(txid, lsn);
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), txid, COMMIT));
	flushLogTail(lsn);
	logtail.push_back(new LogRecord(se->nextLSN(), lsn, txid, END));
	tx_table.erase(txid);
}

void LogMgr::pageFlushed(int page_id){
	if(logtail.size() != 0)
	flushLogTail(logtail[logtail.size() - 1].getLSN());
	if(dirty_page_table.find(page_id) != dirty_page_table.end()) {
		dirty_page_table.erase(page_id);
	}
}

// TODOL
void LogMgr::recover(string log){
	vector<LogRecord*> disk_log =  stringToLRVector(log);
	analyze(disk_log);
	redo();
	undo();
}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
	int lsn = se->nextLSN();
	if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
		dirty_page_table[page_id] = lsn;
	}
	setLastLSN(txid, lsn);
	logtail.push_back(new UpdateLogRecord(lsn, getLastLSN(txid), txid, page_id, offset, oldtext, input));
}

void LogMgr::setStorageEngine(StorageEngine* engine){
	se = engine;
}

int smallestRecLSN() {
	int smallest = INT_MAX;
	for (auto const & row : dirty_page_table) {
		if(row.second < smallest)
			smallest = row.second;
	}
	return smallest
}

void redoUpdate(UpdateLogRecord* log) {
	if(dirty_page_table.find(log->getPageID()) != dirty_page_table.end()) {
		if(dirty_page_table[log->getPageID()] <= log->getLSN()) {

			if ()
		}
	}
}

void redoCLR() {

}