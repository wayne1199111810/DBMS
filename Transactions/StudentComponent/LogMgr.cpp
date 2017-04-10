#include <stdexcept>
#include <climits>
#include <sstream>
#include <queue>
#include "LogMgr.h"

typedef unsigned int zplus;

int smallestRecLSN(map<int, int>);
void redoUpdate(UpdateLogRecord*, StorageEngine*, map <int, int>);
void redoCLR(CompensationLogRecord*, StorageEngine*, map <int, int>);

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
	int master = se->get_master();
	int start_log = 0;
	for (zplus i = log.size() - 1; i >= 0; i--){
		if (log[i]->getLSN() != master) {
			continue;
		}
		else { 
			start_log = i;
			break;
		}
	}
	for(zplus i = start_log + 1; i < log.size(); i++){
		if(log[i]->getType() == END_CKPT) {
			tx_table = ((ChkptLogRecord*)log[i])->getTxTable();
			dirty_page_table = ((ChkptLogRecord*)log[i])->getDirtyPageTable();
		} else if(log[i]->getType() == END) {
			tx_table.erase(log[i]->getTxID());
		}
		if(tx_table.find(log[i]->getTxID()) == tx_table.end()) {
			tx_table[log[i]->getTxID()] = txTableEntry(log[i]->getLSN(), U);
		} else {
			tx_table[log[i]->getTxID()].lastLSN = log[i]->getLSN();
		}
		if(log[i]->getType() == UPDATE) {
			if(dirty_page_table.find(((UpdateLogRecord*)log[i])->getPageID()) == dirty_page_table.end()) {
				dirty_page_table[((UpdateLogRecord*)log[i])->getPageID()] = log[i]->getLSN();
			}
		} else if(log[i]->getType() == COMMIT) {
			tx_table[log[i]->getTxID()].status = C;
		}
	}
}

bool LogMgr::redo(vector <LogRecord*> log){
	int start_log = smallestRecLSN(dirty_page_table);
	for(zplus i = start_log; i < log.size(); i++){
		if(log[i]->getType() == UPDATE) {
			redoUpdate((UpdateLogRecord*)log[i], se, dirty_page_table);
		} else if(log[i]->getType() == CLR) {
			redoCLR((CompensationLogRecord*)log[i], se, dirty_page_table);
		}
	}
	// TODO
	return false;
}

// TODO
void LogMgr::undo(vector <LogRecord*> log, int txnum){
	queue<int> to_undo;
	if(txnum != NULL_TX) {

	} else {

	}
	for(auto it = log.end() - 1; it >= log.begin(); it--) {

	}
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
	flushLogTail(logtail[logtail.size() - 1]->getLSN());
	if(dirty_page_table.find(page_id) != dirty_page_table.end()) {
		dirty_page_table.erase(page_id);
	}
}

// TODO
void LogMgr::recover(string log){
	// vector<LogRecord*> disk_log = stringToLRVector(log);
	// analyze(disk_log);
	// redo(disk_log);
	// undo(disk_log);
}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
	int lsn = se->nextLSN();
	if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
		dirty_page_table[page_id] = lsn;
	}
	setLastLSN(txid, lsn);
	logtail.push_back(new UpdateLogRecord(lsn, getLastLSN(txid), txid, page_id, offset, oldtext, input));
	return lsn;
}

void LogMgr::setStorageEngine(StorageEngine* engine){
	se = engine;
}

int smallestRecLSN(map<int, int> dirty_page_table) {
	int smallest = INT_MAX;
	for (auto const & row : dirty_page_table) {
		if(row.second < smallest)
			smallest = row.second;
	}
	return smallest;
}

void redoUpdate(UpdateLogRecord* log, StorageEngine* se, map <int, int> dirty_page_table) {
	if(dirty_page_table.find(log->getPageID()) != dirty_page_table.end()) {
		if(dirty_page_table[log->getPageID()] <= log->getLSN()) {
			if (se->getLSN(log->getPageID()) < log->getLSN()) {
				se->pageWrite(log->getPageID(), log->getOffset(), log->getAfterImage(), log->getLSN());
			}
		}
	}
}

void redoCLR(CompensationLogRecord* log, StorageEngine* se, map <int, int> dirty_page_table) {
	if(dirty_page_table.find(log->getPageID()) != dirty_page_table.end()) {
		if(dirty_page_table[log->getPageID()] <= log->getLSN()) {
			if (se->getLSN(log->getPageID()) < log->getLSN()) {
				se->pageWrite(log->getPageID(), log->getOffset(), log->getAfterImage(), log->getLSN());
			}
		}
	}
}