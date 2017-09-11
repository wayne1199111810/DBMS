#include <stdexcept>
#include <climits>
#include <sstream>
#include <queue>
#include "LogMgr.h"

typedef unsigned int zplus;

int smallestRecLSN(map<int, int>, vector <LogRecord*>);
bool redoUpdate(UpdateLogRecord*, StorageEngine*, map <int, int>);
bool redoCLR(CompensationLogRecord*, StorageEngine*, map <int, int>);
void lastLSNotAliveTxs(map<int, txTableEntry>, priority_queue<int>&);
int move(priority_queue<int>&);
LogRecord* getLogRecord(int maxLSN, vector <LogRecord*> log);
void redoEnd(StorageEngine*, map <int, txTableEntry>&, vector <LogRecord*>&);
bool isExist(vector<LogRecord*>, int);
int mostRecentCheckPoint(vector <LogRecord*>, int);

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
	zplus i;
	for (i = 0; i < logtail.size() && logtail[i]->getLSN() <= maxLSN; i++) {
		se->updateLog(logtail[i]->toString());
	}
	logtail.erase(logtail.begin(), logtail.begin() + i);
}

void LogMgr::analyze(vector <LogRecord*> log){
	int master = se->get_master();
	int start_log = mostRecentCheckPoint(log, master);
	for(zplus i = start_log; i < log.size(); i++){
		if(log[i]->getType() == END_CKPT) {
			tx_table = ((ChkptLogRecord*)log[i])->getTxTable();
			dirty_page_table = ((ChkptLogRecord*)log[i])->getDirtyPageTable();
		} else if(log[i]->getType() == END) {
			tx_table.erase(log[i]->getTxID());
		} else {
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
}

bool LogMgr::redo(vector <LogRecord*> log){
	int start_log = smallestRecLSN(dirty_page_table, log);
	for(zplus i = start_log; i < log.size(); i++){
		if(log[i]->getType() == UPDATE) {
			if(!redoUpdate((UpdateLogRecord*)log[i], se, dirty_page_table))
				return false;
		} else if(log[i]->getType() == CLR) {
			if(!redoCLR((CompensationLogRecord*)log[i], se, dirty_page_table))
				return false;
		} 
	}
	redoEnd(se, tx_table, logtail);
	return true;
}

void LogMgr::undo(vector <LogRecord*> log, int txnum){
	priority_queue<int> to_undo;
	if(txnum != NULL_TX) {
		to_undo.push(getLastLSN(txnum));
	} else {
		lastLSNotAliveTxs(tx_table, to_undo);
	}
	while(!to_undo.empty()) {
		int maxLSN = move(to_undo);
		LogRecord* current_log = getLogRecord(maxLSN, log);
		if(current_log->getType() == UPDATE) {
			UpdateLogRecord* update = (UpdateLogRecord*)current_log;
			int lsn = se->nextLSN();
			int txid = current_log->getTxID();
			int prevLSN = getLastLSN(txid);
			int page_id = update->getPageID();
			int offset = update->getOffset();
			string before_image = update->getBeforeImage();
			int undoNextLSN = update->getprevLSN();
			setLastLSN(txid, lsn);
			logtail.push_back(new CompensationLogRecord(lsn, prevLSN, txid, page_id, offset, before_image, undoNextLSN));
			// TODO push WRITE PAGE
			if(!se->pageWrite(page_id, offset, before_image, lsn))
				return;
			if(dirty_page_table.find(page_id) == dirty_page_table.end())
				dirty_page_table[page_id] = lsn;
			if(undoNextLSN != NULL_LSN) {
				to_undo.push(undoNextLSN);
			} else {
				logtail.push_back(new LogRecord(se->nextLSN(), lsn, txid, END));
				tx_table.erase(txid);
			}
		} else if(current_log->getType() == CLR) {
			int undo_nextLSN = ((CompensationLogRecord*)current_log)->getUndoNextLSN();
			if(undo_nextLSN != NULL_LSN) {
				to_undo.push(undo_nextLSN);
			} else {
				int lsn = current_log->getLSN();
				int txid = current_log->getTxID();
				logtail.push_back(new LogRecord(se->nextLSN(), lsn, txid, END));
				tx_table.erase(txid);
			}
		} else if(current_log->getType() == ABORT) {
			if(current_log->getprevLSN() != NULL_LSN) {
				to_undo.push(current_log->getprevLSN());
			} else {
				int txid = current_log->getTxID();
				tx_table.erase(txid);
			}
		}
	}
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
	vector<LogRecord*> result;
	istringstream stream(logstring);
	string line;
	while(getline(stream, line)) {
		LogRecord* lr = LogRecord::stringToRecordPtr(line);
		result.push_back(lr);
	}
	return result;
}

void LogMgr::abort(int txid){
	int lsn = se->nextLSN();
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), txid, ABORT));
	setLastLSN(txid, lsn);
	vector<LogRecord*> disk_log = stringToLRVector(se->getLog());
	vector <LogRecord*> log(0);
	for(zplus i = 0; i < disk_log.size(); i++){
		if(disk_log[i]->getTxID() == txid)
			log.push_back(disk_log[i]);
	}
	for(zplus i = 0; i < logtail.size(); i++) {
		if(logtail[i]->getTxID() == txid && !isExist(log, logtail[i]->getLSN()))
			log.push_back(logtail[i]);
	}
	undo(log, txid);
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
	flushLogTail(lsn);
}

void LogMgr::commit(int txid){
	int lsn = se->nextLSN();
	if (tx_table.find(txid) != tx_table.end()) {
		tx_table[txid].status = C;
	} else {
		tx_table[txid] = txTableEntry(lsn, C);
	}
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), txid, COMMIT));
	setLastLSN(txid, lsn);
	flushLogTail(lsn);
	logtail.push_back(new LogRecord(se->nextLSN(), lsn, txid, END));
	tx_table.erase(txid);
}

void LogMgr::pageFlushed(int page_id){
	int maxLSN = se->getLSN(page_id);
	flushLogTail(maxLSN);
	dirty_page_table.erase(page_id);
}

void LogMgr::recover(string log){
	vector<LogRecord*> disk_log = stringToLRVector(log);	
	analyze(disk_log);
	if(redo(disk_log)) {
		undo(disk_log);
	}

}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
	int lsn = se->nextLSN();
	logtail.push_back(new UpdateLogRecord(lsn, getLastLSN(txid), txid, page_id, offset, oldtext, input));
	if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
		dirty_page_table[page_id] = lsn;
	}
	setLastLSN(txid, lsn);
	return lsn;
}

void LogMgr::setStorageEngine(StorageEngine* engine){
	se = engine;
}

int smallestRecLSN(map<int, int> dirty_page_table, vector <LogRecord*> log) {
	int smallest = INT_MAX;
	for (auto const & row : dirty_page_table) {
		if(row.second < smallest)
			smallest = row.second;
	}
	int idx = 0;
	if(smallest != INT_MAX) {
		for(zplus i = 0; i < log.size(); i++) {
			if (log[i]->getLSN() == smallest) {
				idx = i;
				break;
			}
		}
	}
	return idx;
}

int mostRecentCheckPoint(vector <LogRecord*> log, int master) {
	int start_log = 0;
	for (int i = log.size() - 1; i >= 0; i--){
		if (log[i]->getLSN() != master) {
			continue;
		}
		else { 
			start_log = i;
			break;
		}
	}
	return start_log;
}

bool redoUpdate(UpdateLogRecord* log, StorageEngine* se, map <int, int> dirty_page_table) {
	if(dirty_page_table.find(log->getPageID()) != dirty_page_table.end()) {
		if(dirty_page_table[log->getPageID()] <= log->getLSN()) {
			if (se->getLSN(log->getPageID()) < log->getLSN()) {
				return se->pageWrite(log->getPageID(), log->getOffset(), log->getAfterImage(), log->getLSN());
			}
		}
	}
	return true;
}

bool redoCLR(CompensationLogRecord* log, StorageEngine* se, map <int, int> dirty_page_table) {	
	if(dirty_page_table.find(log->getPageID()) != dirty_page_table.end()) {
		if(dirty_page_table[log->getPageID()] <= log->getLSN()) {
			if (se->getLSN(log->getPageID()) < log->getLSN()) {
				return se->pageWrite(log->getPageID(), log->getOffset(), log->getAfterImage(), log->getLSN());
			}
		}
	}
	return true;
}

void redoEnd(StorageEngine* se, map <int, txTableEntry>& tx_table, vector <LogRecord*>& logtail){
	for(auto const & entry : tx_table) {
		if(entry.second.status == C){
			logtail.push_back(new LogRecord(se->nextLSN(), entry.second.lastLSN, entry.first, END));
			tx_table.erase(entry.first);
		}
	}
}

void lastLSNotAliveTxs(map<int, txTableEntry> tx_table, priority_queue<int>&to_undo){
	for(auto it = tx_table.begin(); it != tx_table.end(); it++) {
		if(it->second.status == U) {
			to_undo.push(it->second.lastLSN);
		}
	}
}

int move(priority_queue<int> &to_undo) {
	int front = to_undo.top();
	to_undo.pop();
	return front;
}

LogRecord* getLogRecord(int maxLSN, vector <LogRecord*> log) {
	for(zplus i = 0; i < log.size(); i++) {
		if (log[i]->getLSN() == maxLSN)
			return log[i];
	}
	return nullptr;
}

bool isExist(vector<LogRecord*> log, int lsn) {
	for(zplus i = 0; i < log.size(); i++) {
		if(log[i]->getLSN() == lsn)
			return true;
	}
	return false;
}