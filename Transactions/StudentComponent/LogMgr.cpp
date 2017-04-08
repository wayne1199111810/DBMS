#include "LogMgr.h"

int LogMgr::getLastLSN(int txnum){

}

void LogMgr::setLastLSN(int txnum, int lsn){

}

void LogMgr::flushLogTail(int maxLSN){

}

void LogMgr::analyze(vector <LogRecord*> log){
	
}

bool LogMgr::redo(vector <LogRecord*> log){
	
}

void LogMgr::undo(vector <LogRecord*> log, int txnum = NULL_TX){
	
}

void LogMgr::abort(int txid){
	
}

void LogMgr::checkpoint(){
	
}

void LogMgr::commit(int txid){
	
}

void LogMgr::pageFlushed(int page_id){
	
}

void LogMgr::recover(string log){
	
}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
	
}

void LogMgr::setStorageEngine(StorageEngine* engine){
	
}