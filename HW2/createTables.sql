CREATE TABLE Student 
(
	sid NUMBER PRIMARY KEY,
	name VARCHAR2(100),
	major VARCHAR2(100)
);

CREATE TABLE Project
(
 	pid NUMBER PRIMARY KEY,
 	ptitle VARCHAR2(100)
);

CREATE TABLE Course
(
	cid NUMBER PRIMARY KEY,
	title VARCHAR2(100)
);

CREATE TABLE Member
(
	pid NUMBER,
	sid NUMBER,
	PRIMARY KEY (pid, sid),
	FOREIGN KEY (pid) REFERENCES Project(pid),
	FOREIGN KEY (sid) REFERENCES Student(sid)
);

CREATE TABLE Enrolled
(
	sid NUMBER,
	cid NUMBER,
	PRIMARY KEY (sid, cid),
	FOREIGN KEY (cid) REFERENCES Course(cid),
	FOREIGN KEY (sid) REFERENCES Student(sid)
);