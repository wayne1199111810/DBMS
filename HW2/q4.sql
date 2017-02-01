CREATE VIEW StudentPairs (sid1, sid2)
AS SELECT DISTINCT S1.sid, S2.sid 
FROM Student S1, Student S2, Enrolled E1, Enrolled E2, Course C
WHERE S1.sid = E1.sid AND S2.sid = E2.sid AND E1.cid = E2.cid AND S1.sid <= S2.sid
MINUS
SELECT DISTINCT S1.sid, S2.sid FROM Student S1, Student S2, Member M1, Member M2 
WHERE S1.sid = M1.sid AND S2.sid = M2.sid AND M1.pid = M2.pid AND S1.sid <= S2.sid
;