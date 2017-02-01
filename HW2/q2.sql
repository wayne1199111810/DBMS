SELECT DISTINCT S1.sid
FROM Student S1, Student S2, Member M1, Member M2, Enrolled E1, Enrolled E2, Enrolled E3, Course C1, Course C2, Course C3
WHERE
S1.sid = M1.sid AND M2.sid = S2.sid AND M1.pid = M2.pid AND S1.sid <> S2.sid
AND S2.sid = E1.sid AND E1.cid = C1.cid
AND S2.sid = E2.sid AND E2.cid = C2.cid
AND S2.sid = E3.sid AND E3.cid = C3.cid
AND (C1.title = 'EECS482' OR C1.title = 'EECS483')
AND (C2.title = 'EECS484' OR C2.title = 'EECS485')
AND C3.title = 'EECS280';