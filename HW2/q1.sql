SELECT DISTINCT S.sid
FROM Student S, Enrolled E1, Enrolled E2, Course C1, Course C2
WHERE S.sid = E1.sid AND C1.cid = E1.cid AND S.sid = E2.sid AND C2.cid = E2.cid AND 
(C1.title = 'EECS484' AND C2.title = 'EECS485' OR C1.title = 'EECS482' AND C2.title = 'EECS486')
OR S.sid = E1.sid AND C1.cid = E1.cid AND C1.title = 'EECS281';