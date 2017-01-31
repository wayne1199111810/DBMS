SELECT DISTINCT S1.sid
FROM Student S1, Student S2, Member M, Enrolled E1, Enrolled E2, Course C1, Course C2
WHERE
S1.sid = M.sid AND M.sid = S2.sid AND
(
S2.sid = E1.sid AND C1.cid = E1.cid AND S2.sid = E2.sid AND C2.cid = E2.cid AND 
(C1.title = 'EECS484' AND C2.title = 'EECS485' OR C1.title = 'EECS482' AND C2.title = 'EECS486')
OR S2.sid = E1.sid AND C1.cid = E1.cid AND C1.title = 'EECS281'
);