CREATE VIEW nonHeaveyClass (cid, enroll_num)
AS SELECT DISTINCT E.cid, COUNT(*) AS enroll_num
FROM
(
	SELECT DISTINCT E.sid AS sid, E.cid AS cid
	FROM Course C, Enrolled E, Student S
	WHERE S.sid = E.sid AND C.cid = E.cid
	MINUS
	SELECT DISTINCT E.sid AS sid, E.cid AS cid
	FROM Course C, Enrolled E, Student S
	WHERE S.major = 'CS' AND S.sid = E.sid AND C.cid = E.cid
)E GROUP BY E.cid;

SELECT DISTINCT S.sid, S.name
FROM Student S, Enrolled E, nonHeaveyClass C
WHERE  S.major = 'CS' AND E.sid = S.sid AND E.cid = C.cid AND C.enroll_num > 100
ORDER BY S.name DESC;

DROP VIEW nonHeaveyClass;