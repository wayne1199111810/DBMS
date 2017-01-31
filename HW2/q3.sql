SELECT DISTINCT S.sid, S.name
FROM Student S
WHERE S.major <> 'CS'  
ORDER BY S.name DESC;