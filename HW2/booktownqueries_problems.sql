-- Homework #2: EECS 484.
-- Your uniquname:
-- include your teamate's uniqname if you are working in a team of two
-- We use the 'PROMPT' command to print out the problem number. DO NOT DELETE THAT, otherwise this may cause you failing the testcases.

-- Your answer should work for any instance of the database, not just the one given.

-- EXAMPLE
-- Q0: "list titles of all books". Answer given below.

SELECT title FROM books;

-- Q1
PROMPT Question 5.1;
-- List the ISBN of all books written by "Tom Christiansen"
SELECT DISTINCT E.isbn
FROM books B, editions E, authors A
WHERE B.author_id = A.author_id AND A.first_name = 'Tom' AND A.last_name = 'Christiansen'
AND E.book_id = B.book_id;


-- Q2
PROMPT Question 5.2;
-- List last name and first name of authors who have written both
-- Short Story and Horror books. In general, there could be two different authors
-- with the same name, one who has written a horror book and another
-- who has written short stories. 
SELECT DISTINCT A.last_name, A.first_name
FROM authors A, books B1, books B2, subjects S1, subjects S2
WHERE A.author_id = B1.author_id AND B1.subject_id = S1.subject_id AND S1.subject = 'Short Story'
AND A.author_id = B2.author_id AND B2.subject_id = S2.subject_id AND S2.subject = 'Horror';

-- Q3
PROMPT Question 5.3;
-- List titles, publication, author's id, author's last name, and author's first name of all books 
-- by authors who have published at least one book after 1999-10-01 but before 2001-10-01. 
-- Note: this may require a nested query. The answer can include books that are not published in between 
-- the publication requirements. You can also use views. But DROP any views at the end of your query.
-- Using a single query is likely to be more 
-- efficient in practice. Moreover, there shouldn't be any duplication for the returned records.
SELECT DISTINCT B.title, E.publication, A.author_id, A.last_name, A.first_name
FROM books B, editions E, 
(
	SELECT DISTINCT A.author_id AS author_id, A.last_name AS last_name, A.first_name AS first_name
	FROM authors A, books B, editions E
	WHERE B.author_id = A.author_id AND B.book_id = E.book_id AND E.publication > '1999-10-01'
	AND E.publication < '2001-10-01'
)A
WHERE B.author_id = A.author_id AND B.book_id = E.book_id
;

-- Q4
PROMPT Question 5.4;
-- Find id, first name, and last name of authors who wrote books for all the 
-- subjects of books written by Edgar Allen Poe.
CREATE VIEW author_subject (author_id, first_name, last_name, subject)
AS SELECT DISTINCT A.author_id, A.first_name, A.last_name, S.subject
FROM authors A, books B, subjects S
WHERE A.author_id = B.author_id AND B.subject_id = S.subject_id;

SELECT DISTINCT author_id, first_name, last_name FROM author_subject
MINUS
SELECT author_id, first_name, last_name FROM
(
	SELECT author_id, first_name, last_name, subject FROM
	(
		SELECT author_id, first_name, last_name FROM author_subject
	),
	(
		SELECT S.subject AS subject
		FROM authors A, books B, subjects S
		WHERE A.first_name = 'Edgar Allen' AND A.last_name = 'Poe' AND A.author_id = B.author_id
		AND B.subject_id = S.subject_id
	)
	MINUS
	SELECT author_id, first_name, last_name, subject FROM author_subject
)
;
DROP VIEW author_subject;

-- Q5
PROMPT Question 5.5;
-- Find the book_id and its corresponding total stock available for all book editions ordered
-- in descending order by the total stock. Name the column for total stock as TOTAL_STOCK. 
-- NOTE: You do not need to consider editions of books that are not in the Stock Table.
SELECT DISTINCT B.book_id, SUM (S.stock) AS TOTAL_STOCK
FROM books B, editions E, stock S
WHERE B.book_id = E.book_id AND S.isbn = E.isbn
GROUP BY B.book_id
ORDER BY TOTAL_STOCK DESC;

-- Q6
PROMPT Question 5.6;
-- Find the name and id of all publishers who have published books for authors
-- who have written exactly 2 books. Result should be ordered by publisher id in descending order;
SELECT DISTINCT P.name, P.publisher_id
FROM publishers P, books B, editions E,
(
	SELECT DISTINCT A.author_id AS author_id
	FROM authors A, books B
	WHERE A.author_id = B.author_id
	GROUP BY A.author_id
	HAVING COUNT(*) = 2
)A
WHERE A.author_id = B.author_id AND B.book_id = E.book_id AND E.publisher_id = P.publisher_id
ORDER BY P.publisher_id DESC;

-- Q7
PROMPT Question 5.7;
-- Find the last name and first name of authors who haven't written any book.
-- Name the last name column as l_name, the first name column as f_name.
SELECT DISTINCT A.last_name AS l_name, A.first_name AS f_name
FROM authors A, books B
WHERE A.author_id NOT IN
(
	SELECT DISTINCT A.author_id
	FROM authors A, books B
	WHERE A.author_id = B.author_id
);

-- Q8
PROMPT Question 5.8;
-- Find author_id of authors who have written exactly 1 book. Name the author_id column as aid. 
-- Order the id in ascending order. 
SELECT DISTINCT A.author_id AS aid
FROM authors A, books B
WHERE A.author_id = B.author_id
GROUP BY A.author_id HAVING COUNT(*) = 1;