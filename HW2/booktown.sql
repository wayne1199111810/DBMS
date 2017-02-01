-- Following like is Oracle-specific.
-- Needed in Oracle to have ampersand in strings
SET DEFINE OFF;    

-- Uncomment these lines to drop the tables.
--DROP TABLE stock;
--DROP TABLE editions;
--DROP TABLE books;
--DROP TABLE subjects;
--DROP TABLE publishers;
--DROP TABLE authors;

CREATE TABLE authors (
       author_id integer PRIMARY KEY, 
       last_name VARCHAR(20),
       first_name VARCHAR(20)
);

CREATE TABLE publishers ( 
       publisher_id integer  PRIMARY KEY, 
       name	VARCHAR(50), 
       address VARCHAR(100) 
);

CREATE TABLE subjects ( 
       subject_id integer PRIMARY KEY, 
       subject VARCHAR(30), 
       location VARCHAR(40)
);

CREATE TABLE books (
       book_id integer PRIMARY KEY, 
       title VARCHAR(80) NOT NULL, 
       author_id integer,
       subject_id integer, 
       FOREIGN KEY (author_id) REFERENCES authors(author_id),
       FOREIGN KEY (subject_id) REFERENCES subjects(subject_id) 
);


CREATE TABLE editions ( 
       isbn VARCHAR(13) PRIMARY KEY, 
       book_id integer,
	edition integer, 
       publisher_id integer,
       publication VARCHAR(10),   -- publication date. Non-standard format.
       type character(1), -- hardcover or paperback 
       FOREIGN KEY(book_id) REFERENCES books(book_id),
       FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
);



CREATE TABLE stock ( 
       isbn VARCHAR(13),
       cost numeric(5,2),
       retail numeric(5,2), 
       stock integer,
       PRIMARY KEY (isbn),
       FOREIGN KEY(isbn) REFERENCES editions(isbn) 
);


INSERT INTO authors VALUES(16,'Alcott','Louisa May');
INSERT INTO authors VALUES(115,'Poe','Edgar Allen');
INSERT INTO authors VALUES(116,'Archer','Jeffery'); 
INSERT INTO authors VALUES(1111,'Denham','Ariel');
INSERT INTO authors VALUES(1212,'Worsley','John');
INSERT INTO authors VALUES(1213,'Brookins','Andrew');
INSERT INTO authors VALUES(1533,'Brautigan','Richard');
INSERT INTO authors VALUES(1644,'Hogarth','Burne');
INSERT INTO authors VALUES(1717,'Brite','Poppy Z.');
INSERT INTO authors VALUES(1809,'Seuss','Geisel');
INSERT INTO authors VALUES(1866,'Herbert','Frank');
INSERT INTO authors VALUES(2001,'Clarke','Arthur C.');
INSERT INTO authors VALUES(2031,'Brown','Margaret Wise');
INSERT INTO authors VALUES(2112,'Gorey','Edward');
INSERT INTO authors VALUES(4156,'King','Stephen');
INSERT INTO authors VALUES(4157,'King','Stephen'); 
INSERT INTO authors VALUES(7805,'Lutz','Mark');
INSERT INTO authors VALUES(7806,'Christiansen','Tom');
INSERT INTO authors VALUES(15990,'Bourgeois','Paulette');
INSERT INTO authors VALUES(25041,'Bianco','Margery Williams');


INSERT INTO subjects VALUES(0,'Arts','Creativity St');
INSERT INTO subjects VALUES(1,'Business','Productivity Ave');
INSERT INTO subjects VALUES(2,'Children''s Books','Kids Ct');
INSERT INTO subjects VALUES(3,'Classics','Academic Rd');
INSERT INTO subjects VALUES(4,'Computers','Productivity Ave');
INSERT INTO subjects VALUES(5,'Cooking','Creativity St');
INSERT INTO subjects VALUES(6,'Drama','Main St');
INSERT INTO subjects VALUES(7,'Entertainment','Main St');
INSERT INTO subjects VALUES(8,'History','Academic Rd');
INSERT INTO subjects VALUES(9,'Horror','Black Raven Dr');
INSERT INTO subjects VALUES(10,'Mystery','Black Raven Dr');
INSERT INTO subjects VALUES(11,'Poetry','Sunset Dr');
INSERT INTO subjects VALUES(12,'Religion','\N');
INSERT INTO subjects VALUES(13,'Romance','Main St');
INSERT INTO subjects VALUES(14,'Science','Productivity Ave');
INSERT INTO subjects VALUES(15,'Science Fiction','Main St');
INSERT INTO subjects VALUES(16,'Short Story','Main St');


INSERT INTO publishers VALUES(59,'Random House','Random House, Inc, 1540 Broadway, New York, NY 10036');
INSERT INTO publishers VALUES(62,'Watson-Guptill Publications','1515 Boradway, New York, NY 10036');
INSERT INTO publishers VALUES(65,'HarperCollins','HarperCollins Publishers, 10 E 53rd St, New York, NY 10022');
INSERT INTO publishers VALUES(75,'Doubleday','Random House, Inc, 1540 Broadway, New York, NY 10036');
INSERT INTO publishers VALUES(91,'Henry Holt & Company, Inc.',
       'Henry Holt & Company, Inc. 115 West 18th Street New York, NY 10011');
INSERT INTO publishers VALUES(99,'Ace Books','The Berkley Publishing Group, Penguin Putnam Inc, 375 Hudson St, New York, NY 10014');
INSERT INTO publishers VALUES(101,'Roc','Penguin Putnam Inc, 375 Hudson St, New York, NY 10014');
INSERT INTO publishers VALUES(102,'Penguin','Penguin Putnam Inc, 375 Hudson St, New York, NY 10014');
INSERT INTO publishers VALUES(105,'Noonday Press','Farrar Straus & Giroux Inc, 19 Union Square W, New York, NY 10003');
INSERT INTO publishers VALUES(113,'OReilly & Associates','OReilly & Associates, Inc. 101 Morris St, Sebastopol, CA 95472');
INSERT INTO publishers VALUES(150,'Kids Can Press','Kids Can Press, 29 Birch Ave. Toronto, ON, M4V1E2');
INSERT INTO publishers VALUES(163,'Mojo Press','Mojo Press, PO Box 1215, Dripping Springs, TX 78720');
INSERT INTO publishers VALUES(171,'Books of Wonder','Books of Wonder, 16 W. 18th St. New York, NY, 10011');

INSERT INTO books VALUES(156,'The Tell-Tale Heart',115,9);
INSERT INTO books VALUES(190,'Little Women',16,6);
INSERT INTO books VALUES(1000,'The Gold Bug',115,16);
INSERT INTO books VALUES(1234,'The Velvete Rabbit',25041,3);
INSERT INTO books VALUES(1501,'Goodnight Moon',2031,2);
INSERT INTO books VALUES(1590,'Bartholomew and the Oobleck',1809,2);
INSERT INTO books VALUES(1608,'The Cat in the Hat',1809,2);
INSERT INTO books VALUES(2038,'Dynamic Anatomy',1644,0);
INSERT INTO books VALUES(4267,'2001: A Space Odyssey',2001,15);
INSERT INTO books VALUES(4513,'Dune',1866,15);
INSERT INTO books VALUES(7808,'The Shining',4156,9);
INSERT INTO books VALUES(7809,'A shining short story',4157,16);
INSERT INTO books VALUES(25908,'Franklin in the Dark',15990,2);
INSERT INTO books VALUES(41472,'Practical PostgreSQL',1212,4);
INSERT INTO books VALUES(41473,'Programming Python',7805,4);
INSERT INTO books VALUES(41477,'Learning Python',4157,9);
INSERT INTO books VALUES(41478,'Perl Cookbook',7806,4);


INSERT INTO editions VALUES('039480001X',1608,1,59,'1957-03-01','h');
INSERT INTO editions VALUES('0451160916',7808,1,75,'1981-08-01','p');
INSERT INTO editions VALUES('0394800753',1590,1,59,'1949-03-01','p');
INSERT INTO editions VALUES('0590445065',25908,1,150,'1987-03-01','p');
INSERT INTO editions VALUES('0694003611',1501,1,65,'1947-03-04','p');
INSERT INTO editions VALUES('0679803335',1234,1,102,'1922-01-01','p');
INSERT INTO editions VALUES('0760720002',190,1,91,'1868-01-01','p');
INSERT INTO editions VALUES('0394900014',1608,1,59,'1957-01-01','p');
INSERT INTO editions VALUES('0385121679',7808,2,75,'1993-10-01','h');
INSERT INTO editions VALUES('1885418035',156,1,163,'1995-03-28','p');
INSERT INTO editions VALUES('0929605942',156,2,171,'1998-12-01','p');
INSERT INTO editions VALUES('0441172717',4513,2,99,'1998-09-01','p');
INSERT INTO editions VALUES('044100590X',4513,3,99,'1999-10-01','h');
INSERT INTO editions VALUES('0451457994',4267,3,101,'2000-09-12','p');
INSERT INTO editions VALUES('0451198492',4267,3,101,'1999-10-01','h');
INSERT INTO editions VALUES('0823015505',2038,1,62,'1958-01-01','p');
INSERT INTO editions VALUES('0596000855',41473,2,113,'2001-03-01','p');


INSERT INTO stock VALUES('0385121679',29,36.95,65);
INSERT INTO stock VALUES('039480001X',30,32.95,31);
INSERT INTO stock VALUES('0394900014',23,23.95,0);
INSERT INTO stock VALUES('044100590X',36,45.95,89);
INSERT INTO stock VALUES('0441172717',17,21.95,77);
INSERT INTO stock VALUES('0451160916',24,28.95,22);
INSERT INTO stock VALUES('0451198492',36,46.95,0);
INSERT INTO stock VALUES('0451457994',17,22.95,0);
INSERT INTO stock VALUES('0590445065',23,23.95,10);
INSERT INTO stock VALUES('0679803335',20,24.95,18);
INSERT INTO stock VALUES('0694003611',25,28.95,50);
INSERT INTO stock VALUES('0760720002',18,23.95,28);
INSERT INTO stock VALUES('0823015505',26,28.95,16);
INSERT INTO stock VALUES('0929605942',19,21.95,25);
INSERT INTO stock VALUES('1885418035',23,24.95,77);
INSERT INTO stock VALUES('0394800753',16,16.95,4);
