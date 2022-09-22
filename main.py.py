import argparse, os, logging

from lxml import etree
from pathlib import Path
from datetime import datetime

import mysql.connector


# Answer table: post_id, answer_body, votes, creation , modified, is_accepted
# Question table: post_id, question title, tags, view count, votes, question_body, creation

def transform_column(column):
    _column = column
    if str(column) == "False":
        _column = 0
    elif str(column) == "True":
        _column = 1
    else:
        _column = column.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return _column


def processing(input, columns):
    mydb = mysql.connector.connect(
        host="localhost",  # IP address
        user="root",  # username
        password="admin"  # password
    )

    my_cursor = mydb.cursor(buffered=True)
    my_cursor.execute("DROP DATABASE IF EXISTS stackoverflow")
    my_cursor.execute("CREATE DATABASE stackoverflow")
    my_cursor.execute("USE stackoverflow")

    my_cursor.execute("DROP TABLE IF EXISTS Questions")
    my_cursor.execute("DROP TABLE IF EXISTS Answers")

    questions = """CREATE TABLE Questions (
                Id int NOT null,
                PostTypeId tinyint NULL,
                AcceptedAnswerId int NULL,
                CreationDate datetime NOT NULL,
                ViewCount int NULL,
                Body TEXT NULL,
                Title nvarchar(255) NULL,
                Tags nvarchar(255) NULL
                );
                """
    answers = """CREATE TABLE Answers (
                    Id int NOT null,
                    PostTypeId tinyint NULL,
                    IsAccepted tinyint NULL,
                    CreationDate datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    LastEditDate datetime NULL,
                    Body TEXT NULL
                    );
                    """
    my_cursor.execute(questions)
    my_cursor.execute(answers)

    insert_questions = "INSERT INTO Questions (Id, PostTypeId, AcceptedAnswerId, CreationDate, ViewCount, Body, Title, " \
                       "Tags) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
    insert_answers = "INSERT INTO Answers (Id, PostTypeId, CreationDate, LastEditDate, Body, " \
                     "IsAccepted) VALUES (%s, %s, %s, %s, %s, %s) "
    check_answers = "SELECT * FROM Answers WHERE id = %(id)s"

    context = etree.iterparse(input, events=('end',), tag="row")
    row_counter = 0

    for action, elem in context:
        row_counter += 1
        print(row_counter)
        row = [transform_column(elem.attrib[column]) if column in elem.attrib else None for column in columns]
        if row[1] == '1':  # Question table
            #print("Questions")
            #print(row[6])
            val = (row[0], row[1], row[2], row[4], row[5], row[6], row[8], row[9])
            my_cursor.execute(insert_questions, val)
            if row[2] == None:
                continue
            my_cursor.execute(check_answers, { 'id': row[2] })
            row_count = my_cursor.rowcount
            if row_count == 0:
                my_cursor.execute("INSERT INTO Answers (Id, IsAccepted, PostTypeId) VALUES (%s, %s, "
                                  "%s)", (row[2], 1, 2))
            elif row_count == 1:
                my_cursor.execute("UPDATE Answers SET IsAccepted = %s WHERE Id = %s", (1, row[2]))
            mydb.commit()
        elif row[1] == '2':  # Answer table
            my_cursor.execute(check_answers, { 'id': row[0] })
            row_count = my_cursor.rowcount
            if row_count == 0:
                val = (row[0], row[1], row[4], row[7], row[6], '0')
                my_cursor.execute(insert_answers, val)
            elif row_count == 1:
                my_cursor.execute("UPDATE Answers SET CreationDate= %s, LastEditDate= %s, Body= %s "
                                  "WHERE Id = %s", (row[4], row[7], row[6], row[0]))
            mydb.commit()
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        #if row_counter == 10000:
            #break
    print("DONE")
    mydb.close()


if __name__ == '__main__':
    columns = ["Id", "PostTypeId", "AcceptedAnswerId", "ParentId", "CreationDate", "ViewCount", "Body",
               "LastEditDate", "Title", "Tags", "ContentLicense"]
    processing("Posts.xml", columns)
