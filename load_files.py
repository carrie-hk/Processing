import pymysql
import time
import os
import db_utils as dbutils

con = pymysql.connect(
    host='127.0.0.1',
    user='dbuser',
    password='dbuserdbuser',
    db='general-cia-test',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def insert_docs():

    tone = time.time()

    # for docno in range(1, 25):
    #
    #     docno = str(docno)
    #
    #     raw_ocr_path = "/Users/carriehaykellar/Desktop/HistoryLab/test-set/"
    #     raw_ocr = ""
    #
    #     with open(raw_ocr_path + "CIA" + docno + ".txt", encoding='utf8',
    #               errors='ignore') as f:
    #         for lines in f:
    #             raw_ocr = raw_ocr + lines
    #
    #     raw_ocr = raw_ocr.replace("'", "''")
    #
    #     checked_ocr_path = "/Users/carriehaykellar/Desktop/HistoryLab/Spell-Checked/"
    #     checked_ocr = ""
    #     with open(checked_ocr_path + "SC_CIA" + docno + ".txt", encoding='utf8',
    #               errors='ignore') as f:
    #         for lines in f:
    #             checked_ocr = checked_ocr + lines
    #
    #     checked_ocr = checked_ocr.replace("'", "''")
    #
    #     url = "https://www.cia.gov/library/readingroom/document/" + docno
    #
    #
    #
    #     sql = "insert into docs (docID, raw_doc, sc_doc, url) values ('" + \
    #           docno + "', '" + raw_ocr +"', '" + checked_ocr + "', '" + url + " ')"
    #
    #     try:
    #         dbutils.run_q(sql, conn=con)
    #     except:
    #         print("already inserted")
    #
    #

    rootdir = "/Users/carriehaykellar/Desktop/HistoryLab/meta/"

    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            vals = []

            with open(os.path.join(subdir, file), encoding='utf8', errors='ignore') as f:
                for lines in f:
                    vals.append(lines)

            dir = subdir.replace(rootdir, "")
            vals.insert(0, dir)
            tvals = tuple(c for c in vals)

            sql_insert_meta = "insert into general-cia-test.metadata (folder, title, doctype, collection, docID, release_decis, origclass, pages, " \
                              "docrelease, doccreation, sequenceno, caseno, pubdate, contenttype) values " + str(tvals)
            try:
                dbutils.run_q(sql_insert_meta, conn=con)
            except:
                print("already inserted")

    ttwo = time.time()

    print( ttwo - tone)

insert_docs()





