import pymysql
import time
import os

from pymysql import MySQLError

def format_date_value(v):
    if v != "NULL" or '':
        obj = time.strptime(v, "%B %d, %Y")
        year = obj.tm_year
        month = obj.tm_mon
        day = obj.tm_mday
        date_value = str(year) + "-" + str(month) + "-" + str(day)
    else:
        date_value = None
    return date_value

def main():
    tone = time.time()
    count = 0

        # Directory where metadata flies are located
    rootdir = "c:/Users/arpie/Documents/CIA-test/meta"
    tone = time.time()
    count = 0

    con = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user='root',
        password='Half2_japanese',
        db='cia',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cur = con.cursor()
    cur.execute('truncate table metadata;')

    # walk through each subdirectory
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            try:
                vals = []

                with open(os.path.join(subdir, file), encoding='utf8', errors='ignore') as f:
                    for lines in f:
                        lines = lines.replace('\n', '')
                        lines = lines.replace('(', '')
                        lines = lines.replace(')', '')
                        directory = subdir.replace(rootdir, "")
                        vals.append(lines)

                # formatting doc_release date
                vals[7] = format_date_value(vals[7])
                #print(vals[7])

                # formatting doc_creation date
                vals[8] = format_date_value(vals[8])

                # formatting publication date
                vals[11] = format_date_value(vals[11])

                vals.insert(0, directory)

                # makes sure only inserting 14 values
                t_vals = vals[:14]
                if len(vals)>14:
                    print(subdir, file, len(vals))

                with con:
                    cur = con.cursor()
                    #cur.execute('truncate table metadata;')
                    cur.execute(
                        "insert into metadata (folder, title, doctype, collection, id, "
                        "release_decis, origclass, pages, docrelease, doccreation, sequenceno, "
                        "caseno, pubdate, contenttype) values "
                        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", t_vals)
             # if there is a SQL error
            except MySQLError as e:
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
                count += 1
                pass

                # if there is a Value Error
            except ValueError as e:
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
                count += 1
                pass

    ttwo = time.time()

    print('Files not loaded: %d' % (count))
    print('Runtime is: %d' % (ttwo - tone))


if __name__ == '__main__':
    main()