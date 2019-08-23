#!/usr/bin/env python
from datetime import datetime
from mysql import connector
from setting import *
import pymssql

insert = """
	insert into tbl_mirror_autodebet
	(nopd, status, autodebet_1, autodebet_2, autodebet_3,
	masapajak, tahunpajak, created_at, updated_at)
	values (%s, %d, %s, %s, %s, %s, %d, %s, %s)
"""

def getQuery(months, years):
	query = """SELECT *, {months_2} masapajak, {years_2} tahunpajak, NOW(), NOW()
	    FROM (
	      (
	        SELECT master.*,
	        3 status,
	        COALESCE(sukses1.transactionamount, 0) autodebet_1,
	        COALESCE(sukses2.transactionamount, 0) autodebet_2,
	        COALESCE(sukses3.transactionamount, 0) autodebet_3
	        FROM
	        (
	          SELECT nop
	          FROM mpd_sspd
	          WHERE nop is not null
	          GROUP BY nop
	        ) master
	        -- sukses 3 bulan terakhir
	        left join (
	          SELECT nop, status, sum(transactionamount) as transactionamount
	          FROM mpd_sspd
	          WHERE nop is not null
	          AND STATUS = 3
	          AND masapajak = {months_0}
	          AND tahunpajak = {years_0}
	          GROUP BY nop, status
	        ) sukses1 on sukses1.nop = master.nop
	        -- sukses 2 bulan terakhir
	        left join (
	          SELECT nop, status, sum(transactionamount) as transactionamount
	          FROM mpd_sspd
	          WHERE nop is not null
	          AND STATUS = 3
	          AND masapajak = {months_1}
	          AND tahunpajak = {years_1}
	          GROUP BY nop
	        ) sukses2 on sukses2.nop = master.nop
	        -- sukses 1 bulan terakhir
	        left join (
	          SELECT nop, status, sum(transactionamount) as transactionamount
	          FROM mpd_sspd
	          WHERE nop is not null
	          AND STATUS = 3
	          AND masapajak = {months_2}
	          AND tahunpajak = {years_2}
	          GROUP BY nop
	        ) sukses3 on sukses3.nop = master.nop
	      )
	      UNION
	      (
	        SELECT master.*,
	        4 status,
	        COALESCE(sukses1.transactionamount, 0) autodebet_1,
	        COALESCE(sukses2.transactionamount, 0) autodebet_2,
	        COALESCE(sukses3.transactionamount, 0) autodebet_3
	        FROM
	        (
	          SELECT nop
	          FROM mpd_sspd
	          WHERE nop is not null
	          GROUP BY nop
	        ) master
	        -- sukses 3 bulan terakhir
	        left join (
	          SELECT nop, 4 status, sum(transactionamount) as transactionamount
	          FROM mpd_sspd
	          WHERE nop is not null
	          AND STATUS != 3
	          AND masapajak = {months_0}
	          AND tahunpajak = {years_0}
	          GROUP BY nop, status
	        ) sukses1 on sukses1.nop = master.nop
	        -- sukses 2 bulan terakhir
	        left join (
	          SELECT nop, 4 status, sum(transactionamount) as transactionamount
	          FROM mpd_sspd
	          WHERE nop is not null
	          AND STATUS != 3
	          AND masapajak = {months_1}
	          AND tahunpajak = {years_1}
	          GROUP BY nop
	        ) sukses2 on sukses2.nop = master.nop
	        -- sukses 1 bulan terakhir
	        left join (
	          SELECT nop, 4 status, sum(transactionamount) as transactionamount
	          FROM mpd_sspd
	          WHERE nop is not null
	          AND STATUS != 3
	          AND masapajak = {months_2}
	          AND tahunpajak = {years_2}
	          GROUP BY nop
	        ) sukses3 on sukses3.nop = master.nop
	      )
	    ) mpd_sspd
	    WHERE autodebet_1 != 0
	    AND autodebet_2 != 0
	    -- AND autodebet_3 != 0
	"""
	return query.format(months_0 = months[0], months_1=months[1], months_2=months[2],
		years_0=years[0], years_1=years[1], years_2=years[2])

def autodebet():
	months = []
	years = []
	collections = [3,2,1]
	today = datetime.today()
	for i in collections:
		year = today.year
		month = today.month-i
		if month < 1:
			month = 12 + month
			year = year - 1
		months.append('%02d' % month)
		years.append('%d' % year)
	mysql_conn = connector.connect(host=MYSQL_HOST,user=MYSQL_USER,passwd=MYSQL_PWD,database=MYSQL_DB)
	mssql_conn = pymssql.connect(MSSQL_HOST,MSSQL_USER,MSSQL_PWD,MSSQL_DB)

	mysql_cursor = mysql_conn.cursor()
	mssql_cursor = mssql_conn.cursor()

	try:
		query = getQuery(months, years)
		mysql_cursor.execute(query)
		autodebets = mysql_cursor.fetchall()
		mssql_cursor.execute("truncate table tbl_mirror_autodebet")
		mssql_cursor.executemany(insert, autodebets)
		mssql_conn.commit()
		print "Success! Table tbl_mirror_autodebet has been mirrored."
	except Exception as e:
		mssql_conn.rollback()
		log_error(e)
	finally:
		mysql_conn.close()
		mssql_conn.close()
