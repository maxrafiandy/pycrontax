from datetime import datetime
import src.config
import pymssql

QUERY = """
    select
        nop.nopd,
        nop.nama_objek_usaha as nama_objek_pajak,
        nop.kode_sudin,
        coalesce(sum1.sum_trx,0) as trx1,
        coalesce(sum1.sum_omset,0) as omset1,
        coalesce(sum1.sum_pajak,0) as pajak1,
        coalesce(sum2.sum_trx,0) as trx2,
        coalesce(sum2.sum_omset,0) as omset2,
        coalesce(sum2.sum_pajak,0) as pajak2,
        coalesce(sum3.sum_trx,0) as trx3,
        coalesce(sum3.sum_omset,0) as omset3,
        coalesce(sum3.sum_pajak,0) as pajak3
    from master_pajak_online_dki.dbo.tbl_nopd as nop
    left join pajak_online_dki_{year_month_0}.dbo.tbl_sum_nopd as sum1
        on sum1.nopd=nop.nopd
    left join pajak_online_dki_{year_month_1}.dbo.tbl_sum_nopd as sum2
        on sum2.nopd=nop.nopd
    left join pajak_online_dki.dbo.tbl_sum_nopd as sum3
        on sum3.nopd=nop.nopd
    where upper(nop.nama_objek_usaha) not like '%TUTUP%'
    order by sum3.sum_trx desc 
"""

INSERT = """
    insert into tbl_mirror_progres_omset
    (nopd, nama_objek_pajak, kode_sudin,
    trx1, omset1, pajak1, trx2, omset2, pajak2,
    trx3, omset3, pajak3, masapajak,
    tahunpajak, created_at, updated_at)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, {month}, {year}, GETDATE(), GETDATE())
"""

def getQuery(months, years):
    return QUERY.format(year_month_0='%s%s' % (years[0],months[0]),
        year_month_1='%s%s' % (years[1],months[1]))

def getInsert(month, year):
    return INSERT.format(month=month, year=year)

def pgomset():
    months = []
    years = []
    collections = [2,1]
    today = datetime.today()
    
    for i in collections:
        year = today.year
        month = today.month-i
        if month < 1:
            month = 12 + month
            year = year - 1
        months.append('%02d' % month)
        years.append('%d' % year)
    conn = pymssql.connect(MSSQL_HOST,MSSQL_USER,MSSQL_PWD,MSSQL_DB)
    cursor = conn.cursor()
    
    try:
        cursor.execute(getQuery(months,years))
        progres = cursor.fetchall()
        cursor.execute("truncate table tbl_mirror_progres_omset")
        cursor.executemany(getInsert(months[1], years[1]), progres)
        conn.commit()
        config.loginfo("Success! Table tbl_mirror_progres_omset has been mirrored.")
    
    except Exception as err:
        conn.rollback()
        config.logexception(err)

    finally:
        conn.close()
