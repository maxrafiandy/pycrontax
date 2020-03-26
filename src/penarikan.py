from datetime import datetime
import src.config as config
import pymssql

INSERT = """
    insert into tbl_mirror_penarikan
    (nopd, nama_objek_pajak, kode_sudin,
    trx1, omset1, trx2, omset2, trx3, omset3,
    setoran, masapajak, tahunpajak, created_at, updated_at)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, GETDATE(), GETDATE())
"""

def getQuery(month, year):
    return f"""select nop.nopd,
        nop.nama_objek_usaha as nama_objek_pajak,
        nop.kode_sudin,
        coalesce(sum.sum_trx,0) as trx1,
        coalesce(sum.sum_omset,0) as omset1,
        coalesce(count(adj.nopd),0) as trx2,
        coalesce(sum(adj.amount_adjust),0) as omset2,
        coalesce(sum_pod.sum_trx,0) as trx3,
        coalesce(sum_pod.sum_omset,0) as omset3,
        coalesce(sum(riil.setoran_pajak),0) as setoran,
        {month} as masapajak,
        {year} as tahunpajak
        from master_pajak_online_dki.dbo.tbl_nopd as nop
        left join pajak_online_dki_{year}{month}.dbo.tbl_sum_nopd as sum on sum.nopd = nop.nopd
        left join pajak_online_dki_{year}{month}.dbo.tbl_adjust_manual as adj on adj.nopd = nop.nopd
        left join pajak_online_dki_{year}{month}.dbo.tbl_sum_nopd as sum_pod on sum_pod.nopd = nop.nopd
        left join pajak_online_dki.dbo.tbl_fact_pembayaran_riil as riil on riil.nopd = nop.nopd
        where nop.nama_objek_usaha not like '%TUTUP%'
        group by nop.nopd, nop.nama_objek_usaha, nop.kode_sudin, sum.sum_trx, sum.sum_omset, sum_pod.sum_trx, sum_pod.sum_omset
        order by sum_pod.sum_omset desc
    """

def penarikan():
    today = datetime.today()
    month = '%02d' % (12+today.month if today.month-1 < 1 else today.month-1)
    year = '%d' % (today.year-1 if today.month-1 < 1 else today.year)
    conn = pymssql.connect(MSSQL_HOST,MSSQL_USER,MSSQL_PWD,MSSQL_DB)
    cursor = conn.cursor()
    
    try:
        cursor.execute(getQuery(month,year))
        penarikan = cursor.fetchall()
        cursor.execute("truncate table tbl_mirror_penarikan")
        cursor.executemany(INSERT, penarikan)
        conn.commit()
        config.loginfo("Success! Table tbl_mirror_penarikan has been mirrored.")
    
    except Exception as err:
        conn.rollback()
        config.logexception(err)
    
    finally:
        conn.close()
