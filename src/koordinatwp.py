#!/usr/bin/env python
from setting import *
import pymssql

insertQuery = """
    INSERT INTO koordinat_wp(nopd, npwpd, nama_objek_usaha, alamat, latitude, longitude,
    kode_sudin, status, status_live, ipaddress, last_status, keterangan)
    SELECT n.nopd, n.npwpd, n.nama_objek_usaha, n.alamat, coalesce(g.latitude, '0.00') latitude,
    coalesce(g.longitude, '0.00') longitude, n.kode_sudin,
    case
        when s.tanggal_trx is null then 0
        when s.tanggal_trx < DATEADD(day, -2, GETDATE()) then 0
        else 1
    end status,
    n.status_live,
    coalesce(g.iphost, 'N/A') ipaddress, GETDATE() last_status, null keterangan
    from MASTER_PAJAK_ONLINE_DKI.dbo.TBL_NOPD n
    left join koordinat_wp k on k.nopd = n.nopd
    left join (
        select NOPD, IPHOST, case longitude
        when '0.00' then null
        else longitude
        end latitude,
        case latitude
        when '0.00' then null
        else latitude
        end longitude
        from PAJAKGRABBER_RESTO.dbo.masterinstitusi
        where active = 'Y' union

        select NOPD, IPHOST, case longitude
        when '0.00' then null
        else longitude
        end latitude,
        case latitude
        when '0.00' then null
        else latitude
        end longitude
        from PAJAKGRABBER_PARKIR.dbo.masterinstitusi
        where active = 'Y' union

        select NOPD, IPHOST, case longitude
        when '0.00' then null
        else longitude
        end latitude,
        case latitude
        when '0.00' then null
        else latitude
        end longitude
        from PAJAKGRABBER_HIBURAN.dbo.masterinstitusi
        where active = 'Y'
    ) g on g.nopd = n.nopd
    left join (select nopd, max(LASTUPDATE_TODAY) tanggal_trx from PAJAK_ONLINE_DKI.dbo.TBL_SUM_NOPD group by nopd) s
    on s.nopd = n.nopd
    WHERE UPPER(n.nama_objek_usaha) NOT LIKE '%TUTUP%'
    AND n.status_live in (4,5,6)
    AND k.nopd IS NULL
"""
updateQuery = """
    update os set os.status = ns.status
    from koordinat_wp os
    join (
    	select k.nopd,
    	case
    	   when s.LASTUPDATE_TODAY is null then 0
    	   when s.LASTUPDATE_TODAY < DATEADD(day, -2, GETDATE()) then 0
    	   else 1
    	end status
    	from koordinat_wp k
    	left join PAJAK_ONLINE_DKI.dbo.TBL_SUM_NOPD s on s.nopd = k.nopd
    ) ns on ns.nopd = os.nopd
"""
# mirror table koordinatwp
def koordinatwp():
    conn = pymssql.connect(MSSQL_HOST,MSSQL_USER,MSSQL_PWD,MSSQL_DB)
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute("truncate table koordinat_wp")
        cursor.execute(insertQuery)
        conn.commit()
        print "Success! Table koordinat_wp has been mirrored."
    except Exception as e:
        conn.rollback()
        log_error(e)
    finally:
        conn.close()

# update status online koordinat_wp
def updateKoordinatWp():
    conn = pymssql.connect(MSSQL_HOST,MSSQL_USER,MSSQL_PWD,MSSQL_DB)
    cursor = conn.cursor()
    try:
        cursor.execute(updateQuery)
        conn.commit()
        print "Success! Table koordinat_wp has been updated."
    except Exception as e:
        conn.rollback()
        log_error(e)
    finally:
        conn.close()
