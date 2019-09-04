#!/usr/bin/env python
from datetime import datetime
from setting import *
import pymssql

query = """
    SELECT
        nop.nopd,
        nop.npwpd,
        nop.nama_objek_usaha,
        npd.nama_wajib_pajak,
        nop.alamat,
        nop.kode_sudin,
        case nop.kode_jenis_usaha
          when 'R' then 'RESTO'
          when 'H' then 'HIBURAN'
          when 'T' then 'HOTEL'
          when 'P' then 'PARKIR'
        end as jenis_usaha,
        case nop.status_live
          when '4' then 'TERPASANG'
          when '5' then 'TERKONEKSI'
          when '6' then 'AUTODEBET'
        end as jenis_penarikan,
        kwp.ipaddress,
        kwp.keterangan,
        case kwp.status
          when 1 then 'ONLINE'
          else 'OFFLINE'
        end as status,
        case kwp.status
          when 1 then getdate()
          else null
        end as last_availability,
        coalesce(
          rby.tanggal_trx,
          null) as last_reliable
        from master_pajak_online_dki.dbo.tbl_nopd as nop
        left join monitoring_new.dbo.koordinat_wp as kwp on kwp.nopd = nop.nopd
        left join (select nopd, max(LASTUPDATE_TODAY) tanggal_trx from PAJAK_ONLINE_DKI.dbo.TBL_SUM_NOPD group by nopd)
        rby on rby.nopd = nop.nopd
        left join master_pajak_online_dki.dbo.tbl_npwpd as npd on npd.npwpd = nop.npwpd
        where nop.status_live in (4,5,6)
        and UPPER(nop.nama_objek_usaha) NOT LIKE '%TUTUP%'
        and kwp.status != 1
        group by nop.nopd, nop.npwpd, nop.nama_objek_usaha, npd.nama_wajib_pajak,
        nop.alamat, nop.kode_sudin, nop.kode_jenis_usaha,
        nop.status_live, kwp.ipaddress,
        kwp.keterangan, kwp.status, rby.tanggal_trx, kwp.last_status
"""

insert = """
    insert into tbl_mirror_history_offline
    (nopd, npwpd, nama_objek_usaha, nama_wajib_pajak, alamat,
    kode_sudin, jenis_usaha, jenis_penarikan, ipaddress,
    keterangan, status, last_availability, last_reliable,
    created_at, updated_at) values
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    GETDATE(), GETDATE())
"""

def offline():
    conn = pymssql.connect(MSSQL_HOST, MSSQL_USER, MSSQL_PWD, MSSQL_DB)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        offline_devices = cursor.fetchall()
        cursor.execute("truncate table tbl_mirror_history_offline")
        cursor.executemany(insert, offline_devices)
        conn.commit()
        print "Success! Table tbl_mirror_history_offline has been mirrored."
    except Exception as e:
        conn.rollback()
        print("Error => : %s" % e)
    finally:
        conn.close()
