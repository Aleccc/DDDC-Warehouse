from datawarehouse.common import Connection

DRIVER = '{SQL Server}'
SERVER = 'GSDWPROD.gassouth.com'
DATABASE = 'GSDWSTG'

db = Connection(DRIVER, SERVER, DATABASE)

if __name__ == "__main__":
    sql = ("SELECT top 10 * "
           "from FM_MM_AGL_CD_2018 "
           "where sourcefilename=? "
           "ORDER BY newid() "
           )
    df = db.query(sql, 'AGL_CD_201812')
    db.close()
