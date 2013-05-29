import lxml.html
import MySQLdb

def getBrands():
    html=lxml.html.parse('../Desktop/scraping/mobile-phones-c-254.html')
    anchors=html.xpath('//ul[@class="content_scroll"]/li/a')
    brands={}
    brands=dict((anchor.text_content().split('(')[0].lower(),anchor.get('href')) for anchor in anchors)
    return brands

def insertBrandsInDB():
    conn=MySQLdb.connect(host="localhost",user="root",passwd="",db="scrape",unix_socket='/var/run/mysqld/mysqld.sock')
    cur=conn.cursor()
    cur.execute('select name from brand')
    rows=cur.fetchall()
    brand_list=set(row[0] for row in rows)
    lb_brands=getBrands()
    #f=open('brands.sql','w')
    for lb in lb_brands:
        if lb in brand_list:
            query="update brand set lb_url='%s' where name='%s'" %(lb_brands[lb],lb)
            #f.write(query+'\n')
            cur.execute(query)
        else:
            query="insert into brand(lb_url,name) values ('%s','%s')" %(lb_brands[lb],lb)
            #f.write(query+'\n')
            cur.execute(query)
    conn.commit()
    #f.close()

insertBrandsInDB()




