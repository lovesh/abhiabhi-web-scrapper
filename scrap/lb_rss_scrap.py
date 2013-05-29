from lxml import etree
import re
import MySQLdb
from time import time

start=time()
#text=open('./rss.xml','r').read()
tree=etree.parse('../rss.xml')

def getMobiles():
    data=[]
    brand_pattern=re.compile('\S+')
    name_color_pattern=re.compile('([^(]+)(\([A-Za-z ]+\))?')
    mobiles=[x.getparent() for x in tree.xpath('//item/category[text()="Mobiles"]')]
    for m in mobiles:
        mobile={}           #will contain the data that is to be inserted into DB
        td={}               #this will contain data about the current mobile. this is a temporary dict
        td=dict((x.tag,x.text) for x in m)
        title=td['title']
        if title.startswith('Sony Ericsson'):
            mobile['brand']='Sony Ericsson'
        else:
            mobile['brand']=brand_pattern.search(title).group()
        temp=name_color_pattern.search(title)
        if temp!=None:
            temp=temp.groups()
            mobile['name']=temp[0].strip()
            if temp[1]:
                mobile['color']=temp[1].strip('()')
            else:
                mobile['color']=''
            #mobile['price']=int(float(td['price']))
            mobile['price']=td['price'].split('.')[0]
            mobile['url']=td['link']
            mobile['features']=td['description']
            data.append(mobile)
        else:
            print td
            exit(1)
    return data

mobiles=getMobiles()
print 'parsing ends in '+str(time()-start)
conn=MySQLdb.connect(host="localhost",user="root",passwd="",db="scrape",unix_socket='/var/run/mysqld/mysqld.sock')
cur=conn.cursor()
query="insert into products(brand,name,color,price,url,site) values "
for m in mobiles:
    query=query+'("'+m['brand']+'","'+m['name']+'","'+m['color']+'",'+m['price']+',"'+m['url']+'",'+'2'+'), '
query=query.strip(', ')
#f=open('dump.sql','w')
#f.write(query)
start=time()
cur.execute(query)
conn.commit()
print 'insert done in '+str(time()-start)



