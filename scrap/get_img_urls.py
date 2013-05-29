import pymongo,urllib,lxml.html,downloader

siteurl='http://www.bookadda.com/'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})

con=pymongo.Connection()
bks=con.abhiabhi.scraped_books
img_coll=con.DBName.ba_imgs
counter=0
total=0
urls=[]
docs=bks.find({'img_url':{'$exists':False},'site':'bookadda'},{'_id':1,'url':1},timeout=False)
for doc in docs:
    urls.append(doc['url'])
    counter+=1
    if counter==500:
        dl.putUrls(urls,30)
        result=dl.download()
        images=[]
        for r in result:
            status=str(result[r][0])
            html=result[r][1]
            if int(status) > 199 and int(status) < 400:
                page=lxml.html.document_fromstring(html)
                img=page.xpath('//meta[@property="og:image"]')
                if len(img)>0:
                    img=img[0].get('content').strip()
                    url=page.xpath('//meta[@property="og:url"]')
                    if len(url)>0:
                        url=url[0].get('content').strip()
                        images.append({'page_url':url,'img_url':img})
                        total+=1
        img_coll.insert(images,safe=True)
        counter=0
        urls=[]
        print "total is %d"%total
    
    
