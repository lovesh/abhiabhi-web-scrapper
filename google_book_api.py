import simplejson as json
import urllib

class GoogleBookApi:
    def __init__(self,title=None,author=None,isbn=None,url=None,string=None):
        
        self.api_base_url='https://www.googleapis.com/books/v1/volumes?q='
        api_cur_url=self.api_base_url
        self.result={'books':[]}

        if title is not None:
            title=title.replace(' ','+')
            api_cur_url=self.api_base_url+'intitle:'+title

        if author is not None:
            author=author.replace(' ','+')
            if api_cur_url[-1]!='=':
                api_cur_url+='+inauthor:'+author
            else:
                api_cur_url=self.api_base_url+'inauthor:'+author
        
        if isbn is not None:
            if api_cur_url[-1]!='=':
                api_cur_url+='+isbn:'+isbn
            else:
                api_cur_url=self.api_base_url+'isbn:'+isbn
        
        if url:
            api_cur_url=url
            self.url=api_cur_url
            self.api_response=urllib.urlopen(self.url).read()

        if string:
            self.api_response=string

    def parseResponse(self,books_limit=None):

        json_res=json.loads(self.api_response)
            
        self.result['total']=json_res['totalItems']

        if books_limit:
            limit=books_limit
        else:
            limit=self.result['total']

        books=[]
        for c in range(0,limit):
            book={}
            item=json_res['items'][c]
            bookinfo=item['volumeInfo']
            if 'authors' in bookinfo:
                book['authors']=bookinfo['authors']
            if 'title' in bookinfo:
                book['title']=bookinfo['title']
            if 'subtitle' in bookinfo:
                book['subtitle']=bookinfo['subtitle']
            if 'industryIdentifiers' in bookinfo:
                for identifier in bookinfo['industryIdentifiers']:
                    if identifier['type']=='ISBN_10':
                        book['isbn']=identifier['identifier']
                    if identifier['type']=='ISBN_13':
                        book['isbn13']=identifier['identifier']
            books.append(book)
        
        self.result['books']=books
        return books
    
    def getBooks(self,limit=1):
        if len(self.result['books'])>=limit:
            return self.result['books'][:limit]
        else:
            return self.parseResponse(books_limit=limit)

    




        



    



