from fk_books_scrap_directory import writeBookUrlsToTemporary,insertIntoDB
from fk_books_scrap import getBookUrlsOfCategory

#DBName='abhiabhi'
#temporary=pymongo.Connection().DBName.fk_temporary

new_books_urls=['http://www.flipkart.com/view-books/0/new-releases','http://www.flipkart.com/view-books/1/bestsellers']


def go():
    #for url in new_books_urls:
     #   book_urls=getBookUrlsOfCategory(url)
      #  writeBookUrlsToTemporary(book_urls)
    insertIntoDB()


if __name__ == '__main__':
    go()
