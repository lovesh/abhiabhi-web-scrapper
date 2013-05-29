import os,sys,time

if __name__=='__main__':
    pid=os.fork()
    if pid>0:
        print "parent exiting"
        sys.exit(0)
    else:
        g=open('fork.txt','w')
        g.write('child pid is %s'%os.getpid())
        os.chdir("/home/lovesh/try/scrap")
        os.umask(0)
        os.setsid()
        #os.system('python /home/lovesh/try/qs.py')
        f=open('temp.txt','w')
        from booksadda_scrap import go
        go()
        from rescue import *
        tryy()
        urls=getSubcategoryUrls()
        for url in urls:
            f.write(url+'\n')
        f.close()
        g.close()
