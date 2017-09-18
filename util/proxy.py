#-*- coding: utf-8 -*-

import urllib
import urllib2
import re
import time
import socket
import random
# obtain some ip and port for spider from a site,xicidaili.com.
class ObtainProxy:

    def __init__(self,region = '国内普通'):

        self.region = {'国内普通':'nt/','国内高匿':'nn/','国外普通':'wt/','国外高匿':'wn/','SOCKS':'qq/'}

        self.url = 'http://www.xicidaili.com/' + self.region[region]
        self.header = {}
        self.header['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36'

    def get_prpxy(self):

        req = urllib2.Request(self.url,headers = self.header)
        resp = urllib2.urlopen(req)
        content = resp.read()

        self.get_ip = re.findall(r'(\d+\.\d+\.\d+\.\d+)</td>\s*<td>(\d+)</td>',content)

        self.pro_list = []
        for each in self.get_ip:
            a_info = each[0] + ':' + each[1]
            self.pro_list.append(a_info)

        return self.pro_list

    def save_pro_info(self):
        with open('./proxy.txt','w') as f:
            for each in self.get_ip:
                a_info = each[0] + ':' + each[1] + '\n'
                f.writelines(a_info) 
                 
    def save_valid_pro_info(self):
        with open('./proxy.txt','w') as f:
            for each in self.valid_pro_list:
                f.writelines(each+'\n') 

    def getValidProxy(self):
        self.valid_pro_list = []
        socket.setdefaulttimeout(3)
        url = "http://ip.chinaz.com/getip.aspx"
        for i in range(0,len(self.pro_list)):
            proxy_host = "http://"+self.pro_list[i]
            proxy = {"http":proxy_host}
            try:
                res = urllib.urlopen(url,proxies=proxy).read()
                self.valid_pro_list.append(self.pro_list[i])
                print res
            except Exception,e:
                print proxy
                print e
                continue
            
        return self.valid_pro_list

    def setProxy(self):
        f = open("./proxy.txt")
        lines = f.readlines()
        proxies = []
        for i in range(0,len(lines)):
            proxy_ip_port = lines[i].strip("\n")
            proxy_host = "http://"+proxy_ip_port
            proxy = {"http":proxy_host}
            proxies.append(proxy)
        random_proxy = random.choice(proxies)  
        #下面是使用ip代理进行访问  
        proxy_support = urllib2.ProxyHandler(random_proxy)  
        opener = urllib2.build_opener(proxy_support)  
        urllib2.install_opener(opener) 


if __name__ == '__main__':
    proxy = ObtainProxy()
    # proxy.get_prpxy()
    # # proxy.save_pro_info()
    # proxy.getValidProxy()
    # proxy.save_valid_pro_info()

    # random_proxy = random.choice(proxies)  
    # #下面是使用ip代理进行访问  
    # proxy_support = urllib2.ProxyHandler(random_proxy)  
    # opener = urllib2.build_opener(proxy_support)  
    # urllib2.install_opener(opener) 
    proxy.setProxy()
