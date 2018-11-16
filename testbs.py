# -*- coding: utf-8 -*-  

import requests,io,sys,time
from bs4 import BeautifulSoup
import json,urllib
import cx_Oracle
from lxml import etree
import threading
from queue import Queue

def conndb():
    username="python"
    userpwd="python"
    host="127.0.0.1"
    port=1521
    dbname="python"    
    dsn=cx_Oracle.makedsn(host, port, dbname)
    db=cx_Oracle.connect(username, userpwd, dsn) 
    return db

def ExecDB(sql):
    db=conndb()
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
    #return result

def GetJson(url):
    payload = {}
    headers = {'content-type': 'application/json'}
    ret = requests.post(url, data=json.dumps(payload), headers=headers)
    ret.encoding = 'utf-8'
    ret = ret.text
    text = json.loads(ret)
    return text

def getHtml(url):  
    page=urllib.request.urlopen(url)  
    html=page.read().decode(encoding='utf-8',errors='strict')
    page.close()
    return html

def ListTemp(lists):
    if lists:
        member = lists[0].xpath('string(.)').replace('\n','').replace(' ','').replace(' ','')
    else :
        member = 'None'
    return member

def Getcontent(url):
    text2 = GetJson(url)      
    content = text2['content']
    return content

def Gettxt(text):
    txt=' '.join(text.split())
    return txt

def Trysql(sql):
    try:
        sql = ExecDB(sql.encode("GB18030"))
    except:
        print("sql:",sql)
        pass
    return sql

def GetQ():
    global q
    url = 'http://gs.amac.org.cn/amac-infodisc/api/pof/fund?rand=0.662701102726849&page=0&size=100'
    q = Queue()
    text = GetJson(url)
    n = text['totalPages']
    for i in range(n):
        url2 = 'http://gs.amac.org.cn/amac-infodisc/api/pof/fund?rand=0.662701102726849&page='+str(i)+'&size=100'
        q.put(url2)
    return q

def Thread():
    threads=[]
    for code in range(2):
       thread=threading.Thread(target=Work)
       thread.setDaemom=(True)
       threads.append(thread)
    for t in threads:
       t.setDaemon(True)
       t.start()  #启动一个线程
    for t in threads:
       t.join()  #等待每个线程

def Work():
    while not q.empty():
        try :
          url3 = q.get()
          content = Getcontent(url3)
          for x in range(100):
            dicts = content[x]
            gsid = str(dicts['id'])
            fundno = str(dicts['fundNo'])
            fundname = str(dicts['fundName']).replace('\'','`')
            managername = str(dicts['managerName']).replace('\'','`')
            managertype = str(dicts['managerType'])
            workingstate = str(dicts['workingState'])
            putonrecorddate = str(dicts['putOnRecordDate']).replace('None','0')
            lastquarterupdate = str(dicts['lastQuarterUpdate'])
            isdeputemanage = str(dicts['isDeputeManage'])
            fundurl = str('http://gs.amac.org.cn/amac-infodisc/res/pof/fund/'+str(dicts['url']))
            establishdate = str(dicts['establishDate']).replace('None','0')
            managerurl = str('http://gs.amac.org.cn/amac-infodisc/res/pof'+str(dicts['managerUrl']).replace('..',''))
            mandatorname = str(dicts['mandatorName'])
            sql = "merge into amac_smjj a\n"\
                  "using(select '"+gsid+"' id,'"+fundno+"' fundno,'"+fundname+"' fundname,'"\
                  +managername+"' managername,'"+managertype+"' managertype,'"+workingstate+"' workingstate,"\
                  +"to_char(inttotime("+putonrecorddate+"/1000),'yyyymmdd') putonrecorddate,'"+lastquarterupdate\
                  +"' lastquarterupdate,'"+isdeputemanage+"' isdeputemanage,'"+fundurl+"' url,"\
                  +"to_char(inttotime("+establishdate+"/1000),'yyyymmdd') establishdate,'"+managerurl\
                  +"' managerurl,'"+mandatorname+"' mandatorname from dual) b on (a.id=b.id)\n"\
                  "when matched then update set a.fundno=b.fundno,a.fundname=b.fundname,a.managername=b.managername,"\
                  "a.managertype=b.managertype,a.workingstate=b.workingstate,a.putonrecorddate=b.putonrecorddate,"\
                  "a.lastquarterupdate=b.lastquarterupdate,a.isdeputemanage=b.isdeputemanage,a.url=b.url,a.establishdate"\
                  "=b.establishdate,a.managerurl=b.managerurl,a.mandatorname=b.mandatorname\n"\
                  "when not matched then insert (a.id,a.fundno,a.fundname,a.managername,a.managertype,a.workingstate,"\
                  "a.putonrecorddate,a.lastquarterupdate,a.isdeputemanage,a.url,a.establishdate,a.managerurl,a.mandatorname)\n"\
                  "values(b.id,b.fundno,b.fundname,b.managername,b.managertype,b.workingstate,b.putonrecorddate,"\
                  "b.lastquarterupdate,b.isdeputemanage,b.url,b.establishdate,b.managerurl,b.mandatorname)"
            sql = Trysql(sql)           
            html_doc1 = getHtml(fundurl)
            html = etree.HTML(html_doc1)
            jjmc = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[1]/td[2]'))).strip().replace('\'','\'\'')
            jjbm = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[2]/td[2]'))).strip().replace('\'','\'\'')
            clsj = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[3]/td[2]'))).strip().replace('-','')
            basj = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[4]/td[2]'))).strip().replace('-','')
            jjbajd = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[5]/td[2]'))).strip().replace('-','')
            jjlx = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[6]/td[2]'))).strip().replace('\'','\'\'')
            bz = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[7]/td[2]'))).strip().replace('\'','\'\'')
            jjglr = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[8]/td[2]'))).strip().replace('\'','\'\'')
            gllx = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[9]/td[2]'))).strip().replace('\'','\'\'')
            tgr = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[10]/td[2]'))).strip().replace('\'','\'\'')
            yzzt = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[11]/td[2]'))).strip().replace('\'','\'\'')
            jjxxzhgxsj = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[12]/td[2]'))).strip().replace('-','')
            jjxhtbts = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[13]/td[2]'))).strip().replace('\'','\'\'')
            dyyb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[15]/td[2]'))).strip().replace('\'','\'\'')
            bnb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[16]/td[2]'))).strip().replace('\'','\'\'')
            nb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[17]/td[2]'))).strip().replace('\'','\'\'')
            jb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[18]/td[2]'))).strip().replace('\'','\'\'')
            sql2 = "merge into amac_smjj_fund a\n"\
                  "using(select '"+jjmc+"' jjmc,'"+jjbm+"' jjbm,'"+clsj+"' clsj,'"+basj+"' basj,'"+jjbajd+"' jjbajd,'"\
                  +jjlx+"' jjlx,'"+bz+"' bz,'"+jjglr+"' jjglr,'"+gllx+"' gllx,'"+tgr+"' tgr,'"+yzzt+"' yzzt,'"\
                  +jjxxzhgxsj+"' jjxxzhgxsj,'"+jjxhtbts+"' jjxhtbts,'"+dyyb+"' dyyb,'"+bnb+"' bnb,'"+nb+"' nb,'"\
                  +jb+"' jb from dual) b on (a.jjmc=b.jjmc)\n"\
                  "when matched then update set a.jjbm=b.jjbm,a.clsj=b.clsj,a.basj=b.basj,a.jjbajd=b.jjbajd,"\
                  "a.jjlx=b.jjlx,a.bz=b.bz,a.jjglr=b.jjglr,a.gllx=b.gllx,a.tgr=b.tgr,a.yzzt=b.yzzt,a.jjxxzhgxsj"\
                  "=b.jjxxzhgxsj,a.jjxhtbts=b.jjxhtbts,a.dyyb=b.dyyb,a.bnb=b.bnb,a.nb=b.nb,a.jb=b.jb\n"\
                  "when not matched then insert (a.jjmc,a.jjbm,a.clsj,a.basj,a.jjbajd,a.jjlx,a.bz,a.jjglr,"\
                   "a.gllx,a.tgr,a.yzzt,a.jjxxzhgxsj,a.jjxhtbts,a.dyyb,a.bnb,a.nb,a.jb)\n"\
                  "values(b.jjmc,b.jjbm,b.clsj,b.basj,b.jjbajd,b.jjlx,b.bz,b.jjglr,b.gllx,b.tgr,b.yzzt,"\
                   "b.jjxxzhgxsj,b.jjxhtbts,b.dyyb,b.bnb,b.nb,b.jb)"
            sql2 = Trysql(sql2)
            html_doc2 = getHtml(managerurl)
            html2 = etree.HTML(html_doc2)
            jgcxxx = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[1]/td[2]')))).strip().replace('\'','\'\'')
            jjglrch = str(ListTemp(html2.xpath('//*[@id="complaint2"]'))).replace('&nbsp','').strip().replace('\'','\'\'')
            jjglrzh = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[4]/td[2]'))).strip().replace('\'','\'\'')
            djbh = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[5]/td[2]'))).strip().replace('\'','\'\'')
            zzjgdm = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[6]/td[2]'))).strip().replace('\'','\'\'')
            djsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[7]/td[2]'))).strip().replace('-','')
            clsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[7]/td[4]'))).strip().replace('-','')
            zcdz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[8]/td[2]'))).strip().replace('\'','\'\'')
            bgdz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[9]/td[2]'))).strip().replace('\'','\'\'')
            zczb = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[10]/td[2]'))).strip().replace('\'','\'\'')
            sjzb = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[10]/td[4]'))).strip().replace('\'','\'\'')
            qyxz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[11]/td[2]'))).strip().replace('\'','\'\'')
            zczbsjbl = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[11]/td[4]'))).strip().replace('\'','\'\'')
            gljjzylb = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[12]/td[2]'))).strip().replace('\'','\'\'')
            sqqtywlx = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[12]/td[4]'))).strip().replace('\'','\'\'')
            ygrs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[13]/td[2]'))).strip().replace('\'','\'\'')
            jgwz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[13]/td[4]'))).strip().replace('\'','\'\'')
            sfwhy = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[15]/td[2]'))).strip().replace('\'','\'\'')
            if sfwhy == '是' :           
                dqhylx = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[16]/td[2]'))).strip().replace('\'','\'\'')
                rhsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[16]/td[4]'))).strip().replace('-','')
                flyjszt = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[18]/td[2]'))).strip().replace('\'','\'\'')
                if flyjszt == '办结':
                    lsswsmc = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[19]/td[2]'))).strip().replace('\'','\'\'')
                    lsxm = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[2]'))).strip().replace('\'','\'\'')
                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]'))).strip().replace('\'','\'\'')
                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[2]'))).strip().replace('\'','\'\'')
                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[4]'))).strip().replace('\'','\'\'')
                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[24]/td[2]')))).strip().replace('\'','\'\'')
                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[25]/td[2]')))).strip().replace('\'','\'\'')
                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[30]/td[2]'))).strip().replace('-','')
                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip().replace('\'','\'\'')
                else :
                    lsswsmc = ''
                    lsxm = ''
                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[2]'))).strip().replace('\'','\'\'')
                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[2]'))).strip().replace('\'','\'\'')
                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[4]'))).strip().replace('\'','\'\'')
                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]')))).strip().replace('\'','\'\'')
                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[2]')))).strip().replace('\'','\'\'')
                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[28]/td[2]'))).strip().replace('-','')
                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip()
            else:
                dqhylx = ''
                rhsj = ''
                flyjszt = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[17]/td[2]'))).strip().replace('\'','\'\'')
                if flyjszt == '办结' :
                    lsswsmc = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[18]/td[2]'))).strip().replace('\'','\'\'')
                    lsxm = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[19]/td[2]'))).strip().replace('\'','\'\'')
                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[2]'))).strip().replace('\'','\'\'')
                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]'))).strip().replace('\'','\'\'')
                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[4]'))).strip().replace('\'','\'\'')
                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[2]')))).strip().replace('\'','\'\'')
                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[24]/td[2]')))).strip().replace('\'','\'\'')
                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[29]/td[2]'))).strip().replace('-','')
                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip().replace('\'','\'\'')
                else :
                    lsswsmc = ''
                    lsxm = ''
                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[19]/td[2]'))).strip().replace('\'','\'\'')
                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[2]'))).strip().replace('\'','\'\'')
                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[4]'))).strip().replace('\'','\'\'')
                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[2]')))).strip().replace('\'','\'\'')
                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]')))).strip().replace('\'','\'\'')
                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[27]/td[2]'))).strip().replace('-','')
                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip().replace('\'','\'\'')                 
            sql3 = "declare \n"\
                   "   gzll_v clob; \n"\
                   "   ggqk_v clob; \n"\
                   "begin \n"\
                   "   gzll_v := '"+gzll+"';\n"\
                   "   ggqk_v := '"+ggqk+"';\n"\
                   "   merge into amac_smjj_manager a\n"\
                   "   using(select '"+jgcxxx+"' jgcxxx,'"+jjglrch+"' jjglrch,'"+jjglrzh+"' jjglrzh,'"+djbh+"' djbh,'"\
                   +zzjgdm+"' zzjgdm,'"+djsj+"' djsj,'"+clsj+"' clsj,'"+zcdz+"' zcdz,'"+bgdz+"' bgdz,'"+zczb+"' zczb,'"\
                   +sjzb+"' sjzb,'"+qyxz+"' qyxz,'"+zczbsjbl+"' zczbsjbl,'"+gljjzylb+"' gljjzylb,'"+sqqtywlx+"' sqqtywlx,'"\
                   +ygrs+"' ygrs,'"+jgwz+"' jgwz,'"+sfwhy+"' sfwhy,'"+dqhylx+"' dqhylx,'"+rhsj+"' rhsj,'"+flyjszt\
                   +"' flyjszt,'"+lsswsmc+"' lsswsmc,'"+lsxm+"' lsxm,'"+fddbr+"' fddbr,'"+sfycyzg+"' sfycyzg,'"+zgqdfs\
                   +"' zgqdfs,gzll_v gzll,ggqk_v ggqk,'"+jgxxzhgxsj+"' jgxxzhgxsj,'"+tbtsxx+"' tbtsxx from dual) b on (a.djbh=b.djbh)\n"\
                   "   when matched then update set a.jgcxxx=b.jgcxxx,a.jjglrch=b.jjglrch,a.jjglrzh=b.jjglrzh,a.zzjgdm=b.zzjgdm,"\
                   "a.djsj=b.djsj,a.clsj=b.clsj,a.zcdz=b.zcdz,a.bgdz=b.bgdz,a.zczb=b.zczb,a.sjzb=b.sjzb,a.qyxz=b.qyxz,a.zczbsjbl"\
                   "=b.zczbsjbl,a.gljjzylb=b.gljjzylb,a.sqqtywlx=b.sqqtywlx,a.ygrs=b.ygrs,a.jgwz=b.jgwz,a.sfwhy=b.sfwhy,"\
                   "a.dqhylx=b.dqhylx,a.rhsj=b.rhsj,a.flyjszt=b.flyjszt,a.lsswsmc=b.lsswsmc,a.lsxm=b.lsxm,a.fddbr=b.fddbr,"\
                   "a.sfycyzg=b.sfycyzg,a.zgqdfs=b.zgqdfs,a.gzll=b.gzll,a.ggqk=b.ggqk,a.jgxxzhgxsj=b.jgxxzhgxsj,a.tbtsxx=b.tbtsxx\n"\
                   "   when not matched then insert (a.jgcxxx,a.jjglrch,a.jjglrzh,a.djbh,a.zzjgdm,a.djsj,a.clsj,a.zcdz,a.bgdz,a.zczb,"\
                   "a.sjzb,a.qyxz,a.zczbsjbl,a.gljjzylb,a.sqqtywlx,a.ygrs,a.jgwz,a.sfwhy,a.dqhylx,a.rhsj,a.flyjszt,a.lsswsmc,"\
                   "a.lsxm,a.fddbr,a.sfycyzg,a.zgqdfs,a.gzll,a.ggqk,a.jgxxzhgxsj,a.tbtsxx)\n"\
                   "values(b.jgcxxx,b.jjglrch,b.jjglrzh,b.djbh,b.zzjgdm,b.djsj,b.clsj,b.zcdz,b.bgdz,b.zczb,"\
                   "b.sjzb,b.qyxz,b.zczbsjbl,b.gljjzylb,b.sqqtywlx,b.ygrs,b.jgwz,b.sfwhy,b.dqhylx,b.rhsj,b.flyjszt,b.lsswsmc,"\
                   "b.lsxm,b.fddbr,b.sfycyzg,b.zgqdfs,b.gzll,b.ggqk,b.jgxxzhgxsj,b.tbtsxx);\n"\
                   "end;"
            sql3 = Trysql(sql3)
        except :
          q.put(url3)

def main():
    GetQ()
    #Thread()
    Work()
            
if __name__ == '__main__':
   strat = time.time()
   main()
   end = time.time()
   m, s = divmod(end-start, 60)
   h, m = divmod(m, 60)
   print("运行时长：%02d:%02d:%02d" % (h, m, s))








