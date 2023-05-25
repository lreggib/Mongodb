#!/usr/bin/env python
# -*- coding:utf-8 -*-
# coding=utf-8
import sys
from  argparse import  ArgumentParser
import subprocess

import logging,time
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=time.strftime("%Y-%m-%d.log", time.localtime()),
                    filemode='a')

local_bak_expire_days = 7

# 空间保护
def checkDiskSpace(back_dir):
    use_percent = psutil.disk_usage(back_dir).percent
    #G
    # free_space= psutil.disk_usage(back_dir).free/1024/1024/1024
    # return  use_percent,free_space
    return use_percent

# 删除过期备份
def markDel(base_bak_dir,catalogdb,local_bak_expire_days):

    expire_days_ago = (datetime.datetime.now() - datetime.timedelta(days=local_bak_expire_days)).strftime("%Y-%m-%d")

    cmd = "find %s -name  %s  | xargs  rm -rf " %  (base_bak_dir,expire_days_ago)

    logging.info('清理本地过期备份：%s' % cmd)
    state = runProcess(cmd)

    if state==0:
        logging.info('清理本地%s天前过期备份完成！' % local_bak_expire_days)
        is_deleted = 'Y'
    else:
        logging.warning('清理本地%s天前过期备份失败！' % local_bak_expire_days)
        is_deleted = 'N'

    sql = "update user_backup set is_deleted='Y' where bk_dir like  '%s%s%s' " % (base_bak_dir,expire_days_ago,'%')
    print('markDel:' + sql)
    logging.info('markDel:' + sql)
    catalogdb.dml(sql)
    catalogdb.commit()

    return is_deleted


# 执行备份
def run_dump(cmd):
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while child.poll() == None:
        stdout_line = child.stdout.readline().strip()
        if stdout_line:
            logging.info(stdout_line)
    logging.info(child.stdout.read().strip())
    state = child.returncode

    if state != 0:
        print(' Backup Failed!')
        sys.exit()
    elif state == 0:
        print('End Backup success...')


#  备份工具
class mongodumper:

    def __init__(self):
        usage = "\n"
        usage += "python " + sys.argv[0] + " [options]\n"
        usage += "\n"
        usage += "Desc: mongodb dump tookit\n"
        usage += "example:\n"
    
        usage += "dump all db :  mongodumper --uri  \n"
        usage += "dump db :  mongodumper  uri --uri -d \n"
        usage += "dump collection :     mongodumper  --uri -d -c \n"
        usage += "dump with query :  mongodumper --uri -q '{}' \n"
        usage += "dump with gizp : mongodumper  --gzip  \n"
        usage += "dump with oplog : mongodumper  --oplog  \n"
        usage += "backup dir : mongodumper -o  \n"
        
        # self.parser = OptionParser(usage.decode('utf-8'))
        self.parser = ArgumentParser(usage)
        self.parser.add_argument("--uri",dest="uri",metavar='mongodb-uri',help="mongodb uri connection string")
        self.parser.add_argument('-d', "--db",dest="db",metavar="<database-name>",help="database to use ")
        self.parser.add_argument("-c", "--collection",dest="collection",metavar='<collection-name>',help="collection to use")
        self.parser.add_argument("--gzip",help="compress archive or collection output with Gzip",action="store_true") 
        self.parser.add_argument("-q", "--query",dest="query",help="dquery filter")
        self.parser.add_argument("--oplog",help="use oplog for taking a point-in-time snapshot",action="store_true" )
        self.parser.add_argument("-o","--out",dest="out",metavar="<directory-path>",help="output directory")
        
        self.args = self.parser.parse_args()


    def dump_cmd(self):
        
        '''
        mongodb://root:VZwy72L6RYWFPA6M@10.51.102.10:3310,10.51.102.11:3310,10.51.102.12:3310/test?replicaSet=big_sh1&readPreference=secondary&authSource=admin
        '''

        uri = self.args.uri

        if len(uri.split(','))>1:
            if len(uri.split('/'))>1:
                uri = 'mongodb://root:VZwy72L6RYWFPA6M@%s/?replicaSet=%s&readPreference=secondary&authSource=admin' % (uri.split('/')[0],uri.split('/')[1])
            else:
               uri = 'mongodb://root:VZwy72L6RYWFPA6M@%s/?readPreference=secondary&authSource=admin' % uri

        else:
            uri = 'mongodb://root:VZwy72L6RYWFPA6M@%s/?authSource=admin' % uri 

        print(uri)

        cmd = 'mongodump --uri="%s"  ' % uri

        if self.args.db:
            if self.args.collection:
                if self.args.query:
                    cmd+= '-d %s  -c %s -q %s ' % (self.args.db,self.args.collection,self.args.query)
                else:
                    cmd += '-d %s  -c %s ' % (self.args.db,self.args.collection)
            else:
                cmd += '-d %s ' % self.args.db

        if self.args.out:
            if self.args.out.endswith('/'):
                cmd+= '-o %s' % self.args.out + time.strftime("%Y_%m_%d", time.localtime()) 
                print(cmd,'.............')
            else:

                cmd+= '-o %s' % self.args.out +'/' + time.strftime("%Y_%m_%d", time.localtime()) 

        if self.args.gzip: 
            cmd+= ' --gzip'
        if self.args.oplog:
            cmd+= ' --oplog'
        return cmd
  
if __name__ =="__main__":
    dumper = mongodumper()

    cmd = dumper.dump_cmd()

    try:
        run_dump(cmd)
    except Exception as e:
        logging.error("dump mongodb failed...")
        logging.error(e)

    # 删除本地备份
    
    
    # 检查磁盘情况是否满足备份

    # 备份

    # 是否远程归档
